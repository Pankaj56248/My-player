"""
Telegram Video Streaming Bot - Hybrid Resolver  (pyrofork + curl_cffi + yt-dlp)
===============================================================================

Every link ends in one of two ways: a "Play Video" button that opens the Mini App
player with ?stream=<direct media url>, or - only if every layer failed - the
fallback button chosen by FALLBACK_MODE, plus a silent report in LOG_CHANNEL_ID.

Pipeline
--------
  Layer 0  URL already ends in .m3u8 / .mp4 / .mkv         -> instant, no scraping
  Layer 1  yt-dlp (asyncio.to_thread, format=best)          -> direct media URL
  Layer 2  custom scrapers (curl_cffi, Chrome TLS fingerprint)
             streaam.net   /T/<id> page, <script> keys, packed JS, base64 / reversed
                           strings, iframes
             filesdl.top   any subdomain, /cloud/<id>: media links, download buttons,
                           countdown + form POST emulation
             generic       any other page: HTML / packed JS / iframes
  (hosts with a dedicated scraper - streaam.net, filesdl.top - run Layer 2 first,
   because yt-dlp has no extractor for them and would only burn 25 s)
  Failed   both layers failed -> fallback button (see FALLBACK_MODE) + report in the
           log channel (URL, user, reasons, dump of the page's player scripts).

Cloudflare: all page fetches use curl_cffi.AsyncSession(impersonate="chrome124"),
which sends a real Chrome TLS/HTTP2 fingerprint. That fixes fingerprint-based 403s.
It cannot solve JS challenges / Turnstile, and datacenter IPs can still be blocked
- set PROXY_URL to route scraper traffic through a residential/other proxy.

Environment variables
---------------------
  API_ID, API_HASH, BOT_TOKEN, GITHUB_PLAYER_URL      required
  PORT                  optional (default 8080) - Flask health server
  LOG_CHANNEL_ID        optional - e.g. -1001234567890 (bot must be admin)
  RENDER_EXTERNAL_URL   optional - set by Render, used for a keep-alive ping
  FALLBACK_MODE         when every layer fails:
                          direct  (default) "Play Video" opens the ORIGINAL site in the
                                  user's own Telegram/phone, where Cloudflare verifies
                          iframe  button opens the player with ?fallback=<link>
                          message friendly "could not process" message only
  IMPERSONATE           optional - curl_cffi browser target (default chrome124)
  PROXY_URL             optional - http(s)://user:pass@host:port for scraper traffic
  DISABLE_YTDLP         optional - "1" skips Layer 1

Add a host: subclass PageResolver (HTML hosts) or BaseResolver and put @register
above the class (see StreaamResolver / FilesdlResolver).
"""

import asyncio

# An event loop must exist before pyrogram is imported (Python 3.12+).
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import base64
import functools
import html
import logging
import os
import re
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from urllib.parse import quote, unquote, urlencode, urljoin, urlparse

import requests
from curl_cffi.requests import AsyncSession
from flask import Flask
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatType, MessageEntityType
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from werkzeug.serving import make_server

# ----------------------------------------------------------------------------
# Logging + environment
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("video-bot")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


try:
    API_ID = int(require_env("API_ID"))
except ValueError:
    sys.exit("API_ID must be a number")

API_HASH = require_env("API_HASH")
BOT_TOKEN = require_env("BOT_TOKEN")
GITHUB_PLAYER_URL = require_env("GITHUB_PLAYER_URL")
PORT = int(os.environ.get("PORT", 8080))
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
FALLBACK_MODE = os.environ.get("FALLBACK_MODE", "direct").strip().lower()
if FALLBACK_MODE not in ("direct", "iframe", "message"):
    FALLBACK_MODE = "direct"
IMPERSONATE = os.environ.get("IMPERSONATE", "chrome124").strip() or "chrome124"
PROXY_URL = os.environ.get("PROXY_URL", "").strip()
DISABLE_YTDLP = os.environ.get("DISABLE_YTDLP", "").strip() == "1"

# Terabox resolver: configure an authorized converter API endpoint in Render.
# The endpoint must accept ?url=<Terabox URL> and return JSON containing a
# direct media URL (for example: dlink, direct_url, download_url, or url).
TERABOX_CONVERTER_URL = os.environ.get("TERABOX_CONVERTER_URL", "").strip()
CLOUDFLARE_WORKER_URL = os.environ.get("CLOUDFLARE_WORKER_URL", "").strip()
TERABOX_DOMAINS = ("terabox.com", "teraboxapp.com", "1024tera.com", "freeterabox.com")

LOG_CHANNEL_ID = None
_raw_log_id = os.environ.get("LOG_CHANNEL_ID", "").strip()
if _raw_log_id:
    try:
        LOG_CHANNEL_ID = int(_raw_log_id)
    except ValueError:
        log.warning("LOG_CHANNEL_ID is not a valid integer - channel logging disabled.")

if not GITHUB_PLAYER_URL.lower().startswith("https://"):
    log.warning("GITHUB_PLAYER_URL should start with https:// (Mini Apps require HTTPS).")

MSG_FAILED = (
    "Yeh video stream abhi process nahi ho saki. "
    "Yeh link hamare developer ko report kar di gayi hai, jaldi support add kiya jayega."
)

# ----------------------------------------------------------------------------
# Log channel (plain Bot API over HTTP: works from any thread, even if the
# Pyrogram client is down). Async callers use `await alog(...)` / `adoc(...)`.
# ----------------------------------------------------------------------------
def esc(text: str) -> str:
    return html.escape(text, quote=False)


def fit_pre(text: str, limit: int = 3300, tail: bool = False) -> str:
    """HTML-escape `text`, shrinking it until the escaped form fits `limit`."""
    while len(esc(text)) > limit and len(text) > 50:
        keep = int(len(text) * 0.8)
        text = text[-keep:] if tail else text[:keep]
    return esc(text)


def send_log_sync(text: str) -> None:
    if not LOG_CHANNEL_ID:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": LOG_CHANNEL_ID, "text": text[:4096], "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=15,
        )
        if resp.status_code == 400:  # broken HTML? resend as plain text
            plain = html.unescape(re.sub(r"<[^>]+>", "", text))[:4096]
            resp = requests.post(
                url,
                json={"chat_id": LOG_CHANNEL_ID, "text": plain, "disable_web_page_preview": True},
                timeout=15,
            )
        if resp.status_code != 200:
            log.warning("Log channel rejected message: %s %s", resp.status_code, resp.text[:200])
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not post to log channel: %s", exc)


def send_log_doc_sync(filename: str, data: bytes, caption: str = "") -> None:
    if not LOG_CHANNEL_ID:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
            data={"chat_id": LOG_CHANNEL_ID, "caption": caption[:1000]},
            files={"document": (filename, data)},
            timeout=30,
        )
        if resp.status_code != 200:
            log.warning("Log channel rejected document: %s %s", resp.status_code, resp.text[:200])
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not post document to log channel: %s", exc)


async def alog(text: str) -> None:
    await asyncio.to_thread(send_log_sync, text)


async def adoc(filename: str, data: bytes, caption: str = "") -> None:
    await asyncio.to_thread(send_log_doc_sync, filename, data, caption)


def describe_user(message) -> str:
    user = message.from_user
    if not user:
        chat = message.chat
        return f"chat <code>{chat.id}</code>" if chat else "unknown"
    uname = f"@{esc(user.username)}" if user.username else "no username"
    return f"<code>{user.id}</code> ({uname})"


def crash_text(context: str, exc: BaseException) -> str:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return (
        f"🔥 <b>Unhandled exception</b>\n<b>Where:</b> {esc(context[:300])}\n\n"
        f"<pre>{fit_pre(tb, 3200, tail=True)}</pre>"
    )


def _sys_hook(exc_type, exc, tb):
    log.error("Uncaught exception", exc_info=(exc_type, exc, tb))
    if exc is not None:
        send_log_sync(crash_text("sys.excepthook", exc))


def _thread_hook(args):
    log.error("Uncaught thread exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
    if args.exc_value is not None:
        send_log_sync(crash_text(f"thread {getattr(args.thread, 'name', '?')}", args.exc_value))


sys.excepthook = _sys_hook
threading.excepthook = _thread_hook

# ----------------------------------------------------------------------------
# Flask keep-alive server (Render port binding) - daemon thread
# ----------------------------------------------------------------------------
web = Flask(__name__)


@web.route("/")
def home():
    return "Bot is running (200 OK)", 200


@web.route("/health")
def health():
    return {"status": "ok"}, 200


def run_web() -> None:
    server = make_server("0.0.0.0", PORT, web, threaded=True)
    log.info("Flask server listening on 0.0.0.0:%s", PORT)
    server.serve_forever()


def keep_alive_ping() -> None:
    if not RENDER_EXTERNAL_URL:
        return
    while True:
        time.sleep(600)
        try:
            requests.get(RENDER_EXTERNAL_URL, timeout=15)
        except Exception as exc:  # noqa: BLE001
            log.warning("Keep-alive ping failed: %s", exc)


# ----------------------------------------------------------------------------
# Async HTTP client: curl_cffi with a real Chrome TLS fingerprint (Cloudflare-friendly)
# One shared session for all users (cookies such as cf_clearance are reused).
# ----------------------------------------------------------------------------
MAX_PAGE_BYTES = 2_000_000
MEDIA_EXTS = (".m3u8", ".mp4", ".webm", ".mkv")
HLS_CTYPES = ("application/vnd.apple.mpegurl", "application/x-mpegurl", "audio/mpegurl")
DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)   # only used by yt-dlp; curl_cffi sends its own matching Chrome headers


def _decode(data: bytes, charset) -> str:
    try:
        return data.decode(charset or "utf-8", "ignore")
    except LookupError:
        return data.decode("utf-8", "ignore")


def _charset(ctype: str):
    m = re.search(r"charset=([\w-]+)", ctype or "")
    return m.group(1) if m else None


def origin_of(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def has_media_ext(url: str) -> bool:
    return urlparse(url).path.lower().endswith(MEDIA_EXTS)


def is_media_ctype(ctype: str, url: str) -> bool:
    ctype = (ctype or "").lower()
    if ctype.startswith(("video/", "audio/")) or ctype.startswith(HLS_CTYPES):
        return True
    return ctype.startswith("application/octet-stream") and has_media_ext(url)


_CHALLENGE_MARKERS = (
    "just a moment", "cf-chl", "challenge-platform", "cf_chl_opt", "turnstile",
    "attention required! | cloudflare", "checking your browser",
)


def looks_like_challenge(text: str) -> bool:
    low = (text or "")[:6000].lower()
    return any(m in low for m in _CHALLENGE_MARKERS)


class FetchContext:
    """Collects HTTP outcomes (and a script dump) so a failure can be explained."""

    def __init__(self):
        self.notes: list[str] = []
        self.dump: str | None = None

    def note(self, text: str) -> None:
        self.notes.append(text)

    def reason(self, default: str) -> str:
        return "; ".join(self.notes[-6:]) if self.notes else default


@dataclass
class Probe:
    kind: str            # hls | video | html | dead | unknown
    final_url: str = ""


class Http:
    def __init__(self):
        self.session = None
        self.impersonate = IMPERSONATE

    def _new_session(self):
        kwargs = {
            "impersonate": self.impersonate,
            "timeout": 20,
            "max_clients": 50,
            "allow_redirects": True,
        }
        if PROXY_URL:
            kwargs["proxies"] = {"http": PROXY_URL, "https": PROXY_URL}
        return AsyncSession(**kwargs)

    async def start(self):
        self.session = self._new_session()

    async def close(self):
        if self.session:
            try:
                await self.session.close()
            except Exception:  # noqa: BLE001
                pass

    async def _request(self, method: str, url: str, headers=None, data=None,
                       limit: int = MAX_PAGE_BYTES, skip_media: bool = False):
        """Returns (status, content_type, final_url, body). Streams the body and stops
        after `limit` bytes (or reads nothing for media responses when skip_media)."""
        resp = None
        for attempt in (1, 2):
            try:
                resp = await self.session.request(method, url, headers=headers, data=data, stream=True)
                break
            except Exception as exc:  # noqa: BLE001
                if attempt == 1 and "mpersonat" in str(exc) and self.impersonate != "chrome":
                    log.warning("Impersonation target %s unsupported - switching to 'chrome'", self.impersonate)
                    self.impersonate = "chrome"
                    await self.close()
                    self.session = self._new_session()
                    continue
                raise

        ctype = (resp.headers.get("content-type") or "").lower()
        final = str(resp.url)
        body = b""
        try:
            if not (skip_media and is_media_ctype(ctype, final)):
                try:
                    async for chunk in resp.aiter_content():
                        body += chunk
                        if len(body) >= limit:
                            break
                except AttributeError:
                    body = (resp.content or b"")[:limit]
        finally:
            try:
                await resp.aclose()
            except Exception:  # noqa: BLE001
                pass
        return resp.status_code, ctype, final, body[:limit]

    async def get_text(self, url: str, referer: str, ctx: FetchContext, attempts: int = 2) -> str:
        for _ in range(attempts):
            try:
                status, ctype, _final, body = await self._request("GET", url, headers={"Referer": referer})
            except Exception as exc:  # noqa: BLE001
                ctx.note(f"{type(exc).__name__}: {str(exc)[:80]} on {url}")
                await asyncio.sleep(1)
                continue

            text = _decode(body, _charset(ctype))
            challenge = looks_like_challenge(text)
            if status == 200:
                if challenge:
                    ctx.note(f"Cloudflare JS challenge / Turnstile page on {url} (needs a real browser)")
                return text
            cf = status in (403, 429, 503) and challenge
            ctx.note(f"HTTP {status}{' (Cloudflare challenge)' if cf else ''} on {url}")
            if status in (404, 410):
                break  # retrying a missing page is pointless
            await asyncio.sleep(1)
        return ""

    async def submit(self, url: str, data: dict, method: str, referer: str, ctx: FetchContext):
        """Emulate a browser form submit. Returns (final_url, content_type, text) or None.
        If the response is already media, no body is read."""
        headers = {"Referer": referer, "Origin": origin_of(referer)}
        try:
            if method == "post":
                status, ctype, final, body = await self._request("POST", url, headers=headers, data=data, skip_media=True)
            else:
                sep = "&" if "?" in url else "?"
                status, ctype, final, body = await self._request("GET", url + sep + urlencode(data), headers=headers, skip_media=True)
        except Exception as exc:  # noqa: BLE001
            ctx.note(f"{type(exc).__name__}: {str(exc)[:80]} on form {method.upper()} {url}")
            return None
        if status >= 400:
            ctx.note(f"HTTP {status} on form {method.upper()} {url}")
            return None
        if is_media_ctype(ctype, final):
            return final, ctype, ""
        return final, ctype, _decode(body, _charset(ctype))

    async def probe(self, url: str, referer: str) -> Probe:
        """Look at the first 2 KB to classify a URL. 'unknown' = could not tell
        (403, timeout...) - IP/referer-bound links often work for the real client."""
        try:
            status, ctype, final, chunk = await self._request(
                "GET", url, headers={"Referer": referer, "Range": "bytes=0-2047"}, limit=2048
            )
        except Exception:  # noqa: BLE001
            return Probe("unknown", url)
        if status in (404, 410):
            return Probe("dead", final)
        if status in (200, 206):
            head = chunk.lstrip()[:16].lower()
            if b"#extm3u" in chunk[:1024].lower():
                return Probe("hls", final)
            if (ctype.startswith(("video/", "audio/")) or b"ftyp" in chunk[:64]
                    or chunk[:4] == b"\x1a\x45\xdf\xa3"):
                return Probe("video", final)
            if "text/html" in ctype or head.startswith((b"<!doctype", b"<html")):
                return Probe("html", final)
        return Probe("unknown", final)


HTTP = Http()

# ----------------------------------------------------------------------------
# Page parsing: candidates, eval(packed), base64 / reversed strings, key patterns
# ----------------------------------------------------------------------------
_EXT = r"(?:m3u8|mp4|webm|mkv)"
ABS_MEDIA_RE = re.compile(
    r"""https?://[^\s"'<>\\()\[\]{}|]+?\.""" + _EXT + r"""(?:\?[^\s"'<>\\()\[\]{}|]*)?""", re.IGNORECASE
)
REL_MEDIA_RE = re.compile(
    r"""["'](/?[^"'\s<>]+?\.""" + _EXT + r"""(?:\?[^"'\s<>]*)?)["']""", re.IGNORECASE
)
# sources:[{file:"..."}], file:"...", hls2:"...", src:"...", "url":"..." inside scripts / JSON
KEY_RE = re.compile(
    r"""["']?\b(?:file|src|source|hls\d?|stream|video_url|playlist|url|link|download)["']?\s*[:=]\s*"""
    r"""["']((?:https?:)?//[^"'\s<>]+|/[^"'\s<>]+)["']""",
    re.IGNORECASE,
)
SOURCE_TAG_RE = re.compile(r"""<(?:source|video)\b[^>]*?\bsrc=["']([^"']+)["']""", re.IGNORECASE)
DENY_RE = re.compile(r"\.(?:jpe?g|png|gif|webp|svg|ico|css|js|json|vtt|srt|woff2?|ttf|html?)(?:\?|$)", re.IGNORECASE)

SCRIPT_RE = re.compile(r"<script\b[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)
IFRAME_RE = re.compile(r"""<iframe[^>]+src=["']([^"']+)["']""", re.IGNORECASE)
META_REFRESH_RE = re.compile(
    r"""<meta[^>]+http-equiv=["']refresh["'][^>]+content=["'][^"']*url=([^"']+)["']""", re.IGNORECASE
)
JS_REDIRECT_RE = re.compile(
    r"""(?:location(?:\.href)?\s*=|location\.(?:replace|assign)\()\s*["']([^"']+)["']""", re.IGNORECASE
)
LITERAL_RE = re.compile(r"""(["'])((?:\\.|(?!\1)[^\\\r\n]){16,}?)\1""")

# Dean Edwards packer: eval(function(p,a,c,k,e,d){...}('payload',radix,count,'a|b|c'.split('|'),0,{}))
PACKED_RE = re.compile(
    r"""\}\(\s*(['"])(.*?)\1\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(['"])(.*?)\5\s*\.split\(\s*['"]\|['"]\s*\)""",
    re.DOTALL,
)
_ALPHA = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
_KEYWORDS = ("http", ".m3u8", ".mp4", "eval(", "sources", "file:", "file\":")


def _encode(num: int, radix: int) -> str:
    head = "" if num < radix else _encode(num // radix, radix)
    rem = num % radix
    return head + (chr(rem + 29) if rem > 35 else _ALPHA[rem])


def _decode_packed(match: "re.Match"):
    try:
        payload = match.group(2)
        radix = int(match.group(3))
        count = int(match.group(4))
        words = match.group(6).split("|")
        if radix < 2:
            return None
        payload = payload.replace("\\'", "'").replace('\\"', '"').replace("\\\\", "\\")
        table = {}
        for i in range(count):
            key = _encode(i, radix)
            table[key] = words[i] if i < len(words) and words[i] else key
        return re.sub(r"\b\w+\b", lambda m: table.get(m.group(0), m.group(0)), payload)
    except Exception as exc:  # noqa: BLE001
        log.debug("unpack failed: %s", exc)
        return None


def unpack_all(text: str, depth: int = 0) -> list[str]:
    results: list[str] = []
    if depth > 3:
        return results
    for match in PACKED_RE.finditer(text):
        out = _decode_packed(match)
        if out:
            results.append(out)
            results.extend(unpack_all(out, depth + 1))
    return results


def _printable_ratio(s: str) -> float:
    sample = s[:2000]
    if not sample:
        return 0.0
    return sum(1 for c in sample if c.isprintable() or c in "\r\n\t") / len(sample)


def _b64(s: str):
    t = s.strip()
    if len(t) < 16 or not re.fullmatch(r"[A-Za-z0-9+/_\-]+={0,2}", t):
        return None
    t = t.replace("-", "+").replace("_", "/")
    t += "=" * (-len(t) % 4)
    try:
        out = base64.b64decode(t).decode("utf-8", "ignore")
    except Exception:  # noqa: BLE001
        return None
    return out if _printable_ratio(out) > 0.9 else None


def literal_variants(lit: str) -> list[str]:
    """Plain / reversed / base64 / base64+reverse / reverse+base64 / %-escaped decodings."""
    out: list[str] = []
    rev = lit[::-1]
    for cand in (lit, rev):
        dec = _b64(cand)
        if dec:
            out.append(dec)
            out.append(dec[::-1])
    out.append(rev)
    if "%" in lit:
        out.append(unquote(lit))
    if "\\x" in lit or "\\u" in lit:
        try:
            out.append(lit.encode("latin-1", "ignore").decode("unicode_escape"))
        except Exception:  # noqa: BLE001
            pass
    return [o for o in out if any(k in o.lower() for k in _KEYWORDS)]


def decode_literals(text: str) -> list[str]:
    scope = "\n".join(SCRIPT_RE.findall(text)) if "<script" in text.lower() else text
    found: list[str] = []
    for i, m in enumerate(LITERAL_RE.finditer(scope[:700_000])):
        if i >= 400:
            break
        found.extend(literal_variants(m.group(2)))
    return found


def expand_texts(page_html: str) -> list[str]:
    """The page plus everything we can unpack/decode out of it (3 rounds)."""
    texts = [page_html]
    pending = [page_html]
    for _ in range(3):
        nxt: list[str] = []
        for t in pending:
            nxt.extend(unpack_all(t))
            nxt.extend(decode_literals(t))
        nxt = [n for n in dict.fromkeys(nxt) if n not in texts][:40]
        if not nxt:
            break
        texts.extend(nxt)
        pending = nxt
    return texts


def _normalize(text: str) -> str:
    return (
        text.replace("\\/", "/")
        .replace("\\u0026", "&")
        .replace("\\x26", "&")
        .replace("&amp;", "&")
    )


def find_candidates(page_html: str, base_url: str) -> list[tuple[str, bool]]:
    """Ordered (url, strong) candidates. strong = URL has an .m3u8/.mp4/... extension;
    weak ones (from file:/hls:/src: keys without extension) must be verified by probe."""
    m3u8_master: list[str] = []
    m3u8: list[str] = []
    other: list[str] = []
    weak: list[str] = []
    seen: set[str] = set()

    def add(url: str, force_weak: bool = False):
        if not url.startswith("http") or url in seen:
            return
        seen.add(url)
        path = urlparse(url).path.lower()
        if not force_weak and path.endswith(".m3u8"):
            (m3u8_master if "master" in url.lower() else m3u8).append(url)
        elif not force_weak and path.endswith((".mp4", ".webm", ".mkv")):
            other.append(url)
        elif not DENY_RE.search(url):
            weak.append(url)

    for raw in expand_texts(page_html):
        text = _normalize(raw)
        for url in ABS_MEDIA_RE.findall(text):
            add(url)
        for rel in REL_MEDIA_RE.findall(text):
            if not rel.lower().startswith("http"):
                add(urljoin(base_url, rel))
        for src in SOURCE_TAG_RE.findall(text):
            add(urljoin(base_url, src))
        for m in KEY_RE.finditer(text):
            value = m.group(1)
            if DENY_RE.search(value):
                continue
            add(urljoin(base_url, value))

    ordered = [(u, True) for u in m3u8_master + m3u8 + other] + [(u, False) for u in weak[:4]]
    return ordered


def script_dump(page_url: str, page_html: str) -> str:
    """Player-relevant script text for the log channel (so a dedicated scraper can be written)."""
    scripts = [s.strip() for s in SCRIPT_RE.findall(page_html) if s.strip()]
    kw = ("player", "jwplayer", "setup", "sources", "hls", "atob", "eval", "file", "m3u8", "mp4", "token")
    scripts.sort(key=lambda s: sum(s.lower().count(k) for k in kw), reverse=True)
    head = (
        f"URL: {page_url}\nHTML length: {len(page_html)}\n"
        f"Inline scripts: {len(scripts)}\niframes: {IFRAME_RE.findall(page_html)[:5]}\n\n"
    )
    body = "\n\n/* ---- script ---- */\n".join(scripts) if scripts else page_html[:20000]
    return (head + body)[:300_000]


# Anchors / forms / timers (used by hosts with download pages)
ANCHOR_RE = re.compile(r"""<a\b[^>]*?href=["']([^"'#][^"']*)["'][^>]*>(.*?)</a>""", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
HINT_RE = re.compile(r"download|stream|watch|play|direct|get\s*link|server", re.IGNORECASE)
FORM_RE = re.compile(r"<form\b([^>]*)>(.*?)</form>", re.IGNORECASE | re.DOTALL)
INPUT_RE = re.compile(r"<input\b([^>]*)>", re.IGNORECASE)
TIMER_RES = [
    re.compile(r"""(?:count(?:down)?|timer|seconds?|wait(?:ing)?|delay)\w*\s*[=:]\s*(\d{1,3})\b""", re.IGNORECASE),
    re.compile(r"""id=["'](?:countdown|timer|seconds?)["'][^>]*>\s*(\d{1,3})\s*<""", re.IGNORECASE),
    re.compile(r"""(?:please\s+)?wait[^<\d]{0,30}(\d{1,3})\s*(?:s\b|sec)""", re.IGNORECASE),
]
MAX_FORM_WAIT = 20


def _attr(tag: str, name: str) -> str:
    m = re.search(name + r"""\s*=\s*["']([^"']*)["']""", tag, re.IGNORECASE)
    return html.unescape(m.group(1)) if m else ""


def timer_seconds(page_html: str) -> int:
    for rx in TIMER_RES:
        for m in rx.finditer(page_html):
            n = int(m.group(1))
            if 1 <= n <= 120:
                return n
    return 0


# ----------------------------------------------------------------------------
# Resolver framework
# ----------------------------------------------------------------------------
class ResolveError(Exception):
    """Raised when a link cannot be turned into a media URL. `dump` = debug text."""

    def __init__(self, reason: str, dump: str | None = None):
        super().__init__(reason)
        self.reason = reason
        self.dump = dump


class BaseResolver:
    name = "base"
    public = True    # False = catch-all (not used to decide whether to answer in groups)
    prefer_custom = False   # True = run this scraper before yt-dlp

    def matches(self, url: str) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    async def resolve(self, url: str, http: Http) -> str:  # pragma: no cover - interface
        raise NotImplementedError


RESOLVERS: list[BaseResolver] = []


def register(cls):
    """Class decorator: instantiate the resolver and add it to the registry (order matters)."""
    RESOLVERS.append(cls())
    return cls


def find_resolver(url: str):
    for resolver in RESOLVERS:
        try:
            if resolver.matches(url):
                return resolver
        except Exception:  # noqa: BLE001
            log.exception("resolver %s.matches crashed", resolver.name)
    return None


class PageResolver(BaseResolver):
    """Fetch HTML -> media candidates (scripts, packed JS, base64...) -> follow iframes.
    Optional: follow download anchors and emulate countdown + form submits."""

    host_re: re.Pattern = re.compile(r"$^")
    base = ""                    # fixed origin; empty = use the link's own origin
    max_pages = 6
    follow_anchors = False
    handle_forms = False

    def matches(self, url: str) -> bool:
        return bool(self.host_re.match(urlparse(url).netloc))

    def origin(self, url: str) -> str:
        return self.base or origin_of(url)

    def candidates(self, url: str) -> list[str]:
        return [url]

    def discover(self, page_html: str, page_url: str) -> list[str]:
        found = []
        for src in IFRAME_RE.findall(page_html):
            found.append(urljoin(page_url, html.unescape(src)))
        for target in META_REFRESH_RE.findall(page_html) + JS_REDIRECT_RE.findall(page_html):
            found.append(urljoin(page_url, html.unescape(target.strip())))
        return [u for u in found if u.startswith("http")]

    async def _accept(self, cands, page_url: str, http: Http, ctx: FetchContext):
        for url, strong in cands[:6]:
            pr = await http.probe(url, page_url)
            if pr.kind == "dead" or (strong and pr.kind == "html"):
                ctx.note(f"Candidate rejected ({pr.kind}): {url}")
                continue
            if strong:
                return url
            if pr.kind in ("hls", "video"):
                return pr.final_url or url
            ctx.note(f"Weak candidate is not media ({pr.kind}): {url}")
        return None

    async def _follow_anchors(self, page_html, page_url, http, ctx, queue, visited):
        tried = 0
        for href, label in ANCHOR_RE.findall(page_html):
            href = _normalize(html.unescape(href.strip()))
            label = TAG_RE.sub("", label)
            target = urljoin(page_url, href)
            if not target.startswith("http") or target == page_url:
                continue
            media = has_media_ext(target)
            if not (media or HINT_RE.search(label) or HINT_RE.search(href)):
                continue
            if tried >= 5:
                break
            tried += 1
            pr = await http.probe(target, page_url)
            if pr.kind in ("hls", "video"):
                return pr.final_url or target
            if media and pr.kind == "unknown":
                return target
            if pr.kind == "html" and target not in visited:
                queue.append((target, page_url))
        return None

    async def _submit_forms(self, page_html, page_url, http, ctx, queue, visited):
        for attrs, inner in FORM_RE.findall(page_html)[:3]:
            method = (_attr(attrs, "method") or "get").lower()
            method = "post" if method == "post" else "get"
            action = urljoin(page_url, _attr(attrs, "action") or page_url)
            data: dict[str, str] = {}
            hidden = 0
            for tag in INPUT_RE.findall(inner):
                name = _attr(tag, "name")
                if not name:
                    continue
                typ = (_attr(tag, "type") or "text").lower()
                if typ == "hidden":
                    hidden += 1
                    data[name] = _attr(tag, "value")
                elif typ in ("submit", "button") and name not in data:
                    data[name] = _attr(tag, "value")
            if method == "get" and not hidden:
                continue
            if not data and method == "post":
                continue

            wait = timer_seconds(page_html)
            if wait:
                ctx.note(f"waiting {wait}s for countdown before form submit")
                await asyncio.sleep(min(wait, MAX_FORM_WAIT) + 0.5)

            res = await http.submit(action, data, method, page_url, ctx)
            if not res:
                continue
            final_url, ctype, text = res
            if is_media_ctype(ctype, final_url):
                return final_url
            if text:
                found = await self._accept(find_candidates(text, final_url), final_url, http, ctx)
                if found:
                    return found
                if final_url not in visited:
                    queue.append((final_url, page_url))   # maybe a second page/timer
        return None

    async def resolve(self, url: str, http: Http) -> str:
        ctx = FetchContext()
        origin = self.origin(url)
        queue = [(c, f"{origin}/") for c in self.candidates(url)]
        visited: set[str] = set()
        fetched = 0

        while queue and fetched < self.max_pages:
            page_url, referer = queue.pop(0)
            if page_url in visited:
                continue
            visited.add(page_url)

            page_html = await http.get_text(page_url, referer, ctx)
            fetched += 1
            if not page_html:
                continue

            cands = find_candidates(page_html, page_url)
            found = await self._accept(cands, page_url, http, ctx)
            if found:
                log.info("%s resolved via %s", self.name, page_url)
                return found
            if not cands:
                ctx.note(f"No m3u8/mp4 found in {page_url} (HTTP 200)")
                if len(page_html) < 6000:
                    ctx.note(f"page is only {len(page_html)} bytes - probably an anti-bot shell, not the player")
            if ctx.dump is None:
                ctx.dump = script_dump(page_url, page_html)

            if self.follow_anchors:
                found = await self._follow_anchors(page_html, page_url, http, ctx, queue, visited)
                if found:
                    return found
            if self.handle_forms:
                found = await self._submit_forms(page_html, page_url, http, ctx, queue, visited)
                if found:
                    return found

            for nxt in self.discover(page_html, page_url):
                if nxt not in visited:
                    queue.append((nxt, page_url))

        raise ResolveError(ctx.reason("No page could be fetched"), dump=ctx.dump)


# ----------------------------------------------------------------------------
# Layer 0: direct media links
# ----------------------------------------------------------------------------
@register
class DirectMediaResolver(BaseResolver):
    name = "direct-media"

    def matches(self, url: str) -> bool:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and parsed.path.lower().endswith((".m3u8", ".mp4", ".mkv"))

    async def resolve(self, url: str, http: Http) -> str:
        return url


# ----------------------------------------------------------------------------
# Layer 2: custom scrapers
# ----------------------------------------------------------------------------
# Token = everything up to whitespace / quote / angle bracket, so $ _ - ~ % and
# any other special character is preserved exactly as sent.
STREAAM_RE = re.compile(
    r"""https?://(?:www\.)?streaam\.net/(?:T|e)/([^\s"'<>]+)""", re.IGNORECASE
)


def clean_token(token: str) -> str:
    """Drop only sentence punctuation that sticks to the end of a pasted link."""
    return token.rstrip(".,;:!?)]}")


@register
class StreaamResolver(PageResolver):
    name = "streaam.net"
    host_re = re.compile(r"^(?:www\.)?streaam\.net$", re.IGNORECASE)
    base = "https://streaam.net"
    prefer_custom = True

    def matches(self, url: str) -> bool:
        return super().matches(url) and bool(STREAAM_RE.search(url))

    def candidates(self, url: str) -> list[str]:
        vid = clean_token(STREAAM_RE.search(url).group(1))
        # /T/ answers HTTP 200 (and /e/ may 404), so /T/ goes first.
        return [f"{self.base}/T/{vid}", f"{self.base}/e/{vid}"]

    def discover(self, page_html: str, page_url: str) -> list[str]:
        found = super().discover(page_html, page_url)
        for emb in STREAAM_RE.finditer(page_html):
            found.append(f"{self.base}/T/{clean_token(emb.group(1))}")
        return found


@register
class FilesdlResolver(PageResolver):
    name = "filesdl.top"
    host_re = re.compile(r"^(?:[a-zA-Z0-9-]+\.)*filesdl\.top(?::\d+)?$", re.IGNORECASE)
    follow_anchors = True
    handle_forms = True
    max_pages = 8
    prefer_custom = True


@register
class TeraBoxResolver(BaseResolver):
    """Resolve Terabox links only through a configured, authorized converter API.

    The converter endpoint must return a JSON object containing a direct media URL.
    This resolver does not fabricate media URLs or attempt to bypass access controls.
    """
    name = "terabox"
    prefer_custom = True

    def matches(self, url: str) -> bool:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        return parsed.scheme in ("http", "https") and any(
            host == domain or host.endswith("." + domain) for domain in TERABOX_DOMAINS
        )

    @staticmethod
    def _find_media_url(payload) -> str | None:
        """Find a direct .vdo/.mp4 URL in common converter JSON response shapes."""
        keys = ("dlink", "direct_url", "download_url", "downloadUrl", "directLink", "url")
        if isinstance(payload, dict):
            for key in keys:
                value = payload.get(key)
                if isinstance(value, str) and value.startswith(("https://", "http://")):
                    path = urlparse(value).path.lower()
                    if path.endswith((".vdo", ".mp4")):
                        return value
            for value in payload.values():
                found = TeraBoxResolver._find_media_url(value)
                if found:
                    return found
        elif isinstance(payload, list):
            for value in payload:
                found = TeraBoxResolver._find_media_url(value)
                if found:
                    return found
        return None

    async def resolve(self, url: str, http: Http) -> str:
        if not TERABOX_CONVERTER_URL:
            raise ResolveError("TERABOX_CONVERTER_URL is not configured")
        if not CLOUDFLARE_WORKER_URL:
            raise ResolveError("CLOUDFLARE_WORKER_URL is not configured")
        try:
            response = await http.session.get(
                TERABOX_CONVERTER_URL,
                params={"url": url},
                headers={"Accept": "application/json", "User-Agent": DESKTOP_UA},
                timeout=25,
                allow_redirects=True,
            )
            try:
                if response.status_code != 200:
                    raise ResolveError(f"Terabox converter returned HTTP {response.status_code}")
                try:
                    payload = response.json()
                except Exception as exc:
                    raise ResolveError("Terabox converter did not return valid JSON") from exc
            finally:
                await response.aclose()
        except ResolveError:
            raise
        except Exception as exc:
            raise ResolveError(f"Terabox converter request failed: {type(exc).__name__}") from exc

        direct_dlink = self._find_media_url(payload)
        if not direct_dlink:
            raise ResolveError("Converter response did not contain a .vdo or .mp4 direct URL")
        parsed_media = urlparse(direct_dlink)
        if parsed_media.scheme != "https":
            raise ResolveError("Direct media URL must use HTTPS")

        # Wrap the authorized direct URL through the configured Cloudflare Worker.
        sep = "&" if "?" in CLOUDFLARE_WORKER_URL else "?"
        return (
            f"{CLOUDFLARE_WORKER_URL}{sep}url="
            f"{quote(direct_dlink, safe='')}"
        )


@register
class GenericPageResolver(PageResolver):
    """Catch-all: any other http(s) page (HTML / packed JS / iframes)."""

    name = "generic-html"
    public = False

    def matches(self, url: str) -> bool:
        return urlparse(url).scheme in ("http", "https")


# ----------------------------------------------------------------------------
# Layer 1: yt-dlp (runs in a worker thread, never blocks the event loop)
# ----------------------------------------------------------------------------
YTDLP_TIMEOUT = 25
YTDLP_SEM = asyncio.Semaphore(2)          # keeps RAM low on Render's 512 MB
_ALLOWED_PROTOCOLS = {"http", "https", "m3u8", "m3u8_native"}
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _pick_media(info):
    if not info:
        return None
    if info.get("entries"):
        for entry in info["entries"]:
            if entry:
                info = entry
                break
    url = info.get("url")
    if url and str(url).startswith("http") and (not info.get("protocol") or info["protocol"] in _ALLOWED_PROTOCOLS):
        return url
    formats = [
        f for f in (info.get("formats") or [])
        if str(f.get("url", "")).startswith("http")
        and f.get("protocol") in _ALLOWED_PROTOCOLS
        and f.get("vcodec") != "none"
    ]
    if not formats:
        return None
    best = formats[-1]
    manifest = best.get("manifest_url")
    if manifest and str(manifest).startswith("http"):
        return manifest          # HLS master playlist = real quality switching in the player
    return best["url"]


def _ytdlp_extract(url: str):
    import yt_dlp  # imported lazily: keeps startup RAM low

    opts = {
        "format": "best",
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
        "noplaylist": True,
        "skip_download": True,
        "socket_timeout": 15,
        "retries": 1,
        "extractor_retries": 1,
        "cachedir": False,
        "no_color": True,
        "http_headers": {"User-Agent": DESKTOP_UA},
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return _pick_media(ydl.extract_info(url, download=False))


async def ytdlp_resolve(url: str) -> str:
    if DISABLE_YTDLP:
        raise ResolveError("yt-dlp disabled")
    try:
        async with YTDLP_SEM:
            media = await asyncio.wait_for(asyncio.to_thread(_ytdlp_extract, url), timeout=YTDLP_TIMEOUT)
    except asyncio.TimeoutError:
        raise ResolveError(f"timed out after {YTDLP_TIMEOUT}s")
    except ImportError:
        raise ResolveError("yt-dlp is not installed")
    except Exception as exc:  # noqa: BLE001
        msg = _ANSI_RE.sub("", str(exc)).strip().splitlines()[0:1]
        msg = (msg[0] if msg else type(exc).__name__)[:220]
        raise ResolveError("unsupported URL" if "Unsupported URL" in msg else msg)

    if not media:
        raise ResolveError("no playable format returned")
    pr = await HTTP.probe(media, origin_of(url) + "/")
    if pr.kind in ("dead", "html"):
        raise ResolveError(f"returned URL is not media ({pr.kind})")
    return media


# ----------------------------------------------------------------------------
# Resolve pipeline: cache + in-flight de-duplication
# ----------------------------------------------------------------------------
@dataclass
class Result:
    kind: str                   # "stream" or "failed"
    stream_url: str = ""
    resolver: str = "none"
    reason: str = ""
    dump: str | None = None
    exc: BaseException | None = None


FETCH_SEM = asyncio.Semaphore(25)       # max simultaneous scraper runs
SCRAPER_TIMEOUT = 70
CACHE_TTL = {"stream": 300, "failed": 120}
_cache: dict[str, tuple[float, Result]] = {}
_inflight: dict[str, "asyncio.Future[Result]"] = {}


async def _do_resolve(url: str) -> Result:
    resolver = find_resolver(url)
    if resolver is not None and resolver.name == "direct-media":
        return Result("stream", stream_url=url, resolver="direct-media")

    notes: list[str] = []
    state: dict = {"dump": None, "exc": None}

    async def run_ytdlp():
        try:
            return Result("stream", stream_url=await ytdlp_resolve(url), resolver="yt-dlp")
        except ResolveError as err:
            notes.append(f"yt-dlp: {err.reason}")
        except Exception as err:  # noqa: BLE001
            log.exception("yt-dlp layer crashed on %s", url)
            notes.append(f"yt-dlp crashed: {err!r}")
            state["exc"] = err
        return None

    async def run_custom():
        if resolver is None:
            notes.append("no custom resolver for this URL")
            return None
        try:
            async with FETCH_SEM:
                stream = await asyncio.wait_for(resolver.resolve(url, HTTP), timeout=SCRAPER_TIMEOUT)
            return Result("stream", stream_url=stream, resolver=resolver.name)
        except ResolveError as err:
            notes.append(f"{resolver.name}: {err.reason}")
            state["dump"] = err.dump
        except asyncio.TimeoutError:
            notes.append(f"{resolver.name}: timed out after {SCRAPER_TIMEOUT}s")
        except Exception as err:  # noqa: BLE001
            log.exception("resolver %s crashed on %s", resolver.name, url)
            notes.append(f"{resolver.name} crashed: {err!r}")
            state["exc"] = err
        return None

    steps = [run_custom, run_ytdlp] if (resolver is not None and resolver.prefer_custom) else [run_ytdlp, run_custom]
    for step in steps:
        result = await step()
        if result is not None:
            return result

    return Result(
        "failed",
        resolver=resolver.name if resolver else "none",
        reason=" | ".join(notes),
        dump=state["dump"],
        exc=state["exc"],
    )


async def resolve_link(url: str) -> Result:
    """Never raises. Identical links requested by many users share one run."""
    now = time.time()
    hit = _cache.get(url)
    if hit and now - hit[0] < CACHE_TTL[hit[1].kind]:
        return hit[1]

    fut = _inflight.get(url)
    if fut is None:
        fut = asyncio.ensure_future(_do_resolve(url))
        _inflight[url] = fut
    try:
        result = await asyncio.shield(fut)
    finally:
        if fut.done():
            _inflight.pop(url, None)

    _cache[url] = (time.time(), result)
    if len(_cache) > 500:
        cutoff = time.time() - 300
        for key in [k for k, v in _cache.items() if v[0] < cutoff]:
            _cache.pop(key, None)
    return result


_recent_logs: dict[tuple[str, str], float] = {}


async def log_failure(message, url: str, result: Result) -> None:
    """Silent report when both layers failed (one per URL+reason per 10 minutes)."""
    key = (url, result.reason)
    now = time.time()
    if now - _recent_logs.get(key, 0) < 600:
        return
    _recent_logs[key] = now
    if len(_recent_logs) > 1000:
        for k in [k for k, t in _recent_logs.items() if now - t > 600]:
            _recent_logs.pop(k, None)

    await alog(
        "🔴 <b>Both layers failed</b>\n\n"
        f"<b>URL:</b> <code>{esc(url[:1000])}</code>\n"
        f"<b>User:</b> {describe_user(message)}\n"
        f"<b>Resolver:</b> {esc(result.resolver)}\n"
        f"<b>User got:</b> {FALLBACK_MODE} fallback\n"
        f"<b>Reason:</b> {esc(result.reason[:1500])}"
    )
    if result.dump:
        await alog(
            "🧩 <b>Player script dump</b> (full text attached as file)\n\n"
            f"<pre>{fit_pre(result.dump, 3000)}</pre>"
        )
        host = re.sub(r"[^A-Za-z0-9.-]", "_", urlparse(url).netloc or "page")
        await adoc(f"dump_{host}.txt", result.dump.encode("utf-8", "ignore"), caption=url[:300])
    if result.exc is not None:
        await alog(crash_text(f"resolver {result.resolver} for {url[:200]}", result.exc))


# ----------------------------------------------------------------------------
# Player URLs
# ----------------------------------------------------------------------------
def player_link(param: str, value: str) -> str:
    sep = "&" if "?" in GITHUB_PLAYER_URL else "?"
    return f"{GITHUB_PLAYER_URL}{sep}{param}={quote(value, safe='')}"


# ----------------------------------------------------------------------------
# Telegram bot
# ----------------------------------------------------------------------------
bot = Client(
    "video_stream_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,   # Render's disk is ephemeral
    workers=32,
)

URL_RE = re.compile(r"""https?://[^\s"'<>]+""", re.IGNORECASE)
TRAILING = ".,;:!?)]}"          # '$' ~ % - _ are deliberately never stripped
MAX_LINKS_PER_MESSAGE = 3
MAX_ACTIVE_PER_USER = 4

_tasks: set[asyncio.Task] = set()
_active: dict[int, int] = {}


def extract_urls(message) -> list[str]:
    text = message.text or message.caption or ""
    entities = message.entities or message.caption_entities or []
    urls: list[str] = []

    for m in URL_RE.finditer(text):
        url = m.group(0).rstrip(TRAILING)
        if url and url not in urls:
            urls.append(url)
    for ent in entities:
        if ent.type == MessageEntityType.TEXT_LINK and ent.url:
            url = ent.url.rstrip(TRAILING)
            if url and url not in urls:
                urls.append(url)
    return urls


async def safe_edit(message, text, markup=None):
    for _ in range(2):
        try:
            return await message.edit_text(text, reply_markup=markup)
        except FloodWait as exc:
            await asyncio.sleep(exc.value)
        except Exception as exc:  # noqa: BLE001
            log.warning("edit failed: %s", exc)
            return None
    return None


async def safe_reply(message, text, markup=None):
    for _ in range(2):
        try:
            return await message.reply_text(text, reply_markup=markup)
        except FloodWait as exc:
            await asyncio.sleep(exc.value)
        except Exception as exc:  # noqa: BLE001
            log.warning("reply failed: %s", exc)
            return None
    return None


async def deliver(message, status, player_url: str, is_private: bool,
                  text: str = "Stream ready. Tap the button to watch.") -> None:
    """Send / edit the 'Play Video' button (web_app in private chats, URL button elsewhere)."""
    if is_private:
        button = InlineKeyboardButton("Play Video", web_app=WebAppInfo(url=player_url))
    else:
        button = InlineKeyboardButton("Play Video", url=player_url)
    markup = InlineKeyboardMarkup([[button]])

    sent = await (safe_edit(status, text, markup) if status else safe_reply(message, text, markup))
    if sent is None and is_private:
        plain = InlineKeyboardMarkup([[InlineKeyboardButton("Play Video", url=player_url)]])
        await (safe_edit(status, text, plain) if status else safe_reply(message, text, plain))


async def handle_message(message) -> None:
    urls = extract_urls(message)
    if not urls:
        return
    is_private = message.chat.type == ChatType.PRIVATE

    for url in urls[:MAX_LINKS_PER_MESSAGE]:
        resolver = find_resolver(url)
        # In groups only react to hosts we know; in private chats every link is tried.
        if resolver is None or (not is_private and not resolver.public):
            continue

        if resolver.name == "direct-media":
            await deliver(message, None, player_link("stream", url), is_private)   # Layer 0: instant
            continue

        status = await safe_reply(message, "Processing link...")
        result = await resolve_link(url)

        if result.kind == "stream":
            await deliver(message, status, player_link("stream", result.stream_url), is_private)
            continue

        await log_failure(message, url, result)      # silent report -> dedicated scraper later
        if FALLBACK_MODE == "message":
            if status:
                await safe_edit(status, MSG_FAILED)
            else:
                await safe_reply(message, MSG_FAILED)
        elif FALLBACK_MODE == "iframe":
            await deliver(message, status, player_link("fallback", url), is_private)
        else:
            # "direct": the user's own phone opens the site, so Cloudflare verifies the real client.
            await deliver(
                message, status, url, is_private and url.lower().startswith("https://"),
                text="Tap Play Video to open the video page.",
            )

async def guarded_handle(message) -> None:
    """Isolated task per message; crashes go to the log channel, never to other users."""
    uid = message.from_user.id if message.from_user else message.chat.id
    if _active.get(uid, 0) >= MAX_ACTIVE_PER_USER:
        await safe_reply(message, "Please wait - your previous links are still being processed.")
        return
    _active[uid] = _active.get(uid, 0) + 1
    try:
        await handle_message(message)
    except Exception as exc:  # noqa: BLE001
        log.exception("handler crashed")
        await alog(crash_text("handle_message", exc))
        await safe_reply(message, "Something went wrong. Please send the link again.")
    finally:
        _active[uid] -= 1
        if _active[uid] <= 0:
            _active.pop(uid, None)


def spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


@bot.on_message(filters.command(["start", "help"]) & filters.incoming)
async def start_cmd(_, message):
    spawn(safe_reply(
        message,
        "Send me a video link (Terabox, streaam.net, filesdl.top, a direct .m3u8 / .mp4 / .mkv link, "
        "or any site yt-dlp supports) and I'll try to give you a player button.",
    ))


@bot.on_message(
    filters.incoming & (filters.text | filters.caption) & ~filters.command(["start", "help"])
)
async def on_message(_, message):
    spawn(guarded_handle(message))      # returns immediately: no handler blocks another user


# ----------------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------------
async def main() -> None:
    await HTTP.start()
    await bot.start()
    names = ", ".join(r.name for r in RESOLVERS)
    log.info("Bot started. Resolvers: %s", names)
    await alog(
        "♻️ <b>Bot restarted / online</b>\n"
        f"<b>Layers:</b> direct → yt-dlp{' (disabled)' if DISABLE_YTDLP else ''} → {esc(names)}\n"
        f"<b>HTTP:</b> curl_cffi impersonate={esc(HTTP.impersonate)}{' + proxy' if PROXY_URL else ''}\n"
        f"<b>Fallback:</b> {FALLBACK_MODE}\n"
        f"<b>Terabox converter:</b> {'configured' if TERABOX_CONVERTER_URL else 'not configured'}; "
        f"<b>Worker:</b> {'configured' if CLOUDFLARE_WORKER_URL else 'not configured'}\n"
        f"<b>Python:</b> {sys.version.split()[0]}"
    )
    try:
        await idle()
    finally:
        await bot.stop()
        await HTTP.close()


if __name__ == "__main__":
    threading.Thread(target=run_web, name="flask", daemon=True).start()
    threading.Thread(target=keep_alive_ping, name="keepalive", daemon=True).start()
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except Exception as exc:  # noqa: BLE001
        log.exception("Bot crashed")
        send_log_sync(crash_text("main()", exc))
        raise
