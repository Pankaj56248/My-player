"""
Telegram Video Streaming Bot  (pyrofork + aiohttp + Flask keep-alive for Render)
================================================================================

Every link a user sends ends with a "Play Video" button - never an error:

  Layer 1  direct .m3u8 / .mp4 / .mkv link         -> ?stream=<media url>
  Layer 2  host resolvers (streaam.net, filesdl.top)
           HTML / packed JS / iframes -> media url -> ?stream=<media url>
  Layer 3  anything else (unsupported host, scraper failed, crash)
           -> ?fallback=<original link>   (player shows it in a sandboxed iframe)
           and the reason is logged silently to LOG_CHANNEL_ID so a dedicated
           scraper can be written later.

Environment variables
---------------------
  API_ID, API_HASH, BOT_TOKEN, GITHUB_PLAYER_URL      required
  PORT                  optional (default 8080) - Flask health server
  LOG_CHANNEL_ID        optional - e.g. -1001234567890 (bot must be admin)
  RENDER_EXTERNAL_URL   optional - set by Render, used for a keep-alive ping

Adding a host: subclass PageResolver (HTML hosts) or BaseResolver and put
@register above the class. See StreaamResolver / FilesdlResolver below.
"""

import asyncio

# An event loop must exist before pyrogram is imported (Python 3.12+).
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import base64
import html
import logging
import os
import re
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from urllib.parse import quote, urljoin, urlparse

import aiohttp
import requests
from flask import Flask
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatType, MessageEntityType
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from werkzeug.serving import make_server
from yarl import URL

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

LOG_CHANNEL_ID = None
_raw_log_id = os.environ.get("LOG_CHANNEL_ID", "").strip()
if _raw_log_id:
    try:
        LOG_CHANNEL_ID = int(_raw_log_id)
    except ValueError:
        log.warning("LOG_CHANNEL_ID is not a valid integer - channel logging disabled.")

if not GITHUB_PLAYER_URL.lower().startswith("https://"):
    log.warning("GITHUB_PLAYER_URL should start with https:// (Mini Apps require HTTPS).")

# ----------------------------------------------------------------------------
# Log channel (plain Bot API over HTTP: works from any thread, even if the
# Pyrogram client is down). Async callers use `await alog(...)`.
# ----------------------------------------------------------------------------
def send_log_sync(text: str) -> None:
    if not LOG_CHANNEL_ID:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": LOG_CHANNEL_ID,
                "text": text[:4000],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning("Log channel rejected message: %s %s", resp.status_code, resp.text[:200])
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not post to log channel: %s", exc)


async def alog(text: str) -> None:
    await asyncio.to_thread(send_log_sync, text)


def describe_user(message) -> str:
    user = message.from_user
    if not user:
        chat = message.chat
        return f"chat <code>{chat.id}</code>" if chat else "unknown"
    uname = f"@{html.escape(user.username)}" if user.username else "no username"
    return f"<code>{user.id}</code> ({uname})"


def crash_text(context: str, exc: BaseException) -> str:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-3000:]
    return (
        f"🔥 <b>Unhandled exception</b>\n<b>Where:</b> {html.escape(context)}\n\n"
        f"<pre>{html.escape(tb)}</pre>"
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
# Async HTTP client (one shared aiohttp session for all users)
# ----------------------------------------------------------------------------
DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
BASE_HEADERS = {
    "User-Agent": DESKTOP_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
}
MAX_PAGE_BYTES = 2_000_000


def _yurl(url: str) -> URL:
    """Keep $ ~ % - _ exactly as sent (no re-encoding by yarl)."""
    try:
        return URL(url, encoded=True)
    except Exception:  # noqa: BLE001
        return URL(url)


class FetchContext:
    """Collects HTTP outcomes so a failure can be explained in the log channel."""

    def __init__(self):
        self.notes: list[str] = []

    def note(self, text: str) -> None:
        self.notes.append(text)

    def reason(self, default: str) -> str:
        return "; ".join(self.notes[-5:]) if self.notes else default


class Http:
    def __init__(self):
        self.session = None

    async def start(self):
        self.session = aiohttp.ClientSession(
            headers=BASE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=20, connect=8),
            connector=aiohttp.TCPConnector(limit=100, ttl_dns_cache=300),
        )

    async def close(self):
        if self.session:
            await self.session.close()

    async def get_text(self, url: str, referer: str, ctx: FetchContext, attempts: int = 2) -> str:
        for attempt in range(attempts):
            try:
                async with self.session.get(
                    _yurl(url), headers={"Referer": referer}, allow_redirects=True
                ) as resp:
                    data = await resp.content.read(MAX_PAGE_BYTES)
                    text = data.decode(resp.charset or "utf-8", "ignore")
                    if resp.status == 200:
                        return text
                    low = text[:3000].lower()
                    cf = resp.status in (403, 429, 503) and (
                        "just a moment" in low or "cf-chl" in low or "cloudflare" in low
                    )
                    ctx.note(f"HTTP {resp.status}{' (Cloudflare challenge)' if cf else ''} on {url}")
                    if resp.status in (404, 410):
                        break  # retrying a missing page is pointless
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                ctx.note(f"{type(exc).__name__} on {url}")
            await asyncio.sleep(1)
        return ""

    async def verify(self, stream_url: str, referer: str) -> bool:
        """False only when the stream is definitely dead (404/410, or an m3u8 that
        is not a playlist). Anything uncertain (403, timeouts) returns True so the
        player gets a chance - IP/referer-bound links often differ per client."""
        try:
            async with self.session.get(
                _yurl(stream_url), headers={"Referer": referer, "Range": "bytes=0-2047"}
            ) as resp:
                if resp.status in (404, 410):
                    return False
                if resp.status in (200, 206):
                    chunk = await resp.content.read(2048)
                    if urlparse(stream_url).path.lower().endswith(".m3u8"):
                        return b"#EXTM3U" in chunk
                return True
        except Exception:  # noqa: BLE001
            return True


HTTP = Http()


# ----------------------------------------------------------------------------
# Page parsing: m3u8 / mp4 discovery + eval(packed) de-obfuscation
# ----------------------------------------------------------------------------
M3U8_ABS_RE = re.compile(
    r"""https?://[^\s"'<>\\()\[\]{}|]+?\.m3u8(?:\?[^\s"'<>\\()\[\]{}|]*)?""", re.IGNORECASE
)
M3U8_REL_RE = re.compile(
    r"""["'](/?[^"'\s<>]+?\.m3u8(?:\?[^"'\s<>]*)?)["']""", re.IGNORECASE
)
MP4_ABS_RE = re.compile(
    r"""https?://[^\s"'<>\\()\[\]{}|]+?\.mp4(?:\?[^\s"'<>\\()\[\]{}|]*)?""", re.IGNORECASE
)
IFRAME_RE = re.compile(r"""<iframe[^>]+src=["']([^"']+)["']""", re.IGNORECASE)
ATOB_RE = re.compile(r"""atob\(\s*['"]([A-Za-z0-9+/=_-]{16,})['"]\s*\)""")

# Dean Edwards packer: eval(function(p,a,c,k,e,d){...}('payload',radix,count,'a|b|c'.split('|'),0,{}))
PACKED_RE = re.compile(
    r"""\}\(\s*(['"])(.*?)\1\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(['"])(.*?)\5\s*\.split\(\s*['"]\|['"]\s*\)""",
    re.DOTALL,
)
_ALPHA = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


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


def _normalize(text: str) -> str:
    return (
        text.replace("\\/", "/")
        .replace("\\u0026", "&")
        .replace("\\x26", "&")
        .replace("&amp;", "&")
    )


def find_stream(page_html: str, base_url: str):
    """Return a playable stream URL found in the page (m3u8 preferred), or None."""
    texts = [page_html] + unpack_all(page_html)

    for blob in ATOB_RE.findall(page_html):
        try:
            padded = blob.replace("-", "+").replace("_", "/")
            padded += "=" * (-len(padded) % 4)
            decoded = base64.b64decode(padded).decode("utf-8", "ignore")
            texts.append(decoded)
            texts.extend(unpack_all(decoded))
        except Exception:  # noqa: BLE001
            continue

    m3u8: list[str] = []
    mp4: list[str] = []
    for raw in texts:
        text = _normalize(raw)
        for url in M3U8_ABS_RE.findall(text):
            if url not in m3u8:
                m3u8.append(url)
        for rel in M3U8_REL_RE.findall(text):
            if rel.lower().startswith("http"):
                continue
            url = urljoin(base_url, rel)
            if url not in m3u8:
                m3u8.append(url)
        for url in MP4_ABS_RE.findall(text):
            if url not in mp4:
                mp4.append(url)

    if m3u8:
        for url in m3u8:
            if "master" in url.lower():
                return url
        return m3u8[0]
    return mp4[0] if mp4 else None


# ----------------------------------------------------------------------------
# Resolver framework
# ----------------------------------------------------------------------------
class ResolveError(Exception):
    """Raised by a resolver when a link cannot be turned into a media URL."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class BaseResolver:
    name = "base"

    def matches(self, url: str) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    async def resolve(self, url: str, http: Http) -> str:  # pragma: no cover - interface
        raise NotImplementedError


RESOLVERS: list[BaseResolver] = []


def register(cls):
    """Class decorator: instantiate the resolver and add it to the registry."""
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
    """Generic 'fetch HTML -> find stream, follow iframes' resolver."""

    host_re: re.Pattern = re.compile(r"$^")
    base = ""
    max_pages = 6

    def matches(self, url: str) -> bool:
        return bool(self.host_re.match(urlparse(url).netloc))

    def candidates(self, url: str) -> list[str]:
        return [url]

    def discover(self, page_html: str, page_url: str) -> list[str]:
        found = []
        for src in IFRAME_RE.findall(page_html):
            nxt = urljoin(page_url, src)
            if nxt.startswith("http"):
                found.append(nxt)
        return found

    async def resolve(self, url: str, http: Http) -> str:
        ctx = FetchContext()
        queue = [(c, f"{self.base}/") for c in self.candidates(url)]
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

            found = find_stream(page_html, page_url)
            if found:
                if await http.verify(found, page_url):
                    log.info("%s resolved via %s", self.name, page_url)
                    return found
                ctx.note(f"Extracted stream failed verification: {found}")
            else:
                ctx.note(f"No m3u8/mp4 found in {page_url} (HTTP 200)")

            for nxt in self.discover(page_html, page_url):
                if nxt not in visited:
                    queue.append((nxt, page_url))

        raise ResolveError(ctx.reason("No page could be fetched"))


# ----------------------------------------------------------------------------
# Layer 1: direct media links
# ----------------------------------------------------------------------------
DIRECT_EXT_RE = re.compile(r"\.(?:m3u8|mp4|mkv)$", re.IGNORECASE)


@register
class DirectMediaResolver(BaseResolver):
    name = "direct-media"

    def matches(self, url: str) -> bool:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(DIRECT_EXT_RE.search(parsed.path))

    async def resolve(self, url: str, http: Http) -> str:
        return url


# ----------------------------------------------------------------------------
# Layer 2: host resolvers
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

    def matches(self, url: str) -> bool:
        return super().matches(url) and bool(STREAAM_RE.search(url))

    def candidates(self, url: str) -> list[str]:
        vid = clean_token(STREAAM_RE.search(url).group(1))
        # The server answers HTTP 200 on /T/ and 404 on /e/, so /T/ goes first.
        return [f"{self.base}/T/{vid}", f"{self.base}/e/{vid}"]

    def discover(self, page_html: str, page_url: str) -> list[str]:
        found = super().discover(page_html, page_url)
        for emb in STREAAM_RE.finditer(page_html):
            found.append(f"{self.base}/T/{clean_token(emb.group(1))}")
        return found


@register
class FilesdlResolver(PageResolver):
    name = "filesdl.top"
    host_re = re.compile(r"^(?:www\.)?filesdl\.top$", re.IGNORECASE)
    base = "https://filesdl.top"


# ----------------------------------------------------------------------------
# Resolve pipeline: cache + in-flight de-duplication + automatic fallback
# ----------------------------------------------------------------------------
@dataclass
class Result:
    kind: str                 # "stream" or "fallback"
    stream_url: str = ""
    resolver: str = "none"
    reason: str = ""
    exc: BaseException | None = None


FETCH_SEM = asyncio.Semaphore(25)       # max simultaneous scraper runs
RESOLVE_TIMEOUT = 45
CACHE_TTL = {"stream": 300, "fallback": 120}
_cache: dict[str, tuple[float, Result]] = {}
_inflight: dict[str, "asyncio.Future[Result]"] = {}


async def _do_resolve(url: str) -> Result:
    resolver = find_resolver(url)
    if resolver is None:
        host = urlparse(url).netloc or "unknown"
        return Result("fallback", reason=f"Unsupported host '{host}'")
    try:
        async with FETCH_SEM:
            stream = await asyncio.wait_for(resolver.resolve(url, HTTP), timeout=RESOLVE_TIMEOUT)
        return Result("stream", stream_url=stream, resolver=resolver.name)
    except ResolveError as exc:
        return Result("fallback", resolver=resolver.name, reason=exc.reason)
    except asyncio.TimeoutError:
        return Result("fallback", resolver=resolver.name, reason=f"Resolver timed out after {RESOLVE_TIMEOUT}s")
    except Exception as exc:  # noqa: BLE001
        log.exception("resolver %s crashed on %s", resolver.name, url)
        return Result("fallback", resolver=resolver.name, reason=f"Internal error: {exc!r}", exc=exc)


async def resolve_link(url: str) -> Result:
    """Never raises. Identical links requested by many users share one fetch."""
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


_recent_fallback_logs: dict[tuple[str, str], float] = {}


async def log_fallback(message, url: str, result: Result) -> None:
    """Silent Layer-3 report (one per URL+reason per 10 minutes)."""
    key = (url, result.reason)
    now = time.time()
    if now - _recent_fallback_logs.get(key, 0) < 600:
        return
    _recent_fallback_logs[key] = now
    if len(_recent_fallback_logs) > 1000:
        for k in [k for k, t in _recent_fallback_logs.items() if now - t > 600]:
            _recent_fallback_logs.pop(k, None)

    await alog(
        "🟡 <b>Fallback mode (Layer 3)</b>\n\n"
        f"<b>URL:</b> <code>{html.escape(url)}</code>\n"
        f"<b>User:</b> {describe_user(message)}\n"
        f"<b>Resolver:</b> {html.escape(result.resolver)}\n"
        f"<b>Reason:</b> {html.escape(result.reason)[:1500]}"
    )
    if result.exc is not None:
        await alog(crash_text(f"resolver {result.resolver} for {url}", result.exc))


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


async def deliver(message, status, player_url: str, is_private: bool) -> None:
    """Send / edit the 'Play Video' button (web_app in private chats)."""
    text = "Stream ready. Tap the button to watch."
    if is_private:
        button = InlineKeyboardButton("Play Video", web_app=WebAppInfo(url=player_url))
    else:
        button = InlineKeyboardButton("Play Video", url=player_url)
    markup = InlineKeyboardMarkup([[button]])

    sent = await (safe_edit(status, text, markup) if status else safe_reply(message, text, markup))
    if sent is None and is_private:
        # Telegram rejected the web_app button - fall back to a plain URL button.
        plain = InlineKeyboardMarkup([[InlineKeyboardButton("Play Video", url=player_url)]])
        await (safe_edit(status, text, plain) if status else safe_reply(message, text, plain))


async def handle_message(message) -> None:
    urls = extract_urls(message)
    if not urls:
        return
    is_private = message.chat.type == ChatType.PRIVATE

    for url in urls[:MAX_LINKS_PER_MESSAGE]:
        resolver = find_resolver(url)
        # In groups only react to hosts we know; in private chats every link gets a button.
        if resolver is None and not is_private:
            continue

        if resolver is not None and resolver.name == "direct-media":
            await deliver(message, None, player_link("stream", url), is_private)   # Layer 1: instant
            continue

        status = await safe_reply(message, "Processing link...")
        result = await resolve_link(url)

        if result.kind == "stream":
            player_url = player_link("stream", result.stream_url)          # Layer 2
        else:
            player_url = player_link("fallback", url)                       # Layer 3
            await log_fallback(message, url, result)

        await deliver(message, status, player_url, is_private)


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
        "Send me any video link (streaam.net, filesdl.top, or a direct .m3u8 / .mp4 / .mkv link) "
        "and I'll give you a button that plays it inside Telegram.",
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
        f"<b>Resolvers:</b> {html.escape(names)}\n"
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
