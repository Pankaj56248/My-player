"""
Telegram Video Streaming Bot  (Pyrogram/pyrofork + Flask for Render free tier)
==============================================================================

User sends a video link  ->  bot resolves it to a playable stream  ->  bot
replies with an inline Mini App button:  GITHUB_PLAYER_URL?stream=<encoded url>

Environment variables
---------------------
  API_ID, API_HASH, BOT_TOKEN, GITHUB_PLAYER_URL, PORT   (required, PORT optional)
  LOG_CHANNEL_ID        optional - channel/chat id (e.g. -1001234567890) where
                        failures and crash tracebacks are reported. The bot
                        must be an admin of that channel.
  RENDER_EXTERNAL_URL   optional - set by Render; used for a keep-alive ping.

Adding a new host (Terabox, ...)
--------------------------------
Write a class that extends BaseResolver and decorate it with @register:

    @register
    class TeraboxResolver(BaseResolver):
        name = "terabox"
        def matches(self, url): return "terabox" in urlparse(url).netloc
        def resolve(self, url): ...   # return a playable http(s) URL
                                      # raise ResolveError("reason") on failure
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
from urllib.parse import quote, urljoin, urlparse

import requests
from flask import Flask
from pyrogram import Client, filters
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
PORT = int(os.environ.get("PORT", "10000"))
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

MSG_FAILED = (
    "Yeh video stream abhi process nahi ho saki. "
    "Yeh link hamare developer ko report kar di gayi hai."
)
MSG_UNSUPPORTED = (
    "Yeh host abhi supported nahi hai. "
    "Yeh link hamare developer ko report kar di gayi hai."
)


# ----------------------------------------------------------------------------
# Log channel (plain Bot API over HTTP, so it works from any thread and even
# when the Pyrogram client itself is down)
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


def describe_user(message) -> str:
    user = message.from_user
    if not user:
        chat = message.chat
        return f"chat <code>{chat.id}</code>" if chat else "unknown"
    uname = f"@{html.escape(user.username)}" if user.username else "no username"
    return f"<code>{user.id}</code> ({uname})"


def report_failure(message, url: str, resolver: str, reason: str) -> None:
    send_log_sync(
        "⚠️ <b>Extraction failed</b>\n\n"
        f"<b>URL:</b> <code>{html.escape(url)}</code>\n"
        f"<b>User:</b> {describe_user(message)}\n"
        f"<b>Resolver:</b> {html.escape(resolver)}\n"
        f"<b>Reason:</b> {html.escape(reason)[:1500]}"
    )


def report_crash(context: str, exc: BaseException) -> None:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    tb = tb[-3000:]
    send_log_sync(
        f"🔥 <b>Unhandled exception</b>\n<b>Where:</b> {html.escape(context)}\n\n"
        f"<pre>{html.escape(tb)}</pre>"
    )


def _sys_hook(exc_type, exc, tb):
    log.error("Uncaught exception", exc_info=(exc_type, exc, tb))
    if exc is not None:
        report_crash("sys.excepthook", exc)


def _thread_hook(args):
    log.error("Uncaught thread exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
    if args.exc_value is not None:
        report_crash(f"thread {getattr(args.thread, 'name', '?')}", args.exc_value)


sys.excepthook = _sys_hook
threading.excepthook = _thread_hook


# ----------------------------------------------------------------------------
# Flask dummy server (Render port binding + uptime monitor target)
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
# Resolver framework
# ----------------------------------------------------------------------------
class ResolveError(Exception):
    """Raised by a resolver when a link cannot be turned into a stream."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class BaseResolver:
    name = "base"

    def matches(self, url: str) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    def resolve(self, url: str) -> str:  # pragma: no cover - interface
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


# ----------------------------------------------------------------------------
# HTTP helpers (realistic desktop browser, one Session per resolve call)
# ----------------------------------------------------------------------------
DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def make_session(referer: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DESKTOP_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Referer": referer,
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Connection": "keep-alive",
        }
    )
    return session


class FetchContext:
    """Collects HTTP outcomes so a failure can be explained in the log channel."""

    def __init__(self):
        self.notes: list[str] = []

    def note(self, text: str) -> None:
        self.notes.append(text)

    def reason(self, default: str) -> str:
        return "; ".join(self.notes[-5:]) if self.notes else default


def http_get(session: requests.Session, url: str, referer: str, ctx: FetchContext) -> str:
    for attempt in range(2):
        try:
            resp = session.get(url, headers={"Referer": referer}, timeout=20, allow_redirects=True)
        except requests.RequestException as exc:
            ctx.note(f"{type(exc).__name__} on {url}")
            time.sleep(1)
            continue

        if resp.status_code == 200:
            return resp.text

        body = (resp.text or "")[:3000].lower()
        cf = resp.status_code in (403, 429, 503) and (
            "just a moment" in body or "cf-chl" in body or "cloudflare" in body
        )
        ctx.note(f"HTTP {resp.status_code}{' (Cloudflare challenge)' if cf else ''} on {url}")
        if resp.status_code in (404, 410):
            break  # retrying a missing page is pointless
        time.sleep(1)
    return ""


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
# Resolver: direct media links (.m3u8 / .mp4 / .mkv)  - no scraping at all
# ----------------------------------------------------------------------------
DIRECT_EXT_RE = re.compile(r"\.(?:m3u8|mp4|mkv)$", re.IGNORECASE)


@register
class DirectMediaResolver(BaseResolver):
    name = "direct-media"

    def matches(self, url: str) -> bool:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(DIRECT_EXT_RE.search(parsed.path))

    def resolve(self, url: str) -> str:
        return url


# ----------------------------------------------------------------------------
# Resolver: streaam.net  (/T/<id> or /e/<id>, ids may contain $ _ - ~ % ...)
# ----------------------------------------------------------------------------
# Token = everything up to whitespace / quote / angle bracket, so $ _ - ~ % and any
# other special character is preserved exactly as sent.
STREAAM_RE = re.compile(
    r"""https?://(?:www\.)?streaam\.net/(?:T|e)/([^\s"'<>]+)""", re.IGNORECASE
)


def clean_token(token: str) -> str:
    """Drop only sentence punctuation that sticks to the end of a pasted link."""
    return token.rstrip(".,;:!?)]}")
STREAAM_HOST_RE = re.compile(r"^(?:www\.)?streaam\.net$", re.IGNORECASE)


@register
class StreaamResolver(BaseResolver):
    name = "streaam.net"
    BASE = "https://streaam.net"

    def matches(self, url: str) -> bool:
        return bool(STREAAM_HOST_RE.match(urlparse(url).netloc)) and bool(STREAAM_RE.search(url))

    def resolve(self, url: str) -> str:
        match = STREAAM_RE.search(url)
        if not match:
            raise ResolveError("Could not parse the streaam.net link")
        vid = clean_token(match.group(1))  # full token, all special characters preserved

        # Embed page first (/T/ is safely mapped to /e/), then the original page.
        pages = [(f"{self.BASE}/e/{vid}", f"{self.BASE}/"), (f"{self.BASE}/T/{vid}", f"{self.BASE}/")]

        session = make_session(f"{self.BASE}/")
        ctx = FetchContext()
        visited: set[str] = set()
        fetched = 0

        while pages and fetched < 6:
            page_url, referer = pages.pop(0)
            if page_url in visited:
                continue
            visited.add(page_url)

            page_html = http_get(session, page_url, referer, ctx)
            fetched += 1
            if not page_html:
                continue

            found = find_stream(page_html, page_url)
            if found:
                log.info("streaam.net resolved via %s", page_url)
                return found
            ctx.note(f"No m3u8/mp4 found in {page_url} (HTTP 200)")

            for src in IFRAME_RE.findall(page_html):
                nxt = urljoin(page_url, src)
                if nxt.startswith("http") and nxt not in visited:
                    pages.append((nxt, page_url))
            for emb in STREAAM_RE.finditer(page_html):
                nxt = f"{self.BASE}/e/{clean_token(emb.group(1))}"
                if nxt not in visited:
                    pages.append((nxt, page_url))

        raise ResolveError(ctx.reason("No page could be fetched"))


# ----------------------------------------------------------------------------
# Player URL
# ----------------------------------------------------------------------------
def build_player_url(stream_url: str) -> str:
    sep = "&" if "?" in GITHUB_PLAYER_URL else "?"
    return f"{GITHUB_PLAYER_URL}{sep}stream={quote(stream_url, safe='')}"


# ----------------------------------------------------------------------------
# Telegram bot
# ----------------------------------------------------------------------------
bot = Client(
    "video_stream_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,  # Render's disk is ephemeral
)

URL_RE = re.compile(r"""https?://[^\s<>"']+""", re.IGNORECASE)
TRAILING = ".,;:!?)]}>\"'"   # '$' is deliberately NOT stripped
MAX_LINKS_PER_MESSAGE = 3
WORKERS = asyncio.Semaphore(3)


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
            if url not in urls:
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


def guard(handler):
    """Any unhandled exception -> traceback to the log channel + friendly reply."""

    @functools.wraps(handler)
    async def wrapper(client, message):
        try:
            await handler(client, message)
        except Exception as exc:  # noqa: BLE001
            log.exception("handler %s crashed", handler.__name__)
            await asyncio.to_thread(report_crash, f"handler {handler.__name__}", exc)
            try:
                await message.reply_text(MSG_FAILED)
            except Exception:  # noqa: BLE001
                pass

    return wrapper


@bot.on_message(filters.command(["start", "help"]) & filters.incoming)
@guard
async def start_cmd(_, message):
    await message.reply_text(
        "Send me a video link (streaam.net or a direct .m3u8 / .mp4 / .mkv link) "
        "and I'll give you a button that plays it inside Telegram."
    )


@bot.on_message(
    filters.incoming & (filters.text | filters.caption) & ~filters.command(["start", "help"])
)
@guard
async def on_message(_, message):
    urls = extract_urls(message)
    if not urls:
        return

    is_private = message.chat.type == ChatType.PRIVATE

    for url in urls[:MAX_LINKS_PER_MESSAGE]:
        resolver = find_resolver(url)

        if resolver is None:
            # Unknown host: only answer/report in private chats (groups are full of random links).
            if is_private:
                host = urlparse(url).netloc or "unknown"
                await asyncio.to_thread(
                    report_failure, message, url, "none", f"Unsupported link / no resolver for host '{host}'"
                )
                await message.reply_text(MSG_UNSUPPORTED)
            continue

        status = await message.reply_text("Processing link...")
        try:
            async with WORKERS:
                stream_url = await asyncio.to_thread(resolver.resolve, url)
        except ResolveError as exc:
            log.info("resolve failed (%s): %s", resolver.name, exc.reason)
            await asyncio.to_thread(report_failure, message, url, resolver.name, exc.reason)
            await safe_edit(status, MSG_FAILED)
            continue
        except Exception as exc:  # noqa: BLE001
            log.exception("resolver %s crashed", resolver.name)
            await asyncio.to_thread(report_crash, f"resolver {resolver.name} for {url}", exc)
            await safe_edit(status, MSG_FAILED)
            continue

        player_url = build_player_url(stream_url)

        # web_app buttons only work in private chats; groups get a normal URL button.
        if is_private:
            button = InlineKeyboardButton("Play Video", web_app=WebAppInfo(url=player_url))
        else:
            button = InlineKeyboardButton("Play Video", url=player_url)

        sent = await safe_edit(
            status, "Stream ready. Tap the button to watch.", InlineKeyboardMarkup([[button]])
        )
        if sent is None and is_private:
            fallback = InlineKeyboardMarkup([[InlineKeyboardButton("Play Video", url=player_url)]])
            await safe_edit(status, "Stream ready. Tap the button to watch.", fallback)


# ----------------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    threading.Thread(target=run_web, name="flask", daemon=True).start()
    threading.Thread(target=keep_alive_ping, name="keepalive", daemon=True).start()
    log.info("Registered resolvers: %s", ", ".join(r.name for r in RESOLVERS))
    send_log_sync("✅ <b>Bot started</b>\nResolvers: " + html.escape(", ".join(r.name for r in RESOLVERS)))
    try:
        bot.run()
    except Exception as exc:  # noqa: BLE001
        log.exception("Bot crashed")
        report_crash("bot.run()", exc)
        raise
