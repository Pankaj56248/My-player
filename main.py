"""
Streaam link -> Telegram Mini App player bot
--------------------------------------------
Pyrogram (pyrofork) bot + Flask dummy web server (daemon thread) for the
Render.com free Web Service tier.

Flow:
  1. User sends a streaam.net link (/T/<id> or /e/<id>).
  2. Bot fetches the page in the background (spoofed User-Agent / Referer),
     looks for a direct .m3u8 URL in the HTML or in eval-packed JavaScript.
  3. Bot replies with an inline button that opens
     GITHUB_PLAYER_URL?stream=<url-encoded m3u8> as a Telegram Mini App.

Environment variables:
  API_ID, API_HASH, BOT_TOKEN, GITHUB_PLAYER_URL, PORT
  (optional) RENDER_EXTERNAL_URL - set automatically by Render, used for the
                                   keep-alive self ping.
"""

import asyncio

# Make sure an event loop exists before pyrogram is imported (Python 3.12+).
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import base64
import logging
import os
import re
import sys
import threading
import time
from collections import deque
from urllib.parse import quote, urljoin, urlparse

import requests
from flask import Flask
from pyrogram import Client, filters
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("streaam-bot")


# ----------------------------------------------------------------------------
# Environment
# ----------------------------------------------------------------------------
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
GITHUB_PLAYER_URL = require_env("GITHUB_PLAYER_URL").strip()
PORT = int(os.environ.get("PORT", "10000"))
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip()

if not GITHUB_PLAYER_URL.lower().startswith("https://"):
    log.warning("GITHUB_PLAYER_URL should start with https:// (Telegram Mini Apps require HTTPS).")


# ----------------------------------------------------------------------------
# Flask dummy server (Render port binding + uptime monitoring)
# ----------------------------------------------------------------------------
web = Flask(__name__)


@web.route("/")
def home():
    return "Bot is running (200 OK)", 200


@web.route("/health")
def health():
    return {"status": "ok"}, 200


def run_web() -> None:
    log.info("Flask server listening on 0.0.0.0:%s", PORT)
    web.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)


def keep_alive_ping() -> None:
    """Pings our own public URL so Render's free tier does not spin down."""
    if not RENDER_EXTERNAL_URL:
        return
    while True:
        time.sleep(600)
        try:
            requests.get(RENDER_EXTERNAL_URL, timeout=15)
        except Exception as exc:  # noqa: BLE001
            log.warning("Keep-alive ping failed: %s", exc)


# ----------------------------------------------------------------------------
# Extractor
# ----------------------------------------------------------------------------
LINK_PATTERN = re.compile(
    r"https?://(?:[\w-]+\.)?streaam\.net/(?:T|e)/[\w-]+(?:\?[^\s]*)?",
    re.IGNORECASE,
)
ID_PATTERN = re.compile(r"^(https?://(?:[\w-]+\.)?streaam\.net)/(T|e)/([\w-]+)", re.IGNORECASE)

USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
)

M3U8_ABS_RE = re.compile(
    r"""https?://[^\s"'<>\\()\[\]{}|]+?\.m3u8(?:\?[^\s"'<>\\()\[\]{}|]*)?""",
    re.IGNORECASE,
)
M3U8_REL_RE = re.compile(
    r"""["'](/?[^"'\s<>]+?\.m3u8(?:\?[^"'\s<>]*)?)["']""",
    re.IGNORECASE,
)
IFRAME_RE = re.compile(r"""<iframe[^>]+src=["']([^"']+)["']""", re.IGNORECASE)
ATOB_RE = re.compile(r"""atob\(\s*['"]([A-Za-z0-9+/=_-]{16,})['"]\s*\)""")

# Dean Edwards packer:  eval(function(p,a,c,k,e,d){...}('payload',radix,count,'a|b|c'.split('|'),0,{}))
PACKED_RE = re.compile(
    r"""\}\(\s*(['"])(.*?)\1\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(['"])(.*?)\5\s*\.split\(\s*['"]\|['"]\s*\)""",
    re.DOTALL,
)
_ALPHA = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _encode(num: int, radix: int) -> str:
    head = "" if num < radix else _encode(num // radix, radix)
    rem = num % radix
    return head + (chr(rem + 29) if rem > 35 else _ALPHA[rem])


def _decode_packed(match: "re.Match") -> str | None:
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


def find_m3u8(html: str, base_url: str) -> str | None:
    texts = [html] + unpack_all(html)

    # base64 blobs hidden behind atob("...")
    for blob in ATOB_RE.findall(html):
        try:
            padded = blob.replace("-", "+").replace("_", "/")
            padded += "=" * (-len(padded) % 4)
            decoded = base64.b64decode(padded).decode("utf-8", "ignore")
            texts.append(decoded)
            texts.extend(unpack_all(decoded))
        except Exception:  # noqa: BLE001
            continue

    candidates: list[str] = []
    for raw in texts:
        text = _normalize(raw)
        for url in M3U8_ABS_RE.findall(text):
            if url not in candidates:
                candidates.append(url)
        for rel in M3U8_REL_RE.findall(text):
            if rel.lower().startswith("http"):
                continue
            url = urljoin(base_url, rel)
            if url not in candidates:
                candidates.append(url)

    if not candidates:
        return None
    for url in candidates:
        if "master" in url.lower():
            return url
    return candidates[0]


def _fetch(session: requests.Session, url: str, referer: str) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "Upgrade-Insecure-Requests": "1",
    }
    for attempt in range(2):
        try:
            resp = session.get(url, headers=headers, timeout=15, allow_redirects=True)
            if resp.status_code == 200:
                return resp.text
            log.info("GET %s -> HTTP %s", url, resp.status_code)
        except requests.RequestException as exc:
            log.info("GET %s failed (attempt %s): %s", url, attempt + 1, exc)
        time.sleep(1)
    return ""


def extract_stream(link: str) -> str | None:
    """Blocking extractor. Run it in a worker thread."""
    session = requests.Session()
    parsed = urlparse(link)
    origin = f"{parsed.scheme}://{parsed.netloc}/"

    pages: deque[tuple[str, str]] = deque()
    pages.append((link, origin))

    # Also try the sibling page (/T/<id> <-> /e/<id>).
    m = ID_PATTERN.match(link)
    if m:
        host, kind, vid = m.groups()
        alt_kind = "e" if kind.upper() == "T" else "T"
        pages.append((f"{host}/{alt_kind}/{vid}", origin))

    visited: set[str] = set()
    fetched = 0
    while pages and fetched < 6:
        url, referer = pages.popleft()
        if url in visited:
            continue
        visited.add(url)
        html = _fetch(session, url, referer)
        fetched += 1
        if not html:
            continue

        found = find_m3u8(html, url)
        if found:
            log.info("Extracted m3u8 from %s", url)
            return found

        # Follow embedded players (iframes / embed links on the same host).
        for src in IFRAME_RE.findall(html):
            nxt = urljoin(url, src)
            if nxt.startswith("http") and nxt not in visited:
                pages.append((nxt, url))
        for emb in LINK_PATTERN.findall(html):
            if emb not in visited:
                pages.append((emb, url))

    return None


# Small in-memory cache (links/tokens expire quickly, so keep it short).
_CACHE: dict[str, tuple[float, str]] = {}
_CACHE_LOCK = threading.Lock()
CACHE_TTL = 300


def get_stream(link: str) -> str | None:
    key = link.lower()
    now = time.time()
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit and now - hit[0] < CACHE_TTL:
            return hit[1]
    result = extract_stream(link)
    if result:
        with _CACHE_LOCK:
            _CACHE[key] = (now, result)
            for k in [k for k, v in _CACHE.items() if now - v[0] > CACHE_TTL]:
                _CACHE.pop(k, None)
    return result


def build_player_url(m3u8: str) -> str:
    sep = "&" if "?" in GITHUB_PLAYER_URL else "?"
    return f"{GITHUB_PLAYER_URL}{sep}stream={quote(m3u8, safe='')}"


# ----------------------------------------------------------------------------
# Telegram bot
# ----------------------------------------------------------------------------
bot = Client(
    "streaam_player_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,  # Render's disk is ephemeral
)

MAX_LINKS_PER_MESSAGE = 3
WORKERS = asyncio.Semaphore(3)


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


@bot.on_message(filters.command(["start", "help"]) & filters.incoming)
async def start_cmd(_, message):
    await message.reply_text(
        "Send me a streaam.net link (/T/... or /e/...) and I'll give you a "
        "button that plays it inside Telegram."
    )


@bot.on_message(filters.regex(LINK_PATTERN.pattern, flags=re.IGNORECASE) & filters.incoming)
async def on_link(_, message):
    text = message.text or message.caption or ""
    links: list[str] = []
    for m in LINK_PATTERN.finditer(text):
        if m.group(0) not in links:
            links.append(m.group(0))
    links = links[:MAX_LINKS_PER_MESSAGE]
    if not links:
        return

    is_private = message.chat.type == ChatType.PRIVATE

    for link in links:
        status = await message.reply_text("Extracting stream link...")
        try:
            async with WORKERS:
                m3u8 = await asyncio.to_thread(get_stream, link)
        except Exception:  # noqa: BLE001
            log.exception("extractor crashed for %s", link)
            m3u8 = None

        if not m3u8:
            await safe_edit(
                status,
                "Could not find a playable stream in that link. "
                "It may be expired, private, or protected.",
            )
            continue

        player_url = build_player_url(m3u8)

        # web_app buttons only work in private chats; groups get a normal URL button.
        if is_private:
            button = InlineKeyboardButton("Play Video", web_app=WebAppInfo(url=player_url))
        else:
            button = InlineKeyboardButton("Play Video", url=player_url)
        markup = InlineKeyboardMarkup([[button]])

        result = await safe_edit(status, "Stream ready. Tap the button to watch.", markup)
        if result is None and is_private:
            # Fallback if Telegram rejected the web_app button.
            fallback = InlineKeyboardMarkup([[InlineKeyboardButton("Play Video", url=player_url)]])
            await safe_edit(status, "Stream ready. Tap the button to watch.", fallback)


# ----------------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    threading.Thread(target=keep_alive_ping, daemon=True).start()
    log.info("Starting Telegram bot...")
    bot.run()
