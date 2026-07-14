"""
main.py — TGStream Hybrid Predictive Streamer
Railway deployment.

Proxy logic (the core of this rewrite):
  1. On first stream request -> start DownloadTask (background sequential fetch)
  2. For each Range request:
     a. Check DownloadMap: is [start,end] fully on disk?
        YES -> serve from SparseFile (pread)        <- zero Telegram cost, instant
        NO  -> serve from Telegram live (ByteStreamer)
     b. Hint downloader about play-head position
  3. Player never notices the switch.
"""
from __future__ import annotations

import asyncio
import hashlib
import httpx
import json
import math
import os
import re
import time
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Optional

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, AuthKeyDuplicated
from pyrogram.handlers import MessageHandler, RawUpdateHandler, CallbackQueryHandler
from pyrogram.raw.types import UpdateDeleteChannelMessages
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from starlette.status import HTTP_401_UNAUTHORIZED

import pyrogram.utils
import state as st
from clients import pool as client_pool
from downloader import DownloadMap, download_manager, STORAGE_DIR, LOCAL_READY_BYTES, MAX_LOCAL_GB
from streamer import ByteStreamer, TG_CHUNK
from metrics import metrics

# Monkey-patch Pyrogram to support newer 64-bit channel/chat IDs (> 32-bit suffixes)
def get_peer_type_patched(peer_id: int) -> str:
    peer_id_str = str(peer_id)
    if not peer_id_str.startswith("-"):
        return "user"
    elif peer_id_str.startswith("-100"):
        return "channel"
    else:
        return "chat"

pyrogram.utils.get_peer_type = get_peer_type_patched

load_dotenv()

API_ID             = int(os.getenv("API_ID", "0"))
API_HASH           = os.getenv("API_HASH", "")
SESSION_STRING     = os.getenv("SESSION_STRING", "")
BASE_URL           = os.getenv("BASE_URL", "")

CHANNEL_USERNAME   = os.getenv("CHANNEL_USERNAME", "").strip()
if CHANNEL_USERNAME:
    try:
        if CHANNEL_USERNAME.startswith("-") and CHANNEL_USERNAME[1:].isdigit():
            CHANNEL_USERNAME = int(CHANNEL_USERNAME)
        elif CHANNEL_USERNAME.isdigit():
            CHANNEL_USERNAME = int(CHANNEL_USERNAME)
    except ValueError:
        pass

REDIS_URL          = os.getenv("REDIS_URL", "")
SYNC_INTERVAL      = int(os.getenv("SYNC_INTERVAL", "300"))
FULL_RECONCILE_S   = int(os.getenv("FULL_RECONCILE_S", "300"))  # full history rescan cadence (deletions)
STREAM_CONCURRENCY = int(os.getenv("STREAM_CONCURRENCY", "3"))  # live proxy streams; keep low to avoid MTProto congestion
WAIT_TIMEOUT_S     = float(os.getenv("WAIT_TIMEOUT_S", "1.0"))  # Reduced from 2.0s for aggressive Path C
STARTUP_CHUNKS     = int(os.getenv("STARTUP_CHUNKS", "2"))  # 2 chunks × 1MB = 2MB initial fetch
LOCAL_READ_CHUNK   = int(os.getenv("LOCAL_READ_CHUNK", str(1024 * 1024)))  # Match TG_CHUNK for consistency
SHORT_WAIT_GRACE_BYTES = int(os.getenv("SHORT_WAIT_GRACE_BYTES", str(2 * 1024 * 1024)))  # 2MB grace window for Path B
DEBUG_PASSWORD     = os.getenv("DEBUG_PASSWORD", "")  # Password for /debug/* endpoints (if set)
# LOCAL_READY_BYTES imported from downloader (default 15MB)

source_chat_id: int | None = None

def get_tg() -> Client:
    return client_pool.primary()
redis_client: aioredis.Redis = None
byte_streamer: ByteStreamer = None
stream_sem: asyncio.Semaphore = None
_sync_lock = asyncio.Lock()
bot_client: Client = None  # dedicated Pyrogram client logged in as the bot (MTProto, not HTTPS)


def _schedule(coro):
    task = asyncio.create_task(coro)
    task.add_done_callback(_log_task_exception)
    return task


def _log_task_exception(task: asyncio.Task):
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[task] {type(e).__name__}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global source_chat_id, redis_client, byte_streamer, stream_sem, bot_client
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    # Configure Redis with retry and connection pool settings for resilience
    redis_client = aioredis.from_url(
        REDIS_URL,
        decode_responses=False,
        socket_connect_timeout=10,
        socket_keepalive=True,
        retry_on_timeout=True,
        health_check_interval=30,
    )
    stream_sem   = asyncio.Semaphore(STREAM_CONCURRENCY)

    await client_pool.start(API_ID, API_HASH, CHANNEL_USERNAME)

    if BOT_TOKEN:
        try:
            bot_client = Client(
                "notify_bot", api_id=API_ID, api_hash=API_HASH,
                bot_token=BOT_TOKEN, in_memory=True, no_updates=False,
                parse_mode=ParseMode.DISABLED,
            )
            await bot_client.start()
            print("[notify] bot_client started (MTProto, used for reliable notify send/edit)")

            if ADMIN_USER_ID:
                async def _pyro_admin_command(client, message):
                    if not message.from_user or str(message.from_user.id) != ADMIN_USER_ID:
                        return
                    text = message.text or ""
                    if text.startswith("/"):
                        try:
                            await _handle_admin_command(message.chat.id, text)
                        except Exception as e:
                            print(f"[bot] admin command failed: {type(e).__name__}: {e!r}")

                async def _pyro_admin_callback(client, callback_query):
                    if not callback_query.from_user or str(callback_query.from_user.id) != ADMIN_USER_ID:
                        return
                    cq_dict = {
                        "id": callback_query.id,
                        "data": callback_query.data,
                        "message": {
                            "chat": {"id": callback_query.message.chat.id},
                            "message_id": callback_query.message.id,
                        },
                    }
                    try:
                        await _handle_admin_callback(cq_dict)
                    except Exception as e:
                        print(f"[bot] callback failed: {type(e).__name__}: {e!r}")

                bot_client.add_handler(MessageHandler(_pyro_admin_command, filters.private & filters.text))
                bot_client.add_handler(CallbackQueryHandler(_pyro_admin_callback))
                print(f"[bot] MTProto admin command/callback handlers registered for user {ADMIN_USER_ID}")
        except Exception as e:
            print(f"[notify] bot_client failed to start, will fall back to HTTP API only: {type(e).__name__}: {e!r}")
            bot_client = None
        await _register_bot_commands()
    if CHANNEL_USERNAME:
        try:
            source_chat = await get_tg().get_chat(CHANNEL_USERNAME)
            source_chat_id = source_chat.id
            print(f"[listener] Resolved source channel id: {source_chat_id}")
        except Exception as e:
            print(f"[listener] failed to resolve source channel id for delete listener: {e}")

    # Register real-time Pyrogram update listener for instant prefetching on new channel posts
    async def _instant_sync_handler(client, message):
        print(f"[listener] Pyrogram new post detected ({message.id}) — instant sync")
        try:
            media = message.video or message.document
            if media:
                fn = getattr(media, "file_name", None)
                if fn:
                    mid = st.movie_id(fn)
                    existing_movies = await st.load_movies(redis_client)
                    if mid not in existing_movies:
                        print(f"[listener] instantly adding new movie: {mid}")
                        await st.save_movie(redis_client, mid, {
                            "message_id": message.id, "file_name": fn,
                            "file_size": media.file_size,
                            "file_size_text": st.fmt_size(media.file_size),
                            "quality": st.quality(fn), "source": st.source(fn),
                            "synced_at": int(time.time()),
                        })
                        # Genuine new upload event -> auto-prefetch.
                        try:
                            prefetch_queue.put_nowait(mid)
                        except asyncio.QueueFull:
                            print(f"[listener] prefetch_queue full, dropping {mid}")
            # Sync in background to reconcile index and clean up deletions
            _schedule(_sync_channel(force=True))
        except Exception as se:
            print(f"[listener] Pyrogram instant sync failed: {se}")

    async def _instant_delete_handler(client, update, users, chats):
        if not isinstance(update, UpdateDeleteChannelMessages):
            return
        if source_chat_id is None:
            return
        if _channel_update_chat_id(update) != source_chat_id:
            return
        try:
            removed = await _remove_deleted_messages(set(update.messages), "telegram channel delete update")
            if removed:
                await redis_client.set(st.R_SYNC_TS, str(time.time()))
        except Exception as se:
            print(f"[listener] instant delete cleanup failed: {se}")

    if CHANNEL_USERNAME:
        chat_filter = filters.chat(CHANNEL_USERNAME)
        media_filter = filters.video | filters.document
        get_tg().add_handler(MessageHandler(_instant_sync_handler, chat_filter & media_filter))
        get_tg().add_handler(RawUpdateHandler(_instant_delete_handler))
        print(f"[listener] Registered Pyrogram instant post/delete handlers for {CHANNEL_USERNAME}")

    byte_streamer = ByteStreamer(client_pool)
    download_manager.init_pool_size()
    download_manager.on_alert = _notify_send
    download_manager.on_evict = lambda mid: deferred_notifications.pop(mid, None)  # #2
    client_pool.on_health_event = _notify_send
    print(f"Pyrogram pool started ({len(client_pool)} client(s))")

    _schedule(_sync_loop())
    for i in range(download_manager._max_concurrent_downloads):
        _schedule(_prefetch_worker(worker_id=i))
    _schedule(_bot_channel_listener())
    _schedule(_sweep_loop())


    yield
    await download_manager.shutdown()
    await client_pool.stop()
    if bot_client:
        await bot_client.stop()
    await redis_client.aclose()
    await st.close_http_client()


app = FastAPI(title="TGStream", version="2.0.0", lifespan=lifespan, docs_url="/api/docs")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET", "HEAD", "OPTIONS"], allow_headers=["*"])

app.mount("/dashboard", StaticFiles(directory="static", html=True), name="dashboard")


async def _fetch_msg(msg_id: int, client: Client = None):
    c = client or get_tg()
    try:
        return await c.get_messages(CHANNEL_USERNAME, msg_id)
    except AuthKeyDuplicated as ae:
        print(f"[_fetch_msg] AuthKeyDuplicated on client. Marking client broken: {ae}")
        client_pool.mark_broken_by_client(c)
        alt_c = get_tg()
        if alt_c != c:
            return await alt_c.get_messages(CHANNEL_USERNAME, msg_id)
        raise ae


def _channel_update_chat_id(update: UpdateDeleteChannelMessages) -> int:
    return int(f"-100{update.channel_id}")


async def _remove_deleted_messages(message_ids: set[int], reason: str = "delete update") -> int:
    if not message_ids:
        return 0

    movies = await st.load_movies(redis_client)
    removed = []
    for mid, movie in movies.items():
        if int(movie.get("message_id", 0) or 0) in message_ids:
            removed.append((mid, movie.get("file_name", mid)))

    for mid, file_name in removed:
        print(f"[delete-listener] removing {mid} ({file_name}) from index/cache: {reason}")
        await st.del_movie(redis_client, mid)
        await download_manager.evict(mid, redis_client)
        deferred_notifications.pop(mid, None)

    if removed:
        await _notify_send(f"🗑 Removed {len(removed)} deleted movie{'s' if len(removed) != 1 else ''}")
    return len(removed)


SWEEP_INTERVAL_S = int(os.getenv("SWEEP_INTERVAL_S", "1800"))  # prune stale caches every 30min

async def _sweep_loop():
    """Bounds two otherwise-unbounded in-memory structures on a long-lived
    process: ByteStreamer._msg_cache and DownloadManager._tasks/_maps/_files
    for finished, long-idle movies."""
    while True:
        await asyncio.sleep(SWEEP_INTERVAL_S)
        try:
            n1 = byte_streamer.prune_msg_cache()
            n2 = await download_manager.prune_finished_tasks()
            if n1 or n2:
                print(f"[sweep] pruned {n1} msg-cache entries, {n2} finished task entries")
        except Exception as e:
            print(f"[sweep] {e}")


SYNC_POLL_S = int(os.getenv("SYNC_POLL_S", "120"))  # auto-detect new/removed movies

async def _sync_loop():
    while True:
        try:
            await _sync_channel(force=False)
        except AuthKeyDuplicated:
            print("[sync_loop] AuthKeyDuplicated, retrying instantly with healthy client")
            try:
                await _sync_channel(force=False)
            except Exception as e:
                print(f"[sync_loop] retry failed: {e}")
        except Exception as e:
            print(f"[sync_loop] {e}")
        await asyncio.sleep(SYNC_POLL_S)


BOT_TOKEN      = os.getenv("BOT_TOKEN", "").strip()        # from @BotFather
NOTIFY_CHAT_ID = os.getenv("NOTIFY_CHAT_ID", "").strip()   # channel/chat id, bot must be admin
TG_API_BASE    = os.getenv("TELEGRAM_API_URL", "https://api.telegram.org").strip().rstrip("/")
_TG_API        = f"{TG_API_BASE}/bot{BOT_TOKEN}"
DISABLE_BOT_LISTENER = os.getenv("DISABLE_BOT_LISTENER", "false").strip().lower() == "true"
ADMIN_USER_ID  = os.getenv("ADMIN_USER_ID", "").strip()  # Telegram user id allowed to issue /commands via DM
_START_TIME    = time.time()


async def _register_bot_commands():
    """Registers the / command menu shown by Telegram's client UI.
    Purely cosmetic — commands already work when typed manually via
    _handle_admin_command regardless of this call."""
    commands = [
        ("status", "Pool/cache/queue snapshot"),
        ("list", "Browse catalog, cache state, tap to delete"),
        ("pause", "Pause background prefetching"),
        ("resume", "Resume background prefetching"),
        ("evict", "Drop a cached movie: /evict <id>"),
        ("find", "Search catalog: /find <name>"),
        ("help", "Show available commands"),
    ]
    if bot_client and bot_client.is_connected:
        try:
            await bot_client.set_bot_commands([BotCommand(c, d) for c, d in commands])
            print("[bot] command menu registered (via bot_client)")
            return
        except Exception as e:
            print(f"[bot] set_bot_commands via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{_TG_API}/setMyCommands",
                              json={"commands": [{"command": c, "description": d} for c, d in commands]})
            data = r.json()
            if data.get("ok"):
                print("[bot] command menu registered")
            else:
                print(f"[bot] setMyCommands rejected: {data.get('description')}")
    except Exception as e:
        print(f"[bot] setMyCommands failed: {type(e).__name__}: {e!r}")


def _resolve_chat_id(raw: str) -> int | str:
    if not raw:
        return ""
    try:
        if raw.startswith("-") and raw[1:].isdigit():
            return int(raw)
        elif raw.isdigit():
            return int(raw)
    except ValueError:
        pass
    return raw


async def _notify_send(text: str) -> int | None:
    if not NOTIFY_CHAT_ID:
        return None

    chat_id = _resolve_chat_id(NOTIFY_CHAT_ID)

    # Prefer MTProto via dedicated bot_client — bypasses the api.telegram.org
    # HTTPS ConnectTimeout issues seen on some hosts, still posts as the bot.
    if bot_client and bot_client.is_connected:
        try:
            msg = await bot_client.send_message(chat_id, text)
            return msg.id
        except Exception as pe:
            print(f"[notify] bot_client send failed, falling back to HTTP: {type(pe).__name__}: {pe!r}")

    # Fallback: HTTP Bot API
    if not BOT_TOKEN:
        return None
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{_TG_API}/sendMessage",
                              json={"chat_id": NOTIFY_CHAT_ID, "text": text})
            return r.json().get("result", {}).get("message_id")
    except Exception as e:
        print(f"[notify] HTTP send failed: {type(e).__name__}: {e!r}")
        return None


async def _notify_edit(msg_id: int, text: str) -> float:
    """Returns 0 on success/harmless-no-op, or seconds to wait if rate-limited."""
    if not NOTIFY_CHAT_ID or not msg_id:
        return 0

    chat_id = _resolve_chat_id(NOTIFY_CHAT_ID)

    # Prefer MTProto via dedicated bot_client, same reasoning as _notify_send
    if bot_client and bot_client.is_connected:
        try:
            await bot_client.edit_message_text(chat_id, msg_id, text)
            return 0
        except FloodWait as fw:
            print(f"[notify] bot_client edit rate-limited, backing off {fw.value}s")
            return float(fw.value)
        except Exception as pe:
            desc = str(pe)
            if "MESSAGE_NOT_MODIFIED" in desc or "not modified" in desc.lower():
                return 0
            print(f"[notify] bot_client edit failed, falling back to HTTP: {type(pe).__name__}: {pe!r}")

    # Fallback: HTTP Bot API
    if not BOT_TOKEN:
        return 0
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{_TG_API}/editMessageText",
                             json={"chat_id": NOTIFY_CHAT_ID, "message_id": msg_id, "text": text})
            data = r.json()
            if not data.get("ok"):
                desc = data.get("description", "")
                if data.get("error_code") == 429:
                    wait = data.get("parameters", {}).get("retry_after", 3)
                    print(f"[notify] HTTP rate-limited, backing off {wait}s")
                    return float(wait)
                if "not modified" not in desc:
                    print(f"[notify] HTTP edit rejected: {desc}")
            return 0
    except Exception as e:
        print(f"[notify] HTTP edit failed: {type(e).__name__}: {e!r}")
        return 0


def _progress_bar(pct: int, width: int = 12) -> str:
    filled = round(width * pct / 100)
    return "▓" * filled + "░" * (width - filled)


def _fmt_eta(remaining_mb: float, speed_mbps: float) -> str:
    if speed_mbps <= 0.05:
        return "…"
    secs = remaining_mb / speed_mbps
    if secs < 60:
        return f"{secs:.0f}s"
    if secs < 3600:
        return f"{secs/60:.0f}m"
    return f"{secs/3600:.1f}h"


PROGRESS_EDIT_MIN_PCT = int(os.getenv("PROGRESS_EDIT_MIN_PCT", "5"))   # only edit on >=this % change
PROGRESS_EDIT_MAX_S   = int(os.getenv("PROGRESS_EDIT_MAX_S", "10"))    # ...or after this many seconds, whichever first


async def _progress_reporter(movie_id: str, file_name: str, file_size: int, msg_id: int | None):
    """Edit the notify message with a progress bar, %, speed, and ETA.
    Throttled to only actually call the Telegram edit API on a meaningful
    percent change or after PROGRESS_EDIT_MAX_S, whichever comes first —
    a 1s fixed interval burns edit-API calls (and risks FloodWait) for
    large files that take many minutes."""
    if not msg_id:
        return
    last_bytes = 0
    last_ts = time.time()
    last_sent_pct = -100
    last_sent_ts = 0.0
    rate_limit_cooldown = 0.0
    while True:
        task = download_manager.get(movie_id)
        if not task or not task._task or task._task.done():
            break
        dl_map = download_manager.get_map(movie_id)
        done_bytes = dl_map.total_bytes() if dl_map else 0
        pct = min(100, int(done_bytes / file_size * 100)) if file_size else 0

        now = time.time()
        elapsed = now - last_ts
        speed_mbps = ((done_bytes - last_bytes) / 1024 / 1024) / elapsed if elapsed > 0 else 0.0
        last_bytes, last_ts = done_bytes, now

        should_send = (
            rate_limit_cooldown <= 0
            and (abs(pct - last_sent_pct) >= PROGRESS_EDIT_MIN_PCT or (now - last_sent_ts) >= PROGRESS_EDIT_MAX_S)
        )
        if should_send:
            size_mb = file_size / 1024 / 1024
            done_mb = done_bytes / 1024 / 1024
            eta = _fmt_eta(size_mb - done_mb, speed_mbps)
            text = (f"⬇️ Prefetching: {file_name}\n"
                    f"{_progress_bar(pct)} {pct}%\n"
                    f"{done_mb:.0f}MB / {size_mb:.0f}MB · {speed_mbps:.2f} MB/s · ETA {eta}")
            rate_limit_cooldown = await _notify_edit(msg_id, text)
            last_sent_pct = pct
            last_sent_ts = now
        else:
            rate_limit_cooldown = max(0.0, rate_limit_cooldown - 1)

        await asyncio.sleep(1)


prefetch_queue: "asyncio.Queue[str]" = asyncio.Queue(maxsize=200)


async def _bot_reply(chat_id, text: str):
    """Admin command replies — prefers MTProto bot_client (reliable on hosts
    where HTTPS to api.telegram.org is flaky/blocked), HTTP as fallback."""
    if bot_client and bot_client.is_connected:
        try:
            await bot_client.send_message(chat_id, text)
            return
        except Exception as e:
            print(f"[bot] reply via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(f"{_TG_API}/sendMessage", json={"chat_id": chat_id, "text": text})
    except Exception as e:
        print(f"[bot] reply failed: {type(e).__name__}: {e!r}")


LIST_PAGE_SIZE = 8


def _short_id(mid: str) -> str:
    """Short stable hash for callback_data — movie_id itself is often
    too long for Telegram's 64-byte callback_data limit."""
    return hashlib.sha1(mid.encode()).hexdigest()[:10]


async def _render_list_page(page: int = 0):
    """Builds (text, inline_keyboard) for /list — cached symbol per movie,
    one delete button per row, prev/next nav."""
    movies = await st.load_movies(redis_client)
    items = sorted(movies.items(), key=lambda kv: kv[1].get("file_name", kv[0]))
    total = len(items)
    pages = max(1, math.ceil(total / LIST_PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    chunk = items[page * LIST_PAGE_SIZE: page * LIST_PAGE_SIZE + LIST_PAGE_SIZE]

    lines = [f"🎬 Catalog ({total}) — page {page+1}/{pages}", ""]
    keyboard = []
    for mid, m in chunk:
        cached = await redis_client.get(f"tgstream:dl:done:{mid}")
        symbol = "✅" if cached == b"1" else "⬜"
        fn = m.get("file_name", mid)
        lines.append(f"{symbol} {fn}")
        keyboard.append([{
            "text": f"🗑 Delete: {fn[:30]}",
            "callback_data": f"del:{_short_id(mid)}:{page}",
        }])

    nav = []
    if page > 0:
        nav.append({"text": "⬅ Prev", "callback_data": f"pg:{page-1}"})
    if page < pages - 1:
        nav.append({"text": "Next ➡", "callback_data": f"pg:{page+1}"})
    if nav:
        keyboard.append(nav)

    if not chunk:
        lines.append("(empty)")

    return "\n".join(lines), {"inline_keyboard": keyboard}


def _to_pyro_markup(keyboard: dict) -> InlineKeyboardMarkup:
    rows = keyboard.get("inline_keyboard", [])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(btn["text"], callback_data=btn["callback_data"]) for btn in row]
        for row in rows
    ])


async def _bot_send_keyboard(chat_id, text: str, keyboard: dict):
    if bot_client and bot_client.is_connected:
        try:
            msg = await bot_client.send_message(chat_id, text, reply_markup=_to_pyro_markup(keyboard))
            return msg.id
        except Exception as e:
            print(f"[bot] list send via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return None
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{_TG_API}/sendMessage",
                              json={"chat_id": chat_id, "text": text, "reply_markup": keyboard})
            return r.json().get("result", {}).get("message_id")
    except Exception as e:
        print(f"[bot] list send failed: {type(e).__name__}: {e!r}")
        return None


async def _bot_edit_keyboard(chat_id, message_id, text: str, keyboard: dict):
    if bot_client and bot_client.is_connected:
        try:
            await bot_client.edit_message_text(chat_id, message_id, text, reply_markup=_to_pyro_markup(keyboard))
            return
        except Exception as e:
            desc = str(e)
            if "MESSAGE_NOT_MODIFIED" in desc:
                return
            print(f"[bot] list edit via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(f"{_TG_API}/editMessageText",
                          json={"chat_id": chat_id, "message_id": message_id,
                                "text": text, "reply_markup": keyboard})
    except Exception as e:
        print(f"[bot] list edit failed: {type(e).__name__}: {e!r}")


async def _bot_answer_callback(callback_id: str, text: str | None = None):
    if bot_client and bot_client.is_connected:
        try:
            await bot_client.answer_callback_query(callback_id, text=text or "")
            return
        except Exception as e:
            print(f"[bot] answerCallbackQuery via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            payload = {"callback_query_id": callback_id}
            if text:
                payload["text"] = text
            await c.post(f"{_TG_API}/answerCallbackQuery", json=payload)
    except Exception as e:
        print(f"[bot] answerCallbackQuery failed: {type(e).__name__}: {e!r}")


async def _handle_admin_callback(cq: dict):
    """Handles inline-button presses from /list: pagination + delete."""
    data = cq.get("data", "")
    msg = cq.get("message", {}) or {}
    chat_id = msg.get("chat", {}).get("id")
    message_id = msg.get("message_id")
    cq_id = cq.get("id")
    if chat_id is None or message_id is None:
        return

    if data.startswith("pg:"):
        page = int(data.split(":", 1)[1])
        text, kb = await _render_list_page(page)
        await _bot_edit_keyboard(chat_id, message_id, text, kb)
        await _bot_answer_callback(cq_id)
        return

    if data.startswith("del:"):
        _, short, page_s = data.split(":", 2)
        page = int(page_s)
        movies = await st.load_movies(redis_client)
        target = next((mid for mid in movies if _short_id(mid) == short), None)
        if not target:
            await _bot_answer_callback(cq_id, "Not found (already deleted?)")
            text, kb = await _render_list_page(page)
            await _bot_edit_keyboard(chat_id, message_id, text, kb)
            return
        fn = movies[target].get("file_name", target)
        await download_manager.evict(target, redis_client)
        await st.del_movie(redis_client, target)
        await _bot_answer_callback(cq_id, f"Deleted: {fn[:50]}")
        text, kb = await _render_list_page(page)
        await _bot_edit_keyboard(chat_id, message_id, text, kb)
        return

    await _bot_answer_callback(cq_id)


async def _handle_admin_command(chat_id, text: str):
    """Minimal remote control over DM. Only responds to ADMIN_USER_ID."""
    parts = text.strip().split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd in ("/status", "/status@" ):
        uptime_s = time.time() - _START_TIME
        uptime = f"{uptime_s/3600:.1f}h" if uptime_s > 3600 else f"{uptime_s/60:.0f}m"
        healthy = client_pool.healthy_count()
        stats = download_manager.stats()
        active = sum(1 for s in stats.values() if s["task_running"])
        total_local = sum(s["size_on_disk_mb"] for s in stats.values())
        movies = await st.load_movies(redis_client)
        await _bot_reply(chat_id,
            f"📊 Status\n"
            f"Uptime: {uptime}\n"
            f"Clients: {healthy}/{len(client_pool)} healthy\n"
            f"Catalog: {len(movies)} movies\n"
            f"Downloads: {active} active, {len(stats)} tracked\n"
            f"Local cache: {total_local/1024:.1f}GB / {MAX_LOCAL_GB:.0f}GB\n"
            f"Prefetch queue: {prefetch_queue.qsize()}\n"
            f"Prefetch paused: {download_manager.paused}")

    elif cmd == "/pause":
        download_manager.paused = True
        await _bot_reply(chat_id, "⏸ Prefetching paused. Active playback downloads unaffected.")

    elif cmd == "/resume":
        download_manager.paused = False
        await _bot_reply(chat_id, "▶️ Prefetching resumed.")

    elif cmd == "/evict":
        if not arg:
            await _bot_reply(chat_id, "Usage: /evict <movie_id>")
            return
        movies = await st.load_movies(redis_client)
        if arg not in movies:
            await _bot_reply(chat_id, f"Not found: {arg}")
            return
        await download_manager.evict(arg, redis_client)
        await _bot_reply(chat_id, f"🗑 Evicted: {arg}")

    elif cmd == "/find":
        if not arg:
            await _bot_reply(chat_id, "Usage: /find <name>")
            return
        movies = await st.load_movies(redis_client)
        matches = [
            (mid, m) for mid, m in movies.items()
            if st.flex_match(arg, m.get("file_name", ""))
        ][:5]
        if not matches:
            await _bot_reply(chat_id, f"No matches for: {arg}")
            return
        lines = []
        for mid, m in matches:
            cached = await redis_client.get(f"tgstream:dl:done:{mid}")
            state_tag = "✅ cached" if cached == b"1" else "—"
            lines.append(f"{m.get('file_name', mid)} ({state_tag})\nid: {mid}")
        await _bot_reply(chat_id, "🔎 Matches:\n\n" + "\n\n".join(lines))

    elif cmd == "/list":
        text, kb = await _render_list_page(0)
        await _bot_send_keyboard(chat_id, text, kb)

    elif cmd in ("/help", "/start"):
        await _bot_reply(chat_id,
            "Commands:\n"
            "/status — pool/cache/queue snapshot\n"
            "/pause /resume — toggle background prefetching\n"
            "/list — browse catalog, ✅/⬜ cache state, tap to delete\n"
            "/evict <id> — drop a cached movie\n"
            "/find <name> — search catalog + cache state")

    # unknown commands are ignored silently — DMs to the bot aren't
    # necessarily commands and shouldn't get a noisy reply


async def _bot_channel_listener():
    """Long-poll the bot's own getUpdates for channel_post events.
    Fires an instant force-sync the moment a new post lands in the
    channel — no waiting for SYNC_POLL_S. Falls back to normal poll
    loop if BOT_TOKEN not set."""
    if DISABLE_BOT_LISTENER:
        print("[listener] DISABLE_BOT_LISTENER is true, skipping instant-post listener")
        return
    if not BOT_TOKEN:
        print("[listener] BOT_TOKEN not set, skipping instant-post listener")
        return
    if bot_client and bot_client.is_connected:
        print("[listener] MTProto bot_client is active, skipping HTTP long-poll updates listener")
        return
    if ADMIN_USER_ID:
        print(f"[listener] admin commands enabled for user {ADMIN_USER_ID}")
    else:
        print("[listener] ADMIN_USER_ID not set — /status /pause /resume /evict /find disabled")
    offset = 0
    async with httpx.AsyncClient(timeout=45) as poll_client:
        while True:
            try:
                r = await poll_client.get(f"{_TG_API}/getUpdates", params={
                    "offset": offset, "timeout": 30,
                    "allowed_updates": '["channel_post","message","callback_query"]',
                })
                if r.status_code != 200:
                    print(f"[listener] HTTP error {r.status_code}: {r.text[:200]}")
                    await asyncio.sleep(15)
                    continue

                try:
                    data = r.json()
                except Exception as je:
                    print(f"[listener] JSON decode failed: {je}. Response: {r.text[:200]}")
                    await asyncio.sleep(15)
                    continue

                if not data.get("ok"):
                    desc = data.get("description", "Unknown error")
                    err_code = data.get("error_code")
                    print(f"[listener] Telegram error {err_code}: {desc}")
                    await asyncio.sleep(15)
                    continue

                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    post = upd.get("channel_post")
                    if post and (post.get("video") or post.get("document")):
                        print("[listener] new channel post detected — instant sync")
                        try:
                            await _sync_channel(force=True)
                        except Exception as e:
                            print(f"[listener] sync failed: {e}")
                        continue

                    dm = upd.get("message")
                    if dm and ADMIN_USER_ID:
                        sender_id = str(dm.get("from", {}).get("id", ""))
                        text = dm.get("text", "")
                        if sender_id == ADMIN_USER_ID and text.startswith("/"):
                            try:
                                await _handle_admin_command(dm["chat"]["id"], text)
                            except Exception as e:
                                print(f"[listener] admin command failed: {e}")
                        continue

                    cq = upd.get("callback_query")
                    if cq and ADMIN_USER_ID:
                        sender_id = str(cq.get("from", {}).get("id", ""))
                        if sender_id == ADMIN_USER_ID:
                            try:
                                await _handle_admin_callback(cq)
                            except Exception as e:
                                print(f"[listener] callback failed: {e}")
            except Exception as e:
                print(f"[listener] poll error ({type(e).__name__}): {repr(e)}")
                if not isinstance(e, (httpx.TimeoutException, httpx.NetworkError)):
                    traceback.print_exc()
                await asyncio.sleep(10)


deferred_notifications = {}

async def _prefetch_worker(worker_id: int = 0):
    """Pulls one movie_id at a time, downloads it fully in background.
    Skipped/paused automatically whenever a real Stremio stream is live
    (see live_streams check in downloader.py) -> streaming always wins.

    Multiple instances of this coroutine run concurrently (sized to
    download_manager._max_concurrent_downloads) so prefetching actually
    uses all the concurrent download slots instead of draining the queue
    one movie at a time regardless of pool size."""
    while True:
        movie_id = await prefetch_queue.get()
        try:
            if download_manager.paused:
                print(f"[prefetch:{worker_id}] paused, requeueing {movie_id}")
                await asyncio.sleep(15)
                try:
                    prefetch_queue.put_nowait(movie_id)
                except asyncio.QueueFull:
                    print(f"[prefetch:{worker_id}] prefetch_queue full, dropping {movie_id} on pause requeue")
                continue
            movies = await st.load_movies(redis_client)
            m = movies.get(movie_id)
            if not m:
                continue
            fn = m.get("file_name", movie_id)
            print(f"[prefetch:{worker_id}] starting {movie_id} ({fn})")

            # Start download task first so it starts instantly
            task = await download_manager.get_or_create(
                movie_id=movie_id,
                file_size=m["file_size"],
                message_id=m["message_id"],
                redis=redis_client,
                byte_streamer=byte_streamer,
                fetch_msg_fn=_fetch_msg,
                priority=False,
            )

            # Send notification afterwards (if task successfully started)
            msg_id = deferred_notifications.pop(movie_id, None)
            reporter = None  # N5: initialize before any await so finally can always cancel
            if task and task._task:
                if msg_id:
                    await _notify_edit(msg_id, f"⬇️ Prefetching: {fn}\n0/100")
                else:
                    msg_id = await _notify_send(f"⬇️ Prefetching: {fn}\n0/100")
                
                reporter = asyncio.create_task(
                    _progress_reporter(movie_id, fn, m["file_size"], msg_id)
                )
                try:
                    await task._task  # wait till done/cancelled/evicted before next queued item
                finally:
                    reporter.cancel()
                # Cancelled mid-way by a priority (play) request? -> not really
                # done, put back at the end of the queue to finish later.
                # But if the user explicitly paused/evicted it from the
                # dashboard, respect that instead of instantly restarting.
                done_val = await redis_client.get(f"tgstream:dl:done:{movie_id}")
                stopped = await redis_client.get(f"tgstream:dl:stopped:{movie_id}")
                if done_val == b"1":
                    await _notify_edit(msg_id, f"✅ Prefetched: {fn}\n100/100\n{BASE_URL}/proxy/{movie_id}")
                elif stopped == b"1":
                    print(f"[prefetch:{worker_id}] {movie_id} explicitly stopped, not requeueing")
                    await _notify_edit(msg_id, f"⏸ Paused: {fn}")
                else:
                    print(f"[prefetch:{worker_id}] {movie_id} preempted, requeueing")
                    try:
                        prefetch_queue.put_nowait(movie_id)
                    except asyncio.QueueFull:
                        print(f"[prefetch:{worker_id}] prefetch_queue full, dropping {movie_id} on preempt requeue")
            else:
                done_val = await redis_client.get(f"tgstream:dl:done:{movie_id}")
                if done_val == b"1":
                    ready_text = f"✅ Already cached: {fn}\n{BASE_URL}/proxy/{movie_id}"
                    if msg_id:
                        await _notify_edit(msg_id, ready_text)
                    else:
                        await _notify_send(ready_text)
                else:
                    # another download (priority or another prefetch) is
                    # active right now — wait a bit, then retry
                    print(f"[prefetch:{worker_id}] {movie_id} deferred, another download active")
                    if msg_id:
                        await _notify_edit(msg_id, f"⏳ Waiting to prefetch: {fn}\n(Another download is currently active)")
                    else:
                        msg_id = await _notify_send(f"⏳ Waiting to prefetch: {fn}\n(Another download is currently active)")
                    if msg_id:
                        deferred_notifications[movie_id] = msg_id
                    await asyncio.sleep(15)
                    try:
                        prefetch_queue.put_nowait(movie_id)
                    except asyncio.QueueFull:
                        print(f"[prefetch:{worker_id}] prefetch_queue full, dropping {movie_id} on deferred requeue")
            print(f"[prefetch:{worker_id}] finished {movie_id}")
        except Exception as e:
            print(f"[prefetch:{worker_id}] {movie_id} failed: {e}")
            if reporter is not None and not reporter.done():
                reporter.cancel()
        finally:
            prefetch_queue.task_done()


async def _sync_channel(force: bool = False) -> int:
    async with _sync_lock:
        if not force:
            last = await redis_client.get(st.R_SYNC_TS)
            if last:
                try:
                    interval = SYNC_POLL_S if DISABLE_BOT_LISTENER else SYNC_INTERVAL
                    if (time.time() - float(last)) < interval:
                        # Return the current movies count
                        return await redis_client.hlen(st.R_MOVIES)
                except ValueError:
                    pass

        acquired = await redis_client.set(st.R_SYNC_LCK, "1", ex=600, nx=True)
        if not acquired:
            return 0
        try:
            existing_movies = await st.load_movies(redis_client)
            existing_ids = set(existing_movies.keys())

            # Full history walk only on forced syncs (manual /sync, instant
            # post handler) and roughly every FULL_RECONCILE_S otherwise —
            # routine polling uses min_id so it only pulls NEW messages
            # instead of re-scanning the whole channel every cycle.
            last_full = await redis_client.get(st.R_SYNC_FULL_TS)
            do_full = force or not last_full or (time.time() - float(last_full)) > FULL_RECONCILE_S

            min_id = 0
            if not do_full:
                raw_max = await redis_client.get(st.R_SYNC_MAX_ID)
                min_id = int(raw_max) if raw_max else 0

            count = 0
            found_ids = set(existing_ids) if not do_full else set()
            max_id_seen = min_id
            active_tg = get_tg()
            try:
                # Pyrogram 2.x get_chat_history has no min_id filter — it
                # walks newest -> oldest via offset_id. For an incremental
                # pass we just stop as soon as we hit a message id we've
                # already synced, instead of paging through the full
                # history every cycle.
                async for msg in active_tg.get_chat_history(CHANNEL_USERNAME):
                    if not do_full and msg.id <= min_id:
                        break
                    try:
                        media = msg.video or msg.document
                        if not media: continue
                        fn = getattr(media, "file_name", None)
                        if not fn: continue
                        mid = st.movie_id(fn)
                        await st.save_movie(redis_client, mid, {
                            "message_id": msg.id, "file_name": fn,
                            "file_size": media.file_size,
                            "file_size_text": st.fmt_size(media.file_size),
                            "quality": st.quality(fn), "source": st.source(fn),
                            "synced_at": int(time.time()),
                        })
                        found_ids.add(mid)
                        count += 1
                        max_id_seen = max(max_id_seen, msg.id)
                    except Exception: continue
            except AuthKeyDuplicated as ae:
                print(f"[sync] AuthKeyDuplicated on client. Marking client broken: {ae}")
                client_pool.mark_broken_by_client(active_tg)
                raise ae

            # New movies get added to the catalog only — no auto-prefetch.
            # Download starts on demand (Stremio stream request or dashboard).
            new_ids = found_ids - existing_ids
            for mid in new_ids:
                print(f"Sync: new movie detected (catalog only, no auto-prefetch): {mid}")

            # Clean up deleted movies — only meaningful on a full walk;
            # an incremental (min_id) pass never sees old messages so it
            # must never be treated as evidence they were deleted.
            removed_ids = set()
            if do_full:
                removed_ids = existing_ids - found_ids
                for mid in removed_ids:
                    print(f"Sync: removing deleted movie {mid} from index")
                    await st.del_movie(redis_client, mid)
                    await download_manager.evict(mid, redis_client)
                await redis_client.set(st.R_SYNC_FULL_TS, str(time.time()))

            if new_ids or removed_ids:
                await _notify_send(f"🔄 Synced: {len(new_ids)} new, {len(removed_ids)} removed")

            if max_id_seen > min_id:
                await redis_client.set(st.R_SYNC_MAX_ID, str(max_id_seen))
            await redis_client.set(st.R_SYNC_TS, str(time.time()))
            print(f"Sync: {count} new/updated movies ({'full' if do_full else 'incremental'})")
            return await redis_client.hlen(st.R_MOVIES)
        finally:
            await redis_client.delete(st.R_SYNC_LCK)


MANIFEST = {
    "id": "org.tgstream.hybrid", "version": "2.0.0", "name": "TGStream",
    "description": "Hybrid predictive streaming from Telegram via Stremio",
    "resources": ["catalog", "meta", "stream", "subtitles"], "types": ["movie", "series"],
    "idPrefixes": ["tgm:", "tgs:"],
    "catalogs": [
        {"type": "movie",  "id": "tgstream_movies", "name": "TG Movies"},
        {"type": "series", "id": "tgstream_series", "name": "TG Series"},
    ],
    "behaviorHints": {"configurable": False, "configurationRequired": False},
}


@app.get("/")
async def health():
    movies = await redis_client.hlen(st.R_MOVIES)
    last   = await redis_client.get(st.R_SYNC_TS)
    age    = round((time.time() - float(last)) / 60, 1) if last else None
    dl     = download_manager.stats()
    return {"status": "ok", "movies": movies, "channel": CHANNEL_USERNAME,
            "sync_age_min": age, "active_downloads": len(dl), "download_stats": dl}


@app.get("/manifest.json")
async def manifest(): return JSONResponse(MANIFEST)


@app.get("/sync")
async def manual_sync():
    try:
        return {"synced": await _sync_channel(force=True)}
    except AuthKeyDuplicated:
        print("[sync] Retrying manual sync after marking previous client broken")
        return {"synced": await _sync_channel(force=True)}


async def _debug_auth(request: Request):
    """Check debug endpoint authentication if DEBUG_PASSWORD is set."""
    if not DEBUG_PASSWORD:
        return  # No password set, allow access
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        if token == DEBUG_PASSWORD:
            return
    raise HTTPException(status_code=HTTP_401_UNAUTHORIZED, detail="Unauthorized")


@app.get("/debug/movies")
async def debug_movies(request: Request):
    await _debug_auth(request)
    movies = await st.load_movies(redis_client)
    for mid, m in movies.items():
        task = download_manager.get(mid)
        dl_map = download_manager.get_map(mid)
        if not dl_map:
            dl_map = await download_manager._load_map(mid, redis_client)
            
        file_path = STORAGE_DIR / f"{mid}.bin"
        exists = file_path.exists()
        
        cached_bytes = dl_map.total_bytes() if exists else 0
        m["cached_bytes"] = cached_bytes
        m["cached_text"] = st.fmt_size(cached_bytes)
        
        fs = m.get("file_size", 0)
        m["pct"] = round(cached_bytes / fs * 100, 1) if fs and exists else 0
        
        is_done = False
        if exists:
            done_val = await redis_client.get(f"tgstream:dl:done:{mid}")
            is_done = done_val == b"1" or cached_bytes >= fs
            
        m["is_done"] = is_done
        m["is_active"] = bool(task and task._task and not task._task.done())
    return movies


@app.get("/debug/downloads")
async def debug_downloads(request: Request):
    await _debug_auth(request)
    stats  = download_manager.stats()
    movies = await st.load_movies(redis_client)
    for mid, s in stats.items():
        movie = movies.get(mid, {})
        fs    = movie.get("file_size", 0)
        s["total_mb"]   = round(fs / 1024 / 1024, 1) if fs else 0
        s["pct_done"]   = round(s["downloaded_mb"] / s["total_mb"] * 100, 1) if s.get("total_mb") else 0
        s["file_name"]  = movie.get("file_name", mid)
    return stats


@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    # Always trigger sync on catalog request for freshest data (using rate-limiting inside _sync_channel)
    print(f"Catalog request: triggering fresh sync")
    try:
        await _sync_channel(force=False)
    except Exception as e:
        print(f"Catalog sync failed: {e}")
    
    movies = await st.load_movies(redis_client)
    def is_series(m): return bool(st.IS_SERIES_RE.search(m.get("file_name","")))
    
    if type == "movie":
        filtered = {mid: m for mid, m in movies.items() if not is_series(m)}
        async def build(mid, m):
            fn = m.get("file_name","Unknown")
            try:
                poster, imdb_id = await st.get_poster_and_imdb(redis_client, fn)
            except Exception as e:
                print(f"[catalog] Poster fetch failed for {fn}: {e}")
                poster, imdb_id = "https://via.placeholder.com/300x450?text=No+Poster", ""
            title, year = st.parse_title_year(fn)
            meta = {"id": f"tgm:{mid}", "type": "movie", "name": title or fn,
                    "poster": poster, "posterShape": "poster", "year": year}
            if imdb_id:
                meta["imdb_id"] = imdb_id
            return meta
        # Process movies in batches to avoid overwhelming Redis
        metas = []
        batch_size = 5
        items = list(filtered.items())
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            batch_metas = await asyncio.gather(*[build(mid, m) for mid, m in batch], return_exceptions=True)
            for result in batch_metas:
                if isinstance(result, Exception):
                    print(f"[catalog] Build failed: {result}")
                else:
                    metas.append(result)
        return JSONResponse({"metas": list(metas)}, headers={"Cache-Control": "no-store"})
    
    else:  # type == "series"
        series_groups = {}
        for mid, m in movies.items():
            if not is_series(m): continue
            fn = m.get("file_name","Unknown")
            show_title = st.parse_show_title(fn)
            sid = st.show_id(fn)
            if sid not in series_groups:
                series_groups[sid] = {"title": show_title, "files": []}
            series_groups[sid]["files"].append((mid, m))
            
        async def build_series(sid, group):
            fn = group["files"][0][1].get("file_name","Unknown")
            try:
                poster, imdb_id = await st.get_poster_and_imdb(redis_client, fn)
            except Exception as e:
                print(f"[catalog] Poster fetch failed for {fn}: {e}")
                poster, imdb_id = "https://via.placeholder.com/300x450?text=No+Poster", ""
            year = ""
            for _, m in group["files"]:
                _, y = st.parse_title_year(m.get("file_name",""))
                if y:
                    year = y
                    break
            meta = {"id": f"tgs:{sid}", "type": "series", "name": group["title"],
                    "poster": poster, "posterShape": "poster", "year": year}
            if imdb_id:
                meta["imdb_id"] = imdb_id
            return meta
        # Process series in batches to avoid overwhelming Redis
        metas = []
        batch_size = 5
        items = list(series_groups.items())
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            batch_metas = await asyncio.gather(*[build_series(sid, group) for sid, group in batch], return_exceptions=True)
            for result in batch_metas:
                if isinstance(result, Exception):
                    print(f"[catalog] Build failed: {result}")
                else:
                    metas.append(result)
        return JSONResponse({"metas": list(metas)}, headers={"Cache-Control": "no-store"})


@app.get("/meta/{type}/{id}.json")
async def meta(type: str, id: str):
    if id.startswith("tt"):
        title, year = await st.get_cinemeta(type, id)
        meta_obj = {"id": id, "type": type, "name": title, "year": year}
        if type == "series" and title:
            movies = await st.load_movies(redis_client)
            videos = []
            seen_episodes = set()
            matching = [m for m in movies.values() if st.flex_match(title, m.get("file_name",""))]
            matching.sort(key=lambda m: m.get("file_name",""))
            for m in matching:
                fn = m.get("file_name","")
                info = st.parse_series(fn)
                s = info["season"] if info else 1
                ep = info["episode"] if info else 1
                key = (s, ep)
                if key in seen_episodes: continue
                seen_episodes.add(key)
                videos.append({
                    "id": f"{id}:{s}:{ep}", "season": s, "episode": ep, "title": f"Episode {ep}",
                    "released": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(m.get("synced_at", time.time()))),
                })
            videos.sort(key=lambda x: (x["season"], x["episode"]))
            meta_obj["videos"] = videos
        return JSONResponse({"meta": meta_obj})
        
    prefix = "tgm:" if type == "movie" else "tgs:"
    clean  = id[len(prefix):] if id.startswith(prefix) else id
    movies = await st.load_movies(redis_client)
    
    if type == "movie":
        movie = movies.get(clean)
        if not movie: return JSONResponse({"meta": {}})
        fn = movie.get("file_name","Unknown")
        title, year = st.parse_title_year(fn)
        try:
            poster = await st.get_poster(redis_client, fn)
        except Exception as e:
            print(f"[meta] Poster fetch failed for {fn}: {e}")
            poster = "https://via.placeholder.com/300x450?text=No+Poster"
        return JSONResponse({"meta": {"id": id, "type": type, "name": title or fn, "year": year,
            "poster": poster, "description": fn, "posterShape": "poster"}})
    else:  # type == "series"
        matching_files = [m for m in movies.values() if st.show_id(m.get("file_name", "")) == clean]
        if not matching_files: return JSONResponse({"meta": {}})
        matching_files.sort(key=lambda m: m.get("file_name", ""))
        
        first_file = matching_files[0]
        fn = first_file.get("file_name", "Unknown")
        show_title = st.parse_show_title(fn)
        try:
            poster = await st.get_poster(redis_client, fn)
        except Exception as e:
            print(f"[meta] Poster fetch failed for {fn}: {e}")
            poster = "https://via.placeholder.com/300x450?text=No+Poster"
        year = ""
        for m in matching_files:
            _, y = st.parse_title_year(m.get("file_name", ""))
            if y:
                year = y
                break
                
        videos = []
        seen_episodes = set()
        for m in matching_files:
            m_fn = m.get("file_name", "")
            info = st.parse_series(m_fn)
            s = info["season"] if info else 1
            ep = info["episode"] if info else 1
            key = (s, ep)
            if key in seen_episodes: continue
            seen_episodes.add(key)
            
            vid = f"tgs:{clean}:{s}:{ep}"
            videos.append({
                "id": vid, "season": s, "episode": ep, "title": f"Episode {ep}",
                "released": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(m.get("synced_at", time.time()))),
            })
        videos.sort(key=lambda x: (x["season"], x["episode"]))
        
        return JSONResponse({"meta": {
            "id": id, "type": "series", "name": show_title, "year": year,
            "poster": poster, "description": f"Series: {show_title}", "posterShape": "poster", "videos": videos
        }})


@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    movies = await st.load_movies(redis_client)
    prefix = "tgm:" if type == "movie" else "tgs:"
    if id.startswith("tt"):
        parts   = id.split(":")
        imdb_id = parts[0]
        season  = int(parts[1]) if len(parts) > 1 else None
        episode = int(parts[2]) if len(parts) > 2 else None
        title, year = await st.get_cinemeta(type, imdb_id)
        if not title: return JSONResponse({"streams": []})
        streams = []
        for mid, m in movies.items():
            fn = m.get("file_name","")
            if not st.flex_match(title, fn): continue
            if year and type == "movie":
                try:
                    my = int(year)
                    if not any(str(my+d) in fn for d in (-1,0,1)): continue
                except Exception:
                    m4 = re.match(r"(\d{4})", year)
                    if m4:
                        if m4.group(1) not in fn: continue
                    elif year not in fn: continue
            if season and episode:
                info = st.parse_series(fn)
                if info and (info["season"]!=season or info["episode"]!=episode): continue
            try:
                fs = m.get("file_size")
                _schedule(_ensure_download(mid, fs, m["message_id"]))
            except Exception as e:
                print(f"[stream] warn: {e}")
            q,sz,src = m.get("quality","Unknown"),m.get("file_size_text","Unknown"),m.get("source","")
            cached = await _is_cached(mid)
            label = "TGStream ⚡" if cached else "TGStream"
            streams.append({"name":label,"title":f"{fn}\n{q}{' | '+src if src else ''} | {sz}","url":f"{BASE_URL}/proxy/{mid}"})
        return JSONResponse({"streams": streams})

    clean = id[len(prefix):] if id.startswith(prefix) else id
    
    if type == "series" and ":" in clean:
        parts = clean.split(":")
        sid = parts[0]
        try:
            season = int(parts[1])
            episode = int(parts[2])
        except Exception:
            return JSONResponse({"streams": []})
            
        streams = []
        for mid, m in movies.items():
            fn = m.get("file_name", "")
            if st.show_id(fn) != sid: continue
            info = st.parse_series(fn)
            s = info["season"] if info else 1
            ep = info["episode"] if info else 1
            if s == season and ep == episode:
                try:
                    fs = m.get("file_size")
                    _schedule(_ensure_download(mid, fs, m["message_id"]))
                except Exception as e:
                    print(f"[stream] warn: {e}")
                
                q   = m.get("quality","Unknown")
                sz  = m.get("file_size_text","Unknown")
                src = m.get("source","")
                cached = await _is_cached(mid)
                label = "TGStream ⚡" if cached else "TGStream"
                streams.append({
                    "name": label,
                    "title": f"{fn}\n{q}{' | '+src if src else ''} | {sz}",
                    "url": f"{BASE_URL}/proxy/{mid}"
                })
        return JSONResponse({"streams": streams})

    movie = movies.get(clean)
    if not movie: return JSONResponse({"streams": []})
    try:
        msg   = await _fetch_msg(movie["message_id"])
        media = msg.video or msg.document
        if not media:
            await st.del_movie(redis_client, clean)
            return JSONResponse({"streams": []})
        fs = movie.get("file_size") or media.file_size
        _schedule(_ensure_download(clean, fs, movie["message_id"]))
    except Exception as e:
        print(f"[stream] warn: {e}")
    fn  = movie.get("file_name","Unknown")
    q   = movie.get("quality","Unknown")
    sz  = movie.get("file_size_text","Unknown")
    src = movie.get("source","")
    cached = await _is_cached(clean)
    label = "TGStream ⚡" if cached else "TGStream"
    return JSONResponse({"streams": [{"name":label,
        "title":f"{fn}\n{q}{' | '+src if src else ''} | {sz}","url":f"{BASE_URL}/proxy/{clean}"}]})


@app.get("/subtitles/{type}/{id}.json")
async def subtitles(type: str, id: str):
    prefix = "tgm:" if type == "movie" else "tgs:"
    if not id.startswith(prefix):
        return JSONResponse({"subtitles": []})
    
    clean = id[len(prefix):]
    movies = await st.load_movies(redis_client)
    
    # Resolve file name
    filename = ""
    season, episode = None, None
    if type == "movie":
        movie = movies.get(clean)
        if movie:
            filename = movie.get("file_name", "")
    else:  # type == "series"
        # tgs:show_id:season:episode
        parts = clean.split(":")
        if len(parts) >= 3:
            sid = parts[0]
            try:
                season = int(parts[1])
                episode = int(parts[2])
            except Exception:
                pass
            for m in movies.values():
                if st.show_id(m.get("file_name", "")) == sid:
                    info = st.parse_series(m.get("file_name", ""))
                    if info and info["season"] == season and info["episode"] == episode:
                        filename = m.get("file_name", "")
                        break

    if not filename:
        return JSONResponse({"subtitles": []})
        
    # Get IMDB ID
    _, imdb_id = await st.get_poster_and_imdb(redis_client, filename)
    if not imdb_id:
        return JSONResponse({"subtitles": []})
        
    # Format OpenSubtitles request ID
    if type == "movie":
        os_id = imdb_id
    else:
        os_id = f"{imdb_id}:{season}:{episode}"
        
    # Query OpenSubtitles v3 addon
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"https://opensubtitles-v3.strem.io/subtitles/{type}/{os_id}.json")
            if r.status_code == 200:
                return JSONResponse(r.json())
    except Exception as e:
        print(f"[subtitles] failed to fetch from OpenSubtitles: {e}")
        
    return JSONResponse({"subtitles": []})


async def _ensure_download(movie_id: str, file_size: int, message_id: int):
    await download_manager.get_or_create(
        movie_id=movie_id, file_size=file_size, message_id=message_id,
        redis=redis_client, byte_streamer=byte_streamer, fetch_msg_fn=_fetch_msg,
        priority=True,
    )
    await download_manager.evict_lru_if_needed(redis_client)


async def _is_cached(movie_id: str) -> bool:
    done = await redis_client.get(f"tgstream:dl:done:{movie_id}")
    if done != b"1":
        return False
    sparse_path = STORAGE_DIR / f"{movie_id}.bin"
    return sparse_path.exists()


async def _yield_local_file(dl_file, start: int, length: int, request: Request):
    sent = 0
    while sent < length:
        if await request.is_disconnected():
            break
        size = min(LOCAL_READ_CHUNK, length - sent)
        data = await dl_file.pread(start + sent, size)
        if not data:
            break
        sent += len(data)
        yield data



async def _hydrate_if_cached(movie_id: str, file_size: int) -> bool:
    """
    Returns True if the file is fully downloaded locally and ready to serve.
    Side-effect: ensures download_manager._maps/_files are populated for this movie_id
    so proxy Path A can pread immediately.
    Never touches Telegram.
    """
    return await download_manager.hydrate_cached(movie_id, file_size, redis_client)

# ─── HYBRID PROXY — the heart of v2 ──────────────────────────────────────────
@app.api_route("/proxy/{movie_id}", methods=["GET", "HEAD"])
async def proxy(movie_id: str, request: Request):
    """
    Four-path resolution (in order):
      A. Range fully in local SparseFile  -> pread, instant
      B. Short wait for downloader catch-up -> pread if ready (aggressive with reduced timeout)
      C. Partial local prefix + live Telegram for remainder -> mixed stream (triggers when LOCAL_READY_BYTES ahead cached)
      D. Fully live Telegram MTProto       -> StreamingResponse fallback
    X-Source header reveals which path was used (visible in dev tools).
    """
    await metrics.record_proxy_request()

    movies = await st.load_movies(redis_client)
    movie  = movies.get(movie_id)
    if not movie: raise HTTPException(404, "Not found")

    file_size = movie.get("file_size")
    filename  = movie.get("file_name", "video.mp4")
    ctype_val = st.ctype(filename)

    if not file_size:
        try:
            msg       = await _fetch_msg(movie["message_id"])
            file_size = (msg.video or msg.document).file_size
        except Exception: raise HTTPException(502, "Telegram unavailable")

    etag = f'"{movie["message_id"]}-{file_size}"'

    if request.method == "HEAD":
        return Response(status_code=200, headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size), "Content-Type": ctype_val,
            "Cache-Control": "public, max-age=3600", "ETag": etag,
        })

    # ── Skip Telegram entirely if file already fully cached ─────────────────
    _cached = await _hydrate_if_cached(movie_id, file_size)
    if not _cached:
        _schedule(_ensure_download(movie_id, file_size, movie["message_id"]))

    # Parse Range
    start, end = 0, file_size - 1
    rh = request.headers.get("range", "")
    if rh.startswith("bytes="):
        spec = rh[6:]
        try:
            if "," in spec:
                raise ValueError("Multiple ranges are not supported")
            if spec.startswith("-"):
                suffix_len = int(spec[1:])
                if suffix_len <= 0:
                    raise ValueError("Invalid suffix range")
                start = max(0, file_size - suffix_len)
                end   = file_size - 1
            else:
                p = spec.split("-")
                if p[0]: start = int(p[0])
                if len(p) > 1 and p[1]: end = int(p[1])
        except Exception:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})
    end = min(end, file_size - 1)
    req_start = start
    req_end = end

    # Hint downloader — but ignore suffix-range probes (bytes=-N) and tiny
    # metadata reads near EOF; these are container/moov-atom probes, not
    # real playback position, and would wrongly drag the downloader to EOF.
    task    = download_manager.get(movie_id)
    dl_map  = download_manager.get_map(movie_id)
    dl_file = download_manager.get_file(movie_id)

    # Check cache status
    covered = dl_map.covered_prefix(req_start) if (dl_map and dl_file and dl_file.exists()) else 0

    # Path selection and capping
    use_path = None
    if covered > 0 and (req_start + covered - 1) >= req_end:
        # Path A: Range fully in local SparseFile
        use_path = "local"
        end = req_end
    elif covered > 0 and (req_start + covered - 1) >= req_end - SHORT_WAIT_GRACE_BYTES:
        # Path B: almost there, wait briefly then re-check
        if task:
            try:
                await asyncio.wait_for(task.progress_event().wait(), timeout=0.3)
            except asyncio.TimeoutError:
                pass
            covered = dl_map.covered_prefix(req_start)
            if covered > 0 and (req_start + covered - 1) >= req_end:
                use_path = "local"
                end = req_end
    if use_path is None and covered >= LOCAL_READY_BYTES:
        # Path C: Mixed local prefix + live Telegram tail
        use_path = "mixed"
        end = req_end
    if use_path is None:
        # Path D: Telegram live fallback. Cap open-ended requests to avoid rate limits/over-streaming.
        use_path = "telegram-live"
        if not rh:
            end = min(req_start + STARTUP_CHUNKS * TG_CHUNK - 1, req_end)
        elif rh.endswith("-"):
            end = min(req_start + STARTUP_CHUNKS * TG_CHUNK - 1, req_end)
        else:
            end = min(req_end, file_size - 1)

    if start < 0 or start >= file_size or end < start:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    total = end - start + 1

    _is_suffix_probe = rh.startswith("bytes=-")
    _is_tail_probe   = total <= 2 * 1024 * 1024 and start > file_size - (10 * 1024 * 1024)
    if task and not _is_suffix_probe and not _is_tail_probe:
        task.hint(start)

    headers = {
        "Accept-Ranges": "bytes", "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(total), "Content-Type": ctype_val,
        "Cache-Control": "public, max-age=3600", "ETag": etag, "Vary": "Range",
    }

    # ── Path A: fully local ───────────────────────────────────────────────────
    if use_path == "local":
        await metrics.record_stream_path("local")
        await metrics.record_cache_hit(total)
        return StreamingResponse(
            _yield_local_file(dl_file, start, total, request),
            status_code=206,
            headers={**headers, "X-Source": "local"},
            media_type=ctype_val,
        )

    # ── Path C: local prefix + live tail ────────────────────────────────────────
    if use_path == "mixed":
        await metrics.record_stream_path("mixed")
        await metrics.record_cache_hit(covered)
        await metrics.record_cache_miss(total - covered)

        rest_start = start + covered

        async def _mixed():
            async for chunk in _yield_local_file(dl_file, start, covered, request):
                yield chunk
            async with stream_sem:
                try: msg = await _fetch_msg(movie["message_id"])
                except Exception: return
                aligned   = (rest_start // TG_CHUNK) * TG_CHUNK
                first_cut = rest_start - aligned
                last_cut  = (end % TG_CHUNK) + 1
                parts     = math.ceil((end+1)/TG_CHUNK) - (aligned//TG_CHUNK)
                byte_streamer.mark_live_start(movie_id)
                try:
                    async for chunk in byte_streamer.yield_file(msg, aligned, first_cut, last_cut, parts):
                        if await request.is_disconnected(): break
                        yield chunk
                finally:
                    byte_streamer.mark_live_end(movie_id)

        return StreamingResponse(_mixed(), status_code=206,
                                 headers={**headers, "X-Source": "mixed"}, media_type=ctype_val)

    # ── Path D: fully live Telegram ───────────────────────────────────────────
    await metrics.record_stream_path("telegram-live")
    await metrics.record_cache_miss(total)

    try:
        msg = await _fetch_msg(movie["message_id"])
    except FloodWait as e:
        raise HTTPException(503, f"Rate limited — retry after {e.value}s")
    except Exception:
        raise HTTPException(502, "Telegram unavailable")

    if not (msg.video or msg.document):
        await st.del_movie(redis_client, movie_id)
        raise HTTPException(404, "Deleted from Telegram")

    aligned   = (start // TG_CHUNK) * TG_CHUNK
    first_cut = start - aligned
    last_cut  = (end % TG_CHUNK) + 1
    parts     = math.ceil((end+1)/TG_CHUNK) - (aligned//TG_CHUNK)

    async def _live():
        # No semaphore here — live proxy requests must never queue behind each other.
        # Pyrogram handles MTProto-level concurrency internally.
        byte_streamer.mark_live_start(movie_id)
        try:
            async for chunk in byte_streamer.yield_file(msg, aligned, first_cut, last_cut, parts):
                if await request.is_disconnected(): break
                yield chunk
        finally:
            byte_streamer.mark_live_end(movie_id)

    return StreamingResponse(_live(), status_code=206,
                             headers={**headers, "X-Source": "telegram-live"}, media_type=ctype_val)


# ── Media Control API Endpoints ───────────────────────────────────────────────
@app.post("/api/media/{movie_id}/download")
async def start_download_media(movie_id: str):
    movies = await st.load_movies(redis_client)
    movie = movies.get(movie_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found in index")
    
    file_size = movie.get("file_size")
    if not file_size:
        try:
            msg = await _fetch_msg(movie["message_id"])
            file_size = (msg.video or msg.document).file_size
        except Exception:
            raise HTTPException(status_code=502, detail="Telegram unavailable")
            
    await redis_client.delete(f"tgstream:dl:stopped:{movie_id}")
    _schedule(_ensure_download(movie_id, file_size, movie["message_id"]))
    return {"status": "ok"}


@app.post("/api/media/{movie_id}/pause")
async def pause_download_media(movie_id: str):
    task = download_manager.get(movie_id)
    if task:
        await redis_client.set(f"tgstream:dl:stopped:{movie_id}", "1", ex=86400)
        task.cancel()
        return {"status": "ok"}
    return {"status": "ignored"}


@app.post("/api/media/{movie_id}/evict")
async def evict_cache_media(movie_id: str):
    await redis_client.set(f"tgstream:dl:stopped:{movie_id}", "1", ex=86400)
    await download_manager.evict(movie_id, redis_client)
    deferred_notifications.pop(movie_id, None)  # #2: prevent leak on API eviction
    return {"status": "ok"}


@app.delete("/api/media/{movie_id}")
async def delete_media(movie_id: str, delete_tg: bool = False):
    movies = await st.load_movies(redis_client)
    movie = movies.get(movie_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found in index")
    
    # 1. Evict cache from downloader
    await download_manager.evict(movie_id, redis_client)
    deferred_notifications.pop(movie_id, None)  # #2: prevent leak on delete
    
    # 2. Optionally delete from Telegram
    if delete_tg:
        active_tg = get_tg()
        try:
            await active_tg.delete_messages(CHANNEL_USERNAME, [movie["message_id"]])
        except AuthKeyDuplicated as ae:
            print(f"[delete_media] AuthKeyDuplicated on client. Marking client broken: {ae}")
            client_pool.mark_broken_by_client(active_tg)
            await get_tg().delete_messages(CHANNEL_USERNAME, [movie["message_id"]])
        except Exception as e:
            print(f"[delete_media] failed to delete from Telegram: {e}")
            raise HTTPException(status_code=502, detail=f"Failed to delete from Telegram: {e}")
            
    # 3. Delete from index
    await st.del_movie(redis_client, movie_id)
    return {"status": "ok"}


# ── Configuration Endpoint ───────────────────────────────────────────────────
@app.get("/api/config")
async def api_config():
    manifest_url = f"{BASE_URL}/manifest.json"
    stremio_url  = manifest_url.replace("https://", "stremio://").replace("http://", "stremio://")
    return {
        "channel": str(CHANNEL_USERNAME),
        "manifest_url": manifest_url,
        "stremio_url": stremio_url
    }


# ── Monitoring Endpoints ─────────────────────────────────────────────────────
@app.get("/api/metrics")
async def get_metrics():
    """Get comprehensive metrics snapshot."""
    return metrics.get_stats()


@app.get("/api/metrics/rate-limits")
async def get_rate_limits():
    """Get detailed rate limit analytics."""
    now = time.time()
    hour_ago = now - 3600
    day_ago = now - 86400
    
    recent_hour = [e for e in metrics.rate_limit_events if e[0] > hour_ago]
    recent_day = [e for e in metrics.rate_limit_events if e[0] > day_ago]
    
    # Group by DC
    dc_stats = {}
    for ts, dc_id, wait_s in recent_day:
        if dc_id not in dc_stats:
            dc_stats[dc_id] = {"count": 0, "total_wait": 0, "max_wait": 0}
        dc_stats[dc_id]["count"] += 1
        dc_stats[dc_id]["total_wait"] += wait_s
        dc_stats[dc_id]["max_wait"] = max(dc_stats[dc_id]["max_wait"], wait_s)
    
    return {
        "hour": {
            "events": len(recent_hour),
            "total_wait_s": round(sum(e[2] for e in recent_hour), 1),
        },
        "day": {
            "events": len(recent_day),
            "total_wait_s": round(sum(e[2] for e in recent_day), 1),
            "avg_wait_s": round(sum(e[2] for e in recent_day) / max(1, len(recent_day)), 1),
        },
        "by_datacenter": {str(dc): stats for dc, stats in dc_stats.items()},
    }


@app.get("/api/metrics/cache")
async def get_cache_metrics():
    """Get cache performance metrics."""
    stats = metrics.get_stats()
    cache_stats = stats["cache"]
    total_requests = cache_stats["hits"] + cache_stats["misses"]
    
    # Estimate bandwidth saved
    bandwidth_saved_mb = cache_stats["bytes_cached"] / 1024 / 1024
    
    return {
        **cache_stats,
        "total_requests": total_requests,
        "bandwidth_saved_mb": round(bandwidth_saved_mb, 1),
        "avg_hit_size_kb": round(cache_stats["bytes_cached"] / max(1, cache_stats["hits"]) / 1024, 1),
    }


@app.get("/api/metrics/streaming")
async def get_streaming_metrics():
    """Get streaming path statistics."""
    stats = metrics.get_stats()
    return stats["streaming"]


@app.get("/api/metrics/health")
async def get_health_metrics():
    """Get system health indicators."""
    stats = metrics.get_stats()
    dl_stats = download_manager.stats()
    
    return {
        "http": stats["http"],
        "downloads": stats["downloads"],
        "rate_limit_pressure": {
            "events_per_hour": stats["rate_limits"]["recent_hour"],
            "avg_backoff_s": stats["rate_limits"]["avg_wait_s"],
        },
        "active_tasks": len(dl_stats),
        "memory_usage_estimate_mb": sum(
            s.get("size_on_disk_mb", 0) for s in dl_stats.values()
        ),
    }


@app.get("/api/metrics/export")
async def export_metrics():
    """Export metrics in Prometheus format."""
    stats = metrics.get_stats()
    lines = [
        "# HELP tgstream_rate_limit_events_total Total rate limit events",
        f"tgstream_rate_limit_events_total {stats['rate_limits']['total_events']}",
        "# HELP tgstream_rate_limit_wait_seconds Total time spent in rate limit backoff",
        f"tgstream_rate_limit_wait_seconds {stats['rate_limits']['total_wait_s']}",
        "# HELP tgstream_cache_hits_total Successful cache reads",
        f"tgstream_cache_hits_total {stats['cache']['hits']}",
        "# HELP tgstream_cache_misses_total Cache misses (fetched from Telegram)",
        f"tgstream_cache_misses_total {stats['cache']['misses']}",
        "# HELP tgstream_http_requests_total Total HTTP requests",
        f"tgstream_http_requests_total {stats['http']['total_requests']}",
        "# HELP tgstream_http_errors_total HTTP errors",
        f"tgstream_http_errors_total {stats['http']['errors']}",
        "# HELP tgstream_downloads_active Active download tasks",
        f"tgstream_downloads_active {stats['downloads']['active']}",
    ]
    return Response(content="\n".join(lines), media_type="text/plain")