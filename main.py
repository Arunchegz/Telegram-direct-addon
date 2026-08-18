"""
main.py — TGStream Hybrid Predictive Streamer
HuggingFace Space deployment (Docker SDK, port 7860).

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
import logging

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
from store import HybridStore as _HybridStore
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
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
from appstate import appstate
from sync import (  # noqa: E402
    _ensure_channel_reactions_enabled,
    _reconcile_reactions,
    _send_channel_reaction,
    _sync_channel,
    _sync_loop,
    configure as _sync_configure,
)
import hfbucket
from clients import pool as client_pool
from downloader import DownloadMap, download_manager, STORAGE_DIR, LOCAL_READY_BYTES, MAX_LOCAL_GB, find_cache_path, cache_path, R_DL_STOPPED
from streamer import ByteStreamer, TG_CHUNK
from metrics import metrics

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tgstream.main")

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
STREAM_CONCURRENCY = int(os.getenv("STREAM_CONCURRENCY", "3"))  # live proxy streams; keep low to avoid MTProto congestion
WAIT_TIMEOUT_S     = float(os.getenv("WAIT_TIMEOUT_S", "1.0"))  # Reduced from 2.0s for aggressive Path C
STARTUP_CHUNKS     = int(os.getenv("STARTUP_CHUNKS", "2"))  # 2 chunks × 1MB = 2MB initial fetch
LOCAL_READ_CHUNK   = int(os.getenv("LOCAL_READ_CHUNK", str(1024 * 1024)))  # Match TG_CHUNK for consistency
SHORT_WAIT_GRACE_BYTES = int(os.getenv("SHORT_WAIT_GRACE_BYTES", str(2 * 1024 * 1024)))  # 2MB grace window for Path B
DEBUG_PASSWORD     = os.getenv("DEBUG_PASSWORD", "")  # Password for /debug/* endpoints (if set)
# LOCAL_READY_BYTES imported from downloader (default 15MB)

# ── HuggingFace bucket (permanent private storage) ───────────────────────────
# When a file is fully cached and the bucket is configured, player requests are
# 302-redirected to a signed bucket URL — served from HF's CDN, zero Telegram
# cost, survives restarts/evictions. See hfbucket.py for the env surface.
HF_REDIRECT_DONE = os.getenv("HF_REDIRECT_DONE", "true").strip().lower() != "false"

# Bot / notification config (defined here so lifespan and all helpers can reference without forward refs)
BOT_TOKEN      = os.getenv("BOT_TOKEN", "").strip()        # from @BotFather
NOTIFY_CHAT_ID = os.getenv("NOTIFY_CHAT_ID", "").strip()   # channel/chat id, bot must be admin
TG_API_BASE    = os.getenv("TELEGRAM_API_URL", "https://api.telegram.org").strip().rstrip("/")
_TG_API        = f"{TG_API_BASE}/bot{BOT_TOKEN}"
DISABLE_BOT_LISTENER = os.getenv("DISABLE_BOT_LISTENER", "false").strip().lower() == "true"
ADMIN_USER_ID  = os.getenv("ADMIN_USER_ID", "").strip()  # Telegram user id allowed to issue /commands via DM
_START_TIME    = time.time()

appstate.source_chat_id: int | None = None

# ── In-process movie catalog cache ───────────────────────────────────────────
# load_movies() does HGETALL on every call. Proxy, stream, catalog, and meta
# endpoints all call it — that's one Redis round-trip per chunk request.
# A 30-second TTL snapshot eliminates the redundant round-trips while keeping
# catalog updates (sync, instant post, delete) visible within 30 seconds.
_movies_cache: dict = {}
_movies_cache_ts: float = 0.0
_MOVIES_CACHE_TTL = 30.0  # seconds
_movies_cache_lock = asyncio.Lock()


async def _get_movies() -> dict:
    global _movies_cache, _movies_cache_ts
    now = time.time()
    if now - _movies_cache_ts < _MOVIES_CACHE_TTL:
        return _movies_cache
    async with _movies_cache_lock:
        # Re-check inside lock — another coroutine may have refreshed while we waited
        if time.time() - _movies_cache_ts < _MOVIES_CACHE_TTL:
            return _movies_cache
        _movies_cache = await st.load_movies(appstate.redis_client)
        _movies_cache_ts = time.time()
    return _movies_cache


def _invalidate_movies_cache():
    """Call after any write to the movie index so next read is fresh."""
    global _movies_cache_ts
    _movies_cache_ts = 0.0


def get_tg() -> Client:
    return client_pool.primary()


# Lifespan-owned state (appstate.redis_client, appstate.byte_streamer, appstate.stream_sem, appstate.bot_client,
# appstate.source_chat_id) lives in appstate — see appstate.py.



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
        log.info(f"[task] {type(e).__name__}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    # Hybrid store: Redis for persistent state, local JSON for dl maps, in-memory for poster cache
    _redis = aioredis.from_url(
        REDIS_URL,
        decode_responses=False,
        max_connections=10,
        socket_connect_timeout=10,
        socket_keepalive=True,
        retry_on_timeout=True,
        health_check_interval=30,
    )
    appstate.redis_client = _HybridStore(_redis)
    await appstate.redis_client.load()
    appstate.stream_sem   = asyncio.Semaphore(STREAM_CONCURRENCY)

    await client_pool.start(API_ID, API_HASH, CHANNEL_USERNAME)

    if BOT_TOKEN:
        try:
            appstate.bot_client = Client(
                "notify_bot", api_id=API_ID, api_hash=API_HASH,
                bot_token=BOT_TOKEN, in_memory=True, no_updates=False,
                parse_mode=ParseMode.DISABLED,
            )
            await appstate.bot_client.start()
            log.info("[notify] bot_client started (MTProto, used for reliable notify send/edit)")

            if ADMIN_USER_ID:
                async def _pyro_admin_command(client, message):
                    if not message.from_user or str(message.from_user.id) != ADMIN_USER_ID:
                        return
                    text = message.text or ""
                    if text.startswith("/"):
                        try:
                            await _handle_admin_command(message.chat.id, text)
                        except Exception as e:
                            log.error(f"[bot] admin command failed: {type(e).__name__}: {e!r}")

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
                        log.error(f"[bot] callback failed: {type(e).__name__}: {e!r}")

                appstate.bot_client.add_handler(MessageHandler(_pyro_admin_command, filters.private & filters.text))
                appstate.bot_client.add_handler(CallbackQueryHandler(_pyro_admin_callback))
                log.info(f"[bot] MTProto admin command/callback handlers registered for user {ADMIN_USER_ID}")
        except Exception as e:
            log.error(f"[notify] bot_client failed to start, will fall back to HTTP API only: {type(e).__name__}: {e!r}")
            appstate.bot_client = None
        await _register_bot_commands()
    if CHANNEL_USERNAME:
        try:
            source_chat = await get_tg().get_chat(CHANNEL_USERNAME)
            appstate.source_chat_id = source_chat.id
            log.info(f"[listener] Resolved source channel id: {appstate.source_chat_id}")
        except Exception as e:
            log.error(f"[listener] failed to resolve source channel id for delete listener: {e}")

    # Register real-time Pyrogram update listener for instant prefetching on new channel posts
    async def _instant_sync_handler(client, message):
        log.info(f"[listener] Pyrogram new post detected ({message.id}) — instant sync")
        try:
            media = message.video or message.document
            if media:
                fn = getattr(media, "file_name", None)
                if fn:
                    mid = st.movie_id(fn)
                    movies = await _get_movies()
                    if mid not in movies:
                        log.info(f"[listener] instantly adding new movie to catalog: {mid}")
                        await st.save_movie(appstate.redis_client, mid, {
                            "message_id": message.id, "file_name": fn,
                            "file_size": media.file_size,
                            "file_size_text": st.fmt_size(media.file_size),
                            "quality": st.quality(fn), "source": st.source(fn),
                            "synced_at": int(time.time()),
                        })
                        _invalidate_movies_cache()
                        # React 👨‍💻 on the channel post to signal prefetch queued/pending
                        _schedule(_send_channel_reaction(message.id, "👨‍💻"))
                        # Auto-prefetch new posts immediately (matches sync handler policy)
                        stopped = await appstate.redis_client.get(R_DL_STOPPED.format(mid))
                        if stopped != b"1":
                            if not _queue_put(mid):
                                log.warning(f"[listener] prefetch_queue full, skipping {mid}")
            # Sync in background to reconcile index and clean up deletions
            _schedule(_sync_channel(force=False))
        except Exception as se:
            log.error(f"[listener] Pyrogram instant sync failed: {se}")

    async def _instant_delete_handler(client, update, users, chats):
        if not isinstance(update, UpdateDeleteChannelMessages):
            return
        if appstate.source_chat_id is None:
            return
        if _channel_update_chat_id(update) != appstate.source_chat_id:
            return
        try:
            removed = await _remove_deleted_messages(set(update.messages), "telegram channel delete update")
            if removed:
                await appstate.redis_client.set(st.R_SYNC_TS, str(time.time()))
        except Exception as se:
            log.error(f"[listener] instant delete cleanup failed: {se}")

    if CHANNEL_USERNAME:
        chat_filter = filters.chat(CHANNEL_USERNAME)
        media_filter = filters.video | filters.document
        get_tg().add_handler(MessageHandler(_instant_sync_handler, chat_filter & media_filter))
        get_tg().add_handler(RawUpdateHandler(_instant_delete_handler))
        log.info(f"[listener] Registered Pyrogram instant post/delete handlers for {CHANNEL_USERNAME}")

    appstate.byte_streamer = ByteStreamer(client_pool)
    download_manager.init_pool_size()
    download_manager.streamer = appstate.byte_streamer
    download_manager.on_alert = _notify_send
    download_manager.on_evict = lambda mid: deferred_notifications.pop(mid, None)  # sync callback; single atomic dict op is GIL-safe

    async def _on_download_complete(movie_id: str, message_id: int) -> None:
        """Fired by the downloader whenever a movie finishes caching —
        covers both prefetch and on-demand (play-triggered) downloads.
        Ensures the 👨‍💻 'downloading' reaction is replaced with ⚡."""
        try:
            movies = await _get_movies()
            m = movies.get(movie_id)
            msg_id = (m or {}).get("message_id") or message_id
            if msg_id:
                _schedule(_send_channel_reaction(msg_id, "⚡"))
            # Mirror the finished file into the HF bucket (permanent storage)
            # when the bucket is not mounted at STORAGE_DIR. On mounted
            # deployments the file already IS the bucket object.
            if m and hfbucket.configured() and not hfbucket.HF_BUCKET_MOUNTED:
                rel = _bucket_rel_path(movie_id, m.get("file_name"))
                if rel:
                    _schedule(_mirror_to_bucket(movie_id, rel))
        except Exception as e:
            log.error(f"[reaction] on_complete hook failed for {movie_id}: {type(e).__name__}: {e!r}")

    download_manager.on_complete = _on_download_complete
    client_pool.on_health_event = _notify_send
    log.info(f"Pyrogram pool started ({len(client_pool)} client(s))")
    if hfbucket.configured():
        log.info(f"[hfbucket] bucket={hfbucket.HF_BUCKET_ID} mounted={hfbucket.HF_BUCKET_MOUNTED} "
                 f"redirects={HF_REDIRECT_DONE} presign={'yes' if hfbucket.HF_S3_ACCESS_KEY and hfbucket.HF_S3_SECRET_KEY else 'no'}")
    else:
        log.info("[hfbucket] not configured (HF_BUCKET_ID unset) — bucket streaming disabled")

    _schedule(_sync_loop())
    for i in range(download_manager._max_concurrent_downloads):
        _schedule(_prefetch_worker(worker_id=i))
    _schedule(_bot_channel_listener())
    _schedule(_sweep_loop())
    _schedule(_ensure_channel_reactions_enabled())
    _schedule(_reconcile_reactions())


    yield
    await download_manager.shutdown()
    await client_pool.stop()
    if appstate.bot_client:
        await appstate.bot_client.stop()
    await appstate.redis_client.aclose()
    await st.close_http_client()


app = FastAPI(title="TGStream", version="2.0.0", lifespan=lifespan, docs_url="/api/docs")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET", "HEAD", "OPTIONS"], allow_headers=["*"])


@app.middleware("http")
async def _count_http_request(request: Request, call_next):
    """Feeds the /api/metrics 'http' block — record_http_request was defined
    but never called, leaving total_requests/errors permanently zero."""
    try:
        response = await call_next(request)
        await metrics.record_http_request(response.status_code < 500)
        return response
    except Exception:
        await metrics.record_http_request(False)
        raise


app.mount("/dashboard", StaticFiles(directory="static", html=True), name="dashboard")


async def _fetch_msg(msg_id: int, client: Client = None):
    c = client or get_tg()
    try:
        return await c.get_messages(CHANNEL_USERNAME, msg_id)
    except AuthKeyDuplicated as ae:
        log.error(f"[_fetch_msg] AuthKeyDuplicated on client. Suspending client: {ae}")
        client_pool.suspend_auth(c)
        alt_c = get_tg()
        if alt_c != c and alt_c is not None and not client_pool.is_bot(alt_c):
            return await alt_c.get_messages(CHANNEL_USERNAME, msg_id)
        raise ae


def _channel_update_chat_id(update: UpdateDeleteChannelMessages) -> int:
    return int(f"-100{update.channel_id}")


async def _remove_deleted_messages(message_ids: set[int], reason: str = "delete update") -> int:
    if not message_ids:
        return 0

    movies = await _get_movies()
    removed = []
    for mid, movie in movies.items():
        if int(movie.get("message_id", 0) or 0) in message_ids:
            removed.append((mid, movie.get("file_name", mid)))

    removed_ids = {mid for mid, _ in removed}
    for mid, file_name in removed:
        log.info(f"[delete-listener] removing {mid} ({file_name}) from index/cache: {reason}")
        # Mark explicitly stopped BEFORE evict so an in-flight prefetch worker
        # sees "stopped" (not "preempted") after its download task is cancelled
        # and does not requeue the deleted movie.
        await appstate.redis_client.set(R_DL_STOPPED.format(mid), "1")
        await st.del_movie(appstate.redis_client, mid)
        # Invalidate the index cache before releasing the download task, so a
        # concurrent worker cannot re-read a stale index still containing it.
        _invalidate_movies_cache()
        await download_manager.evict(mid, appstate.redis_client, file_name=file_name)
        await _deferred_pop(mid)
        # Remove the file from the HF bucket (permanent storage).
        _schedule(_delete_bucket_object(mid, file_name))
    # Drop the deleted movies from the prefetch queue (can't remove arbitrary
    # items from asyncio.Queue — drain and skip matching ids).
    drained = []
    while not prefetch_queue.empty():
        try:
            drained.append(prefetch_queue.get_nowait())
        except asyncio.QueueEmpty:
            break
    for qmid in drained:
        if qmid in removed_ids:
            _prefetch_queued.discard(qmid)
            log.info(f"[delete-listener] dropped {qmid} from prefetch queue")
        else:
            _queue_put(qmid)

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
            n1 = appstate.byte_streamer.prune_msg_cache()
            n2 = await download_manager.prune_finished_tasks()
            if n1 or n2:
                log.info(f"[sweep] pruned {n1} msg-cache entries, {n2} finished task entries")
        except Exception as e:
            log.error(f"[sweep] {e}")





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
    if appstate.bot_client and appstate.bot_client.is_connected:
        try:
            await appstate.bot_client.set_bot_commands([BotCommand(c, d) for c, d in commands])
            log.info("[bot] command menu registered (via bot_client)")
            return
        except Exception as e:
            log.error(f"[bot] set_bot_commands via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return
    try:
        c = st._get_http_client()
        r = await c.post(f"{_TG_API}/setMyCommands",
                          json={"commands": [{"command": c, "description": d} for c, d in commands]})
        data = r.json()
        if data.get("ok"):
            log.info("[bot] command menu registered")
        else:
            log.info(f"[bot] setMyCommands rejected: {data.get('description')}")
    except Exception as e:
        log.error(f"[bot] setMyCommands failed: {type(e).__name__}: {e!r}")


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
    if appstate.bot_client and appstate.bot_client.is_connected:
        try:
            msg = await appstate.bot_client.send_message(chat_id, text)
            return msg.id
        except Exception as pe:
            log.error(f"[notify] bot_client send failed, falling back to HTTP: {type(pe).__name__}: {pe!r}")

    # Fallback: HTTP Bot API
    if not BOT_TOKEN:
        return None
    try:
        c = st._get_http_client()
        r = await c.post(f"{_TG_API}/sendMessage",
                          json={"chat_id": NOTIFY_CHAT_ID, "text": text})
        return r.json().get("result", {}).get("message_id")
    except Exception as e:
        log.error(f"[notify] HTTP send failed: {type(e).__name__}: {e!r}")
        return None


async def _notify_edit(msg_id: int, text: str) -> float:
    """Returns 0 on success/harmless-no-op, or seconds to wait if rate-limited."""
    if not NOTIFY_CHAT_ID or not msg_id:
        return 0

    chat_id = _resolve_chat_id(NOTIFY_CHAT_ID)

    # Prefer MTProto via dedicated bot_client, same reasoning as _notify_send
    if appstate.bot_client and appstate.bot_client.is_connected:
        try:
            await appstate.bot_client.edit_message_text(chat_id, msg_id, text)
            return 0
        except FloodWait as fw:
            log.info(f"[notify] bot_client edit rate-limited, backing off {fw.value}s")
            return float(fw.value)
        except Exception as pe:
            desc = str(pe)
            if "MESSAGE_NOT_MODIFIED" in desc or "not modified" in desc.lower():
                return 0
            log.error(f"[notify] bot_client edit failed, falling back to HTTP: {type(pe).__name__}: {pe!r}")

    # Fallback: HTTP Bot API
    if not BOT_TOKEN:
        return 0
    try:
        c = st._get_http_client()
        r = await c.post(f"{_TG_API}/editMessageText",
                         json={"chat_id": NOTIFY_CHAT_ID, "message_id": msg_id, "text": text})
        data = r.json()
        if not data.get("ok"):
            desc = data.get("description", "")
            if data.get("error_code") == 429:
                wait = data.get("parameters", {}).get("retry_after", 3)
                log.info(f"[notify] HTTP rate-limited, backing off {wait}s")
                return float(wait)
            if "not modified" not in desc:
                log.info(f"[notify] HTTP edit rejected: {desc}")
        return 0
    except Exception as e:
        log.error(f"[notify] HTTP edit failed: {type(e).__name__}: {e!r}")
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
            if rate_limit_cooldown > 0:
                # Sleep the actual cooldown Telegram returned, not 1s at a time
                await asyncio.sleep(min(rate_limit_cooldown, 60))
                rate_limit_cooldown = 0.0
                continue

        await asyncio.sleep(1)


prefetch_queue: "asyncio.Queue[str]" = asyncio.Queue(maxsize=200)

# Mirror of what's currently in prefetch_queue. asyncio.Queue has no way to
# check membership without touching the private `._queue` deque, so we keep a
# parallel set, updated at every put/get. Single event loop: the operations
# below are not awaited, so the set stays consistent with the queue.
_prefetch_queued: set[str] = set()



def _queue_put(mid: str) -> bool:
    """put_nowait + track in _prefetch_queued. True if queued."""
    try:
        prefetch_queue.put_nowait(mid)
        _prefetch_queued.add(mid)
        return True
    except asyncio.QueueFull:
        return False


async def _bot_reply(chat_id, text: str):
    """Admin command replies — prefers MTProto bot_client (reliable on hosts
    where HTTPS to api.telegram.org is flaky/blocked), HTTP as fallback."""
    if appstate.bot_client and appstate.bot_client.is_connected:
        try:
            await appstate.bot_client.send_message(chat_id, text)
            return
        except Exception as e:
            log.error(f"[bot] reply via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return
    try:
        c = st._get_http_client()
        await c.post(f"{_TG_API}/sendMessage", json={"chat_id": chat_id, "text": text})
    except Exception as e:
        log.error(f"[bot] reply failed: {type(e).__name__}: {e!r}")


LIST_PAGE_SIZE = 8


def _short_id(mid: str) -> str:
    """Short stable hash for callback_data — movie_id itself is often
    too long for Telegram's 64-byte callback_data limit."""
    return hashlib.sha1(mid.encode()).hexdigest()[:10]


async def _render_list_page(page: int = 0):
    """Builds (text, inline_keyboard) for /list — cached symbol per movie,
    one delete button per row, prev/next nav."""
    movies = await _get_movies()
    items = sorted(movies.items(), key=lambda kv: kv[1].get("file_name", kv[0]))
    total = len(items)
    pages = max(1, math.ceil(total / LIST_PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    chunk = items[page * LIST_PAGE_SIZE: page * LIST_PAGE_SIZE + LIST_PAGE_SIZE]

    lines = [f"🎬 Catalog ({total}) — page {page+1}/{pages}", ""]
    keyboard = []
    # Batch all done-flag lookups in one mget round-trip instead of N GETs
    done_keys = [f"tgstream:dl:done:{mid}" for mid, _ in chunk]
    done_vals = await appstate.redis_client.mget(*done_keys) if done_keys else []
    for (mid, m), done_val in zip(chunk, done_vals):
        symbol = "✅" if done_val == b"1" else "⬜"
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
    if appstate.bot_client and appstate.bot_client.is_connected:
        try:
            msg = await appstate.bot_client.send_message(chat_id, text, reply_markup=_to_pyro_markup(keyboard))
            return msg.id
        except Exception as e:
            log.error(f"[bot] list send via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return None
    try:
        c = st._get_http_client()
        r = await c.post(f"{_TG_API}/sendMessage",
                          json={"chat_id": chat_id, "text": text, "reply_markup": keyboard})
        return r.json().get("result", {}).get("message_id")
    except Exception as e:
        log.error(f"[bot] list send failed: {type(e).__name__}: {e!r}")
        return None


async def _bot_edit_keyboard(chat_id, message_id, text: str, keyboard: dict):
    if appstate.bot_client and appstate.bot_client.is_connected:
        try:
            await appstate.bot_client.edit_message_text(chat_id, message_id, text, reply_markup=_to_pyro_markup(keyboard))
            return
        except Exception as e:
            desc = str(e)
            if "MESSAGE_NOT_MODIFIED" in desc:
                return
            log.error(f"[bot] list edit via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return
    try:
        c = st._get_http_client()
        await c.post(f"{_TG_API}/editMessageText",
                      json={"chat_id": chat_id, "message_id": message_id,
                            "text": text, "reply_markup": keyboard})
    except Exception as e:
        log.error(f"[bot] list edit failed: {type(e).__name__}: {e!r}")


async def _bot_answer_callback(callback_id: str, text: str | None = None):
    if appstate.bot_client and appstate.bot_client.is_connected:
        try:
            await appstate.bot_client.answer_callback_query(callback_id, text=text or "")
            return
        except Exception as e:
            log.error(f"[bot] answerCallbackQuery via bot_client failed, falling back to HTTP: {type(e).__name__}: {e!r}")
    if not BOT_TOKEN:
        return
    try:
        c = st._get_http_client()
        payload = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
        await c.post(f"{_TG_API}/answerCallbackQuery", json=payload)
    except Exception as e:
        log.error(f"[bot] answerCallbackQuery failed: {type(e).__name__}: {e!r}")


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
        movies = await _get_movies()
        target = next((mid for mid in movies if _short_id(mid) == short), None)
        if not target:
            await _bot_answer_callback(cq_id, "Not found (already deleted?)")
            text, kb = await _render_list_page(page)
            await _bot_edit_keyboard(chat_id, message_id, text, kb)
            return
        fn = movies[target].get("file_name", target)
        await download_manager.evict(target, appstate.redis_client, file_name=fn)
        await st.del_movie(appstate.redis_client, target)
        _invalidate_movies_cache()
        await _bot_answer_callback(cq_id, f"Deleted: {fn[:50]}")
        text, kb = await _render_list_page(page)
        await _bot_edit_keyboard(chat_id, message_id, text, kb)
        return

    await _bot_answer_callback(cq_id)


async def _handle_admin_command(chat_id, text: str):
    """Minimal remote control over DM. Only responds to ADMIN_USER_ID."""
    parts = text.strip().split(maxsplit=1)
    # Strip the @botname suffix so "/status@MyBot" matches "/status"
    cmd = parts[0].lower().split("@")[0]
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd == "/status":
        uptime_s = time.time() - _START_TIME
        uptime = f"{uptime_s/3600:.1f}h" if uptime_s > 3600 else f"{uptime_s/60:.0f}m"
        healthy = client_pool.healthy_count()
        stats = download_manager.stats()
        active = sum(1 for s in stats.values() if s["task_running"])
        total_local = sum(s["size_on_disk_mb"] for s in stats.values())
        movies = await _get_movies()
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
        movies = await _get_movies()
        if arg not in movies:
            await _bot_reply(chat_id, f"Not found: {arg}")
            return
        # Mark explicitly stopped so the prefetch worker doesn't instantly
        # requeue the movie after the task is cancelled (matches the API
        # /api/media/{id}/evict semantics).
        await appstate.redis_client.set(R_DL_STOPPED.format(arg), "1", ex=86400)
        await download_manager.evict(arg, appstate.redis_client, file_name=movies[arg].get("file_name"))
        await _bot_reply(chat_id, f"🗑 Evicted: {arg}")

    elif cmd == "/find":
        if not arg:
            await _bot_reply(chat_id, "Usage: /find <name>")
            return
        movies = await _get_movies()
        matches = [
            (mid, m) for mid, m in movies.items()
            if st.flex_match(arg, m.get("file_name", ""))
        ][:5]
        if not matches:
            await _bot_reply(chat_id, f"No matches for: {arg}")
            return
        lines = []
        for mid, m in matches:
            cached = await appstate.redis_client.get(f"tgstream:dl:done:{mid}")
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
        log.warning("[listener] DISABLE_BOT_LISTENER is true, skipping instant-post listener")
        return
    if not BOT_TOKEN:
        log.warning("[listener] BOT_TOKEN not set, skipping instant-post listener")
        return
    if appstate.bot_client and appstate.bot_client.is_connected:
        log.warning("[listener] MTProto bot_client is active, skipping HTTP long-poll updates listener")
        return
    if ADMIN_USER_ID:
        log.info(f"[listener] admin commands enabled for user {ADMIN_USER_ID}")
    else:
        log.info("[listener] ADMIN_USER_ID not set — /status /pause /resume /evict /find disabled")
    offset = 0
    async with httpx.AsyncClient(timeout=45) as poll_client:
        while True:
            try:
                r = await poll_client.get(f"{_TG_API}/getUpdates", params={
                    "offset": offset, "timeout": 30,
                    "allowed_updates": '["channel_post","message","callback_query"]',
                })
                if r.status_code != 200:
                    log.error(f"[listener] HTTP error {r.status_code}: {r.text[:200]}")
                    await asyncio.sleep(15)
                    continue

                try:
                    data = r.json()
                except Exception as je:
                    log.error(f"[listener] JSON decode failed: {je}. Response: {r.text[:200]}")
                    await asyncio.sleep(15)
                    continue

                if not data.get("ok"):
                    desc = data.get("description", "Unknown error")
                    err_code = data.get("error_code")
                    log.error(f"[listener] Telegram error {err_code}: {desc}")
                    await asyncio.sleep(15)
                    continue

                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    post = upd.get("channel_post")
                    if post and (post.get("video") or post.get("document")):
                        log.info("[listener] new channel post detected — instant sync")
                        try:
                            # force=False: respect SYNC_INTERVAL cooldown.
                            # This path only active when appstate.bot_client is absent
                            # (MTProto _instant_sync_handler handles the real-time
                            # catalog update when appstate.bot_client is present).
                            await _sync_channel(force=False)
                        except Exception as e:
                            log.error(f"[listener] sync failed: {e}")
                        continue

                    dm = upd.get("message")
                    if dm and ADMIN_USER_ID:
                        sender_id = str(dm.get("from", {}).get("id", ""))
                        text = dm.get("text", "")
                        if sender_id == ADMIN_USER_ID and text.startswith("/"):
                            try:
                                await _handle_admin_command(dm["chat"]["id"], text)
                            except Exception as e:
                                log.error(f"[listener] admin command failed: {e}")
                        continue

                    cq = upd.get("callback_query")
                    if cq and ADMIN_USER_ID:
                        sender_id = str(cq.get("from", {}).get("id", ""))
                        if sender_id == ADMIN_USER_ID:
                            try:
                                await _handle_admin_callback(cq)
                            except Exception as e:
                                log.error(f"[listener] callback failed: {e}")
            except Exception as e:
                log.error(f"[listener] poll error ({type(e).__name__}): {repr(e)}")
                if not isinstance(e, (httpx.TimeoutException, httpx.NetworkError)):
                    traceback.print_exc()
                await asyncio.sleep(10)


deferred_notifications = {}
_deferred_lock = asyncio.Lock()


async def _deferred_pop(movie_id: str, default=None):
    """Pop a deferred notification id under the lock — mutating this dict
    across awaits (workers, evict, delete) must not interleave."""
    async with _deferred_lock:
        return deferred_notifications.pop(movie_id, default)


async def _deferred_set(movie_id: str, msg_id: int) -> None:
    async with _deferred_lock:
        deferred_notifications[movie_id] = msg_id


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
        _prefetch_queued.discard(movie_id)
        reporter = None  # must be defined before try so except/finally can always cancel it
        try:
            if download_manager.paused:
                log.info(f"[prefetch:{worker_id}] paused, requeueing {movie_id}")
                await asyncio.sleep(15)
                if not _queue_put(movie_id):
                    log.info(f"[prefetch:{worker_id}] prefetch_queue full, dropping {movie_id} on pause requeue")
                continue
            movies = await _get_movies()
            m = movies.get(movie_id)
            if not m:
                continue
            fn = m.get("file_name", movie_id)
            file_size = m.get("file_size") or 0
            message_id = m.get("message_id")
            if not file_size or not message_id:
                log.warning(f"[prefetch:{worker_id}] skipping {movie_id}: missing file_size or message_id in index")
                continue
            log.info(f"[prefetch:{worker_id}] starting {movie_id} ({fn})")

            # Start download task first so it starts instantly
            task = await download_manager.get_or_create(
                movie_id=movie_id,
                file_size=file_size,
                message_id=message_id,
                redis=appstate.redis_client,
                byte_streamer=appstate.byte_streamer,
                fetch_msg_fn=_fetch_msg,
                priority=False,
                file_name=fn,
            )

            # Send notification afterwards (if task successfully started)
            msg_id = await _deferred_pop(movie_id)
            if task and task._task:
                if msg_id:
                    await _notify_edit(msg_id, f"⬇️ Prefetching: {fn}\n0/100")
                else:
                    msg_id = await _notify_send(f"⬇️ Prefetching: {fn}\n0/100")
                
                reporter = asyncio.create_task(
                    _progress_reporter(movie_id, fn, file_size, msg_id)
                )
                try:
                    await task._task  # wait till done/cancelled/evicted before next queued item
                finally:
                    reporter.cancel()
                # Cancelled mid-way by a priority (play) request? -> not really
                # done, put back at the end of the queue to finish later.
                # But if the user explicitly paused/evicted it from the
                # dashboard, respect that instead of instantly restarting.
                done_val = await appstate.redis_client.get(f"tgstream:dl:done:{movie_id}")
                stopped = await appstate.redis_client.get(R_DL_STOPPED.format(movie_id))
                if done_val == b"1":
                    await _notify_edit(msg_id, f"✅ Prefetched: {fn}\n100/100\n{BASE_URL}/proxy/{movie_id}")
                    # Replace 👨💻 with ⚡ on the channel post to signal download complete
                    _schedule(_send_channel_reaction(message_id, "⚡"))
                elif stopped == b"1":
                    log.info(f"[prefetch:{worker_id}] {movie_id} explicitly stopped, not requeueing")
                    await _notify_edit(msg_id, f"⏸ Paused: {fn}")
                else:
                    log.info(f"[prefetch:{worker_id}] {movie_id} preempted, requeueing")
                    if not _queue_put(movie_id):
                        log.info(f"[prefetch:{worker_id}] prefetch_queue full, dropping {movie_id} on preempt requeue")
            else:
                done_val = await appstate.redis_client.get(f"tgstream:dl:done:{movie_id}")
                if done_val == b"1":
                    ready_text = f"✅ Already cached: {fn}\n{BASE_URL}/proxy/{movie_id}"
                    if msg_id:
                        await _notify_edit(msg_id, ready_text)
                    else:
                        await _notify_send(ready_text)
                    # Already cached — fire ⚡ reaction (covers restart-recovery path)
                    _schedule(_send_channel_reaction(message_id, "⚡"))
                else:
                    # another download (priority or another prefetch) is
                    # active right now — wait a bit, then retry
                    log.info(f"[prefetch:{worker_id}] {movie_id} deferred, another download active")
                    if msg_id:
                        await _notify_edit(msg_id, f"⏳ Waiting to prefetch: {fn}\n(Another download is currently active)")
                    else:
                        msg_id = await _notify_send(f"⏳ Waiting to prefetch: {fn}\n(Another download is currently active)")
                    if msg_id:
                        await _deferred_set(movie_id, msg_id)
                    await asyncio.sleep(15)
                    if not _queue_put(movie_id):
                        log.info(f"[prefetch:{worker_id}] prefetch_queue full, dropping {movie_id} on deferred requeue")
            log.info(f"[prefetch:{worker_id}] finished {movie_id}")
        except Exception as e:
            log.error(f"[prefetch:{worker_id}] {movie_id} failed: {e}")
            if reporter is not None and not reporter.done():
                reporter.cancel()
        finally:
            prefetch_queue.task_done()




MANIFEST = {
    "id": "org.tgstream.hybrid", "version": "2.0.0", "name": "TGStream",
    "description": "Hybrid predictive streaming from Telegram via Stremio",
    "resources": ["catalog", "meta", "stream", "subtitles"], "types": ["movie", "series"],
    "idPrefixes": ["tgm:", "tgs:", "tt"],
    "catalogs": [
        {"type": "movie",  "id": "tgstream_movies", "name": "TG Movies"},
        {"type": "series", "id": "tgstream_series", "name": "TG Series"},
    ],
    "behaviorHints": {"configurable": False, "configurationRequired": False},
}


@app.get("/")
async def health():
    movies = await _get_movies()
    last   = await appstate.redis_client.get(st.R_SYNC_TS)
    age    = round((time.time() - float(last)) / 60, 1) if last else None
    dl     = download_manager.stats()
    return {"status": "ok", "movies": len(movies), "channel": CHANNEL_USERNAME,
            "sync_age_min": age, "active_downloads": len(dl), "download_stats": dl}


@app.get("/manifest.json")
async def manifest(): return JSONResponse(MANIFEST)


@app.get("/sync")
async def manual_sync(request: Request):
    await _debug_auth(request)
    try:
        return {"synced": await _sync_channel(force=True)}
    except AuthKeyDuplicated:
        log.error("[sync] Retrying manual sync after marking previous client broken")
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
    movies = await _get_movies()
    result = {}
    for mid, m in movies.items():
        # Shallow copy — never mutate the shared cache dict
        entry = dict(m)
        task = download_manager.get(mid)
        dl_map = download_manager.get_map(mid)
        if not dl_map:
            dl_map = await download_manager._load_map(mid, appstate.redis_client)

        file_path = find_cache_path(mid, entry.get("file_name"))
        exists = file_path.exists()

        cached_bytes = dl_map.total_bytes() if exists else 0
        entry["cached_bytes"] = cached_bytes
        entry["cached_text"] = st.fmt_size(cached_bytes)

        fs = entry.get("file_size", 0)
        entry["pct"] = round(cached_bytes / fs * 100, 1) if fs and exists else 0

        is_done = False
        if exists:
            done_val = await appstate.redis_client.get(f"tgstream:dl:done:{mid}")
            is_done = done_val == b"1" or cached_bytes >= fs

        entry["is_done"] = is_done
        entry["is_active"] = bool(task and task._task and not task._task.done())
        result[mid] = entry
    return result


@app.get("/debug/downloads")
async def debug_downloads(request: Request):
    await _debug_auth(request)
    stats  = download_manager.stats()
    movies = await _get_movies()
    for mid, s in stats.items():
        movie = movies.get(mid, {})
        fs    = movie.get("file_size", 0)
        s["total_mb"]   = round(fs / 1024 / 1024, 1) if fs else 0
        s["pct_done"]   = round(s["downloaded_mb"] / s["total_mb"] * 100, 1) if s.get("total_mb") else 0
        s["file_name"]  = movie.get("file_name", mid)
    return stats


@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    movies = await _get_movies()
    def is_series(m): return bool(st.IS_SERIES_RE.search(m.get("file_name","")))

    # Semaphore limits concurrent Redis/HTTP calls — all items fly in parallel
    # within the concurrency cap instead of serialising batch-by-batch.
    _catalog_sem = asyncio.Semaphore(5)

    if type == "movie":
        filtered = {mid: m for mid, m in movies.items() if not is_series(m)}
        async def build(mid, m):
            async with _catalog_sem:
                fn = m.get("file_name","Unknown")
                try:
                    poster, imdb_id = await st.get_poster_and_imdb(appstate.redis_client, fn)
                except Exception as e:
                    log.error(f"[catalog] Poster fetch failed for {fn}: {e}")
                    poster, imdb_id = st._local_placeholder_poster(fn), ""
                title, year = st.parse_title_year(fn)
                meta = {"id": f"tgm:{mid}", "type": "movie", "name": title or fn,
                        "poster": poster, "posterShape": "poster", "year": year}
                if imdb_id:
                    meta["imdb_id"] = imdb_id
                return meta
        results = await asyncio.gather(*[build(mid, m) for mid, m in filtered.items()], return_exceptions=True)
        metas = [r for r in results if not isinstance(r, Exception)]
        for r in results:
            if isinstance(r, Exception):
                log.error(f"[catalog] Build failed: {r}")
        return JSONResponse({"metas": metas}, headers={"Cache-Control": "no-store"})

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
            async with _catalog_sem:
                fn = group["files"][0][1].get("file_name","Unknown")
                try:
                    poster, imdb_id = await st.get_poster_and_imdb(appstate.redis_client, fn)
                except Exception as e:
                    log.error(f"[catalog] Poster fetch failed for {fn}: {e}")
                    poster, imdb_id = st._local_placeholder_poster(fn), ""
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
        results = await asyncio.gather(*[build_series(sid, group) for sid, group in series_groups.items()], return_exceptions=True)
        metas = [r for r in results if not isinstance(r, Exception)]
        for r in results:
            if isinstance(r, Exception):
                log.error(f"[catalog] Build failed: {r}")
        return JSONResponse({"metas": metas}, headers={"Cache-Control": "no-store"})


@app.get("/meta/{type}/{id}.json")
async def meta(type: str, id: str):
    if id.startswith("tt"):
        title, year = await st.get_cinemeta(type, id)
        meta_obj = {"id": id, "type": type, "name": title, "year": year}
        if type == "series" and title:
            movies = await _get_movies()
            videos = []
            seen_episodes = set()
            
            # Use VideoMatcher for scoring
            scored_files = []
            for m in movies.values():
                fn = m.get("file_name", "")
                # Parse SE from file for the video object
                s, ep = st.parse_season_episode(fn)
                s = s if s is not None else 1
                ep = ep if ep is not None else 1
                
                # Calculate score
                score = st.VideoMatcher.calculate_match_score(fn, title, year, s, ep)
                if score >= st.VideoMatcher.DEFAULT_THRESHOLD:
                    scored_files.append((score, m, s, ep))
            
            # Sort by score descending
            scored_files.sort(key=lambda x: x[0], reverse=True)
            
            for score, m, s, ep in scored_files:
                key = (s, ep)
                if key in seen_episodes: 
                    continue
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
    movies = await _get_movies()
    
    if type == "movie":
        movie = movies.get(clean)
        if not movie: return JSONResponse({"meta": {}})
        fn = movie.get("file_name","Unknown")
        title, year = st.parse_title_year(fn)
        try:
            poster = await st.get_poster(appstate.redis_client, fn)
        except Exception as e:
            log.error(f"[meta] Poster fetch failed for {fn}: {e}")
            poster = st._local_placeholder_poster(fn)
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
            poster = await st.get_poster(appstate.redis_client, fn)
        except Exception as e:
            log.error(f"[meta] Poster fetch failed for {fn}: {e}")
            poster = st._local_placeholder_poster(fn)
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
            s, ep = st.parse_season_episode(m_fn)
            s = s if s is not None else 1
            ep = ep if ep is not None else 1
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
    movies = await _get_movies()
    prefix = "tgm:" if type == "movie" else "tgs:"
    if id.startswith("tt"):
        parts   = id.split(":")
        imdb_id = parts[0]
        season  = int(parts[1]) if len(parts) > 1 else None
        episode = int(parts[2]) if len(parts) > 2 else None
        title, year = await st.get_cinemeta(type, imdb_id)
        if not title: return JSONResponse({"streams": []})
        streams = []
        
        # Use VideoMatcher for scoring
        scored_files = []
        for mid, m in movies.items():
            fn = m.get("file_name","")
            # Calculate score
            score = st.VideoMatcher.calculate_match_score(fn, title, year, season, episode)
            if score >= st.VideoMatcher.DEFAULT_THRESHOLD:
                scored_files.append((score, mid, m))
        
        # Sort by score descending
        scored_files.sort(key=lambda x: x[0], reverse=True)

        for score, mid, m in scored_files:
            fn = m.get("file_name","")
            try:
                fs = m.get("file_size") or 0
                if fs:
                    _schedule(_ensure_download(mid, fs, m["message_id"], m.get("file_name")))
            except Exception as e:
                log.warning(f"[stream] warn: {e}")
            q,sz,src = m.get("quality","Unknown"),m.get("file_size_text","Unknown"),m.get("source","")
            cached = await _is_cached(mid, m.get("file_name"))
            label = "TGStream ⚡" if cached else "TGStream"
            streams.append({"name":label,"title":f"{fn}\n{q}{' | '+src if src else ''} | {sz}","url":f"{BASE_URL}/proxy/{mid}","behaviorHints":{"notWebReady":False}})
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
            s, ep = st.parse_season_episode(fn)
            s = s if s is not None else 1
            ep = ep if ep is not None else 1
            if s == season and ep == episode:
                try:
                    fs = m.get("file_size") or 0
                    if fs:
                        _schedule(_ensure_download(mid, fs, m["message_id"], m.get("file_name")))
                except Exception as e:
                    log.warning(f"[stream] warn: {e}")
                
                q   = m.get("quality","Unknown")
                sz  = m.get("file_size_text","Unknown")
                src = m.get("source","")
                cached = await _is_cached(mid, m.get("file_name"))
                label = "TGStream ⚡" if cached else "TGStream"
                streams.append({
                    "name": label,
                    "title": f"{fn}\n{q}{' | '+src if src else ''} | {sz}",
                    "url": f"{BASE_URL}/proxy/{mid}",
                    "behaviorHints": {"notWebReady": False}
                })
        return JSONResponse({"streams": streams})

    movie = movies.get(clean)
    if not movie: return JSONResponse({"streams": []})
    try:
        msg   = await _fetch_msg(movie["message_id"])
        media = msg.video or msg.document
        if not media:
            await st.del_movie(appstate.redis_client, clean)
            _invalidate_movies_cache()
            return JSONResponse({"streams": []})
        fs = movie.get("file_size") or media.file_size
        _schedule(_ensure_download(clean, fs, movie["message_id"], movie.get("file_name")))
    except Exception as e:
        log.warning(f"[stream] warn: {e}")
    fn  = movie.get("file_name","Unknown")
    q   = movie.get("quality","Unknown")
    sz  = movie.get("file_size_text","Unknown")
    src = movie.get("source","")
    cached = await _is_cached(clean, movie.get("file_name"))
    label = "TGStream ⚡" if cached else "TGStream"
    return JSONResponse({"streams": [{"name":label,
        "title":f"{fn}\n{q}{' | '+src if src else ''} | {sz}","url":f"{BASE_URL}/proxy/{clean}",
        "behaviorHints":{"notWebReady":False}}]})



@app.get("/subtitles/{type}/{id}.json")
async def subtitles(type: str, id: str):
    prefix = "tgm:" if type == "movie" else "tgs:"
    if not id.startswith(prefix):
        return JSONResponse({"subtitles": []})
    
    clean = id[len(prefix):]
    movies = await _get_movies()
    
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
                    s, ep = st.parse_season_episode(m.get("file_name", ""))
                    if s == season and ep == episode:
                        filename = m.get("file_name", "")
                        break

    if not filename:
        return JSONResponse({"subtitles": []})
        
    # Get IMDB ID
    _, imdb_id = await st.get_poster_and_imdb(appstate.redis_client, filename)
    if not imdb_id:
        return JSONResponse({"subtitles": []})
        
    # Format OpenSubtitles request ID
    if type == "movie":
        os_id = imdb_id
    else:
        os_id = f"{imdb_id}:{season}:{episode}"
        
    # Query OpenSubtitles v3 addon
    try:
        client = st._get_http_client()
        r = await client.get(f"https://opensubtitles-v3.strem.io/subtitles/{type}/{os_id}.json")
        if r.status_code == 200:
            return JSONResponse(r.json())
    except Exception as e:
        log.error(f"[subtitles] failed to fetch from OpenSubtitles: {e}")
        
    return JSONResponse({"subtitles": []})


async def _ensure_download(movie_id: str, file_size: int, message_id: int, file_name: str = None):
    # A play request overrides a previous explicit pause/evict.
    await appstate.redis_client.delete(R_DL_STOPPED.format(movie_id))
    await download_manager.get_or_create(
        movie_id=movie_id, file_size=file_size, message_id=message_id,
        redis=appstate.redis_client, byte_streamer=appstate.byte_streamer, fetch_msg_fn=_fetch_msg,
        priority=True, file_name=file_name,
    )
    await download_manager.evict_lru_if_needed(appstate.redis_client)


async def _is_cached(movie_id: str, file_name: str = None) -> bool:
    done = await appstate.redis_client.get(f"tgstream:dl:done:{movie_id}")
    if done != b"1":
        return False
    sparse_path = find_cache_path(movie_id, file_name)
    return sparse_path.exists()


async def _yield_local_file(dl_file, start: int, length: int, request: Request):
    sent = 0
    while sent < length:
        if await request.is_disconnected():
            break
        size = min(LOCAL_READ_CHUNK, length - sent)
        try:
            data = await dl_file.pread(start + sent, size)
        except OSError:
            # File evicted/unlinked mid-stream (LRU eviction, delete) —
            # end the stream cleanly instead of aborting the connection.
            break
        if not data:
            break
        sent += len(data)
        yield data



async def _hydrate_if_cached(movie_id: str, file_size: int, file_name: str = None) -> bool:
    """
    Returns True if the file is fully downloaded locally and ready to serve.
    Side-effect: ensures download_manager._maps/_files are populated for this movie_id
    so proxy Path A can pread immediately.
    Never touches Telegram.
    """
    return await download_manager.hydrate_cached(movie_id, file_size, appstate.redis_client, file_name)


def _bucket_rel_path(movie_id: str, file_name: str | None = None) -> str | None:
    """Object path (relative to STORAGE_DIR) used as the bucket key."""
    try:
        return cache_path(movie_id, file_name).relative_to(STORAGE_DIR).as_posix()
    except ValueError:
        return None


async def _mirror_to_bucket(movie_id: str, rel: str) -> None:
    """Mirror a fully cached file into the HF bucket (permanent storage).
    No-op when the bucket is mounted at STORAGE_DIR or unconfigured."""
    try:
        local = STORAGE_DIR / rel
        if local.is_file():
            await hfbucket.upload_file(str(local), rel)
    except Exception as e:
        log.error(f"[hfbucket] mirror hook failed for {movie_id}: {e}")


async def _delete_bucket_object(movie_id: str, file_name: str | None) -> None:
    """Best-effort removal of a movie's bucket object (user-triggered evictions
    and deletes). LRU evictions skip this on purpose — the bucket is the
    permanent copy."""
    rel = _bucket_rel_path(movie_id, file_name)
    if rel:
        await hfbucket.delete_object(rel)

# ─── HYBRID PROXY — the heart of v2 ──────────────────────────────────────────
@app.api_route("/proxy/{movie_id}", methods=["GET", "HEAD"], operation_id="proxy_movie")
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

    movies = await _get_movies()
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
    _cached = await _hydrate_if_cached(movie_id, file_size, filename)

    # ── HuggingFace bucket: serve completed files from permanent storage ─────
    # 302 the player straight to the (signed) bucket URL so bytes come from
    # HF's CDN — zero Telegram cost, survives restarts and local evictions.
    # When the object isn't available yet, fall through to the local proxy.
    if HF_REDIRECT_DONE and hfbucket.configured():
        rel = _bucket_rel_path(movie_id, filename)
        if rel:
            bucket_url = await hfbucket.try_redirect(rel)
            if bucket_url:
                await metrics.record_stream_path("bucket")
                return RedirectResponse(bucket_url, status_code=302)

    if not _cached:
        _schedule(_ensure_download(movie_id, file_size, movie["message_id"], filename))

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
    waited = False  # set when Path B's short wait converted to local
    if covered > 0 and (req_start + covered - 1) >= req_end:
        # Path A: Range fully in local SparseFile
        use_path = "local"
        end = req_end
    elif covered > 0 and (req_start + covered - 1) >= req_end - SHORT_WAIT_GRACE_BYTES:
        # Path B: almost there, wait briefly then re-check. A done task will
        # never fire its progress event again — skip the wasted timeout wait.
        if task and not task.is_done():
            try:
                await asyncio.wait_for(task.progress_event().wait(), timeout=WAIT_TIMEOUT_S)
            except asyncio.TimeoutError:
                pass
            # Task/map/file may have been replaced (preempted, evicted, restarted)
            # while we waited — refresh before re-checking coverage.
            dl_map  = download_manager.get_map(movie_id)
            dl_file = download_manager.get_file(movie_id)
            covered = dl_map.covered_prefix(req_start) if (dl_map and dl_file and dl_file.exists()) else 0
            if covered > 0 and (req_start + covered - 1) >= req_end:
                use_path = "local"
                end = req_end
                waited = True
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
        await metrics.record_stream_path("local-waited" if waited else "local")
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
            async with appstate.stream_sem:
                try: msg = await _fetch_msg(movie["message_id"])
                except Exception: return
                aligned   = (rest_start // TG_CHUNK) * TG_CHUNK
                first_cut = rest_start - aligned
                last_cut  = (end % TG_CHUNK) + 1
                parts     = math.ceil((end+1)/TG_CHUNK) - (aligned//TG_CHUNK)
                appstate.byte_streamer.mark_live_start(movie_id)
                try:
                    async for chunk in appstate.byte_streamer.yield_file(msg, aligned, first_cut, last_cut, parts):
                        if await request.is_disconnected(): break
                        yield chunk
                finally:
                    appstate.byte_streamer.mark_live_end(movie_id)

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
        await st.del_movie(appstate.redis_client, movie_id)
        _invalidate_movies_cache()
        raise HTTPException(404, "Deleted from Telegram")

    aligned   = (start // TG_CHUNK) * TG_CHUNK
    first_cut = start - aligned
    last_cut  = (end % TG_CHUNK) + 1
    parts     = math.ceil((end+1)/TG_CHUNK) - (aligned//TG_CHUNK)

    async def _live():
        # No semaphore here — live proxy requests must never queue behind each other.
        # Pyrogram handles MTProto-level concurrency internally.
        appstate.byte_streamer.mark_live_start(movie_id)
        try:
            async for chunk in appstate.byte_streamer.yield_file(msg, aligned, first_cut, last_cut, parts):
                if await request.is_disconnected(): break
                yield chunk
        finally:
            appstate.byte_streamer.mark_live_end(movie_id)

    return StreamingResponse(_live(), status_code=206,
                             headers={**headers, "X-Source": "telegram-live"}, media_type=ctype_val)


# ── Media Control API Endpoints ───────────────────────────────────────────────

def _check_api_auth(x_api_key: str | None):
    """Raise 401 if DEBUG_PASSWORD is set and the X-Api-Key header doesn't match.
    When DEBUG_PASSWORD is unset the endpoints are open (backwards-compatible)."""
    if DEBUG_PASSWORD and x_api_key != DEBUG_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.post("/api/media/{movie_id}/download")
async def start_download_media(movie_id: str, x_api_key: str | None = Header(default=None)):
    _check_api_auth(x_api_key)
    movies = await _get_movies()
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

    await appstate.redis_client.delete(R_DL_STOPPED.format(movie_id))
    _schedule(_ensure_download(movie_id, file_size, movie["message_id"], movie.get("file_name")))
    return {"status": "ok"}


@app.post("/api/media/{movie_id}/pause")
async def pause_download_media(movie_id: str, x_api_key: str | None = Header(default=None)):
    _check_api_auth(x_api_key)
    task = download_manager.get(movie_id)
    if task:
        await appstate.redis_client.set(R_DL_STOPPED.format(movie_id), "1", ex=86400)
        task.cancel()
        return {"status": "ok"}
    return {"status": "ignored"}


@app.post("/api/media/{movie_id}/evict")
async def evict_cache_media(movie_id: str, x_api_key: str | None = Header(default=None)):
    _check_api_auth(x_api_key)
    await appstate.redis_client.set(R_DL_STOPPED.format(movie_id), "1", ex=86400)
    movies = await _get_movies()
    await download_manager.evict(movie_id, appstate.redis_client,
                                 file_name=movies.get(movie_id, {}).get("file_name"))
    await _deferred_pop(movie_id)  # #2: prevent leak on API eviction
    _schedule(_delete_bucket_object(movie_id, movies.get(movie_id, {}).get("file_name")))
    return {"status": "ok"}


@app.delete("/api/media/{movie_id}")
async def delete_media(movie_id: str, delete_tg: bool = False, x_api_key: str | None = Header(default=None)):
    _check_api_auth(x_api_key)
    movies = await _get_movies()
    movie = movies.get(movie_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found in index")
    
    # 1. Evict cache from downloader
    await download_manager.evict(movie_id, appstate.redis_client,
                                 file_name=movie.get("file_name"))
    await _deferred_pop(movie_id)  # #2: prevent leak on delete
    _schedule(_delete_bucket_object(movie_id, movie.get("file_name")))
    
    # 2. Optionally delete from Telegram
    if delete_tg:
        active_tg = get_tg()
        try:
            await active_tg.delete_messages(CHANNEL_USERNAME, [movie["message_id"]])
        except AuthKeyDuplicated as ae:
            log.error(f"[delete_media] AuthKeyDuplicated on client. Suspending client: {ae}")
            client_pool.suspend_auth(active_tg)
            await get_tg().delete_messages(CHANNEL_USERNAME, [movie["message_id"]])
        except Exception as e:
            log.error(f"[delete_media] failed to delete from Telegram: {e}")
            raise HTTPException(status_code=502, detail=f"Failed to delete from Telegram: {e}")
            
    # 3. Delete from index
    await st.del_movie(appstate.redis_client, movie_id)
    _invalidate_movies_cache()
    return {"status": "ok"}


# ── Configuration Endpoint ───────────────────────────────────────────────────
@app.get("/api/config")
async def api_config():
    manifest_url = f"{BASE_URL}/manifest.json"
    stremio_url  = manifest_url.replace("https://", "stremio://").replace("http://", "stremio://")
    return {
        "channel": str(CHANNEL_USERNAME),
        "manifest_url": manifest_url,
        "stremio_url": stremio_url,
        "hf_bucket": {
            "configured": hfbucket.configured(),
            "id": hfbucket.HF_BUCKET_ID or None,
            "mounted": hfbucket.HF_BUCKET_MOUNTED,
            "redirect_done": HF_REDIRECT_DONE,
            "signed": bool(hfbucket.HF_S3_ACCESS_KEY and hfbucket.HF_S3_SECRET_KEY),
        },
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

# Wire sync.py's runtime helpers (bound here, at module setup, so sync.py can
# keep a one-way import direction and stay importable on its own).
_sync_configure(
    _get_movies=_get_movies,
    _invalidate_movies_cache=_invalidate_movies_cache,
    _is_cached=_is_cached,
    _schedule=_schedule,
    _notify_send=_notify_send,
    _queue_put=_queue_put,
    _prefetch_queued=_prefetch_queued,
    _delete_bucket_object=_delete_bucket_object,
    get_tg=get_tg,
    CHANNEL_USERNAME=CHANNEL_USERNAME,
    DISABLE_BOT_LISTENER=DISABLE_BOT_LISTENER,
)
