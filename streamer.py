"""  
streamer.py — Pyrogram MTProto ByteStreamer with rate limit mitigation.
Extracted module; imported by main.py and downloader.py.

Rate limit strategies:
  1. Exponential backoff with jitter on FloodWait
  2. Per-DC session pooling (reuse sessions, reduce auth overhead)
  3. Request throttling between GetFile calls
  4. Adaptive chunking (smaller chunks when rate limited)
"""
from __future__ import annotations
import logging
import asyncio
import os
import random
import time
from typing import AsyncGenerator

from pyrogram import Client, raw, utils
from pyrogram.errors import AuthBytesInvalid, FileReferenceExpired, FloodWait, RpcConnectFailed, Timeout
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session

TG_CHUNK = 1024 * 1024        # Live streaming chunk size (1MB) - balances startup speed and API calls
PREFETCH_CHUNK = 2 * 1024 * 1024   # Background prefetch logical chunk size (2MB) - fewer GetFile requests, higher throughput
TG_MAX_LIMIT = 1024 * 1024      # Telegram's maximum allowed limit per GetFile request (hard API limit)
MIN_THROTTLE_MS = int(os.getenv("MIN_THROTTLE_MS", "100"))      # Live stream: inter-request delay per client (100ms → ~10 req/s cap)
MIN_DL_THROTTLE_MS = int(os.getenv("MIN_DL_THROTTLE_MS", "600")) # Background dl: slower per client (600ms → ~1.6 req/s per client)
MAX_BACKOFF_S = 60     # Max backoff on rate limit (Telegram's max is typically 2-60s)
MAX_CONCURRENT_GETFILE = 1  # Single concurrent GetFile to prevent request storms
MAX_MSG_CACHE_SIZE = 500   # Auto-prune message cache above this threshold

log = logging.getLogger("tgstream.streamer")


class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client
        self._last_invoke_time: dict = {}      # key: c_idx (None for single-client mode)
        self._throttle_locks: dict = {}        # per-client lock, created lazily
        self._session_locks: dict = {}         # Lock to serialize session creation per client
        self._backoff_until = {}  # Per-client and DC backoff state: {(c_idx, dc_id): until_timestamp}
        self._msg_cache: dict = {}  # (chat_id, msg_id, c_idx) -> (msg, fetched_at)
        # If `client` is actually a ClientPool (has __len__), scale concurrent
        # GetFile slots to the pool size — one slot per session — instead of
        # serializing every stream in the process through a single global lock.
        pool_size = len(client) if hasattr(client, "__len__") else 1
        concurrency = max(MAX_CONCURRENT_GETFILE, pool_size)
        self._concurrent_semaphore = asyncio.Semaphore(concurrency)  # Global concurrency limit

        # Live-playback priority: counts requests currently pulling bytes
        # for active/foreground streaming (Path C tail + Path D). Background
        # downloader checks this and pauses while a different movie is streaming.
        self.live_streams = 0
        self.live_movie_ids = set()

    def mark_live_start(self, movie_id: str = None) -> None:
        self.live_streams += 1
        if movie_id:
            self.live_movie_ids.add(movie_id)

    def mark_live_end(self, movie_id: str = None) -> None:
        self.live_streams = max(0, self.live_streams - 1)
        if movie_id and movie_id in self.live_movie_ids:
            self.live_movie_ids.remove(movie_id)

    async def _throttle(self, c_idx=None) -> None:
        """Enforce minimum inter-request delay to avoid Telegram rate limits.

        Keyed per client (c_idx) — each session gets its own budget
        instead of all sessions sharing one global timer. A pool of N
        clients can therefore sustain ~N req/s combined instead of being
        capped at ~1 req/s system-wide regardless of pool size.
        """
        await self._throttle_with_ms(c_idx, MIN_THROTTLE_MS)

    async def _dl_throttle(self, c_idx=None) -> None:
        """Slower throttle for background downloads.

        Background prefetch uses MIN_DL_THROTTLE_MS (default 600ms) per client
        instead of the 100ms live-stream rate. With 3 clients and 2 concurrent
        downloads this yields ~1.1 req/s per client — well within Telegram's
        ~2 req/s limit — preventing the paired FloodWaits seen at 100ms spacing.
        """
        await self._throttle_with_ms(c_idx, MIN_DL_THROTTLE_MS)

    async def _throttle_with_ms(self, c_idx, min_ms: int) -> None:
        """Core throttle implementation, parameterized by minimum interval."""
        self._throttle_locks.setdefault(c_idx, asyncio.Lock())
        lock = self._throttle_locks[c_idx]
        async with lock:
            last = self._last_invoke_time.get(c_idx, 0.0)
            elapsed = (time.time() - last) * 1000
            if elapsed < min_ms:
                await asyncio.sleep((min_ms - elapsed) / 1000)
            self._last_invoke_time[c_idx] = time.time()

    async def _wait_backoff(self, dc_id: int, flood_wait_s: int, c_idx: int | None = None) -> None:
        """Exponential backoff with jitter on FloodWait."""
        # Add jitter: ±20% to spread requests
        jitter = random.uniform(0.8, 1.2)
        wait_s = min(flood_wait_s * jitter, MAX_BACKOFF_S)
        until = time.time() + wait_s
        self._backoff_until[(c_idx, dc_id)] = until
        log.warning(f"[streamer] Client {c_idx} DC {dc_id} rate limited. Backoff {wait_s:.1f}s (Telegram req: {flood_wait_s}s)")
        try:
            from metrics import metrics
            await metrics.record_rate_limit(dc_id, wait_s)
        except Exception as e:
            log.error(f"[streamer] metrics error: {e}")
        await asyncio.sleep(wait_s)

    async def _get_fresh_msg(self, chat_id: int, message_id: int, client: Client, client_idx: int | None):
        """Get or fetch a fresh message for the specific client."""
        now = time.time()
        # Use id(client) as scalar fallback — Client objects don't implement __hash__
        # by session identity, so mixing object refs and ints as dict keys causes misses.
        key = (chat_id, message_id, client_idx if client_idx is not None else id(client))
        cached_msg, fetched_at = self._msg_cache.get(key, (None, 0.0))
        if cached_msg is None or (now - fetched_at) > 3000:
            try:
                msg = await client.get_messages(chat_id, message_id)
            except Exception:
                # Fallback to env variable CHANNEL_USERNAME if direct lookup fails
                import os
                channel = os.getenv("CHANNEL_USERNAME", "").strip()
                if channel.startswith("-") and channel[1:].isdigit():
                    channel = int(channel)
                elif channel.isdigit():
                    channel = int(channel)
                if not channel:
                    raise
                msg = await client.get_messages(channel, message_id)
            self._msg_cache[key] = (msg, now)
            if len(self._msg_cache) > MAX_MSG_CACHE_SIZE:
                self.prune_msg_cache()
            return msg
        return cached_msg

    def _invalidate_msg_cache(self, chat_id: int, message_id: int, client: Client, client_idx: int | None):
        key = (chat_id, message_id, client_idx if client_idx is not None else id(client))
        if key in self._msg_cache:
            del self._msg_cache[key]

    def prune_msg_cache(self, max_age_s: float = 3000):
        """Drop entries older than max_age_s. Cache has no natural eviction
        otherwise and grows forever on a long-lived process."""
        now = time.time()
        stale = [k for k, (_, ts) in self._msg_cache.items() if (now - ts) > max_age_s]
        for k in stale:
            del self._msg_cache[k]
        return len(stale)

    MAX_YIELD_RETRIES = 5  # hard cap on retry attempts per yield_file call

    async def yield_file(
        self,
        msg,
        offset: int,
        first_cut: int,
        last_cut: int,
        parts: int,
        chunk: int = TG_CHUNK,
        _retry: bool = True,
        c: Client = None,
        c_idx: int = None,
        is_background: bool = False,  # True for prefetch/download; uses slower dl_throttle
    ) -> AsyncGenerator[bytes, None]:
        """Stream file bytes from Telegram MTProto, iterating across chunks.

        Retry logic is fully iterative (no recursion) to avoid stack
        overflow on repeated FloodWait/FileReferenceExpired mid-stream.
        MAX_YIELD_RETRIES caps total retries across all error types.

        is_background=True uses MIN_DL_THROTTLE_MS (600ms) instead of
        MIN_THROTTLE_MS (100ms) to prevent background downloads from
        flooding Telegram sessions and causing FloodWaits.
        """
        throttle = self._dl_throttle if is_background else self._throttle
        # for round-robin load distribution across pool sessions.
        cur_c_idx, cur_c = c_idx, c
        if cur_c is None:
            if hasattr(self.client, "pick"):
                cur_c_idx, cur_c = await self.client.pick()
            else:
                cur_c_idx, cur_c = None, self.client

        # Ensure msg is bound to the chosen client's session
        if hasattr(msg, "_client") and msg._client != cur_c:
            try:
                msg = await self._get_fresh_msg(msg.chat.id, msg.id, cur_c, cur_c_idx)
            except Exception:
                pass

        fid     = _extract_fid(msg)
        session = await self._session(cur_c, fid)
        loc     = _location(fid)
        part    = 1
        off     = offset
        dc_id   = fid.dc_id
        retries = 0

        # Check if DC is in backoff for the chosen client; if so, wait
        backoff_key = (cur_c_idx, dc_id)
        if backoff_key in self._backoff_until:
            until = self._backoff_until[backoff_key]
            if time.time() < until:
                remaining = until - time.time()
                log.info(f"[streamer] Waiting for Client {cur_c_idx} DC {dc_id} backoff: {remaining:.1f}s")
                await asyncio.sleep(remaining)
            del self._backoff_until[backoff_key]

        # ── First chunk ───────────────────────────────────────────────────────
        while True:
            try:
                async with self._concurrent_semaphore:
                    await throttle(cur_c_idx)
                    r = await session.invoke(
                        raw.functions.upload.GetFile(location=loc, offset=off, limit=chunk)
                    )
                break  # success
            except (FloodWait, Timeout, RpcConnectFailed) as e:
                if not _retry or retries >= self.MAX_YIELD_RETRIES:
                    raise
                retries += 1
                wait_s = e.value if hasattr(e, "value") else 5
                if cur_c_idx is not None and hasattr(self.client, "mark_cooldown"):
                    self.client.mark_cooldown(cur_c_idx, wait_s)
                await self._wait_backoff(dc_id, wait_s, cur_c_idx)
                # Re-pick fresh client after rate-limit backoff
                if hasattr(self.client, "pick"):
                    cur_c_idx, cur_c = await self.client.pick()
                fid = _extract_fid(msg)
                session = await self._session(cur_c, fid)
                loc = _location(fid)
            except FileReferenceExpired:
                if not _retry or retries >= self.MAX_YIELD_RETRIES:
                    raise
                retries += 1
                self._invalidate_msg_cache(msg.chat.id, msg.id, cur_c, cur_c_idx)
                if hasattr(self.client, "pick"):
                    cur_c_idx, cur_c = await self.client.pick()
                try:
                    msg = await self._get_fresh_msg(msg.chat.id, msg.id, cur_c, cur_c_idx)
                except Exception:
                    if hasattr(self.client, "pick"):
                        cur_c_idx, cur_c = await self.client.pick()
                    msg = await self._get_fresh_msg(msg.chat.id, msg.id, cur_c, cur_c_idx)
                fid = _extract_fid(msg)
                session = await self._session(cur_c, fid)
                loc = _location(fid)

        if not isinstance(r, raw.types.upload.File):
            return

        # ── Yield loop across all parts ───────────────────────────────────────
        while True:
            data = r.bytes
            if not data:
                break
            if parts == 1:
                yield data[first_cut:last_cut]
            elif part == 1:
                yield data[first_cut:]
            elif part == parts:
                yield data[:last_cut]
            else:
                yield data

            part += 1
            off  += chunk
            if part > parts:
                break

            # Re-pick client for each subsequent chunk (round-robin load distribution)
            if hasattr(self.client, "pick"):
                cur_c_idx, cur_c = await self.client.pick()

            # Ensure msg is bound to the new client's session
            if hasattr(msg, "_client") and msg._client != cur_c:
                try:
                    msg = await self._get_fresh_msg(msg.chat.id, msg.id, cur_c, cur_c_idx)
                except Exception:
                    pass

            fid = _extract_fid(msg)
            loc = _location(fid)

            await throttle(cur_c_idx)
            while True:
                try:
                    current_session = await self._session(cur_c, fid)
                    async with self._concurrent_semaphore:
                        r = await current_session.invoke(
                            raw.functions.upload.GetFile(location=loc, offset=off, limit=chunk)
                        )
                    break  # success
                except (FloodWait, Timeout, RpcConnectFailed) as e:
                    if not _retry or retries >= self.MAX_YIELD_RETRIES:
                        raise
                    retries += 1
                    wait_s = e.value if hasattr(e, "value") else 5
                    if cur_c_idx is not None and hasattr(self.client, "mark_cooldown"):
                        self.client.mark_cooldown(cur_c_idx, wait_s)
                    await self._wait_backoff(dc_id, wait_s, cur_c_idx)
                    if hasattr(self.client, "pick"):
                        cur_c_idx, cur_c = await self.client.pick()
                    fid = _extract_fid(msg)
                    current_session = await self._session(cur_c, fid)
                    loc = _location(fid)
                except FileReferenceExpired:
                    if not _retry or retries >= self.MAX_YIELD_RETRIES:
                        raise
                    retries += 1
                    self._invalidate_msg_cache(msg.chat.id, msg.id, cur_c, cur_c_idx)
                    if hasattr(self.client, "pick"):
                        cur_c_idx, cur_c = await self.client.pick()
                    try:
                        msg = await self._get_fresh_msg(msg.chat.id, msg.id, cur_c, cur_c_idx)
                    except Exception:
                        if hasattr(self.client, "pick"):
                            cur_c_idx, cur_c = await self.client.pick()
                        msg = await self._get_fresh_msg(msg.chat.id, msg.id, cur_c, cur_c_idx)
                    fid = _extract_fid(msg)
                    loc = _location(fid)

    async def _session(self, c: Client, fid: FileId) -> Session:
        dc = fid.dc_id
        # Lazily create the per-client session lock first (lock dict is only
        # written from the event-loop thread, so no race on the dict itself).
        if c not in self._session_locks:
            self._session_locks[c] = asyncio.Lock()
        lock = self._session_locks[c]
        async with lock:
            # Initialise media_sessions inside the lock so two coroutines
            # cannot both pass the hasattr check and both assign the dict.
            if not hasattr(c, "media_sessions"):
                c.media_sessions = {}
            if dc in c.media_sessions:
                return c.media_sessions[dc]

            if dc != await c.storage.dc_id():
                s = Session(
                    c, dc,
                    await Auth(c, dc, await c.storage.test_mode()).create(),
                    await c.storage.test_mode(),
                    is_media=True,
                )
                await s.start()
                for _ in range(6):
                    exp = await c.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc))
                    try:
                        await s.invoke(
                            raw.functions.auth.ImportAuthorization(id=exp.id, bytes=exp.bytes)
                        )
                        break
                    except AuthBytesInvalid:
                        continue
                else:
                    await s.stop()
                    raise AuthBytesInvalid
            else:
                # Same DC as client's home — reuse the client's own session
                # rather than opening a new Session with the same auth_key.
                # Opening a second Session with the same key causes
                # AuthKeyDuplicated on Telegram's side.
                s = c.session

            c.media_sessions[dc] = s
            return s


def _extract_fid(msg) -> FileId:
    media = msg.video or msg.document
    if not media:
        raise ValueError("No streamable media")
    return FileId.decode(media.file_id)


def _location(fid: FileId):
    ft = fid.file_type
    if ft == FileType.CHAT_PHOTO:
        if fid.chat_id > 0:
            peer = raw.types.InputPeerUser(user_id=fid.chat_id, access_hash=fid.chat_access_hash)
        elif fid.chat_access_hash == 0:
            peer = raw.types.InputPeerChat(chat_id=-fid.chat_id)
        else:
            peer = raw.types.InputPeerChannel(
                channel_id=utils.get_channel_id(fid.chat_id),
                access_hash=fid.chat_access_hash,
            )
        return raw.types.InputPeerPhotoFileLocation(
            peer=peer, volume_id=fid.volume_id, local_id=fid.local_id,
            big=fid.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
        )
    elif ft == FileType.PHOTO:
        return raw.types.InputPhotoFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size=fid.thumbnail_size,
        )
    else:
        return raw.types.InputDocumentFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size=fid.thumbnail_size,
        )
