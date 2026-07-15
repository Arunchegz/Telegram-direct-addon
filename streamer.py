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
MIN_THROTTLE_MS = int(os.getenv("MIN_THROTTLE_MS", "100"))  # Throttle between GetFile calls (100ms default); lower is faster
MAX_BACKOFF_S = 60     # Max backoff on rate limit (Telegram's max is typically 2-60s)
MAX_CONCURRENT_GETFILE = 1  # Single concurrent GetFile to prevent request storms


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

        Keyed per client (c_idx) — each session gets its own 500ms budget
        instead of all sessions sharing one global timer. A pool of N
        clients can therefore sustain ~N req/s combined instead of being
        capped at ~1 req/s system-wide regardless of pool size.
        """
        if c_idx not in self._throttle_locks:
            self._throttle_locks[c_idx] = asyncio.Lock()
        lock = self._throttle_locks[c_idx]
        async with lock:
            last = self._last_invoke_time.get(c_idx, 0.0)
            elapsed = (time.time() - last) * 1000
            if elapsed < MIN_THROTTLE_MS:
                await asyncio.sleep((MIN_THROTTLE_MS - elapsed) / 1000)
            self._last_invoke_time[c_idx] = time.time()

    async def _wait_backoff(self, dc_id: int, flood_wait_s: int, c_idx: int | None = None) -> None:
        """Exponential backoff with jitter on FloodWait."""
        # Add jitter: ±20% to spread requests
        jitter = random.uniform(0.8, 1.2)
        wait_s = min(flood_wait_s * jitter, MAX_BACKOFF_S)
        until = time.time() + wait_s
        self._backoff_until[(c_idx, dc_id)] = until
        print(f"[streamer] Client {c_idx} DC {dc_id} rate limited. Backoff {wait_s:.1f}s (Telegram req: {flood_wait_s}s)")
        try:
            from metrics import metrics
            await metrics.record_rate_limit(dc_id, wait_s)
        except Exception as e:
            print(f"[streamer] metrics error: {e}")
        await asyncio.sleep(wait_s)

    async def _get_fresh_msg(self, chat_id: int, message_id: int, client: Client, client_idx: int | None):
        """Get or fetch a fresh message for the specific client."""
        now = time.time()
        key = (chat_id, message_id, client_idx if client_idx is not None else client)
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
            return msg
        return cached_msg

    def _invalidate_msg_cache(self, chat_id: int, message_id: int, client: Client, client_idx: int | None):
        key = (chat_id, message_id, client_idx if client_idx is not None else client)
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
    ) -> AsyncGenerator[bytes, None]:
        # Pick client at entry if not provided — but do NOT reuse it across
        # the entire multi-chunk loop below. Instead, re-pick per chunk to
        # ensure round-robin distribution even within a single stream request.
        # This prevents one session from absorbing all rate limits while the
        # other sits idle.
        initial_c_idx, initial_c = c_idx, c
        if initial_c is None:
            if hasattr(self.client, "pick"):
                initial_c_idx, initial_c = await self.client.pick()
            else:
                initial_c_idx, initial_c = None, self.client

        # Ensure msg is bound to the chosen client's session
        if hasattr(msg, "_client") and msg._client != initial_c:
            try:
                msg = await self._get_fresh_msg(msg.chat.id, msg.id, initial_c, initial_c_idx)
            except Exception:
                pass

        fid     = _extract_fid(msg)
        session = await self._session(initial_c, fid)
        loc     = _location(fid)
        part    = 1
        off     = offset
        dc_id   = fid.dc_id

        # Check if DC is in backoff for the chosen client; if so, wait
        backoff_key = (initial_c_idx, dc_id)
        if backoff_key in self._backoff_until:
            until = self._backoff_until[backoff_key]
            if time.time() < until:
                remaining = until - time.time()
                print(f"[streamer] Waiting for Client {initial_c_idx} DC {dc_id} backoff: {remaining:.1f}s")
                await asyncio.sleep(remaining)
            del self._backoff_until[backoff_key]

        try:
            async with self._concurrent_semaphore:
                await self._throttle(initial_c_idx)  # Apply inter-request throttle
                try:
                    r = await session.invoke(
                        raw.functions.upload.GetFile(location=loc, offset=off, limit=chunk)
                    )
                    # NOTE: semaphore only wraps the first GetFile call.
                    # Subsequent chunks in the loop below invoke without it —
                    # intentional: per-chunk throttling + per-client round-robin
                    # provide sufficient rate-limit protection without serialising
                    # an entire multi-chunk stream through a single semaphore slot.
                except (FloodWait, Timeout, RpcConnectFailed) as e:
                    wait_s = e.value if hasattr(e, 'value') else 5
                    if initial_c_idx is not None and hasattr(self.client, "mark_cooldown"):
                        self.client.mark_cooldown(initial_c_idx, wait_s)
                    await self._wait_backoff(dc_id, wait_s, initial_c_idx)
                    # Retry after backoff — re-pick client to distribute load
                    if _retry:
                        # Re-pick a fresh client (don't reuse the rate-limited one)
                        if hasattr(self.client, "pick"):
                            c_idx, c = await self.client.pick()
                        async for b in self.yield_file(msg, offset, first_cut, last_cut, parts, chunk, False, c, c_idx):
                            yield b
                        return
                    else:
                        raise
        except FileReferenceExpired:
            if not _retry:
                raise
            # Invalidate cache
            self._invalidate_msg_cache(msg.chat.id, msg.id, initial_c, initial_c_idx)
            # Refresh message to get new file reference using a valid client
            refresh_client = initial_c
            refresh_c_idx = initial_c_idx
            if refresh_client is None:
                refresh_c_idx, refresh_client = await self.client.pick()
            try:
                msg = await self._get_fresh_msg(msg.chat.id, msg.id, refresh_client, refresh_c_idx)
            except Exception:
                # If refresh fails, try picking another client
                refresh_c_idx, refresh_client = await self.client.pick()
                msg = await self._get_fresh_msg(msg.chat.id, msg.id, refresh_client, refresh_c_idx)
            # Restart from beginning with fresh client selection
            async for b in self.yield_file(msg, offset, first_cut, last_cut, parts, chunk, False, refresh_client, refresh_c_idx):
                yield b
            return

        if not isinstance(r, raw.types.upload.File):
            return

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

            # Re-pick client for each subsequent chunk to distribute load
            # evenly across all sessions instead of sticking with one client
            # for the entire stream (which causes imbalanced rate limiting).
            current_c_idx, current_c = initial_c_idx, initial_c
            if hasattr(self.client, "pick"):
                current_c_idx, current_c = await self.client.pick()
            
            # Ensure msg is bound to the chosen client's session
            if hasattr(msg, "_client") and msg._client != current_c:
                try:
                    msg = await self._get_fresh_msg(msg.chat.id, msg.id, current_c, current_c_idx)
                except Exception:
                    pass

            fid = _extract_fid(msg)
            loc = _location(fid)

            await self._throttle(current_c_idx)  # Throttle between chunks
            try:
                # Create new session for the newly picked client
                current_session = await self._session(current_c, fid)
                async with self._concurrent_semaphore:
                    r = await current_session.invoke(
                        raw.functions.upload.GetFile(location=loc, offset=off, limit=chunk)
                    )
            except (FloodWait, Timeout, RpcConnectFailed) as e:
                wait_s = e.value if hasattr(e, 'value') else 5
                if current_c_idx is not None and hasattr(self.client, "mark_cooldown"):
                    self.client.mark_cooldown(current_c_idx, wait_s)
                await self._wait_backoff(dc_id, wait_s, current_c_idx)
                # Retry after backoff — re-pick client to distribute load
                if _retry:
                    # Re-pick a fresh client (don't reuse the rate-limited one)
                    if hasattr(self.client, "pick"):
                        c_idx, c = await self.client.pick()
                    async for b in self.yield_file(msg, off, 0, last_cut, parts - part + 1, chunk, True, c, c_idx):
                        yield b
                    return
                else:
                    raise
            except FileReferenceExpired:
                if not _retry:
                    raise
                # Invalidate cache
                self._invalidate_msg_cache(msg.chat.id, msg.id, current_c, current_c_idx)
                # Refresh message to get new file reference using a valid client
                refresh_client = current_c
                refresh_c_idx = current_c_idx
                if refresh_client is None:
                    refresh_c_idx, refresh_client = await self.client.pick()
                try:
                    msg = await self._get_fresh_msg(msg.chat.id, msg.id, refresh_client, refresh_c_idx)
                except Exception:
                    # If refresh fails, try picking another client
                    refresh_c_idx, refresh_client = await self.client.pick()
                    msg = await self._get_fresh_msg(msg.chat.id, msg.id, refresh_client, refresh_c_idx)
                # Continue from current offset with fresh client selection
                async for b in self.yield_file(msg, off, 0, last_cut, parts - part + 1, chunk, True, refresh_client, refresh_c_idx):
                    yield b
                return

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
                s = Session(
                    c, dc,
                    await c.storage.auth_key(),
                    await c.storage.test_mode(),
                    is_media=True,
                )
                await s.start()

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
