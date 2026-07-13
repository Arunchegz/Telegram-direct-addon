"""
downloader.py — Hybrid predictive download engine with rate-limit awareness.

Architecture:
  SparseFile    — pre-truncated file, pwrite at any offset
  DownloadMap   — sorted merged interval list [[start,end], ...]
                  O(log n) lookup, O(n) merge
  DownloadTask  — asyncio task per movie: sequential MTProto fetch
                  writes to SparseFile, updates DownloadMap in Redis
                  signals waiting proxy requests via asyncio.Event
                  respects rate limits via adaptive backoff

Player never knows — proxy checks DownloadMap before each range,
serves local file if available, falls back to live Telegram otherwise.
"""
from __future__ import annotations

import asyncio
import bisect
import json
import os
import time
from pathlib import Path
from typing import Optional

import redis.asyncio as aioredis

from clients import pool as client_pool

# ── Constants ───────────────────────────────────────────────────────────[...]
# Import chunk sizes from streamer module to ensure consistency
from streamer import TG_CHUNK, PREFETCH_CHUNK, TG_MAX_LIMIT

LOCAL_READY_MB = int(os.getenv("LOCAL_READY_MB", "15"))  # Switch to local when 15MB ahead cached (empirically tuned for stability)
STORAGE_DIR    = Path(os.getenv("STORAGE_DIR", "/tmp/tgstream"))
MAX_LOCAL_GB   = float(os.getenv("MAX_LOCAL_GB", "10"))  # evict LRU beyond this
DL_MIN_BACKOFF = float(os.getenv("DL_MIN_BACKOFF", "2"))  # Backoff on error (seconds)
SEEK_GRACE_DELAY_S = float(os.getenv("SEEK_GRACE_DELAY_S", "5"))  # Pause downloader after seek, let live proxy claim MTProto first
DL_THROTTLE_S  = float(os.getenv("DL_THROTTLE_S", "0.15"))  # Sleep between successful chunks (reduced for larger prefetch chunks)
LOCAL_READY_BYTES = LOCAL_READY_MB * 1024 * 1024

# Redis key templates
R_DL_MAP  = "tgstream:dl:map:{}"    # JSON [[start,end],...]
R_DL_DONE = "tgstream:dl:done:{}"   # "1" when fully downloaded
R_DL_PATH = "tgstream:dl:path:{}"   # local file path string
R_DL_TS   = "tgstream:dl:ts:{}"     # last access timestamp (for LRU eviction)


# ────────────────────────────────────────────────────────────────────────[...]
# DownloadMap: sorted merged interval list
# ────────────────────────────────────────────────────────────────────────[...]
class DownloadMap:
    """
    Sorted list of non-overlapping [start, end] byte intervals.
    Merge on insert. O(log n) contains check.
    """

    def __init__(self, intervals: list[list[int]] | None = None):
        self._ivs: list[list[int]] = intervals or []

    # ── Serialisation ────────────────────────────────────────────────────────[...]
    def to_json(self) -> str:
        return json.dumps(self._ivs)

    @classmethod
    def from_json(cls, s: str | bytes) -> "DownloadMap":
        return cls(json.loads(s))

    # ── Query ───────────────────────────────────────────────────────────[...]
    def has_range(self, start: int, end: int) -> bool:
        """True if [start, end] fully covered by stored intervals."""
        if not self._ivs:
            return False
        # Binary search for rightmost interval whose start <= start
        idx = bisect.bisect_right(self._ivs, [start, float("inf")]) - 1
        if idx < 0:
            return False
        iv_start, iv_end = self._ivs[idx]
        return iv_start <= start and iv_end >= end

    def covered_prefix(self, start: int) -> int:
        """
        How many contiguous bytes are available starting from `start`.
        Returns 0 if nothing available at start.
        """
        if not self._ivs:
            return 0
        idx = bisect.bisect_right(self._ivs, [start, float("inf")]) - 1
        if idx < 0:
            return 0
        iv_start, iv_end = self._ivs[idx]
        if iv_start > start:
            return 0
        # Walk forward through contiguous intervals
        covered_end = iv_end
        for i in range(idx + 1, len(self._ivs)):
            ns, ne = self._ivs[i]
            if ns <= covered_end + 1:
                covered_end = max(covered_end, ne)
            else:
                break
        return max(0, covered_end - start + 1)

    def total_bytes(self) -> int:
        return sum(e - s + 1 for s, e in self._ivs)

    # ── Mutate ──────────────────────────────────────────────────────────[...]
    def add(self, start: int, end: int) -> None:
        """Insert [start, end] and merge overlapping/adjacent intervals."""
        new_iv = [start, end]
        merged: list[list[int]] = []
        inserted = False

        for iv in self._ivs:
            if iv[1] < new_iv[0] - 1:
                merged.append(iv)
            elif iv[0] > new_iv[1] + 1:
                if not inserted:
                    merged.append(new_iv)
                    inserted = True
                merged.append(iv)
            else:
                new_iv[0] = min(new_iv[0], iv[0])
                new_iv[1] = max(new_iv[1], iv[1])

        if not inserted:
            merged.append(new_iv)

        self._ivs = merged

    def clone(self) -> "DownloadMap":
        return DownloadMap([list(iv) for iv in self._ivs])


# ────────────────────────────────────────────────────────────────────────[...]
# SparseFile: pre-truncated file, pwrite semantics
# ────────────────────────────────────────────────────────────────────────[...]
class SparseFile:
    """
    Pre-allocated (sparse) file. Supports concurrent pwrite + pread.
    Uses asyncio.to_thread for blocking I/O so event loop stays free.
    """

    def __init__(self, path: Path):
        self.path = path
        self._lock = asyncio.Lock()

    @classmethod
    async def create(cls, path: Path, size: int) -> "SparseFile":
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            # ftruncate creates sparse file (no disk allocation until written)
            await asyncio.to_thread(_truncate_file, path, size)
        return cls(path)

    async def pwrite(self, data: bytes, offset: int) -> None:
        await asyncio.to_thread(_pwrite, self.path, data, offset)

    async def pread(self, offset: int, length: int) -> bytes:
        return await asyncio.to_thread(_pread, self.path, offset, length)

    def exists(self) -> bool:
        return self.path.exists()

    async def delete(self) -> None:
        await asyncio.to_thread(self.path.unlink, missing_ok=True)


def _truncate_file(path: Path, size: int):
    with open(path, "wb") as f:
        f.truncate(size)


def _pwrite(path: Path, data: bytes, offset: int):
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)


def _pread(path: Path, offset: int, length: int) -> bytes:
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(length)


# ────────────────────────────────────────────────────────────────────────[...]
# DownloadTask: per-movie background downloader
# ────────────────────────────────────────────────────────────────────────[...]
class DownloadTask:
    """
    Sequentially downloads a Telegram file to local SparseFile.
    Priority: start from current play-head hint, then continue forward.
    
    Lifecycle: created on first stream request, runs until file complete
    or task cancelled (eviction / shutdown).
    
    The proxy signals play-head via hint(offset) so downloader stays ahead.
    Handles rate limits gracefully with exponential backoff.
    """

    def __init__(
        self,
        movie_id: str,
        file_size: int,
        sparse: SparseFile,
        dl_map: DownloadMap,
        redis: aioredis.Redis,
        byte_streamer,          # streamer.ByteStreamer
        fetch_msg_fn,           # async fn(msg_id) -> msg
        message_id: int,
        dl_semaphore: asyncio.Semaphore,
        alert_fn=None,   # optional async fn(text) — fired after repeated consecutive failures
    ):
        self.movie_id    = movie_id
        self.file_size   = file_size
        self.sparse      = sparse
        self.dl_map      = dl_map
        self.redis       = redis
        self.streamer    = byte_streamer
        self.fetch_msg   = fetch_msg_fn
        self.message_id  = message_id
        self._semaphore  = dl_semaphore
        self._alert_fn   = alert_fn

        self._task: Optional[asyncio.Task] = None
        self._hint: int = 0              # play-head hint from proxy
        self._progress_event = asyncio.Event()   # fires when new bytes land
        self._done = False
        self._msg = None                 # cached fresh message
        self._msg_fetched_at = 0.0
        self._error_backoff = 1.0        # Exponential backoff multiplier
        self._consecutive_errors = 0     # for stuck-download alerting
        self._alerted_stuck = False      # avoid repeat alerts for the same stuck streak
        self._seek_event = asyncio.Event()       # fires on large seek, aborts current batch

    # ── Public API ─────────────────────────────────────────────────────────[...]
    # Jump threshold: if player seeks > 30MB ahead of current batch, abort and re-anchor
    SEEK_JUMP_THRESHOLD = 30 * 1024 * 1024

    def hint(self, offset: int):
        """Proxy tells downloader where player currently is.
        If offset jumps far ahead of current hint, signal a seek so the
        current batch aborts and re-anchors at the new position.
        """
        jump = offset - self._hint
        if jump > self.SEEK_JUMP_THRESHOLD:
            self._hint = offset
            self._seek_event.set()   # abort current batch
        elif offset > self._hint:
            self._hint = offset

    def is_done(self) -> bool:
        return self._done

    def progress_event(self) -> asyncio.Event:
        return self._progress_event

    def start(self) -> asyncio.Task:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name=f"dl:{self.movie_id}")
        return self._task

    def cancel(self):
        if self._task and not self._task.done():
            self._task.cancel()

    # ── Internal ─────────────────────────────────────────────────────────[...]
    async def _fresh_msg(self, c: Client):
        """Re-fetch message if file_reference may have expired (>50min old)."""
        now = time.time()
        if self._msg is None or (now - self._msg_fetched_at) > 3000:
            self._msg = await self.fetch_msg(self.message_id, client=c)
            self._msg_fetched_at = now
        return self._msg

    async def _run(self):
        """Continuous background downloader from play-head to EOF.

        Strategy:
          - Download sequentially from current hint position to end of file.
          - Skip already-cached chunks.
          - On seek (hint jumps > 30MB): re-anchor to new position immediately.
          - Never waits for 90% triggers or batch boundaries.
          - Proxy switches to local once LOCAL_READY_BYTES ahead of hint is cached.
        """
        # No longer pin one client for the whole task lifetime — client
        # selection now happens per chunk below, alternating across the
        # pool so a single background download uses both sessions' worth
        # of throughput instead of one client doing all the work.
        pool_size = len(self.streamer.client) if hasattr(self.streamer.client, "__len__") else 1
        c_idx, c = (None, None) if hasattr(self.streamer.client, "pick") else (None, self.streamer.client)

        print(f"[dl:{self.movie_id}] start size={self.file_size/1024/1024:.1f}MB "
              f"{'alternating across ' + str(pool_size) + ' client(s)' if hasattr(self.streamer.client, 'pick') else 'using single client'}")
        try:
            from metrics import metrics
            metrics.downloads_active += 1
        except Exception:
            pass

        try:
            current_offset = (self._hint // PREFETCH_CHUNK) * PREFETCH_CHUNK

            while current_offset < self.file_size:
                # Re-anchor if seek jumped ahead
                if self._seek_event.is_set():
                    self._seek_event.clear()
                    current_offset = (self._hint // PREFETCH_CHUNK) * PREFETCH_CHUNK
                    print(f"[dl:{self.movie_id}] seek → re-anchor at {current_offset/1024/1024:.1f}MB")
                    # Grace delay: let the live proxy stream claim the MTProto session
                    # first after a seek, instead of both proxy and downloader hitting
                    # GetFile simultaneously and triggering FloodWait.
                    await asyncio.sleep(SEEK_GRACE_DELAY_S)

                # Skip already-cached chunk
                chunk_end = min(current_offset + PREFETCH_CHUNK - 1, self.file_size - 1)
                if self.dl_map.has_range(current_offset, chunk_end):
                    current_offset = chunk_end + 1
                    continue

                chunk_len = chunk_end - current_offset + 1

                # Give priority to live playback: pause background fetch
                # ONLY if a DIFFERENT movie is actively streaming.
                # If the user is streaming THIS movie, we want to download it
                # as fast as possible to catch up and cache ahead of the play-head!
                live_movies = getattr(self.streamer, "live_movie_ids", set())
                while any(mid != self.movie_id for mid in live_movies):
                    await asyncio.sleep(0.5)
                    if self._seek_event.is_set():
                        break  # re-check seek/offset before resuming below
                    live_movies = getattr(self.streamer, "live_movie_ids", set())

                # Alternate clients per chunk — each chunk independently
                # picks whichever session is least recently used pool-wide,
                # so this task naturally interleaves with both its own
                # other chunks and any concurrent downloads/live streams.
                if hasattr(self.streamer.client, "pick"):
                    c_idx, c = await self.streamer.client.pick()
                else:
                    c_idx, c = None, self.streamer.client

                try:
                    msg  = await self._fresh_msg(c)
                    data = bytearray()
                    async with self._semaphore:
                        # PREFETCH_CHUNK is the logical batch size, but Telegram's
                        # GetFile limit is capped at TG_MAX_LIMIT (1MB). We therefore
                        # loop internally to fetch the full 2MB batch in two 1MB requests.
                        bytes_remaining = chunk_len
                        current_pos = current_offset
                        while bytes_remaining > 0:
                            request_chunk = min(TG_MAX_LIMIT, bytes_remaining)
                            # Re-pick client for each sub-chunk within the prefetch batch
                            # to ensure even distribution across sessions when downloading
                            # large files that require multiple GetFile calls per chunk.
                            if hasattr(self.streamer.client, "pick") and current_pos > current_offset:
                                c_idx, c = await self.streamer.client.pick()

                            start_mb = current_pos / 1024 / 1024
                            end_mb = (current_pos + request_chunk) / 1024 / 1024
                            c_num = c_idx if c_idx is not None else 0
                            print(f"[dl:{self.movie_id}] client {c_num} downloading {start_mb:.1f}-{end_mb:.1f} MB")

                            async for piece in self.streamer.yield_file(
                                msg,
                                offset=current_pos,
                                first_cut=0,
                                last_cut=request_chunk,
                                parts=1,
                                chunk=TG_MAX_LIMIT,
                                c=c,
                                c_idx=c_idx,
                            ):
                                data.extend(piece)
                            bytes_remaining -= request_chunk
                            current_pos += request_chunk

                    if not data:
                        raise Exception("No data from Telegram")

                    await self.sparse.pwrite(bytes(data), current_offset)
                    self.dl_map.add(current_offset, current_offset + len(data) - 1)
                    await self._persist_map()
                    self._progress_event.set()
                    self._progress_event = asyncio.Event()
                    self._error_backoff = 1.0
                    self._consecutive_errors = 0
                    self._alerted_stuck = False

                    try:
                        from metrics import metrics
                        metrics.total_downloaded_mb += len(data) / 1024 / 1024
                    except Exception:
                        pass

                    await asyncio.sleep(DL_THROTTLE_S)

                except asyncio.CancelledError:
                    print(f"[dl:{self.movie_id}] cancelled at {current_offset/1024/1024:.1f}MB")
                    return
                except Exception as e:
                    backoff_s = DL_MIN_BACKOFF * self._error_backoff
                    self._error_backoff = min(self._error_backoff * 2, 8)
                    self._consecutive_errors += 1
                    print(f"[dl:{self.movie_id}] error at {current_offset/1024/1024:.1f}MB: {e}, backoff {backoff_s:.1f}s")
                    if self._consecutive_errors >= 5 and not self._alerted_stuck and self._alert_fn:
                        self._alerted_stuck = True
                        await self._alert_fn(
                            f"⚠️ Download stuck: {self.movie_id}\n"
                            f"{self._consecutive_errors} consecutive failures at "
                            f"{current_offset/1024/1024:.1f}MB — last error: {e}"
                        )
                    await asyncio.sleep(backoff_s)
                    self._msg = None
                    continue

                current_offset = chunk_end + 1

                # If we reached EOF but the file is not fully cached, wrap around to download gaps
                if current_offset >= self.file_size:
                    if not self.dl_map.has_range(0, self.file_size - 1):
                        next_gap = self._find_next_gap(0)
                        if next_gap < self.file_size:
                            current_offset = next_gap
                            print(f"[dl:{self.movie_id}] reached EOF with gaps; wrapping around to download from {current_offset/1024/1024:.1f}MB")

            # EOF
            self._done = True
            await self.redis.set(R_DL_DONE.format(self.movie_id), "1")
            print(f"[dl:{self.movie_id}] complete {self.dl_map.total_bytes()/1024/1024:.1f}MB cached")
            try:
                from metrics import metrics
                metrics.downloads_completed += 1
            except Exception:
                pass
        finally:
            self._finished_at = time.time()
            try:
                from metrics import metrics
                metrics.downloads_active = max(0, metrics.downloads_active - 1)
            except Exception:
                pass


    def _find_next_gap(self, from_offset: int) -> int:
        """Find first byte >= from_offset not in dl_map."""
        ivs = self.dl_map._ivs
        if not ivs:
            return (from_offset // PREFETCH_CHUNK) * PREFETCH_CHUNK
        candidate = (from_offset // PREFETCH_CHUNK) * PREFETCH_CHUNK
        for s, e in ivs:
            if candidate < s:
                return candidate
            if s <= candidate <= e:
                candidate = e + 1
        return candidate

    async def _persist_map(self):
        """Save interval map to Redis for crash recovery."""
        await self.redis.set(
            R_DL_MAP.format(self.movie_id),
            self.dl_map.to_json(),
            ex=86400,   # 24h TTL — sparse file lives in /tmp
        )
        await self.redis.set(
            R_DL_TS.format(self.movie_id),
            str(time.time()),
            ex=86400,
        )


# ────────────────────────────────────────────────────────────────────────[...]
# DownloadManager: registry of active DownloadTasks
# ────────────────────────────────────────────────────────────────────────[...]
class DownloadManager:
    """
    Singleton registry. main.py imports and uses this directly.
    Handles task creation, dedup, eviction.

    Up to MAX_CONCURRENT_DOWNLOADS movies may background-download at once
    (default: min(2, pool size)). Previously this was hard-capped at 1
    regardless of pool size, which left extra sessions idle for prefetch
    whenever only one movie was actively caching.
    """

    def __init__(self):
        self._tasks: dict[str, DownloadTask] = {}
        self._maps:  dict[str, DownloadMap]  = {}
        self._files: dict[str, SparseFile]   = {}
        self._lock   = asyncio.Lock()
        # Sized to client pool — each session has its own throttle/cooldown
        # bucket in streamer.py, so we can safely run one download per
        # client concurrently instead of serialising everything through 1.
        self._dl_semaphore = asyncio.Semaphore(1)  # placeholder, resized in init_pool_size()
        self._active_movie_ids: set[str] = set()   # movies with a live DownloadTask
        self._active_task_mids: set[str] = set()    # movies currently allowed to actively download
        self._priority_mids: set[str] = set()        # of the above, which were started as priority
        self._max_concurrent_downloads = 1           # placeholder, resized in init_pool_size()
        self.paused = False    # set via /pause admin command — blocks new prefetch starts
        self.on_alert = None   # optional async fn(text) for health/failure notifications set by main.py

    def init_pool_size(self):
        """Call once after client_pool.start() so the semaphore reflects
        the real number of sessions. Safe to call multiple times."""
        n = max(1, len(client_pool))
        self._dl_semaphore = asyncio.Semaphore(n)
        default_concurrent = min(2, n)
        self._max_concurrent_downloads = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", str(default_concurrent)))
        print(f"[dm] download semaphore sized to {n} (matches client pool), "
              f"max concurrent background downloads = {self._max_concurrent_downloads}")

    async def _fire_alert(self, text: str):
        if self.on_alert:
            try:
                await self.on_alert(text)
            except Exception as e:
                print(f"[dm] alert hook failed: {e}")

    async def get_or_create(
        self,
        movie_id: str,
        file_size: int,
        message_id: int,
        redis: aioredis.Redis,
        byte_streamer,
        fetch_msg_fn,
        priority: bool = False,
    ) -> Optional[DownloadTask]:
        async with self._lock:
            task = self._tasks.get(movie_id)
            if task and task._task and not task._task.done():
                return task  # Already downloading this movie

            # ── Fast-path: file fully cached — skip Telegram entirely ─────────
            sparse_path = STORAGE_DIR / f"{movie_id}.bin"
            if sparse_path.exists():
                done_val = await redis.get(R_DL_DONE.format(movie_id))
                if done_val == b"1":
                    dl_map = await self._load_map(movie_id, redis)
                    self._maps[movie_id] = dl_map
                    if movie_id not in self._files:
                        self._files[movie_id] = SparseFile(sparse_path)
                    print(f"[dl:{movie_id}] fully cached — skipping downloader")
                    return None
                # Fallback: interval coverage check (Redis flag may be missing after crash)
                dl_map = await self._load_map(movie_id, redis)
                if dl_map.has_range(0, file_size - 1):
                    self._maps[movie_id] = dl_map
                    if movie_id not in self._files:
                        self._files[movie_id] = SparseFile(sparse_path)
                    await redis.set(R_DL_DONE.format(movie_id), b"1")
                    print(f"[dl:{movie_id}] fully cached (map verify) — skipping downloader")
                    return None
            # ─────────────────────────────────────────────────────────────────

            # Up to _max_concurrent_downloads movies may actively download
            # at once. Priority requests (user pressed play) preempt a
            # lower-priority (prefetch) download when at capacity — the
            # preempted task's partial progress is already persisted to
            # Redis, so it simply resumes later. Two priority requests
            # never fight over this since actual playback also has the
            # live-MTProto proxy fallback path and doesn't strictly need
            # the bg task.
            live_active = {
                mid for mid in self._active_task_mids
                if (t := self._tasks.get(mid)) and t._task and not t._task.done()
            }
            if movie_id not in live_active and len(live_active) >= self._max_concurrent_downloads:
                if priority:
                    # Preempt a non-priority slot first; only fall back to
                    # preempting another priority download if every active
                    # slot happens to be priority (rare).
                    victim = next((m for m in live_active if m not in self._priority_mids), None)
                    victim = victim or next(iter(live_active), None)
                    if victim:
                        print(f"[dm] preempting {victim} for priority download {movie_id}")
                        self._tasks[victim].cancel()
                else:
                    return None  # low-priority (prefetch) — wait your turn

            # Restore map from Redis if exists (crash recovery)
            dl_map = await self._load_map(movie_id, redis)

            # Check if local file still valid (may have been wiped on restart)
            sparse_path = STORAGE_DIR / f"{movie_id}.bin"
            if not sparse_path.exists():
                # Disk wiped — reset map
                dl_map = DownloadMap()
                await redis.delete(R_DL_MAP.format(movie_id))
                await redis.delete(R_DL_DONE.format(movie_id))

            sparse = await SparseFile.create(sparse_path, file_size)
            self._files[movie_id] = sparse
            self._maps[movie_id]  = dl_map

            dt = DownloadTask(
                movie_id=movie_id,
                file_size=file_size,
                sparse=sparse,
                dl_map=dl_map,
                redis=redis,
                byte_streamer=byte_streamer,
                fetch_msg_fn=fetch_msg_fn,
                message_id=message_id,
                dl_semaphore=self._dl_semaphore,
                alert_fn=self._fire_alert,
            )
            dt.start()
            self._tasks[movie_id] = dt
            self._active_movie_ids.add(movie_id)
            self._active_task_mids.add(movie_id)
            if priority:
                self._priority_mids.add(movie_id)

            def _on_done(_t, mid=movie_id):
                self._active_movie_ids.discard(mid)
                self._active_task_mids.discard(mid)
                self._priority_mids.discard(mid)
            dt._task.add_done_callback(_on_done)

            # Update access timestamp for LRU eviction
            await redis.set(R_DL_TS.format(movie_id), str(time.time()), ex=86400)

            return dt

    def get(self, movie_id: str) -> Optional[DownloadTask]:
        return self._tasks.get(movie_id)

    def get_map(self, movie_id: str) -> Optional[DownloadMap]:
        return self._maps.get(movie_id)

    def get_file(self, movie_id: str) -> Optional[SparseFile]:
        return self._files.get(movie_id)

    async def evict(self, movie_id: str, redis: aioredis.Redis):
        """Cancel task, delete local file, clear Redis download state."""
        async with self._lock:
            task = self._tasks.pop(movie_id, None)
            if task:
                task.cancel()
            f = self._files.pop(movie_id, None)
            if f:
                await f.delete()
            self._maps.pop(movie_id, None)
        await redis.delete(
            R_DL_MAP.format(movie_id),
            R_DL_DONE.format(movie_id),
            R_DL_PATH.format(movie_id),
            R_DL_TS.format(movie_id),
        )
        print(f"[dm] evicted {movie_id}")

    async def evict_lru_if_needed(self, redis: aioredis.Redis):
        """Evict oldest accessed movies if total local storage > MAX_LOCAL_GB."""
        total = sum(
            f.path.stat().st_size
            for f in self._files.values()
            if f.path.exists()
        )
        limit = MAX_LOCAL_GB * 1024 ** 3
        if total <= limit:
            return

        await self._fire_alert(
            f"🗄 Disk pressure: {total/1024**3:.1f}GB used (limit {MAX_LOCAL_GB:.0f}GB) — evicting LRU entries"
        )

        # Build LRU order from Redis timestamps
        order = []
        for mid in list(self._files.keys()):
            ts = await redis.get(R_DL_TS.format(mid))
            order.append((float(ts) if ts else 0.0, mid))
        order.sort()

        for _, mid in order:
            if total <= limit:
                break
            f = self._files.get(mid)
            size = f.path.stat().st_size if f and f.path.exists() else 0
            await self.evict(mid, redis)
            total -= size
            print(f"[dm] LRU evict {mid} freed {size/1024/1024:.0f}MB")

    async def prune_finished_tasks(self, max_idle_s: float = 3600):
        """Drop DownloadTask/Map/SparseFile entries for movies finished and
        untouched for a while. self._tasks otherwise keeps every finished
        task object forever, growing unbounded on a large catalog."""
        now = time.time()
        removed = 0
        for mid in list(self._tasks.keys()):
            task = self._tasks.get(mid)
            if not task or not task._task or not task._task.done():
                continue  # still running, leave alone
            if mid in self._active_task_mids:
                continue
            finished_at = getattr(task, "_finished_at", None)
            if finished_at is None:
                continue  # hasn't gone through _run's finally yet
            if (now - finished_at) > max_idle_s:
                self._tasks.pop(mid, None)
                self._maps.pop(mid, None)
                self._files.pop(mid, None)
                removed += 1
        if removed:
            print(f"[dm] pruned {removed} stale finished task entr{'y' if removed == 1 else 'ies'}")
        return removed

    async def shutdown(self):
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(
            *[t._task for t in self._tasks.values() if t._task],
            return_exceptions=True,
        )

    async def _load_map(self, movie_id: str, redis: aioredis.Redis) -> DownloadMap:
        raw = await redis.get(R_DL_MAP.format(movie_id))
        if raw:
            try:
                return DownloadMap.from_json(raw)
            except Exception:
                pass
        return DownloadMap()

    def stats(self) -> dict:
        result = {}
        for mid, task in self._tasks.items():
            f = self._files.get(mid)
            dm = self._maps.get(mid)
            size_on_disk = f.path.stat().st_size if f and f.path.exists() else 0
            result[mid] = {
                "done":           task.is_done(),
                "downloaded_mb":  round(dm.total_bytes() / 1024 / 1024, 1) if dm else 0,
                "size_on_disk_mb": round(size_on_disk / 1024 / 1024, 1),
                "task_running":   bool(task._task and not task._task.done()),
            }
        return result

    async def hydrate_cached(self, movie_id: str, file_size: int, redis: aioredis.Redis) -> bool:
        """
        Returns True if the file is fully downloaded locally and ready to serve.
        Side-effect: ensures self._maps/_files are populated for this movie_id
        so proxy Path A can pread immediately.
        Never touches Telegram.
        """
        sparse_path = STORAGE_DIR / f"{movie_id}.bin"
        if not sparse_path.exists():
            return False

        # In-memory map already covers full range?
        dl_map = self.get_map(movie_id)
        if dl_map and dl_map.has_range(0, file_size - 1):
            return True

        # Cheap Redis flag check
        done_val = await redis.get(R_DL_DONE.format(movie_id))
        if done_val != b"1":
            # Last resort: load map and verify coverage
            dl_map = await self._load_map(movie_id, redis)
            if not dl_map.has_range(0, file_size - 1):
                return False
            # Coverage confirmed — backfill flag
            await redis.set(R_DL_DONE.format(movie_id), b"1")

        # Hydrate in-memory state so Path A works
        if self.get_map(movie_id) is None:
            dl_map = await self._load_map(movie_id, redis)
            self._maps[movie_id] = dl_map
        if self.get_file(movie_id) is None:
            self._files[movie_id] = SparseFile(sparse_path)

        return True


# Module-level singleton — imported by main.py
download_manager = DownloadManager()
