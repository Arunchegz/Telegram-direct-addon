"""
hf_bucket.py — Persistent streaming via a public HuggingFace bucket.

The Space mounts the bucket at /data (read-write), so every completed prefetch
file stored under STORAGE_DIR automatically lands in the bucket — no upload
code needed. This module verifies the file is publicly reachable and registers
its resolve URL in Redis; the proxy then 302-redirects completed-file streams
to the bucket CDN.

Bucket resolve URLs support anonymous Range requests (206), so players stream
directly from HF — zero Telegram cost, survives restarts and local wipes.

Env config:
  HF_BUCKET_ID      — bucket id, e.g. "arunchegz1/Telegram_stremio-storage"
  HF_BUCKET_PREFIX  — key prefix of STORAGE_DIR inside the mount (default: tgstream)
  HF_STREAM_ENABLED — master switch (default true)
  HF_URL_TTL        — Redis URL TTL in seconds (default 30 days)

Redis state:
  tgstream:hf:url:{movie_id} -> resolve URL (TTL 30 days, refreshed on hit)
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

import httpx

HF_BUCKET_ID = os.getenv("HF_BUCKET_ID", "").strip().strip("/")
HF_BUCKET_PREFIX = os.getenv("HF_BUCKET_PREFIX", "tgstream").strip().strip("/")
HF_STREAM_ENABLED = os.getenv("HF_STREAM_ENABLED", "true").strip().lower() == "true"
HF_URL_TTL = int(os.getenv("HF_URL_TTL", str(30 * 86400)))
# Only needed for deletion of bucket files (write access). Streaming needs none.
HF_TOKEN = os.getenv("HF_TOKEN", "").strip()

R_HF_URL = "tgstream:hf:url:{}"
_HF_BUCKETS_BASE = "https://huggingface.co/buckets"
_MAX_REGISTER_ATTEMPTS = 5
_VERIFY_ACCEPTED = (200, 302)  # 302 = redirect to signed CDN URL = object exists


class HfUploader:
    """Verifies completed files are live on the bucket and registers their URLs."""

    def __init__(self):
        self._urls: dict[str, str] = {}      # movie_id -> resolve URL (in-memory cache)
        self._started: set[str] = set()      # movie_ids with an in-flight registration

    # ── Config ───────────────────────────────────────────────────────────────
    @property
    def enabled(self) -> bool:
        return HF_STREAM_ENABLED and bool(HF_BUCKET_ID)

    def resolve_url(self, movie_id: str, file_name: str = "") -> str:
        # Bucket key mirrors STORAGE_DIR inside the /data mount: {prefix}/{movie_id}.{ext}
        ext = "bin"
        if file_name:
            suffix = Path(file_name).suffix.lower().lstrip(".")
            if suffix and len(suffix) <= 8:
                ext = suffix
        return f"{_HF_BUCKETS_BASE}/{HF_BUCKET_ID}/resolve/{HF_BUCKET_PREFIX}/{movie_id}.{ext}"

    # ── State ────────────────────────────────────────────────────────────────
    async def get_uploaded_url(self, movie_id: str, redis) -> str | None:
        """Resolve URL if the file is live on the bucket — memory first, then Redis."""
        url = self._urls.get(movie_id)
        if url:
            return url
        raw = await redis.get(R_HF_URL.format(movie_id))
        if raw:
            url = raw.decode()
            self._urls[movie_id] = url
        return url

    async def forget(self, movie_id: str, redis):
        """Drop all bucket state for a movie (called on evict/delete)."""
        self._urls.pop(movie_id, None)
        self._started.discard(movie_id)
        await redis.delete(R_HF_URL.format(movie_id))

    # ── Deletion ─────────────────────────────────────────────────────────────
    def delete_remote(self, movie_id: str, file_name: str, redis):
        """Delete the movie's blob(s) from the bucket (fire-and-forget).
        Requires HF_TOKEN with write access. Both the canonical extension and
        legacy .bin keys are removed — buckets are unversioned, so this is
        permanent. Only called on intentional deletes, never on LRU eviction."""
        if not HF_TOKEN:
            print(f"[hf] cannot delete {movie_id} from bucket — HF_TOKEN not set")
            return
        if movie_id in self._started:
            return  # registration in flight — skip to avoid racing
        paths = {f"{HF_BUCKET_PREFIX}/{movie_id}.bin"}
        if file_name:
            suffix = Path(file_name).suffix.lower().lstrip(".")
            if suffix and len(suffix) <= 8:
                paths.add(f"{HF_BUCKET_PREFIX}/{movie_id}.{suffix}")
        task = asyncio.create_task(self._delete_remote(movie_id, list(paths)), name=f"hf-delete:{movie_id}")
        task.add_done_callback(self._log)

    async def _delete_remote(self, movie_id: str, paths: list[str]):
        try:
            from huggingface_hub import batch_bucket_files
            await asyncio.to_thread(batch_bucket_files, HF_BUCKET_ID, delete=paths, token=HF_TOKEN)
            print(f"[hf] deleted from bucket: {', '.join(paths)}")
        except Exception as e:
            print(f"[hf] bucket delete failed for {movie_id} ({paths}): {type(e).__name__}: {e}")

    # ── Registration ─────────────────────────────────────────────────────────
    def ensure_upload(self, movie_id: str, local_path: Path, file_name: str,
                      redis, on_done=None):
        """Verify the completed file is publicly visible in the bucket and
        register its URL. Never blocks. No-op if already registered."""
        if not self.enabled or not local_path.exists():
            return
        if movie_id in self._urls or movie_id in self._started:
            return
        self._started.add(movie_id)
        task = asyncio.create_task(
            self._register(movie_id, file_name, redis, on_done),
            name=f"hf-register:{movie_id}",
        )
        task.add_done_callback(self._log)

    @staticmethod
    def _log(task: asyncio.Task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[hf] register task failed: {type(e).__name__}: {e}")

    async def _register(self, movie_id: str, file_name: str, redis, on_done=None):
        # Try the canonical (extension-aware) URL first, then legacy .bin —
        # pre-extension builds stored files as {movie_id}.bin in the bucket.
        urls = [self.resolve_url(movie_id, file_name)]
        legacy = self.resolve_url(movie_id, "")
        if legacy not in urls:
            urls.append(legacy)
        try:
            # Already registered on a previous run? Redis survives restarts.
            raw = await redis.get(R_HF_URL.format(movie_id))
            if raw:
                self._urls[movie_id] = raw.decode()
                if on_done:
                    await self._call(on_done, movie_id, raw.decode())
                return
        except Exception as e:
            print(f"[hf] redis check failed for {movie_id}: {e}")

        for attempt in range(1, _MAX_REGISTER_ATTEMPTS + 1):
            for url in urls:
                if await self._verify(url):
                    await redis.set(R_HF_URL.format(movie_id), url, ex=HF_URL_TTL)
                    self._urls[movie_id] = url
                    print(f"[hf] registered {movie_id} -> {url}")
                    if on_done:
                        await self._call(on_done, movie_id, url)
                    return
            wait = min(5 * attempt, 120)
            print(f"[hf] {movie_id} not visible in bucket yet (attempt {attempt}/{_MAX_REGISTER_ATTEMPTS})"
                  f" — mount sync may lag; retrying in {wait}s")
            await asyncio.sleep(wait)
        # Give up for now — allow a later trigger (next proxy/prefetch pass) to retry.
        self._started.discard(movie_id)

    async def _verify(self, url: str) -> bool:
        """HEAD the bucket resolve URL — 200/302 means the object is public."""
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.head(url)
                return r.status_code in _VERIFY_ACCEPTED
        except Exception as e:
            print(f"[hf] verify failed for {url}: {e}")
            return False

    @staticmethod
    async def _call(fn, *args):
        try:
            res = fn(*args)
            if asyncio.iscoroutine(res):
                await res
        except Exception as e:
            print(f"[hf] on_done hook failed: {type(e).__name__}: {e}")


# Module-level singleton — imported by main.py / downloader.py
hf_uploader = HfUploader()
