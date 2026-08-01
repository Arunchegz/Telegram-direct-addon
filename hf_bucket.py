"""
hf_bucket.py — Persistent streaming via a public HuggingFace bucket.

The Space mounts the bucket at /data (read-write). CAVEAT: the mount snapshots
files at creation — sparse prefetch files (all zeros until written) land in the
bucket as all-zero blobs and are never re-synced. So this module explicitly
uploads the completed local file to the bucket via batch_bucket_files, which
overwrites the zero blob with real bytes, then verifies public reachability and
registers the resolve URL in Redis; the proxy then 302-redirects completed-file
streams to the bucket CDN.

Bucket resolve URLs support anonymous Range requests (206), so players stream
directly from HF — zero Telegram cost, survives restarts and local wipes.

Env config:
  HF_BUCKET_ID      — bucket id, e.g. "arunchegz1/Telegram_stremio-storage"
  HF_BUCKET_PREFIX  — key prefix of STORAGE_DIR inside the mount (default: tgstream)
  HF_STREAM_ENABLED — master switch (default true)
  HF_URL_TTL        — Redis URL TTL in seconds (default 30 days)
  HF_TOKEN          — HF token with bucket write access (required for upload/delete)

Redis state:
  tgstream:hf:url:{movie_id}      -> resolve URL (TTL 30 days, refreshed on hit)
  tgstream:hf:uploaded:{movie_id} -> set after a successful explicit upload
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
# Required for uploads (overwriting the mount's zero blobs) and deletion.
HF_TOKEN = os.getenv("HF_TOKEN", "").strip()

R_HF_URL = "tgstream:hf:url:{}"
R_HF_DONE = "tgstream:hf:uploaded:{}"
_HF_BUCKETS_BASE = "https://huggingface.co/buckets"
_MAX_REGISTER_ATTEMPTS = 5
_VERIFY_ACCEPTED = (200, 302)  # 302 = redirect to signed CDN URL = object exists
_BACKOFF = (5, 15, 45, 120, 240)


class HfUploader:
    """Uploads completed files to the bucket and registers their URLs."""

    def __init__(self):
        self._urls: dict[str, str] = {}      # movie_id -> resolve URL (in-memory cache)
        self._started: set[str] = set()      # movie_ids with an in-flight upload/registration
        self._uploaded: set[str] = set()     # movie_ids confirmed uploaded this process

    # ── Config ───────────────────────────────────────────────────────────────
    @property
    def enabled(self) -> bool:
        return HF_STREAM_ENABLED and bool(HF_BUCKET_ID) and bool(HF_TOKEN)

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
        self._uploaded.discard(movie_id)
        await redis.delete(R_HF_URL.format(movie_id), R_HF_DONE.format(movie_id))

    # ── Deletion ─────────────────────────────────────────────────────────────
    def delete_remote(self, movie_id: str, file_name: str, redis):
        """Delete the movie's blob(s) from the bucket (fire-and-forget).
        Both the canonical extension and legacy .bin keys are removed — buckets
        are unversioned, so this is permanent. Only called on intentional
        deletes, never on LRU eviction."""
        if not HF_TOKEN:
            print(f"[hf] cannot delete {movie_id} from bucket — HF_TOKEN not set")
            return
        if movie_id in self._started:
            return  # upload/registration in flight — skip to avoid racing
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

    # ── Upload + registration ────────────────────────────────────────────────
    def ensure_upload(self, movie_id: str, local_path: Path, file_name: str,
                      redis, on_done=None):
        """Upload the completed file to the bucket and register its URL.
        Never blocks. No-op if already uploaded this process or in-flight."""
        if not self.enabled or not local_path.exists():
            return
        if movie_id in self._uploaded or movie_id in self._started:
            return
        self._started.add(movie_id)
        task = asyncio.create_task(
            self._upload_and_register(movie_id, local_path, file_name, redis, on_done),
            name=f"hf-upload:{movie_id}",
        )
        task.add_done_callback(self._log)

    @staticmethod
    def _log(task: asyncio.Task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[hf] upload task failed: {type(e).__name__}: {e}")

    async def _upload_and_register(self, movie_id: str, local_path: Path,
                                   file_name: str, redis, on_done=None):
        url = self.resolve_url(movie_id, file_name)
        key = f"{HF_BUCKET_PREFIX}/{movie_id}{Path(url).suffix}"
        try:
            # Fast path: already explicitly uploaded on a previous run.
            if await redis.get(R_HF_DONE.format(movie_id)) and await self._verify(url):
                self._uploaded.add(movie_id)
                await self._register(movie_id, url, redis, on_done)
                return
        except Exception as e:
            print(f"[hf] redis check failed for {movie_id}: {e}")

        for attempt, wait in enumerate(_BACKOFF, start=1):
            try:
                await self._upload(local_path, key)
                if await self._verify(url):
                    await self._register(movie_id, url, redis, on_done)
                    try:
                        await redis.set(R_HF_DONE.format(movie_id), "1", ex=HF_URL_TTL)
                    except Exception as e:
                        print(f"[hf] upload flag set failed for {movie_id}: {e}")
                    self._uploaded.add(movie_id)
                    return
            except asyncio.CancelledError:
                self._started.discard(movie_id)
                raise
            except Exception as e:
                print(f"[hf] upload attempt {attempt} failed for {movie_id}: {type(e).__name__}: {e}")
            print(f"[hf] {movie_id} upload not confirmed (attempt {attempt}/{_MAX_REGISTER_ATTEMPTS})"
                  f" — retrying in {wait}s")
            await asyncio.sleep(wait)
        # Give up for now — allow a later trigger (next proxy/prefetch pass) to retry.
        self._started.discard(movie_id)

    async def _upload(self, local_path: Path, key: str):
        """Upload the completed file to the bucket key, overwriting the mount's
        zero blob. Reads the real data from the (now complete) sparse file."""
        from huggingface_hub import batch_bucket_files
        print(f"[hf] uploading {local_path.name} -> {HF_BUCKET_ID}/{key}"
              f" ({local_path.stat().st_size} bytes)")
        await asyncio.to_thread(
            batch_bucket_files, HF_BUCKET_ID, add=[(str(local_path), key)], token=HF_TOKEN,
        )
        print(f"[hf] upload OK: {key}")

    async def _register(self, movie_id: str, url: str, redis, on_done=None):
        await redis.set(R_HF_URL.format(movie_id), url, ex=HF_URL_TTL)
        self._urls[movie_id] = url
        print(f"[hf] registered {movie_id} -> {url}")
        if on_done:
            await self._call(on_done, movie_id, url)

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
