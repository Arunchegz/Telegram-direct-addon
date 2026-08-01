"""
hf_bucket.py — Persistent storage via a public HuggingFace dataset bucket.

When a prefetch completes, the file is uploaded to a public HF dataset repo
(HF_REPO_ID, e.g. "username/tgstream-cache"). Completed files are then
streamed straight from the HF CDN resolve URL, which survives restarts and
local disk wipes — that is the persistent layer on top of STORAGE_DIR.

Env config:
  HF_TOKEN            — write token (required to upload)
  HF_REPO_ID          — dataset repo id, e.g. "username/tgstream-cache"
  HF_STREAM_ENABLED   — master switch (default true)
  HF_UPLOAD_CONCURRENCY — parallel uploads (default 2)

Redis state:
  tgstream:hf:url:{movie_id} -> resolve URL (TTL 30 days, refreshed on hit)
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import httpx

HF_TOKEN = os.getenv("HF_TOKEN", "").strip()
HF_REPO_ID = os.getenv("HF_REPO_ID", "").strip().strip("/")
HF_STREAM_ENABLED = os.getenv("HF_STREAM_ENABLED", "true").strip().lower() == "true"
HF_UPLOAD_CONCURRENCY = int(os.getenv("HF_UPLOAD_CONCURRENCY", "2"))
HF_URL_TTL = int(os.getenv("HF_URL_TTL", str(30 * 86400)))

R_HF_URL = "tgstream:hf:url:{}"
_HF_CDN_BASE = "https://huggingface.co/datasets"
_MAX_UPLOAD_ATTEMPTS = 5


def sanitize_filename(fn: str) -> str:
    fn = fn.replace("\\", "_").replace("/", "_")
    fn = "".join(ch for ch in fn if ch.isprintable() and ch not in ':"<>|?*').strip()
    return fn or "video.bin"


class HfUploader:
    """Fire-and-forget uploader with in-process + Redis dedupe."""

    def __init__(self):
        self._sem = asyncio.Semaphore(HF_UPLOAD_CONCURRENCY)
        self._urls: dict[str, str] = {}      # movie_id -> resolve URL (in-memory cache)
        self._started: set[str] = set()      # movie_ids with an in-flight/queued upload
        self._api = None                     # lazy huggingface_hub.HfApi

    # ── Config ───────────────────────────────────────────────────────────────
    @property
    def enabled(self) -> bool:
        return HF_STREAM_ENABLED and bool(HF_REPO_ID) and bool(HF_TOKEN)

    def resolve_url(self, movie_id: str, file_name: str) -> str:
        return f"{_HF_CDN_BASE}/{HF_REPO_ID}/resolve/main/{movie_id}/{sanitize_filename(file_name)}"

    # ── State ────────────────────────────────────────────────────────────────
    async def get_uploaded_url(self, movie_id: str, redis) -> str | None:
        """Resolve URL if the file was uploaded — memory first, then Redis."""
        url = self._urls.get(movie_id)
        if url:
            return url
        raw = await redis.get(R_HF_URL.format(movie_id))
        if raw:
            url = raw.decode()
            self._urls[movie_id] = url
        return url

    async def forget(self, movie_id: str, redis):
        """Drop all HF state for a movie (called on evict/delete)."""
        self._urls.pop(movie_id, None)
        self._started.discard(movie_id)
        await redis.delete(R_HF_URL.format(movie_id))

    # ── Upload ───────────────────────────────────────────────────────────────
    def ensure_upload(self, movie_id: str, local_path: Path, file_name: str,
                      redis, on_done=None):
        """Schedule an upload if not already uploaded/queued. Never blocks."""
        if not self.enabled or not local_path.exists():
            return
        if movie_id in self._urls or movie_id in self._started:
            return
        self._started.add(movie_id)
        task = asyncio.create_task(
            self._upload(movie_id, local_path, file_name, redis, on_done),
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

    async def _upload(self, movie_id: str, local_path: Path, file_name: str,
                      redis, on_done=None):
        url = self.resolve_url(movie_id, file_name)
        try:
            # Already uploaded on a previous run? Redis survives restarts.
            raw = await redis.get(R_HF_URL.format(movie_id))
            if raw:
                self._urls[movie_id] = raw.decode()
                if on_done:
                    await self._call(on_done, movie_id, url)
                return
        except Exception as e:
            print(f"[hf] redis check failed for {movie_id}: {e}")

        async with self._sem:
            for attempt in range(1, _MAX_UPLOAD_ATTEMPTS + 1):
                try:
                    await asyncio.to_thread(self._upload_sync, movie_id, local_path, file_name)
                    if not await self._verify(url):
                        raise RuntimeError("HEAD verification failed")
                    await redis.set(R_HF_URL.format(movie_id), url, ex=HF_URL_TTL)
                    self._urls[movie_id] = url
                    print(f"[hf] uploaded {movie_id} -> {url}")
                    if on_done:
                        await self._call(on_done, movie_id, url)
                    return
                except Exception as e:
                    wait = min(5 * attempt, 120)
                    print(f"[hf] upload {movie_id} attempt {attempt}/{_MAX_UPLOAD_ATTEMPTS} failed: "
                          f"{type(e).__name__}: {e} — retrying in {wait}s")
                    await asyncio.sleep(wait)
        # Give up for now — allow a later trigger (next proxy/prefetch pass) to retry.
        self._started.discard(movie_id)

    def _upload_sync(self, movie_id: str, local_path: Path, file_name: str):
        from huggingface_hub import HfApi
        from huggingface_hub.errors import UploadSkippedError
        if self._api is None:
            self._api = HfApi(token=HF_TOKEN)
        path_in_repo = f"{movie_id}/{sanitize_filename(file_name)}"
        try:
            self._api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=path_in_repo,
                repo_id=HF_REPO_ID,
                repo_type="dataset",
            )
        except UploadSkippedError:
            pass  # identical file already in repo — treat as success

    async def _verify(self, url: str) -> bool:
        """HEAD the CDN resolve URL to confirm the file is really public."""
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
                r = await c.head(url)
                return r.status_code == 200
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
