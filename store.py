"""
store.py — Hybrid store: Redis for persistent state, local files for download maps,
in-memory LRU for poster cache.

Redis usage (low-frequency only):
  - tgstream:movies hash       (sync only, rare)
  - tgstream:dl:done:*         (one write per completed download)
  - tgstream:dl:stopped:*      (one write per manual evict/pause)
  - tgstream:last_sync etc.    (sync timestamps, very rare)

Local JSON (STORAGE_DIR/dl_maps.json):
  - tgstream:dl:map:*          (crash recovery, written debounced 10s)
  - tgstream:dl:ts:*           (LRU timestamps, in-memory + local JSON)

In-memory only:
  - tgstream:poster:*          (24h TTL dict, lost on restart — refetched cheaply)
  - tgstream:imdb:*            (same)

Drop-in: all callers use appstate.redis_client which is a HybridStore instance.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

log = logging.getLogger("tgstream.store")

STORAGE_DIR   = Path(os.getenv("STORAGE_DIR", str(Path.home() / "tgstream_storage")))
DL_STATE_FILE = STORAGE_DIR / "dl_state.json"

_PERSIST_DEBOUNCE_S = 10.0

# Key prefixes routed to local storage instead of Redis
_LOCAL_PREFIXES = (
    "tgstream:dl:map:",
    "tgstream:dl:ts:",
)
# Key prefixes kept in-memory only (poster/imdb cache)
_MEM_PREFIXES = (
    "tgstream:poster:",
    "tgstream:imdb:",
)

def _to_bytes(v: Any) -> bytes:
    if isinstance(v, bytes):
        return v
    return str(v).encode()


class HybridStore:
    """
    Wraps aioredis.Redis. Routes:
      dl:map / dl:ts  → local JSON file (no Redis calls)
      poster / imdb   → in-memory dict with TTL (no Redis calls)
      everything else → Redis
    """

    def __init__(self, redis: aioredis.Redis):
        self._redis = redis
        # Local store: key → (bytes_value, expire_ts | None)
        self._local: dict[str, tuple[bytes, float | None]] = {}
        # Mem store: key → (bytes_value, expire_ts)
        self._mem: dict[str, tuple[bytes, float]] = {}
        self._dirty = False
        self._last_persist = 0.0
        self._lock = asyncio.Lock()

    # ── Routing ───────────────────────────────────────────────────────────────

    @staticmethod
    def _is_local(key: str) -> bool:
        return any(key.startswith(p) for p in _LOCAL_PREFIXES)

    @staticmethod
    def _is_mem(key: str) -> bool:
        return any(key.startswith(p) for p in _MEM_PREFIXES)

    # ── Bootstrap ─────────────────────────────────────────────────────────────

    async def load(self):
        STORAGE_DIR.mkdir(parents=True, exist_ok=True)
        data = await asyncio.to_thread(self._read_dl_state)
        async with self._lock:
            self._local = data
        log.info(f"[store] loaded {len(data)} local dl_state keys")

    def _read_dl_state(self) -> dict:
        if not DL_STATE_FILE.exists():
            return {}
        try:
            raw = json.loads(DL_STATE_FILE.read_text())
            now = time.time()
            return {
                k: (_to_bytes(v), exp)
                for k, (v, exp) in raw.items()
                if exp is None or exp > now
            }
        except Exception as e:
            log.warning(f"[store] dl_state load failed: {e}")
            return {}

    # ── Persist local state (debounced) ───────────────────────────────────────

    def _schedule_persist(self):
        self._dirty = True
        asyncio.create_task(self._maybe_persist())

    async def _maybe_persist(self):
        now = time.time()
        if now - self._last_persist < _PERSIST_DEBOUNCE_S:
            return
        self._last_persist = now
        self._dirty = False
        async with self._lock:
            snapshot = dict(self._local)
        await asyncio.to_thread(self._write_dl_state, snapshot)

    def _write_dl_state(self, snapshot: dict):
        try:
            STORAGE_DIR.mkdir(parents=True, exist_ok=True)
            now = time.time()
            out = {
                k: (v.decode() if isinstance(v, bytes) else v, exp)
                for k, (v, exp) in snapshot.items()
                if exp is None or exp > now
            }
            tmp = DL_STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(out))
            tmp.replace(DL_STATE_FILE)
        except Exception as e:
            log.error(f"[store] dl_state persist failed: {e}")

    async def force_persist(self):
        self._last_persist = 0
        if self._dirty:
            await self._maybe_persist()

    # ── TTL helper ────────────────────────────────────────────────────────────

    def _local_expired(self, key: str) -> bool:
        entry = self._local.get(key)
        if entry is None:
            return True
        _, exp = entry
        if exp is not None and exp < time.time():
            del self._local[key]
            return True
        return False

    def _mem_get(self, key: str) -> bytes | None:
        entry = self._mem.get(key)
        if entry is None:
            return None
        v, exp = entry
        if exp < time.time():
            del self._mem[key]
            return None
        return v

    # ── Redis-compatible async API ────────────────────────────────────────────

    async def get(self, key: str) -> bytes | None:
        if self._is_mem(key):
            return self._mem_get(key)
        if self._is_local(key):
            async with self._lock:
                if self._local_expired(key):
                    return None
                return self._local[key][0]
        return await self._redis.get(key)

    async def set(self, key: str, value: Any, ex: int | None = None, nx: bool = False) -> bool | None:
        if self._is_mem(key):
            exp = time.time() + (ex or 86400)
            self._mem[key] = (_to_bytes(value), exp)
            return True
        if self._is_local(key):
            exp = (time.time() + ex) if ex else None
            async with self._lock:
                if nx and not self._local_expired(key):
                    return None
                self._local[key] = (_to_bytes(value), exp)
            self._schedule_persist()
            return True
        return await self._redis.set(key, value, ex=ex, nx=nx)

    async def setex(self, key: str, seconds: int, value: Any) -> bool:
        return await self.set(key, value, ex=seconds)

    async def mget(self, *keys: str) -> list[bytes | None]:
        # Partition keys
        result = [None] * len(keys)
        redis_idx = []
        redis_keys = []

        for i, k in enumerate(keys):
            if self._is_mem(k):
                result[i] = self._mem_get(k)
            elif self._is_local(k):
                async with self._lock:
                    if not self._local_expired(k):
                        result[i] = self._local[k][0]
            else:
                redis_idx.append(i)
                redis_keys.append(k)

        if redis_keys:
            vals = await self._redis.mget(*redis_keys)
            for i, v in zip(redis_idx, vals):
                result[i] = v

        return result

    async def delete(self, *keys: str) -> int:
        count = 0
        local_dirty = False
        redis_keys = []

        for k in keys:
            if self._is_mem(k):
                if k in self._mem:
                    del self._mem[k]
                    count += 1
            elif self._is_local(k):
                async with self._lock:
                    if k in self._local:
                        del self._local[k]
                        count += 1
                        local_dirty = True
            else:
                redis_keys.append(k)

        if local_dirty:
            self._schedule_persist()
        if redis_keys:
            count += await self._redis.delete(*redis_keys)
        return count

    # ── Hash commands — always Redis ──────────────────────────────────────────

    async def hset(self, name: str, key: str, value: Any) -> int:
        return await self._redis.hset(name, key, value)

    async def hget(self, name: str, key: str) -> bytes | None:
        return await self._redis.hget(name, key)

    async def hexists(self, name: str, key: str) -> bool:
        return await self._redis.hexists(name, key)

    async def hdel(self, name: str, *keys: str) -> int:
        return await self._redis.hdel(name, *keys)

    async def hgetall(self, name: str) -> dict:
        return await self._redis.hgetall(name)

    async def hlen(self, name: str) -> int:
        return await self._redis.hlen(name)

    async def aclose(self):
        await self.force_persist()
        await self._redis.aclose()
