"""
store.py — In-process key-value store replacing Redis/Upstash.

Drop-in replacement for the redis.asyncio.Redis interface used by this app.
Backed by:
  - in-memory dicts (fast path)
  - JSON files on STORAGE_DIR (crash recovery, persists across restarts)

Supported commands (all async):
  get(key)                    → bytes | None
  set(key, value, ex=None, nx=False)   → True | None
  setex(key, seconds, value)  → True
  mget(*keys)                 → list[bytes | None]
  delete(*keys)               → int
  hexists(name, key)          → bool
  hset(name, key, value)      → int
  hget(name, key)             → bytes | None
  hdel(name, *keys)           → int
  hgetall(name)               → dict[bytes, bytes]
  hlen(name)                  → int

Not needed / not implemented: connection pool, pipeline, pubsub.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("tgstream.store")

STORAGE_DIR = Path(os.getenv("STORAGE_DIR", str(Path.home() / "tgstream_storage")))
STORE_FILE  = STORAGE_DIR / "kv_store.json"
HASH_FILE   = STORAGE_DIR / "hash_store.json"

# Persist at most once every N seconds to avoid hammering disk
_PERSIST_DEBOUNCE_S = 10.0


def _to_bytes(v: Any) -> bytes:
    if isinstance(v, bytes):
        return v
    if isinstance(v, (int, float)):
        return str(v).encode()
    if isinstance(v, str):
        return v.encode()
    raise TypeError(f"store: unsupported value type {type(v)}")


def _from_json_val(v: str | None) -> bytes | None:
    if v is None:
        return None
    return v.encode()


class LocalStore:
    """
    Async key-value store with TTL support.
    Thread-safe via asyncio.Lock (single-process only, which is the
    deployment model here — HF Spaces / Railway single instance).
    """

    def __init__(self):
        self._kv: dict[str, tuple[bytes, float | None]] = {}   # key → (value, expire_ts | None)
        self._hashes: dict[str, dict[str, bytes]] = {}          # hash_name → {field: value}
        self._lock = asyncio.Lock()
        self._kv_dirty = False
        self._hash_dirty = False
        self._last_kv_persist = 0.0
        self._last_hash_persist = 0.0
        self._loaded = False

    # ── Bootstrap ────────────────────────────────────────────────────────────

    async def load(self):
        """Load persisted state from disk. Call once at startup."""
        async with self._lock:
            if self._loaded:
                return
            STORAGE_DIR.mkdir(parents=True, exist_ok=True)
            self._kv     = await asyncio.to_thread(self._load_kv_file)
            self._hashes = await asyncio.to_thread(self._load_hash_file)
            self._loaded = True
            log.info(f"[store] loaded {len(self._kv)} kv keys, {sum(len(v) for v in self._hashes.values())} hash fields")

    def _load_kv_file(self) -> dict:
        if not STORE_FILE.exists():
            return {}
        try:
            raw = json.loads(STORE_FILE.read_text())
            now = time.time()
            result = {}
            for k, (v, exp) in raw.items():
                if exp is not None and exp < now:
                    continue  # expired
                result[k] = (v.encode() if isinstance(v, str) else v, exp)
            return result
        except Exception as e:
            log.warning(f"[store] kv load failed: {e}")
            return {}

    def _load_hash_file(self) -> dict:
        if not HASH_FILE.exists():
            return {}
        try:
            raw = json.loads(HASH_FILE.read_text())
            # Values stored as strings on disk → convert back to bytes
            return {
                hname: {f: v.encode() if isinstance(v, str) else v for f, v in fields.items()}
                for hname, fields in raw.items()
            }
        except Exception as e:
            log.warning(f"[store] hash load failed: {e}")
            return {}

    # ── Persistence (debounced) ───────────────────────────────────────────────

    def _schedule_persist_kv(self):
        self._kv_dirty = True
        asyncio.create_task(self._maybe_persist_kv())

    def _schedule_persist_hash(self):
        self._hash_dirty = True
        asyncio.create_task(self._maybe_persist_hash())

    async def _maybe_persist_kv(self):
        now = time.time()
        if now - self._last_kv_persist < _PERSIST_DEBOUNCE_S:
            return
        self._last_kv_persist = now
        self._kv_dirty = False
        snapshot = dict(self._kv)  # shallow copy under no lock (fine for dict)
        await asyncio.to_thread(self._write_kv_file, snapshot)

    async def _maybe_persist_hash(self):
        now = time.time()
        if now - self._last_hash_persist < _PERSIST_DEBOUNCE_S:
            return
        self._last_hash_persist = now
        self._hash_dirty = False
        snapshot = {k: dict(v) for k, v in self._hashes.items()}
        await asyncio.to_thread(self._write_hash_file, snapshot)

    def _write_kv_file(self, snapshot: dict):
        try:
            STORAGE_DIR.mkdir(parents=True, exist_ok=True)
            now = time.time()
            out = {}
            for k, (v, exp) in snapshot.items():
                if exp is not None and exp < now:
                    continue
                out[k] = (v.decode() if isinstance(v, bytes) else v, exp)
            tmp = STORE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(out))
            tmp.replace(STORE_FILE)
        except Exception as e:
            log.error(f"[store] kv persist failed: {e}")

    def _write_hash_file(self, snapshot: dict):
        try:
            STORAGE_DIR.mkdir(parents=True, exist_ok=True)
            out = {
                hname: {f: v.decode() if isinstance(v, bytes) else v for f, v in fields.items()}
                for hname, fields in snapshot.items()
            }
            tmp = HASH_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(out))
            tmp.replace(HASH_FILE)
        except Exception as e:
            log.error(f"[store] hash persist failed: {e}")

    async def force_persist(self):
        """Force immediate flush — call on shutdown."""
        self._last_kv_persist = 0
        self._last_hash_persist = 0
        if self._kv_dirty:
            await self._maybe_persist_kv()
        if self._hash_dirty:
            await self._maybe_persist_hash()

    # ── TTL helpers ───────────────────────────────────────────────────────────

    def _is_expired(self, key: str) -> bool:
        entry = self._kv.get(key)
        if entry is None:
            return True
        _, exp = entry
        if exp is not None and exp < time.time():
            del self._kv[key]
            return True
        return False

    # ── Redis-compatible async API ────────────────────────────────────────────

    async def get(self, key: str) -> bytes | None:
        async with self._lock:
            if self._is_expired(key):
                return None
            val, _ = self._kv[key]
            return val

    async def set(
        self,
        key: str,
        value: Any,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool | None:
        async with self._lock:
            if nx and not self._is_expired(key):
                return None  # key exists, NX fails
            exp = (time.time() + ex) if ex else None
            self._kv[key] = (_to_bytes(value), exp)
        self._schedule_persist_kv()
        return True

    async def setex(self, key: str, seconds: int, value: Any) -> bool:
        return await self.set(key, value, ex=seconds)

    async def mget(self, *keys: str) -> list[bytes | None]:
        async with self._lock:
            result = []
            for k in keys:
                if self._is_expired(k):
                    result.append(None)
                else:
                    result.append(self._kv[k][0])
            return result

    async def delete(self, *keys: str) -> int:
        count = 0
        async with self._lock:
            for k in keys:
                if k in self._kv:
                    del self._kv[k]
                    count += 1
        if count:
            self._schedule_persist_kv()
        return count

    # ── Hash commands ─────────────────────────────────────────────────────────

    async def hset(self, name: str, key: str, value: Any) -> int:
        async with self._lock:
            h = self._hashes.setdefault(name, {})
            is_new = key not in h
            h[key] = _to_bytes(value)
        self._schedule_persist_hash()
        return 1 if is_new else 0

    async def hget(self, name: str, key: str) -> bytes | None:
        async with self._lock:
            return self._hashes.get(name, {}).get(key)

    async def hexists(self, name: str, key: str) -> bool:
        async with self._lock:
            return key in self._hashes.get(name, {})

    async def hdel(self, name: str, *keys: str) -> int:
        count = 0
        async with self._lock:
            h = self._hashes.get(name, {})
            for k in keys:
                if k in h:
                    del h[k]
                    count += 1
        if count:
            self._schedule_persist_hash()
        return count

    async def hgetall(self, name: str) -> dict[bytes, bytes]:
        async with self._lock:
            h = self._hashes.get(name, {})
            return {k.encode() if isinstance(k, str) else k: v for k, v in h.items()}

    async def hlen(self, name: str) -> int:
        async with self._lock:
            return len(self._hashes.get(name, {}))

    async def aclose(self):
        """Compatibility shim — flush to disk on shutdown."""
        await self.force_persist()


# Module-level singleton — imported everywhere instead of aioredis.from_url()
store = LocalStore()
