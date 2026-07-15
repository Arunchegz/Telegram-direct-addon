"""  
metrics.py — TGStream monitoring and metrics collection.

Tracks:
  - Rate limit events (count, duration, backoff)
  - Stream performance (paths used, cache hits)
  - Download progress (speed, ETA)
  - System health (concurrent requests, errors)
"""
from __future__ import annotations
import time
from typing import Dict, List, Tuple, Optional


class Metrics:
    """Async-safe metrics collection.

    All async record_* methods are called exclusively from the asyncio event
    loop thread, so CPython's GIL makes individual attribute increments
    (int +=, list.append) safe without a lock.  get_stats() / get_rate_limit_events()
    are also called from the same event-loop thread (FastAPI endpoints), so
    no cross-thread synchronisation is needed.

    If get_stats() is ever called from a worker thread in the future, wrap the
    read with asyncio.run_coroutine_threadsafe or switch to asyncio.Lock.
    """

    def __init__(self):
        # Rate limit tracking
        self.rate_limit_events: List[Tuple[float, int, float]] = []  # [(timestamp, dc_id, wait_s)]
        self.rate_limit_total_wait: float = 0.0   # Total seconds spent in backoff
        self.rate_limit_count: int = 0            # Total rate limit events

        # Stream path tracking
        self.stream_paths: Dict[str, int] = {    # path -> count
            "local": 0,
            "local-waited": 0,
            "mixed": 0,
            "telegram-live": 0,
        }
        self.stream_total: int = 0

        # Cache performance
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self.cache_hit_bytes: int = 0
        self.cache_miss_bytes: int = 0

        # Download stats
        self.downloads_completed: int = 0
        self.downloads_active: int = 0
        self.total_downloaded_mb: float = 0.0

        # Request tracking
        self.http_requests_total: int = 0
        self.http_errors: int = 0
        self.proxy_requests: int = 0

    async def record_rate_limit(self, dc_id: int, wait_s: float) -> None:
        """Record a rate limit event."""
        self.rate_limit_events.append((time.time(), dc_id, wait_s))
        self.rate_limit_total_wait += wait_s
        self.rate_limit_count += 1
        # Keep only last 1000 events
        if len(self.rate_limit_events) > 1000:
            self.rate_limit_events = self.rate_limit_events[-1000:]

    async def record_stream_path(self, path: str) -> None:
        """Record which streaming path was used."""
        if path in self.stream_paths:
            self.stream_paths[path] += 1
        self.stream_total += 1

    async def record_cache_hit(self, bytes_read: int) -> None:
        """Record successful cache read."""
        self.cache_hits += 1
        self.cache_hit_bytes += bytes_read

    async def record_cache_miss(self, bytes_needed: int) -> None:
        """Record cache miss (had to fetch from Telegram)."""
        self.cache_misses += 1
        self.cache_miss_bytes += bytes_needed

    async def record_http_request(self, success: bool) -> None:
        """Record HTTP request."""
        self.http_requests_total += 1
        if not success:
            self.http_errors += 1

    async def record_proxy_request(self) -> None:
        """Record /proxy/ request."""
        self.proxy_requests += 1

    async def record_download_start(self) -> None:
        """Increment active download count."""
        self.downloads_active += 1

    async def record_download_chunk(self, bytes_len: int) -> None:
        """Record a downloaded chunk by converting bytes to MB."""
        self.total_downloaded_mb += bytes_len / (1024 * 1024)

    async def record_download_complete(self) -> None:
        """Decrement active download count (if any) and increment completed count."""
        if self.downloads_active > 0:
            self.downloads_active -= 1
        self.downloads_completed += 1

    async def record_download_stop(self) -> None:
        """Decrement active download count safely."""
        if self.downloads_active > 0:
            self.downloads_active -= 1

    def get_rate_limit_events(self) -> List[Tuple[float, int, float]]:
        """Return a copy of the rate limit events."""
        return list(self.rate_limit_events)

    def get_stats(self) -> dict:
        """Return current metrics snapshot."""
        # Cache stats
        cache_total = self.cache_hits + self.cache_misses
        cache_hit_rate = (self.cache_hits / cache_total * 100) if cache_total > 0 else 0.0

        # Rate limit stats (last hour)
        now = time.time()
        hour_ago = now - 3600
        recent_events = [e for e in self.rate_limit_events if e[0] > hour_ago]
        avg_wait = (sum(e[2] for e in recent_events) / len(recent_events)) if recent_events else 0.0

        # Stream path distribution
        path_dist: Dict[str, float] = {}
        if self.stream_total > 0:
            for path, count in self.stream_paths.items():
                path_dist[path] = round(count / self.stream_total * 100, 1)

        # Error rate
        if self.http_requests_total > 0:
            http_success_rate = ((self.http_requests_total - self.http_errors) / self.http_requests_total * 100)
        else:
            http_success_rate = 100.0

        # Local ratio
        local_count = self.stream_paths["local"] + self.stream_paths["local-waited"]
        local_ratio = (local_count / max(1, self.stream_total) * 100)

        return {
            "rate_limits": {
                "total_events": self.rate_limit_count,
                "total_wait_s": round(self.rate_limit_total_wait, 1),
                "avg_wait_s": round(avg_wait, 1),
                "recent_hour": len(recent_events),
            },
            "cache": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_rate_pct": round(cache_hit_rate, 1),
                "bytes_cached": self.cache_hit_bytes,
                "bytes_uncached": self.cache_miss_bytes,
            },
            "streaming": {
                "paths": path_dist,
                "total_requests": self.stream_total,
                "local_ratio_pct": round(local_ratio, 1),
            },
            "http": {
                "total_requests": self.http_requests_total,
                "errors": self.http_errors,
                "success_rate_pct": round(http_success_rate, 1),
                "proxy_requests": self.proxy_requests,
            },
            "downloads": {
                "active": self.downloads_active,
                "completed": self.downloads_completed,
                "total_mb": round(self.total_downloaded_mb, 1),
            },
        }


# Global singleton
metrics = Metrics()
