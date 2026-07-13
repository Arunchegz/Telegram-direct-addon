"""
clients.py — Pool of Pyrogram clients for FloodWait failover.

Multiple Telegram sessions, all added to the same source channel(s).
Round-robin selection, skipping any client currently in FloodWait cooldown.
If every client is cooling down, waits for the soonest one to free up
rather than blocking forever.

Env vars:
  SESSION_STRING_1, SESSION_STRING_2, ... (preferred, any number)
  SESSION_STRING (back-compat fallback, used as the only client if no
                   numbered vars are set)
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Dict, List, Tuple

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait


class ClientPool:
    def __init__(self):
        self.clients: List[Client] = []
        self._rr_counter = 0
        self._cooldown_until: Dict[int, float] = {}
        self._download_load: Dict[int, int] = {}   # idx -> # active DownloadTasks pinned to it
        self._broken: Dict[int, bool] = {}         # idx -> True if client connection is broken
        self._lock = asyncio.Lock()
        self.on_health_event = None   # optional async fn(text), set by main.py for alerts
        self._last_alert_ts: Dict[str, float] = {}
        self._alert_min_interval_s = 300  # don't re-alert same condition more than once per 5min

    def _fire_alert(self, key: str, text: str):
        """Fire-and-forget, rate-limited per `key` so a flapping client
        doesn't spam the notify channel."""
        if not self.on_health_event:
            return
        now = time.time()
        if now - self._last_alert_ts.get(key, 0) < self._alert_min_interval_s:
            return
        self._last_alert_ts[key] = now
        try:
            asyncio.get_running_loop().create_task(self.on_health_event(text))
        except RuntimeError:
            pass  # no running loop (e.g. during early startup) — skip

    @staticmethod
    def _load_sessions() -> List[str]:
        sessions = []
        seen = set()
        i = 1
        while True:
            s = os.getenv(f"SESSION_STRING_{i}", "").strip()
            if not s:
                break
            if s not in seen:
                sessions.append(s)
                seen.add(s)
            else:
                print(f"[clients] WARNING: SESSION_STRING_{i} is a duplicate of a previously loaded session. Skipping to avoid AuthKeyDuplicated.")
            i += 1
        if not sessions:
            s = os.getenv("SESSION_STRING", "").strip()
            if s and s not in seen:
                sessions.append(s)
        return sessions

    async def start(self, api_id: int, api_hash: str, channel_username: str | int = None):
        sessions = self._load_sessions()
        if not sessions:
            raise RuntimeError(
                "No sessions found. Set SESSION_STRING_1 (and optionally "
                "SESSION_STRING_2, ...) or fall back to SESSION_STRING."
            )
        for i, sess in enumerate(sessions):
            self._broken[i] = False
            no_updates = False if i == 0 else True
            try:
                if ":" in sess:
                    c = Client(
                        f"streamer_{i}", api_id=api_id, api_hash=api_hash,
                        bot_token=sess, no_updates=no_updates, workers=16,
                        sleep_threshold=0, in_memory=True, parse_mode=ParseMode.DISABLED,
                    )
                else:
                    c = Client(
                        f"streamer_{i}", api_id=api_id, api_hash=api_hash,
                        session_string=sess, no_updates=no_updates, workers=16,
                        sleep_threshold=0, in_memory=True, parse_mode=ParseMode.DISABLED,
                    )
                await c.start()
                if channel_username:
                    try:
                        await c.get_chat(channel_username)
                        print(f"[clients] client {i} successfully resolved channel {channel_username}")
                    except Exception as e:
                        print(f"[clients] client {i} failed to resolve channel {channel_username}: {e}")
                else:
                    try:
                        async for _ in c.get_dialogs(limit=100):
                            pass
                    except Exception as e:
                        print(f"[clients] peer-cache warmup failed for client {i}: {e}")
                self.clients.append(c)
                print(f"[clients] client {i} started")
            except FloodWait as fw:
                print(f"[clients] client {i} failed to start due to FloodWait (cooldown {fw.value}s)")
                # Instantiate a dummy client object to maintain index symmetry in the pool
                c = Client(
                    f"streamer_{i}", api_id=api_id, api_hash=api_hash,
                    bot_token=sess if ":" in sess else None,
                    session_string=None if ":" in sess else sess,
                    no_updates=no_updates, workers=1, in_memory=True,
                    parse_mode=ParseMode.DISABLED,
                )
                self.clients.append(c)
                self.mark_broken(i)
                self.mark_cooldown(i, fw.value)
            except Exception as e:
                print(f"[clients] client {i} failed to start due to error: {e}")
                c = Client(
                    f"streamer_{i}", api_id=api_id, api_hash=api_hash,
                    bot_token=sess if ":" in sess else None,
                    session_string=None if ":" in sess else sess,
                    no_updates=no_updates, workers=1, in_memory=True,
                    parse_mode=ParseMode.DISABLED,
                )
                self.clients.append(c)
                self.mark_broken(i)
        
        # Check if we have at least one healthy client
        healthy_count = sum(1 for idx in range(len(self.clients)) if not self._broken.get(idx, False))
        if healthy_count == 0:
            raise RuntimeError("All clients in the pool failed to start. Cannot proceed.")
        print(f"[clients] pool ready with {healthy_count} healthy client(s)")

    async def stop(self):
        for c in self.clients:
            for s in list(getattr(c, "media_sessions", {}).values()):
                try:
                    await s.stop()
                except Exception:
                    pass
            if hasattr(c, "media_sessions"):
                c.media_sessions.clear()
            try:
                await c.stop()
            except Exception:
                pass

    def mark_cooldown(self, idx: int, seconds: float):
        self._cooldown_until[idx] = time.time() + seconds
        print(f"[clients] client {idx} cooling down for {seconds:.1f}s")

    def _available(self) -> List[int]:
        now = time.time()
        return [i for i in range(len(self.clients)) 
                if self._cooldown_until.get(i, 0) <= now and not self._broken.get(i, False)]

    async def pick(self) -> Tuple[int, Client]:
        """Round-robin among clients not currently in cooldown.

        If all are cooling down, sleeps until the soonest one is free
        rather than picking a client guaranteed to FloodWait again.
        """
        async with self._lock:
            avail = self._available()
            if not avail:
                if self._cooldown_until:
                    soonest = min(self._cooldown_until.values())
                    wait = max(0.0, soonest - time.time())
                else:
                    wait = 5.0
                print(f"[clients] all {len(self.clients)} client(s) unavailable, waiting {wait:.1f}s")
                if wait > 30:
                    self._fire_alert("all_cooldown", f"🟡 All {len(self.clients)} Telegram client(s) cooling down, waiting {wait:.0f}s")
                await asyncio.sleep(wait)
                avail = self._available() or list(range(len(self.clients)))
            # Rotate over the full client count, not len(avail) — avail shrinks
            # whenever a client is cooling down, which skewed the round-robin
            # toward whichever clients happened to be available at pick time.
            self._rr_counter = (self._rr_counter + 1) % len(self.clients)
            chosen = avail[self._rr_counter % len(avail)]
            return chosen, self.clients[chosen]

    def primary(self) -> Client:
        """Client used for cheap metadata calls (get_messages etc) that
        rarely trip FloodWait — no need to rotate these."""
        for i in range(len(self.clients)):
            if not self._broken.get(i, False):
                return self.clients[i]
        return self.clients[0] if self.clients else None

    def mark_broken(self, idx: int):
        self._broken[idx] = True
        print(f"[clients] client {idx} marked as broken (auth key duplicated / invalidated)")
        self._fire_alert(f"broken:{idx}", f"🔴 Telegram client {idx} marked broken (auth key duplicated/invalidated)")

    def mark_broken_by_client(self, client: Client):
        for i, c in enumerate(self.clients):
            if c == client:
                self.mark_broken(i)
                break

    async def acquire_download_slot(self) -> Tuple[int, Client]:
        """Pick the client with the fewest active background DownloadTasks
        pinned to it (not cooling down), instead of blind round-robin.

        Plain pick() alternates purely by call count, so multiple long-lived
        DownloadTasks started close together can all land on the same client
        while others sit idle — the exact opposite of what the pool is for.
        Caller must call release_download_slot(idx) when the task ends.
        """
        async with self._lock:
            avail = self._available()
            if not avail:
                avail = list(range(len(self.clients)))
            chosen = min(avail, key=lambda i: self._download_load.get(i, 0))
            self._download_load[chosen] = self._download_load.get(chosen, 0) + 1
            return chosen, self.clients[chosen]

    def release_download_slot(self, idx: int) -> None:
        if idx in self._download_load:
            self._download_load[idx] = max(0, self._download_load[idx] - 1)

    def __len__(self):
        return len(self.clients)

    def healthy_count(self) -> int:
        return sum(1 for i in range(len(self.clients)) if not self._broken.get(i, False))


pool = ClientPool()
