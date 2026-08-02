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
from pyrogram.errors import AuthKeyDuplicated, FloodWait


class ClientPool:
    def __init__(self):
        self.clients: List[Client] = []
        self._rr_counter = 0
        self._cooldown_until: Dict[int, float] = {}
        self._download_load: Dict[int, int] = {}   # idx -> # active DownloadTasks pinned to it
        self._broken: Dict[int, bool] = {}         # idx -> True if client connection is broken
        self._is_bot: Dict[int, bool] = {}         # idx -> True if session is a bot token
        self._lock = asyncio.Lock()
        self.on_health_event = None   # optional async fn(text), set by main.py for alerts
        self._last_alert_ts: Dict[str, float] = {}
        self._alert_min_interval_s = 300  # don't re-alert same condition more than once per 5min
        self._bg_tasks: set = set()  # strong refs to fire-and-forget tasks to prevent GC

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
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._safe_alert(text))
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
        except RuntimeError:
            pass  # no running loop (e.g. during early startup) — skip

    async def _safe_alert(self, text: str):
        try:
            await self.on_health_event(text)
        except Exception as e:
            print(f"[clients] alert task failed: {e}")

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
            self._is_bot[i] = ":" in sess
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
                await self._start_with_auth_retry(c, i, channel_username)
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
            except AuthKeyDuplicated as ae:
                print(f"[clients] client {i} AuthKeyDuplicated — suspending, will auto-retry in background")
                c = Client(
                    f"streamer_{i}", api_id=api_id, api_hash=api_hash,
                    bot_token=sess if ":" in sess else None,
                    session_string=None if ":" in sess else sess,
                    no_updates=no_updates, workers=1, in_memory=True,
                    parse_mode=ParseMode.DISABLED,
                )
                self.clients.append(c)
                self._broken[i] = True
                self._cooldown_until[i] = time.time() + 60
                try:
                    loop = asyncio.get_running_loop()
                    task = loop.create_task(self._recover_auth(i, 90))
                    self._bg_tasks.add(task)
                    task.add_done_callback(self._bg_tasks.discard)
                except RuntimeError:
                    pass
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

    async def _start_with_auth_retry(self, c: Client, i: int, channel_username):
        """Start a client, retrying on AUTH_KEY_DUPLICATED with backoff.

        During a Space redeploy/restart the previous container may still hold
        the session's auth key for a few seconds; the new container's client
        then fails with AuthKeyDuplicated and would be marked broken forever.
        Retrying shortly after lets it win the key once the old container is
        gone, so deployments self-heal."""
        delay = (10, 30, 60)
        for attempt in range(1, 4):
            try:
                await c.start()
                break
            except AuthKeyDuplicated as e:
                if attempt >= 3:
                    raise
                wait = delay[attempt - 1]
                print(f"[clients] client {i} AuthKeyDuplicated (attempt {attempt}/3)"
                      f" — previous holder still using the session; retrying in {wait}s")
                await asyncio.sleep(wait)
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

    async def stop(self):
        for c in self.clients:
            for s in list(getattr(c, "media_sessions", {}).values()):
                # Skip sessions that are the client's own session (same-DC alias
                # set in streamer._session); stopping them here would kill the
                # main connection before c.stop() and cause errors.
                if s is getattr(c, "session", None):
                    continue
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

        If all are cooling down, releases the lock, sleeps until the soonest
        one is free, then re-acquires — so other coroutines are not blocked
        during the wait.
        """
        while True:
            async with self._lock:
                avail = self._available()
                if avail:
                    self._rr_counter = (self._rr_counter + 1) % len(avail)
                    chosen = avail[self._rr_counter]
                    return chosen, self.clients[chosen]
                # All cooling down — compute wait outside sleep (lock held only briefly)
                if self._cooldown_until:
                    soonest = min(self._cooldown_until.values())
                    wait = max(0.0, soonest - time.time())
                else:
                    wait = 5.0
                n = len(self.clients)
                print(f"[clients] all {n} client(s) unavailable, waiting {wait:.1f}s")
                if wait > 30:
                    self._fire_alert("all_cooldown", f"🟡 All {n} Telegram client(s) cooling down, waiting {wait:.0f}s")
            # Lock released — sleep without blocking other callers
            await asyncio.sleep(wait)

    def is_bot(self, client: Client) -> bool:
        for i, c in enumerate(self.clients):
            if c == client:
                return self._is_bot.get(i, False)
        return False

    def primary(self) -> Client:
        """Client used for cheap metadata calls (get_messages, get_chat_history,
        sync). Skips broken AND bot sessions — bots can't use get_chat_history
        (BOT_METHOD_INVALID) or fetch channel history."""
        for i in range(len(self.clients)):
            if not self._broken.get(i, False) and not self._is_bot.get(i, False):
                return self.clients[i]
        # Fall back to the first non-broken client even if it's a bot
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

    def suspend_auth(self, client: Client, cooldown_s: int = 90):
        """AuthKeyDuplicated mid-operation = another holder (usually the
        previous container during a redeploy) still has the session — a
        transient condition. Suspend the client briefly and reconnect later
        instead of permanently breaking it; the duplicate disappears once the
        other holder is gone, and this client is the only non-bot session."""
        idx = next((i for i, c in enumerate(self.clients) if c == client), None)
        if idx is None:
            return
        self._broken[idx] = True
        self._cooldown_until[idx] = time.time() + cooldown_s
        print(f"[clients] client {idx} suspended {cooldown_s}s on AuthKeyDuplicated (transient) — will auto-recover")
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._recover_auth(idx, cooldown_s))
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
        except RuntimeError:
            pass

    async def _recover_auth(self, idx: int, cooldown_s: int):
        await asyncio.sleep(cooldown_s)
        c = self.clients[idx]
        try:
            if c.is_connected:
                await c.stop()
        except Exception:
            pass
        try:
            await c.start()
            self._broken[idx] = False
            self._cooldown_until.pop(idx, None)
            print(f"[clients] client {idx} recovered after AuthKeyDuplicated suspension")
        except AuthKeyDuplicated:
            print(f"[clients] client {idx} still suspended — session still in use elsewhere, retrying later")
            try:
                loop = asyncio.get_running_loop()
                task = loop.create_task(self._recover_auth(idx, cooldown_s))
                self._bg_tasks.add(task)
                task.add_done_callback(self._bg_tasks.discard)
            except RuntimeError:
                pass
        except Exception as e:
            print(f"[clients] client {idx} reconnect failed: {e}")

    async def acquire_download_slot(self) -> Tuple[int, Client]:
        """Pick the client with the fewest active background DownloadTasks
        pinned to it (not cooling down), instead of blind round-robin.

        Plain pick() alternates purely by call count, so multiple long-lived
        DownloadTasks started close together can all land on the same client
        while others sit idle — the exact opposite of what the pool is for.
        Caller must call release_download_slot(idx) when the task ends.

        Waits for a healthy client rather than falling back to broken/cooling
        clients — mirrors pick() behaviour to avoid guaranteed FloodWait.
        """
        while True:
            async with self._lock:
                avail = self._available()
                if avail:
                    chosen = min(avail, key=lambda i: self._download_load.get(i, 0))
                    self._download_load[chosen] = self._download_load.get(chosen, 0) + 1
                    return chosen, self.clients[chosen]
                # All clients cooling — compute wait and release lock before sleeping
                if self._cooldown_until:
                    soonest = min(self._cooldown_until.values())
                    wait = max(1.0, soonest - time.time())
                else:
                    wait = 5.0
                print(f"[clients] acquire_download_slot: all clients unavailable, waiting {wait:.1f}s")
            await asyncio.sleep(wait)

    def release_download_slot(self, idx: int) -> None:
        if idx in self._download_load:
            self._download_load[idx] = max(0, self._download_load[idx] - 1)

    def __len__(self):
        return len(self.clients)

    def healthy_count(self) -> int:
        return sum(1 for i in range(len(self.clients)) if not self._broken.get(i, False))


pool = ClientPool()
