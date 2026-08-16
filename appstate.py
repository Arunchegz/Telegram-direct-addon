"""
appstate.py — lifespan-owned shared application state.

These values used to live as bare, None-assigned module globals in main.py,
which meant any code path touching them before the lifespan startup finished
(e.g. an early route hit, a test import, a background task racing startup)
silently got None. They now live in a single holder, populated only by the
lifespan entrypoint, so they are trivial to inspect, mock and test.
"""

from __future__ import annotations


class AppState:
    """Holds runtime objects created during lifespan startup."""

    # HybridStore instance (Redis + local JSON + in-memory poster cache)
    redis_client = None
    # ByteStreamer — chunk streaming with per-client throttle/backoff
    byte_streamer = None
    # asyncio.Semaphore bounding concurrent live streams
    stream_sem = None
    # Dedicated Pyrogram client logged in as the bot (MTProto, not HTTPS);
    # used for reliable notify send/edit and admin commands/callbacks.
    bot_client = None
    # Resolved source channel id (peer for reactions / delete listener)
    source_chat_id = None


appstate = AppState()
