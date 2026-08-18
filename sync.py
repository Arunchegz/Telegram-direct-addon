"""
sync.py — channel sync, rescan, and reaction-reconciliation domain.

Extracted from main.py so the sync/reaction workers are independently
reviewable and testable. The module intentionally never imports main:
helpers owned by main (movie-index cache, notify, queue wiring, pool access)
are bound at runtime via configure() — wiring happens once, at main's module
setup, keeping the dependency direction one-way (main -> sync).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

from pyrogram.errors import AuthKeyDuplicated, FloodWait, ReactionInvalid
from pyrogram.raw.functions.messages import SendReaction, SetChatAvailableReactions, GetMessagesReactions
from pyrogram.raw.types import ChatReactionsAll, ReactionEmoji, UpdateMessageReactions

import state as st
from appstate import appstate
from clients import pool as client_pool
from downloader import download_manager, find_cache_path, R_DL_STOPPED

log = logging.getLogger("tgstream.sync")

# ── Config ─────────────────────────────────────────────────────────────────────
SYNC_POLL_S        = int(os.getenv("SYNC_POLL_S", "120"))   # auto-detect new/removed movies
SYNC_INTERVAL      = int(os.getenv("SYNC_INTERVAL", "600"))
FULL_RECONCILE_S   = int(os.getenv("FULL_RECONCILE_S", "300"))  # full history rescan cadence (deletions)

# Serialize full history walks (startup reconcile + sync do_full can race).
_sync_lock = asyncio.Lock()
_rescan_lock = asyncio.Lock()

# ── Wired by main.configure() at startup (avoids an import cycle) ──────────────
_get_movies = None             # -> main._get_movies (cached index read)
_invalidate_movies_cache = None
_is_cached = None
_schedule = None
_notify_send = None
_queue_put = None
_prefetch_queued = None        # live set object from main (queue membership)
_delete_bucket_object = None   # -> main._delete_bucket_object
get_tg = None
CHANNEL_USERNAME = None
DISABLE_BOT_LISTENER = None


def configure(**links) -> None:
    """Bind main.py-provided helpers/constants. Called once by main at
    module setup. Delayed binding keeps the import graph acyclic — names
    below resolve at call time, never at import time."""
    for name, value in links.items():
        globals()[name] = value

async def _sync_loop():
    while True:
        try:
            await _sync_channel(force=False)
        except AuthKeyDuplicated:
            log.error("[sync_loop] AuthKeyDuplicated, retrying instantly with healthy client")
            try:
                if get_tg() is not None and not client_pool.is_bot(get_tg()):
                    await _sync_channel(force=False)
            except Exception as e:
                log.error(f"[sync_loop] retry failed: {e}")
        # Sleep between iterations — without this the loop spins tight, calling
        # Redis (get R_SYNC_TS + hlen R_MOVIES) thousands of times per minute
        # even when _sync_channel returns early. Use half the minimum interval
        # so we never miss a scheduled sync window by more than one tick.
        await asyncio.sleep(min(SYNC_POLL_S, SYNC_INTERVAL) // 2)

REACTION_FALLBACK = os.getenv("REACTION_FALLBACK", "🤔")  # used when the primary emoji is not in the channel's allowed reactions


async def _ensure_channel_reactions_enabled() -> None:
    """Enable all reactions on the source channel so 👨‍💻 and ⚡ always work.
    Requires the user client to be a channel admin. Fails silently if not."""
    if not appstate.source_chat_id:
        return
    try:
        tg = get_tg()
        peer = await tg.resolve_peer(appstate.source_chat_id)
        await tg.invoke(SetChatAvailableReactions(
            peer=peer,
            available_reactions=ChatReactionsAll(),
        ))
        log.info("[reaction] channel reactions set to ALL ✓")
    except Exception as e:
        log.info(f"[reaction] could not enable all reactions (not admin?): {type(e).__name__}: {e!r}")

async def _send_channel_reaction(message_id: int, emoji: str) -> None:
    """Send/replace a reaction on a channel message using the user MTProto client.

    Uses the first available pooled user client (not the bot) because bots
    cannot send reactions on channel posts they don't own.
    Falls back silently on any error so reactions never break the main flow.
    """
    if not appstate.source_chat_id or not message_id:
        return
    for attempt, candidate in enumerate((emoji, REACTION_FALLBACK)):
        try:
            tg = get_tg()
            await tg.invoke(
                SendReaction(
                    peer=await tg.resolve_peer(appstate.source_chat_id),
                    msg_id=message_id,
                    reaction=[ReactionEmoji(emoticon=candidate)],
                )
            )
            log.debug(f"[reaction] set {candidate} on msg {message_id}")
            return
        except FloodWait as fw:
            log.warning(f"[reaction] flood-wait {fw.value}s, skipping reaction for msg {message_id}")
            return
        except ReactionInvalid:
            if attempt == 0 and candidate != REACTION_FALLBACK:
                log.info(f"[reaction] {candidate} rejected on msg {message_id} "
                      f"(channel reaction restrictions) — falling back to {REACTION_FALLBACK}")
                continue
            log.error(f"[reaction] failed for msg {message_id}: {candidate} invalid on this channel")
            return
        except Exception as e:
            log.error(f"[reaction] failed for msg {message_id}: {type(e).__name__}: {e!r}")
            return


async def _get_our_reaction(message_id: int) -> str | None:
    """Return the emoji we have reacted with on a channel message, or None."""
    if not appstate.source_chat_id or not message_id:
        return None
    try:
        _RE = ReactionEmoji
        tg = get_tg()
        peer = await tg.resolve_peer(appstate.source_chat_id)
        updates = await tg.invoke(GetMessagesReactions(peer=peer, id=[message_id]))
        for upd in getattr(updates, "updates", []):
            if isinstance(upd, UpdateMessageReactions) and upd.msg_id == message_id:
                for rc in upd.reactions.results:
                    if rc.chosen_order is not None and isinstance(rc.reaction, _RE):
                        return rc.reaction.emoticon
    except Exception as e:
        log.error(f"[reaction] get_our_reaction failed for msg {message_id}: {type(e).__name__}: {e!r}")
    return None


async def _clear_channel_reaction(message_id: int) -> None:
    """Remove our reaction from a channel message.

    Telegram's SendReaction with reaction=[] triggers INPUT_REQUEST_TOO_LONG
    on some peer types in layer 158. Instead we re-send the same reaction with
    add_to_recent=False which acts as a toggle-off on the server side.
    If we can't determine which reaction to clear, fall back to the empty list
    but catch the error gracefully.
    """
    if not appstate.source_chat_id or not message_id:
        return
    try:
        tg = get_tg()
        peer = await tg.resolve_peer(appstate.source_chat_id)
        # Determine which reaction is currently set so we can toggle it off
        current = await _get_our_reaction(message_id)
        if current:
            # Toggle off by re-sending the same emoji — Telegram treats this as removal
            await tg.invoke(
                SendReaction(
                    peer=peer,
                    msg_id=message_id,
                    reaction=[ReactionEmoji(emoticon=current)],
                    add_to_recent=False,
                )
            )
        else:
            # Nothing to clear
            pass
        log.info(f"[reaction] cleared reaction on msg {message_id}")
    except FloodWait as fw:
        log.warning(f"[reaction] flood-wait {fw.value}s clearing reaction for msg {message_id}")
    except Exception as e:
        log.error(f"[reaction] clear failed for msg {message_id}: {type(e).__name__}: {e!r}")

async def _rescan_missing_files(movies: dict) -> int:
    """HF Spaces wipe the local disk on restart, but Redis state persists.
    Find movies Redis still marks as fully downloaded whose local sparse
    file is gone, reset their download state and re-enqueue a prefetch so
    they get re-cached automatically (self-heal)."""
    # Serialized: _reconcile_reactions (startup) and _sync_channel (do_full)
    # can fire this concurrently; a lock prevents duplicate requeues.
    async with _rescan_lock:
        requeued = 0
        for mid, m in movies.items():
            try:
                done = await appstate.redis_client.get(f"tgstream:dl:done:{mid}")
                if done != b"1":
                    continue
                if find_cache_path(mid, m.get("file_name")).exists():
                    continue
                stopped = await appstate.redis_client.get(R_DL_STOPPED.format(mid))
                await appstate.redis_client.delete(
                    f"tgstream:dl:map:{mid}",
                    f"tgstream:dl:done:{mid}",
                    f"tgstream:dl:path:{mid}",
                    f"tgstream:dl:ts:{mid}",
                )
                if stopped == b"1":
                    log.info(f"[rescan] {mid}: Redis done but file missing (disk wiped) — "
                          f"state reset, user-stopped, not requeueing")
                    continue
                if not _queue_put(mid):
                    log.info(f"[rescan] prefetch_queue full, dropping {mid}")
                log.info(f"[rescan] {mid}: Redis done but file missing (disk wiped) — "
                      f"reset, requeued prefetch")
                requeued += 1
            except Exception as e:
                log.error(f"[rescan] error for {mid}: {type(e).__name__}: {e!r}")
                continue
        if requeued:
            log.info(f"[rescan] done: {requeued} file(s) missing, re-enqueued for prefetch")
            _schedule(_notify_send(f"♻️ Local cache was wiped by a restart — "
                                   f"re-prefetching {requeued} movie(s)"))
        else:
            log.info("[rescan] done: no missing files")
    return requeued


async def _reconcile_reactions() -> None:
    """On startup: for every known movie
      - if file is fully cached locally  -> ensure ⚡ reaction
      - if we have ⚡ but file is NOT cached -> clear the reaction (or set 👨‍💻 if queued/downloading)
      - if file is cached but we have 👨‍💻   -> upgrade to ⚡
    Rate-limited: 0.5s between each API call to avoid FloodWait.
    """
    await asyncio.sleep(15)  # wait for pool + first sync to settle
    log.info("[reaction] starting startup reconciliation scan...")
    try:
        movies = await st.load_movies(appstate.redis_client)
    except Exception as e:
        log.warning(f"[reaction] reconcile aborted, could not load movies: {e}")
        return

    await _rescan_missing_files(movies)

    checked = fixed = cleared = 0
    for mid, m in movies.items():
        msg_id = m.get("message_id")
        if not msg_id:
            continue
        try:
            cached = await _is_cached(mid, m.get("file_name"))
            current_reaction = await _get_our_reaction(msg_id)
            await asyncio.sleep(0.5)  # rate-limit

            if cached:
                if current_reaction != "⚡":
                    await _send_channel_reaction(msg_id, "⚡")
                    await asyncio.sleep(0.5)
                    fixed += 1
            else:
                if current_reaction == "⚡":
                    # File gone (evicted/deleted/disk-wiped) but reaction still
                    # shows ⚡ -- wrong. If queued or actively downloading,
                    # downgrade to 👨‍💻 instead of clearing entirely.
                    queued = mid in _prefetch_queued
                    dl_task = download_manager.get(mid)
                    downloading = bool(dl_task and dl_task._task and not dl_task._task.done())
                    if queued or downloading:
                        await _send_channel_reaction(msg_id, "👨‍💻")
                    else:
                        await _clear_channel_reaction(msg_id)
                    await asyncio.sleep(0.5)
                    cleared += 1
            checked += 1
        except Exception as e:
            log.error(f"[reaction] reconcile error for {mid}: {type(e).__name__}: {e!r}")
            continue

    log.info(f"[reaction] reconcile done: {checked} checked, {fixed} set ⚡, {cleared} corrected")


async def _sync_channel(force: bool = False) -> int:
    async with _sync_lock:
        if not force:
            last = await appstate.redis_client.get(st.R_SYNC_TS)
            if last:
                try:
                    interval = SYNC_POLL_S if DISABLE_BOT_LISTENER else SYNC_INTERVAL
                    if (time.time() - float(last)) < interval:
                        # Return the current movies count from the in-memory cache
                        # (avoids a Redis hlen round-trip on every early-return tick).
                        movies = await _get_movies()
                        return len(movies)
                except ValueError:
                    pass

        acquired = await appstate.redis_client.set(st.R_SYNC_LCK, "1", ex=600, nx=True)
        if not acquired:
            return 0
        try:
            existing_movies = await _get_movies()
            existing_ids = set(existing_movies.keys())

            # Full history walk only on forced syncs (manual /sync, instant
            # post handler) and roughly every FULL_RECONCILE_S otherwise —
            # routine polling uses min_id so it only pulls NEW messages
            # instead of re-scanning the whole channel every cycle.
            last_full = await appstate.redis_client.get(st.R_SYNC_FULL_TS)
            do_full = force or not last_full or (time.time() - float(last_full)) > FULL_RECONCILE_S

            min_id = 0
            if not do_full:
                raw_max = await appstate.redis_client.get(st.R_SYNC_MAX_ID)
                min_id = int(raw_max) if raw_max else 0

            count = 0
            found_ids = set(existing_ids) if not do_full else set()
            found_msg_ids: dict = {}  # mid -> message_id, for 👨‍💻 reaction on new movies
            max_id_seen = min_id
            active_tg = get_tg()
            if active_tg is None or client_pool.is_bot(active_tg) or not active_tg.is_connected:
                log.error("[sync] no healthy user client available (all suspended/broken) — skipping sync pass")
                return 0
            try:
                # Pyrogram 2.x get_chat_history has no min_id filter — it
                # walks newest -> oldest via offset_id. For an incremental
                # pass we just stop as soon as we hit a message id we've
                # already synced, instead of paging through the full
                # history every cycle.
                async for msg in active_tg.get_chat_history(CHANNEL_USERNAME):
                    if not do_full and msg.id <= min_id:
                        break
                    try:
                        media = msg.video or msg.document
                        if not media: continue
                        fn = getattr(media, "file_name", None)
                        if not fn: continue
                        mid = st.movie_id(fn)
                        await st.save_movie(appstate.redis_client, mid, {
                            "message_id": msg.id, "file_name": fn,
                            "file_size": media.file_size,
                            "file_size_text": st.fmt_size(media.file_size),
                            "quality": st.quality(fn), "source": st.source(fn),
                            "synced_at": int(time.time()),
                        })
                        found_ids.add(mid)
                        found_msg_ids[mid] = msg.id
                        count += 1
                        max_id_seen = max(max_id_seen, msg.id)
                    except Exception: continue
            except AuthKeyDuplicated as ae:
                log.error(f"[sync] AuthKeyDuplicated on client. Suspending client: {ae}")
                client_pool.suspend_auth(active_tg)
                raise ae

            # Single invalidation after the full scan instead of per-message
            if count:
                _invalidate_movies_cache()

            new_ids = found_ids - existing_ids
            for mid in new_ids:
                log.info(f"Sync: new movie detected, enqueueing for prefetch: {mid}")
                stopped = await appstate.redis_client.get(R_DL_STOPPED.format(mid))
                if stopped != b"1":
                    if not _queue_put(mid):
                        log.info(f"[sync] prefetch_queue full, dropping {mid}")
                    elif mid in found_msg_ids:
                        # React 👨‍💻 on the channel post to signal prefetch queued
                        _schedule(_send_channel_reaction(found_msg_ids[mid], "👨‍💻"))

            # Clean up deleted movies — only meaningful on a full walk;
            # an incremental (min_id) pass never sees old messages so it
            # must never be treated as evidence they were deleted.
            removed_ids = set()
            if do_full:
                removed_ids = existing_ids - found_ids
                for mid in removed_ids:
                    file_name = existing_movies.get(mid, {}).get("file_name")
                    log.info(f"Sync: removing deleted movie {mid} from index")
                    await st.del_movie(appstate.redis_client, mid)
                    await download_manager.evict(mid, appstate.redis_client, file_name=file_name)
                    # Remove the file from the HF bucket (permanent storage).
                    if _delete_bucket_object is not None:
                        _schedule(_delete_bucket_object(mid, file_name))
                if removed_ids:
                    _invalidate_movies_cache()
                await appstate.redis_client.set(st.R_SYNC_FULL_TS, str(time.time()))

            if new_ids or removed_ids:
                await _notify_send(f"🔄 Synced: {len(new_ids)} new, {len(removed_ids)} removed")

            if do_full:
                # Self-heal: re-prefetch anything Redis marks done whose local
                # file vanished (e.g. HF Space disk wiped while this was down).
                await _rescan_missing_files(existing_movies)

            if max_id_seen > min_id:
                await appstate.redis_client.set(st.R_SYNC_MAX_ID, str(max_id_seen))
            await appstate.redis_client.set(st.R_SYNC_TS, str(time.time()))
            log.info(f"Sync: {count} new/updated movies ({'full' if do_full else 'incremental'})")
            movies = await _get_movies()
            return len(movies)
        finally:
            await appstate.redis_client.delete(st.R_SYNC_LCK)
