# Deploy to HuggingFace Spaces

## 1 — Create the Space

1. Go to https://huggingface.co/new-space
2. **Space name**: e.g. `tgstream`
3. **License**: any (MIT is fine)
4. **SDK**: **Docker**
5. **Hardware**: CPU Basic (free tier is fine for personal use)
6. **Space storage**: tick **Persistent storage** — it mounts a persistent disk at `/data`
7. Create the Space, then push this repo to it (or paste files in the Space UI editor)

## 2 — Create + mount the bucket (persistent storage)

1. Create a **public bucket**: https://huggingface.co/new-bucket → e.g. `Telegram_stremio-storage`
2. Space → **Settings → Storage** → attach the bucket, mount path **`/data`** (read-write)

The bucket is your persistent disk: prefetched files written to `/data` sync to it
automatically, and completed files stream straight from the bucket CDN.

## 3 — Add Redis

HuggingFace Spaces has **no managed Redis addon** — use an external provider:

- **Upstash** (free tier): https://upstash.com → create a Redis database → copy `REDIS_URL`
- Or Redis Cloud free tier: https://redis.com

## 4 — Set Secrets

HF Space → **Settings → Variables and secrets** (store tokens as *Secrets*):

```
API_ID              = (from my.telegram.org)
API_HASH            = (from my.telegram.org)
SESSION_STRING      = (generate below)
CHANNEL_USERNAME    = @yourchannel (or -100xxxxxxxxx for private channels)
REDIS_URL           = rediss://...:...@...upstash.io  (external Redis)
BASE_URL            = https://<your-username>-tgstream.hf.space
SYNC_INTERVAL       = 300
FULL_RECONCILE_S    = 300
STREAM_CONCURRENCY  = 5
STORAGE_DIR         = /data/tgstream
HF_BUCKET_ID        = <your-username>/Telegram_stremio-storage
HF_BUCKET_PREFIX    = tgstream
HF_REDIRECT_DONE    = true
```

> [!IMPORTANT]
> **For Private Channels**:
> 1. Use the numeric chat ID starting with `-100` (e.g. `CHANNEL_USERNAME = -1001234567890`) instead of a username.
> 2. Ensure that the Telegram account used to generate the `SESSION_STRING` has joined the private channel, as it acts as a userbot to fetch messages/media.

### Generate SESSION_STRING

```bash
pip install pyrogram tgcrypto
python3 -c "
from pyrogram import Client
with Client(':memory:', api_id=YOUR_ID, api_hash='YOUR_HASH') as c:
    print(c.export_session_string())
"
```

## 5 — Deploy

```bash
git remote add space https://huggingface.co/spaces/<your-username>/<space-name>
git push space main
```

(Space UI edits / `git lfs` not needed — no large files in this repo.)

## 6 — Verify

```bash
curl https://<your-username>-tgstream.hf.space/
# {"status":"ok","movies":0,"channel":"@yourchannel","sync_age_min":null}

# Trigger first sync
curl https://<your-username>-tgstream.hf.space/sync
```

## 7 — Install in Stremio

```
https://<your-username>-tgstream.hf.space/manifest.json
```

Or open: `stremio://<your-username>-tgstream.hf.space/manifest.json`

Dashboard: `https://<your-username>-tgstream.hf.space/dashboard`

---

## Notes

- Free Spaces **sleep** after ~48h idle — streaming users wake it (cold start).
  For 24/7 streaming use a paid hardware tier or a **watchdog** cron hitting `/` every 5 min.
- `/data` bucket storage is charged per GB — keep `MAX_LOCAL_GB` reasonable
  (default 10, tune it in your secrets).
- The bucket is the durable copy: completed files stream from its CDN even if
  Space storage is lost.
- Logs: **Settings → Logs** in the Space dashboard.
