# Deploy to HuggingFace Spaces

## 1 — Create the Space

1. Go to https://huggingface.co/new-space
2. **Space name**: e.g. `tgstream`
3. **License**: any (MIT is fine)
4. **SDK**: **Docker**
5. **Hardware**: CPU Basic (free tier is fine for personal use)
6. **Space storage**: tick **Persistent storage** — it mounts a persistent disk at `/data`
7. Create the Space, then push this repo to it (or paste files in the Space UI editor)

## 2 — Enable Persistent Storage

HF Spaces storage is **ephemeral** by default — the disk is wiped on every restart.
Enable it in **Settings → Persistent Storage** (Beta), then set:

```
STORAGE_DIR = /data/tgstream
```

Prefetched files now survive restarts. Completed files are additionally mirrored
to your public dataset bucket (see `HF_REPO_ID` below) — the truly persistent layer.

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
HF_TOKEN            = (write token from https://huggingface.co/settings/tokens)
HF_REPO_ID          = <your-username>/tgstream-cache   (public dataset repo)
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
  For 24/7 streaming use a paid hardware tier or **watchdog** cron hitting `/` every 5 min.
- `/data` persistent storage is charged per GB — keep `MAX_LOCAL_GB` reasonable
  (default 10, tune it in your secrets).
- The HF dataset bucket (`HF_REPO_ID`) is the durable copy: even if Space storage
  is lost, completed files stream straight from the HF CDN via 302 redirect.
- Logs: **Settings → Logs** in the Space dashboard.
