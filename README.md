---
title: Telegram Stremio
emoji: 📺
colorFrom: blue
colorTo: purple
sdk: docker
pinned: false
---

# Telegram Stremio

Telegram streaming addon for Stremio. Runs as a **HuggingFace Space** (Docker SDK, port 7860) — see [DEPLOY.md](DEPLOY.md) for the full Space setup guide.

## Persistent streaming via HuggingFace bucket

The Space mounts a public **bucket** at `/data` (read-write), so every completed prefetch file under `STORAGE_DIR` lands in the bucket automatically. Completed files are then streamed (302-redirected) straight from the bucket's CDN — persistent, survives restarts, zero Telegram cost.

1. Create a public bucket: https://huggingface.co/new-bucket (e.g. `Telegram_stremio-storage`)
2. Mount it in the Space at `/data` (Settings → Storage), then set:

```
HF_BUCKET_ID      = yourusername/Telegram_stremio-storage
HF_BUCKET_PREFIX  = tgstream     # key prefix of STORAGE_DIR inside the mount
HF_REDIRECT_DONE  = true         # redirect completed-file streams to the bucket CDN (default)
STORAGE_DIR       = /data/tgstream
```

No token needed — the bucket resolve URLs are public and Range-capable. Registration is verified with a HEAD request (retried, mount sync can lag a few seconds) before a file is considered streamable. Files deleted/evicted locally are dropped from the streaming index too.

Uploads run in the background (deduped via Redis), retry with backoff, and are verified with a HEAD request before the file is considered streamable. Files deleted/evicted locally are removed from the bucket index too.
