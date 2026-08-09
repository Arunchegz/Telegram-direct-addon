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

The Space mounts a **Storage Bucket** at `/data` (read-write), so every completed prefetch file under `STORAGE_DIR` lands in the bucket automatically. Completed files are then streamed (302-redirected) straight from the bucket's CDN — persistent, survives restarts, zero Telegram cost. **Private buckets** are fully supported: either the addon SigV4-presigns an S3-gateway GET URL (only needed with S3 credentials), or it resolves the object with the HF token and redirects the player to a signed public CDN URL — credentials never reach the player.

1. Create a bucket (public or private): https://huggingface.co/new-bucket (e.g. `Telegram_stremio-storage`)
2. Mount it in the Space at `/data` (Settings → Storage), then set:

```
HF_BUCKET_ID      = arunchegz1/Telegram_stremio-storage
HF_BUCKET_PREFIX  = tgstream     # key prefix of STORAGE_DIR inside the bucket
HF_BUCKET_MOUNTED = true         # bucket is mounted read-write at STORAGE_DIR
HF_REDIRECT_DONE  = true         # redirect completed-file streams to the bucket (default)
STORAGE_DIR       = /data/tgstream
```

### Private buckets

Private objects need authentication. Two modes:

1. **Bearer resolve (recommended, token-only)** — `HF_TOKEN` set, no S3 credentials needed. The addon resolves the object with the token and 302s the player to the resulting signed CDN URL (public, Range-capable). Works out of the box for `arunchegz1/Telegram_stremio-storage`.
2. **S3 presign (needs extra setup)** — generate S3 credentials from your HF token: https://huggingface.co/settings/tokens → *Generate S3 credentials* (the token needs Read+Write on your bucket), then set:

```
HF_S3_ACCESS_KEY  = HFAK...     # generated access key
HF_S3_SECRET_KEY  = ...         # generated secret (shown once)
HF_S3_EXPIRES     = 3600        # presigned URL lifetime (seconds)
```

If no key and no token are set, the addon falls back to the plain public resolve URL (works for **public** buckets only).

### Running outside HF Spaces (local/Termux)

With `HF_BUCKET_MOUNTED=false` (default), completed files are mirrored to the bucket automatically via `huggingface_hub` (`HF_TOKEN` required), and evicted objects are removed with a presigned DELETE when you use the delete/evict API endpoints. LRU evictions keep the bucket copy untouched — that copy is the permanent one.

Bucket availability is verified before a file is considered streamable (signed URL probe, retried; mount sync can lag a few seconds). Files deleted/evicted locally are dropped from the streaming index too.
