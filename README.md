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

## Persistent storage via HuggingFace bucket

When a prefetch completes, the file is mirrored to a **public HuggingFace dataset repo** and streams are then served (302-redirected) straight from the HF CDN — persistent, survives restarts, zero Telegram cost.

1. Create a public dataset repo: https://huggingface.co/new-dataset (any name, e.g. `tgstream-cache`)
2. Set env vars:

```
HF_TOKEN      = (write token from https://huggingface.co/settings/tokens)
HF_REPO_ID    = yourusername/tgstream-cache
HF_REDIRECT_DONE = true   # redirect completed-file streams to the HF CDN (default)
STORAGE_DIR   = (optional; Space: set to /data/tgstream with persistent storage enabled)
```

Uploads run in the background (deduped via Redis), retry with backoff, and are verified with a HEAD request before the file is considered streamable. Files deleted/evicted locally are removed from the bucket index too.

Uploads run in the background (deduped via Redis), retry with backoff, and are verified with a HEAD request before the file is considered streamable. Files deleted/evicted locally are removed from the bucket index too.
