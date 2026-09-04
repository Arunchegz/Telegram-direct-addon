"""
hfbucket.py — HuggingFace permanent private Storage Bucket integration.

Completed prefetch files are stored in a Hugging Face **Storage Bucket**
(permanent, optionally private). When a file is fully cached the proxy
302-redirects the player to a signed URL so bytes are served from HF's
CDN — persistent across restarts/evictions, zero Telegram cost, credentials
never reach the player.

URL strategies (in order):
  1. **SigV4-presigned S3 gateway GET** (https://s3.hf.co) — requires S3
     credentials generated from a HF token (Settings → Access Tokens →
     Generate S3 credentials). Works for public AND private buckets: the
     player hits the public gateway and is 302-redirected to the public CDN
     (us.aws.cdn.hf.co) because the request originates from the player's
     network. Credentials stay server-side.
  2. **Bearer-token resolve** — when no S3 credentials are set, resolve the
     object with the HF token (Authorization header, no Range) and hand the
     player the signed CDN URL in the 302 Location. Works for private buckets
     too. The CDN URL is cached until it expires.
  3. **Plain resolve URL** — public buckets only; the player resolves from
     their own network and HF redirects them to the public CDN.

Serving flow:
  - On HF Spaces with the bucket mounted read-write at STORAGE_DIR, completed
    files are already bucket objects — nothing to upload.
  - Elsewhere (local/Termux), files are uploaded on completion via
    huggingface_hub (HF_TOKEN).

Env:
  HF_BUCKET_ID      "owner/bucket" of your Storage Bucket (e.g. "arunchegz1/Telegram_stremio-storage")
  HF_BUCKET_PREFIX  key prefix applied to each object (default "tgstream")
  HF_BUCKET_MOUNTED "true" when the bucket is mounted read-write at STORAGE_DIR
  HF_TOKEN          HF token (Bearer resolve, upload mirroring, deletes)
  HF_S3_ACCESS_KEY  S3 credentials generated from a HF token — recommended
  HF_S3_SECRET_KEY  (Settings → Access Tokens → Generate S3 credentials)
  HF_S3_ENDPOINT    default https://s3.hf.co
  HF_S3_EXPIRES     presigned URL lifetime in seconds (default 21600)
  HF_BUCKET_VERIFY  probe availability before 302 (default "true")
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import time
import urllib.parse
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

log = logging.getLogger("tgstream.hfbucket")

# ── Config ────────────────────────────────────────────────────────────────────
HF_BUCKET_ID      = os.getenv("HF_BUCKET_ID", "").strip()
HF_BUCKET_PREFIX  = os.getenv("HF_BUCKET_PREFIX", "tgstream").strip().strip("/")
HF_BUCKET_MOUNTED = os.getenv("HF_BUCKET_MOUNTED", "false").strip().lower() == "true"
HF_TOKEN          = os.getenv("HF_TOKEN", "").strip()
HF_S3_ACCESS_KEY  = os.getenv("HF_S3_ACCESS_KEY", "").strip()
HF_S3_SECRET_KEY  = os.getenv("HF_S3_SECRET_KEY", "").strip()
HF_S3_ENDPOINT    = os.getenv("HF_S3_ENDPOINT", "https://s3.hf.co").rstrip("/")
HF_S3_REGION      = os.getenv("HF_S3_REGION", "us-east-1").strip() or "us-east-1"
HF_S3_EXPIRES     = int(os.getenv("HF_S3_EXPIRES", "21600"))
HF_BUCKET_VERIFY  = os.getenv("HF_BUCKET_VERIFY", "true").strip().lower() != "false"

_S3_HOST = urllib.parse.urlparse(HF_S3_ENDPOINT).netloc

# rel_path -> (expires_at, url) cache: the last verified player-facing URL.
_VERIFY: "OrderedDict[str, tuple[float, str]]" = OrderedDict()
# internal-only hosts the CDN may redirect to when resolved from inside HG's network
_INTERNAL_HOST_SUBSTRINGS = ("cas-bridge", "hf-internal", ".internal", "internal.")

_http: Optional[httpx.AsyncClient] = None


def _client() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(timeout=10.0, follow_redirects=False)
    return _http


# ── Config helpers ────────────────────────────────────────────────────────────

def configured() -> bool:
    """True when HF_BUCKET_ID is a valid owner/bucket pair."""
    if "/" not in HF_BUCKET_ID:
        return False
    owner, bucket = HF_BUCKET_ID.split("/", 1)
    return bool(owner and bucket)


def _owner() -> str:
    return HF_BUCKET_ID.split("/", 1)[0]


def _bucket_name() -> str:
    return HF_BUCKET_ID.split("/", 1)[1]


def remote_key(rel_path: str) -> str:
    """Object key inside the bucket (prefix applied)."""
    return f"{HF_BUCKET_PREFIX}/{rel_path}".strip("/")


def s3_canonical_key(rel_path: str) -> str:
    """S3-gateway path: namespace treated as bucket, HF bucket name prepended
    to the object key (see HF storage-buckets-s3 "Addressing buckets" #2)."""
    return f"{_bucket_name()}/{HF_BUCKET_PREFIX}/{rel_path}".strip("/")


def resolve_url(rel_path: str) -> str:
    """HuggingFace resolve URL for a bucket object."""
    return (
        f"https://huggingface.co/buckets/{HF_BUCKET_ID}/resolve"
        f"/{HF_BUCKET_PREFIX}/{urllib.parse.quote(rel_path, safe='')}"
    )


# ── SigV4 presigned URLs (S3-compatible gateway) ──────────────────────────────

def _sigv4_sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _sigv4_signing_key(secret_key: str, date_stamp: str, region: str) -> bytes:
    k_date = _sigv4_sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _sigv4_sign(k_date, region)
    k_service = _sigv4_sign(k_region, "s3")
    return _sigv4_sign(k_service, "aws4_request")


def _sigv4_query_string(access_key: str, amz_date: str, date_stamp: str, region: str) -> str:
    """Sorted canonical query string of the presign parameters."""
    scope = f"{date_stamp}/{region}/s3/aws4_request"
    params = [
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Credential", f"{access_key}/{scope}"),
        ("X-Amz-Date", amz_date),
        ("X-Amz-Expires", str(HF_S3_EXPIRES)),
        ("X-Amz-SignedHeaders", "host"),
    ]
    return "&".join(
        f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
        for k, v in sorted(params)
    )


def _sigv4_canonical_request(method: str, canonical_uri: str, query_string: str, host: str) -> str:
    return "\n".join([
        method.upper(), canonical_uri, query_string,
        f"host:{host}\n", "host", "UNSIGNED-PAYLOAD",
    ])


def _sigv4_string_to_sign(amz_date: str, date_stamp: str, region: str, canonical_request: str) -> str:
    scope = f"{date_stamp}/{region}/s3/aws4_request"
    return "\n".join([
        "AWS4-HMAC-SHA256", amz_date, scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])


def presigned_uri(method: str, canonical_key: str, now=None) -> str:
    """SigV4-presign the given S3 gateway request.
    Returns '' when S3 credentials are not configured."""
    if not (HF_S3_ACCESS_KEY and HF_S3_SECRET_KEY):
        return ""

    canonical_uri = "/" + _owner() + "/" + urllib.parse.quote(canonical_key, safe="/~")

    t = now or datetime.now(timezone.utc)
    amz_date = t.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = t.strftime("%Y%m%d")

    qs = _sigv4_query_string(HF_S3_ACCESS_KEY, amz_date, date_stamp, HF_S3_REGION)
    canonical_request = _sigv4_canonical_request(method, canonical_uri, qs, _S3_HOST)
    string_to_sign = _sigv4_string_to_sign(amz_date, date_stamp, HF_S3_REGION, canonical_request)
    signature = hmac.new(
        _sigv4_signing_key(HF_S3_SECRET_KEY, date_stamp, HF_S3_REGION),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{HF_S3_ENDPOINT}{canonical_uri}?{qs}&X-Amz-Signature={signature}"


# ── URL resolution ────────────────────────────────────────────────────────────

def _is_public_host(host: str) -> bool:
    """Reject HF-internal hosts — a signed URL pointing at one would be
    unusable by players. Only accept clearly-public hosts."""
    low = host.lower()
    if any(sub in low for sub in _INTERNAL_HOST_SUBSTRINGS):
        return False
    # allow cdn.hf.co and *.cdn.hf.co plus s3 gateway + huggingface.co
    return low.endswith("cdn.hf.co") or low == "s3.hf.co" or low.endswith("huggingface.co")


async def _probe_url(url: str) -> bool:
    """Range-probe a signed URL — GET 1 byte, follow redirects. Accepts the
    2xx headers from the gateway (302→CDN) and the CDN 200/206 itself."""
    try:
        r = await _client().get(url, headers={"Range": "bytes=0-0"},
                                follow_redirects=True)
        return 200 <= r.status_code < 300
    except Exception:
        return False


async def _resolve_with_token(rel_path: str) -> str:
    """Resolve a bucket object with Bearer auth; return the signed CDN URL
    (Location of the 302), or '' on failure. NO Range header — the CDN
    signature is bound to the range requested at resolve time."""
    if not HF_TOKEN:
        return ""
    url = resolve_url(rel_path)
    try:
        r = await _client().get(url, headers={"Authorization": f"Bearer {HF_TOKEN}"})
        if r.status_code in (301, 302, 303, 307, 308):
            loc = r.headers.get("location", "")
            if not loc or not _is_public_host(urllib.parse.urlparse(loc).netloc):
                log.warning(f"[hfbucket] resolve gave unusable host for {rel_path}: {(loc or '')[:90]}")
                return ""
            return loc
        log.info(f"[hfbucket] resolve {rel_path} → HTTP {r.status_code}")
        return ""
    except Exception as e:
        log.warning(f"[hfbucket] resolve failed for {rel_path}: {e}")
        return ""


def _url_expires_s(url: str) -> int:
    """How many seconds until a CloudFront-signed URL expires (best effort).
    Returns the default TTL when no Expires param is parseable."""
    try:
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        exp = int(q.get("Expires", ["0"])[0])
        remain = exp - int(time.time())
        return remain if remain > 60 else 60
    except Exception:
        return 900


# ── Dispatcher used by main.py ───────────────────────────────────────────────

async def try_redirect(rel_path: str) -> str:
    """Return a verified bucket URL to 302 the player to, or '' when the
    object isn't reachable yet. Resolution result is cached per rel_path
    (S3 presigns: HF_S3_EXPIRES; Bearer-resolve: the CDN Expires)."""
    if not configured() or not rel_path:
        return ""

    now = time.time()
    hit = _VERIFY.get(rel_path)
    if hit and hit[0] > now:
        return hit[1]

    url = ""
    if HF_S3_ACCESS_KEY and HF_S3_SECRET_KEY:
        url = presigned_uri("GET", s3_canonical_key(rel_path))
        if HF_BUCKET_VERIFY and not await _probe_url(url):
            url = ""
        # Expire the cache entry 60s before the URL itself expires so we
        # never serve a stale/expired presigned URL to the player.
        ttl = max(HF_S3_EXPIRES - 60, 60)
    elif HF_TOKEN:
        url = await _resolve_with_token(rel_path)
        if url and HF_BUCKET_VERIFY and not await _probe_url(url):
            # private resolve worked but the CDN URL is not public-reachable —
            # don't cache a dead URL
            url = ""
        ttl = _url_expires_s(url) if url else 0
    else:
        url = resolve_url(rel_path)
        if HF_BUCKET_VERIFY and not await _probe_url(url):
            url = ""
        ttl = 900

    if url:
        _VERIFY[rel_path] = (now + ttl, url)
        _VERIFY.move_to_end(rel_path)
        if len(_VERIFY) > 1000:
            # LRU-style eviction: drop the oldest ~10% instead of clearing
            # everything, so a cache miss doesn't thundering-herd the bucket.
            for _ in range(len(_VERIFY) - 900):
                _VERIFY.popitem(last=False)
    else:
        _VERIFY.pop(rel_path, None)
    return url


def invalidate(rel_path: str) -> None:
    _VERIFY.pop(rel_path, None)


# ── Upload (only when the bucket is NOT mounted) ──────────────────────────────

def upload_sync(local_path: str, rel_path: str) -> bool:
    """Synchronous mirror of a local cache file into the bucket via
    huggingface_hub. Best effort — returns success."""
    if not configured() or HF_BUCKET_MOUNTED:
        return False
    if not Path(local_path).is_file():
        return False
    try:
        from huggingface_hub import batch_bucket_files
        batch_bucket_files(HF_BUCKET_ID, add=[(local_path, remote_key(rel_path))])
        log.info(f"[hfbucket] uploaded {rel_path}")
        return True
    except ImportError:
        log.warning("[hfbucket] huggingface_hub not installed — add it to requirements for upload support")
        return False
    except Exception as e:
        log.error(f"[hfbucket] upload failed for {rel_path}: {e}")
        return False


async def upload_file(local_path: str, rel_path: str) -> bool:
    """Async wrapper around upload_sync (runs in a worker thread)."""
    if not configured() or HF_BUCKET_MOUNTED or not Path(local_path).is_file():
        return False
    return await asyncio.to_thread(upload_sync, local_path, rel_path)


# ── Delete (best-effort) ─────────────────────────────────────────────────────

async def delete_object(rel_path: str) -> None:
    """Remove an object from the bucket. No-op when unconfigured or when the
    mount is deleting it already (local unlink on mounted storage)."""
    if not configured() or HF_BUCKET_MOUNTED:
        return
    if HF_S3_ACCESS_KEY and HF_S3_SECRET_KEY:
        url = presigned_uri("DELETE", s3_canonical_key(rel_path))
        try:
            r = await _client().delete(url)
            if not (200 <= r.status_code < 300):
                log.warning(f"[hfbucket] DELETE {rel_path} → HTTP {r.status_code}")
        except Exception as e:
            log.warning(f"[hfbucket] S3 delete failed for {rel_path}: {e}")
        invalidate(rel_path)
        return
    if HF_TOKEN:
        try:
            from huggingface_hub import batch_bucket_files
            await asyncio.to_thread(
                batch_bucket_files, HF_BUCKET_ID,
                delete=[remote_key(rel_path)],
            )
            invalidate(rel_path)
        except Exception as e:
            log.warning(f"[hfbucket] hub delete failed for {rel_path}: {e}")
