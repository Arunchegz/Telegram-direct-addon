"""
state.py — Redis helpers for movie index + poster cache.
Download state lives in downloader.py (separate key namespace).
"""
from __future__ import annotations
import base64
import hashlib
import json
import re
import time
from typing import Optional

import httpx
import redis.asyncio as aioredis

# Shared connection-pooled client — a fresh httpx.AsyncClient per call
# (the old behaviour) re-does a TLS handshake every poster/Cinemeta lookup.
# Created lazily so importing this module has no side effect at import time.
_http_client: httpx.AsyncClient | None = None


def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=10, follow_redirects=True)
    return _http_client


async def close_http_client():
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        await _http_client.aclose()
    _http_client = None


# ── Key templates ─────────────────────────────────────────────────────────────
R_MOVIES   = "tgstream:movies"
R_POSTER   = "tgstream:poster:{}"
R_IMDB     = "tgstream:imdb:{}"
R_SYNC_TS  = "tgstream:last_sync"
R_SYNC_LCK = "tgstream:rate:sync"
R_SYNC_MAX_ID = "tgstream:sync:max_msg_id"
R_SYNC_FULL_TS = "tgstream:sync:last_full"


# ── Movie index ───────────────────────────────────────────────────────────────
async def load_movies(redis: aioredis.Redis) -> dict:
    raw = await redis.hgetall(R_MOVIES)
    return {k.decode(): json.loads(v) for k, v in raw.items()}


async def save_movie(redis: aioredis.Redis, mid: str, data: dict):
    await redis.hset(R_MOVIES, mid, json.dumps(data))


async def del_movie(redis: aioredis.Redis, mid: str):
    await redis.hdel(R_MOVIES, mid)


# ── Poster cache ──────────────────────────────────────────────────────────────
async def _fetch_poster(filename: str) -> tuple[str, str]:
    """Returns (poster_url, imdb_id). imdb_id is \'\' if not found."""
    is_series = bool(IS_SERIES_RE.search(filename))
    if is_series:
        title = parse_show_title(filename)
        year = ""
        catalog_type = "series"
    else:
        title, year = parse_title_year(filename)
        catalog_type = "movie"
    if not title:
        return _local_placeholder_poster(""), ""
    query = f"{title} {year}".strip()
    try:
        c = _get_http_client()
        r = await c.get(
            f"https://v3-cinemeta.strem.io/catalog/{catalog_type}/top/search={query}.json",
        )
        metas = r.json().get("metas", [])
        if metas:
            poster = metas[0].get("poster") or _local_placeholder_poster(title)
            imdb_id = metas[0].get("id", "")
            if not imdb_id.startswith("tt"):
                imdb_id = ""
            return poster, imdb_id
    except Exception:
        pass
    return _local_placeholder_poster(title), ""


async def get_poster(redis: aioredis.Redis, filename: str) -> str:
    """Legacy compat: returns only poster URL."""
    poster, _ = await get_poster_and_imdb(redis, filename)
    return poster


async def get_poster_and_imdb(redis: aioredis.Redis, filename: str) -> tuple[str, str]:
    """Returns (poster_url, imdb_id). Both cached in Redis for 24h."""
    poster_key = R_POSTER.format(filename[:80])
    imdb_key = R_IMDB.format(filename[:80])
    try:
        cached_poster = await redis.get(poster_key)
        cached_imdb = await redis.get(imdb_key)
        if cached_poster:
            return cached_poster.decode(), (cached_imdb.decode() if cached_imdb else "")
    except Exception as e:
        print(f"[poster] Redis get failed for {filename}: {e}")
    poster, imdb_id = await _fetch_poster(filename)
    try:
        await redis.setex(poster_key, 86400, poster)
        if imdb_id:
            await redis.setex(imdb_key, 86400, imdb_id)
    except Exception as e:
        print(f"[poster] Redis set failed for {filename}: {e}")
    return poster, imdb_id


def _local_placeholder_poster(title: str) -> str:
    """Inline SVG data URI — no external dependency, never 404s/times out."""
    safe_title = (title or "No Poster")[:40].replace("&", "and")
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="300" height="450">'
        f'<rect width="300" height="450" fill="#1a1a1a"/>'
        f'<text x="150" y="225" fill="#888" font-family="sans-serif" '
        f'font-size="18" text-anchor="middle" dominant-baseline="middle">'
        f'{safe_title}</text></svg>'
    )
    b64 = base64.b64encode(svg.encode()).decode()
    return f"data:image/svg+xml;base64,{b64}"


async def get_cinemeta(type_name: str, imdb_id: str) -> tuple[str, str]:
    try:
        c = _get_http_client()
        r = await c.get(f"https://v3-cinemeta.strem.io/meta/{type_name}/{imdb_id}.json")
        meta = r.json().get("meta", {})
        year_val = meta.get("year") or meta.get("releaseInfo") or ""
        return meta.get("name", ""), str(year_val)
    except Exception:
        return "", ""


# ── String helpers ────────────────────────────────────────────────────────────
def movie_id(filename: str) -> str:
    slug = re.sub(r"[^a-z0-9_]", "_", filename.lower())
    suffix = hashlib.md5(filename.encode()).hexdigest()[:8]
    return f"{slug}_{suffix}"


def fmt_size(size) -> str:
    if not size:
        return "Unknown"
    size = float(size)
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {u}"
        size /= 1024
    return f"{size:.1f} PB"


def quality(fn: str) -> str:
    n = fn.lower()
    for tag in ["2160p", "4k", "1440p", "1080p", "720p", "480p", "360p"]:
        if tag in n:
            return tag.upper()
    return "Unknown"


def source(fn: str) -> str:
    n = fn.lower()
    for tag in ["bluray", "bdrip", "web-dl", "webdl", "webrip", "hdrip", "dvdrip", "hdtv", "remux"]:
        if tag in n:
            return tag.upper()
    return ""


def ctype(fn: str) -> str:
    n = fn.lower()
    if n.endswith(".mkv"):
        return "video/x-matroska"
    if n.endswith(".webm"):
        return "video/webm"
    return "video/mp4"


def normalize(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[._\-–—+]", " ", text)
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def flex_match(title: str, filename: str) -> bool:
    tn, fn = normalize(title), normalize(filename)
    if not tn or not fn:
        return False
    if tn in fn:
        return True
    tw, fw = tn.split(), fn.split()
    return sum(1 for w in tw if w in fw) >= max(1, len(tw) * 0.7)


def parse_title_year(filename: str) -> tuple[str, str]:
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", filename)
    name = re.sub(r"[._]", " ", name)
    ym   = re.search(r"\b(19|20)\d{2}\b", name)
    year = ym.group(0) if ym else ""
    cut  = re.split(
        r"\b(?:19|20)\d{2}\b|\b(?:1080p|2160p|720p|480p|bluray|webrip|web dl|"
        r"bdrip|hdrip|remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
        name, maxsplit=1, flags=re.IGNORECASE,
    )[0]
    return re.sub(r"\s+", " ", cut).strip().title(), year


# Unified series-detection regex — used by is_series() checks everywhere
IS_SERIES_RE = re.compile(
    r"[Ss]\d{1,2}[Ee]\d{1,3}"          # S01E01 / S1E5
    r"|[Ss]eason[\s._-]*\d+"            # Season.2 / Season 2
    r"|[Ee]pisode[\s._-]*\d+",          # Episode.3 / Episode 3
    re.IGNORECASE,
)

def parse_series(filename: str) -> Optional[dict]:
    # SxxExx / S1E5
    m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,3})", filename)
    if m:
        return {"season": int(m.group(1)), "episode": int(m.group(2))}
    # Season N ... Episode N (dots/spaces/dashes as separators)
    m2 = re.search(r"[Ss]eason[\s._-]*(\d+)[\s\S]*?[Ee]pisode[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m2:
        return {"season": int(m2.group(1)), "episode": int(m2.group(2))}
    # Season N only — no episode marker
    m3 = re.search(r"[Ss]eason[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m3:
        return {"season": int(m3.group(1)), "episode": 1}
    return None


def parse_show_title(filename: str) -> str:
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", filename)
    name = re.sub(r"[._\-–—+]", " ", name)
    
    # Split by common season/episode patterns
    for pattern in [r"\b[Ss]\d{1,2}[Ee]\d{1,3}\b", r"\b[Ss]eason\s*\d+\b", r"\b[Ee]pisode\s*\d+\b"]:
        parts = re.split(pattern, name, flags=re.IGNORECASE)
        if len(parts) > 1:
            name = parts[0]
            break
            
    # Split by year
    parts = re.split(r"\b(?:19|20)\d{2}\b", name)
    if len(parts) > 1:
        name = parts[0]
        
    # Split by video quality/source keywords
    name = re.split(
        r"\b(?:1080p|2160p|720p|480p|bluray|webrip|web dl|"
        r"bdrip|hdrip|remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
        name, maxsplit=1, flags=re.IGNORECASE,
    )[0]
    
    return re.sub(r"\s+", " ", name).strip().title()


def show_id(filename: str) -> str:
    title = parse_show_title(filename)
    return movie_id(title)