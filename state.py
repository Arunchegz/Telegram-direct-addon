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
import unicodedata
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
    """Returns (poster_url, imdb_id). imdb_id is '' if not found."""
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


# ── Advanced Matching Logic ──────────────────────────────────────────────────

def _normalize_filename(text: str) -> str:
    """Normalizes separators, strips extensions, normalizes numbers, and season/episode terms."""
    if not text:
        return ""
    # Strip extension
    text = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", text)
    # Normalize separators to spaces
    text = re.sub(r"[._\-–—+]", " ", text)
    # Normalize numbers words to digits (basic)
    text = re.sub(r"\bone\b", "1", text, flags=re.IGNORECASE)
    text = re.sub(r"\btwo\b", "2", text, flags=re.IGNORECASE)
    text = re.sub(r"\bthree\b", "3", text, flags=re.IGNORECASE)
    text = re.sub(r"\bfour\b", "4", text, flags=re.IGNORECASE)
    text = re.sub(r"\bfive\b", "5", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsix\b", "6", text, flags=re.IGNORECASE)
    text = re.sub(r"\bseven\b", "7", text, flags=re.IGNORECASE)
    text = re.sub(r"\beight\b", "8", text, flags=re.IGNORECASE)
    text = re.sub(r"\bnine\b", "9", text, flags=re.IGNORECASE)
    # Normalize season/episode terms
    text = re.sub(r"\btemporada\b", "season", text, flags=re.IGNORECASE)
    text = re.sub(r"\bcapitulo\b", "episode", text, flags=re.IGNORECASE)
    text = re.sub(r"\bseason\b", "season", text, flags=re.IGNORECASE)
    text = re.sub(r"\bepisode\b", "episode", text, flags=re.IGNORECASE)
    # Lowercase and clean
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_season_episode(filename: str) -> tuple[Optional[int], Optional[int]]:
    """
    Uses regexes to find (season, episode).
    Returns (season, episode) or (1, episode) for standalone episodes.
    Returns (None, None) if no SE found.
    """
    # SxxExx / S1E5
    m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,3})", filename)
    if m:
        return int(m.group(1)), int(m.group(2))
    
    # Spanish/Portuguese: Temporada N Capitulo M
    m2 = re.search(r"[Tt]emporada[\s._-]*(\d+)[\s\S]*?[Cc]apitulo[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m2:
        return int(m2.group(1)), int(m2.group(2))
        
    # English: Season N Episode M
    m3 = re.search(r"[Ss]eason[\s._-]*(\d+)[\s\S]*?[Ee]pisode[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m3:
        return int(m3.group(1)), int(m3.group(2))

    # Standalone Episode: Episode N or Capitulo N or just N at end (if context implies)
    # We look for "Episode N" or "Capitulo N"
    m4 = re.search(r"[Ee]pisode[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m4:
        return 1, int(m4.group(1))
        
    m5 = re.search(r"[Cc]apitulo[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m5:
        return 1, int(m5.group(1))

    # Season N only
    m6 = re.search(r"[Ss]eason[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m6:
        return int(m6.group(1)), 1
        
    m7 = re.search(r"[Tt]emporada[\s._-]*(\d+)", filename, re.IGNORECASE)
    if m7:
        return int(m7.group(1)), 1

    return None, None


def normalize_title(title: str) -> str:
    """Removes diacritics, converts Roman numerals (II-X to 2-10), and lowers/strips."""
    if not title:
        return ""
    # Remove diacritics via NFD decomposition
    nfkd = unicodedata.normalize('NFD', title)
    title = ''.join(c for c in nfkd if unicodedata.category(c) != 'Mn')
    
    # Convert Roman Numerals II to X to numbers
    # Simple replacement for common ones
    roman_map = {
        'II': '2', 'III': '3', 'IV': '4', 'V': '5', 'VI': '6',
        'VII': '7', 'VIII': '8', 'IX': '9', 'X': '10'
    }
    for roman, num in roman_map.items():
        # Use word boundaries to avoid replacing parts of other words
        title = re.sub(r'\b' + roman + r'\b', num, title, flags=re.IGNORECASE)
        
    return title.lower().strip()


def _clean_title_prefix(filename: str) -> str:
    """Extracts filename prefix up to the season/episode or year."""
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", filename)
    name = re.sub(r"[._\-–—+]", " ", name)
    
    # Remove Season/Episode markers
    name = re.sub(r"\b[Ss]\d{1,2}[Ee]\d{1,3}\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\b[Ss]eason\s*\d+\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\b[Ee]pisode\s*\d+\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\b[Tt]emporada\s*\d+\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\b[Cc]apitulo\s*\d+\b", "", name, flags=re.IGNORECASE)
    
    # Remove Year
    name = re.sub(r"\b(?:19|20)\d{2}\b", "", name)
    
    # Remove Quality/Source keywords
    name = re.sub(
        r"\b(?:1080p|2160p|720p|480p|bluray|webrip|web dl|"
        r"bdrip|hdrip|remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
        "", name, flags=re.IGNORECASE
    )
    
    return re.sub(r"\s+", " ", name).strip().lower()


def matches_title(filename: str, title: str) -> bool:
    """Checks if title is in prefix, or all major keywords are in prefix."""
    prefix = _clean_title_prefix(filename)
    norm_title = normalize_title(title)
    norm_prefix = normalize_title(prefix)
    
    if not norm_title or not norm_prefix:
        return False
        
    # Exact match of normalized strings
    if norm_title == norm_prefix:
        return True
        
    # Title is contained in prefix
    if norm_title in norm_prefix:
        return True
        
    # Check if all major keywords from title are in prefix
    title_words = set(norm_title.split())
    prefix_words = set(norm_prefix.split())
    
    # Filter out very short words (like 'the', 'a', 'of') if desired, 
    # but for robustness we check most.
    # Let's require at least 70% of title words to be in prefix, min 1 word.
    if not title_words:
        return False
        
    matches = sum(1 for w in title_words if w in prefix_words)
    return matches >= max(1, len(title_words) * 0.7)


class VideoMatcher:
    """
    Robust score-based matching logic for Stremio/Telegram integration.
    """
    DEFAULT_THRESHOLD = 55

    @staticmethod
    def calculate_match_score(filename: str, title: str, year: str, season: int, episode: int) -> int:
        """
        Calculates a match score between a file and a meta object.
        Returns score between 0 and 100.
        """
        score = 0
        
        # 1. Title Match
        if not matches_title(filename, title):
            return 0  # Immediate rejection if title doesn't match at all
        
        score += 20  # Base score for title match

        # 2. Year Match
        file_year = None
        ym = re.search(r"\b(19|20)\d{2}\b", filename)
        if ym:
            file_year = int(ym.group(0))
        
        if year:
            try:
                meta_year = int(year)
                if file_year == meta_year:
                    score += 20  # Exact year match
                elif file_year and abs(file_year - meta_year) == 1:
                    score += 5   # Off-by-1 year tolerance
                else:
                    score -= 10  # Mismatch
            except ValueError:
                pass
        else:
            # No year in meta, if file has year, slight penalty or neutral? 
            # Spec says: no-year is +5. This implies if meta has no year, we give +5 if file has one? 
            # Or if meta has no year, we don't penalize? 
            # "no-year is +5" usually means if the meta object lacks a year, we give a small bonus for matching files that DO have a year (as it's better than no info).
            # Let's interpret: If meta year is empty, we give +5 if file has a year.
            if file_year:
                score += 5
        
        # 3. Season/Episode Match
        file_season, file_episode = parse_season_episode(filename)
        
        if season is not None and episode is not None:
            # Specific SE requested
            if file_season is not None and file_episode is not None:
                if file_season == season and file_episode == episode:
                    score += 20  # Exact SE match
                else:
                    score = 0    # Mismatch is immediate rejection (0)
            else:
                # File has no SE info, but meta requests specific SE
                # This is a mismatch for specific SE request
                score = 0
        else:
            # No specific SE requested (e.g. movie or general series listing)
            if file_season is not None and file_episode is not None:
                # File has SE, meta doesn't care. 
                # Spec: "no SE in file is -10". 
                # Implies: If file HAS SE, it's fine? Or is it neutral?
                # Let's assume neutral if meta doesn't specify SE.
                pass
            else:
                # File has no SE info
                score -= 10

        # Cap score
        score = max(0, min(100, score))
        return score
