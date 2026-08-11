import asyncio
import os
import re
import httpx
from rapidfuzz import fuzz

from state import parse_title_year as _state_parse_title_year, _get_http_client

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

TMDB_URL = "https://api.themoviedb.org/3"
CINEMETA_URL = "https://v3-cinemeta.strem.io"

# HTTP client is shared with state.py — single connection pool, no duplicate TLS handshakes.


async def _http_get_json(url: str, params: dict | None = None, attempts: int = 2):
    """GET with one retry — Cinemeta is flaky (429s/timeouts) and TMDB is
    third-party; a single failure would otherwise abort matching outright.
    Returns parsed JSON on success, None on final failure (callers degrade
    gracefully)."""
    for attempt in range(attempts):
        try:
            r = await _get_http_client().get(url, params=params)
            if r.status_code == 200:
                return r.json()
        except (httpx.HTTPError, ValueError):
            pass
        if attempt < attempts - 1:
            await asyncio.sleep(1.0 * (attempt + 1))
    return None


# --------------------------------------------------
# Parse filename
# Delegates to state.parse_title_year so both modules share one parser
# and produce identical titles → consistent cache keys.
# --------------------------------------------------

def parse_title_year(filename: str):
    title, year_str = _state_parse_title_year(filename)
    year = int(year_str) if year_str and year_str.isdigit() else None
    return title, year


# --------------------------------------------------
# TMDB SEARCH
# --------------------------------------------------

async def tmdb_search(title, year):
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
    }

    if year:
        params["year"] = year

    data = await _http_get_json(f"{TMDB_URL}/search/movie", params=params)
    if not data:
        return None

    results = data.get("results", [])

    if not results:
        return None

    return results[0]


# --------------------------------------------------
# TMDB -> IMDb
# --------------------------------------------------

async def tmdb_to_imdb(tmdb_id):
    data = await _http_get_json(
        f"{TMDB_URL}/movie/{tmdb_id}/external_ids",
        params={"api_key": TMDB_API_KEY},
    )
    if not data:
        return None

    return data.get("imdb_id")


# --------------------------------------------------
# Cinemeta by IMDb
# --------------------------------------------------

async def cinemeta_from_imdb(imdb_id):
    data = await _http_get_json(f"{CINEMETA_URL}/meta/movie/{imdb_id}.json")
    if not data:
        return None

    return data.get("meta")


# --------------------------------------------------
# Cinemeta Search
# --------------------------------------------------

async def cinemeta_search(title):
    data = await _http_get_json(f"{CINEMETA_URL}/catalog/movie/top/search={title}.json")
    if not data:
        return []

    return data.get("metas", [])


# --------------------------------------------------
# Similarity Match
# --------------------------------------------------

def best_similarity_match(title, year, metas):
    best = None
    score = 0

    for meta in metas:
        meta_name = meta.get("name", "")
        s = fuzz.token_sort_ratio(
            title.lower(),
            meta_name.lower(),
        )

        meta_year_val = meta.get("year")
        meta_year = None
        if meta_year_val:
            try:
                if isinstance(meta_year_val, int):
                    meta_year = meta_year_val
                elif isinstance(meta_year_val, str):
                    ym = re.search(r"\b(19|20)\d{2}\b", meta_year_val)
                    if ym:
                        meta_year = int(ym.group(0))
            except Exception:
                pass

        if year and meta_year:
            if abs(meta_year - year) == 0:
                s += 20
            elif abs(meta_year - year) == 1:
                s += 10

        if s > score:
            score = s
            best = meta

    if score < 80:
        return None

    return best


# --------------------------------------------------
# MAIN FUNCTION
# --------------------------------------------------

async def resolve_movie(filename):
    title, year = parse_title_year(filename)

    # Step 1: TMDB (if API key configured)
    if TMDB_API_KEY:
        tmdb = await tmdb_search(title, year)
        if tmdb:
            imdb = await tmdb_to_imdb(tmdb["id"])
            if imdb:
                meta = await cinemeta_from_imdb(imdb)
                if meta:
                    return meta

    # Step 2: Fallback to Cinemeta search
    metas = await cinemeta_search(title)
    if not metas:
        return None

    best = best_similarity_match(title, year, metas)
    if best:
        return best

    return None
