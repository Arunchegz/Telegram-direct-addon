import os
import re
import httpx
from rapidfuzz import fuzz

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

TMDB_URL = "https://api.themoviedb.org/3"
CINEMETA_URL = "https://v3-cinemeta.strem.io"

# Shared connection-pooled client — avoids a TLS handshake per resolve call.
# Mirrors the pattern in state.py (_get_http_client).
_http_client: httpx.AsyncClient | None = None


def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=10, follow_redirects=True)
    return _http_client


# --------------------------------------------------
# Parse filename
# Delegates to state.parse_title_year so both modules share one parser
# and produce identical titles → consistent cache keys.
# --------------------------------------------------

def parse_title_year(filename: str):
    """Thin wrapper that reuses state.parse_title_year for consistency."""
    try:
        from state import parse_title_year as _st_parse
        title, year_str = _st_parse(filename)
        year = int(year_str) if year_str and year_str.isdigit() else None
        return title, year
    except Exception:
        pass
    # Fallback (state not importable): local implementation
    name = os.path.splitext(filename)[0]
    name = name.replace(".", " ").replace("_", " ")
    year = None
    m = re.search(r"(19|20)\d{2}", name)
    if m:
        year = int(m.group())
        name = name[:m.start()]
    for word in ["1080p","720p","2160p","hdrip","webrip","webdl","bluray",
                 "x264","x265","hevc","10bit","aac","dd5","esub","proper","hq","hdr","dv"]:
        name = re.sub(rf"\b{word}\b", "", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip(), year


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

    client = _get_http_client()
    r = await client.get(
        f"{TMDB_URL}/search/movie",
        params=params,
    )

    if r.status_code != 200:
        return None

    results = r.json().get("results", [])

    if not results:
        return None

    return results[0]


# --------------------------------------------------
# TMDB -> IMDb
# --------------------------------------------------

async def tmdb_to_imdb(tmdb_id):
    client = _get_http_client()
    r = await client.get(
        f"{TMDB_URL}/movie/{tmdb_id}/external_ids",
        params={"api_key": TMDB_API_KEY},
    )

    if r.status_code != 200:
        return None

    return r.json().get("imdb_id")


# --------------------------------------------------
# Cinemeta by IMDb
# --------------------------------------------------

async def cinemeta_from_imdb(imdb_id):
    client = _get_http_client()
    r = await client.get(
        f"{CINEMETA_URL}/meta/movie/{imdb_id}.json"
    )

    if r.status_code != 200:
        return None

    return r.json().get("meta")


# --------------------------------------------------
# Cinemeta Search
# --------------------------------------------------

async def cinemeta_search(title):
    client = _get_http_client()
    r = await client.get(
        f"{CINEMETA_URL}/catalog/movie/top/search={title}.json"
    )

    if r.status_code != 200:
        return []

    return r.json().get("metas", [])


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

    print("Title :", title)
    print("Year  :", year)

    # --------------------------------------
    # Step 1
    # TMDB
    # --------------------------------------
    if TMDB_API_KEY:
        tmdb = await tmdb_search(title, year)
        if tmdb:
            imdb = await tmdb_to_imdb(tmdb["id"])
            if imdb:
                meta = await cinemeta_from_imdb(imdb)
                if meta:
                    print("Matched via TMDB")
                    return meta

    # --------------------------------------
    # Step 2
    # Fallback to Cinemeta
    # --------------------------------------
    metas = await cinemeta_search(title)
    if not metas:
        return None

    best = best_similarity_match(
        title,
        year,
        metas,
    )

    if best:
        print("Matched via Cinemeta")
        return best

    return None
