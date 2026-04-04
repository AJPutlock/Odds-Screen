"""
Betting Odds Screen - Backend
Multi-sport, multi-source: The Odds API + bet365 scraper.
"""

import threading
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, send_from_directory, request
import requests

import random

# bet365 scraper (optional — gracefully absent if scrapers/ not present)
try:
    from scrapers.bet365 import fetch_bet365
    from scrapers.parse_har import games_from_har
    from scrapers.parse_bookmaker_har import games_from_bookmaker_har
    BET365_AVAILABLE = True
except ImportError:
    BET365_AVAILABLE = False
    def fetch_bet365(sport_key, **kw):
        return None
    def games_from_har(path):
        return []
    def games_from_bookmaker_har(path):
        return []

# ── bet365 background refresh ─────────────────────────────────────────────────
# Fetches independently of the Odds API on a randomised interval to avoid
# detection. Base interval 10 min ± 90 sec jitter.
B365_BASE_INTERVAL = 420   # seconds (7 min)
B365_JITTER        = 90    # ± seconds

_b365_caches: dict = {}       # sport_key -> {data, last_updated, next_refresh, status, error}
_b365_lock = threading.Lock()
_b365_timers: dict = {}       # sport_key -> threading.Timer
_b365_login_events: dict = {} # sport_key -> threading.Event (set when user confirms login)

_bkmkr_caches: dict = {}      # sport_key -> {data, last_updated, status}
_bkmkr_lock = threading.Lock()


def _b365_next_interval() -> float:
    return B365_BASE_INTERVAL + random.uniform(-B365_JITTER, B365_JITTER)


def _b365_refresh(sport_key: str, login_event=None):
    """Run a bet365 fetch and update cache. No auto-reschedule — manual only."""
    games = fetch_bet365(sport_key, login_event=login_event) if BET365_AVAILABLE else None

    with _b365_lock:
        _b365_caches[sport_key] = {
            "data":         games or [],
            "last_updated": datetime.now(timezone.utc).isoformat() if games is not None else
                            (_b365_caches.get(sport_key) or {}).get("last_updated"),
            "next_refresh": None,
            "status":       "ready" if games is not None else "error",
            "error":        None if games is not None else "Fetch failed",
        }


FORCE_REFRESH_AFTER = 300  # seconds — treat cache as stale after 5 min on manual reload


def b365_trigger(sport_key: str, force: bool = False):
    """
    Trigger a bet365 fetch in the background.
    - force=False (default): skip if already loading or recently fetched
    - force=True: always re-fetch, cancelling any pending timer (used on manual reload)
    """
    with _b365_lock:
        cache = _b365_caches.get(sport_key)

        # Always skip if already mid-fetch
        if cache and cache.get("status") == "loading":
            return

        if not force:
            # Skip if data is fresh (fetched within FORCE_REFRESH_AFTER seconds)
            last = cache.get("last_updated") if cache else None
            if last:
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds()
                if age < FORCE_REFRESH_AFTER:
                    return

        # Set loading state and cancel any pending auto-refresh timer
        _b365_caches[sport_key] = {
            "data":         (cache or {}).get("data", []),
            "last_updated": (cache or {}).get("last_updated"),
            "next_refresh": None,
            "status":       "loading",
            "error":        None,
        }
        existing = _b365_timers.pop(sport_key, None)
        if existing:
            existing.cancel()

    t = threading.Thread(target=_b365_refresh, args=[sport_key], daemon=True)
    t.start()


def b365_get(sport_key: str) -> dict:
    with _b365_lock:
        return dict(_b365_caches.get(sport_key) or {
            "data": [], "last_updated": None,
            "next_refresh": None, "status": "idle", "error": None,
        })

app = Flask(__name__, static_folder="static")

# ── Configuration ─────────────────────────────────────────────────────────────
#API_KEY  = "82ad3d819597cf9b56b60fb682f2df87"
API_KEY  = "6564dbf4768c34046a96a283c099e47f"
BASE_URL = "https://api.the-odds-api.com/v4"

DEFAULT_MARKETS = ["h2h", "spreads", "totals"]

# Sport-specific markets to fetch (full game + partials)
SPORT_MARKETS = {
    "basketball_nba":         ["h2h", "spreads", "totals", "h2h_h1", "spreads_h1", "totals_h1", "h2h_q1", "spreads_q1", "totals_q1"],
    "americanfootball_nfl":   ["h2h", "spreads", "totals", "h2h_h1", "spreads_h1", "totals_h1", "h2h_q1", "spreads_q1", "totals_q1"],
    "americanfootball_ncaaf": ["h2h", "spreads", "totals", "h2h_h1", "spreads_h1", "totals_h1", "h2h_q1", "spreads_q1", "totals_q1"],
    "basketball_ncaab":       ["h2h", "spreads", "totals", "h2h_h1", "spreads_h1", "totals_h1"],
    "icehockey_nhl":          ["h2h", "spreads", "totals", "h2h_p1", "spreads_p1", "totals_p1"],
    "baseball_mlb":           ["h2h", "spreads", "totals", "h2h_1st_5_innings", "spreads_1st_5_innings", "totals_1st_5_innings", "h2h_1st_1_innings", "spreads_1st_1_innings", "totals_1st_1_innings"],
    "baseball_ncaa":          ["h2h", "spreads", "totals"],
}

MARKET_DISPLAY = {
    # Full game
    "h2h":        "Moneyline",
    "spreads":    "Spread",
    "totals":     "Total",
    # 1st half / F5 (MLB) / 1st period (NHL — overridden per-sport below)
    "h2h_h1":     "1H ML",
    "spreads_h1": "1H Spread",
    "totals_h1":  "1H Total",
    # Quarter / 1st inning (MLB — overridden per-sport below)
    "h2h_q1":     "Q1 ML",
    "spreads_q1": "Q1 Spread",
    "totals_q1":  "Q1 Total",
    # Period (NHL)
    "h2h_p1":     "P1 ML",
    "spreads_p1": "P1 Spread",
    "totals_p1":  "P1 Total",
}

# Per-sport overrides for market display labels
SPORT_MARKET_DISPLAY = {
    "baseball_mlb": {
        "h2h_1st_5_innings":     "F5 ML",
        "spreads_1st_5_innings": "F5 Spread",
        "totals_1st_5_innings":  "F5 Total",
        "h2h_1st_1_innings":     "1st Inn ML",
        "spreads_1st_1_innings": "1st Inn Spread",
        "totals_1st_1_innings":  "1st Inn Total",
    },
}

# ── Sports ────────────────────────────────────────────────────────────────────
SPORTS = {
    "basketball_ncaab": {
        "label": "NCAAB", "sublabel": "Men's CBB", "emoji": "🏀",
    },
    "basketball_nba": {
        "label": "NBA", "sublabel": "Pro Basketball", "emoji": "🏀",
    },
    "americanfootball_nfl": {
        "label": "NFL", "sublabel": "Pro Football", "emoji": "🏈",
    },
    "americanfootball_ncaaf": {
        "label": "NCAAF", "sublabel": "College Football", "emoji": "🏈",
    },
    "baseball_mlb": {
        "label": "MLB", "sublabel": "Pro Baseball", "emoji": "⚾",
    },
    "baseball_ncaa": {
        "label": "NCAA Baseball", "sublabel": "College Baseball", "emoji": "⚾",
    },
    "icehockey_nhl": {
        "label": "NHL", "sublabel": "Pro Hockey", "emoji": "🏒",
    },
}

# ── Sportsbooks ───────────────────────────────────────────────────────────────
# To add a book:    add its Odds API key to BOOKMAKERS + BOOKMAKER_DISPLAY + DISPLAY_BOOKS
# To remove a book: delete it from BOOKMAKERS (stops API fetch) and DISPLAY_BOOKS (hides column)
#                   Leave it in BOOKMAKER_DISPLAY so the display name is still defined.
# bet365 / bookmaker: HAR-scraped — never in BOOKMAKERS, only in DISPLAY_BOOKS
# betonlineag:        reference-only (sharp line, excluded from best-available calc)
#
# All known book keys (comment/uncomment to enable or disable):
BOOKMAKERS = [
    # "novig",          # not available in all states
    "draftkings",
    "fanduel",
    "williamhill_us",
    "hardrockbet",
    "fanatics",
    "espnbet",
    "betmgm",
    # "betrivers",      # not available in all states
    "betonlineag",
]

# Display name for every book key (keep all entries here even if a book is disabled)
BOOKMAKER_DISPLAY = {
    "novig":          "NoVig",
    "draftkings":     "DraftKings",
    "fanduel":        "FanDuel",
    "williamhill_us": "Caesars",
    "bet365":         "bet365",       # scraped — HAR import only
    "bookmaker":      "Bookmaker",    # scraped — HAR import only
    "hardrockbet":    "Hard Rock",
    "fanatics":       "Fanatics",
    "espnbet":        "theScore",
    "betmgm":         "BetMGM",
    "betrivers":      "BetRivers",
    "betonlineag":    "BetOnline",
}

# Column order in the UI — remove a key here to hide its column
DISPLAY_BOOKS = [
    "bookmaker",      # HAR-scraped sharp reference
    # "novig",        # disabled — not available in all states
    "draftkings",
    "fanduel",
    "williamhill_us",
    # "bet365",       # disabled — not available in all states
    "hardrockbet",
    "fanatics",
    "espnbet",
    "betmgm",
    # "betrivers",    # disabled — not available in all states
]

# ── Available markets cache (fetched once per sport on first load) ─────────────
_available_markets: dict = {}   # sport_key -> set of market keys

def get_available_markets(sport_key: str) -> set:
    """Fetch the list of markets available for a sport from the Odds API (free call)."""
    if sport_key in _available_markets:
        return _available_markets[sport_key]
    try:
        resp = requests.get(
            f"{BASE_URL}/sports/{sport_key}/markets",
            params={"apiKey": API_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        keys = {m["key"] for m in resp.json()}
        print(f"[markets] {sport_key} available: {sorted(keys)}")
        _available_markets[sport_key] = keys
        return keys
    except Exception as e:
        print(f"[markets] {sport_key} failed to fetch markets: {e} — using all configured")
        return set()   # empty = don't filter, try everything

# ── Cache ─────────────────────────────────────────────────────────────────────
_caches = {
    sk: {
        "data": [], "raw_games": [], "last_updated": None,
        "remaining_requests": None, "used_requests": None, "error": None,
    }
    for sk in SPORTS
}
_lock = threading.Lock()


# ── Odds math ─────────────────────────────────────────────────────────────────

def american_odds(decimal_odds):
    if decimal_odds is None:
        return None
    try:
        d = float(decimal_odds)
        if d <= 1.0:
            return None
        if d >= 2.0:
            return f"+{int(round((d - 1) * 100))}"
        else:
            return str(int(round(-100 / (d - 1))))
    except Exception:
        return None


# ── Team name matching for bet365 merge ───────────────────────────────────────

# ── Team name alias table ─────────────────────────────────────────────────────
# Maps known alternate names → canonical token form used for matching.
# Add entries here whenever a team fails to match between Odds API and bet365.
# Keys are lowercased, punctuation-stripped — same as _normalize output.
_ALIASES: dict[str, str] = {
    # ── NBA ───────────────────────────────────────────────────────────────────
    # bet365 uses 2-3 letter city abbreviations; Odds API uses full city names.
    # Teams whose abbreviation scores ≤ 0.25 against the full name (2-letter
    # abbrev vs 3-word city) need aliases so the combined score clears 0.6.
    "ny knicks":          "new york knicks",
    "la lakers":          "los angeles lakers",
    "la clippers":        "los angeles clippers",
    "gs warriors":        "golden state warriors",
    "sa spurs":           "san antonio spurs",
    "no pelicans":        "new orleans pelicans",   # bet365 uses NO not NOP
    "nop pelicans":       "new orleans pelicans",   # keep old alias too
    # Teams with overlap = 0.333 that can tip below 0.6 when paired with
    # another low-scorer (e.g. two 0.25 teams combined = 0.50 < 0.60).
    "cle cavaliers":      "cleveland cavaliers",
    "okc thunder":        "oklahoma city thunder",
    # ── NHL ───────────────────────────────────────────────────────────────────
    # Same issue: 2-letter abbreviation vs 3-word city name scores only 0.25.
    "la kings":           "los angeles kings",
    "sj sharks":          "san jose sharks",
    "tb lightning":       "tampa bay lightning",
    "ny islanders":       "new york islanders",
    "ny rangers":         "new york rangers",
    "nj devils":          "new jersey devils",
    # Utah — team renamed to Utah Mammoth for 2025-26; Odds API may use either
    "uta mammoth":        "utah mammoth",
    "utah hockey club":   "utah mammoth",
    # VGS is bet365's abbreviation for Vegas Golden Knights
    "vgs golden knights": "vegas golden knights",
    # NCAAB
    "georgia st panthers":       "georgia state",
    "georgia st":                "georgia state",
    "louisiana ragin cajuns":    "ul lafayette",
    "louisiana":                 "ul lafayette",
    "umbc retrievers":           "md baltimore co",
    "umbc":                      "md baltimore co",
    "njit highlanders":          "njit",
    "unc asheville":             "north carolina asheville",
    "etsu":                      "east tennessee state",
    "east tennessee st":         "east tennessee state",
    "utep miners":               "texas el paso",
    "utep":                      "texas el paso",
    "uic flames":                "illinois chicago",
    "uic":                       "illinois chicago",
    "ucf knights":               "central florida",
    "ucf":                       "central florida",
    "uconn huskies":             "connecticut",
    "uconn":                     "connecticut",
    "vcu rams":                  "virginia commonwealth",
    "vcu":                       "virginia commonwealth",
    "smu mustangs":              "southern methodist",
    "smu":                       "southern methodist",
    "unlv rebels":               "nevada las vegas",
    "unlv":                      "nevada las vegas",
    "lsu tigers":                "louisiana state",
    "lsu":                       "louisiana state",
    "ole miss rebels":           "mississippi",
    "ole miss":                  "mississippi",
    "tcu horned frogs":          "texas christian",
    "tcu":                       "texas christian",
    "pitt panthers":             "pittsburgh",
    "pitt":                      "pittsburgh",
    "usc trojans":               "southern california",
    "usc":                       "southern california",
    "a&m":                       "am",
    "texas am aggies":           "texas am",
    "oklahoma st cowboys":        "oklahoma state",
    "oklahoma st":                "oklahoma state",
    "arkansas pine bluff golden lions": "arkansas pine bluff",
    "jackson st tigers":          "jackson state",
    "jackson st":                 "jackson state",
    "miss valley st delta devils":  "mississippi valley state",
    "miss valley st":               "mississippi valley state",
    "alcorn st braves":             "alcorn state",
    "alcorn st":                    "alcorn state",
    "ohio bobcats":               "ohio",
    "massachusetts minutemen":    "umass",
    "massachusetts":              "umass",
    "san jose st spartans":       "san jose state",
    "san jose st":                "san jose state",
    "fresno st bulldogs":         "fresno state",
    "fresno st":                  "fresno state",
}


def _normalize(name: str) -> str:
    """Lowercase, strip punctuation/articles for fuzzy matching."""
    import re
    name = name.lower().strip()
    # Normalize accented characters (e.g. José -> jose)
    import unicodedata
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^a-z0-9 ]", "", name)
    for prefix in ("the ", "university of ", "university "):
        if name.startswith(prefix):
            name = name[len(prefix):]
    name = name.strip()
    return _ALIASES.get(name, name)


def _match_teams(api_away: str, api_home: str,
                 b365_games: list) -> dict | None:
    """
    Find the best-matching bet365 game for an Odds API game.
    Returns the bet365 game dict or None if no confident match.
    """
    api_a = _normalize(api_away)
    api_h = _normalize(api_home)

    best_score = 0
    best_game  = None

    for g in b365_games:
        b_a = _normalize(g["away_team"])
        b_h = _normalize(g["home_team"])

        # Simple token overlap scoring
        def overlap(s1, s2):
            t1, t2 = set(s1.split()), set(s2.split())
            return len(t1 & t2) / max(len(t1 | t2), 1)

        score = overlap(api_a, b_a) + overlap(api_h, b_h)
        if score > best_score:
            best_score = score
            best_game  = g

    # Require at least 0.5 overlap on each side combined
    return best_game if best_score >= 0.6 else None


# ── Fetch & process ───────────────────────────────────────────────────────────

def _market_sort_key(market: str) -> tuple:
    """
    Returns (period_rank, type_rank) so rows sort as:
      period 0 = full game   (h2h, spreads, totals)
      period 1 = 1st half / F5 / 1st period
      period 2 = 1st quarter / 1st inning
    Within each period: ML(0) → spread(1) → total(2)
    """
    _PERIOD = {
        # Full game
        "h2h": 0, "spreads": 0, "totals": 0,
        # 1st half / F5 (MLB) / 1st period (NHL)
        "h2h_h1": 1, "spreads_h1": 1, "totals_h1": 1,
        "h2h_p1": 1, "spreads_p1": 1, "totals_p1": 1,
        "h2h_1st_5_innings": 1, "spreads_1st_5_innings": 1, "totals_1st_5_innings": 1,
        # 1st quarter / 1st inning (MLB)
        "h2h_q1": 2, "spreads_q1": 2, "totals_q1": 2,
        "h2h_1st_1_innings": 2, "spreads_1st_1_innings": 2, "totals_1st_1_innings": 2,
    }
    period = _PERIOD.get(market, 9)
    mtype  = 0 if market.startswith("h2h") else 1 if market.startswith("spreads") else 2 if market.startswith("totals") else 9
    return (period, mtype)


def fetch_odds(sport_key: str):
    if sport_key not in SPORTS:
        return None

    now_utc       = datetime.now(timezone.utc)
    commence_from = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    commence_to   = (now_utc + timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%SZ")

    configured_markets = SPORT_MARKETS.get(sport_key, DEFAULT_MARKETS)

    # Split into full-game (bulk endpoint) and partial-game (events endpoint)
    full_markets    = [m for m in configured_markets if m in DEFAULT_MARKETS]
    partial_markets = [m for m in configured_markets if m not in DEFAULT_MARKETS]
    partial_batches = [partial_markets[i:i+3] for i in range(0, len(partial_markets), 3)]

    b365_games = b365_get(sport_key).get("data") or []

    all_rows       = []
    api_error      = None
    remaining      = None
    used           = None
    full_game_data = []

    # ── Step 1: Full-game markets via bulk endpoint ───────────────────────────
    print(f"[fetch_odds] {sport_key} — requesting full-game markets: {full_markets}")
    params = {
        "apiKey":           API_KEY,
        "regions":          "us",
        "markets":          ",".join(full_markets),
        "oddsFormat":       "decimal",
        "bookmakers":       ",".join(BOOKMAKERS),
        "dateFormat":       "iso",
        "commenceTimeFrom": commence_from,
        "commenceTimeTo":   commence_to,
    }
    try:
        resp = requests.get(
            f"{BASE_URL}/sports/{sport_key}/odds",
            params=params,
            timeout=15,
        )
        if not resp.ok:
            print(f"[fetch_odds] {sport_key} — full-game HTTP {resp.status_code} body: {resp.text}")
        resp.raise_for_status()
        remaining      = resp.headers.get("x-requests-remaining")
        used           = resp.headers.get("x-requests-used")
        full_game_data = resp.json()
        print(f"[fetch_odds] {sport_key} — full-game returned {len(full_game_data)} games")
        rows = process_games(full_game_data, b365_games, sport_key,
                             markets_override=full_markets)
        print(f"[fetch_odds] {sport_key} — full-game produced {len(rows)} rows")
        all_rows.extend(rows)
    except requests.exceptions.RequestException as e:
        print(f"[fetch_odds] {sport_key} — FAILED full-game: {e}")
        api_error = str(e)

    # ── Step 2: Partial markets via per-event endpoint ────────────────────────
    # /v4/sports/{sport}/events/{eventId}/odds supports h2h_h1, spreads_h1, etc.
    # The bulk /sports/{sport}/odds endpoint does NOT support these markets.
    if partial_batches and full_game_data:
        print(f"[fetch_odds] {sport_key} — fetching partial markets "
              f"({partial_markets}) for {len(full_game_data)} events")
        for game in full_game_data:
            event_id = game.get("id")
            if not event_id:
                continue
            for batch in partial_batches:
                params = {
                    "apiKey":     API_KEY,
                    "regions":    "us",
                    "markets":    ",".join(batch),
                    "oddsFormat": "decimal",
                    "bookmakers": ",".join(BOOKMAKERS),
                    "dateFormat": "iso",
                }
                try:
                    resp = requests.get(
                        f"{BASE_URL}/sports/{sport_key}/events/{event_id}/odds",
                        params=params,
                        timeout=15,
                    )
                    if not resp.ok:
                        print(f"[fetch_odds] {sport_key} — event {event_id} "
                              f"{batch} HTTP {resp.status_code}: {resp.text}")
                        resp.raise_for_status()
                    remaining  = resp.headers.get("x-requests-remaining")
                    used       = resp.headers.get("x-requests-used")
                    event_data = resp.json()   # single game object, not a list
                    rows = process_games([event_data], b365_games, sport_key,
                                        markets_override=batch)
                    print(f"[fetch_odds] {sport_key} — event {event_id} "
                          f"{batch} produced {len(rows)} rows")
                    all_rows.extend(rows)
                except requests.exceptions.RequestException as e:
                    print(f"[fetch_odds] {sport_key} — FAILED event {event_id} {batch}: {e}")

    # Re-sort combined rows from all batches:
    # group by game, then period (full → half/F5/1st-period → quarter/1st-inning),
    # then market type (ML → spread → total) within each period.
    all_rows.sort(key=lambda r: (r["commence_time"] or "", r["away_team"]) + _market_sort_key(r["market"]))

    with _lock:
        _caches[sport_key]["data"]               = all_rows
        _caches[sport_key]["raw_games"]          = full_game_data
        _caches[sport_key]["last_updated"]       = datetime.now(timezone.utc).isoformat()
        _caches[sport_key]["remaining_requests"] = remaining
        _caches[sport_key]["used_requests"]      = used
        _caches[sport_key]["error"]              = api_error

    return all_rows


def process_games(raw_games: list, b365_games: list, sport_key: str = "",
                  markets_override: list = None) -> list:
    rows = []

    for game in raw_games:
        try:
            ct = datetime.fromisoformat(
                game.get("commence_time", "").replace("Z", "+00:00")
            )
        except Exception:
            ct = None

        home_team = game.get("home_team", "")
        away_team = game.get("away_team", "")

        # Build lookup for Odds API books
        book_data = {}
        for bm in game.get("bookmakers", []):
            bk = bm.get("key", "")
            if bk not in BOOKMAKERS:
                continue
            book_data[bk] = {}
            for market in bm.get("markets", []):
                mk       = market.get("key", "")
                outcomes = {o["name"]: o for o in market.get("outcomes", [])}

                if mk.startswith("h2h"):
                    book_data[bk][mk] = {
                        "home_price": outcomes.get(home_team, {}).get("price"),
                        "away_price": outcomes.get(away_team, {}).get("price"),
                        "home_point": None, "away_point": None,
                    }
                elif mk.startswith("spreads"):
                    book_data[bk][mk] = {
                        "home_price": outcomes.get(home_team, {}).get("price"),
                        "away_price": outcomes.get(away_team, {}).get("price"),
                        "home_point": outcomes.get(home_team, {}).get("point"),
                        "away_point": outcomes.get(away_team, {}).get("point"),
                    }
                elif mk.startswith("totals"):
                    book_data[bk][mk] = {
                        "home_price": outcomes.get("Over",  {}).get("price"),
                        "away_price": outcomes.get("Under", {}).get("price"),
                        "home_point": outcomes.get("Over",  {}).get("point"),
                        "away_point": outcomes.get("Under", {}).get("point"),
                    }

        # Try to match a bet365 game and inject its odds
        b365_match = _match_teams(away_team, home_team, b365_games) if b365_games else None

        if b365_match:
            # New scraper returns a `markets` dict keyed by Odds API market names
            # (h2h, spreads, totals, h2h_h1, spreads_q1, h2h_p1, …).
            # Each value already uses {away_odds, home_odds, away_point, home_point}.
            b365_markets = b365_match.get("markets") or {}
            if b365_markets:
                book_data["bet365"] = b365_markets

        mkt_display_overrides = SPORT_MARKET_DISPLAY.get(sport_key, {})
        row_markets = markets_override if markets_override is not None else SPORT_MARKETS.get(sport_key, DEFAULT_MARKETS)
        for mk in row_markets:
            books_for_row = {}
            for bk in DISPLAY_BOOKS:
                entry = book_data.get(bk, {})
                if entry is None:
                    books_for_row[bk] = None
                    continue

                # bet365 odds are already in American format (strings)
                if bk == "bet365":
                    mkt_entry = entry.get(mk) if isinstance(entry, dict) else None
                    if mkt_entry:
                        books_for_row[bk] = {
                            "home_odds":  mkt_entry.get("home_odds"),
                            "away_odds":  mkt_entry.get("away_odds"),
                            "home_point": mkt_entry.get("home_point"),
                            "away_point": mkt_entry.get("away_point"),
                        }
                    else:
                        books_for_row[bk] = None
                else:
                    mkt_entry = entry.get(mk) if entry else None
                    if mkt_entry:
                        books_for_row[bk] = {
                            "home_odds":  american_odds(mkt_entry["home_price"]),
                            "away_odds":  american_odds(mkt_entry["away_price"]),
                            "home_point": mkt_entry["home_point"],
                            "away_point": mkt_entry["away_point"],
                        }
                    else:
                        books_for_row[bk] = None

            # Compute best BEFORE adding betonlineag (excluded from best-available)
            best_home, best_away = find_best(books_for_row, mk)

            # Now add betonlineag to books_for_row for display only
            # (excluded from find_best — reference book, not part of best-available)
            for ref_bk in ("betonlineag",):
                ref_entry = book_data.get(ref_bk, {})
                if ref_entry:
                    mkt_entry = ref_entry.get(mk) if ref_entry else None
                    if mkt_entry:
                        books_for_row[ref_bk] = {
                            "home_odds":  american_odds(mkt_entry["home_price"]),
                            "away_odds":  american_odds(mkt_entry["away_price"]),
                            "home_point": mkt_entry["home_point"],
                            "away_point": mkt_entry["away_point"],
                        }
                    else:
                        books_for_row[ref_bk] = None
                else:
                    books_for_row[ref_bk] = None

            rows.append({
                "game_id":       game.get("id", ""),
                "market":        mk,
                "market_label":  mkt_display_overrides.get(mk, MARKET_DISPLAY.get(mk, mk)),
                "home_team":     home_team,
                "away_team":     away_team,
                "commence_time": ct.isoformat() if ct else None,
                "best_home":     best_home,
                "best_away":     best_away,
                "novig_home":    None,   # reserved
                "novig_away":    None,   # reserved
                "books":         books_for_row,
            })

    rows.sort(key=lambda r: (r["commence_time"] or "", r["away_team"]))
    return rows


def find_best(books_for_row, market):
    def parse_odds(s):
        if s is None:
            return None
        try:
            return int(str(s).replace("+", ""))
        except ValueError:
            return None

    def is_better(c_pt, c_odds, cur_pt, cur_odds, side, mkt):
        if mkt.startswith("h2h"):
            if cur_odds is None: return True
            return c_odds > cur_odds
        elif mkt.startswith("spreads"):
            if cur_pt is None: return True
            if c_pt > cur_pt:  return True
            if c_pt == cur_pt: return c_odds > cur_odds
            return False
        elif mkt.startswith("totals"):
            if cur_pt is None: return True
            if side == "home":
                if c_pt < cur_pt:  return True
                if c_pt == cur_pt: return c_odds > cur_odds
            else:
                if c_pt > cur_pt:  return True
                if c_pt == cur_pt: return c_odds > cur_odds
            return False
        return False

    best = {"home": (None, None, None), "away": (None, None, None)}

    for bk, entry in books_for_row.items():
        if entry is None:
            continue
        for side in ("home", "away"):
            odds_val  = parse_odds(entry.get(f"{side}_odds"))
            point_val = entry.get(f"{side}_point")
            if odds_val is None:
                continue
            cur_pt, cur_odds, _ = best[side]
            if is_better(point_val, odds_val, cur_pt, cur_odds, side, market):
                best[side] = (point_val, odds_val, bk)

    def fmt(side):
        pt, odds_int, bk = best[side]
        if odds_int is None:
            return None
        return {
            "odds":     f"+{odds_int}" if odds_int >= 0 else str(odds_int),
            "book":     BOOKMAKER_DISPLAY.get(bk, bk),
            "book_key": bk,
            "point":    pt,
        }

    return fmt("home"), fmt("away")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/sports")
def get_sports():
    return jsonify({
        "sports":            list(SPORTS.keys()),
        "sport_meta":        SPORTS,
        "bookmakers":        DISPLAY_BOOKS,
        "bookmaker_display": BOOKMAKER_DISPLAY,
        "bet365_available":  BET365_AVAILABLE,
    })


@app.route("/api/odds/<sport_key>")
def get_odds(sport_key):
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    fetch_odds(sport_key)
    with _lock:
        c = _caches[sport_key]
        return jsonify({
            "sport_key":          sport_key,
            "data":               c["data"],
            "last_updated":       c["last_updated"],
            "remaining_requests": c["remaining_requests"],
            "used_requests":      c["used_requests"],
            "error":              c["error"],
            "bookmakers":         DISPLAY_BOOKS,
            "bookmaker_display":  BOOKMAKER_DISPLAY,
        })


@app.route("/api/bet365/open-browser/<sport_key>", methods=["POST"])
def bet365_open_browser(sport_key):
    """Open a visible Chrome window and wait for the user to log in."""
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    with _b365_lock:
        status = (_b365_caches.get(sport_key) or {}).get("status")
        if status in ("loading", "waiting_for_login"):
            return jsonify({"status": status})  # already in progress

    event = threading.Event()
    with _b365_lock:
        _b365_login_events[sport_key] = event
        _b365_caches[sport_key] = {
            "data":         (_b365_caches.get(sport_key) or {}).get("data", []),
            "last_updated": (_b365_caches.get(sport_key) or {}).get("last_updated"),
            "next_refresh": None,
            "status":       "waiting_for_login",
            "error":        None,
        }

    t = threading.Thread(target=_b365_refresh, args=[sport_key], kwargs={"login_event": event}, daemon=True)
    t.start()
    return jsonify({"status": "waiting_for_login"})


@app.route("/api/bet365/confirm-login/<sport_key>", methods=["POST"])
def bet365_confirm_login(sport_key):
    """Signal that the user has logged in — scraping starts immediately."""
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    with _b365_lock:
        event = _b365_login_events.get(sport_key)
        if not event:
            return jsonify({"error": "No pending login for this sport"}), 400
        _b365_caches[sport_key]["status"] = "loading"
        del _b365_login_events[sport_key]

    event.set()
    return jsonify({"status": "loading"})


@app.route("/api/bet365/import-har/<sport_key>", methods=["POST"])
def bet365_import_har(sport_key):
    """
    Accept a HAR file exported from Charles Proxy and populate bet365 odds
    for the given sport without any browser automation.
    """
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded — send as multipart/form-data field 'file'"}), 400

    import tempfile, os
    har_file = request.files["file"]
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".har")
    os.close(tmp_fd)  # close fd before writing — required on Windows
    try:
        har_file.save(tmp_path)
        games = games_from_har(tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if not games:
        return jsonify({"error": "No bet365 odds found in HAR — make sure you browsed the sport page in Charles"}), 400

    now = datetime.now(timezone.utc).isoformat()
    with _b365_lock:
        _b365_caches[sport_key] = {
            "data":         games,
            "last_updated": now,
            "next_refresh": None,
            "status":       "ready",
            "error":        None,
        }

    return jsonify({"status": "ready", "games": len(games), "last_updated": now})


@app.route("/api/bookmaker/import-har/<sport_key>", methods=["POST"])
def bookmaker_import_har(sport_key):
    """Accept a HAR file from bookmaker.eu and populate bookmaker odds."""
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    import tempfile, os
    har_file = request.files["file"]
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".har")
    os.close(tmp_fd)
    try:
        har_file.save(tmp_path)
        games = games_from_bookmaker_har(tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if not games:
        return jsonify({"error": "No bookmaker.eu odds found in HAR — make sure you browsed the sport page"}), 400

    now = datetime.now(timezone.utc).isoformat()
    with _bkmkr_lock:
        _bkmkr_caches[sport_key] = {
            "data":         games,
            "last_updated": now,
            "status":       "ready",
        }

    return jsonify({"status": "ready", "games": len(games), "last_updated": now})


@app.route("/api/bet365/<sport_key>")
def get_bet365(sport_key):
    """
    Returns current bet365 cache for a sport.
    Frontend polls this every few seconds while status == 'loading',
    then merges the data into the displayed table.
    """
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    cache  = b365_get(sport_key)
    b365_games = cache.get("data") or []

    # Inject bet365 into the existing cached rows (which already contain all
    # Odds API data — full-game + per-event partial markets from fetch_odds).
    # Re-generating from raw_games would lose the per-event partial data.
    with _lock:
        existing_rows = list(_caches[sport_key].get("data") or [])

    _REFERENCE_BOOKS = {"betonlineag"}

    with _bkmkr_lock:
        bkmkr_games = list((_bkmkr_caches.get(sport_key) or {}).get("data") or [])

    if (b365_games or bkmkr_games) and existing_rows:
        for row in existing_rows:
            mk   = row.get("market", "")
            away = row.get("away_team", "")
            home = row.get("home_team", "")

            # ── bet365 injection ─────────────────────────────────────────────
            b365_match = _match_teams(away, home, b365_games) if b365_games else None
            b365_entry = None
            if b365_match:
                raw_mkt = (b365_match.get("markets") or {}).get(mk)
                if raw_mkt:
                    b365_entry = {
                        "home_odds":  raw_mkt.get("home_odds"),
                        "away_odds":  raw_mkt.get("away_odds"),
                        "home_point": raw_mkt.get("home_point"),
                        "away_point": raw_mkt.get("away_point"),
                    }
            row["books"]["bet365"] = b365_entry

            # ── bookmaker injection ──────────────────────────────────────────
            bkmkr_match = _match_teams(away, home, bkmkr_games) if bkmkr_games else None
            bkmkr_entry = None
            if bkmkr_match:
                raw_mkt = (bkmkr_match.get("markets") or {}).get(mk)
                if raw_mkt:
                    bkmkr_entry = {
                        "home_odds":  raw_mkt.get("home_odds"),
                        "away_odds":  raw_mkt.get("away_odds"),
                        "home_point": raw_mkt.get("home_point"),
                        "away_point": raw_mkt.get("away_point"),
                    }
            row["books"]["bookmaker"] = bkmkr_entry

            best_books = {bk: v for bk, v in row["books"].items() if bk not in _REFERENCE_BOOKS}
            row["best_home"], row["best_away"] = find_best(best_books, mk)

        merged = existing_rows
        with _lock:
            _caches[sport_key]["data"] = merged
    else:
        merged = existing_rows

    return jsonify({
        "sport_key":    sport_key,
        "status":       cache.get("status", "idle"),
        "last_updated": cache.get("last_updated"),
        "next_refresh": cache.get("next_refresh"),
        "error":        cache.get("error"),
        "data":         merged,
        "game_count":   len(b365_games),
    })


@app.route("/api/debug/unmatched/<sport_key>")
def get_unmatched(sport_key):
    """Lists all Odds API games that failed to match a bet365 game, and the
    closest bet365 candidate — useful for building the alias table."""
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    b365_games = b365_get(sport_key).get("data") or []
    with _lock:
        raw_games = list(_caches[sport_key].get("raw_games") or [])

    if not raw_games:
        return jsonify({"error": "No Odds API data cached — load the sport first"}), 400
    if not b365_games:
        return jsonify({"error": "No bet365 data cached — wait for bet365 to load"}), 400

    def overlap(s1, s2):
        t1, t2 = set(s1.split()), set(s2.split())
        return len(t1 & t2) / max(len(t1 | t2), 1)

    unmatched = []
    for game in raw_games:
        api_away = game.get("away_team", "")
        api_home = game.get("home_team", "")
        api_a = _normalize(api_away)
        api_h = _normalize(api_home)

        best_score, best_game = 0, None
        for g in b365_games:
            s = overlap(api_a, _normalize(g["away_team"])) + overlap(api_h, _normalize(g["home_team"]))
            if s > best_score:
                best_score, best_game = s, g

        if best_score < 0.6:
            unmatched.append({
                "odds_api":         f"{api_away} @ {api_home}",
                "odds_api_norm":    f"{api_a} @ {api_h}",
                "best_b365":        f"{best_game['away_team']} @ {best_game['home_team']}" if best_game else None,
                "best_b365_norm":   f"{_normalize(best_game['away_team'])} @ {_normalize(best_game['home_team'])}" if best_game else None,
                "score":            round(best_score, 3),
            })

    return jsonify({
        "sport_key":       sport_key,
        "total_api_games": len(raw_games),
        "total_b365_games":len(b365_games),
        "unmatched_count": len(unmatched),
        "unmatched":       unmatched,
    })


@app.route("/api/debug/bet365/<sport_key>")
def debug_bet365_markets(sport_key):
    """
    Diagnostic endpoint — shows what markets each cached bet365 game has.
    Useful for verifying whether coupon fetches are populating 1H/1Q data.

    Call: GET /api/debug/bet365/basketball_nba
    """
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    b365_data = b365_get(sport_key)
    games = b365_data.get("data") or []

    summary = []
    for g in games:
        mkts = sorted((g.get("markets") or {}).keys())
        summary.append({
            "game":      f"{g.get('away_team')} @ {g.get('home_team')}",
            "_fi":       g.get("_fi"),
            "time":      g.get("commence_time"),
            "markets":   mkts,
            "has_h1":    any(k.endswith("_h1") for k in mkts),
            "has_q1":    any(k.endswith("_q1") for k in mkts),
            "has_p1":    any(k.endswith("_p1") for k in mkts),
            "has_f5":    any("1st_5" in k for k in mkts),
        })

    return jsonify({
        "sport_key":    sport_key,
        "status":       b365_data.get("status", "idle"),
        "last_updated": b365_data.get("last_updated"),
        "game_count":   len(games),
        "games":        summary,
    })


@app.route("/api/cache/<sport_key>")
def get_cache(sport_key):
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    with _lock:
        c = _caches[sport_key]
        return jsonify({
            "sport_key":          sport_key,
            "data":               c["data"],
            "last_updated":       c["last_updated"],
            "remaining_requests": c["remaining_requests"],
            "used_requests":      c["used_requests"],
            "error":              c["error"],
            "bookmakers":         DISPLAY_BOOKS,
            "bookmaker_display":  BOOKMAKER_DISPLAY,
        })


if __name__ == "__main__":
    print("🏀  Odds Screen  →  http://localhost:5000")
    print(f"    bet365 scraper: {'enabled' if BET365_AVAILABLE else 'not found'}")
    app.run(debug=True, port=5000)
