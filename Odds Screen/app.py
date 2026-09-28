"""
Betting Odds Screen - Backend
Multi-sport, multi-source: The Odds API + bet365 scraper.
"""

import os
import re
import json
import subprocess
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, send_from_directory, request
import requests

import random

import history_tracker
import novig_filter
import prop_logger

# bet365 scraper (optional — gracefully absent if scrapers/ not present)
try:
    from scrapers.bet365 import fetch_bet365
    from scrapers.parse_har import games_from_har, props_from_b365_har
    from scrapers.parse_bookmaker_har import games_from_bookmaker_har, props_from_bookmaker_har
    BET365_AVAILABLE = True
except ImportError:
    BET365_AVAILABLE = False

# Live bookmaker.eu collector — separate try block so a missing/broken
# collector can't take the HAR import paths down with it.
try:
    from scrapers.kalshi import fetch_kalshi
    KALSHI_AVAILABLE = True
except ImportError:
    KALSHI_AVAILABLE = False
    def fetch_kalshi(sport_key):
        return []

try:
    from scrapers.polymarket import fetch_polymarket
    POLYMARKET_AVAILABLE = True
except ImportError:
    POLYMARKET_AVAILABLE = False
    def fetch_polymarket(sport_key):
        return []

try:
    from scrapers.bookmaker_live import (fetch_bookmaker, SPORT_PAGES as BKMKR_PAGES,
                                         last_login_state as bkmkr_login_state,
                                         open_login_window as bkmkr_open_login_window,
                                         request_login_window_focus as bkmkr_focus_login_window)
    BKMKR_LIVE_AVAILABLE = True
except ImportError:
    BKMKR_LIVE_AVAILABLE = False
    BKMKR_PAGES = {}
    def fetch_bookmaker(sport_key, **kw):
        return None
    def bkmkr_login_state(sport_key):
        return "unknown"
    def bkmkr_open_login_window():
        return None
    def bkmkr_focus_login_window():
        return None

if not BET365_AVAILABLE:
    def fetch_bet365(sport_key, **kw):
        return None
    def games_from_har(path):
        return []
    def games_from_bookmaker_har(path, sport_key=""):
        return []
    def props_from_bookmaker_har(path, sport_key=""):
        return {}
    def props_from_b365_har(path):
        return {}

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

# ── Bookmaker.eu live collection ──────────────────────────────────────────────
#   manual    — "BK NOW": one pull, any time, for the sport on screen
#   scheduled — hourly, on the hour ± 5 min, ONLY inside these windows (ET):
#               weekdays 6-8 AM and 4-10 PM, weekends 9 AM-10 PM (both ends
#               included: a weekday gets pulls at 6/7/8 and 4-10 PM). Outside
#               them no browser opens at all. "BK AUTO" turns the schedule
#               on/off (saved across restarts in data/bookmaker_schedule.json).
# Every pull (either kind) also re-prices the board's full-game rows, so sharp
# and rec prices come from the same hour. bet365 is deliberately NOT scheduled:
# it is the book actually being bet into, so every visit to it is
# user-initiated (see /api/bet365/start-scrape). Bookmaker is reference-only
# sharp odds, which is what makes unattended pulls worth the exposure there.
BKMKR_SCHEDULE_SPORTS = ["americanfootball_nfl", "americanfootball_ncaaf"]
# In season (~late Oct): "basketball_nba", "icehockey_nhl" (pages already in
# SPORT_PAGES) and "basketball_ncaab" (its bookmaker.eu page slug is unconfirmed).
BKMKR_WINDOWS = {"weekday": [(6, 8), (16, 22)], "weekend": [(9, 22)]}   # ET hours, inclusive
BKMKR_JITTER_MIN = 5
_ET = ZoneInfo("America/New_York")
_BKMKR_SCHEDULE_FILE = Path(__file__).resolve().parent / "data" / "bookmaker_schedule.json"
_bkmkr_schedule = {"enabled": True, "next_run": None}


def _bkmkr_load_schedule_flag():
    try:
        _bkmkr_schedule["enabled"] = bool(json.loads(_BKMKR_SCHEDULE_FILE.read_text()).get("enabled", True))
    except (FileNotFoundError, ValueError):
        pass


def _bkmkr_save_schedule_flag():
    _BKMKR_SCHEDULE_FILE.parent.mkdir(exist_ok=True)
    _BKMKR_SCHEDULE_FILE.write_text(json.dumps({"enabled": _bkmkr_schedule["enabled"]}))


def _bkmkr_slot_allowed(slot_et: datetime) -> bool:
    windows = BKMKR_WINDOWS["weekend" if slot_et.weekday() >= 5 else "weekday"]
    return any(lo <= slot_et.hour <= hi for lo, hi in windows)


def _bkmkr_next_run(now_utc: datetime, after_slot: datetime = None) -> tuple:
    """(slot, fire) as UTC datetimes: the next allowed top-of-hour slot after
    `after_slot` (the one just handled — so an early run at 5:57 can't repeat
    the 6:00 slot) and its run time, slot ± BKMKR_JITTER_MIN. A slot whose
    jittered time already passed runs right away while still inside its window."""
    now_et = now_utc.astimezone(_ET)
    top = now_et.replace(minute=0, second=0, microsecond=0)
    for h in range(-1, 72):
        slot = (top + timedelta(hours=h)).astimezone(_ET)   # DST-safe wall clock
        if after_slot is not None and slot <= after_slot:
            continue
        if not _bkmkr_slot_allowed(slot):
            continue
        fire = slot + timedelta(minutes=random.uniform(-BKMKR_JITTER_MIN, BKMKR_JITTER_MIN))
        soon = now_et + timedelta(seconds=30)
        if fire < soon:
            if now_et >= slot + timedelta(minutes=BKMKR_JITTER_MIN):
                continue   # window for this slot is over
            fire = soon
        return slot.astimezone(timezone.utc), fire.astimezone(timezone.utc)
    return None, None


def _keep_system_awake():
    """Stop Windows from sleeping while the app runs, so the 6 AM / 9 AM pulls
    happen with nobody at the machine. The display can still turn off and the
    screen can be locked — only system sleep is blocked. Held by the calling
    (scheduler) thread for its lifetime."""
    if os.name != "nt":
        return
    try:
        import ctypes
        ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
        ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
    except Exception as e:
        print(f"[bookmaker] could not request keep-awake: {e}")


def _bkmkr_schedule_loop():
    """Runs forever: sleep until the next allowed slot, then pull every
    scheduled sport one after another (one browser profile, one at a time)."""
    _keep_system_awake()
    print(f"[bookmaker] schedule loop started for {BKMKR_SCHEDULE_SPORTS} "
          f"(enabled={_bkmkr_schedule['enabled']})")
    last_slot = None
    while True:
        slot, fire = _bkmkr_next_run(datetime.now(timezone.utc), last_slot)
        if fire is None:
            time.sleep(3600)
            continue
        last_slot = slot
        _bkmkr_schedule["next_run"] = fire.isoformat()
        # Short sleeps so a PC that was asleep or a toggled schedule is noticed.
        while datetime.now(timezone.utc) < fire:
            time.sleep(min(60.0, max(1.0, (fire - datetime.now(timezone.utc)).total_seconds())))
        late = (datetime.now(timezone.utc) - fire).total_seconds()
        if not _bkmkr_schedule["enabled"]:
            continue
        if late > 1800:   # machine was asleep through the slot — skip it
            print(f"[bookmaker] skipped the {slot.astimezone(_ET):%H:%M} ET slot ({late/60:.0f} min late)")
            continue
        for sk in BKMKR_SCHEDULE_SPORTS:
            if sk not in BKMKR_PAGES:
                continue
            with _bkmkr_lock:
                busy = (_bkmkr_caches.get(sk) or {}).get("status") == "loading"
                if not busy:
                    cache = dict(_bkmkr_caches.get(sk) or {})
                    cache.update(status="loading", error=None)
                    _bkmkr_caches[sk] = cache
            if not busy:
                _bkmkr_collect(sk)


def _bkmkr_set(sport_key: str, **fields):
    with _bkmkr_lock:
        cache = dict(_bkmkr_caches.get(sport_key) or {})
        cache.update(fields)
        _bkmkr_caches[sport_key] = cache


def _bkmkr_collect(sport_key: str):
    """One live collection run, then re-price the board's full-game rows."""
    games = None
    try:
        games = fetch_bookmaker(sport_key) if BKMKR_LIVE_AVAILABLE else None
    except Exception as e:
        print(f"[bookmaker] {sport_key} — collection raised: {type(e).__name__}: {e}")

    if games:
        _bkmkr_set(sport_key,
                   data=games,
                   last_updated=datetime.now(timezone.utc).isoformat(),
                   status="ready",
                   error=None)
        print(f"[bookmaker] {sport_key} — {len(games)} games collected")
        try:
            _refresh_board_full_game(sport_key)
        except Exception as e:
            print(f"[bookmaker] {sport_key} — board refresh failed: {type(e).__name__}: {e}")
    else:
        # Keep whatever we had; a failed poll should never blank the board.
        logged_out = BKMKR_LIVE_AVAILABLE and bkmkr_login_state(sport_key) == "logged_out"
        _bkmkr_set(sport_key,
                   status="ready" if (_bkmkr_caches.get(sport_key) or {}).get("data") else "error",
                   error=("Logged out — click BK LOGIN" if logged_out
                          else "Collection failed — check login (BK LOGIN)"))
        print(f"[bookmaker] {sport_key} — collection returned nothing"
              f"{' (logged out)' if logged_out else ''}")


def bkmkr_trigger(sport_key: str) -> bool:
    """Kick off a manual collection. Returns False if one is already running."""
    with _bkmkr_lock:
        if (_bkmkr_caches.get(sport_key) or {}).get("status") == "loading":
            return False
        cache = dict(_bkmkr_caches.get(sport_key) or {})
        cache.update(status="loading", error=None)
        _bkmkr_caches[sport_key] = cache

    t = threading.Thread(target=_bkmkr_collect, args=[sport_key], daemon=True)
    t.start()
    return True

_circa_caches: dict = {}      # sport_key -> {data, last_updated, status}
_circa_lock = threading.Lock()

_bkmkr_props_caches: dict = {}  # sport_key -> {market_key -> {player_name -> {over_odds, under_odds, line}}}
_bkmkr_props_lock = threading.Lock()

_b365_props_caches: dict = {}  # sport_key -> {market_key -> {player_name -> {over_odds, under_odds, line}}}
_b365_props_lock = threading.Lock()


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


def circa_get(sport_key: str) -> dict:
    with _circa_lock:
        return dict(_circa_caches.get(sport_key) or {
            "data": [], "last_updated": None, "status": "idle", "error": None, "source_file": None,
        })


# ── Circa "Load Latest Recording" button ────────────────────────────────────
# On-demand alternative to a continuous background watcher (circa_watcher.py
# still exists and works the same way, this doesn't replace it). Deliberately
# NOT a long-running polling process: the actual OCR parser (parse_circa_
# recording.py) has proven completely reliable every time it's invoked fresh/
# directly, but a continuous background watcher process repeatedly appeared
# to hang in testing — root cause never conclusively pinned down, possibly
# specific to how that process was being launched/backgrounded during
# testing rather than a real bug in the watcher itself. A button sidesteps
# the fragile long-running-process part entirely while reusing the exact
# same subprocess-isolated, timeout-protected parse call circa_watcher.py
# uses — see memory: odds_screen_recording_ingestion for the full history.
_CIRCA_DIR          = Path(__file__).parent
_CIRCA_WATCH_DIR    = Path(r"C:\Users\ajput\iCloudDrive\OddsRecordings")
_CIRCA_VENV_PYTHON  = _CIRCA_DIR / ".venv-ocr" / "Scripts" / "python.exe"
_CIRCA_PARSE_SCRIPT = _CIRCA_DIR / "scrapers" / "parse_circa_recording.py"
_CIRCA_VIDEO_EXTS   = {".mov", ".mp4"}
_CIRCA_SUBPROCESS_TIMEOUT = 300  # seconds — matches circa_watcher.py's ceiling


def _circa_find_latest_video() -> Path | None:
    if not _CIRCA_WATCH_DIR.is_dir():
        return None
    candidates = [f for f in _CIRCA_WATCH_DIR.iterdir()
                  if f.is_file() and f.suffix.lower() in _CIRCA_VIDEO_EXTS]
    if not candidates:
        return None
    return max(candidates, key=lambda f: f.stat().st_mtime)


def _circa_set_cache(sport_key: str, **updates):
    with _circa_lock:
        cur = dict(_circa_caches.get(sport_key) or {"data": [], "last_updated": None, "error": None, "source_file": None})
        cur.update(updates)
        _circa_caches[sport_key] = cur


def _circa_load_latest(sport_key: str):
    """Runs in a background thread — finds the newest recording, parses it
    in an isolated subprocess (hard timeout, same as circa_watcher.py), and
    updates the cache. Never raises into the calling thread."""
    try:
        video = _circa_find_latest_video()
        if not video:
            _circa_set_cache(sport_key, status="error", error=f"No recording found in {_CIRCA_WATCH_DIR}")
            return

        try:
            result = subprocess.run(
                [str(_CIRCA_VENV_PYTHON), str(_CIRCA_PARSE_SCRIPT), "--json", str(video)],
                capture_output=True, text=True, timeout=_CIRCA_SUBPROCESS_TIMEOUT,
            )
            games = json.loads(result.stdout) if result.returncode == 0 else None
            err = None if games is not None else (result.stderr[-500:] or f"exit code {result.returncode}")
        except subprocess.TimeoutExpired:
            games, err = None, f"Parsing exceeded {_CIRCA_SUBPROCESS_TIMEOUT}s and was stopped"
        except json.JSONDecodeError as e:
            games, err = None, f"Bad output from parser: {e}"

        if games is not None:
            # The recording's own timestamp, not parse time — prices are as old as the video.
            recorded = datetime.fromtimestamp(video.stat().st_mtime, tz=timezone.utc).isoformat()
            _circa_set_cache(sport_key, data=games, last_updated=recorded,
                              status="ready", error=None, source_file=video.name)
        else:
            _circa_set_cache(sport_key, status="error", error=err)
    except Exception as e:
        _circa_set_cache(sport_key, status="error", error=repr(e))


app = Flask(__name__, static_folder="static")
app.json.compact = True   # debug mode would otherwise pretty-print every response (~38% larger boards)

# ── Configuration ─────────────────────────────────────────────────────────────
API_KEY  = "28f45f78ba5db46eb4be2c986bf5f912"
#API_KEY  = "82ad3d819597cf9b56b60fb682f2df87"
#API_KEY  = "6564dbf4768c34046a96a283c099e47f"
BASE_URL = "https://api.the-odds-api.com/v4"

# Sports where API calls are restricted to today's games only (ET date), never tomorrow
DATE_FILTER_SPORTS = {'baseball_mlb', 'icehockey_nhl', 'basketball_nba', 'basketball_ncaab', 'basketball_wnba'}
# NFL/NCAAF play ~once per team per week, so the default 48h rolling window
# (fine for sports with games most days) is too restrictive — it can miss a
# team's only upcoming game entirely. Widened to 144h (6 days) for these two.
EXTENDED_WINDOW_SPORTS = {'americanfootball_nfl', 'americanfootball_ncaaf'}
_ET_TZ = ZoneInfo('America/New_York')

def _get_today_et_end_utc() -> datetime:
    """Return the UTC datetime for midnight tonight ET (i.e. start of tomorrow ET).
    Using this as commence_to prevents pulling tomorrow's games for daily sports."""
    now_et       = datetime.now(_ET_TZ)
    tomorrow_et  = now_et.date() + timedelta(days=1)
    midnight_et  = datetime(tomorrow_et.year, tomorrow_et.month, tomorrow_et.day,
                            0, 0, 0, tzinfo=_ET_TZ)
    return midnight_et.astimezone(timezone.utc)

DEFAULT_MARKETS = ["h2h", "spreads", "totals"]

# Sport-specific markets to fetch (full game + partials)
SPORT_MARKETS = {
    "basketball_nba":         ["h2h", "spreads", "totals", "h2h_h1", "spreads_h1", "totals_h1", "h2h_q1", "spreads_q1", "totals_q1"],
    "basketball_wnba":        ["h2h", "spreads", "totals", "h2h_h1", "spreads_h1", "totals_h1", "h2h_q1", "spreads_q1", "totals_q1"],
    "americanfootball_nfl":   ["h2h", "spreads", "totals", "h2h_h1", "spreads_h1", "totals_h1", "h2h_q1", "spreads_q1", "totals_q1"],
    "americanfootball_ncaaf": ["h2h", "spreads", "totals", "h2h_h1", "spreads_h1", "totals_h1", "h2h_q1", "spreads_q1", "totals_q1"],
    "basketball_ncaab":       ["h2h", "spreads", "totals", "h2h_h1", "spreads_h1", "totals_h1"],
    "icehockey_nhl":          ["h2h", "spreads", "totals", "h2h_p1", "spreads_p1", "totals_p1"],
    "baseball_mlb":           ["h2h", "spreads", "totals", "h2h_1st_5_innings", "spreads_1st_5_innings", "totals_1st_5_innings", "totals_1st_1_innings"],
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
    "basketball_wnba": {
        "label": "WNBA", "sublabel": "Women's Pro Basketball", "emoji": "🏀",
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
# kalshi, polymarket: own public-API feeds (scrapers/kalshi.py, polymarket.py)
#                     with the taker fee built into every price — never requested
#                     from the Odds API, whose raw exchange quotes leave the fee out.
#
# All known book keys (comment/uncomment to enable or disable):
BOOKMAKERS = [
    "novig",
    "draftkings",
    "fanduel",
    "williamhill_us",
    "hardrockbet",
    "fanatics",
    "espnbet",
    "betmgm",
    "betrivers",
    "betonlineag",
]

# Display name for every book key (keep all entries here even if a book is disabled)
BOOKMAKER_DISPLAY = {
    "novig":          "NoVig",
    "kalshi":         "Kalshi",         # own feed, net of taker fee — never via the Odds API
    "polymarket":     "Polymarket",     # Polymarket US own feed, net of taker fee — never via the Odds API
    "draftkings":     "DraftKings",
    "fanduel":        "FanDuel",
    "williamhill_us": "Caesars",
    "bet365":         "bet365",       # scraped — HAR import only
    "bookmaker":      "Bookmaker",    # scraped — HAR import only
    "circa":          "Circa",        # scraped — screen-recording OCR import only
    "hardrockbet":    "Hard Rock",
    "fanatics":       "Fanatics",
    "espnbet":        "theScore",
    "betmgm":         "BetMGM",
    "betrivers":      "BetRivers",
    "betonlineag":    "BetOnline",
}
history_tracker.set_book_display(BOOKMAKER_DISPLAY)

# ── Hourly line-history capture (see history_tracker.py) ──────────────────────
# NFL is tracked alongside NCAAF because the prop backtest needs the game
# total/spread as of the moment each prop was quoted — that's the anchor for
# any prop model's game-environment layer.
HISTORY_TRACKED_SPORTS = ["americanfootball_ncaaf", "americanfootball_nfl"]

# ── Player-prop line history capture (see prop_logger.py) ─────────────────────
PROP_HISTORY_TRACKED_SPORTS = ["americanfootball_nfl"]
PROP_CAPTURE_TICK_SECONDS = 600

# Column order in the UI — remove a key here to hide its column
DISPLAY_BOOKS = [
    "circa",          # screen-recording-OCR-scraped sharp reference (highest priority)
    "bookmaker",      # HAR-scraped sharp reference
    "novig",
    "kalshi",         # own public feed (scrapers/kalshi.py) — prices net of taker fee
    "polymarket",     # own public feed (scrapers/polymarket.py) — prices net of taker fee
    "draftkings",
    "fanduel",
    "williamhill_us",
    "bet365",         # HAR-scraped
    "hardrockbet",
    "fanatics",
    "espnbet",
    "betmgm",
    "betrivers",
]

# ── Player props ───────────────────────────────────────────────────────────────
# Prop markets per sport. Add/remove market keys here to enable/disable.
PROP_MARKETS = {
    "baseball_mlb":   [
        "pitcher_strikeouts",
        "pitcher_hits_allowed",
        "batter_total_bases",
        "batter_hits_runs_rbis",
    ],
    "basketball_nba": ["player_points", "player_rebounds", "player_assists"],
    "americanfootball_nfl": [
        "player_pass_yds",
        "player_pass_attempts",
        "player_pass_completions",
        "player_rush_yds",
        "player_rush_attempts",
        "player_reception_yds",
        "player_receptions",
        "player_solo_tackles",
        "player_tackles_assists",
    ],
}

PROP_MARKET_DISPLAY = {
    "pitcher_strikeouts":     "Pitcher K",
    "pitcher_hits_allowed":   "Pitcher HA",
    "batter_total_bases":     "Total Bases",
    "batter_hits_runs_rbis":  "H+R+RBI",
    "player_points":          "Player Pts",
    "player_rebounds":        "Player Reb",
    "player_assists":         "Player Ast",
    "player_pass_yds":        "Pass Yds",
    "player_pass_attempts":   "Pass Att",
    "player_pass_completions":"Completions",
    "player_rush_yds":        "Rush Yds",
    "player_rush_attempts":   "Rushes",
    "player_reception_yds":   "Rec Yds",
    "player_receptions":      "Receptions",
    "player_solo_tackles":    "Tackles",
    "player_tackles_assists": "Tkl+Ast",
}

# All books for props — includes books disabled for game lines (regional
# restrictions). NoVig excluded here (kept for main game lines, where it
# hasn't shown this problem) — its prop lines repeatedly turned out wildly
# wrong across unrelated players/markets (Kenneth Walker III rush yards at
# 109.5 vs. everyone else's 59.5-62.5; Malik Willis pass yards at 274.5 vs.
# 179.5-184.5; Noah Gray AND Jack Bech receptions both at exactly double
# every other book's unanimous 1.5; Malik Davis rush yards at 24.5 vs. a
# real 13.5-18.5 spread) — a real, systemic pattern, not one-off noise, and
# simpler to exclude at the source than to keep tuning outlier-detection
# statistics around one recurring bad source.
ALL_PROP_BOOKMAKERS = [
    "draftkings", "fanduel", "williamhill_us", "bet365",
    "hardrockbet", "fanatics", "espnbet", "betmgm", "betrivers", "betonlineag",
]

_props_caches: dict = {}
_props_lock = threading.Lock()

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

def _implied_from_decimal(decimal_odds):
    """Exact implied probability of a decimal price — NoVig quotes in probability,
    and rounding to American odds first can move it 0.1pp."""
    try:
        d = float(decimal_odds)
    except (TypeError, ValueError):
        return None
    return 1 / d if d > 1.0 else None


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
    "ut martin skyhawks":        "tennessee martin",
    "ut martin":                 "tennessee martin",
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


@lru_cache(maxsize=8192)
def _normalize(name: str) -> str:
    """Lowercase, strip punctuation/articles for fuzzy matching."""
    name = name.lower().strip()
    # Normalize accented characters (e.g. José -> jose)
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^a-z0-9 ]", "", name)
    for prefix in ("the ", "university of ", "university "):
        if name.startswith(prefix):
            name = name[len(prefix):]
    name = name.strip()
    return _ALIASES.get(name, name)


# Words too generic to count as a real match signal on their own — a LOT of
# college teams share these, so overlapping on just one of them (e.g. two
# unrelated "___ State" schools) looks like a real match to plain token
# overlap even though it isn't. Stripped before scoring; see _match_teams.
_MATCH_STOPWORDS = {"state", "st", "university", "college"}


def _swap_team_fields(game: dict) -> dict:
    """
    Return a copy of a bet365/circa/bookmaker game dict with away/home
    reversed — used when a source's own away/home labeling for a matchup is
    the opposite of the Odds API's (neutral-site games especially cause
    this). Every downstream consumer trusts that a game's away_point/
    away_odds describe the SAME team as the Odds API's own away_team, so
    finding the right game isn't enough on its own — its fields have to
    actually agree with that convention too, or real data ends up correctly
    located but attached to the wrong team.

    Only touches h2h/spreads-shaped entries (away/home genuinely means a
    specific team there) — totals entries reuse the same away/home key names
    for Under/Over, which have nothing to do with which team is home or
    away, so those are left untouched on purpose.
    """
    g = dict(game)
    g["away_team"], g["home_team"] = game.get("home_team"), game.get("away_team")

    def swap_side_fields(d: dict) -> dict:
        d = dict(d)
        d["away_point"], d["home_point"] = d.get("home_point"), d.get("away_point")
        d["away_odds"],  d["home_odds"]  = d.get("home_odds"),  d.get("away_odds")
        if "away_prob" in d or "home_prob" in d:   # exchange feeds' raw prices travel with their odds
            d["away_prob"], d["home_prob"] = d.get("home_prob"), d.get("away_prob")
        return d

    markets = game.get("markets")
    if isinstance(markets, dict):
        g["markets"] = {
            mk: (swap_side_fields(mv) if (mk == "h2h" or mk.startswith("h2h_") or
                                           mk == "spreads" or mk.startswith("spreads_"))
                 and isinstance(mv, dict) else mv)
            for mk, mv in markets.items()
        }

    alt_lines = game.get("alt_lines")
    if isinstance(alt_lines, dict):
        g["alt_lines"] = {
            ak: ([swap_side_fields(e) for e in av]
                 if (ak == "spreads" or ak.startswith("spreads_")) and isinstance(av, list) else av)
            for ak, av in alt_lines.items()
        }

    return g


def _match_teams(api_away: str, api_home: str,
                 b365_games: list) -> dict | None:
    """
    Find the best-matching bet365/bookmaker game for an Odds API game.
    Returns the game dict or None if no confident match.

    Caught a real false positive in production: "Grambling State Tigers" @
    "TCU Horned Frogs" matched to the completely unrelated "Ohio State @
    Texas" — "grambling state" vs "ohio state" shared only the generic word
    "state", and the "tcu" -> "texas christian" alias shared "texas" with
    Texas's own name, and those two weak, generic overlaps summed just over
    the combined threshold. Fixed two ways: strip generic words like "state"
    before scoring, and require BOTH sides to independently show real
    similarity — a strong score on one side can no longer compensate for a
    near-zero score on the other.
    """
    api_a = _normalize(api_away)
    api_h = _normalize(api_home)

    def overlap(s1, s2):
        t1 = set(s1.split()) - _MATCH_STOPWORDS
        t2 = set(s2.split()) - _MATCH_STOPWORDS
        if not t1 or not t2:
            t1, t2 = set(s1.split()), set(s2.split())   # name is ~entirely stopwords — don't strip to nothing
        return len(t1 & t2) / max(len(t1 | t2), 1)

    best_score   = 0
    best_game    = None
    best_swapped = False

    for g in b365_games:
        b_a = _normalize(g["away_team"])
        b_h = _normalize(g["home_team"])

        # Try both orientations — some sources disagree on which team is
        # "home" for a given matchup (neutral-site games especially), and
        # without this a real match can score 0/0 on the correct-but-swapped
        # pairing while never getting a chance at the reversed one. Caught on
        # a real case: the Odds API had Kansas @ Arizona State while
        # Bookmaker.eu listed the same game as Arizona State @ Kansas — the
        # straight pairing scored 0.0/0.0 (complete non-match) even though
        # the swapped pairing scores 0.5/0.33, comfortably real.
        straight = (overlap(api_a, b_a), overlap(api_h, b_h))
        swapped  = (overlap(api_a, b_h), overlap(api_h, b_a))
        is_swapped = sum(swapped) > sum(straight)
        a_score, h_score = swapped if is_swapped else straight
        if a_score < 0.3 or h_score < 0.3:
            continue   # each side must independently look like a real match

        score = a_score + h_score
        if score > best_score:
            best_score   = score
            best_game    = g
            best_swapped = is_swapped

    # Require at least 0.5 overlap on each side combined
    if best_score < 0.6:
        return None
    # Reorient the matched game's own fields so away/home genuinely agree
    # with the Odds API's convention — finding the right game isn't enough
    # if its data is then read as if the source's own away/home matched.
    return _swap_team_fields(best_game) if best_swapped else best_game


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


def _commence_window(sport_key: str) -> tuple[str, str]:
    """(commenceTimeFrom, commenceTimeTo) for a sport's odds/events requests."""
    now_utc = datetime.now(timezone.utc)
    # For MLB/NHL/NBA/NCAAB: only fetch today's games (ET midnight cutoff) to save API credits
    if sport_key in DATE_FILTER_SPORTS:
        end = _get_today_et_end_utc()
    elif sport_key in EXTENDED_WINDOW_SPORTS:
        end = now_utc + timedelta(hours=144)
    else:
        end = now_utc + timedelta(hours=48)
    return now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_full_game_raw(sport_key: str, markets: list) -> tuple[list, str | None, str | None]:
    """
    One bulk call covering every game's full-game markets. Billed at
    markets x regions (3 credits for h2h/spreads/totals) regardless of how
    many games come back — unlike the per-event endpoint, which bills per game.
    Returns (raw_games, remaining, used); raises requests.RequestException.
    """
    commence_from, commence_to = _commence_window(sport_key)
    resp = requests.get(
        f"{BASE_URL}/sports/{sport_key}/odds",
        params={
            "apiKey":           API_KEY,
            "regions":          "us",
            "markets":          ",".join(markets),
            "oddsFormat":       "decimal",
            "bookmakers":       ",".join(BOOKMAKERS),
            "dateFormat":       "iso",
            "commenceTimeFrom": commence_from,
            "commenceTimeTo":   commence_to,
        },
        timeout=15,
    )
    if not resp.ok:
        print(f"[fetch_odds] {sport_key} — full-game HTTP {resp.status_code} body: {resp.text}")
    resp.raise_for_status()
    return resp.json(), resp.headers.get("x-requests-remaining"), resp.headers.get("x-requests-used")


EVENT_FETCH_WORKERS = 5


def _fetch_event_markets(sport_key: str, event_id: str, markets: list) -> tuple[dict, str | None, str | None]:
    """One per-event odds call (period markets aren't on the bulk endpoint).
    Retries once on HTTP 429 — requests run concurrently, see EVENT_FETCH_WORKERS."""
    params = {
        "apiKey":     API_KEY,
        "regions":    "us",
        "markets":    ",".join(markets),
        "oddsFormat": "decimal",
        "bookmakers": ",".join(BOOKMAKERS),
        "dateFormat": "iso",
    }
    url = f"{BASE_URL}/sports/{sport_key}/events/{event_id}/odds"
    resp = requests.get(url, params=params, timeout=15)
    if resp.status_code == 429:
        time.sleep(1.5)
        resp = requests.get(url, params=params, timeout=15)
    if not resp.ok:
        print(f"[fetch_odds] {sport_key} — event {event_id} {markets} HTTP {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return resp.json(), resp.headers.get("x-requests-remaining"), resp.headers.get("x-requests-used")


def fetch_odds(sport_key: str):
    if sport_key not in SPORTS:
        return None

    configured_markets = SPORT_MARKETS.get(sport_key, DEFAULT_MARKETS)

    # Split into full-game (bulk endpoint) and partial-game (events endpoint)
    full_markets    = [m for m in configured_markets if m in DEFAULT_MARKETS]
    partial_markets = [m for m in configured_markets if m not in DEFAULT_MARKETS]
    partial_batches = [partial_markets[i:i+3] for i in range(0, len(partial_markets), 3)]

    b365_games  = b365_get(sport_key).get("data") or []
    circa_games = circa_get(sport_key).get("data") or []
    exch_games  = _exchange_games(sport_key)

    all_rows       = []
    api_error      = None
    remaining      = None
    used           = None
    full_game_data = []

    # ── Step 1: Full-game markets via bulk endpoint ───────────────────────────
    print(f"[fetch_odds] {sport_key} — requesting full-game markets: {full_markets}")
    try:
        full_game_data, remaining, used = _fetch_full_game_raw(sport_key, full_markets)
        print(f"[fetch_odds] {sport_key} — full-game returned {len(full_game_data)} games")
        rows = process_games(full_game_data, b365_games, sport_key,
                             markets_override=full_markets, circa_games=circa_games,
                             exchange_games=exch_games)
        print(f"[fetch_odds] {sport_key} — full-game produced {len(rows)} rows")
        all_rows.extend(rows)
    except requests.exceptions.RequestException as e:
        print(f"[fetch_odds] {sport_key} — FAILED full-game: {e}")
        api_error = str(e)

    # ── Step 2: Partial markets via per-event endpoint ────────────────────────
    # /v4/sports/{sport}/events/{eventId}/odds supports h2h_h1, spreads_h1, etc.
    # The bulk /sports/{sport}/odds endpoint does NOT support these markets.
    if partial_batches and full_game_data:
        jobs = [(g["id"], batch) for g in full_game_data if g.get("id") for batch in partial_batches]
        print(f"[fetch_odds] {sport_key} — fetching partial markets "
              f"({partial_markets}) for {len(full_game_data)} events, {len(jobs)} requests")
        with ThreadPoolExecutor(max_workers=EVENT_FETCH_WORKERS) as pool:
            futures = {pool.submit(_fetch_event_markets, sport_key, eid, batch): (eid, batch)
                       for eid, batch in jobs}
            for fut in as_completed(futures):
                event_id, batch = futures[fut]
                try:
                    event_data, rem, u = fut.result()
                except requests.exceptions.RequestException as e:
                    print(f"[fetch_odds] {sport_key} — FAILED event {event_id} {batch}: {e}")
                    continue
                # Responses arrive out of order — the lowest "remaining" is the latest.
                if rem is not None and (remaining is None or int(rem) < int(remaining)):
                    remaining, used = rem, u
                rows = process_games([event_data], b365_games, sport_key,
                                     markets_override=batch, circa_games=circa_games,
                                     exchange_games=exch_games)
                all_rows.extend(rows)

    # Re-sort combined rows from all batches:
    # group by game, then period (full → half/F5/1st-period → quarter/1st-inning),
    # then market type (ML → spread → total) within each period.
    all_rows.sort(key=lambda r: (r["commence_time"] or "", r["away_team"]) + _market_sort_key(r["market"]))

    now_iso = datetime.now(timezone.utc).isoformat()
    with _lock:
        _caches[sport_key]["data"]               = all_rows
        _caches[sport_key]["raw_games"]          = full_game_data
        _caches[sport_key]["last_updated"]       = now_iso
        _caches[sport_key]["full_updated"]       = now_iso
        _caches[sport_key]["periods_updated"]    = now_iso
        _caches[sport_key]["remaining_requests"] = remaining
        _caches[sport_key]["used_requests"]      = used
        _caches[sport_key]["error"]              = api_error

    return all_rows


_last_bulk: dict = {}   # sport_key -> (epoch seconds, raw full-game games) — latest bulk pull


def _refresh_board_full_game(sport_key: str):
    """
    Re-price the board's full-game rows after a Bookmaker pull, so sharp and
    rec prices come from the same hour. Bulk endpoint (3 credits), or the
    top-of-hour history capture's pull when it's under 10 minutes old.
    1H/Q1 rows would cost ~6 credits per game every hour (~440/pull for
    NFL+NCAAF), so they keep their own timestamp (periods_updated) and the page
    only compares them with scraped prices captured near that time. On an
    empty board (nothing loaded since the app started) this builds the
    full-game rows from scratch, so BK NOW alone fills the board; Reload adds
    the 1H/Q1 rows.
    """
    with _lock:
        cached = list(_caches[sport_key].get("data") or [])
    full_markets = [m for m in SPORT_MARKETS.get(sport_key, DEFAULT_MARKETS) if m in DEFAULT_MARKETS]
    last = _last_bulk.get(sport_key)
    if last and time.time() - last[0] < 600:
        raw = last[1]
    else:
        raw, remaining, used = _fetch_full_game_raw(sport_key, DEFAULT_MARKETS)
        _last_bulk[sport_key] = (time.time(), raw)
        if remaining is not None:
            with _lock:
                for c in _caches.values():
                    c["remaining_requests"] = remaining
    rows = process_games(raw, b365_get(sport_key).get("data") or [], sport_key,
                         markets_override=full_markets,
                         circa_games=circa_get(sport_key).get("data") or [],
                         exchange_games=_exchange_games(sport_key))
    live = {r["game_id"] for r in rows}
    # Games that kicked off drop out of the bulk pull — drop their 1H/Q1 rows too.
    merged = rows + [r for r in cached if r["market"] not in full_markets and r["game_id"] in live]
    merged.sort(key=lambda r: (r["commence_time"] or "", r["away_team"]) + _market_sort_key(r["market"]))
    now_iso = datetime.now(timezone.utc).isoformat()
    with _lock:
        _caches[sport_key]["data"]         = merged
        _caches[sport_key]["raw_games"]    = raw
        _caches[sport_key]["last_updated"] = now_iso
        _caches[sport_key]["full_updated"] = now_iso
    print(f"[bookmaker] {sport_key} — board full-game rows refreshed ({len(rows)} rows)")


EXCHANGE_FEED_BOOKS = ("kalshi", "polymarket")   # books priced from their own feeds (net of fees)


def _exchange_games(sport_key: str) -> dict:
    """{book: games} from the exchanges' own feeds (free public APIs)."""
    out = {}
    if KALSHI_AVAILABLE:
        out["kalshi"] = fetch_kalshi(sport_key)
    if POLYMARKET_AVAILABLE:
        out["polymarket"] = fetch_polymarket(sport_key)
    return out


def _near_date(d, ct) -> bool:
    """Exchange game date (ET) within a day of the Odds API kickoff."""
    if d is None or ct is None:
        return True
    return abs((ct.astimezone(ZoneInfo("America/New_York")).date() - d).days) <= 1


def process_games(raw_games: list, b365_games: list, sport_key: str = "",
                  markets_override: list = None, circa_games: list = None,
                  exchange_games: dict = None) -> list:
    circa_games = circa_games or []
    exchange_games = exchange_games or {}
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
                if bk == novig_filter.NOVIG_BOOK and (len(outcomes) != 2 or not novig_filter.pair_ok(
                        *(o.get("price") for o in outcomes.values()))):
                    continue   # empty or stale order book — see novig_filter

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

        # Circa (screen-recording OCR import) — same markets shape as bet365
        circa_match = _match_teams(away_team, home_team, circa_games) if circa_games else None
        if circa_match:
            circa_markets = circa_match.get("markets") or {}
            if circa_markets:
                book_data["circa"] = circa_markets

        # Exchanges with their own feeds: date-checked, then name-matched.
        exch_matches = {}
        for ex_bk, ex_games in exchange_games.items():
            cands = [g for g in ex_games if _near_date(g.get("_date"), ct)]
            m = _match_teams(away_team, home_team, cands) if cands else None
            if m:
                exch_matches[ex_bk] = m

        mkt_display_overrides = SPORT_MARKET_DISPLAY.get(sport_key, {})
        row_markets = markets_override if markets_override is not None else SPORT_MARKETS.get(sport_key, DEFAULT_MARKETS)
        for mk in row_markets:
            books_for_row = {}
            for bk in DISPLAY_BOOKS:
                entry = book_data.get(bk, {})
                if entry is None:
                    books_for_row[bk] = None
                    continue

                if bk in EXCHANGE_FEED_BOOKS:
                    ex = ((exch_matches.get(bk) or {}).get("markets") or {}).get(mk)
                    books_for_row[bk] = dict(ex) if ex else None
                    continue
                # bet365/circa odds are already in American format (strings)
                if bk in ("bet365", "circa"):
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
                        if bk == novig_filter.NOVIG_BOOK:
                            books_for_row[bk]["home_prob"] = _implied_from_decimal(mkt_entry["home_price"])
                            books_for_row[bk]["away_prob"] = _implied_from_decimal(mkt_entry["away_price"])
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
                # Every strike an exchange lists for this market (net-of-fee,
                # main-line shape) — the +EV scan treats each as an alt line.
                "exchange_ladders": {bk: m["alt_lines"][mk] for bk, m in exch_matches.items()
                                     if (m.get("alt_lines") or {}).get(mk)},
            })

    rows.sort(key=lambda r: (r["commence_time"] or "", r["away_team"]))
    return rows


# Never surface a "best available" price worse than this — a book offering an
# extreme alternate point (e.g. an alt spread of -6.5 at -2500) used to always
# win "best" on point alone, since is_better() only falls back to odds as a
# tiebreaker for an identical point. A price this lopsided isn't a bet anyone
# would actually place, so it shouldn't be able to dominate best-available or
# get surfaced as a "+EV" opportunity at all — better to show N/A for a side
# than a price the user would never take.
MIN_BEST_ODDS = -150


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
            if odds_val < MIN_BEST_ODDS:
                continue
            cur_pt, cur_odds, _ = best[side]
            if is_better(point_val, odds_val, cur_pt, cur_odds, side, market):
                best[side] = (point_val, odds_val, bk)

    def fmt(side):
        pt, odds_int, bk = best[side]
        if odds_int is None:
            return None
        out = {
            "odds":     f"+{odds_int}" if odds_int >= 0 else str(odds_int),
            "book":     BOOKMAKER_DISPLAY.get(bk, bk),
            "book_key": bk,
            "point":    pt,
        }
        prob = (books_for_row.get(bk) or {}).get(f"{side}_prob")
        if prob is not None:
            out["prob"] = prob
        return out

    return fmt("home"), fmt("away")


def _prop_implied_prob(odds_str):
    try:
        v = int(str(odds_str).replace("+", ""))
    except (TypeError, ValueError):
        return None
    return (-v) / (-v + 100) if v < 0 else 100 / (v + 100)


def _weighted_median(items: list[tuple[float, float]]):
    """items: (value, weight) pairs. Returns the weight-weighted median value,
    or None if nothing has positive weight."""
    valid = [(v, w) for v, w in items if w and w > 0]
    if not valid:
        return None
    valid.sort(key=lambda t: t[0])
    total_w = sum(w for _, w in valid)
    cum = 0.0
    for v, w in valid:
        cum += w
        if cum >= total_w / 2:
            return v
    return valid[-1][0]


def find_best_prop(book_data: dict, side: str, consensus_line=None) -> dict | None:
    """Best odds for 'over' or 'under' across books (excludes sharp reference books).
    When consensus_line is given, only considers books offering that exact line."""
    key = f"{side}_odds"
    best_val, best_bk = None, None
    for bk, entry in book_data.items():
        if bk in ("betonlineag", "bookmaker") or not entry:
            continue
        if consensus_line is not None and entry.get("line") != consensus_line:
            continue
        odds_str = entry.get(key)
        if odds_str is None:
            continue
        try:
            v = int(str(odds_str).replace("+", ""))
        except ValueError:
            continue
        if best_val is None or v > best_val:
            best_val, best_bk = v, bk
    if best_val is None:
        return None
    return {
        "odds":     f"+{best_val}" if best_val >= 0 else str(best_val),
        "book":     BOOKMAKER_DISPLAY.get(best_bk, best_bk),
        "book_key": best_bk,
    }


def _find_bkmkr_game(sport_key: str, event_id: str) -> dict | None:
    """Find the bookmaker HAR game entry that matches an Odds API event_id."""
    with _lock:
        raw_games = _caches.get(sport_key, {}).get("raw_games", [])
    with _bkmkr_lock:
        bkmkr_games = (_bkmkr_caches.get(sport_key) or {}).get("data", [])
    if not raw_games or not bkmkr_games:
        return None
    api_game = next((g for g in raw_games if g.get("id") == event_id), None)
    if not api_game:
        return None
    return _match_teams(api_game["away_team"], api_game["home_team"], bkmkr_games)


def _process_alt_totals(event_data: dict, market_key: str) -> dict:
    """
    Process one Odds API alternate-totals market response.
    Returns: {book_key: {str(point): {over_odds, under_odds}}}
    """
    result: dict = {}
    for bm in event_data.get("bookmakers", []):
        bk = bm.get("key", "")
        pts: dict = {}
        for market in bm.get("markets", []):
            if market.get("key", "") != market_key:
                continue
            for outcome in market.get("outcomes", []):
                pt    = outcome.get("point")
                name  = outcome.get("name", "")
                price = outcome.get("price")
                if pt is None or price is None:
                    continue
                pt_key = str(float(pt))   # normalise e.g. "7" → "7.0"
                pts.setdefault(pt_key, {})
                if name == "Over":
                    pts[pt_key]["over_odds"]  = american_odds(price)
                elif name == "Under":
                    pts[pt_key]["under_odds"] = american_odds(price)
        if bk == novig_filter.NOVIG_BOOK:
            pts = _clean_novig_alt_totals(bm, market_key)
        if pts:
            result[bk] = pts
    return result


def _clean_novig_alt_totals(bm: dict, market_key: str) -> dict:
    """NoVig's alt totals with empty/stale rungs dropped (see novig_filter) and
    the exact probability of each price kept for display."""
    raw: dict = {}
    for market in bm.get("markets", []):
        if market.get("key", "") != market_key:
            continue
        for oc in market.get("outcomes", []):
            if oc.get("point") is not None and oc.get("price") is not None and oc.get("name") in ("Over", "Under"):
                raw.setdefault(float(oc["point"]), {})[oc["name"]] = oc["price"]
    rungs = [(pt, 1 / p["Under"], 1 / p["Over"], pt) for pt, p in raw.items()
             if "Over" in p and "Under" in p and novig_filter.pair_ok(p["Over"], p["Under"])]
    return {str(pt): {"over_odds":  american_odds(raw[pt]["Over"]),  "over_prob":  1 / raw[pt]["Over"],
                      "under_odds": american_odds(raw[pt]["Under"]), "under_prob": 1 / raw[pt]["Under"]}
            for pt in novig_filter.monotone_ladder(rungs)}


def _process_alt_spreads(event_data: dict, market_key: str) -> dict:
    """
    Process one Odds API alternate-spreads market response.
    Returns: {book_key: {str(away_point): {away_point, home_point, away_odds, home_odds}}}
    Uses the away team's spread as the canonical key (negative = away favored).
    """
    home_team = event_data.get("home_team", "")
    away_team = event_data.get("away_team", "")

    result: dict = {}
    for bm in event_data.get("bookmakers", []):
        bk = bm.get("key", "")
        # Collect away and home outcomes separately then pair by away_point
        away_outcomes: dict = {}   # away_point (float) → price
        home_outcomes: dict = {}   # home_point (float) → price
        for market in bm.get("markets", []):
            if market.get("key", "") != market_key:
                continue
            for outcome in market.get("outcomes", []):
                name  = outcome.get("name", "")
                point = outcome.get("point")
                price = outcome.get("price")
                if point is None or price is None:
                    continue
                if name == away_team:
                    away_outcomes[float(point)] = price
                elif name == home_team:
                    home_outcomes[float(point)] = price

        pts: dict = {}
        for away_pt, away_price in away_outcomes.items():
            home_pt  = round(-away_pt, 1)
            home_pt_key = next((k for k in home_outcomes if abs(k - home_pt) < 0.01), None)
            home_price  = home_outcomes.get(home_pt_key) if home_pt_key is not None else None
            key = str(away_pt)
            pts[key] = {
                "away_point": away_pt,
                "home_point": home_pt,
                "away_odds":  american_odds(away_price),
                "home_odds":  american_odds(home_price) if home_price else None,
            }
            if bk == novig_filter.NOVIG_BOOK:
                pts[key]["away_prob"] = _implied_from_decimal(away_price)
                pts[key]["home_prob"] = _implied_from_decimal(home_price)
                pts[key]["_ok"] = bool(home_price) and novig_filter.pair_ok(away_price, home_price)
        if bk == novig_filter.NOVIG_BOOK:
            # Empty/stale rungs out (see novig_filter); away cover chance rises with the away point.
            rungs = [(v["away_point"], v["away_prob"], v["home_prob"], k) for k, v in pts.items() if v["_ok"]]
            kept = set(novig_filter.monotone_ladder(rungs))
            pts = {k: {f: x for f, x in v.items() if f != "_ok"} for k, v in pts.items() if k in kept}
        if pts:
            result[bk] = pts
    return result


# Sports that offer alternate spreads via the Odds API
ALT_SPREADS_SPORTS = frozenset({
    "basketball_nba",
    "basketball_wnba",
    "americanfootball_nfl",
    "americanfootball_ncaaf",
    "basketball_ncaab",
})


def process_prop_event(event_data: dict, market_key: str) -> list:
    """Parse a single event's odds response into one prop row per player."""
    game_id       = event_data.get("id", "")
    home_team     = event_data.get("home_team", "")
    away_team     = event_data.get("away_team", "")
    commence_time = event_data.get("commence_time", "")

    # players[player_name][book_key] = {over_odds, under_odds, line}
    players: dict = {}

    for bm in event_data.get("bookmakers", []):
        bk = bm.get("key", "")
        if bk not in ALL_PROP_BOOKMAKERS:
            continue
        for market in bm.get("markets", []):
            if market.get("key", "") != market_key:
                continue
            by_player: dict = {}
            for outcome in market.get("outcomes", []):
                player = outcome.get("description") or "Unknown"
                side   = outcome.get("name", "")    # "Over" or "Under"
                by_player.setdefault(player, {})[side] = outcome
            for player, sides in by_player.items():
                over_o  = sides.get("Over",  {})
                under_o = sides.get("Under", {})
                line    = over_o.get("point") or under_o.get("point")
                players.setdefault(player, {})[bk] = {
                    "over_odds":  american_odds(over_o.get("price"))  if over_o.get("price")  else None,
                    "under_odds": american_odds(under_o.get("price")) if under_o.get("price") else None,
                    "line":       line,
                }

    rows = []
    for player_name, book_data in players.items():
        # Consensus line = the vig-weighted MEDIAN line across books, not the
        # plain mode — a mode ties arbitrarily and treats every book as
        # equally trustworthy regardless of how tight its own market is. This
        # is purely a display/labeling value (the frontend's model-aware
        # best-available/+EV logic reads real per-book lines directly, not
        # this field) — see buildPropCalibration for the fuller weighted-
        # median-of-solved-parameters treatment used for the actual model.
        line_weights = []
        for e in book_data.values():
            if not e or e.get("line") is None:
                continue
            po = _prop_implied_prob(e.get("over_odds"))
            pu = _prop_implied_prob(e.get("under_odds"))
            weight = 1 / max(po + pu - 1, 0.01) if po is not None and pu is not None else 1.0
            line_weights.append((e["line"], weight))
        consensus_line = _weighted_median(line_weights)

        rows.append({
            "game_id":           game_id,
            "home_team":         home_team,
            "away_team":         away_team,
            "commence_time":     commence_time,
            "prop_market":       market_key,
            "prop_market_label": PROP_MARKET_DISPLAY.get(market_key, market_key),
            "player_name":       player_name,
            "consensus_line":    consensus_line,
            "books":             book_data,
            "best_over":         find_best_prop(book_data, "over",  consensus_line),
            "best_under":        find_best_prop(book_data, "under", consensus_line),
        })

    rows.sort(key=lambda r: r["player_name"])
    return rows


def fetch_props(sport_key: str) -> list:
    """Fetch player props for all upcoming events for a sport (on-demand)."""
    market_keys = PROP_MARKETS.get(sport_key, [])
    if not market_keys:
        return []

    commence_from, commence_to = _commence_window(sport_key)

    try:
        resp = requests.get(
            f"{BASE_URL}/sports/{sport_key}/events",
            params={
                "apiKey":           API_KEY,
                "dateFormat":       "iso",
                "commenceTimeFrom": commence_from,
                "commenceTimeTo":   commence_to,
            },
            timeout=15,
        )
        resp.raise_for_status()
        events = resp.json()
        print(f"[fetch_props] {sport_key} — {len(events)} events")
    except Exception as e:
        print(f"[fetch_props] {sport_key} — failed to fetch events: {e}")
        return []

    all_rows: list = []
    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue
        for market_key in market_keys:
            try:
                resp = requests.get(
                    f"{BASE_URL}/sports/{sport_key}/events/{event_id}/odds",
                    params={
                        "apiKey":     API_KEY,
                        "regions":    "us",
                        "markets":    market_key,
                        "oddsFormat": "decimal",
                        "bookmakers": ",".join(ALL_PROP_BOOKMAKERS),
                        "dateFormat": "iso",
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
                books_returned = [bm.get("key") for bm in data.get("bookmakers", [])]
                missing = [bk for bk in ALL_PROP_BOOKMAKERS if bk not in books_returned]
                rows = process_prop_event(data, market_key)
                print(f"[fetch_props] {sport_key} {market_key} — {len(rows)} players | books: {books_returned or 'NONE'}")
                if missing:
                    print(f"[fetch_props]   missing from API response: {missing}")
                all_rows.extend(rows)
            except Exception as e:
                print(f"[fetch_props] {sport_key} event {event_id} {market_key}: {e}")

    return all_rows


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/sports")
def get_sports():
    return jsonify({
        "sports":              list(SPORTS.keys()),
        "sport_meta":          SPORTS,
        "bookmakers":          DISPLAY_BOOKS,
        "bookmaker_display":   BOOKMAKER_DISPLAY,
        "bet365_available":    BET365_AVAILABLE,
        "prop_sports":         list(PROP_MARKETS.keys()),
        "prop_market_display": PROP_MARKET_DISPLAY,
        "prop_bookmakers":     ALL_PROP_BOOKMAKERS,
    })


def _source_times(sport_key: str) -> dict:
    """When each price source's data was captured — the frontend drops a
    scraped source whose prices are hours older than the Odds API pull,
    since comparing across that gap turns every line move into an 'edge'."""
    with _lock:
        c = _caches[sport_key]
        odds_api = c.get("full_updated") or c.get("last_updated")
        odds_api_periods = c.get("periods_updated") or odds_api
    with _bkmkr_lock:
        bookmaker = (_bkmkr_caches.get(sport_key) or {}).get("last_updated")
    return {
        "odds_api":  odds_api,           # full-game rows
        "odds_api_periods": odds_api_periods,   # 1H/Q1 rows (only re-pulled on a page load)
        "bookmaker": bookmaker,
        "circa":     circa_get(sport_key).get("last_updated"),
        "bet365":    b365_get(sport_key).get("last_updated"),
    }


@app.route("/api/odds/<sport_key>")
def get_odds(sport_key):
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    fetch_odds(sport_key)
    times = _source_times(sport_key)
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
            "source_times":       times,
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


@app.route("/api/bet365/start-scrape/<sport_key>", methods=["POST"])
def bet365_start_scrape(sport_key):
    """
    Manual "Start Scraping" — the ONLY way a bet365 collection begins.

    Deliberately user-initiated with no scheduled counterpart: bet365 is the
    book actually being bet into, so its account carries real limiting risk
    and every visit should correspond to the user actually looking at the
    board. Bookmaker.eu, which is reference-only, is the one that gets a
    continuous mode.

    Uses the saved session in bet365_profile/ (see login_bet365.py) — no login
    prompt unless that session has expired, which the response reports.
    """
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    if not BET365_AVAILABLE:
        return jsonify({"error": "bet365 scraper not available"}), 503

    with _b365_lock:
        status = (_b365_caches.get(sport_key) or {}).get("status")
        if status in ("loading", "waiting_for_login"):
            return jsonify({"status": status, "already_running": True})

    b365_trigger(sport_key, force=True)
    return jsonify({"status": "loading"})


@app.route("/api/bookmaker/collect/<sport_key>", methods=["POST"])
def bookmaker_collect(sport_key):
    """
    Start a bookmaker.eu live collection.

    JSON body: {"mode": "once"}       — a single run, now
               {"mode": "continuous"} — turn the hourly schedule on
               {"mode": "stop"}       — turn the hourly schedule off
    """
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    if not BKMKR_LIVE_AVAILABLE:
        return jsonify({"error": "bookmaker live collector not available"}), 503
    if sport_key not in BKMKR_PAGES:
        return jsonify({
            "error": f"No bookmaker.eu page list configured for {sport_key} — "
                     f"add it to SPORT_PAGES in scrapers/bookmaker_live.py"
        }), 400

    mode = (request.get_json(silent=True) or {}).get("mode", "once")
    if mode in ("continuous", "stop"):
        _bkmkr_schedule["enabled"] = mode == "continuous"
        _bkmkr_save_schedule_flag()
        with _bkmkr_lock:
            status = (_bkmkr_caches.get(sport_key) or {}).get("status", "idle")
        return jsonify({"status": status, "continuous": _bkmkr_schedule["enabled"]})
    if mode != "once":
        return jsonify({"error": f"Unknown mode: {mode}"}), 400

    if _bkmkr_window["open"]:
        bkmkr_focus_login_window()
        return jsonify({"error": "The Bookmaker window is open (brought to the front). "
                                 "Close it and the app pulls this sport right away."}), 409
    started = bkmkr_trigger(sport_key)
    return jsonify({"status": "loading", "already_running": not started,
                    "continuous": _bkmkr_schedule["enabled"]})


_bkmkr_window = {"open": False}


@app.route("/api/bookmaker/login-window/<sport_key>", methods=["POST"])
def bookmaker_login_window(sport_key):
    """
    BK LOGIN: open the collector's own Chrome window on-screen to log in or
    browse lines (same session — nothing gets kicked). Pulls wait while it's
    open; when the user closes it, pull this sport right away, which both
    confirms the login and refreshes the board.
    """
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    if not BKMKR_LIVE_AVAILABLE:
        return jsonify({"error": "bookmaker live collector not available"}), 503
    if _bkmkr_window["open"]:
        bkmkr_focus_login_window()   # it may be hidden behind other windows — bring it back
        return jsonify({"status": "already_open"})

    def run():
        _bkmkr_window["open"] = True
        try:
            bkmkr_open_login_window()
        except Exception as e:
            print(f"[bookmaker] login window: {type(e).__name__}: {e}")
        finally:
            _bkmkr_window["open"] = False
        if sport_key in BKMKR_PAGES:
            bkmkr_trigger(sport_key)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "open"})


@app.route("/api/bookmaker/status/<sport_key>")
def bookmaker_status(sport_key):
    """Cache state for the bookmaker collector — drives the UI pill."""
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    with _bkmkr_lock:
        cache = dict(_bkmkr_caches.get(sport_key) or {})
    scheduled = _bkmkr_schedule["enabled"] and sport_key in BKMKR_SCHEDULE_SPORTS
    return jsonify({
        "status":       cache.get("status", "idle"),
        "games":        len(cache.get("data") or []),
        "last_updated": cache.get("last_updated"),
        "next_refresh": _bkmkr_schedule["next_run"] if scheduled else None,
        "continuous":   scheduled,
        "login_window": _bkmkr_window["open"],
        "error":        cache.get("error"),
        "available":    BKMKR_LIVE_AVAILABLE and sport_key in BKMKR_PAGES,
    })


def _har_capture_time(har_path: str, url_markers: tuple = ()) -> str | None:
    """
    When the prices in a HAR were actually captured: the latest startedDateTime
    among its (optionally URL-filtered) 200 responses, as UTC ISO. Import time
    says nothing about how stale the odds are — a HAR captured hours earlier
    otherwise looks exactly as fresh as the live Odds API prices it's compared to.
    """
    try:
        with open(har_path, encoding="utf-8") as f:
            entries = json.load(f).get("log", {}).get("entries", [])
    except Exception:
        return None
    stamps = []
    for e in entries:
        if e.get("response", {}).get("status") != 200:
            continue
        if url_markers and not any(m in e.get("request", {}).get("url", "") for m in url_markers):
            continue
        try:
            stamps.append(datetime.fromisoformat(e["startedDateTime"].replace("Z", "+00:00")))
        except (KeyError, ValueError):
            continue
    return max(stamps).astimezone(timezone.utc).isoformat() if stamps else None


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
        captured = _har_capture_time(tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if not games:
        return jsonify({"error": "No bet365 odds found in HAR — make sure you browsed the sport page in Charles"}), 400

    now = captured or datetime.now(timezone.utc).isoformat()
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
        games = games_from_bookmaker_har(tmp_path, sport_key=sport_key)
        captured = _har_capture_time(tmp_path, ("GetSchedule", "GetGameView"))
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if not games:
        return jsonify({"error": "No bookmaker.eu odds found in HAR — make sure you browsed the sport page"}), 400

    now = captured or datetime.now(timezone.utc).isoformat()
    with _bkmkr_lock:
        _bkmkr_caches[sport_key] = {
            "data":         games,
            "last_updated": now,
            "status":       "ready",
        }

    return jsonify({"status": "ready", "games": len(games), "last_updated": now})


@app.route("/api/circa/import-parsed/<sport_key>", methods=["POST"])
def circa_import_parsed(sport_key):
    """
    Accept already-parsed Circa game dicts (JSON body: {"games": [...]}) —
    unlike the HAR imports above, parsing happens out-of-process in the
    circa_watcher.py script (which runs under Odds Screen/.venv-ocr, the
    isolated venv holding pytesseract/opencv; those deps are deliberately
    NOT installed here — see Odds Screen/scrapers/parse_circa_recording.py's
    module docstring for why). This endpoint just receives the result.

    Each game dict must match parse_bookmaker_har's shape: away_team,
    home_team, commence_time, markets — the same shape bet365/bookmaker use,
    so no separate merge path is needed (see fetch_odds's circa_games
    handling and get_bet365's circa injection block).
    """
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    body = request.get_json(silent=True) or {}
    games = body.get("games")
    if not isinstance(games, list):
        return jsonify({"error": "Expected JSON body {\"games\": [...]}"}), 400

    now = datetime.now(timezone.utc).isoformat()
    with _circa_lock:
        _circa_caches[sport_key] = {
            "data":         games,
            "last_updated": now,
            "status":       "ready",
        }

    return jsonify({"status": "ready", "games": len(games), "last_updated": now})


@app.route("/api/circa/<sport_key>")
def get_circa(sport_key):
    """Return the current Circa cache for a sport — lets the watcher's caller
    (or a debugging session) check what's currently loaded without POSTing.
    Also what the frontend polls after clicking "Load Latest Recording"."""
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    return jsonify(circa_get(sport_key))


@app.route("/api/circa/import-latest/<sport_key>", methods=["POST"])
def circa_import_latest(sport_key):
    """Kicks off parsing the newest recording in OddsRecordings/ in the
    background; frontend polls GET /api/circa/<sport_key> for status."""
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    with _circa_lock:
        if (_circa_caches.get(sport_key) or {}).get("status") == "loading":
            return jsonify({"status": "loading"})   # already in progress

    _circa_set_cache(sport_key, status="loading", error=None)
    threading.Thread(target=_circa_load_latest, args=[sport_key], daemon=True).start()
    return jsonify({"status": "loading"})


@app.route("/api/alt_lines/<sport_key>/<event_id>")
def get_alt_lines(sport_key, event_id):
    """
    Return alternate lines (totals + spreads) for a specific event combining:
      - bookmaker.eu alternate lines (free, from HAR cache)
      - Odds API alternate_totals / alternate_spreads per soft book
        (1 quota per market fetched)

    Spreads alternates are fetched only for: NBA, WNBA, NFL, NCAAF, NCAAB.

    Returns:
      {
        "bookmaker_alts": { market_key: [{...}, ...] },
        "api_alts":       { market_key: { book_key: { key: {...}, ... } } },
        "remaining":      <quota remaining after all calls>,
        "bookmakers":     DISPLAY_BOOKS,
        "bookmaker_display": BOOKMAKER_DISPLAY,
      }
    """
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404

    # ── 1. Bookmaker HAR alt lines (free) ──────────────────────────────────────
    bkmkr_game     = _find_bkmkr_game(sport_key, event_id)
    bookmaker_alts = (bkmkr_game or {}).get("alt_lines", {})

    # ── 2. Build Odds API market map ───────────────────────────────────────────
    # { standard_key: (market_type, api_key) }
    configured = SPORT_MARKETS.get(sport_key, DEFAULT_MARKETS)
    # MLB: only alternate totals for full game and first half — no run-line alts, no inning alts
    MLB_ALT_TOTALS_WHITELIST = {"totals", "totals_h1"}
    alt_market_map: dict[str, tuple] = {}
    for mk in configured:
        if mk.startswith("totals"):
            if sport_key == "baseball_mlb" and mk not in MLB_ALT_TOTALS_WHITELIST:
                continue
            suffix  = mk[len("totals"):]
            alt_market_map[mk] = ("totals", "alternate_totals" + suffix)
        elif mk.startswith("spreads") and sport_key in ALT_SPREADS_SPORTS:
            if sport_key == "baseball_mlb":
                continue  # no alternate run lines for MLB
            suffix  = mk[len("spreads"):]
            alt_market_map[mk] = ("spreads", "alternate_spreads" + suffix)

    # ── 3. Fetch Odds API alternates per market ────────────────────────────────
    api_alts: dict = {}   # standard_key → processed output
    remaining = None

    for std_key, (mtype, api_key) in alt_market_map.items():
        try:
            resp = requests.get(
                f"{BASE_URL}/sports/{sport_key}/events/{event_id}/odds",
                params={
                    "apiKey":     API_KEY,
                    "regions":    "us",
                    "markets":    api_key,
                    "oddsFormat": "decimal",
                    "bookmakers": ",".join(BOOKMAKERS),
                    "dateFormat": "iso",
                },
                timeout=15,
            )
            if resp.ok:
                remaining  = resp.headers.get("x-requests-remaining")
                event_data = resp.json()
                if mtype == "totals":
                    api_alts[std_key] = _process_alt_totals(event_data, api_key)
                else:
                    api_alts[std_key] = _process_alt_spreads(event_data, api_key)
                n_lines = sum(len(v) for v in api_alts[std_key].values())
                print(f"[alt_lines] {sport_key} {event_id} {api_key}: "
                      f"{n_lines} lines across {len(api_alts[std_key])} books")
            else:
                print(f"[alt_lines] {sport_key} {event_id} {api_key}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"[alt_lines] {sport_key} {event_id} {api_key}: {e}")

    # Propagate updated quota to main cache
    if remaining is not None:
        with _lock:
            for c in _caches.values():
                c["remaining_requests"] = remaining

    return jsonify({
        "event_id":          event_id,
        "bookmaker_alts":    bookmaker_alts,
        "api_alts":          api_alts,
        "remaining":         remaining,
        "bookmakers":        DISPLAY_BOOKS,
        "bookmaker_display": BOOKMAKER_DISPLAY,
    })


def _norm_player(name: str) -> str:
    """Normalize a player name for fuzzy matching (whitespace, accents, periods)."""
    import unicodedata as _ud
    name = re.sub(r"\s+", " ", name).strip()
    name = _ud.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return name.lower().replace(".", "")


def _match_bkmkr_prop(player_name: str, bkmkr_mkt: dict) -> dict | None:
    """Find a bookmaker prop entry for a player, tolerating minor name differences."""
    if not bkmkr_mkt:
        return None
    target = _norm_player(player_name)
    # Try exact normalized match first
    for bk_name, entry in bkmkr_mkt.items():
        if _norm_player(bk_name) == target:
            return entry
    # Fallback: first-token (last name) match to handle "F. Lindor" vs "Francisco Lindor"
    target_tokens = target.split()
    if target_tokens:
        last = target_tokens[-1]
        for bk_name, entry in bkmkr_mkt.items():
            bk_tokens = _norm_player(bk_name).split()
            if bk_tokens and bk_tokens[-1] == last:
                return entry
    return None


@app.route("/api/props/<sport_key>")
def get_props(sport_key):
    """Fetch and return player props for a sport (on-demand, costs API quota)."""
    if sport_key not in PROP_MARKETS:
        return jsonify({"error": f"No props configured for {sport_key}"}), 404

    rows = fetch_props(sport_key)

    # Inject bookmaker.eu props from HAR import (if available)
    with _bkmkr_props_lock:
        bkmkr_props = dict(_bkmkr_props_caches.get(sport_key) or {})

    if bkmkr_props:
        for row in rows:
            market_key     = row.get("prop_market", "")
            player_name    = row.get("player_name", "")
            bkmkr_mkt      = bkmkr_props.get(market_key, {})
            bkmkr_entry    = _match_bkmkr_prop(player_name, bkmkr_mkt)
            row["books"]["bookmaker"] = bkmkr_entry
            # Recompute best with bookmaker now included
            consensus_line = row.get("consensus_line")
            row["best_over"]  = find_best_prop(row["books"], "over",  consensus_line)
            row["best_under"] = find_best_prop(row["books"], "under", consensus_line)

    # Inject bet365 props from HAR import (if available)
    with _b365_props_lock:
        b365_props = dict(_b365_props_caches.get(sport_key) or {})

    if b365_props:
        for row in rows:
            market_key  = row.get("prop_market", "")
            player_name = row.get("player_name", "")
            b365_mkt    = b365_props.get(market_key, {})
            b365_entry  = _match_bkmkr_prop(player_name, b365_mkt)
            row["books"]["bet365"] = b365_entry
            # Recompute best with bet365 now included
            consensus_line = row.get("consensus_line")
            row["best_over"]  = find_best_prop(row["books"], "over",  consensus_line)
            row["best_under"] = find_best_prop(row["books"], "under", consensus_line)

    now = datetime.now(timezone.utc).isoformat()
    with _props_lock:
        _props_caches[sport_key] = {"data": rows, "last_updated": now, "error": None}

    return jsonify({
        "sport_key":           sport_key,
        "data":                rows,
        "last_updated":        now,
        "prop_markets":        PROP_MARKETS.get(sport_key, []),
        "prop_market_display": PROP_MARKET_DISPLAY,
        "prop_bookmakers":     ALL_PROP_BOOKMAKERS,
        "bookmaker_display":   BOOKMAKER_DISPLAY,
    })


@app.route("/api/bookmaker/import-har/props/<sport_key>", methods=["POST"])
def bookmaker_import_props_har(sport_key):
    """Accept a HAR file from bookmaker.eu and populate bookmaker player prop odds."""
    if sport_key not in PROP_MARKETS:
        return jsonify({"error": f"No props configured for {sport_key}"}), 404

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    import tempfile, os
    har_file = request.files["file"]
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".har")
    os.close(tmp_fd)
    try:
        har_file.save(tmp_path)
        props = props_from_bookmaker_har(tmp_path, sport_key=sport_key)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if not props:
        return jsonify({"error": "No bookmaker.eu props found in HAR — make sure you browsed the props page"}), 400

    total = sum(len(v) for v in props.values())
    now = datetime.now(timezone.utc).isoformat()
    with _bkmkr_props_lock:
        _bkmkr_props_caches[sport_key] = props

    return jsonify({"status": "ready", "props": total, "markets": list(props.keys()), "last_updated": now})


@app.route("/api/bet365/import-har/props/<sport_key>", methods=["POST"])
def bet365_import_props_har(sport_key):
    """Accept a HAR file from bet365 (category page) and populate bet365 player prop odds."""
    if sport_key not in PROP_MARKETS:
        return jsonify({"error": f"No props configured for {sport_key}"}), 404

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    import tempfile, os
    har_file = request.files["file"]
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".har")
    os.close(tmp_fd)
    try:
        har_file.save(tmp_path)
        props = props_from_b365_har(tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if not props:
        return jsonify({"error": "No bet365 props found in HAR — make sure you browsed a props category page (e.g. Pitcher Strikeouts O/U)"}), 400

    total = sum(len(v) for v in props.values())
    now = datetime.now(timezone.utc).isoformat()
    with _b365_props_lock:
        if sport_key not in _b365_props_caches:
            _b365_props_caches[sport_key] = {}
        _b365_props_caches[sport_key].update(props)

    return jsonify({"status": "ready", "props": total, "markets": list(props.keys()), "last_updated": now})


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

    _REFERENCE_BOOKS = {"betonlineag", "bookmaker", "circa"}  # sharp reference books — excluded from best-available

    with _bkmkr_lock:
        bkmkr_games = list((_bkmkr_caches.get(sport_key) or {}).get("data") or [])
    with _circa_lock:
        circa_games = list((_circa_caches.get(sport_key) or {}).get("data") or [])

    if (b365_games or bkmkr_games or circa_games) and existing_rows:
        # Every market row of a game (9 for football) shares one matchup, so
        # match each game once rather than once per row.
        matches: dict = {}
        for row in existing_rows:
            mk   = row.get("market", "")
            away = row.get("away_team", "")
            home = row.get("home_team", "")
            if (away, home) not in matches:
                matches[(away, home)] = (
                    _match_teams(away, home, b365_games) if b365_games else None,
                    _match_teams(away, home, bkmkr_games) if bkmkr_games else None,
                    _match_teams(away, home, circa_games) if circa_games else None,
                )
            b365_match, bkmkr_match, circa_match = matches[(away, home)]

            # ── bet365 injection ─────────────────────────────────────────────
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
            # bet365's real alt-line ladder for this market, same shape and
            # same source-of-truth rule as bookmaker_ladder below (real quoted
            # rungs only, main line excluded — see parse_har.py). Populated for
            # football league-page HAR imports; None everywhere else.
            # NOTE: nothing in the frontend reads this yet — the cross-line
            # model still anchors on bookmaker_ladder only. Wiring it in would
            # change +EV numbers, so that is a deliberate separate step.
            row["bet365_ladder"] = (b365_match.get("alt_lines") or {}).get(mk) if b365_match else None

            # ── bookmaker injection ──────────────────────────────────────────
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
            # Bookmaker's real alt-line ladder for this market (free — already
            # scraped via the Charles Proxy HAR import, no extra API cost).
            # Full game/half spreads & totals for CFB/NFL typically get 6-8
            # real points here; quarters and other sports normally get none,
            # which is fine — the frontend cross-line model falls back to the
            # PMF translation whenever this list is empty or doesn't cover a
            # book's line. See getBookmakerLadderPrice() in index.html.
            row["bookmaker_ladder"] = (bkmkr_match.get("alt_lines") or {}).get(mk) if bkmkr_match else None

            # ── circa injection ───────────────────────────────────────────────
            circa_entry = None
            if circa_match:
                raw_mkt = (circa_match.get("markets") or {}).get(mk)
                if raw_mkt:
                    circa_entry = {
                        "home_odds":  raw_mkt.get("home_odds"),
                        "away_odds":  raw_mkt.get("away_odds"),
                        "home_point": raw_mkt.get("home_point"),
                        "away_point": raw_mkt.get("away_point"),
                    }
            row["books"]["circa"] = circa_entry

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
        "source_times": _source_times(sport_key),
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


def _run_history_capture(sport_key: str):
    """
    Snapshot full-game lines and run off-market detection. Bulk endpoint only
    (3 credits): fetch_odds would also pull 1H/Q1 per game (~6 credits each,
    ~430/hour for a 71-game NCAAF slate) that history never stores, and would
    overwrite the on-screen rows, dropping their merged Bookmaker/bet365 data.
    """
    raw, remaining, _ = _fetch_full_game_raw(sport_key, DEFAULT_MARKETS)
    _last_bulk[sport_key] = (time.time(), raw)   # reused by a Bookmaker pull within 10 min
    if remaining is not None:
        with _lock:
            for c in _caches.values():
                c["remaining_requests"] = remaining
    if not raw:
        print(f"[history] {sport_key} — no raw games to capture, skipping")
        return
    n = history_tracker.capture_snapshot(sport_key, raw)
    alerts = history_tracker.compute_and_send_alerts(sport_key, raw)
    print(f"[history] {sport_key} — captured {n} rows, {len(alerts)} new off-market alert(s)")


def _hourly_capture_loop():
    import time
    print("[history] hourly capture loop started for:", HISTORY_TRACKED_SPORTS)
    while True:
        now = datetime.now(timezone.utc)
        next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        time.sleep(max(1.0, (next_hour - now).total_seconds()))
        for sk in HISTORY_TRACKED_SPORTS:
            try:
                _run_history_capture(sk)
            except Exception as e:
                print(f"[history] {sk} — capture failed: {e}")


def _list_prop_events(sport_key: str) -> list:
    """Upcoming events inside the prop-capture window."""
    now_utc = datetime.now(timezone.utc)
    resp = requests.get(
        f"{BASE_URL}/sports/{sport_key}/events",
        params={
            "apiKey":           API_KEY,
            "dateFormat":       "iso",
            "commenceTimeFrom": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "commenceTimeTo":   (now_utc + timedelta(hours=prop_logger.MAX_HOURS_BEFORE_KICKOFF))
                                .strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _fetch_event_props(sport_key: str, event_id: str) -> tuple:
    """
    Every configured prop market for one event — one API call per market.
    Returns (rows, quota_remaining); quota is the headline cost of this logger,
    so it gets reported on every capture rather than discovered at renewal.
    """
    rows, remaining = [], None
    for market_key in PROP_MARKETS.get(sport_key, []):
        try:
            resp = requests.get(
                f"{BASE_URL}/sports/{sport_key}/events/{event_id}/odds",
                params={
                    "apiKey":     API_KEY,
                    "regions":    "us",
                    "markets":    market_key,
                    "oddsFormat": "decimal",
                    "bookmakers": ",".join(ALL_PROP_BOOKMAKERS),
                    "dateFormat": "iso",
                },
                timeout=15,
            )
            resp.raise_for_status()
            remaining = resp.headers.get("x-requests-remaining", remaining)
            rows.extend(process_prop_event(resp.json(), market_key))
        except Exception as e:
            print(f"[props-log] {sport_key} event {event_id} {market_key}: {e}")
    return rows, remaining


def _run_prop_capture(sport_key: str) -> dict:
    """Capture props for every event whose time-to-kickoff tier says it's due."""
    try:
        events = _list_prop_events(sport_key)
    except Exception as e:
        print(f"[props-log] {sport_key} — event list failed: {e}")
        return {"due": 0, "changed": 0}

    due = prop_logger.events_due(sport_key, events)
    if not due:
        return {"due": 0, "changed": 0}

    print(f"[props-log] {sport_key} — {len(due)} of {len(events)} event(s) due")
    total_changed, remaining = 0, None
    for event in due:
        rows, remaining = _fetch_event_props(sport_key, event.get("id"))
        if not rows:
            continue
        stats = prop_logger.capture_props(sport_key, rows)
        total_changed += stats["changed"]
        print(f"[props-log]   {event.get('away_team')} @ {event.get('home_team')} — "
              f"{stats['changed']} change(s) of {stats['seen']} quote(s)")
    print(f"[props-log] {sport_key} — {total_changed} total change(s), quota remaining: {remaining}")
    return {"due": len(due), "changed": total_changed, "quota_remaining": remaining}


def _prop_capture_loop():
    import time
    print("[props-log] capture loop started for:", PROP_HISTORY_TRACKED_SPORTS)
    while True:
        for sk in PROP_HISTORY_TRACKED_SPORTS:
            try:
                _run_prop_capture(sk)
            except Exception as e:
                print(f"[props-log] {sk} — capture failed: {e}")
        time.sleep(PROP_CAPTURE_TICK_SECONDS)


@app.route("/api/props/history/capture-now/<sport_key>", methods=["POST"])
def props_capture_now(sport_key):
    if sport_key not in PROP_MARKETS:
        return jsonify({"error": f"No props configured for {sport_key}"}), 404
    try:
        result = _run_prop_capture(sport_key)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True, **result})


@app.route("/api/props/history/stats")
@app.route("/api/props/history/stats/<sport_key>")
def props_history_stats(sport_key=None):
    return jsonify(prop_logger.get_stats(sport_key))


@app.route("/api/props/history/<event_id>/<player_name>/<market>")
def props_history_for_player(event_id, player_name, market):
    return jsonify({
        "rows": prop_logger.get_player_history(event_id, player_name, market),
        "bookmaker_display": BOOKMAKER_DISPLAY,
    })


@app.route("/api/history/capture-now/<sport_key>", methods=["POST"])
def history_capture_now(sport_key):
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    try:
        _run_history_capture(sport_key)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


@app.route("/api/history/alerts/<sport_key>")
def history_alerts(sport_key):
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    return jsonify({"alerts": history_tracker.get_active_alerts(sport_key)})


@app.route("/api/history/events/<sport_key>")
def history_events(sport_key):
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    return jsonify({"events": history_tracker.get_tracked_events(sport_key)})


@app.route("/api/history/<sport_key>/<event_id>")
def history_for_event(sport_key, event_id):
    if sport_key not in SPORTS:
        return jsonify({"error": f"Unknown sport: {sport_key}"}), 404
    return jsonify({
        "rows": history_tracker.get_history(sport_key, event_id),
        "bookmaker_display": BOOKMAKER_DISPLAY,
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
    try:
        print("🏀  Odds Screen  →  http://localhost:5000")
        print(f"    bet365 scraper: {'enabled' if BET365_AVAILABLE else 'not found'}")
    except UnicodeEncodeError:
        # Windows console stuck on a non-UTF-8 codepage — fall back to ASCII
        print("Odds Screen -> http://localhost:5000")
        print(f"    bet365 scraper: {'enabled' if BET365_AVAILABLE else 'not found'}")
    # Flask's debug reloader re-execs this script as a child with WERKZEUG_RUN_MAIN=true;
    # that child is the one that actually serves requests. debug=True is hardcoded below,
    # so the reloader is always active — checking app.debug here (before app.run sets it)
    # would be True in BOTH the watcher and the child and start this thread twice.
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Thread(target=_hourly_capture_loop, daemon=True).start()
        if BKMKR_LIVE_AVAILABLE:
            _bkmkr_load_schedule_flag()
            threading.Thread(target=_bkmkr_schedule_loop, daemon=True).start()
        threading.Thread(target=_prop_capture_loop, daemon=True).start()
    app.run(debug=True, port=5000)
