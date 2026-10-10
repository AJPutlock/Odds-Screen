"""
odds-api.io feed for the board's Circa and bet365 columns (10/8).

Circa used to come from screen-recording OCR (Load Circa) and bet365 from a
scraper / HAR import; odds-api.io carries both books' real prices, so while
ODDS_API_IO_KEY is set in .env they come from here instead (app.circa_get /
b365_get prefer this feed when it has games for the sport; the old sources
are the fallback). Same game-dict shape the scrapers produce:

  {away_team, home_team, commence_time, _date (ET date), markets: {market_key:
   {away_odds, home_odds, away_point, home_point}}, alt_lines: {market_key: [...]}}

Odds are American strings ("+120", "-110"); totals put the Over in the home
slot. A market's main line is its row priced closest to even; bet365's other
rows become its alt ladder. Plan limit 5,000 requests/hour: one events call +
one odds call per 10 games, cached FRESH_SECONDS per sport.
"""
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

try:
    from dotenv import load_dotenv
except ImportError:          # pragma: no cover
    load_dotenv = None

BASE = "https://api.odds-api.io/v3"
BOOKS = {"Circa": "circa", "Bet365": "bet365"}
# "Bet365" on odds-api.io is bet365's international site; "bet365 NJ" is the
# US book (the prices an Ohio account sees). The bet365 column uses NJ whenever
# it's selected on the account, else the international feed.
BET365_US = "bet365 NJ"
_selected = {"at": 0.0, "names": set()}


def _books():
    """odds-api.io name -> board key for this account's selected books."""
    if time.time() - _selected["at"] > 600:
        try:
            sel = _get("/bookmakers/selected")
            _selected.update(at=time.time(), names=set((sel or {}).get("bookmakers") or []))
        except (FeedError, ValueError):
            _selected["at"] = time.time() - 480       # retry in ~2 min
    names = _selected["names"]
    if not names:                                  # couldn't read the selection: ask for both
        return dict(BOOKS)
    out = {n: k for n, k in (("Circa", "circa"), (BET365_US, "bet365"), ("Bet365", "bet365")) if n in names}
    if BET365_US in out:
        out.pop("Bet365", None)                    # US book wins when both are selected
    return out
FRESH_SECONDS = 60
ET = ZoneInfo("America/New_York")

# app sport key -> (odds-api.io sport, [league slugs], hours ahead)
SPORTS = {
    "americanfootball_nfl":   ("american-football", ["usa-nfl"], 168),
    "americanfootball_ncaaf": ("american-football", ["usa-college"], 168),
    "basketball_nba":         ("basketball", ["usa-nba"], 36),
    "basketball_nba_preseason": ("basketball", ["usa-nba-preseason"], 36),
    "basketball_ncaab":       ("basketball", ["usa-ncaa"], 36),
    "basketball_wnba":        ("basketball", ["usa-wnba", "usa-wnba-playoffs"], 36),
    "baseball_ncaa":          ("baseball", ["usa-ncaa"], 36),   # slug unconfirmed (offseason); skipped if unknown
    "icehockey_nhl":          ("ice-hockey", ["usa-nhl"], 36),
    "baseball_mlb":           ("baseball", ["usa-mlb", "usa-mlb-playoffs"], 36),
}

# odds-api.io market name -> board suffix, per sport family
_PERIODS = {
    "american-football": {"": "", "HT": "_h1", "1Q": "_q1"},
    "basketball":        {"": "", "HT": "_h1", "1Q": "_q1"},
    "ice-hockey":        {"": "", "1Q": "_p1"},          # hockey's "1Q" is the 1st period
}
_BASEBALL = {"ML": "h2h", "Spread": "spreads", "Totals": "totals",
             "First 5 Innings ML": "h2h_1st_5_innings", "First 5 Innings Spread": "spreads_1st_5_innings",
             "First 5 Innings Totals": "totals_1st_5_innings", "1st Inning Totals": "totals_1st_1_innings"}
_BASE = {"ML": "h2h", "Spread": "spreads", "Totals": "totals"}

_lock = threading.Lock()
_cache: dict = {}          # sport_key -> {"at": epoch, "fetched_at": iso, "circa": [...], "bet365": [...], "error": str|None}
_fetching: set = set()


def _key():
    if load_dotenv:
        load_dotenv(Path(__file__).parent / ".env", override=True)
    return os.environ.get("ODDS_API_IO_KEY", "").strip()


def enabled() -> bool:
    return bool(_key())


class FeedError(Exception):
    """An odds-api.io failure, described without the request URL (it carries the key)."""
    def __init__(self, status, path, text=""):
        super().__init__(f"HTTP {status} on {path}{': ' + text[:120] if text else ''}")
        self.status = status


def _get(path, **params):
    # "Server busy, retry shortly" (503), rate limits (429) and network blips
    # are retried twice with a short wait; a refused request (403 etc.) isn't.
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE}{path}", params={"apiKey": _key(), **params}, timeout=20)
        except requests.exceptions.RequestException as e:
            err = FeedError("network", path, type(e).__name__)
        else:
            if r.ok:
                return r.json()
            err = FeedError(r.status_code, path, r.text)
            if r.status_code not in (429, 500, 502, 503, 504):
                raise err
        if attempt < 2:
            time.sleep(2 + 3 * attempt)
    raise err


def _am(dec):
    try:
        d = float(dec)
    except (TypeError, ValueError):
        return None
    if d <= 1.01:
        return None                       # suspended / placeholder price
    a = round((d - 1) * 100) if d >= 2 else round(-100 / (d - 1))
    return f"+{a}" if a > 0 else str(a)


def _market_key(sport, name):
    if sport == "baseball":
        return _BASEBALL.get(name)
    parts = name.split(" ", 1)
    base = _BASE.get(parts[0])
    if not base:
        return None
    suffix = _PERIODS.get(sport, {}).get(parts[1] if len(parts) > 1 else "")
    return None if suffix is None else base + suffix


def _rows_to_market(base, rows):
    """(main entry, alt ladder) from one market's odds rows."""
    def dec(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None
    if base == "h2h":
        r = rows[0] if rows else {}
        h, a = _am(r.get("home")), _am(r.get("away"))
        return ({"home_odds": h, "away_odds": a, "home_point": None, "away_point": None} if h and a else None), []
    entries = []
    for r in rows:
        line = r.get("hdp", r.get("max"))
        if line is None:
            continue
        if base == "spreads":
            h, a = _am(r.get("home")), _am(r.get("away"))
            if not (h and a):
                continue
            entries.append((abs((dec(r.get("home")) or 0) - (dec(r.get("away")) or 0)),
                            {"home_point": float(line), "away_point": -float(line), "home_odds": h, "away_odds": a}))
        else:
            o, u = _am(r.get("over")), _am(r.get("under"))
            if not (o and u):
                continue
            entries.append((abs((dec(r.get("over")) or 0) - (dec(r.get("under")) or 0)),
                            {"home_point": float(line), "away_point": float(line), "home_odds": o, "away_odds": u}))
    if not entries:
        return None, []
    main = min(entries, key=lambda e: e[0])[1]
    alts = []
    for _, e in entries:
        if e is main:
            continue
        if base == "spreads":
            alts.append({"away_point": e["away_point"], "home_point": e["home_point"],
                         "away_odds": e["away_odds"], "home_odds": e["home_odds"]})
        else:
            alts.append({"point": e["home_point"], "over_odds": e["home_odds"], "under_odds": e["away_odds"]})
    alts.sort(key=lambda x: x.get("away_point", x.get("point")) or 0)
    return main, alts


def _fetch(sport_key):
    sport, leagues, hours = SPORTS[sport_key]
    now = datetime.now(timezone.utc)
    # Only games Circa or bet365 actually price (college lists FCS/D-II too).
    books = _books()
    events, seen = [], set()
    for lg in leagues:
        for api_name in books:
            try:
                evs = _get("/events", sport=sport, league=lg, status="pending", bookmaker=api_name,
                           **{"from": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                              "to": (now + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")})
            except FeedError as e:
                if e.status == 403:
                    raise                     # book selection changed: _refresh re-reads it
                continue                      # e.g. an off-season / unknown league slug
            for e in evs if isinstance(evs, list) else []:
                if e.get("id") not in seen:
                    seen.add(e.get("id"))
                    events.append(e)
    out = {bk: [] for bk in BOOKS.values()}
    newest = {}
    for i in range(0, len(events), 10):
        batch = events[i:i + 10]
        res = _get("/odds/multi", eventIds=",".join(str(e["id"]) for e in batch), bookmakers=",".join(books))
        for ev in res if isinstance(res, list) else []:
            try:
                ct = datetime.fromisoformat(str(ev.get("date")).replace("Z", "+00:00"))
            except ValueError:
                ct = None
            for api_name, bk in books.items():
                mkts = (ev.get("bookmakers") or {}).get(api_name) or []
                markets, alt_lines = {}, {}
                for m in mkts:
                    key = _market_key(sport, m.get("name") or "")
                    if not key:
                        continue
                    main, alts = _rows_to_market(key.split("_")[0], m.get("odds") or [])
                    if main:
                        markets[key] = main
                    if alts:
                        alt_lines[key] = alts
                    if m.get("updatedAt"):
                        newest[bk] = max(newest.get(bk, ""), m["updatedAt"])
                if markets:
                    out[bk].append({"away_team": ev.get("away"), "home_team": ev.get("home"),
                                    "commence_time": ev.get("date"), "_date": ct.astimezone(ET).date() if ct else None,
                                    "markets": markets, "alt_lines": alt_lines, "_oddsio_id": ev.get("id")})
    out["_bet365_source"] = next((n for n in books if n != "Circa"), None)
    return out, newest, len(events)


def _refresh(sport_key):
    prev = _cache.get(sport_key)
    try:
        try:
            data, newest, n = _fetch(sport_key)
        except FeedError as e:
            if e.status != 403:
                raise
            _selected["at"] = 0                    # a selected book changed: re-read it, retry once
            data, newest, n = _fetch(sport_key)
        entry = {"at": time.time(), "fetched_at": datetime.now(timezone.utc).isoformat(), "events": n,
                 "newest": newest, "error": None, **data}
    except (FeedError, ValueError) as e:
        print(f"[oddsio] {sport_key}: {e}")
        # Try again in ~15 s rather than waiting out the full FRESH_SECONDS.
        entry = {**(prev or {}), "at": time.time() - FRESH_SECONDS + 15, "error": str(e)}
    with _lock:
        _cache[sport_key] = entry
        _fetching.discard(sport_key)


def get(sport_key, book):
    """The book's games for this sport from odds-api.io (refreshed when older
    than FRESH_SECONDS), or None when the feed is off / unsupported / failed."""
    if sport_key not in SPORTS or not enabled():
        return None
    with _lock:
        hit = _cache.get(sport_key)
        stale = not hit or time.time() - hit["at"] > FRESH_SECONDS
        go = stale and sport_key not in _fetching
        if go:
            _fetching.add(sport_key)
    if go:
        if hit:      # have prices already: refresh in the background, serve these meanwhile
            threading.Thread(target=_refresh, args=(sport_key,), daemon=True).start()
        else:
            _refresh(sport_key)
            with _lock:
                hit = _cache.get(sport_key)
    if not hit or not hit.get(book):
        return None
    return {"data": hit[book], "last_updated": hit.get("fetched_at"), "status": "ready",
            "error": hit.get("error"), "source": "odds-api.io"}


def get_fresh(sport_key, book, max_age=15, wait=30):
    """get(), but with prices at most max_age seconds old: fetched right now
    (or, when a refresh is already running, after it lands). For the closing
    capture, which shouldn't store a minute-old cached price."""
    if sport_key not in SPORTS or not enabled():
        return None
    deadline = time.time() + wait
    while True:
        with _lock:
            hit = _cache.get(sport_key)
            fresh = bool(hit) and not hit.get("error") and time.time() - hit["at"] <= max_age
            go = not fresh and sport_key not in _fetching
            if go:
                _fetching.add(sport_key)
        if go:
            _refresh(sport_key)
            break
        if fresh or time.time() > deadline:
            break
        time.sleep(0.5)            # another refresh is in flight: wait for it
    return get(sport_key, book)


def status(sport_key):
    with _lock:
        hit = dict(_cache.get(sport_key) or {})
    return {"enabled": enabled(), "fetched_at": hit.get("fetched_at"), "events": hit.get("events"),
            "circa_games": len(hit.get("circa") or []), "bet365_games": len(hit.get("bet365") or []),
            "bet365_source": hit.get("_bet365_source"),
            "newest_price": hit.get("newest"), "error": hit.get("error")}
