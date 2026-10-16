"""
Bookmaker column from OddsPapi (10/15) -- the board's Bookmaker slot without
a Bookmaker.eu login.

The user's Bookmaker.eu account was blocked on 10/14, so the scrape
(scrapers/bookmaker_live.py) returns nothing. OddsPapi carries Bookmaker's
own prices: full game main lines plus its alt ladder (no 1H/Q1/periods). Its
Pinnacle feed has the periods, so a 1H/Q1/P1 market Bookmaker doesn't price
gets Pinnacle's MAIN line in the slot instead (never Pinnacle's alt rungs:
their no-vig was noise in the 10/13 comparison). Those entries carry
via='pinnacle'.

Cost: $0. Prices come from the FREE /historical-odds endpoint (last entry of
each outcome = the current price, one fixture and up to 3 books per call,
5.3 s apart). The fixture lists are the cached ones pinnacle_close.py keeps
fresh; this module never makes a billable call.

The page's Bookmaker fade (bkAgeFade in index.html) keys on
source_times.bookmaker, which is set to each sport's SWEEP time here: full
weight for 10 min after a sweep, then Circa and BetOnline set the line.

Runs only while BK AUTO (the scrape's hourly schedule) is off and BK_SOURCE in
.env isn't "scrape", so the two never write the same cache.
"""
import os
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import oddspapi as op

SWEEP_EVERY = 3600            # per sport, seconds between sweeps
SPORTS = ("americanfootball_nfl", "americanfootball_ncaaf", "icehockey_nhl", "basketball_nba")
BOOKS = ("bookmaker.eu", "pinnacle")
PERIOD_SUFFIXES = ("_h1", "_q1", "_p1")

_status = {"running": False, "sport": None, "done": 0, "total": 0, "last": {}, "errors": []}
_lock = threading.Lock()
# sport -> {fixture_id: game}: a fixture that fails (or isn't reached) on one
# sweep keeps its previous game rather than dropping out of the slot for an hour.
_games: dict = {}
ET = ZoneInfo("America/New_York")
RETRY_429_S = 30


def source() -> str:
    try:
        from dotenv import dotenv_values
        v = dotenv_values(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")).get("BK_SOURCE")
    except Exception:
        v = None
    # 10/16 (user): OddsPapi's free Bookmaker prices ran stale, so the slot is
    # OFF unless BK_SOURCE=oddspapi; the board runs on Circa + BetOnline alone.
    return (v or os.environ.get("BK_SOURCE") or "off").strip().lower()


def status() -> dict:
    acc = op.account_cached() or {}
    with _lock:
        return {"source": source(),
                # OddsPapi's own monthly counter (GET /account, free), refreshed after each sweep
                "account": {"limit": acc.get("request_limit"), "used": acc.get("request_count")},
                **{k: (dict(v) if isinstance(v, dict) else list(v) if isinstance(v, list) else v)
                   for k, v in _status.items()}}


def request_now():
    """RELOAD ALL: every sport due on the loop's next pass (within 30 s)."""
    with _lock:
        _status["last"] = {k: dict(v, at=0) for k, v in _status["last"].items()}


def _entry(r):
    return {"home_odds": r["home_odds"], "away_odds": r["away_odds"],
            "home_point": r["home_point"], "away_point": r["away_point"]}


def _rung(mk, r):
    if mk.startswith("totals"):            # board orientation: home = Over
        return {"point": r["home_point"], "over_odds": r["home_odds"], "under_odds": r["away_odds"]}
    return {"away_point": r["away_point"], "home_point": r["home_point"],
            "away_odds": r["away_odds"], "home_odds": r["home_odds"]}


def to_game(fx, parsed):
    """One board-shaped Bookmaker game (markets + alt_lines) from a parsed
    history response, or None when neither book prices anything."""
    markets, alts = {}, {}
    for mk, rows in (parsed.get("bookmaker.eu") or {}).items():
        main = op.main_row(rows)
        if not main:
            continue
        markets[mk] = _entry(main)
        if not mk.startswith("h2h"):
            ladder = [_rung(mk, r) for r in op.live(rows) if r is not main]
            key = "point" if mk.startswith("totals") else "away_point"
            ladder = sorted((x for x in ladder if x.get(key) is not None), key=lambda x: x[key])
            if ladder:
                alts[mk] = ladder
    for mk, rows in (parsed.get("pinnacle") or {}).items():
        if mk in markets or not mk.endswith(PERIOD_SUFFIXES):
            continue
        main = op.main_row(rows)
        if main:
            markets[mk] = dict(_entry(main), via="pinnacle")
    if not markets:
        return None
    ts = op._ts(fx.get("commence_time"))
    return {"away_team": fx["away_team"], "home_team": fx["home_team"], "commence_time": fx["commence_time"],
            # ET date, so app._dated() keeps an NHL/NBA rematch two days later from cross-matching
            "_date": datetime.fromtimestamp(ts, ET).date() if ts else None,
            "markets": markets, "alt_lines": alts, "source": "oddspapi"}


def sweep(sport_key, now=None) -> list:
    """Every upcoming fixture in the cached list -> Bookmaker games. Free.
    Fixtures not refreshed this time (error, 429) keep their last game."""
    c = op.fixtures_cached(sport_key)
    if not c:
        return []
    now = now or time.time()
    days = op.SPORTS.get(sport_key, (None, None, None, 2))[3]
    todo = [f for f in c.get("fixtures") or []
            if f.get("status") in (0, None) and now < (op._ts(f.get("commence_time")) or 0) < now + days * 86400]
    with _lock:
        _status.update(sport=sport_key, done=0, total=len(todo))
    prev = _games.get(sport_key) or {}
    keep = {f["fixture_id"] for f in todo}
    out = {fid: g for fid, g in prev.items() if fid in keep}      # started games drop out
    throttled = False
    for f in todo:
        if throttled:
            break
        for attempt in (1, 2):
            try:
                g = to_game(f, op.parse_history(op.history(f["fixture_id"], list(BOOKS))))
                if g:
                    out[f["fixture_id"]] = g
                else:
                    out.pop(f["fixture_id"], None)
                break
            except op.FeedError as e:
                with _lock:
                    _status["errors"] = (_status["errors"] + [f"{sport_key} {f.get('away_team')} @ {f.get('home_team')}: {e}"])[-10:]
                if e.status == 429 and attempt == 1:
                    time.sleep(RETRY_429_S)          # e.g. the :5066 dashboard's own history calls
                    continue
                throttled = e.status == 429
                break
        with _lock:
            _status["done"] += 1
    _games[sport_key] = out
    return list(out.values())


def run_loop(store, active, clear=None):
    """Background loop (app.py). store(sport_key, games, swept_at_iso) writes
    the Bookmaker cache; active() says whether this source should run now;
    clear() empties any OddsPapi-sourced Bookmaker games while it's off."""
    time.sleep(20)
    print(f"[bk-oddspapi] loop started (source: {source()}; sweeps only with BK_SOURCE=oddspapi and BK AUTO off)",
          flush=True)
    while True:
        try:
            if source() != "oddspapi" and clear:
                clear()                           # cheap: only touches caches it filled
            if active() and source() == "oddspapi" and op.enabled():
                for sk in SPORTS:
                    last = _status["last"].get(sk) or {}
                    if time.time() - last.get("at", 0) < SWEEP_EVERY:
                        continue
                    if not op.fixtures_cached(sk):
                        continue
                    with _lock:
                        _status["running"] = True
                    t0 = time.time()
                    # stamped at the START: the first games of a 5-min NCAAF sweep
                    # are that old by the end, and the page's fade should know it
                    swept = datetime.now(timezone.utc).isoformat()
                    games = sweep(sk)
                    if games:
                        store(sk, games, swept)
                    with _lock:
                        _status["last"][sk] = {"at": time.time(), "games": len(games), "swept_at": swept,
                                               "seconds": round(time.time() - t0)}
                        _status["running"] = False
                    print(f"[bk-oddspapi] {sk}: {len(games)} games in {round(time.time() - t0)} s", flush=True)
                    try:
                        op.account()                      # free: refreshes the requests-left counter
                    except op.FeedError:
                        pass
        except Exception as e:                    # never let one bad sweep stop the loop
            with _lock:
                _status["running"] = False
                _status["errors"] = (_status["errors"] + [f"loop: {type(e).__name__}: {e}"])[-10:]
            print(f"[bk-oddspapi] {type(e).__name__}: {e}", flush=True)
        time.sleep(30)
