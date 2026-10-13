"""
SportsGameOdds test bench (10/13) — a stand-alone dashboard on port 5065 that
pulls SportsGameOdds (SGO) and lines it up against the board the Odds Screen
already builds (The Odds API rec books + odds-api.io Circa/BetOnline + the
Bookmaker scrape + the exchange feeds).

    python sgo_compare.py        ->  http://127.0.0.1:5065

It never touches app.py: the board comes from the running Odds Screen's
GET /api/bet365/<sport_key>, which only merges caches already in memory (no
Odds API credits). If the Odds Screen isn't running, SGO data still shows,
just with nothing to compare against.

Budget: the free (Amateur) key is 2,500 objects/month, 10 requests/minute,
10-minute updates. One object = one event per response, so pulling a
60-game NCAAF slate costs 60. Every pull is saved under data/sgo/ and every
view after that is free; a local ledger counts objects per calendar month and
a pull that would overrun it is refused unless forced.

Key: SPORTSGAMEODDS_API_KEY in .env (git-ignored).
"""
import json
import os
import re
import statistics
import threading
import time
import unicodedata
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from flask import Flask, jsonify, request, send_from_directory

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

HERE = Path(__file__).parent
DATA = HERE / "data" / "sgo"
LEDGER = DATA / "ledger.json"
BASE = "https://api.sportsgameodds.com/v2"
MAIN_APP = os.environ.get("ODDS_SCREEN_URL", "http://127.0.0.1:5000")
PORT = 5065
# SGO_PLAN in .env picks the budget and the update cadence the page warns about.
# Pricing page 10/13: only Pro ($299) and All-Star name Pinnacle, Circa and bet365;
# WebSocket streaming is All-Star only; every paid plan has a 7-day free trial.
PLANS = {
    "amateur": {"name": "Amateur (free)", "objects": 2500,   "rpm": 10,  "cadence": "10 min"},
    "rookie":  {"name": "Rookie $99",     "objects": 100000, "rpm": 50,  "cadence": "3 min"},
    "pro":     {"name": "Pro $299",       "objects": None,   "rpm": 300, "cadence": "sub-minute"},
    "allstar": {"name": "All-Star",       "objects": None,   "rpm": None, "cadence": "real-time"},
}
PAGE_LIMIT = 50

# Odds Screen sport_key -> SGO leagueID
LEAGUES = {
    "americanfootball_nfl":     "NFL",
    "americanfootball_ncaaf":   "NCAAF",
    "icehockey_nhl":            "NHL",
    "basketball_nba":           "NBA",
    # no basketball_nba_preseason: SGO's NBA league starts with the regular season (10/13 pull: only 10/20 games)
    "basketball_ncaab":         "NCAAB",
    "baseball_mlb":             "MLB",
}

# SGO bookmakerID -> board book key, where the two name the same book.
# thescorebet wins over espnbet (the board's "espnbet" is theScore).
SGO_TO_BOARD = {
    "caesars":     "williamhill_us",
    "betonline":   "betonlineag",
    "bookmakereu": "bookmaker",
    "thescorebet": "espnbet",
    "espnbet":     "espnbet",
}
SGO_PRIORITY = {"thescorebet": 2, "espnbet": 1}

# Names for books the board doesn't carry.
SGO_DISPLAY = {"bovada": "Bovada", "unibet": "Unibet", "pointsbet": "PointsBet", "williamhill": "William Hill (SGO)",
               "pinnacle": "Pinnacle", "betus": "BetUS", "mybookie": "MyBookie", "lowvig": "LowVig",
               "sportsbetting_ag": "SportsBetting.ag", "prizepicks": "PrizePicks", "underdog": "Underdog"}

# Where each board book comes from today.
BOARD_SOURCE = {
    "circa":       "odds-api.io (45 s)",
    "betonlineag": "Odds API + odds-api.io (newer wins)",
    "bookmaker":   "Bookmaker scrape (hourly)",
    "kalshi":      "Kalshi feed (20 s)",
    "polymarket":  "Polymarket feed (20 s)",
    "novig":       "NoVig feed (20 s)",
}
DEFAULT_SOURCE = "The Odds API (BK pulls / Reload)"
SHARP = ("circa", "bookmaker", "betonlineag")

# Board market suffix -> SGO periodID
PERIODS = {"": "game", "_h1": "1h", "_q1": "1q", "_p1": "1p"}
BET_TYPES = {"h2h": "ml", "spreads": "sp", "totals": "ou"}

app = Flask(__name__, static_folder=str(HERE / "static"))
_lock = threading.Lock()
_last_request = [0.0]


# ── key, ledger ──────────────────────────────────────────────────────────────

def _key() -> str:
    if load_dotenv:
        load_dotenv(HERE / ".env", override=True)
    return os.environ.get("SPORTSGAMEODDS_API_KEY", "").strip()


def _plan() -> dict:
    _key()   # loads .env
    p = os.environ.get("SGO_PLAN", "amateur").strip().lower()
    return {"id": p if p in PLANS else "amateur", **PLANS.get(p, PLANS["amateur"])}


def _monthly_objects():
    return _plan()["objects"]


def _month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _ledger() -> dict:
    try:
        led = json.loads(LEDGER.read_text())
    except (OSError, ValueError):
        led = {}
    if led.get("month") != _month():
        led = {"month": _month(), "objects": 0, "requests": 0, "pulls": []}
    return led


def _save_ledger(led: dict):
    DATA.mkdir(parents=True, exist_ok=True)
    LEDGER.write_text(json.dumps(led, indent=1))


def _sgo_get(path: str, params: dict) -> requests.Response:
    """One SGO request, spaced to stay under 10/minute."""
    rpm = _plan()["rpm"]
    wait = (60 / rpm + 0.5 if rpm else 0) - (time.time() - _last_request[0])
    if wait > 0:
        time.sleep(wait)
    _last_request[0] = time.time()
    return requests.get(f"{BASE}{path}", params=params,
                        headers={"x-api-key": _key()}, timeout=60)


# ── board (running Odds Screen) ──────────────────────────────────────────────

def _board(sport_key: str) -> dict:
    try:
        r = requests.get(f"{MAIN_APP}/api/bet365/{sport_key}", timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"data": [], "source_times": {}, "error": f"Odds Screen not reachable at {MAIN_APP}: {e}"}


def _board_display() -> dict:
    try:
        return requests.get(f"{MAIN_APP}/api/sports", timeout=10).json().get("bookmaker_display", {})
    except Exception:
        return {}


# ── helpers ──────────────────────────────────────────────────────────────────

def _parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _imp(odds):
    o = _num(odds)
    if o is None or o == 0:
        return None
    return 100 / (o + 100) if o > 0 else -o / (-o + 100)


_STOP = {"the", "university", "of", "st", "state", "fc", "u"}


def _tokens(*names) -> set:
    out = set()
    for n in names:
        if not n:
            continue
        n = unicodedata.normalize("NFKD", str(n)).encode("ascii", "ignore").decode().lower()
        out |= {t for t in re.split(r"[^a-z0-9]+", n) if t and t not in _STOP}
    return out


def _team_names(team: dict) -> list:
    names = team.get("names") or {}
    return [names.get("long"), names.get("medium"), names.get("location"), names.get("short")]


def _tok_hit(t: str, words: set) -> bool:
    """t is in words, or abbreviates one ("miss" -> "mississippi")."""
    return t in words or (len(t) >= 3 and any(w.startswith(t) for w in words))


def _team_score(board_name: str, sgo_team: dict) -> float:
    """Share of SGO's long name found in the board's name. SGO's college
    names are the school alone ("Alabama", "Southern Miss") where the board
    has "Alabama Crimson Tide", so the score is measured against SGO's name;
    the greedy best-first match keeps "Tennessee" off "Tennessee Tech"."""
    a = _tokens(board_name)
    b = _tokens((sgo_team.get("names") or {}).get("long")) or _tokens(*_team_names(sgo_team))
    if not a or not b:
        return 0.0
    score = sum(_tok_hit(t, a) for t in b) / len(b) * (1.0 if len(b) <= len(a) else len(a) / len(b))
    return score - 0.01 * (len(a) - len(b))      # tie-break: "Texas Tech" beats "Texas" for Texas Tech


def _match_games(board_rows: list, events: list) -> dict:
    """board game_id -> SGO event, greedy best score with kickoff within 3 h."""
    games = {}
    for r in board_rows:
        games.setdefault(r["game_id"], r)
    pairs = []
    for gid, r in games.items():
        bt = _parse_ts(r.get("commence_time"))
        for ev in events:
            st = _parse_ts((ev.get("status") or {}).get("startsAt"))
            if bt and st and abs((bt - st).total_seconds()) > 3 * 3600:
                continue
            teams = ev.get("teams") or {}
            s = (_team_score(r["home_team"], teams.get("home") or {}) +
                 _team_score(r["away_team"], teams.get("away") or {})) / 2
            if s >= 0.5:
                pairs.append((s, gid, ev["eventID"]))
    pairs.sort(reverse=True)
    by_id = {ev["eventID"]: ev for ev in events}
    out, used = {}, set()
    for s, gid, eid in pairs:
        if gid in out or eid in used:
            continue
        out[gid] = by_id[eid]
        used.add(eid)
    return out


def _ev_label(ev: dict) -> str:
    t = ev.get("teams") or {}
    return f"{((t.get('away') or {}).get('names') or {}).get('long', '?')} @ " \
           f"{((t.get('home') or {}).get('names') or {}).get('long', '?')}"


def _sgo_side(odd: dict, book: str, line_field: str) -> dict:
    bb = ((odd or {}).get("byBookmaker") or {}).get(book)
    if not bb:
        return None
    return {"odds": bb.get("odds"), "point": _num(bb.get(line_field)) if line_field else None,
            "available": bb.get("available", True), "updated": bb.get("lastUpdatedAt"),
            "alts": len(bb.get("altLines") or [])}


def _sgo_market(ev: dict, market: str) -> dict:
    """SGO prices for one board market, keyed by board book key, in the board's
    orientation (h2h/spreads home/away; totals home = Over, away = Under)."""
    base, suffix = market, ""
    for suf in ("_h1", "_q1", "_p1"):
        if market.endswith(suf):
            base, suffix = market[: -len(suf)], suf
    bt, per = BET_TYPES.get(base), PERIODS.get(suffix)
    if not bt or not per:
        return {}, None
    odds = ev.get("odds") or {}
    if bt == "ou":
        home_odd = odds.get(f"points-all-{per}-ou-over")
        away_odd = odds.get(f"points-all-{per}-ou-under")
        field, fair_field = "overUnder", "fairOverUnder"
    else:
        home_odd = odds.get(f"points-home-{per}-{bt}-home")
        away_odd = odds.get(f"points-away-{per}-{bt}-away")
        field = "spread" if bt == "sp" else None
        fair_field = "fairSpread" if bt == "sp" else None
    books = set(((home_odd or {}).get("byBookmaker") or {})) | set(((away_odd or {}).get("byBookmaker") or {}))
    out, prio = {}, {}
    for b in books:
        key = SGO_TO_BOARD.get(b, b)
        p = SGO_PRIORITY.get(b, 0)
        if key in out and prio[key] >= p:
            continue
        h, a = _sgo_side(home_odd, b, field), _sgo_side(away_odd, b, field)
        if not h and not a:
            continue
        out[key] = {"home_odds": (h or {}).get("odds"), "home_point": (h or {}).get("point"),
                    "away_odds": (a or {}).get("odds"), "away_point": (a or {}).get("point"),
                    "available": bool((h or {}).get("available", True) and (a or {}).get("available", True)),
                    "updated": max(filter(None, [(h or {}).get("updated"), (a or {}).get("updated")]), default=None),
                    "alts": max((h or {}).get("alts", 0), (a or {}).get("alts", 0)),
                    "sgo_id": b}
        prio[key] = p
    fair = None
    if home_odd and home_odd.get("fairOddsAvailable"):
        fair = {"home_odds": home_odd.get("fairOdds"), "away_odds": (away_odd or {}).get("fairOdds"),
                "home_point": _num(home_odd.get(fair_field)) if fair_field else None,
                "away_point": _num((away_odd or {}).get(fair_field)) if fair_field else None,
                "book_home": home_odd.get("bookOdds"), "book_away": (away_odd or {}).get("bookOdds")}
    return out, fair


def _shin(pa, pb):
    """Shin de-vig of a two-way price, same as devigShin in static/index.html."""
    S = pa + pb
    if not S > 1:
        return pa / S, pb / S
    f = lambda q, z: ((z * z + 4 * (1 - z) * q * q / S) ** 0.5 - z) / (2 * (1 - z))
    lo, hi = 0.0, 0.5
    for _ in range(50):
        z = (lo + hi) / 2
        if f(pa, z) + f(pb, z) > 1:
            lo = z
        else:
            hi = z
    z = (lo + hi) / 2
    a, b = f(pa, z), f(pb, z)
    return a / (a + b), b / (a + b)


def _fair_home(v: dict):
    """De-vigged home-side (Over for totals) probability of one book's quote."""
    if not v:
        return None
    ph, pa = _imp(v.get("home_odds")), _imp(v.get("away_odds"))
    if ph is None or pa is None:
        return None
    return _shin(ph, pa)[0]


def _same_number(a: dict, b: dict) -> bool:
    return a.get("home_point") == b.get("home_point") and a.get("away_point") == b.get("away_point")


def _blend(probs: dict, weights: dict):
    w = {k: weights.get(k, 0) for k in probs}
    tot = sum(w.values())
    return sum(probs[k] * w[k] for k in probs) / tot if tot else None


# Our board's locked sharp weights (sharpWeights in static/index.html).
W_CB = {"circa": 0.7, "betonlineag": 0.3}                        # Circa + BetOnline
W_FULL3 = {"circa": 0.5, "bookmaker": 0.3, "betonlineag": 0.2}   # all three
W_FULL2 = {"circa": 0.6, "bookmaker": 0.4}
TARGET_BOOKS = [("pinnacle", "Pinnacle"), ("bet365", "bet365"), ("circa", "Circa"), ("bookmaker", "Bookmaker.eu")]


def _full_weights(has: set) -> dict:
    if len(has) == 1:
        return {k: 1 for k in has}
    if len(has) == 3:
        return W_FULL3
    if has == {"circa", "bookmaker"}:
        return W_FULL2
    return {"circa": 0.7, "bookmaker": 0.7, "betonlineag": 0.3}


def _pp_stats(diffs: list) -> dict:
    if not diffs:
        return {"n": 0, "mean_abs_pp": None, "median_abs_pp": None, "over_1_5_pct": None, "mean_signed_pp": None}
    a = [abs(d) for d in diffs]
    return {"n": len(diffs), "mean_abs_pp": round(statistics.mean(a), 2),
            "median_abs_pp": round(statistics.median(a), 2),
            "over_1_5_pct": round(100 * sum(x > 1.5 for x in a) / len(a), 1),
            "mean_signed_pp": round(statistics.mean(diffs), 2)}


def _compare_cell(board: dict, sgo: dict) -> str:
    """match | price | line | board_only | sgo_only."""
    if not board and not sgo:
        return None
    if not sgo:
        return "board_only"
    if not board:
        return "sgo_only"
    if board.get("home_point") != sgo.get("home_point") or board.get("away_point") != sgo.get("away_point"):
        return "line"
    if str(board.get("home_odds")) == str(sgo.get("home_odds")) and str(board.get("away_odds")) == str(sgo.get("away_odds")):
        return "match"
    return "price"


# ── snapshots ────────────────────────────────────────────────────────────────

def _snapshots(sport_key: str = None) -> list:
    if not DATA.exists():
        return []
    out = []
    for p in sorted(DATA.glob("*.json"), reverse=True):
        if p.name == "ledger.json":
            continue
        sk = p.stem.rsplit("_", 1)[0]
        if sport_key and sk != sport_key:
            continue
        out.append(p.name)
    return out


def _load_snapshot(name: str) -> dict:
    p = DATA / Path(name).name
    return json.loads(p.read_text())


# ── routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "sgo_compare.html")


@app.route("/api/status")
def status():
    led = _ledger()
    try:
        sports = requests.get(f"{MAIN_APP}/api/sports", timeout=10).json().get("sports", [])
        main_ok = True
    except Exception:
        sports, main_ok = list(LEAGUES), False
    return jsonify({
        "key_present": bool(_key()),
        "ledger": led,
        "monthly_objects": _monthly_objects(),
        "plan": _plan(),
        "main_app": MAIN_APP, "main_app_ok": main_ok,
        "sports": [{"key": s, "league": LEAGUES[s]} for s in sports if s in LEAGUES],
        "snapshots": _snapshots(),
    })


def _board_window(sport_key: str, hours: float):
    """Board games kicking off inside the window, and the window's end clipped
    to the board's last kickoff + 1 h. SGO lists games days before the board
    does (10/13: 97 NCAAF / 51 NHL events vs 62 / 17 on the board), and those
    cost objects without anything to compare against."""
    board = _board(sport_key)
    now = datetime.now(timezone.utc)
    end = now + timedelta(hours=hours)
    games, last = {}, None
    for r in board.get("data") or []:
        t = _parse_ts(r.get("commence_time"))
        if t and now - timedelta(hours=4) <= t <= end:
            games[r["game_id"]] = 1
            last = max(last or t, t)
    if last:
        end = min(end, last + timedelta(hours=1))
    return len(games), end, board.get("error")


@app.route("/api/estimate/<sport_key>")
def estimate(sport_key):
    """Objects a pull would cost ≈ board games kicking off inside the window."""
    n, _, err = _board_window(sport_key, float(request.args.get("hours", 168)))
    return jsonify({"estimate": n, "board_error": err})


@app.route("/api/pull", methods=["POST"])
def pull():
    body = request.get_json(silent=True) or {}
    sk = body.get("sport_key")
    league = LEAGUES.get(sk)
    if not league:
        return jsonify({"error": f"No SGO league for {sk}"}), 400
    if not _key():
        return jsonify({"error": "Add SPORTSGAMEODDS_API_KEY=... to Odds Screen/.env, then pull again."}), 400
    hours = float(body.get("hours") or 168)
    est = int(body.get("estimate") or 0)
    with _lock:
        led = _ledger()
        cap = _monthly_objects()
        left = cap - led["objects"] if cap else float("inf")
        if est > left and not body.get("force"):
            return jsonify({"error": f"Pull would cost ~{est} objects; {left} left this month. Force to override."}), 409
        now = datetime.now(timezone.utc)
        params = {"leagueID": league, "oddsAvailable": "true", "limit": PAGE_LIMIT,
                  "startsBefore": _board_window(sk, hours)[1].strftime("%Y-%m-%dT%H:%M:%SZ")}
        if body.get("alts", True):
            params["includeAltLines"] = "true"
        events, notices, reqs, cursor, t0 = [], [], 0, None, time.time()
        error = None
        for _ in range(10):
            if cursor:
                params["cursor"] = cursor
            try:
                r = _sgo_get("/events", params)
            except requests.RequestException as e:
                error = f"{type(e).__name__}: {e}"
                break
            reqs += 1
            if r.status_code != 200:
                error = f"HTTP {r.status_code}: {r.text[:300]}"
                if reqs == 1:
                    led["requests"] += 1
                    _save_ledger(led)
                break
            j = r.json()
            events += j.get("data") or []
            if j.get("notice"):
                notices.append(j["notice"])
            led["objects"] += max(1, len(j.get("data") or []))
            led["requests"] += 1
            cursor = j.get("nextCursor")
            if not cursor or not j.get("data"):
                break
        elapsed = round(time.time() - t0, 1)
        stamp = now.strftime("%Y%m%dT%H%M%S")
        snap = {"sport_key": sk, "league": league, "pulled_at": now.isoformat(), "hours": hours,
                "plan": _plan()["id"],
                "requests": reqs, "objects": len(events), "elapsed_s": elapsed,
                "notices": notices, "error": error, "params": {k: v for k, v in params.items() if k != "cursor"},
                "events": events}
        name = None
        if events:
            DATA.mkdir(parents=True, exist_ok=True)
            name = f"{sk}_{stamp}.json"
            (DATA / name).write_text(json.dumps(snap))
        led["pulls"].append({"at": now.isoformat(), "sport_key": sk, "objects": len(events),
                             "requests": reqs, "snapshot": name, "error": error})
        _save_ledger(led)
    if error and not events:
        return jsonify({"error": error, "ledger": led}), 502
    return jsonify({"snapshot": name, "objects": len(events), "requests": reqs, "elapsed_s": elapsed,
                    "notices": notices, "error": error, "ledger": led})


@app.route("/api/usage", methods=["POST"])
def usage():
    """SGO's own count (one request; may count as one object)."""
    if not _key():
        return jsonify({"error": "No key"}), 400
    with _lock:
        r = _sgo_get("/account/usage", {})
        led = _ledger()
        led["requests"] += 1
        led["objects"] += 1
        _save_ledger(led)
    try:
        return jsonify(r.json()), r.status_code
    except ValueError:
        return jsonify({"error": r.text[:300]}), r.status_code


@app.route("/api/compare/<sport_key>")
def compare(sport_key):
    snaps = _snapshots(sport_key)
    name = request.args.get("snap") or (snaps[0] if snaps else None)
    board = _board(sport_key)
    rows = board.get("data") or []
    display = _board_display()
    if not name:
        return jsonify({"snapshot": None, "snapshots": snaps, "board_error": board.get("error"),
                        "board_games": len({r["game_id"] for r in rows})})
    snap = _load_snapshot(name)
    events = snap.get("events") or []
    pulled = _parse_ts(snap.get("pulled_at"))
    matched = _match_games(rows, events)
    matched_ids = {ev["eventID"] for ev in matched.values()}
    st = board.get("source_times") or {}
    odds_api_at = _parse_ts(st.get("odds_api"))

    out_rows, book_stats, market_stats = [], {}, {}
    target_cov = {b: {} for b, _ in TARGET_BOOKS}      # book -> market -> [sgo games, board games]
    pin_cb, pin_full, pin_rows, pin_skipped = {}, {}, [], {"no_pinnacle": 0, "no_ref": 0, "diff_number": 0}
    sgo_books_all = set()

    def bstat(b):
        return book_stats.setdefault(b, {"board": 0, "sgo": 0, "match": 0, "price": 0, "line": 0,
                                         "board_only": 0, "sgo_only": 0, "sgo_newer": 0,
                                         "ages": [], "alts": 0, "price_gap": []})

    for r in rows:
        ev = matched.get(r["game_id"])
        sgo_books, fair = _sgo_market(ev, r["market"]) if ev else ({}, None)
        ms = market_stats.setdefault(r["market"], {"label": r.get("market_label") or r["market"],
                                                   "board_games": 0, "sgo_games": 0, "board_books": 0, "sgo_books": 0})
        board_books = {b: v for b, v in (r.get("books") or {}).items() if v and (v.get("home_odds") or v.get("away_odds"))}
        if board_books:
            ms["board_games"] += 1
            ms["board_books"] += len(board_books)
        if sgo_books:
            ms["sgo_games"] += 1
            ms["sgo_books"] += len(sgo_books)
        cells = {}
        for b in set(board_books) | set(sgo_books):
            bv, sv = board_books.get(b), sgo_books.get(b)
            if not ev:
                sv = None
            kind = _compare_cell(bv, sv) if ev else None
            s = bstat(b)
            if bv:
                s["board"] += 1
            if sv:
                s["sgo"] += 1
                sgo_books_all.add(b)
                s["alts"] += sv.get("alts", 0)
                u = _parse_ts(sv.get("updated"))
                if u and pulled:
                    s["ages"].append((pulled - u).total_seconds() / 60)
            if kind:
                s[kind] += 1
                if kind == "price":
                    gaps = [abs(_imp(bv.get(k)) - _imp(sv.get(k))) for k in ("home_odds", "away_odds")
                            if _imp(bv.get(k)) is not None and _imp(sv.get(k)) is not None]
                    if gaps:
                        s["price_gap"].append(max(gaps) * 100)
                if kind in ("price", "line"):
                    u = _parse_ts(sv.get("updated"))
                    # board price as of: its own stamp, else its feed's last landing, else the Odds API pull
                    ref = _parse_ts(bv.get("updated_at")) or _parse_ts(st.get(b)) or odds_api_at
                    if u and ref and u > ref:
                        s["sgo_newer"] += 1
            cells[b] = {"board": bv, "sgo": sv, "kind": kind}
        if board_books:
            for b, _ in TARGET_BOOKS:
                c = target_cov[b].setdefault(r["market"], [0, 0, 0])   # sgo, board's own, board games
                c[2] += 1
                c[0] += bool(ev and sgo_books.get(b))
                c[1] += bool(board_books.get(b))

        # Pinnacle (SGO) vs our sharp fair, same number only
        pin = sgo_books.get("pinnacle") if ev else None
        if ev and not pin:
            pin_skipped["no_pinnacle"] += 1
        elif pin:
            p_pin = _fair_home(pin)
            refs = {k: board_books[k] for k in ("circa", "betonlineag", "bookmaker") if board_books.get(k)}
            same = {k: _fair_home(v) for k, v in refs.items() if _same_number(v, pin) and _fair_home(v) is not None}
            if p_pin is None or not refs:
                pin_skipped["no_ref"] += 1
            elif not same:
                pin_skipped["diff_number"] += 1
            else:
                cb = {k: v for k, v in same.items() if k in W_CB}
                d_cb = (p_pin - _blend(cb, W_CB)) * 100 if cb else None
                d_full = (p_pin - _blend(same, _full_weights(set(same)))) * 100
                if d_cb is not None:
                    pin_cb.setdefault(r["market"], []).append(d_cb)
                pin_full.setdefault(r["market"], []).append(d_full)
                pin_rows.append({"game": f"{r['away_team']} @ {r['home_team']}", "market": r["market"],
                                 "label": r.get("market_label") or r["market"], "point": pin.get("home_point"),
                                 "pinnacle": [pin.get("away_odds"), pin.get("home_odds")],
                                 "pin_fair": round(p_pin * 100, 1),
                                 "refs": {k: [refs[k].get("away_odds"), refs[k].get("home_odds")] for k in same},
                                 "diff_cb_pp": None if d_cb is None else round(d_cb, 2),
                                 "diff_full_pp": round(d_full, 2), "pin_updated": pin.get("updated")})

        out_rows.append({"game_id": r["game_id"], "away": r["away_team"], "home": r["home_team"],
                         "commence": r.get("commence_time"), "market": r["market"],
                         "matched": bool(ev), "sgo_event": ev["eventID"] if ev else None,
                         "cells": cells, "fair": fair})

    # what SGO carries beyond the board's markets
    extra = {}
    for ev in events:
        for oid, o in (ev.get("odds") or {}).items():
            if o.get("playerID"):
                k = "player props"
            elif o.get("statEntityID") in ("home", "away") and o.get("betTypeID") == "ou":
                k = f"team totals ({o.get('periodID')})"
            else:
                k = f"{o.get('statID')} · {o.get('periodID')} · {o.get('betTypeID')}"
            extra.setdefault(k, set()).add(ev["eventID"])
    extra = sorted(({"market": k, "events": len(v)} for k, v in extra.items()), key=lambda x: (-x["events"], x["market"]))

    books = []
    for b, s in sorted(book_stats.items(), key=lambda kv: (-(kv[1]["board"] + kv[1]["sgo"]), kv[0])):
        compared = s["match"] + s["price"] + s["line"]
        books.append({
            "key": b, "display": display.get(b) or SGO_DISPLAY.get(b) or b.replace("_", " ").title(),
            "board_source": (BOARD_SOURCE.get(b, DEFAULT_SOURCE) if s["board"] else None),
            "board_rows": s["board"], "sgo_rows": s["sgo"],
            "compared": compared, "match": s["match"], "price": s["price"], "line": s["line"],
            "board_only": s["board_only"], "sgo_only": s["sgo_only"], "sgo_newer": s["sgo_newer"],
            "match_pct": round(100 * s["match"] / compared, 1) if compared else None,
            "median_gap_pp": round(statistics.median(s["price_gap"]), 2) if s["price_gap"] else None,
            "median_age_min": round(statistics.median(s["ages"]), 1) if s["ages"] else None,
            "alts": s["alts"],
        })

    board_games = {r["game_id"]: r for r in rows}
    return jsonify({
        "snapshot": name, "snapshots": snaps,
        "meta": {k: snap.get(k) for k in ("league", "pulled_at", "hours", "requests", "objects",
                                           "elapsed_s", "notices", "error")},
        "board_error": board.get("error"), "source_times": st,
        "games": {
            "board": len(board_games), "sgo": len(events), "matched": len(matched),
            "board_only": sorted(f"{r['away_team']} @ {r['home_team']}" for g, r in board_games.items() if g not in matched),
            "sgo_only": sorted(_ev_label(ev) for ev in events if ev["eventID"] not in matched_ids),
        },
        "books": books,
        "markets": [{"market": k, **v} for k, v in market_stats.items()],
        "extra_markets": extra,
        "rows": out_rows,
        "sharp": list(SHARP),
        "plan": snap.get("plan") or "amateur", "plan_now": _plan(),
        "target_coverage": [
            {"key": b, "display": name,
             "markets": [{"market": m, "label": (market_stats.get(m) or {}).get("label", m),
                          "sgo": c[0], "board": c[1], "games": c[2],
                          "sgo_pct": round(100 * c[0] / c[2], 1) if c[2] else None,
                          "board_pct": round(100 * c[1] / c[2], 1) if c[2] else None}
                         for m, c in target_cov[b].items()]}
            for b, name in TARGET_BOOKS],
        "pinnacle": {
            "vs_circa_bol": {"overall": _pp_stats([d for v in pin_cb.values() for d in v]),
                             "by_market": [{"market": m, "label": (market_stats.get(m) or {}).get("label", m), **_pp_stats(v)}
                                           for m, v in pin_cb.items()]},
            "vs_full_sharp": {"overall": _pp_stats([d for v in pin_full.values() for d in v]),
                              "by_market": [{"market": m, "label": (market_stats.get(m) or {}).get("label", m), **_pp_stats(v)}
                                            for m, v in pin_full.items()]},
            "skipped": pin_skipped,
            "biggest": sorted(pin_rows, key=lambda x: -abs(x["diff_cb_pp"] if x["diff_cb_pp"] is not None else x["diff_full_pp"]))[:25],
        },
    })


if __name__ == "__main__":
    print(f"SGO test bench on http://127.0.0.1:{PORT}  (board from {MAIN_APP})")
    app.run(port=PORT, debug=False, threaded=True)
