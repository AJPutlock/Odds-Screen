"""
Paper auto-betting (10/14): the bets a NoVig / Kalshi / Polymarket auto-bettor
WOULD place, chosen on the server from the merged in-memory board, logged with
a realistic fill and graded on after-fee CLV. Step (i) of the auto-betting plan
agreed with the user and the Senior session; the go / no-go for a live NoVig
pilot (step ii) is read off these rows (GO_CRITERIA, user signs off).

PLACES NOTHING. This module never imports novig_orders or novig_client, never
calls an order or order-preview endpoint, and reads NoVig depth only from the
streamed book (scrapers.novig_ws.levels). tests/test_auto_paper.py checks all
three. It writes only its own tables (auto_paper, auto_paper_parity) in
bets.db: never the bets table, so tracker P/L and the screen's Kelly are
untouched.

Selection mirrors the page's scanEV for MAIN lines priced off REAL sharp
prices only (no outcome model, no Bookmaker-ladder extrapolation, no FG shift):
  * pregame, under AUTO_MAX_HOURS to start (started games never qualify);
  * book in AUTO_BOOKS, its main line (row.books[bk]), not stale;
  * fair = the blended sharp no-vig at the anchor (computeSharpNoVig) when the
    book's point is the anchor's, else the sharp curve at the book's point
    when that is inside the real prices and within MAX_LINE_GAP;
  * edge = fair - implied(board odds) >= evMinEdge (time tier + Overs bump).
The constants below are twins of index.html's (named the same) and the test
parses index.html and asserts they're equal, so the two can't drift silently.

Gated by AUTO_PAPER_ENABLED=1 in .env (read every tick; the user sets it).
Zero Odds API credits: it reads only the merged caches (odds-api.io Circa /
BetOnline, Bookmaker pulls, the exchange feeds) via app._inject_scraped.
"""
import json
import math
import os
import re
import sqlite3
import statistics
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import dotenv_values
from flask import Blueprint, jsonify, request
from zoneinfo import ZoneInfo

try:
    from scrapers import novig_ws as ws      # streamed NoVig books (read key only)
except Exception:                            # pragma: no cover - app runs without NoVig
    ws = None

_HERE = Path(__file__).resolve().parent
DB_PATH = _HERE / "data" / "bets.db"
STATE_PATH = _HERE / "data" / "auto_paper_state.json"
ENV_PATH = _HERE / ".env"
RULE_VERSION = "2026-10-14.1"
ET = ZoneInfo("America/New_York")

# ── twins of static/index.html (tests/test_auto_paper.py asserts equality) ──
EV_MIN_EDGE = 0.01
# Flat since 10/14 (Senior's replay: the 2.5/2.0/1.5 tiers dropped 133 of 184
# CLV-graded bets that still averaged +1.39% CLV); sizing shrinks instead.
EV_TIME_TIERS = ((-math.inf, EV_MIN_EDGE),)
EV_OVER_EXTRA = 0.005
STALE_SOURCE_HOURS = 2
MIN_BEST_ODDS = -150
FULL_GAME_MARKETS = frozenset({"h2h", "spreads", "totals"})
SHARP_WEIGHTS_3 = {"circa": 0.50, "bookmaker": 0.30, "betonlineag": 0.20}      # sharpWeights, all three
SHARP_WEIGHTS_CB = {"circa": 0.6, "bookmaker": 0.4, "betonlineag": 0}          # Circa + Bookmaker
SHARP_WEIGHTS_PAIR_MAIN, SHARP_WEIGHTS_PAIR_BOL = 0.7, 0.3                    # Circa or Bookmaker + BetOnline
P_CLAMP = (0.015, 0.985)
# Bookmaker's weight in the LEVEL fades with its pull age (bkFadeFactor / bkAgeFade).
BK_FRESH_MIN, BK_FADE_END_MIN, BK_FADE_FLOOR, BK_FADE_STALE = 10, 20, 0.15, 0.05
# Kelly on a shrunk edge (kellyShrink): the stake, never the displayed edge.
KELLY_SHRINK_EXCHANGE, KELLY_SHRINK_REC = 0.60, 0.45
KELLY_EXCHANGES = frozenset({"novig", "kalshi", "polymarket"})

# ── paper rules ──────────────────────────────────────────────────────────────
AUTO_BOOKS = ("novig", "kalshi", "polymarket")
AUTO_SPORTS = ("americanfootball_nfl", "americanfootball_ncaaf", "icehockey_nhl")
AUTO_MAX_HOURS = 6.0
# Period markets allowed on top of full game. NCAAF periods are out: the page
# shifts them by Circa's full game (fbPeriodFgAdj), which this port leaves out.
PERIOD_MARKETS = {"americanfootball_nfl": frozenset({"spreads_h1", "totals_h1"})}
MAX_LINE_GAP = 1.0          # book point vs anchor, real-price interpolation only
SHRINK = KELLY_SHRINK_EXCHANGE   # every AUTO_BOOKS book is an exchange (user approved 10/14)
DEFAULT_KELLY_UNIT = 500.0  # bankroll x Kelly fraction; the page posts its own (parity tick)
MIN_STAKE = 1.0
DEFAULT_CAPS = {"AUTO_PAPER_MAX_BET": 25.0, "AUTO_PAPER_MAX_GAME": 50.0, "AUTO_PAPER_MAX_DAY": 100.0}
TICK_SECONDS = 10
SELECT_SECONDS = 30
GRADE_SECONDS = 15 * 60
RECHECKS = ((30, "price_t30"), (120, "price_t120"))
PARITY_MAX_AGE = 180        # server fair must be this fresh to pair with a page post
PARITY_EVERY = 600          # one parity row per key per 10 min
GO_CRITERIA = {"n": 100, "clv": 0.010, "clv_se": 0.005, "slip_pp": 0.005, "t30": 0.70, "parity_pp": 0.002}

_lock = threading.Lock()
_deps = {}                  # set by init(): inject, caches, cache_lock
_server_fair = {}           # parity key -> (fair, epoch)
_rechecks = []              # [(due_epoch, row_id, field, ident)]
_status = {"started": None, "last_tick": None, "last_select": None, "last_grade": None,
           "skips": {}, "errors": [], "selected": 0}


def init(inject, source_times, caches, cache_lock):
    """app.py hands over its board accessors (no import of app, no cycle)."""
    _deps.update(inject=inject, source_times=source_times, caches=caches, cache_lock=cache_lock)
    _ensure_tables()


# ── odds math (twins of index.html) ─────────────────────────────────────────

def american_to_implied(odds):
    """americanToImplied: parseInt semantics ("+130" -> 130)."""
    m = re.match(r"\s*([+-]?\d+)", str(odds)) if odds is not None else None
    if not m:
        return None
    n = int(m.group(1))
    return abs(n) / (abs(n) + 100) if n < 0 else 100 / (n + 100)


def passes_odds_floor(odds):
    m = re.match(r"\s*([+-]?\d+)", str(odds).replace("+", "")) if odds is not None else None
    return bool(m) and int(m.group(1)) >= MIN_BEST_ODDS


def implied_to_american(p):
    if p is None or not (0 < p < 1):
        return None
    return round(-100 * p / (1 - p)) if p > 0.5 else round(100 / p - 100)


def devig_shin(pa, pb):
    s = pa + pb
    if not s > 1:
        return pa / s, pb / s
    f = lambda q, z: (math.sqrt(z * z + 4 * (1 - z) * q * q / s) - z) / (2 * (1 - z))
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


def sharp_weights(has):
    n = sum(1 for k in ("circa", "bookmaker", "betonlineag") if has.get(k))
    if n == 1:
        return {k: float(bool(has.get(k))) for k in ("circa", "bookmaker", "betonlineag")}
    if n == 3:
        return dict(SHARP_WEIGHTS_3)
    if has.get("circa") and has.get("bookmaker"):
        return dict(SHARP_WEIGHTS_CB)
    return {"circa": SHARP_WEIGHTS_PAIR_MAIN if has.get("circa") else 0,
            "bookmaker": SHARP_WEIGHTS_PAIR_MAIN if has.get("bookmaker") else 0,
            "betonlineag": SHARP_WEIGHTS_PAIR_BOL}


def bk_fade_factor(age_min):
    if age_min is None or not math.isfinite(age_min) or age_min > 60:
        return BK_FADE_STALE
    if age_min <= BK_FRESH_MIN:
        return 1.0
    if age_min >= BK_FADE_END_MIN:
        return BK_FADE_FLOOR
    return 1 - (1 - BK_FADE_FLOOR) * (age_min - BK_FRESH_MIN) / (BK_FADE_END_MIN - BK_FRESH_MIN)


def bk_age_fade(w, has, times, now):
    """bkAgeFade: Bookmaker's weight x the fade factor, the others renormalised;
    a Bookmaker-only market keeps it."""
    if not has.get("bookmaker") or not (has.get("circa") or has.get("betonlineag")):
        return w
    t = _parse(times.get("bookmaker"))
    f = bk_fade_factor((now - t).total_seconds() / 60 if t else None)
    if f >= 1:
        return w
    rest = 1 - w["bookmaker"]
    if not rest > 0:
        return w
    nb = w["bookmaker"] * f
    k = (1 - nb) / rest
    return {"circa": w["circa"] * k, "bookmaker": nb, "betonlineag": w["betonlineag"] * k}


def ev_time_floor(hrs):
    if hrs is None or not math.isfinite(hrs):
        return EV_MIN_EDGE
    return next(floor for h, floor in EV_TIME_TIERS if hrs >= h)


def ev_min_edge(mkt, hrs, side):
    """evMinEdge for a main line on real prices (isAlt false, backing 'real')."""
    m = ev_time_floor(hrs)
    if side == "upper" and mkt.startswith("totals"):
        m += EV_OVER_EXTRA
    return m


def _parse(ts):
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, timezone.utc)
    try:
        d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def is_source_stale(times, source, mkt, row, now):
    """isSourceStale with the board's source_times dict."""
    hrs = timedelta(hours=STALE_SOURCE_HOURS)
    period = mkt not in FULL_GAME_MARKETS
    row_ref = row.get("periods_pulled_at") if period else None
    if row_ref and not times.get(source):
        r = _parse(row_ref)
        return r is not None and now - r > hrs
    ref = row_ref or (times.get("odds_api_periods") if period and times.get("odds_api_periods") else times.get("odds_api"))
    if not ref or not times.get(source):
        return False
    a, b = _parse(times[source]), _parse(ref)
    return a is not None and b is not None and abs(a - b) > hrs


def book_sides(e, totals):
    if totals:
        return {"u": e.get("home_odds"), "l": e.get("away_odds"), "uPt": e.get("home_point"), "lPt": e.get("away_point"),
                "uProb": e.get("home_prob"), "lProb": e.get("away_prob"), "uRef": e.get("home_ref"), "lRef": e.get("away_ref"),
                "coef": e.get("_coef"), "uUsd": e.get("home_usd"), "lUsd": e.get("away_usd")}
    return {"u": e.get("away_odds"), "l": e.get("home_odds"), "uPt": e.get("away_point"), "lPt": e.get("home_point"),
            "uProb": e.get("away_prob"), "lProb": e.get("home_prob"), "uRef": e.get("away_ref"), "lRef": e.get("home_ref"),
            "coef": e.get("_coef"), "uUsd": e.get("away_usd"), "lUsd": e.get("home_usd")}


def full_bookmaker_ladder(row, mkt):
    ladder = list(row.get("bookmaker_ladder") or [])
    main = (row.get("books") or {}).get("bookmaker")
    if not main:
        return ladder
    totals = mkt.startswith("totals")
    main_pt = main.get("home_point") if totals else main.get("away_point")
    if main_pt is None:
        return ladder
    if any(abs(((e.get("point") if totals else e.get("away_point")) or 0) - main_pt) < 1e-6
           and (e.get("point") if totals else e.get("away_point")) is not None for e in ladder):
        return ladder
    ladder.append({"point": main.get("home_point"), "over_odds": main.get("home_odds"), "under_odds": main.get("away_odds")}
                  if totals else {"away_point": main.get("away_point"), "home_point": main.get("home_point"),
                                  "away_odds": main.get("away_odds"), "home_odds": main.get("home_odds")})
    return ladder


def curve_real(pts, x):
    """curveValue inside the real prices only: (fair, exact) or None outside
    (the page extends with the outcome model there; this port skips)."""
    if not pts:
        return None
    if pts[0]["pt"] - 1e-9 <= x <= pts[-1]["pt"] + 1e-9:
        for i, p in enumerate(pts):
            if abs(p["pt"] - x) < 1e-9:
                return p["fair"], True
            if p["pt"] > x:
                a, b = pts[i - 1], p
                t = (x - a["pt"]) / (b["pt"] - a["pt"])
                return a["fair"] + t * (b["fair"] - a["fair"]), False
    return None


def build_curve(row, mkt, times, now):
    """buildSharpCurveRaw. None when no sharp book, or when the page would need
    its outcome model to set the level (an offset point outside the ladder)."""
    totals, ml = mkt.startswith("totals"), mkt.startswith("h2h")
    books = row.get("books") or {}

    def fair_u(u, l):
        a, b = american_to_implied(u), american_to_implied(l)
        return None if a is None or b is None else devig_shin(a, b)[0]

    def main(key):
        e = books.get(key)
        if not e or is_source_stale(times, key, mkt, row, now):
            return None
        s = book_sides(e, totals)
        pt = 0 if ml else s["uPt"]
        f = fair_u(s["u"], s["l"]) if (s["u"] and s["l"]) else None
        return None if pt is None or f is None else {"pt": pt, "fair": f}

    bk = []
    if is_source_stale(times, "bookmaker", mkt, row, now):
        pass
    elif ml:
        m = main("bookmaker")
        if m:
            bk = [m]
    else:
        for e in full_bookmaker_ladder(row, mkt):
            pt = e.get("point") if totals else e.get("away_point")
            ok = (e.get("over_odds") and e.get("under_odds")) if totals else (e.get("away_odds") and e.get("home_odds"))
            f = fair_u(e.get("over_odds") if totals else e.get("away_odds"),
                       e.get("under_odds") if totals else e.get("home_odds")) if ok else None
            if pt is not None and f is not None and not any(p["pt"] == pt for p in bk):
                bk.append({"pt": pt, "fair": f})
        bk.sort(key=lambda p: p["pt"])
    others = {"circa": main("circa"), "betonlineag": main("betonlineag")}
    has = {"circa": bool(others["circa"]), "bookmaker": len(bk) > 0, "betonlineag": bool(others["betonlineag"])}
    if not any(has.values()):
        return None
    w = bk_age_fade(sharp_weights(has), has, times, now)
    used = [k for k in has if has[k]]
    if len(bk) >= 2:
        sum_w, sum_off = w["bookmaker"], 0.0
        for k in ("circa", "betonlineag"):
            o = others[k]
            if not o:
                continue
            base = curve_real(bk, o["pt"])
            if base is None:
                return None          # page: model-shifted offset; out of scope here
            sum_w += w[k]
            sum_off += w[k] * (o["fair"] - base[0])
        delta = sum_off / sum_w if sum_w > 0 else 0.0
        return {"pts": [{"pt": p["pt"], "fair": p["fair"] + delta} for p in bk], "ladder": True, "books": used, "weights": w}
    by_pt = {}
    for k, p in (("circa", others["circa"]), ("bookmaker", bk[0] if bk else None), ("betonlineag", others["betonlineag"])):
        if not p:
            continue
        cur = by_pt.setdefault(p["pt"], [0.0, 0.0])
        cur[0] += w[k]
        cur[1] += w[k] * p["fair"]
    pts = sorted(({"pt": pt, "fair": v[1] / v[0]} for pt, v in by_pt.items()), key=lambda p: p["pt"])
    return {"pts": pts, "ladder": False, "books": used, "weights": w}


def sharp_no_vig(row, mkt, times, now, curve):
    """computeSharpNoVig: (pUpper, anchor point on the upper axis) or None."""
    totals, ml = mkt.startswith("totals"), mkt.startswith("h2h")
    books = row.get("books") or {}

    def main_pt(key):
        e = books.get(key)
        if not e or is_source_stale(times, key, mkt, row, now):
            return None
        s = book_sides(e, totals)
        return s["uPt"] if (s["u"] and s["l"]) else None

    if ml:
        anchor = 0
    elif main_pt("circa") is not None:
        anchor = main_pt("circa")
    elif main_pt("bookmaker") is not None:
        anchor = main_pt("bookmaker")
        bol = main_pt("betonlineag")
        if bol is not None and bol != anchor and curve["ladder"]:
            best = abs(anchor - bol)
            for p in curve["pts"]:
                d = abs(p["pt"] - bol)
                if d < best:
                    anchor, best = p["pt"], d
    else:
        anchor = main_pt("betonlineag")
    if anchor is None:
        return None
    v = curve_real(curve["pts"], anchor)
    if v is None:
        return None
    return min(P_CLAMP[1], max(P_CLAMP[0], v[0])), anchor


def side_fair(curve, nv, mkt, side, pt):
    """softSideFair for a main line on real prices: (fair, exact) or None."""
    p_upper, anchor = nv
    totals, ml = mkt.startswith("totals"), mkt.startswith("h2h")
    anchor_side = None if ml else (anchor if (totals or side == "upper") else -anchor)
    at_anchor = p_upper if side == "upper" else 1 - p_upper
    if ml or pt is None or anchor_side is None or pt == anchor_side:
        return at_anchor, True
    if abs(pt - anchor_side) > MAX_LINE_GAP + 1e-9:
        return None
    flip = not totals and side == "lower"
    v = curve_real(curve["pts"], -pt if flip else pt)
    if v is None:
        return None
    fu = min(P_CLAMP[1], max(P_CLAMP[0], v[0]))
    return (fu if side == "upper" else 1 - fu), v[1]


def _hours_to(ct, now):
    k = _parse(ct)
    return None if k is None else (k - now).total_seconds() / 3600


# ── storage ─────────────────────────────────────────────────────────────────

COLUMNS = """
    id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, created_at TEXT, last_seen TEXT, n_seen INTEGER,
    rule_version TEXT, sport_key TEXT, event_id TEXT, away TEXT, home TEXT, commence_time TEXT,
    market TEXT, side TEXT, team TEXT, point REAL, book_key TEXT,
    odds INTEGER, ask REAL, fair_prob REAL, fair_exact INTEGER, edge_raw REAL, edge_shrunk REAL, min_edge REAL,
    qualifies_shrunk INTEGER, hours_to_kick REAL, sharp_books TEXT,
    kelly_unit REAL, stake REAL, held_before REAL, over_cap INTEGER,
    fill_status TEXT, fill_price REAL, fill_odds INTEGER, fill_decimal REAL, fee_usd REAL, fee_pp REAL,
    depth_usd REAL, slip_pp REAL,
    price_t30 TEXT, price_t120 TEXT,
    status TEXT, profit REAL, result_detail TEXT, graded_at TEXT,
    close_fair_bol REAL, clv_bol REAL, clv_bol_source TEXT,
    close_fair_circa REAL, clv_circa REAL, clv_circa_source TEXT, clv_at TEXT
"""


def _connect():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _ensure_tables():
    with _connect() as c:
        c.execute(f"CREATE TABLE IF NOT EXISTS auto_paper ({COLUMNS})")
        c.execute("""CREATE TABLE IF NOT EXISTS auto_paper_parity (
                     id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT, key TEXT, sport_key TEXT, market TEXT,
                     book_key TEXT, page_fair REAL, server_fair REAL, diff REAL)""")


def _env():
    try:
        return dotenv_values(ENV_PATH)
    except Exception:
        return {}


def enabled() -> bool:
    return str(_env().get("AUTO_PAPER_ENABLED") or os.environ.get("AUTO_PAPER_ENABLED") or "").strip() == "1"


def _caps():
    env = _env()
    out = {}
    for k, d in DEFAULT_CAPS.items():
        try:
            out[k] = float(env.get(k) or d)
        except ValueError:
            out[k] = d
    return out


def _load_state():
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}


def kelly_unit() -> float:
    v = _env().get("AUTO_PAPER_KELLY_UNIT") or _load_state().get("kelly_unit")
    try:
        v = float(v)
        return v if v > 0 else DEFAULT_KELLY_UNIT
    except (TypeError, ValueError):
        return DEFAULT_KELLY_UNIT


def kelly_stake(p, odds, unit):
    """kellyStakeDollars with p = the (shrunk) fair."""
    m = re.match(r"\s*([+-]?\d+)", str(odds))
    if not m or not (0 < p < 1):
        return 0.0
    n = int(m.group(1))
    b = n / 100 if n > 0 else 100 / abs(n)
    f = p - (1 - p) / b
    return max(0.0, unit * min(f, 1.0)) if f > 0 else 0.0


# ── fills (read-only) ───────────────────────────────────────────────────────

def novig_fill(ref, other_ref, limit, stake):
    """FOK buy simulated on the STREAMED book: asks for this outcome are
    1 - bids on the other outcome (complementary books); qty in 1c contracts.
    Pregame game lines carry no NoVig fee. Never touches REST or orders."""
    if ws is None or not ref or not other_ref:
        return {"fill_status": "no_stream"}
    market_id, _ = ref.split("|", 1)
    other_id = other_ref.split("|", 1)[1]
    levels = ws.levels(market_id)
    if levels is None:
        return {"fill_status": "no_stream"}
    asks = sorted((round(1 - p, 3), n) for p, n in levels.get(other_id) or [])
    qty = int(stake / (limit * 0.01)) if limit else 0
    if qty < 1:
        return {"fill_status": "too_small"}
    need, cost, depth = qty, 0.0, 0.0
    for a, n in asks:
        if a > limit + 1e-9:
            break
        depth += a * n * 0.01
        take = min(n, need)
        cost += a * take * 0.01
        need -= take
    if need > 0:
        return {"fill_status": "short", "depth_usd": round(depth, 2)}
    price = cost / (qty * 0.01)
    return {"fill_status": "filled", "fill_price": price, "fee_usd": 0.0, "fee_pp": 0.0,
            "depth_usd": round(depth, 2), "stake": round(cost, 2)}


def exchange_fill(odds, ask, usd, stake):
    """Kalshi / Polymarket: board odds are already net of the taker fee
    (1 / (ask + fee)). Fills at the board price when the size at the best ask
    covers the stake; deeper levels aren't in the board, so bigger = short."""
    net = american_to_implied(odds)
    if net is None:
        return {"fill_status": "no_price"}
    if usd is None:
        return {"fill_status": "unchecked"}
    if usd < stake:
        return {"fill_status": "short", "depth_usd": round(usd, 2)}
    fee_pp = max(0.0, net - ask) if ask else 0.0
    return {"fill_status": "filled", "fill_price": net, "fee_pp": fee_pp,
            "fee_usd": round(stake / net * fee_pp, 4) if net else 0.0, "depth_usd": round(usd, 2), "stake": stake}


# ── selection ───────────────────────────────────────────────────────────────

def _held(rows, group_team, group_dir):
    held = 0.0
    for r in rows:
        mk = r["market"] or ""
        if group_dir and mk.startswith("totals") and r["side"] == group_dir:
            held += r["stake"] or 0
        elif group_team and not mk.startswith("totals") and r["team"] == group_team:
            held += r["stake"] or 0
    return held


def held_in_group(c, event_id, team, bet_side):
    """kellyShares' cap group: stake already held on the same team (spread/ML,
    any period) or the same total direction, real open bets + open paper rows."""
    group_dir = bet_side if team is None else None
    real = c.execute("SELECT market, side, team, stake FROM bets WHERE status='open' AND event_id=?", (event_id,)).fetchall()
    paper = c.execute("SELECT market, side, team, stake FROM auto_paper WHERE fill_status='filled' AND status='open' "
                      "AND over_cap=0 AND event_id=?", (event_id,)).fetchall()
    return _held(real, team, group_dir) + _held(paper, team, group_dir)


def evaluate_row(sk, row, times, now):
    """Every exchange main-line quote on one row: [(quote dict)] with the
    server fair, whether it qualifies, and why not. Pure: no DB, no I/O."""
    out = []
    mkt = row.get("market") or ""
    if mkt not in FULL_GAME_MARKETS and mkt not in PERIOD_MARKETS.get(sk, ()):
        return out
    hrs = _hours_to(row.get("commence_time"), now)
    if hrs is None or hrs <= 0:
        return out                                   # started (or no start time): never
    curve = build_curve(row, mkt, times, now)
    nv = sharp_no_vig(row, mkt, times, now, curve) if curve else None
    totals = mkt.startswith("totals")
    for bk in AUTO_BOOKS:
        e = (row.get("books") or {}).get(bk)
        if not e or is_source_stale(times, bk, mkt, row, now):
            continue
        s = book_sides(e, totals)
        for side in ("upper", "lower"):
            odds, pt = s["u" if side == "upper" else "l"], s["uPt" if side == "upper" else "lPt"]
            q = {"sk": sk, "row": row, "bk": bk, "side": side, "odds": odds, "pt": pt, "hrs": hrs, "mkt": mkt,
                 "ask": s["uProb" if side == "upper" else "lProb"], "ref": s["uRef" if side == "upper" else "lRef"],
                 "other_ref": s["lRef" if side == "upper" else "uRef"], "usd": s["uUsd" if side == "upper" else "lUsd"],
                 "books": curve["books"] if curve else []}
            if not odds or not passes_odds_floor(odds):
                q["why"] = "odds_floor"
            elif not curve or not nv:
                q["why"] = "no_real_fair"
            else:
                sf = side_fair(curve, nv, mkt, side, pt)
                impl = american_to_implied(odds)
                if sf is None or impl is None:
                    q["why"] = "no_real_fair"
                else:
                    q["fair"], q["exact"] = sf
                    q["edge"] = q["fair"] - impl
                    q["min_edge"] = ev_min_edge(mkt, hrs, side)
                    q["why"] = ("too_early" if hrs > AUTO_MAX_HOURS
                                else None if q["edge"] >= q["min_edge"] else "under_min")
            out.append(q)
    return out


def _parity_key(sk, game_id, mkt, side, pt, bk):
    return f"{sk}|{game_id}|{mkt}|{side}|{'' if pt is None else float(pt)}|{bk}"


def _bet_side(mkt, side):
    return ("over" if side == "upper" else "under") if mkt.startswith("totals") else ("away" if side == "upper" else "home")


def record(q, now, conn=None):
    """Turn a qualifying quote into a would-bet row (first sighting) or bump
    last_seen. Returns the new row id or None."""
    row, mkt, side, bk, pt = q["row"], q["mkt"], q["side"], q["bk"], q["pt"]
    key = f"{row.get('game_id')}|{mkt}|{side}|{'' if pt is None else float(pt)}|{bk}"
    own = conn is None
    c = conn or _connect()
    try:
        cur = c.execute("SELECT id FROM auto_paper WHERE key=?", (key,)).fetchone()
        iso = now.isoformat(timespec="seconds")
        if cur:
            c.execute("UPDATE auto_paper SET last_seen=?, n_seen=n_seen+1 WHERE id=?", (iso, cur["id"]))
            c.commit()
            return None
        caps, unit = _caps(), kelly_unit()
        impl = american_to_implied(q["odds"])
        edge_shrunk = q["edge"] * SHRINK
        kelly = kelly_stake(impl + edge_shrunk, q["odds"], unit)
        team = None if mkt.startswith("totals") else (row.get("away_team") if side == "upper" else row.get("home_team"))
        held = held_in_group(c, row.get("game_id"), team, _bet_side(mkt, side))
        stake = min(max(0.0, kelly - held), caps["AUTO_PAPER_MAX_BET"])
        stake = math.floor(stake * 100) / 100
        game_used = c.execute("SELECT COALESCE(SUM(stake),0) FROM auto_paper WHERE fill_status='filled' AND over_cap=0 "
                              "AND event_id=?", (row.get("game_id"),)).fetchone()[0] or 0
        et_midnight = now.astimezone(ET).replace(hour=0, minute=0, second=0, microsecond=0)
        day_used = c.execute("SELECT COALESCE(SUM(stake),0) FROM auto_paper WHERE fill_status='filled' AND over_cap=0 "
                             "AND created_at>=?", (et_midnight.astimezone(timezone.utc).isoformat(timespec="seconds"),)).fetchone()[0] or 0
        if stake < MIN_STAKE:
            _skip("stake_under_min" if held <= 0 else "held_kelly")
            return None
        over_cap = int(game_used + stake > caps["AUTO_PAPER_MAX_GAME"] or day_used + stake > caps["AUTO_PAPER_MAX_DAY"])
        if q["bk"] == "novig":
            fill = novig_fill(q["ref"], q["other_ref"], q["ask"], stake) if q["ask"] else {"fill_status": "no_price"}
        else:
            fill = exchange_fill(q["odds"], q["ask"], q["usd"], stake)
        fp = fill.get("fill_price")
        rec = {
            "key": key, "created_at": iso, "last_seen": iso, "n_seen": 1, "rule_version": RULE_VERSION,
            "sport_key": q["sk"], "event_id": row.get("game_id"), "away": row.get("away_team"), "home": row.get("home_team"),
            "commence_time": row.get("commence_time"), "market": mkt, "side": _bet_side(mkt, side), "team": team,
            "point": pt, "book_key": bk, "odds": int(re.match(r"\s*([+-]?\d+)", str(q["odds"])).group(1)),
            "ask": q["ask"], "fair_prob": round(q["fair"], 6), "fair_exact": int(bool(q.get("exact"))),
            "edge_raw": round(q["edge"], 6), "edge_shrunk": round(edge_shrunk, 6), "min_edge": q["min_edge"],
            "qualifies_shrunk": int(edge_shrunk >= q["min_edge"]), "hours_to_kick": round(q["hrs"], 3),
            "sharp_books": json.dumps(q.get("books") or []), "kelly_unit": unit,
            "stake": fill.get("stake", stake), "held_before": round(held, 2), "over_cap": over_cap,
            "fill_status": fill["fill_status"], "fill_price": fp,
            "fill_odds": implied_to_american(fp) if fp else None, "fill_decimal": (1 / fp) if fp else None,
            "fee_usd": fill.get("fee_usd"), "fee_pp": fill.get("fee_pp"), "depth_usd": fill.get("depth_usd"),
            "slip_pp": round(fp - impl, 6) if fp and impl else None,
            "status": "open" if fill["fill_status"] == "filled" else "not_filled",
        }
        cols = ",".join(rec)
        new_id = c.execute(f"INSERT INTO auto_paper ({cols}) VALUES ({','.join('?' * len(rec))})", tuple(rec.values())).lastrowid
        c.commit()
    finally:
        if own:
            c.close()
    ident = (q["sk"], row.get("game_id"), mkt, side, pt, bk)
    with _lock:
        for delay, field in RECHECKS:
            _rechecks.append((time.time() + delay, new_id, field, ident))
    return new_id


def _skip(why):
    with _lock:
        _status["skips"][why] = _status["skips"].get(why, 0) + 1


def _board(sk):
    """The merged board rows (inject = app._inject_scraped: 0 credits)."""
    with _deps["cache_lock"]:
        loaded = bool(_deps["caches"][sk].get("data"))
    if not loaded:
        return None, None
    _, rows, _ = _deps["inject"](sk)
    times = _deps["source_times"](sk)
    return rows, times


def select_once(now=None):
    now = now or datetime.now(timezone.utc)
    made = 0
    for sk in AUTO_SPORTS:
        try:
            rows, times = _board(sk)
            if not rows:
                continue
            for row in list(rows):
                try:
                    for q in evaluate_row(sk, row, times, now):
                        if q.get("fair") is not None:
                            with _lock:
                                _server_fair[_parity_key(sk, row.get("game_id"), q["mkt"], q["side"], q["pt"], q["bk"])] = (q["fair"], time.time())
                        if q["why"]:
                            _skip(q["why"])
                            continue
                        if record(q, now):
                            made += 1
                except Exception as e:      # one bad row can't stop the sport
                    _error(f"{sk} row {row.get('game_id')} {row.get('market')}: {type(e).__name__}: {e}")
        except Exception as e:              # one bad sport can't stop the loop
            _error(f"{sk}: {type(e).__name__}: {e}")
    with _lock:
        _status["last_select"] = now.isoformat(timespec="seconds")
        _status["selected"] += made
    return made


def _error(msg):
    print(f"[auto-paper] {msg}", flush=True)
    with _lock:
        _status["errors"] = (_status["errors"] + [f"{datetime.now(timezone.utc):%H:%M:%S} {msg}"])[-20:]


def _current_odds(ident):
    """The book's current main-line odds at the same point, or 'gone'."""
    sk, game_id, mkt, side, pt, bk = ident
    with _deps["cache_lock"]:
        rows = list(_deps["caches"][sk].get("data") or [])
    for row in rows:
        if row.get("game_id") == game_id and row.get("market") == mkt:
            e = (row.get("books") or {}).get(bk)
            if not e:
                return "gone"
            s = book_sides(e, mkt.startswith("totals"))
            cur_pt = s["uPt" if side == "upper" else "lPt"]
            odds = s["u" if side == "upper" else "l"]
            if odds and (pt is None or (cur_pt is not None and abs(float(cur_pt) - float(pt)) < 1e-9)):
                return str(odds)
            return "gone"
    return "gone"


def run_rechecks(now_epoch=None):
    now_epoch = now_epoch or time.time()
    with _lock:
        due = [r for r in _rechecks if r[0] <= now_epoch]
        _rechecks[:] = [r for r in _rechecks if r[0] > now_epoch]
    if not due:
        return 0
    with _connect() as c:
        for _, rid, field, ident in due:
            try:
                c.execute(f"UPDATE auto_paper SET {field}=? WHERE id=?", (_current_odds(ident), rid))
            except Exception as e:
                _error(f"recheck {rid}: {type(e).__name__}: {e}")
    return len(due)


# ── grading + after-fee CLV (reuses bet_grader / clv / circa_close, read-only) ──

def grade_and_clv(now=None):
    import bet_grader, clv, circa_close, history_tracker
    now = now or datetime.now(timezone.utc)
    graded = clv_done = 0
    with _connect() as c:
        todo = [dict(r) for r in c.execute("SELECT * FROM auto_paper WHERE fill_status='filled' "
                                           "AND (status='open' OR clv_bol_source IS NULL OR clv_circa_source IS NULL)")]
    boards = {}
    hconn = sqlite3.connect(history_tracker.DB_PATH, timeout=30)
    cconn = history_tracker._connect()
    try:
        for b in todo:
            k = _parse(b["commence_time"])
            if not k or now < k:
                continue
            upd = {}
            if b["status"] == "open" and now - k >= bet_grader.GRADE_AFTER and b["sport_key"] in bet_grader.ESPN_SPORTS:
                try:
                    games = []
                    for d in (k.astimezone(bet_grader.ET).date() + timedelta(days=i) for i in (0, -1, 1)):
                        key = (b["sport_key"], d.strftime("%Y%m%d"))
                        if key not in boards:
                            boards[key] = bet_grader._scoreboard(*key)
                        games += boards[key]
                    game, oriented = bet_grader.find_game(b, games)
                    if game and game.get("completed") and str(game.get("status") or "").startswith("STATUS_FINAL"):
                        grade, detail = bet_grader.grade_one(b, game, oriented)
                        if grade:
                            dec = b["fill_decimal"]
                            upd.update(status=grade, result_detail=detail, graded_at=now.isoformat(timespec="seconds"),
                                       profit=round(b["stake"] * (dec - 1), 2) if grade == "won"
                                       else (-b["stake"] if grade == "lost" else 0.0))
                            graded += 1
                except Exception as e:
                    _error(f"grade {b['id']}: {type(e).__name__}: {e}")
            if b["clv_bol_source"] is None and now >= k:
                try:
                    pair = clv._from_close_capture(hconn, b) or clv._from_hourly(hconn, b, k)
                    if pair:
                        fair = clv.power_devig(pair["price"], pair["other_price"])
                        same = b["market"].startswith("h2h") or pair["point"] is None or b["point"] is None \
                            or abs(float(pair["point"]) - float(b["point"])) < 1e-9
                        src = pair["source"]
                        if not same:
                            fair = clv.cross_line_fair(hconn, b, pair, fair)
                            src += "+model" if fair is not None else "+points"
                        upd.update(clv_bol_source=src, close_fair_bol=round(fair, 5) if fair is not None else None,
                                   clv_bol=round(fair * b["fill_decimal"] - 1, 5) if fair is not None else None,
                                   clv_at=now.isoformat(timespec="seconds"))
                        clv_done += 1
                    elif now - k > timedelta(days=3):
                        upd["clv_bol_source"] = "none"
                except Exception as e:
                    _error(f"clv {b['id']}: {type(e).__name__}: {e}")
            if b["clv_circa_source"] is None and now >= k + timedelta(minutes=30):
                try:
                    g = circa_close._find_game(cconn, b)
                    if g:
                        eid, home, away = g
                        if b["market"].startswith("totals"):
                            mine, other = ("Over", "Under") if b["side"] == "over" else ("Under", "Over")
                        else:
                            mine, other = (away, home) if b["side"] == "away" else (home, away)
                        pr = circa_close._pair(cconn, eid, b["market"], mine, other)
                        if pr:
                            price, other_price, cpt = pr
                            fair = clv.power_devig(price, other_price)
                            same = b["market"].startswith("h2h") or cpt is None or b["point"] is None \
                                or abs(float(cpt) - float(b["point"])) < 1e-9
                            if not same:
                                fair = circa_close._carry(cconn, b, eid, home, away, float(cpt), fair)
                            upd.update(clv_circa_source=("same_line" if same else "model") if fair is not None else "points",
                                       close_fair_circa=round(fair, 5) if fair is not None else None,
                                       clv_circa=round(fair * b["fill_decimal"] - 1, 5) if fair is not None else None)
                        elif now - k > timedelta(days=3):
                            upd["clv_circa_source"] = "no_market"
                    elif now - k > timedelta(days=3):
                        upd["clv_circa_source"] = "none"
                except Exception as e:
                    _error(f"circa clv {b['id']}: {type(e).__name__}: {e}")
            if upd:
                with _connect() as c:
                    c.execute(f"UPDATE auto_paper SET {', '.join(k2 + '=?' for k2 in upd)} WHERE id=?", (*upd.values(), b["id"]))
    finally:
        hconn.close()
        cconn.close()
    with _lock:
        _status["last_grade"] = now.isoformat(timespec="seconds")
    return {"graded": graded, "clv": clv_done}


# ── parity (page fair vs server fair) ───────────────────────────────────────

_parity_last = {}


def record_parity(items, kelly_unit_from_page=None, now_epoch=None):
    now_epoch = now_epoch or time.time()
    if kelly_unit_from_page:
        try:
            v = float(kelly_unit_from_page)
            if v > 0 and _load_state().get("kelly_unit") != v:
                STATE_PATH.write_text(json.dumps({**_load_state(), "kelly_unit": v}), encoding="utf-8")
        except (TypeError, ValueError, OSError):
            pass
    rows = []
    for it in items or []:
        try:
            key = _parity_key(it["sport"], it["game_id"], it["market"], it["side"], it.get("point"), it["book"])
            page_fair = float(it["fair"])
        except (KeyError, TypeError, ValueError):
            continue
        with _lock:
            sf = _server_fair.get(key)
        if not sf or now_epoch - sf[1] > PARITY_MAX_AGE or now_epoch - _parity_last.get(key, 0) < PARITY_EVERY:
            continue
        _parity_last[key] = now_epoch
        rows.append((datetime.fromtimestamp(now_epoch, timezone.utc).isoformat(timespec="seconds"), key, it["sport"],
                     it["market"], it["book"], page_fair, sf[0], sf[0] - page_fair))
    if rows:
        with _connect() as c:
            c.executemany("INSERT INTO auto_paper_parity (at,key,sport_key,market,book_key,page_fair,server_fair,diff) "
                          "VALUES (?,?,?,?,?,?,?,?)", rows)
    return len(rows)


# ── summary ─────────────────────────────────────────────────────────────────

def _mean_se(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None, None, 0
    return statistics.mean(xs), (statistics.stdev(xs) / math.sqrt(len(xs)) if len(xs) > 1 else None), len(xs)


def _stats(rows):
    filled = [r for r in rows if r["fill_status"] == "filled"]
    clv_b = [r["clv_bol"] for r in filled if r["clv_bol"] is not None]
    clv_c = [r["clv_circa"] for r in filled if r["clv_circa"] is not None]
    mb, seb, nb = _mean_se(clv_b)
    mc, sec, nc = _mean_se(clv_c)
    settled = [r for r in filled if r["status"] in ("won", "lost", "push", "void")]
    t30 = [r for r in filled if r["price_t30"]]
    survived = [r for r in t30 if r["price_t30"] != "gone"
                and (american_to_implied(r["price_t30"]) or 9) <= (american_to_implied(r["odds"]) or 0) + 1e-9]
    slips = [r["slip_pp"] for r in filled if r["slip_pp"] is not None]
    return {
        "candidates": len(rows), "filled": len(filled), "fill_rate": len(filled) / len(rows) if rows else None,
        "staked": round(sum(r["stake"] or 0 for r in filled), 2),
        "expected_raw": round(sum((r["stake"] or 0) * (r["fair_prob"] * (r["fill_decimal"] or 0) - 1) for r in filled), 2),
        "expected_shrunk": round(sum((r["stake"] or 0) * ((american_to_implied(r["odds"]) + r["edge_shrunk"]) * (r["fill_decimal"] or 0) - 1)
                                     for r in filled), 2),
        "settled": len(settled), "profit": round(sum(r["profit"] or 0 for r in settled), 2),
        "clv_bol": mb, "clv_bol_se": seb, "clv_bol_n": nb,
        "clv_bol_beat": sum(1 for x in clv_b if x > 0) / nb if nb else None,
        "clv_circa": mc, "clv_circa_se": sec, "clv_circa_n": nc,
        "slip_median_pp": statistics.median(slips) if slips else None,
        "t30_n": len(t30), "t30_survival": len(survived) / len(t30) if t30 else None,
    }


def _hours_bucket(h):
    return "<1h" if h < 1 else "1-3h" if h < 3 else "3-6h"


def summary():
    with _connect() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM auto_paper ORDER BY id DESC")]
        par = [r[0] for r in c.execute("SELECT diff FROM auto_paper_parity WHERE at >= ?",
                                       ((datetime.now(timezone.utc) - timedelta(days=7)).isoformat(timespec="seconds"),))]
    within = [r for r in rows if not r["over_cap"]]
    allst = _stats(within)
    parity = {"n": len(par), "median_abs_pp": statistics.median(abs(d) for d in par) if par else None}
    clv_best = allst["clv_bol"]
    go = {
        "n": {"need": GO_CRITERIA["n"], "have": allst["clv_bol_n"], "ok": allst["clv_bol_n"] >= GO_CRITERIA["n"]},
        "clv": {"need": GO_CRITERIA["clv"], "have": clv_best, "ok": clv_best is not None and clv_best >= GO_CRITERIA["clv"]},
        "clv_se": {"need": GO_CRITERIA["clv_se"], "have": allst["clv_bol_se"],
                   "ok": allst["clv_bol_se"] is not None and allst["clv_bol_se"] < GO_CRITERIA["clv_se"]},
        "slip_pp": {"need": GO_CRITERIA["slip_pp"], "have": allst["slip_median_pp"],
                    "ok": allst["slip_median_pp"] is not None and allst["slip_median_pp"] <= GO_CRITERIA["slip_pp"]},
        "t30": {"need": GO_CRITERIA["t30"], "have": allst["t30_survival"],
                "ok": allst["t30_survival"] is not None and allst["t30_survival"] >= GO_CRITERIA["t30"]},
        "parity_pp": {"need": GO_CRITERIA["parity_pp"], "have": parity["median_abs_pp"],
                      "ok": parity["median_abs_pp"] is not None and parity["median_abs_pp"] < GO_CRITERIA["parity_pp"]},
    }
    split = lambda f: {k: _stats([r for r in within if f(r) == k]) for k in sorted({f(r) for r in within})}
    with _lock:
        st = {k: (dict(v) if isinstance(v, dict) else list(v) if isinstance(v, list) else v) for k, v in _status.items()}
    return {
        "enabled": enabled(), "rule_version": RULE_VERSION, "kelly_unit": kelly_unit(), "caps": _caps(),
        "shrink": SHRINK, "max_hours": AUTO_MAX_HOURS, "books": list(AUTO_BOOKS), "status": st,
        "all": allst, "over_cap": _stats([r for r in rows if r["over_cap"]]),
        "by_book": split(lambda r: r["book_key"]), "by_hours": split(lambda r: _hours_bucket(r["hours_to_kick"] or 0)),
        "by_rule": split(lambda r: "shrunk qualifies" if r["qualifies_shrunk"] else "raw only"),
        "parity": parity, "go": go, "go_ready": all(v["ok"] for v in go.values()),
        "recent": rows[:60],
    }


# ── loop + routes ───────────────────────────────────────────────────────────

def run_loop():
    """Daemon (app.py). Does nothing unless AUTO_PAPER_ENABLED=1 in .env."""
    time.sleep(30)
    _status["started"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print("[auto-paper] loop started (idle unless AUTO_PAPER_ENABLED=1)", flush=True)
    last_select = last_grade = 0.0
    while True:
        try:
            _status["last_tick"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            run_rechecks()
            if enabled():
                if time.time() - last_select >= SELECT_SECONDS:
                    last_select = time.time()
                    select_once()
                if time.time() - last_grade >= GRADE_SECONDS:
                    last_grade = time.time()
                    grade_and_clv()
        except Exception as e:                  # never let the loop die
            _error(f"tick: {type(e).__name__}: {e}")
        time.sleep(TICK_SECONDS)


bp = Blueprint("auto_paper", __name__)


@bp.route("/api/auto-paper/summary")
def api_summary():
    return jsonify(summary())


@bp.route("/api/auto-paper/parity", methods=["POST"])
def api_parity():
    d = request.get_json(silent=True) or {}
    return jsonify({"ok": True, "paired": record_parity((d.get("items") or [])[:200], d.get("kelly_unit"))})


@bp.route("/api/auto-paper/grade", methods=["POST"])
def api_grade():
    return jsonify(grade_and_clv())
