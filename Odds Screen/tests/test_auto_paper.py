"""
Paper auto-betting (auto_paper.py): constants twinned with index.html, the
places-nothing guarantees, the fair-value port, selection rules and fills.

Rows are built by hand in the board's shape (books / bookmaker_ladder as
app._inject_scraped leaves them). The port was also checked against the
page's own JavaScript on the 10/14 cached NFL/NCAAF/NHL boards: 1,338
exchange quotes priced by both, max |diff| 0.

Run from Odds Screen/:  python -c "import sys; sys.path.insert(0,'tests'); import test_auto_paper as t; [f() for n,f in vars(t).items() if n.startswith('test')]"
"""
import ast
import math
import os
import re
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import auto_paper as ap  # noqa: E402

NOW = datetime(2026, 10, 17, 16, 0, tzinfo=timezone.utc)
INDEX = Path(ROOT) / "static" / "index.html"


def _html():
    return INDEX.read_text(encoding="utf-8")


def _row(market="spreads", hours=3.0, books=None, ladder=None):
    return {"game_id": "g1", "market": market, "away_team": "Away U", "home_team": "Home U",
            "commence_time": (NOW + timedelta(hours=hours)).isoformat(), "books": books or {},
            "bookmaker_ladder": ladder}


# ── one source of truth with index.html ─────────────────────────────────────

def test_constants_match_index_html():
    h = _html()
    num = lambda name: float(re.search(rf"const {name} = ([0-9.]+);", h).group(1))
    assert num("EV_MIN_EDGE") == ap.EV_MIN_EDGE
    assert num("EV_OVER_EXTRA") == ap.EV_OVER_EXTRA
    assert num("STALE_SOURCE_HOURS") == ap.STALE_SOURCE_HOURS
    assert float(re.search(r"const MIN_BEST_ODDS = (-?\d+);", h).group(1)) == ap.MIN_BEST_ODDS
    tiers = re.search(r"const EV_TIME_TIERS = \[(.*?)\];", h).group(1)
    pairs = re.findall(r"\[(-?Infinity|[0-9.]+), (EV_MIN_EDGE|[0-9.]+)\]", tiers)
    parsed = tuple((-math.inf if a == "-Infinity" else float(a), ap.EV_MIN_EDGE if b == "EV_MIN_EDGE" else float(b))
                   for a, b in pairs)
    assert parsed == ap.EV_TIME_TIERS, parsed
    fg = re.search(r"const FULL_GAME_MARKETS = new Set\(\[(.*?)\]\)", h).group(1)
    assert set(re.findall(r"'(\w+)'", fg)) == set(ap.FULL_GAME_MARKETS)
    w = h[h.index("function sharpWeights("):]
    w = w[:w.index("\n}\n")]
    assert "circa: 0.50, bookmaker: 0.30, betonlineag: 0.20" in w and ap.SHARP_WEIGHTS_3 == {"circa": .5, "bookmaker": .3, "betonlineag": .2}
    assert "circa: 0.6, bookmaker: 0.4, betonlineag: 0" in w and ap.SHARP_WEIGHTS_CB == {"circa": .6, "bookmaker": .4, "betonlineag": 0}
    assert "has.circa ? 0.7 : 0" in w and "betonlineag: 0.3" in w
    assert (ap.SHARP_WEIGHTS_PAIR_MAIN, ap.SHARP_WEIGHTS_PAIR_BOL) == (0.7, 0.3)
    assert "Math.min(0.985, Math.max(0.015" in h and ap.P_CLAMP == (0.015, 0.985)
    bw = re.search(r"const BOARD_WINDOW_HOURS = \{(.*?)\};", h).group(1)
    assert {k: float(v) for k, v in re.findall(r"(\w+): (\d+)", bw)} == ap.BOARD_WINDOW_HOURS
    assert num("BOARD_WINDOW_DEFAULT") == ap.BOARD_WINDOW_DEFAULT
    bk = re.search(r"const BK_FRESH_MIN = ([0-9.]+), BK_FADE_END_MIN = ([0-9.]+), BK_FADE_FLOOR = ([0-9.]+), BK_FADE_STALE = ([0-9.]+);", h)
    assert tuple(float(x) for x in bk.groups()) == (ap.BK_FRESH_MIN, ap.BK_FADE_END_MIN, ap.BK_FADE_FLOOR, ap.BK_FADE_STALE)
    assert "ageMin > 60" in h
    assert "const w = bkAgeFade(sharpWeights(has), has, sk);" in h
    assert num("KELLY_SHRINK_EXCHANGE") == ap.KELLY_SHRINK_EXCHANGE == ap.SHRINK
    assert num("KELLY_SHRINK_REC") == ap.KELLY_SHRINK_REC
    ex = re.search(r"const KELLY_EXCHANGES = new Set\(\[(.*?)\]\)", h).group(1)
    assert set(re.findall(r"'(\w+)'", ex)) == set(ap.KELLY_EXCHANGES) == set(ap.AUTO_BOOKS)


def test_shin_matches_page_formula():
    a, b = ap.devig_shin(ap.american_to_implied("-110"), ap.american_to_implied("-110"))
    assert abs(a - 0.5) < 1e-12 and abs(b - 0.5) < 1e-12
    # Value from index.html devigShin run in node on 10/14: -150 / +130.
    a, _ = ap.devig_shin(ap.american_to_implied("-150"), ap.american_to_implied("+130"))
    assert abs(a - 0.5826086956521739) < 1e-9, a


# ── places nothing ──────────────────────────────────────────────────────────

def test_no_order_path_imported_or_called():
    src = (Path(ROOT) / "auto_paper.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    mods = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            mods |= {a.name for a in n.names}
        elif isinstance(n, ast.ImportFrom):
            mods.add(n.module or "")
            mods |= {f"{n.module}.{a.name}" for a in n.names}
    assert not any("novig_orders" in m or "novig_client" in m or "novig_catalog" in m for m in mods), mods
    code = re.sub(r'"""[\s\S]*?"""', "", src)              # docstrings may name what's banned
    for banned in ("/v3/orders", "preview(", ".place(", "NovigClient", "requests.post", "order/place"):
        assert banned not in code, banned


def test_novig_fill_reads_stream_only():
    calls = []

    class FakeWS:
        def levels(self, market_id):
            calls.append(market_id)
            # Bids on the OTHER outcome: .48 x 1,000 contracts and .47 x 5,000 -> asks .52 / .53 here.
            return {"o2": [(0.48, 1000), (0.47, 5000)], "o1": [(0.40, 100)]}

    real = ap.ws
    ap.ws = FakeWS()
    try:
        f = ap.novig_fill("m1|o1", "m1|o2", 0.52, 5.0)       # 961 contracts at .52 = $4.99
        assert f["fill_status"] == "filled" and abs(f["fill_price"] - 0.52) < 1e-9 and f["fee_usd"] == 0.0, f
        f = ap.novig_fill("m1|o1", "m1|o2", 0.52, 50.0)      # needs .53 too -> FOK at .52 fails
        assert f["fill_status"] == "short", f
        assert calls == ["m1", "m1"]
        ap.ws = type("W", (), {"levels": lambda self, m: None})()
        assert ap.novig_fill("m1|o1", "m1|o2", 0.52, 5.0)["fill_status"] == "no_stream"
    finally:
        ap.ws = real


def test_exchange_fill_after_fee():
    f = ap.exchange_fill("-116", 0.52, 100.0, 10.0)          # Kalshi 52c ask, board -116 net of fee
    assert f["fill_status"] == "filled" and abs(f["fill_price"] - ap.american_to_implied("-116")) < 1e-12
    assert 0.015 < f["fee_pp"] < 0.02, f
    assert ap.exchange_fill("-116", 0.52, 5.0, 10.0)["fill_status"] == "short"
    assert ap.exchange_fill("-116", 0.52, None, 10.0)["fill_status"] == "unchecked"


# ── rules ───────────────────────────────────────────────────────────────────

def test_flat_floor_and_overs():
    for h in (100, 48, 10, 3, None):
        assert ap.ev_min_edge("spreads", h, "upper") == 0.010
    assert abs(ap.ev_min_edge("totals", 3, "upper") - 0.015) < 1e-12
    assert ap.ev_min_edge("totals", 3, "lower") == 0.010


def test_bookmaker_fade():
    assert ap.bk_fade_factor(5) == 1 and ap.bk_fade_factor(20) == 0.15 and ap.bk_fade_factor(None) == 0.05
    assert abs(ap.bk_fade_factor(15) - 0.575) < 1e-12 and ap.bk_fade_factor(61) == 0.05
    has = {"circa": True, "bookmaker": True, "betonlineag": True}
    w = ap.bk_age_fade(ap.sharp_weights(has), has, {"bookmaker": (NOW - timedelta(minutes=30)).isoformat()}, NOW)
    assert abs(w["bookmaker"] - 0.045) < 1e-12 and abs(sum(w.values()) - 1) < 1e-12
    assert abs(w["circa"] / w["betonlineag"] - 2.5) < 1e-12
    only = {"circa": False, "bookmaker": True, "betonlineag": False}
    assert ap.bk_age_fade(ap.sharp_weights(only), only, {}, NOW)["bookmaker"] == 1.0


def _books(novig_away="+110", novig_home="-125"):
    return {
        "circa": {"away_odds": "-110", "home_odds": "-110", "away_point": 3.5, "home_point": -3.5},
        "betonlineag": {"away_odds": "-112", "home_odds": "-108", "away_point": 3.5, "home_point": -3.5},
        "novig": {"away_odds": novig_away, "home_odds": novig_home, "away_point": 3.5, "home_point": -3.5,
                  "away_prob": 0.476, "home_prob": 0.556, "away_ref": "m|a", "home_ref": "m|h"},
    }


def test_same_line_edge_qualifies_and_started_never():
    qs = ap.evaluate_row("americanfootball_nfl", _row(books=_books()), {}, NOW)
    away = next(q for q in qs if q["bk"] == "novig" and q["side"] == "upper")
    # Circa + BetOnline on the same point, no Bookmaker: sharpWeights 70/30, blended, then clamped.
    fair = 0.7 * 0.5 + 0.3 * ap.devig_shin(ap.american_to_implied("-112"), ap.american_to_implied("-108"))[0]
    assert abs(away["fair"] - fair) < 1e-12 and away["exact"]
    assert away["why"] is None and away["edge"] > 0.01
    assert not ap.evaluate_row("americanfootball_nfl", _row(books=_books(), hours=-0.01), {}, NOW)   # started
    # Every pregame candidate on the board counts (10/14 widening), off the board doesn't.
    early = ap.evaluate_row("americanfootball_nfl", _row(books=_books(), hours=100), {}, NOW)
    assert next(q for q in early if q["bk"] == "novig" and q["side"] == "upper")["why"] is None
    assert ap.evaluate_row("americanfootball_nfl", _row(books=_books(), hours=170), {}, NOW) == []
    assert ap.evaluate_row("icehockey_nhl", _row(books=_books(), hours=37), {}, NOW) == []


def test_no_model_lines_and_ncaaf_periods_out():
    b = _books()
    b["novig"] = dict(b["novig"], away_point=6.5, home_point=-6.5)   # 3 pts off every real price
    qs = ap.evaluate_row("americanfootball_nfl", _row(books=b), {}, NOW)
    assert all(q["why"] == "no_real_fair" for q in qs if q["bk"] == "novig")
    assert ap.evaluate_row("americanfootball_ncaaf", _row(market="spreads_h1", books=_books()), {}, NOW) == []
    assert ap.evaluate_row("americanfootball_nfl", _row(market="spreads_h1", books=_books()), {}, NOW)


def test_stale_exchange_is_skipped():
    times = {"odds_api": NOW.isoformat(), "novig": (NOW - timedelta(hours=3)).isoformat()}
    qs = ap.evaluate_row("americanfootball_nfl", _row(books=_books()), times, NOW)
    assert not any(q["bk"] == "novig" for q in qs)


def test_record_sizes_caps_and_dedupes():
    tmp = Path(tempfile.mkdtemp())
    old = (ap.DB_PATH, ap.STATE_PATH, ap.ENV_PATH, ap.ws)
    ap.DB_PATH, ap.STATE_PATH, ap.ENV_PATH = tmp / "bets.db", tmp / "state.json", tmp / "none.env"
    ap.ws = type("W", (), {"levels": lambda self, m: {"h": [(0.524, 100000)]}})()   # ask for away = .476
    try:
        with sqlite3.connect(ap.DB_PATH) as c:
            c.execute("CREATE TABLE bets (id INTEGER PRIMARY KEY, event_id TEXT, market TEXT, side TEXT, team TEXT, stake REAL, status TEXT)")
        ap._ensure_tables()
        q = next(q for q in ap.evaluate_row("americanfootball_nfl", _row(books=_books()), {}, NOW)
                 if q["bk"] == "novig" and q["side"] == "upper")
        rid = ap.record(q, NOW)
        assert rid and ap.record(q, NOW) is None                       # same key: last_seen only
        with sqlite3.connect(ap.DB_PATH) as c:
            c.row_factory = sqlite3.Row
            r = dict(c.execute("SELECT * FROM auto_paper WHERE id=?", (rid,)).fetchone())
            bets_rows = c.execute("SELECT COUNT(*) FROM bets").fetchone()[0]
        assert bets_rows == 0                                          # never writes the real bets table
        assert r["n_seen"] == 2 and r["side"] == "away" and r["team"] == "Away U" and r["fill_status"] == "filled"
        assert r["in_pilot_window"] == 1 and abs(r["hours_to_kick"] - 3.0) < 1e-6
        impl = ap.american_to_implied("+110")
        assert abs(r["edge_shrunk"] - r["edge_raw"] * ap.SHRINK) < 1e-6
        want = math.floor(min(ap.kelly_stake(impl + r["edge_shrunk"], "+110", ap.DEFAULT_KELLY_UNIT), 25.0) * 100) / 100
        assert abs(r["stake"] - want) < 0.02, (r["stake"], want)
        # Same team again on another market: the group's held stake comes off.
        q2 = dict(q, mkt="h2h", pt=None, odds="+180")
        ap.record(q2, NOW)
        with sqlite3.connect(ap.DB_PATH) as c:
            held = c.execute("SELECT held_before FROM auto_paper WHERE market='h2h'").fetchone()
        assert held is None or held[0] >= r["stake"] - 0.01
    finally:
        ap.DB_PATH, ap.STATE_PATH, ap.ENV_PATH, ap.ws = old


def test_parity_pairs_fresh_server_fair_only():
    tmp = Path(tempfile.mkdtemp())
    old = (ap.DB_PATH, ap.STATE_PATH)
    ap.DB_PATH, ap.STATE_PATH = tmp / "bets.db", tmp / "state.json"
    try:
        ap._ensure_tables()
        ap._parity_last.clear()
        k = ap._parity_key("americanfootball_nfl", "g1", "spreads", "upper", 3.5, "novig")
        ap._server_fair[k] = (0.5050, 1000.0)
        item = {"sport": "americanfootball_nfl", "game_id": "g1", "market": "spreads", "side": "upper",
                "point": 3.5, "book": "novig", "fair": 0.5040}
        assert ap.record_parity([item], 600, now_epoch=1100.0) == 1
        assert ap.record_parity([item], None, now_epoch=1200.0) == 0               # 10-min throttle
        assert ap.record_parity([item], None, now_epoch=1000.0 + ap.PARITY_MAX_AGE + 700) == 0   # server fair too old
        assert ap.kelly_unit() == 600
    finally:
        ap.DB_PATH, ap.STATE_PATH = old


def test_recheck_reads_freshly_injected_rows():
    tmp = Path(tempfile.mkdtemp())
    old = (ap.DB_PATH, dict(ap._deps), list(ap._rechecks))
    ap.DB_PATH = tmp / "bets.db"
    try:
        ap._ensure_tables()
        with sqlite3.connect(ap.DB_PATH) as c:
            rid = c.execute("INSERT INTO auto_paper (key) VALUES ('k')").lastrowid
        stale = _row(books=_books(novig_away="+110"))
        fresh = _row(books=_books(novig_away="+102"))          # moved after the bet
        calls = []

        def inject(sk):                                        # app._inject_scraped: (cache, rows, b365 games)
            calls.append(sk)
            return None, [fresh], None

        ap._deps.update(caches={"americanfootball_nfl": {"data": [stale]}}, inject=inject)
        ap._rechecks[:] = [(0, rid, "price_t30", ("americanfootball_nfl", "g1", "spreads", "upper", 3.5, "novig"))]
        assert ap.run_rechecks(now_epoch=10) == 1 and calls == ["americanfootball_nfl"]
        with sqlite3.connect(ap.DB_PATH) as c:
            assert c.execute("SELECT price_t30 FROM auto_paper WHERE id=?", (rid,)).fetchone()[0] == "+102"
    finally:
        ap.DB_PATH = old[0]
        ap._deps.clear(); ap._deps.update(old[1])
        ap._rechecks[:] = old[2]


def test_kelly_unit_source_and_pilot_go():
    tmp = Path(tempfile.mkdtemp())
    old = (ap.DB_PATH, ap.STATE_PATH, ap.ENV_PATH)
    ap.DB_PATH, ap.STATE_PATH, ap.ENV_PATH = tmp / "bets.db", tmp / "state.json", tmp / "none.env"
    try:
        assert ap.kelly_unit_info() == (ap.DEFAULT_KELLY_UNIT, "default")
        ap.STATE_PATH.write_text('{"kelly_unit": 250}', encoding="utf-8")
        assert ap.kelly_unit_info() == (250.0, "page")
        ap.ENV_PATH.write_text("AUTO_PAPER_KELLY_UNIT=900\n", encoding="utf-8")
        assert ap.kelly_unit_info() == (900.0, "env")
        # A table from before the widening gets in_pilot_window, back-filled from hours_to_kick.
        with sqlite3.connect(ap.DB_PATH) as c:
            c.execute("CREATE TABLE auto_paper (id INTEGER PRIMARY KEY, key TEXT UNIQUE, hours_to_kick REAL)")
            c.execute("INSERT INTO auto_paper (key, hours_to_kick) VALUES ('near', 2.0), ('far', 30.0)")
        ap._ensure_tables()
        with sqlite3.connect(ap.DB_PATH) as c:
            got = dict(c.execute("SELECT key, in_pilot_window FROM auto_paper").fetchall())
        assert got == {"near": 1, "far": 0}
        tmp2 = Path(tempfile.mkdtemp())
        ap.DB_PATH = tmp2 / "bets.db"
        ap._ensure_tables()
        with sqlite3.connect(ap.DB_PATH) as c:
            c.executemany("INSERT INTO auto_paper (key, fill_status, status, stake, fair_prob, fill_decimal, odds, edge_shrunk, "
                          "hours_to_kick, in_pilot_window, over_cap, qualifies_shrunk, book_key, clv_bol) VALUES "
                          "(?, 'filled', 'open', 10, 0.52, 2.0, 100, 0.012, ?, ?, ?, 0, 'novig', ?)",
                          [("a", 2.0, 1, 0, 0.02), ("b", 30.0, 0, 1, -0.01), ("c", 50.0, 0, 0, 0.01)])
        j = ap.summary()
        assert j["kelly_unit_source"] == "env" and j["all"]["filled"] == 3 and j["pilot"]["filled"] == 1
        assert j["go"]["n"]["have"] == 3 and j["go_pilot"]["n"]["have"] == 1
        assert set(j["by_cap"]) == {"within caps", "over a cap"} and "e 24-72h" in j["by_hours"]
    finally:
        ap.DB_PATH, ap.STATE_PATH, ap.ENV_PATH = old
