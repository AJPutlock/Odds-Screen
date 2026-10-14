"""
Pinnacle close selection (pinnacle_close.close_entry / close_rows) and the
CLV main-vs-alt rule (pinnacle_close.judge).

Prices modelled on SEA @ DEN (id1400003171515900, kickoff 2026-10-16T00:15Z)
from /v4/historical-odds on 10/13; times after 10/13 are adjusted to sit
around that kickoff.

Run from Odds Screen/:  python -c "import sys; sys.path.insert(0,'tests'); import test_pinnacle_close as t; [f() for n,f in vars(t).items() if n.startswith('test')]"
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import oddspapi as op  # noqa: E402
import pinnacle_close as pc  # noqa: E402

KICK = "2026-10-16T00:15:00.000Z"
K = op._ts(KICK)


def _e(at, price, active=True):
    return {"createdAt": at, "price": price, "limit": 15000.0, "active": active, "exchangeMeta": None}


def test_close_is_last_entry_at_or_before_kickoff_not_live_price():
    arr = [_e("2026-10-15T20:00:00.000Z", 2.20), _e("2026-10-16T00:14:30.000Z", 2.23),
           _e("2026-10-16T00:40:00.000Z", 3.10)]                 # in-play price after kickoff
    assert pc.close_entry(arr, K)["price"] == 2.23


def test_kickoff_suspension_falls_back_to_last_active():
    arr = [_e("2026-10-16T00:10:00.000Z", 2.25), _e("2026-10-16T00:15:00.000Z", 2.25, active=False)]
    assert pc.close_entry(arr, K)["price"] == 2.25


def test_line_pulled_hours_before_kickoff_has_no_close():
    arr = [_e("2026-10-15T12:00:00.000Z", 1.91), _e("2026-10-15T18:00:00.000Z", 1.95, active=False)]
    assert pc.close_entry(arr, K) is None
    assert pc.close_entry([_e("2026-10-16T01:00:00.000Z", 2.0)], K) is None    # nothing before kickoff


MM = {"141":   ("football", "h2h", 0.0, {"141": "1", "142": "2"}),
      "14296": ("football", "spreads", 2.5, {"14296": "1", "14297": "2"}),
      "14300": ("football", "spreads", 3.5, {"14300": "1", "14301": "2"}),
      "14298": ("football", "spreads", 3.0, {"14298": "1", "14299": "2"})}


def _hist():
    pre, live = "2026-10-16T00:12:00.000Z", "2026-10-16T00:30:00.000Z"
    return {"bookmakers": {"pinnacle": {"markets": {
        "141":   {"outcomes": {"141": {"players": {"0": [_e(pre, 2.23), _e(live, 2.6)]}},
                               "142": {"players": {"0": [_e(pre, 1.719), _e(live, 1.5)]}}}},
        "14296": {"outcomes": {"14296": {"players": {"0": [_e(pre, 1.961)]}}, "14297": {"players": {"0": [_e(pre, 1.925)]}}}},
        "14300": {"outcomes": {"14300": {"players": {"0": [_e(pre, 1.70)]}}, "14301": {"players": {"0": [_e(pre, 2.25)]}}}},
        "14298": {"outcomes": {"14298": {"players": {"0": [_e("2026-10-15T09:00:00.000Z", 1.87, active=False)]}},
                               "14299": {"players": {"0": [_e("2026-10-15T09:00:00.000Z", 1.95, active=False)]}}}},
    }}}}


def test_close_rows_shape_main_and_alt():
    op._market_map["map"] = MM
    try:
        fx = {"fixture_id": "id1400003171515900", "home_team": "Denver Broncos", "away_team": "Seattle Seahawks",
              "commence_time": KICK}
        rows = pc.close_rows("americanfootball_nfl", fx, _hist())
    finally:
        op._market_map["map"] = None
    ml = [r for r in rows if r[8] == "h2h"]
    assert [(r[10], r[12]) for r in ml] == [("Seattle Seahawks", 1.719), ("Denver Broncos", 2.23)]   # pre-kickoff, not live
    sp = sorted((r[9], r[10], r[11]) for r in rows if r[8] == "spreads")
    assert sp == [(0, "Denver Broncos", 2.5), (0, "Seattle Seahawks", -2.5),
                  (1, "Denver Broncos", 3.5), (1, "Seattle Seahawks", -3.5)], sp   # 3.0 was pulled
    assert all(r[1] == "pinnacle_close" and r[3] == "op:id1400003171515900" and r[7] == "pinnacle" for r in rows)


ROWS = [(0, "Seattle Seahawks", -2.5, 1.925), (0, "Denver Broncos", 2.5, 1.961),
        (1, "Seattle Seahawks", -3.5, 2.25), (1, "Denver Broncos", 3.5, 1.70)]


def test_judge_main_alt_off_number():
    sea, den = "Seattle Seahawks", "Denver Broncos"
    assert pc.judge(ROWS, {"point": -2.5}, sea, den) == ("main", 1.925, 1.961, -2.5)
    src, *_ = pc.judge(ROWS, {"point": -3.5}, sea, den)
    assert src == "alt"                                         # only a ladder rung: no CLV
    assert pc.judge(ROWS, {"point": -4.5}, sea, den)[0] == "off_number"
    assert pc.judge([(0, sea, None, 1.719), (0, den, None, 2.23)], {"point": None}, sea, den)[0] == "main"
    assert pc.judge(ROWS, {"point": -2.5}, "Over", "Under")[0] == "no_market"


def test_match_fixture_flipped_and_short_names():
    fx = [{"fixture_id": "f1", "home_team": "Jacksonville State Gamecocks", "away_team": "FIU Panthers",
           "commence_time": "2026-10-14T00:00:00.000Z"}]
    bet = {"away": "Florida International Panthers", "home": "Jacksonville State Gamecocks",
           "commence_time": "2026-10-14T00:00:00+00:00"}
    assert pc._match_fixture(bet, fx)["fixture_id"] == "f1"
    flipped = dict(bet, away=bet["home"], home=bet["away"])
    assert pc._match_fixture(flipped, fx)["fixture_id"] == "f1"
    assert pc._match_fixture(dict(bet, commence_time="2026-10-21T00:00:00+00:00"), fx) is None
