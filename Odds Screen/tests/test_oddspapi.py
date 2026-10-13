"""
OddsPapi historical-odds parsing and board matching (oddspapi.py).

Fixture: SEA @ DEN (id1400003171515900), pulled 2026-10-13 from
/v4/historical-odds; market ids/handicaps as in /v4/markets (sport 14):
141 moneyline (1=home DEN, 2=away SEA), 14296 spread handicap +2.5 (home),
14298 spread +3.0 (pulled: last entries inactive), 1448 total 40.5.

Run from Odds Screen/:  python -c "import sys; sys.path.insert(0,'tests'); import test_oddspapi as t; [f() for n,f in vars(t).items() if n.startswith('test')]"
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import oddspapi as op  # noqa: E402

MM = {
    "141":   ("football", "h2h", 0.0, {"141": "1", "142": "2"}),
    "14296": ("football", "spreads", 2.5, {"14296": "1", "14297": "2"}),
    "14298": ("football", "spreads", 3.0, {"14298": "1", "14299": "2"}),
    "1448":  ("football", "totals", 40.5, {"1448": "over", "1449": "under"}),
}


def _e(price, active=True, at="2026-10-13T22:16:37.616Z", limit=None):
    return {"createdAt": at, "price": price, "limit": limit, "active": active, "exchangeMeta": None}


HIST = {"fixtureId": "id1400003171515900", "bookmakers": {"pinnacle": {"markets": {
    "141":   {"outcomes": {"141": {"players": {"0": [_e(2.34), _e(2.23, limit=15000.0)]}},
                           "142": {"players": {"0": [_e(1.671), _e(1.719)]}}}},
    "14296": {"outcomes": {"14296": {"players": {"0": [_e(1.961)]}}, "14297": {"players": {"0": [_e(1.925)]}}}},
    "14298": {"outcomes": {"14298": {"players": {"0": [_e(1.87, active=False)]}},
                           "14299": {"players": {"0": [_e(1.952, active=False)]}}}},
    "1448":  {"outcomes": {"1448": {"players": {"0": [_e(1.917)]}}, "1449": {"players": {"0": [_e(1.934)]}}}},
}}}}


def _parsed():
    op._market_map["map"] = MM
    try:
        return op.parse_history(HIST)["pinnacle"]
    finally:
        op._market_map["map"] = None


def test_moneyline_last_entry_home_is_participant1():
    ml = _parsed()["h2h"]
    assert len(ml) == 1
    assert ml[0]["home_odds"] == "+123" and ml[0]["away_odds"] == "-139", ml[0]
    assert ml[0]["limit"] == 15000.0


def test_spread_handicap_is_home_number_and_pulled_line_skipped():
    sp = _parsed()["spreads"]
    assert sorted((r["home_point"], r["active"]) for r in sp) == [(2.5, True), (3.0, False)], sp
    main = op.main_row(sp)                                      # +3.0 ended inactive
    assert main["home_point"] == 2.5 and main["away_point"] == -2.5
    assert op.row_at(sp, "spreads", -2.5) is main
    assert op.row_at(sp, "spreads", -3.0) is None
    assert op.off_at(sp) is None


def test_book_that_pulled_the_game_reads_off():
    rows = [{"active": False, "changed_at": "2026-10-13T22:23:09.449Z", "away_dec": 1.95, "home_dec": 1.87}]
    assert op.main_row(rows) is None and op.off_at(rows) == "2026-10-13T22:23:09.449Z"


def test_totals_over_in_home_slot():
    t = _parsed()["totals"][0]
    assert t["home_point"] == t["away_point"] == 40.5
    assert t["home_dec"] == 1.917 and t["away_dec"] == 1.934      # Over -109 home, Under -107 away


def test_main_row_closest_to_even():
    rows = [{"away_dec": 1.5, "home_dec": 2.6}, {"away_dec": 1.93, "home_dec": 1.89}, {"away_dec": 2.5, "home_dec": 1.55}]
    assert op.main_row(rows) is rows[1]


def test_match_games_orientation_and_kickoff_window():
    board = [{"game_id": "b1", "away_team": "Seattle Seahawks", "home_team": "Denver Broncos",
              "commence_time": "2026-10-16T00:15:00+00:00"},
             {"game_id": "b2", "away_team": "Ohio State Buckeyes", "home_team": "Texas Longhorns",
              "commence_time": "2026-10-17T19:30:00+00:00"}]
    opg = [{"fixture_id": "f1", "away_team": "Seattle Seahawks", "home_team": "Denver Broncos",
            "commence_time": "2026-10-16T00:15:00.000Z"},
           {"fixture_id": "f2", "away_team": "Texas Longhorns", "home_team": "Ohio State Buckeyes",   # neutral-site flip
            "commence_time": "2026-10-17T19:30:00.000Z"},
           {"fixture_id": "f3", "away_team": "Grambling State Tigers", "home_team": "Texas Southern Tigers",
            "commence_time": "2026-10-17T19:30:00.000Z"}]
    m = op.match_games(board, opg)
    assert m["b1"][0]["fixture_id"] == "f1" and m["b1"][1] is False
    assert m["b2"][0]["fixture_id"] == "f2" and m["b2"][1] is True
    late = [dict(opg[0], commence_time="2026-10-23T00:15:00.000Z")]     # next week's rematch
    assert op.match_games(board[:1], late) == {}


def test_short_college_names_match_board_long_names():
    assert op._overlap("FIU Panthers", "Florida International Panthers") == 1.0      # 10/13: FIU @ Jax State unmatched
    assert op._overlap("Southern Miss Golden Eagles", "Southern Mississippi Golden Eagles") == 1.0


def test_american_round_trip():
    assert op.american(2.23) == "+123" and op.american(1.719) == "-139" and op.american(2.0) == "+100"
    assert abs(op.dec_from_american("-110") - 1.909) < 1e-3
