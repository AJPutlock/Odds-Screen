"""
Polymarket period mapping (scrapers/polymarket.py TYPES) and tail-rung filter.

Fixture: real markets from gateway.polymarket.us/v2/leagues/nfl/events for
BAL Ravens vs. CLE Browns (nfl-bal-cle-2026-10-18), pulled 2026-10-13. Slugs,
titles, lines and quotes are as listed except where marked "adjusted".

Run: python -m pytest tests/test_polymarket_periods.py   (from Odds Screen/)
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers import polymarket as pm  # noqa: E402

EV = "tsc-nfl-bal-cle-2026-10-18"


def _total(smt, tag, line, title, bid, ask):
    n = str(line).replace(".", "pt")
    return {"slug": f"{EV}-{tag}-{n}", "title": title, "line": line, "sportsMarketType": smt,
            "active": True, "closed": False, "hidden": False,
            "bestBidQuote": {"value": f"{bid:.4f}"}, "bestAskQuote": {"value": f"{ask:.4f}"},
            "marketSides": [{"description": "Over", "long": True}, {"description": "Under", "long": False}]}


Q1, H1 = "football_game_first_quarter_total", "football_game_first_half_total"
MARKETS = [
    _total(Q1, "1q", 6.5, "Over 6.5 total points in 1st quarter", 0.41, 0.49),
    _total(Q1, "1q", 7.5, "Over 7.5 total points in 1st quarter", 0.42, 0.48),
    _total(Q1, "1q", 22.5, "Over 22.5 total points in 1st quarter", 0.01, 0.05),
    _total(Q1, "1q", 25.5, "Over 25.5 total points in 1st quarter", 0.01, 0.04),
    _total(H1, "1h", 20.5, "Over 20.5 total points in 1st half", 0.48, 0.51),
    _total(H1, "1h", 22.5, "Over 22.5 total points in 1st half", 0.41, 0.46),
    # adjusted: real slug/title, quote tightened so the Q1 rung survives MAX_SPREAD
    dict(_total(Q1, "1q", 8.5, "Over 8.5 total points in 1st quarter", 0.30, 0.33),
         slug=f"{EV}-1q-8pt5-adjusted"),
    # team 1H total: same "1st half" wording, but one team's points — must stay unmapped
    dict(_total("football_team_first_half_total", "tt1h-bal", 10.5,
                "Baltimore Ravens over 10.5 total points in 1st half", 0.45, 0.48)),
]
EVENT = {"slug": "nfl-bal-cle-2026-10-18", "active": True, "startTime": "2026-10-18T17:00:00Z",
         "teams": [{"name": "Baltimore Ravens"}, {"name": "Cleveland Browns"}], "markets": MARKETS}

PERIOD = {"q1": ("first_quarter", "1st quarter", "1q"), "h1": ("first_half", "1st half", "1h"),
          "": ("full_game", None, None)}


def test_types_period_matches_board_key():
    for smt, key in pm.TYPES.items():
        suffix = key.rsplit("_", 1)[1] if "_" in key else ""
        assert PERIOD[suffix][0] in smt, (smt, key)


def test_fixture_titles_and_slugs_match_mapped_period():
    for m in MARKETS:
        key = pm.TYPES.get(m["sportsMarketType"])
        if not key:
            continue
        suffix = key.rsplit("_", 1)[1] if "_" in key else ""
        _, word, tag = PERIOD[suffix]
        assert word in m["title"] and f"-{tag}-" in m["slug"], m["slug"]
        assert float(re.search(r"over ([\d.]+) total", m["title"], re.I).group(1)) == m["line"]


def test_q1_ladder_drops_floor_quoted_tail_rungs():
    g = pm._game(EVENT)
    q1 = sorted(e["home_point"] for e in g["alt_lines"]["totals_q1"])
    assert q1 == [8.5]                           # 22.5 / 25.5 (1c bid) gone; 6.5 / 7.5 too wide
    assert g["markets"]["totals_q1"]["home_point"] == 8.5
    h1 = sorted(e["home_point"] for e in g["alt_lines"]["totals_h1"])
    assert h1 == [20.5, 22.5]                    # 22.5's 5c spread kept; team 1H total not mixed in
    assert all(e["home_odds"] and e["away_odds"] for lst in g["alt_lines"].values() for e in lst)
