import math

import pytest

from nhl_props import config, scan, sources, tracking
from nhl_props.checker import check_event
from nhl_props.odds_math import (american_to_decimal, count_pmf, decimal_to_american,
                                 implied_mean, no_vig_two_way, prob_over_no_push,
                                 prob_over_under)
from nhl_props.sources import game_date, load_sog_lines, parse_event_odds
from nhl_props.teams import team_abbrev


def _two_way(key, who, line, over, under):
    return {"key": key, "outcomes": [
        {"name": "Over", "description": who, "price": over, "point": line},
        {"name": "Under", "description": who, "price": under, "point": line},
    ]}


def make_event(saves_line=27.5, bos_total=2.5):
    """NYR @ BOS, 7pm ET Oct 7 — shaped like an Odds API event-odds response."""
    return {
        "id": "evt1",
        "commence_time": "2026-10-07T23:00:00Z",
        "home_team": "Boston Bruins",
        "away_team": "New York Rangers",
        "bookmakers": [
            {"key": "draftkings", "markets": [
                _two_way("player_total_saves", "Igor Shesterkin", saves_line, 1.91, 1.91),
                _two_way("player_total_saves", "Jeremy Swayman", 26.5, 1.87, 1.95),
                _two_way("team_totals", "Boston Bruins", bos_total, 1.95, 1.87),
                _two_way("team_totals", "New York Rangers", 3.5, 2.40, 1.57),
            ]},
            {"key": "fanduel", "markets": [
                _two_way("player_total_saves", "Igor Shesterkin", saves_line, 1.87, 1.95),
                # one-sided offer: must be ignored (can't de-vig it)
                {"key": "player_total_saves", "outcomes": [
                    {"name": "Over", "description": "Jeremy Swayman", "price": 1.9, "point": 25.5}]},
            ]},
        ],
    }


GOALIES = {"igor shesterkin": "NYR", "jeremy swayman": "BOS"}
SOG = {("2026-10-07", "BOS"): [{"book": "rec", "line": 28.5,
                                "over": american_to_decimal(-110),
                                "under": american_to_decimal(-110)}]}


# ── odds math ─────────────────────────────────────────────────────────────────

def test_price_conversions():
    assert american_to_decimal(-110) == pytest.approx(1.9091, abs=1e-4)
    assert american_to_decimal("+120") == pytest.approx(2.20)
    assert decimal_to_american(2.20) == "+120"
    assert decimal_to_american(1.9091) == "-110"


def test_no_vig_sums_to_one():
    p_over, p_under = no_vig_two_way(1.87, 1.95)
    assert p_over + p_under == pytest.approx(1.0)
    assert p_over > p_under


def test_poisson_matches_hand_calculation():
    # P(X > 2.5 | λ=3) = 1 − e^-3 (1 + 3 + 4.5)
    p_over, p_under, p_push = prob_over_under(2.5, 3.0)
    assert p_over == pytest.approx(1 - math.exp(-3) * 8.5)
    assert p_push == 0


def test_whole_number_line_pushes():
    p_over, p_under, p_push = prob_over_under(28, 28.0, 1.2)
    assert p_push > 0
    assert p_over + p_under + p_push == pytest.approx(1.0)


def test_negative_binomial_mean_and_variance():
    mean, disp = 28.0, 1.3
    ks = range(200)
    m = sum(k * count_pmf(k, mean, disp) for k in ks)
    var = sum((k - m) ** 2 * count_pmf(k, mean, disp) for k in ks)
    assert m == pytest.approx(mean, rel=1e-6)
    assert var == pytest.approx(mean * disp, rel=1e-4)


@pytest.mark.parametrize("line,p,disp", [(2.5, 0.55, 1.0), (27.5, 0.48, 1.2), (30, 0.6, 1.2)])
def test_implied_mean_round_trips(line, p, disp):
    mean = implied_mean(line, p, disp)
    assert prob_over_no_push(line, mean, disp) == pytest.approx(p, abs=1e-5)


# ── parsing ───────────────────────────────────────────────────────────────────

def test_team_abbrev_handles_names_and_accents():
    assert team_abbrev("Montréal Canadiens") == "MTL"
    assert team_abbrev("St. Louis Blues") == "STL"
    assert team_abbrev("nyr") == "NYR"
    assert team_abbrev("Hartford Whalers") is None


def test_game_date_uses_eastern_time():
    assert game_date("2026-10-08T00:30:00Z") == "2026-10-07"


def test_parse_event_odds_keeps_two_way_offers_only():
    parsed = parse_event_odds(make_event())
    goalies = {(o["goalie"], o["book"]) for o in parsed["saves"]}
    assert goalies == {("Igor Shesterkin", "draftkings"), ("Jeremy Swayman", "draftkings"),
                       ("Igor Shesterkin", "fanduel")}
    assert parsed["team_goals"]["BOS"] == [{"book": "draftkings", "line": 2.5,
                                            "over": 1.95, "under": 1.87}]


def test_load_sog_lines(tmp_path, capsys):
    csv_path = tmp_path / "sog.csv"
    csv_path.write_text("date,team,line,over,under,book\n"
                        "2026-10-07,Boston Bruins,28.5,,,rec\n"
                        "2026-10-07,NYR,31.5,+105,-125,dk\n"
                        "2026-10-07,Nowhere FC,30.5,,,rec\n")
    lines = load_sog_lines(csv_path)
    assert lines[("2026-10-07", "BOS")][0]["over"] == pytest.approx(american_to_decimal(-110))
    assert lines[("2026-10-07", "NYR")][0]["over"] == pytest.approx(2.05)
    assert "unknown team" in capsys.readouterr().out


# ── checker ───────────────────────────────────────────────────────────────────

def test_saves_line_too_high_flags_under():
    event = make_event(saves_line=27.5)
    rows = check_event(event, parse_event_odds(event), GOALIES, SOG)
    shesty = [r for r in rows if r["goalie"] == "Igor Shesterkin"]
    assert len(shesty) == 2
    for r in shesty:
        assert r["status"] == "ok"
        assert r["opponent"] == "BOS"
        # 28.5 SOG − 2.5 goals leaves ~26 saves, but the line is 27.5
        assert r["line_gap"] == pytest.approx(-1.5)
        assert r["implied_saves_mean"] < r["market_saves_mean"]
        assert r["side"] == "under" and r["flag"]
    # fanduel's under (1.95) beats draftkings' (1.91)
    by_book = {r["book"]: r for r in shesty}
    assert by_book["fanduel"]["ev"] > by_book["draftkings"]["ev"]


def test_consistent_lines_are_not_flagged():
    event = make_event(saves_line=25.5)
    rows = check_event(event, parse_event_odds(event), GOALIES, SOG)
    assert not any(r["flag"] for r in rows if r["goalie"] == "Igor Shesterkin")


def test_missing_inputs_explain_themselves():
    event = make_event()
    rows = check_event(event, parse_event_odds(event), {"igor shesterkin": "NYR"}, SOG)
    swayman = next(r for r in rows if r["goalie"] == "Jeremy Swayman")
    assert "not matched" in swayman["status"] and not swayman["flag"]

    rows = check_event(event, parse_event_odds(event), GOALIES, SOG)
    swayman = next(r for r in rows if r["goalie"] == "Jeremy Swayman")
    assert "no NYR SOG line" in swayman["status"]


def test_flag_thresholds_come_from_config(monkeypatch):
    monkeypatch.setattr(config, "MIN_SAVES_EDGE", 10)
    event = make_event()
    rows = check_event(event, parse_event_odds(event), GOALIES, SOG)
    assert not any(r["flag"] for r in rows)


# ── tracking / CLV ────────────────────────────────────────────────────────────

def test_record_flags_dedupes(tmp_path):
    event = make_event()
    rows = check_event(event, parse_event_odds(event), GOALIES, SOG)
    path = tmp_path / "flags.csv"
    first = tracking.record_flags(rows, path)
    second = tracking.record_flags(rows, path)
    assert len(first) == 2 and second == []


def test_clv_positive_when_line_moves_our_way(tmp_path):
    flags, snaps = tmp_path / "flags.csv", tmp_path / "snaps.csv"
    event = make_event(saves_line=27.5)
    rows = check_event(event, parse_event_odds(event), GOALIES, SOG)
    tracking.record_flags(rows, flags, flagged_at="2026-10-07T15:00:00Z")

    # By puck drop the under 27.5 has moved to 26.5
    closing = check_event(make_event(saves_line=26.5), parse_event_odds(make_event(saves_line=26.5)),
                          GOALIES, SOG)
    tracking.record_snapshots(closing, snaps, captured_at="2026-10-07T22:55:00Z")
    # a snapshot after puck drop must be ignored
    tracking.record_snapshots(rows, snaps, captured_at="2026-10-07T23:30:00Z")

    report = tracking.clv_report(flags, snaps, now="2026-10-08T03:00:00Z")
    assert len(report) == 2
    for r in report:
        assert r["close_line"] == 26.5
        assert r["clv_ev"] > 0


# ── end-to-end with a fake API client ─────────────────────────────────────────

class FakeClient:
    remaining_requests = "499"

    def events(self, hours_ahead):
        e = make_event()
        return [{k: e[k] for k in ("id", "commence_time", "home_team", "away_team")}]

    def event_odds(self, event_id):
        return make_event()


def test_run_scan(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SNAPSHOTS_CSV", tmp_path / "snaps.csv")
    monkeypatch.setattr(config, "FLAGS_CSV", tmp_path / "flags.csv")
    monkeypatch.setattr(scan, "load_goalie_teams", lambda teams: GOALIES)
    monkeypatch.setattr(scan, "load_sog_lines", lambda: SOG)

    result = scan.run_scan(client=FakeClient())
    assert len(result["rows"]) == 3
    assert {r["goalie"] for r in result["flags"]} == {"Igor Shesterkin"}
    assert (tmp_path / "snaps.csv").exists() and (tmp_path / "flags.csv").exists()
    assert result["remaining_requests"] == "499"


def test_goalie_overrides_win(tmp_path, monkeypatch):
    overrides = tmp_path / "goalie_teams.csv"
    overrides.write_text("goalie,team\nJeremy Swayman,NYR\n")
    monkeypatch.setattr(sources, "fetch_team_goalies", lambda team: {"BOS": ["Jeremy Swayman"]}[team])
    mapping = sources.load_goalie_teams({"BOS"}, cache_dir=tmp_path, overrides_csv=overrides)
    assert mapping["jeremy swayman"] == "NYR"


def test_model_weight_scales_ev(monkeypatch):
    event = make_event()
    parsed = parse_event_odds(event)
    monkeypatch.setattr(config, "MODEL_WEIGHT", 1.0)
    full = next(r for r in check_event(event, parsed, GOALIES, SOG) if r["book"] == "draftkings")
    monkeypatch.setattr(config, "MODEL_WEIGHT", 0.0)
    none = next(r for r in check_event(event, parsed, GOALIES, SOG) if r["book"] == "draftkings")
    assert none["blended_saves_mean"] == none["market_saves_mean"]
    assert none["ev"] < 0 < full["ev"]          # trusting only the saves market = just paying vig


def test_flask_route(tmp_path, monkeypatch):
    app_module = pytest.importorskip("app")
    monkeypatch.setattr(config, "SNAPSHOTS_CSV", tmp_path / "snaps.csv")
    monkeypatch.setattr(config, "FLAGS_CSV", tmp_path / "flags.csv")
    monkeypatch.setattr(scan, "load_goalie_teams", lambda teams: GOALIES)
    monkeypatch.setattr(scan, "load_sog_lines", lambda: SOG)
    monkeypatch.setattr(sources, "OddsApiClient", lambda key, books: FakeClient())

    client = app_module.app.test_client()
    data = client.get("/api/nhl/saves").get_json()
    assert len(data["flags"]) == 2
    assert client.get("/nhl-saves").status_code == 200
