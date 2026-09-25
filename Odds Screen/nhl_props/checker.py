"""
Cross-market consistency check for goalie saves props.

Shots on goal a team takes are either saved by the goalie in net, go in, or
are faced by a relief goalie after the starter is pulled:

    opponent SOG = starter saves + opponent goals + shots after a pull

Empty-net goals are shots on goal AND goals, so using the opponent's team goal
total (which already includes empty-netters) cancels them out exactly. This
replaces the "+1 for the empty-netter" rule of thumb. So:

    implied saves = E[opponent SOG] − (E[opponent goals] − shootout adj) − pulled-shots adj

Every market's line + price is converted to an implied mean first, so a
2.5 goals line at −160 counts differently from 2.5 at +110. Bets are priced
off a blend of the saves market and the implied number (config.MODEL_WEIGHT).
"""

from statistics import median

from . import config
from .odds_math import (decimal_to_american, expected_value, implied_mean,
                        no_vig_two_way, prob_over_under)
from .sources import game_date
from .teams import normalize, team_abbrev


def consensus(offers: list[dict], dispersion: float) -> tuple[float, float]:
    """
    Average implied mean across books, and the median line.

    Each offer is {line, over, under} in decimal odds.
    """
    means = [implied_mean(o["line"], no_vig_two_way(o["over"], o["under"])[0], dispersion)
             for o in offers]
    return sum(means) / len(means), median(o["line"] for o in offers)


def implied_saves_mean(sog_mean: float, goals_mean: float) -> float:
    return sog_mean - (goals_mean - config.SHOOTOUT_GOAL_ADJ) - config.PULLED_SHOTS_ADJ


def check_event(event: dict, parsed: dict, goalie_teams: dict[str, str],
                sog_lines: dict[tuple[str, str], list[dict]]) -> list[dict]:
    """
    One row per (goalie, book) saves offer in this game.

    event        Odds API event dict (id, home_team, away_team, commence_time)
    parsed       output of sources.parse_event_odds(event)
    goalie_teams {normalized goalie name: team abbrev}
    sog_lines    output of sources.load_sog_lines()
    """
    home = team_abbrev(event.get("home_team", ""))
    away = team_abbrev(event.get("away_team", ""))
    date = game_date(event["commence_time"])

    offers_by_goalie: dict[str, list[dict]] = {}
    for offer in parsed["saves"]:
        offers_by_goalie.setdefault(offer["goalie"], []).append(offer)

    rows = []
    for goalie, offers in offers_by_goalie.items():
        team = goalie_teams.get(normalize(goalie))
        opponent = {home: away, away: home}.get(team)
        market_mean, market_line = consensus(offers, config.SAVES_DISPERSION)

        base = {
            "event_id": event.get("id"),
            "commence_time": event["commence_time"],
            "game_date": date,
            "game": f"{away} @ {home}",
            "goalie": goalie,
            "team": team,
            "opponent": opponent,
            "market_saves_mean": round(market_mean, 2),
            "market_saves_line": market_line,
        }

        # What we need from the other markets, and why a row can't be checked.
        status = "ok"
        sog_offers = sog_lines.get((date, opponent)) if opponent else None
        goals_offers = parsed["team_goals"].get(opponent) if opponent else None
        if not opponent:
            status = "goalie not matched to a team in this game (add to goalie_teams.csv)"
        elif not sog_offers:
            status = f"no {opponent} SOG line in team_sog_lines.csv for {date}"
        elif not goals_offers:
            status = f"no {opponent} team total posted yet"

        model = {}
        if status == "ok":
            sog_mean, sog_line = consensus(sog_offers, config.SOG_DISPERSION)
            goals_mean, goals_line = consensus(goals_offers, config.GOALS_DISPERSION)
            model_mean = implied_saves_mean(sog_mean, goals_mean)
            # The mean used to price bets: part way from the saves market toward
            # the cross-market number, since either market could be the stale one.
            blended_mean = market_mean + config.MODEL_WEIGHT * (model_mean - market_mean)
            model = {
                "sog_line": sog_line,
                "sog_mean": round(sog_mean, 2),
                "goals_line": goals_line,
                "goals_mean": round(goals_mean, 2),
                "implied_saves_mean": round(model_mean, 2),
                "blended_saves_mean": round(blended_mean, 2),
                "saves_edge": round(model_mean - market_mean, 2),
                # Your original line-only rule: > 0 leans over, < 0 leans under.
                "line_gap": round(sog_line - market_line - goals_line, 2),
            }

        for offer in offers:
            row = {**base, **model, "status": status,
                   "book": offer["book"], "line": offer["line"],
                   "over_dec": offer["over"], "under_dec": offer["under"],
                   "over": decimal_to_american(offer["over"]),
                   "under": decimal_to_american(offer["under"]),
                   "market_fair_over": round(no_vig_two_way(offer["over"], offer["under"])[0], 4),
                   "side": None, "ev": None, "flag": False}
            if model:
                _price_offer(row, blended_mean)
            rows.append(row)
    return rows


def _price_offer(row: dict, saves_mean: float) -> None:
    """Fill in model probabilities, EV for each side, and whether to flag it."""
    p_over, p_under, p_push = prob_over_under(row["line"], saves_mean, config.SAVES_DISPERSION)
    ev_over = expected_value(p_over, p_under, row["over_dec"])
    ev_under = expected_value(p_under, p_over, row["under_dec"])
    side, ev = ("over", ev_over) if ev_over >= ev_under else ("under", ev_under)

    edge_toward_side = row["saves_edge"] if side == "over" else -row["saves_edge"]
    row.update({
        "model_p_over": round(p_over, 4),
        "model_p_under": round(p_under, 4),
        "model_p_push": round(p_push, 4),
        "ev_over": round(ev_over, 4),
        "ev_under": round(ev_under, 4),
        "side": side,
        "ev": round(ev, 4),
        "flag": ev >= config.MIN_EV and edge_toward_side >= config.MIN_SAVES_EDGE,
    })
