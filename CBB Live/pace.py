"""
Live fair-value model for men's college basketball (score-only null, v1).

Same interface as CFB Live's pace.evaluate() so game_manager is unchanged.
The model is the calibrated rest-of-game distribution from
NCAAB Live/ncaa_betting_app/backtest/calibrate_null.py (copied here as
model_calibration.json; 9,541 games, 2023-24 + 2024-25, ESPN BET close):

    rest_margin ~ N(a_m(e)*mu*f + b_m(e)*dev_m, sd_m(e)^2)
    rest_total  ~ N(a_t(e)*T*f  + b_t(e)*dev_t, sd_t(e)^2)

f = share of regulation left, mu = -pregame home spread, T = pregame total,
dev = how far the score has run ahead of the line so far. What it captures
that a naive Brownian line does not: rest-of-game totals run ~+3.8 pts over
T*f by halftime (2H free throws), margin deviations revert ~20-30%, and late
variance is 10-20% fatter.

WHAT IT IS NOT (yet): it reads only the scoreboard + the pregame line. Tested
on 3 days of live DK/FD quotes it priced about as well as the books, not
better. The xScore layer (shot-quality residuals) is the planned edge source;
until it lands, an "edge" shown here is model disagreement, not a bet signal.
"""
import bisect
import json
import os
from math import erf, sqrt

_CAL = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_calibration.json")))
_E = sorted(int(k) for k in _CAL if int(k) > 0)
REG = 2400

MODEL_VERSION = "score-only null v1 (calibrated 23-24+24-25)"


def _phi(x):
    return 0.5 * (1 + erf(x / sqrt(2)))


def _coefs(e, kind):
    e = min(max(e, _E[0]), _E[-1])
    i = bisect.bisect_right(_E, e) - 1
    lo, hi = _E[i], _E[min(i + 1, len(_E) - 1)]
    w = 0 if hi == lo else (e - lo) / (hi - lo)
    c0, c1 = _CAL[str(lo)][kind], _CAL[str(hi)][kind]
    return tuple(c0[k] * (1 - w) + c1[k] * w for k in ("a", "b", "sd"))


def evaluate(elapsed_seconds=None, home_score=None, away_score=None, pregame_total=None,
             pregame_spread_home=None, live_total=None, live_spread_home=None, **_ignored):
    view = {
        "model": MODEL_VERSION,
        "elapsed_seconds": elapsed_seconds,
        "points_scored": (home_score or 0) + (away_score or 0),
        "fair_total": None, "total_sd": None, "p_over_live": None, "total_edge": None,
        "fair_spread_home": None, "margin_sd": None, "p_home_cover_live": None, "spread_edge": None,
        "home_wp": None,
        "notes": [],
    }
    if elapsed_seconds is None or home_score is None or away_score is None:
        view["notes"].append("no live game state")
        return view
    e = min(elapsed_seconds, REG)
    f = (REG - e) / REG
    if f <= 0:
        view["notes"].append("regulation over - overtime not modelled")
        return view
    margin, total = home_score - away_score, home_score + away_score

    if pregame_spread_home is not None:
        mu = -pregame_spread_home
        a, b, sd = _coefs(e, "margin")
        mean = margin + a * mu * f + b * (margin - mu * (1 - f))
        view["fair_spread_home"] = round(-mean, 1)
        view["margin_sd"] = round(sd, 2)
        view["home_wp"] = round(1 - _phi((0 - mean) / sd), 4)
        if live_spread_home is not None:
            p = 1 - _phi((-live_spread_home - mean) / sd)
            view["p_home_cover_live"] = round(p, 4)
            view["spread_edge"] = round(p - 0.5, 4)      # vs a fair 50/50 at the live number
    else:
        view["notes"].append("no pregame spread - margin model unavailable")

    if pregame_total is not None:
        a, b, sd = _coefs(e, "total")
        mean = total + a * pregame_total * f + b * (total - pregame_total * (1 - f))
        view["fair_total"] = round(mean, 1)
        view["total_sd"] = round(sd, 2)
        if live_total is not None:
            p = 1 - _phi((live_total - mean) / sd)
            view["p_over_live"] = round(p, 4)
            view["total_edge"] = round(p - 0.5, 4)
    else:
        view["notes"].append("no pregame total - total model unavailable")
    return view


def p_cover(view, home_line):
    """P(home covers `home_line`) from an evaluate() view, for pricing any posted number."""
    if view.get("fair_spread_home") is None:
        return None
    mean = -view["fair_spread_home"]
    return 1 - _phi((-home_line - mean) / view["margin_sd"])


def p_over(view, line):
    if view.get("fair_total") is None:
        return None
    return 1 - _phi((line - view["fair_total"]) / view["total_sd"])
