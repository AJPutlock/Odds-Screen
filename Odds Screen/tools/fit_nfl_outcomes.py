"""
Fit the NFL outcome model embedded in static/index.html as FB_EMPIRICAL.americanfootball_nfl
— the same model as tools/fit_cfb_outcomes.py (see its docstring), fit to NFL data:

  P(outcome = k | market line) ∝ shape((k - line) / scale) * mult(|k|)

Unlike CFB, every NFL game has a pregame spread and total (nflverse: the last line
it recorded before kickoff — near-close, but the book isn't documented; ~2.45% hold
2015-22 looks like Pinnacle, -110 retail 2023+; results scatter around both alike), so:
- scale/tail are fit on 2015-2024 lines and scored out of sample on 2025-26;
- mult (key-number multipliers) come from results only — train seasons for
  the out-of-sample score, all seasons for the emitted constant;
- 1H/Q1 lines aren't in the data, so their scatter is measured around a fitted
  share of the full-game line (same as CFB).

Validation 2026-09-23 (Bookmaker's own NFL alt ladders, 77 ladders / 615 half-
point steps): mean error vs the ladder, old scoring-play model -> this fit:
spreads 0.83 -> 0.44pp (through 3/7: 1.92 -> 0.56), 1H spreads 0.51 -> 0.42,
Q1 spreads 1.39 -> 0.42, totals 0.44 -> 0.37, 1H totals 0.89 -> 0.53, Q1
totals 1.25 -> 0.66. Watch items: totals half points run ~0.3pp richer than
Bookmaker's ladders (not vig: the gap is there at the main line, and de-vig
method moves it <0.1pp; actual results sit between the two), 3/7 steps ~0.2pp
cheaper, and lines sitting on 3 land on 3 a little more than modeled (10.3% vs
8.2%, 439 games, ~1.6 SE).

Data: ../NFL Data/data/nfl_games.csv, built by tools/fetch_nfl_data.py (re-run
both each season).
Usage: python tools/fit_nfl_outcomes.py   -> prints the JS entry + report
"""
import csv, json, math, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fit_cfb_outcomes import (MARKETS, SPAN, KEY_MIN_MULT, NUS, outcomes, line_key, fit_center,
                              multipliers, pmf, mean_ll, fit_scale, fit_key_boost)

ROOT = Path(__file__).resolve().parents[2]
TEST_FROM = 2025
KEY_MIN_SHARE = {"spreads": 0.02, "totals": 0.02, "spreads_h1": 0.04, "totals_h1": 0.04,
                 "spreads_q1": 0.04, "totals_q1": 0.04}
# Full-game margins of 10 and 17 spike at 1.45-1.47x (3.5-4.7% of games), just
# under the shared 1.5 cutoff. Keys only feed the safety gates (more keys =
# stricter), so the NFL spread cutoff is lowered to include them.
KEY_MIN_MULT_BY_MKT = {"spreads": 1.4}


def load():
    games = list(csv.DictReader(open(ROOT / "NFL Data" / "data" / "nfl_games.csv", encoding="utf-8")))
    lined = [{"season": int(r["season"]), "mu": float(r["spread_line"]), "tau": float(r["total_line"]),
              "out": outcomes(r)} for r in games]
    return games, lined


def main():
    games, lined = load()
    train_g = [r for r in games if int(r["season"]) < TEST_FROM]
    train = [x for x in lined if x["season"] < TEST_FROM]
    test = [x for x in lined if x["season"] >= TEST_FROM]
    out, report = {}, {"games": len(games), "train_games": len(train), "test_games": len(test)}
    for mkt in MARKETS:
        alpha, beta = fit_center(mkt, train)
        center_fn = lambda x, a=alpha, b=beta, lk=line_key(mkt): a + b * x[lk]
        m_train, _ = multipliers(mkt, train_g)
        by_nu = {str(nu): fit_scale(mkt, train, center_fn, nu, m_train) for nu in NUS}
        best = max(by_nu, key=lambda k: by_nu[k][1])
        nu = None if best == "None" else int(best)
        scale = round(by_nu[best][0], 3)
        plain_scale, _ = fit_scale(mkt, train, center_fn, None, {})
        m_all, share = multipliers(mkt, games)
        min_mult = KEY_MIN_MULT_BY_MKT.get(mkt, KEY_MIN_MULT)
        keys = sorted(k for k in m_all if m_all[k] >= min_mult and share[k] >= KEY_MIN_SHARE[mkt]
                      and not (mkt == "spreads" and k == 0))
        out[mkt] = {"scale": scale, "nu": nu, "keys": keys, "mult": [round(m_all[k], 3) for k in range(SPAN[mkt] + 1)]}
        rep = {"center": [round(alpha, 3), round(beta, 3)], "scale": scale, "nu": nu, "keys": keys,
               "oos_loglik_plain_normal": round(mean_ll(mkt, test, center_fn, plain_scale, None, {}), 4),
               "oos_loglik_model": round(mean_ll(mkt, test, center_fn, scale, nu, m_train), 4),
               "top_mults": sorted(((k, round(m_all[k], 2), round(share[k], 3)) for k in m_all),
                                   key=lambda t: -t[1])[:10]}
        if mkt == "spreads":
            boost, n, obs, pred1 = fit_key_boost(test, scale, nu, m_train, set(keys))
            rep["key_line_landings_test"] = {"games": n, "observed": obs, "model": round(pred1, 1),
                                             "boost_it_would_imply_not_used": boost}
        report[mkt] = rep
    print("americanfootball_nfl: " + json.dumps(out, separators=(",", ":")))
    print(json.dumps(report, indent=1), file=sys.stderr)


if __name__ == "__main__":
    main()
