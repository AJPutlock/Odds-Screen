"""
How strongly do two bets on one game win together? Phi coefficients (the
correlation of two win/lose outcomes) from real results, for the odds screen's
position sizing and spread/total warnings.

  NFL:  NFL Data/data/nfl_games.csv (nflverse pregame lines, 2015+, 1H scores)
  CFB:  the 2025 games + lines fit_cfb_outcomes.load() pairs up

Pairs measured (pushes dropped):
  fav cover  x  Over            full game, by size of spread  (dog cover x Under is
                                the same number; fav x Under is its negative)
  fav cover 1H x Over 1H        1H line ~ 0.5 x spread, 1H total ~ 0.5 x total
  same team: cover at L  x  cover at L + d        d = 1..7 points
  same team: full-game cover  x  1H cover
  same team: moneyline  x  cover
  Over full  x  Over 1H
Run: python tools/bet_correlations.py
"""
import csv
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))


def half(x):
    """Round to the nearest half point, off the whole number (no push)."""
    return math.floor(x) + 0.5


def phi(pairs):
    pairs = [(a, b) for a, b in pairs if a is not None and b is not None]
    n = len(pairs)
    if n < 30:
        return None, n
    pa = sum(a for a, _ in pairs) / n
    pb = sum(b for _, b in pairs) / n
    pab = sum(a and b for a, b in pairs) / n
    den = math.sqrt(pa * (1 - pa) * pb * (1 - pb))
    return (pab - pa * pb) / den if den else None, n


def covers(margin, line):
    """Did the side with this margin (its own points scored minus allowed) cover `line`?"""
    v = margin + line
    return None if v == 0 else v > 0


def nfl_games():
    out = []
    for r in csv.DictReader(open(ROOT / "NFL Data" / "data" / "nfl_games.csv", encoding="utf-8")):
        if r["completed"] != "1" or not r["spread_line"] or not r["total_line"]:
            continue
        hp, ap = int(r["home_points"]), int(r["away_points"])
        h1 = int(r["home_1h"]) if r["home_1h"] else None
        a1 = int(r["away_1h"]) if r["away_1h"] else None
        # nflverse spread_line = expected home margin (positive: home favored)
        hq = int(r["home_q1"]) if r["home_q1"] else None
        aq = int(r["away_q1"]) if r["away_q1"] else None
        out.append({"mu": float(r["spread_line"]), "tau": float(r["total_line"]),
                    "m": hp - ap, "t": hp + ap,
                    "m1": None if h1 is None else h1 - a1, "t1": None if h1 is None else h1 + a1,
                    "mq": None if hq is None or (hq == 0 and aq == 0 and hp + ap > 20 and h1 is None) else hq - aq,
                    "tq": None if hq is None else hq + aq})
    return out


def cfb_games():
    from fit_cfb_outcomes import load
    _, lined = load()
    return [{"mu": g["mu"], "tau": g["tau"], "m": g["out"]["spreads"], "t": g["out"]["totals"],
             "m1": g["out"]["spreads_h1"], "t1": g["out"]["totals_h1"],
             "mq": g["out"]["spreads_q1"], "tq": g["out"]["totals_q1"]} for g in lined]


def study(name, games):
    print(f"\n=== {name}: {len(games)} games ===")
    fav = []
    for g in games:
        if g["mu"] == 0:
            continue
        s = 1 if g["mu"] > 0 else -1                 # +1: home favored
        fav.append({**g, "fm": s * g["m"], "fm1": None if g["m1"] is None else s * g["m1"],
                    "fmq": None if g.get("mq") is None else s * g["mq"], "spr": abs(g["mu"])})

    print("fav cover x Over (full game), by spread:")
    for lo, hi in ((0, 3.5), (3.5, 7.5), (7.5, 14.5), (14.5, 99), (0, 99)):
        sel = [g for g in fav if lo < g["spr"] <= hi]
        r, n = phi([(covers(g["fm"], -g["spr"]), None if g["t"] == g["tau"] else g["t"] > g["tau"]) for g in sel])
        print(f"  {lo:>4}-{hi:<4} n={n:5d}  phi={r:+.3f}" if r is not None else f"  {lo}-{hi} n={n}")

    r, n = phi([(covers(g["fm1"], -half(g["spr"] / 2)), None if g["t1"] is None else g["t1"] > half(g["tau"] / 2))
                for g in fav if g["fm1"] is not None])
    print(f"fav cover 1H x Over 1H           n={n:5d}  phi={r:+.3f}")

    print("same team, cover at L x cover at L+d (favorite's line, both sides of it):")
    for d in (1, 2, 3, 4, 5, 7, 10):
        r, n = phi([(covers(g["fm"], -g["spr"]), covers(g["fm"], -g["spr"] + d)) for g in fav])
        print(f"  d={d:<2} n={n:5d}  phi={r:+.3f}")

    r, n = phi([(covers(g["fm"], -g["spr"]), covers(g["fm1"], -half(g["spr"] / 2))) for g in fav if g["fm1"] is not None])
    print(f"same team full cover x 1H cover  n={n:5d}  phi={r:+.3f}")
    r, n = phi([(None if g["fm"] == 0 else g["fm"] > 0, covers(g["fm"], -g["spr"])) for g in fav])
    print(f"fav moneyline x fav cover        n={n:5d}  phi={r:+.3f}")
    r, n = phi([(None if g["fm"] == 0 else g["fm"] < 0, covers(-g["fm"], g["spr"])) for g in fav])
    print(f"dog moneyline x dog cover        n={n:5d}  phi={r:+.3f}")
    r, n = phi([(None if g["t"] == g["tau"] else g["t"] > g["tau"],
                 None if g["t1"] is None else g["t1"] > half(g["tau"] / 2)) for g in games])
    print(f"Over full x Over 1H              n={n:5d}  phi={r:+.3f}")
    q = [g for g in fav if g["fmq"] is not None and g["fm1"] is not None]
    r, n = phi([(covers(g["fm"], -g["spr"]), covers(g["fmq"], -half(g["spr"] / 4))) for g in q])
    print(f"same team full cover x Q1 cover  n={n:5d}  phi={r:+.3f}")
    r, n = phi([(covers(g["fm1"], -half(g["spr"] / 2)), covers(g["fmq"], -half(g["spr"] / 4))) for g in q])
    print(f"same team 1H cover x Q1 cover    n={n:5d}  phi={r:+.3f}")
    r, n = phi([(None if g["t"] == g["tau"] else g["t"] > g["tau"],
                 None if g.get("tq") is None else g["tq"] > half(g["tau"] / 4)) for g in games])
    print(f"Over full x Over Q1              n={n:5d}  phi={r:+.3f}")
    r, n = phi([(None if g["t1"] is None else g["t1"] > half(g["tau"] / 2),
                 None if g.get("tq") is None else g["tq"] > half(g["tau"] / 4)) for g in games])
    print(f"Over 1H x Over Q1                n={n:5d}  phi={r:+.3f}")
    print("Over at T x Over at T+d (full game):")
    for d in (1, 2, 3, 5, 7, 10):
        r, n = phi([(None if g["t"] == g["tau"] else g["t"] > g["tau"],
                     None if g["t"] == g["tau"] + d else g["t"] > g["tau"] + d) for g in games])
        print(f"  d={d:<2} n={n:5d}  phi={r:+.3f}")


if __name__ == "__main__":
    study("NFL 2015+", nfl_games())
    study("CFB 2025", cfb_games())
