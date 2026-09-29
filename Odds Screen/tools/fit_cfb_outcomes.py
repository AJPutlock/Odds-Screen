"""
Fit the CFB outcome model embedded in static/index.html as FB_EMPIRICAL.

  P(outcome = k | market line) ∝ shape((k - line) / scale) * mult(|k|)

- scale, tail (nu): how widely real results scatter around the market line,
  fit by out-of-sample likelihood on 2025 games with Pinnacle/LowVig/BetOnline
  lines (1H/Q1 lines aren't stored, so their scatter is measured around a
  fitted share of the full-game line).
- mult: key-number multipliers — how much more (or less) often each exact
  value occurs than a smooth curve predicts, from the pooled 2018-2025
  histogram. Spikes are separated from the smooth shape iteratively so
  neighboring spikes don't inflate each other's baseline; thin-data values
  are shrunk toward 1.
- key_boost (report only, NOT emitted): 2025 results landed on a key number
  the spread sat on more often than the pooled multiplier implies (17 of 207
  vs 9.8 modeled), but a boost fit to that thin sample overvalued half points
  around 3 vs Bookmaker's own alt-ladder pricing (+0.22pp bias on key-number
  steps, 327 real ladder steps, 2026-09-23); without it the model tracks the
  ladder with ~0 bias. The market wins until real closing-line data says otherwise.
- keys: values used by the app's key-number safety gates — multiplier >= 1.5
  AND at least KEY_MIN_SHARE of all games (quarters are lumpy enough that
  multipliers alone flag rare values like a 13-point Q1 total).

Data: ../CFB Data/data/games.csv (results) and ../Answer Key/odds_cache_2025.json (lines).
The 2025 lines are pregame snapshots from a few pulls at different times, NOT
closing lines. Earlier lines are a little less accurate than closes, so the
fitted scatter runs slightly wide and the key_boost slightly low — both make
the model value half points conservatively. Multipliers use results only.
To refit on real closes: the last pre-kickoff row per game in
data/odds_history.db (hourly capture) is within an hour of the close.
Usage: python tools/fit_cfb_outcomes.py        -> prints the JS constant + validation report
"""
import csv, json, math, re, statistics as st, sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MARKETS = ["spreads", "spreads_h1", "spreads_q1", "totals", "totals_h1", "totals_q1"]
SPAN = {"spreads": 75, "spreads_h1": 50, "spreads_q1": 35, "totals": 130, "totals_h1": 85, "totals_q1": 55}
BW = {"spreads": 3.0, "spreads_h1": 2.5, "spreads_q1": 2.0, "totals": 4.0, "totals_h1": 3.0, "totals_q1": 2.5}
KEY_MIN_MULT = 1.5
KEY_MIN_SHARE = {"spreads": 0.02, "totals": 0.025, "spreads_h1": 0.04, "totals_h1": 0.04,
                 "spreads_q1": 0.04, "totals_q1": 0.04}
SHRINK = 25   # pseudo-count pulling thin-data multipliers toward 1
NUS = (None, 30, 12, 8, 5, 4)


def outcomes(r):
    hp, ap = int(r["home_points"]), int(r["away_points"])
    h1, a1 = int(r["home_1h"]), int(r["away_1h"])
    hq, aq = int(r["home_q1"]), int(r["away_q1"])
    return {"spreads": hp - ap, "totals": hp + ap, "spreads_h1": h1 - a1, "totals_h1": h1 + a1,
            "spreads_q1": hq - aq, "totals_q1": hq + aq}


def load():
    games = [r for r in csv.DictReader(open(ROOT / "CFB Data" / "data" / "games.csv", encoding="utf-8"))
             if r["completed"] == "1" and r["home_points"] and r["away_points"]]
    oc = json.load(open(ROOT / "Answer Key" / "odds_cache_2025.json", encoding="utf-8"))["games"]
    norm = lambda s: set(re.sub(r"[^a-z0-9 ]", " ", s.lower()).split())
    g25 = [r for r in games if r["season"] == "2025"]
    lined = []
    for o in oc.values():
        d = o["utc_time"][:10]
        best, bs = None, 0
        for r in g25:
            if abs((int(r["start_date"][8:10]) + 31 * int(r["start_date"][5:7])) - (int(d[8:10]) + 31 * int(d[5:7]))) > 1:
                continue
            h, a = norm(r["home_team"]), norm(r["away_team"])
            if h <= norm(o["home"]) and a <= norm(o["away"]) and len(h) + len(a) > bs:
                best, bs = r, len(h) + len(a)
        b = o["odds"].get("pinnacle") or o["odds"].get("lowvig") or o["odds"].get("betonlineag")
        if best and b and b.get("sprd_point") is not None and b.get("total_point") is not None:
            # sprd_point is the HOME spread; expected home margin = -sprd_point
            lined.append({"mu": -b["sprd_point"], "tau": b["total_point"], "out": outcomes(best)})
    return games, lined


def line_key(mkt):
    return "mu" if mkt.startswith("spreads") else "tau"


def fit_center(mkt, lined):
    xs = [x[line_key(mkt)] for x in lined]; ys = [x["out"][mkt] for x in lined]
    if mkt.startswith("spreads"):
        return 0.0, sum(x * y for x, y in zip(xs, ys)) / sum(x * x for x in xs)
    mx, my = st.mean(xs), st.mean(ys)
    beta = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    return my - beta * mx, beta


def shape(z, nu):
    return math.exp(-0.5 * z * z) if nu is None else (1 + z * z / nu) ** (-(nu + 1) / 2)


def multipliers(mkt, games):
    vals = [outcomes(r)[mkt] for r in games]
    if mkt.startswith("spreads"):
        vals = [abs(v) for v in vals]
    H = Counter(vals)
    share0 = H.get(0, 0) / len(vals)
    if mkt.startswith("spreads"):
        # Folding margins pools +k and -k into |k| for every k except 0, so the
        # tie count is on half the density scale of its neighbors. Double it so
        # mult[0] is a spike ratio on the same footing as mult[k] (undoubled, a
        # Q1 tie priced at ~12pp vs ~23pp on Bookmaker's NFL ladders).
        H[0] = 2 * H.get(0, 0)
    ks = range(0, SPAN[mkt] + 1)
    kern = {d: math.exp(-0.5 * (d / BW[mkt]) ** 2) for d in range(-SPAN[mkt], SPAN[mkt] + 1)}
    m = {k: 1.0 for k in ks}
    for _ in range(40):
        despiked = {k: H.get(k, 0) / m[k] for k in ks}
        S = {k: sum(despiked[j] * kern[j - k] for j in ks) / sum(kern[j - k] for j in ks) for k in ks}
        new = {k: (H.get(k, 0) + SHRINK) / (S[k] + SHRINK) for k in ks}
        done = max(abs(new[k] - m[k]) for k in ks) < 1e-4
        m = new
        if done:
            break
    share = {k: H.get(k, 0) / len(vals) for k in ks}
    share[0] = share0
    return m, share


def pmf(mkt, center, scale, nu, mult, key_boost=1.0, keys=()):
    ks = range(-SPAN[mkt], SPAN[mkt] + 1) if mkt.startswith("spreads") else range(0, SPAN[mkt] + 1)
    w = {}
    for k in ks:
        v = shape((k - center) / scale, nu) * mult.get(abs(k), 1.0)
        if key_boost != 1.0 and abs(k) in keys and abs(k - center) <= 0.5:
            v *= key_boost
        w[k] = v
    z = sum(w.values())
    return {k: v / z for k, v in w.items()}


def mean_ll(mkt, lined, center_fn, scale, nu, mult):
    return sum(math.log(max(pmf(mkt, center_fn(x), scale, nu, mult).get(x["out"][mkt], 1e-12), 1e-12))
               for x in lined) / len(lined)


def fit_scale(mkt, lined, center_fn, nu, mult):
    lo, hi = 2.0, 25.0
    g = (math.sqrt(5) - 1) / 2
    f = lambda s: mean_ll(mkt, lined, center_fn, s, nu, mult)
    a, b = hi - g * (hi - lo), lo + g * (hi - lo)
    fa, fb = f(a), f(b)
    for _ in range(30):
        if fa > fb:
            hi, b, fb = b, a, fa
            a = hi - g * (hi - lo); fa = f(a)
        else:
            lo, a, fa = a, b, fb
            b = lo + g * (hi - lo); fb = f(b)
    s = (lo + hi) / 2
    return s, f(s)


def fit_key_boost(lined, scale, nu, mult, keys):
    """Boost at a key number the spread sits on, matched to (observed - 1 SE) landings on 2025 integer key lines."""
    sel = [x for x in lined if abs(x["mu"] - round(x["mu"])) <= 0.26 and abs(round(x["mu"])) in keys]
    obs = sum(1 for x in sel if x["out"]["spreads"] == round(x["mu"]))
    target = obs - math.sqrt(obs)

    def pred(b):
        return sum(pmf("spreads", x["mu"], scale, nu, mult, b, keys)[round(x["mu"])] for x in sel)
    if pred(1.0) >= target:
        return 1.0, len(sel), obs, pred(1.0)
    lo, hi = 1.0, 4.0
    for _ in range(40):
        b = (lo + hi) / 2
        (lo, hi) = (b, hi) if pred(b) < target else (lo, b)
    return round((lo + hi) / 2, 3), len(sel), obs, pred(1.0)


def main():
    games, lined = load()
    train = [r for r in games if r["season"] != "2025"]
    out, report = {}, {"results_games": len(games), "lined_2025_games": len(lined)}
    for mkt in MARKETS:
        alpha, beta = fit_center(mkt, lined)
        center_fn = lambda x, a=alpha, b=beta, lk=line_key(mkt): a + b * x[lk]
        m_train, _ = multipliers(mkt, train)
        by_nu = {str(nu): fit_scale(mkt, lined, center_fn, nu, m_train) for nu in NUS}
        best = max(by_nu, key=lambda k: by_nu[k][1])
        nu = None if best == "None" else int(best)
        scale = round(by_nu[best][0], 3)
        _, ll_plain = fit_scale(mkt, lined, center_fn, None, {})
        m_all, share = multipliers(mkt, games)
        keys = sorted(k for k in m_all if m_all[k] >= KEY_MIN_MULT and share[k] >= KEY_MIN_SHARE[mkt]
                      and not (mkt == "spreads" and k == 0))
        entry = {"scale": scale, "nu": nu, "keys": keys, "mult": [round(m_all[k], 3) for k in range(SPAN[mkt] + 1)]}
        rep = {"oos_loglik_plain_normal": round(ll_plain, 4), "oos_loglik_model": round(by_nu[best][1], 4),
               "scale": scale, "nu": nu, "keys": keys}
        if mkt == "spreads":
            boost, n, obs, pred1 = fit_key_boost(lined, scale, nu, m_train, set(keys))
            rep["key_line_landings_2025"] = {"games": n, "observed": obs, "model": round(pred1, 1),
                                             "boost_it_would_imply_not_used": boost}
        out[mkt] = entry
        report[mkt] = rep
    print("const FB_EMPIRICAL = { americanfootball_ncaaf: " + json.dumps(out, separators=(",", ":")) + " };")
    print(json.dumps(report, indent=1), file=sys.stderr)


if __name__ == "__main__":
    main()
