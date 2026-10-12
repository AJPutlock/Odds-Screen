"""
Backtest of the 1H/Q1 full-game shift in static/index.html (fbPeriodFgAdj).

The live rule (10/12, provisional, NCAAF only): a football 1H/Q1 sharp price
priced off Bookmaker / BetOnline is shifted by
    K x [derived(Circa full-game 50/50 line) - derived(the period book's full-game line)]
capped at +-CAP, where derived() is the period outcome model centred on
a + b x the full-game line (FB_PERIOD_CENTER, from fit_center in
fit_cfb_outcomes.py / fit_nfl_outcomes.py) and FB_EMPIRICAL's period shapes.

For each game x period market (spread, total, period ML) x horizon:
  BK      Bookmaker's period fair (power de-vig) from the archived pull nearest
          `horizon` hours before kickoff, carried to BetOnline's closing period
          line with the period model
  shifted BK + clamp(K x [derived(Circa full game) - derived(Bookmaker full game, same pull)])
  target  BetOnline's period close (odds_api_close / odds_api_hist_close)
All on the upper side (Over / away).

--circa close   Circa's full game = its CLOSE (circa_close, else circa_t5, else
                BetOnline's close). Future information at long horizons, so an
                upper bound; this is how the 10/11-10/12 numbers were made.
--circa hourly  Circa's full game AS OF the Bookmaker pull: the latest
                'circa_hourly' capture (circa_archive.py, from 10/12) at or
                before the pull and at most --max-age hours older. This is
                how the shift runs live, and the go/no-go test.

Reports MAE before/after, signed error, error along the shift's direction
(>0 = fair ends past the close in the direction it moved = fake edges that
way), how often the shift carries fair >1pp past the close or leaves it >1pp
further away, per sport / period / horizon, plus K fit out of sample (games
split by kickoff, earlier half vs later half; and alternating).

Run from Odds Screen/:  python -I tools/fg_shift_backtest.py --circa hourly
"""
import argparse, math, sqlite3, statistics as st, sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import clv          # noqa: E402  (FB_EMPIRICAL parser, power de-vig, model PMF)
import bet_grader   # noqa: E402  (fuzzy team matching)

CENTER = {   # must match index.html FB_PERIOD_CENTER
    "americanfootball_ncaaf": {"spreads_h1": (0, .6233), "spreads_q1": (0, .2896), "totals_h1": (.895, .5044), "totals_q1": (-2.2, .2599)},
    "americanfootball_nfl":   {"spreads_h1": (0, .575),  "spreads_q1": (0, .2397), "totals_h1": (.102, .5056), "totals_q1": (.559, .1842)},
}
PMKTS = ["spreads_h1", "totals_h1", "h2h_h1", "spreads_q1", "totals_q1", "h2h_q1"]
M = clv._model()


def ts(s):
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def pmf(sk, mkt, c):
    t = M[sk][mkt]; spreads = mkt.startswith("spreads")
    span = len(t["mult"]) - 1; off = span if spreads else 0; n = 2 * span + 1 if spreads else span + 1
    w = []
    for i in range(n):
        d = (i - off - c) / t["scale"]
        w.append((math.exp(-0.5 * d * d) if t["nu"] is None else (1 + d * d / t["nu"]) ** (-(t["nu"] + 1) / 2)) * t["mult"][abs(i - off)])
    z = sum(w)
    return [x / z for x in w], off


def up_raw(dist, mkt, x):
    p, off = dist; spreads = mkt.startswith("spreads"); win = push = 0.0
    for i, w in enumerate(p):
        v = i - off
        if v == x: push += w
        elif (v < x) if spreads else (v > x): win += w
    return None if push >= 1 else win / (1 - push)


_dc = {}
def dist_at(sk, mkt, c):
    k = (sk, mkt, round(c, 3))
    if k not in _dc: _dc[k] = pmf(sk, mkt, c)
    return _dc[k]


_lc = {}
def fifty_line(sk, base, x, fair):
    """The 50/50 full-game line equivalent to a quote (x, fair) — index.html fbFgCenter."""
    k = (sk, base, x, round(fair, 5))
    if k in _lc: return _lc[k]
    spreads = base == "spreads"; lo, hi = x - 40, x + 40
    for _ in range(40):
        m = (lo + hi) / 2; p = up_raw(dist_at(sk, base, m), base, x)
        if p is None or (p > fair) == spreads: lo = m
        else: hi = m
    d = dist_at(sk, base, (lo + hi) / 2); line, prev, L = x, None, x - 15
    while L <= x + 15:
        p = up_raw(d, base, L)
        if p is not None:
            if prev and (prev[1] <= .5) != (p <= .5):
                line = prev[0] + .5 * (prev[1] - .5) / (prev[1] - p); break
            prev = (L, p)
        L += .5
    _lc[k] = line
    return line


def derived(sk, pm, fg_line, x):
    base = "totals" if pm.startswith("totals") else "spreads"
    per = "_h1" if "_h1" in pm else "_q1"
    a, b = CENTER[sk][base + per]
    p = up_raw(dist_at(sk, base + per, a + b * fg_line), base + per, 0 if pm.startswith("h2h") else x)
    return None if p is None or not (.01 <= p <= .99) else p


def pair(rows, mkt, away, home):
    up = [r for r in rows if r[0] in ("Over", away)]; lo = [r for r in rows if r[0] in ("Under", home)]
    if not up or not lo or not up[0][2] or not lo[0][2]: return None
    return (0 if mkt.startswith("h2h") else up[0][1]), clv.power_devig(up[0][2], lo[0][2])


def fg_line(sk, rows_by_mkt, base, away, home):
    p = pair(rows_by_mkt.get(base, []), base, away, home)
    return fifty_line(sk, base, p[0], p[1]) if p else None


def period_at(sk, pm, own_pt, own_fair, x):
    if pm.startswith("h2h") or own_pt == x: return own_fair
    if abs(x - own_pt) > clv.FB_AVG_PTS_PER_PLAY * math.sqrt(.5 if "_h1" in pm else .25): return None
    a = up_raw(dist_at(sk, pm, own_pt), pm, own_pt); b = up_raw(dist_at(sk, pm, own_pt), pm, x)
    return None if a is None or b is None else own_fair + b - a


def load(conn, mode, max_age):
    bk = defaultdict(lambda: defaultdict(list)); meta = {}
    for sk, eid, cap, away, home, ct, mkt, side, pt, price in conn.execute(
            "SELECT sport_key, event_id, captured_at, away_team, home_team, commence_time, market, side, point, price "
            "FROM line_snapshots WHERE source='bookmaker' AND is_alt=0 AND sport_key LIKE 'americanfootball%'"):
        bk[(sk, eid, cap)][mkt].append((side, pt, price)); meta[eid] = (sk, away, home, ct)
    close = defaultdict(lambda: defaultdict(list)); bol_fg = defaultdict(lambda: defaultdict(list))
    for sk, eid, mkt, side, pt, price in conn.execute(
            "SELECT sport_key, event_id, market, side, point, price FROM line_snapshots WHERE source IN "
            "('odds_api_close','odds_api_hist_close') AND book_key='betonlineag' AND sport_key LIKE 'americanfootball%' "
            "AND is_alt=0 ORDER BY captured_at DESC"):
        (bol_fg if mkt in ("spreads", "totals") else close)[(sk, eid)][mkt].append((side, pt, price))
    srcs = ("circa_close", "circa_t5") if mode == "close" else ("circa_hourly",)
    circa = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))   # oio id -> captured_at -> mkt -> rows
    cmeta = {}
    for src, sk, eid, cap, away, home, ct, mkt, side, pt, price in conn.execute(
            "SELECT source, sport_key, event_id, captured_at, away_team, home_team, commence_time, market, side, point, price "
            f"FROM line_snapshots WHERE source IN ({','.join('?' * len(srcs))}) AND sport_key LIKE 'americanfootball%' "
            "AND market IN ('spreads','totals') AND is_alt=0", srcs):
        key = cap if mode == "hourly" else src     # close mode: one bucket per source (circa_close preferred)
        circa[eid][key][mkt].append((side, pt, price)); cmeta[eid] = (sk, away, home, ct)
    return bk, meta, close, bol_fg, circa, cmeta


def match_circa(meta, cmeta):
    """Odds API event id -> odds-api.io (Circa) event id, by teams within 12 h of kickoff."""
    by_sport = defaultdict(list)
    for oid, (sk, away, home, ct) in cmeta.items(): by_sport[sk].append((oid, away, home, ts(ct)))
    out = {}
    for eid, (sk, away, home, ct) in meta.items():
        if eid in cmeta: out[eid] = eid; continue
        k, best = ts(ct), None
        for oid, ca, ch, cct in by_sport[sk]:
            if abs((cct - k).total_seconds()) > 12 * 3600: continue
            s = (bet_grader._team_score(home, {"name": ch, "location": ch, "short_name": ch}),
                 bet_grader._team_score(away, {"name": ca, "location": ca, "short_name": ca}))
            if min(s) >= .6 and (not best or sum(s) > best[0]): best = (sum(s), oid)
        if best: out[eid] = best[1]
    return out


def records(args):
    conn = sqlite3.connect(ROOT / "data" / "odds_history.db")
    bk, meta, close, bol_fg, circa, cmeta = load(conn, args.circa, args.max_age)
    cmap = match_circa(meta, cmeta)
    pulls_by_eid = defaultdict(list)
    for (sk, eid, cap), mk in bk.items(): pulls_by_eid[eid].append((ts(cap), mk))
    recs, skipped = [], defaultdict(int)
    for h in args.horizons:
        for (sk, eid), cmk in close.items():
            if eid not in meta: continue
            _, away, home, ct = meta[eid]; kick = ts(ct)
            cands = [(abs((kick - t).total_seconds() / 3600 - h), t, mk) for t, mk in pulls_by_eid[eid]
                     if (kick - t).total_seconds() > 1800]
            if not cands: continue
            gap, t, mk = min(cands, key=lambda c: c[0])
            if gap > max(1.5, h * .3): continue
            bk_fg = {b: fg_line(sk, mk, b, away, home) for b in ("spreads", "totals")}
            # Circa's full game for this pull
            c_rows, fgsrc = None, None
            oid = cmap.get(eid)
            if args.circa == "hourly":
                if oid:
                    caps = sorted((ts(c), c) for c in circa[oid] if ts(c) <= t and (t - ts(c)).total_seconds() <= args.max_age * 3600)
                    if caps: c_rows, fgsrc = circa[oid][caps[-1][1]], "circa_hourly"
                if c_rows is None: skipped["no circa_hourly at pull"] += 1; continue
            else:
                for src in ("circa_close", "circa_t5"):
                    if oid and src in circa[oid]: c_rows, fgsrc = circa[oid][src], src; break
                if c_rows is None and (sk, eid) in bol_fg: c_rows, fgsrc = bol_fg[(sk, eid)], "betonline_close"
                if c_rows is None: skipped["no full-game close"] += 1; continue
            c_fg = {b: fg_line(sk, c_rows, b, away, home) for b in ("spreads", "totals")}
            for pm in PMKTS:
                base = "totals" if pm.startswith("totals") else "spreads"
                tgt = pair(cmk.get(pm, []), pm, away, home); own = pair(mk.get(pm, []), pm, away, home)
                if not tgt or not own or bk_fg[base] is None or c_fg[base] is None: continue
                b = period_at(sk, pm, own[0], own[1], tgt[0])
                d0, d1 = derived(sk, pm, bk_fg[base], tgt[0]), derived(sk, pm, c_fg[base], tgt[0])
                if b is None or d0 is None or d1 is None: continue
                recs.append(dict(h=h, sk=sk[17:], eid=eid, kick=ct, per="1H" if "_h1" in pm else "Q1", pm=pm,
                                 fgsrc=fgsrc, b=b, delta=d1 - d0, tgt=tgt[1]))
    return recs, skipped


def shifted(r, K, cap):
    s = K * r["delta"]
    return r["b"] + (max(-cap, min(cap, s)) if cap else s)


def stats(rs, K, cap, label):
    if not rs: print(f"  {label:<28} n=   0"); return
    sg = lambda x: (x > 0) - (x < 0)
    eb = [r["b"] - r["tgt"] for r in rs]; ea = [shifted(r, K, cap) - r["tgt"] for r in rs]
    sh = [(r, shifted(r, K, cap) - r["b"]) for r in rs if abs(shifted(r, K, cap) - r["b"]) >= .001]
    ab = [(r["b"] - r["tgt"]) * sg(s) for r, s in sh]; aa = [(shifted(r, K, cap) - r["tgt"]) * sg(s) for r, s in sh]
    past = sum(1 for x, y in zip(ab, aa) if y > .01 and x <= .01)
    worse = sum(1 for x, y in zip(ab, aa) if abs(y) > abs(x) + .01)
    m = lambda v: 100 * st.mean(v) if v else 0.0
    print(f"  {label:<28} n={len(rs):4} [{len({r['eid'] for r in rs}):3} g]  MAE {m(map(abs, eb)):.2f}->{m(map(abs, ea)):.2f}  "
          f"signed {m(eb):+.2f}->{m(ea):+.2f}  | shifted {len(sh):4}: along-shift {m(ab):+.2f}->{m(aa):+.2f}  "
          f">1pp past close {past}  >1pp worse {worse}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--circa", choices=("close", "hourly"), default="hourly")
    ap.add_argument("--horizons", type=float, nargs="+", default=[3, 6, 10, 24, 48])
    ap.add_argument("--max-age", type=float, default=2.0, help="hourly mode: max hours between Circa capture and the BK pull")
    ap.add_argument("--k", type=float, default=0.6)
    ap.add_argument("--cap", type=float, default=0.015, help="0 = no cap")
    args = ap.parse_args()
    recs, skipped = records(args)
    K, cap = args.k, args.cap
    print(f"Circa source: {args.circa}  K={K}  cap=+-{cap * 100:.1f}pp  records={len(recs)}  games={len({r['eid'] for r in recs})}"
          + (f"  kickoffs {min(r['kick'] for r in recs)[:10]}..{max(r['kick'] for r in recs)[:10]}" if recs else ""))
    if skipped: print("  skipped:", dict(skipped))
    if not recs: return
    print("\n== by horizon / sport / period ==")
    for h in args.horizons:
        stats([r for r in recs if r["h"] == h], K, cap, f"{h:g}h all")
        for sk in ("ncaaf", "nfl"):
            for per in ("1H", "Q1"):
                stats([r for r in recs if r["h"] == h and r["sk"] == sk and r["per"] == per], K, cap, f"{h:g}h {sk} {per}")
        stats([r for r in recs if r["h"] == h and abs(K * r["delta"]) >= .01], K, cap, f"{h:g}h raw shift>=1pp")
    mae = lambda rs, k: st.mean(abs(shifted(r, k, cap) - r["tgt"]) for r in rs)
    grid = [i / 20 for i in range(0, 25)]
    for sk in ("ncaaf", "nfl"):
        rs = [r for r in recs if r["sk"] == sk]
        games = sorted({r["eid"] for r in rs}, key=lambda e: min(r["kick"] for r in rs if r["eid"] == e))
        if len(games) < 8:
            print(f"\n== {sk}: {len(games)} games, too few for an out-of-sample K =="); continue
        for name, tr_ids in (("earlier half of games", set(games[:len(games) // 2])), ("alternating games", set(games[0::2]))):
            tr = [r for r in rs if r["eid"] in tr_ids]; te = [r for r in rs if r["eid"] not in tr_ids]
            kstar = min(grid, key=lambda k: mae(tr, k))
            print(f"\n== {sk} out of sample, K fit on {name}: train {len(tr)} rec / {len({r['eid'] for r in tr})} g, "
                  f"test {len(te)} / {len({r['eid'] for r in te})} g, K*={kstar} ==")
            print("   test MAE by K: " + ", ".join(f"{k}:{100 * mae(te, k):.2f}" for k in (0, .3, .5, .6, .75, 1.0)))
            for h in args.horizons:
                stats([r for r in te if r["h"] == h], K, cap, f"test {h:g}h K={K}")


if __name__ == "__main__":
    main()
