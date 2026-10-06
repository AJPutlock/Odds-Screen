"""
NHL totals goal model — the same math as the screen's nhlFit / nhlModelShift
in static/index.html (change both together). Each team's regulation goals are
Poisson; a game tied after regulation gets one more goal (OT or the shootout
goal books count). The total rate is fitted so the model's no-push Over at a
known line equals the known fair there; the home share so home wins (OT/SO
split by scoring rate) match the moneyline. Used by clv.py to carry a closing
total to a bet's own number (half a goal at most, like the screen).

Meant to be replaced by a lookup table of final totals by closing line once
the NHL close capture (line_snapshots, source odds_api_close) has a season.
"""
import math

MAX_GAP = 0.5


def _poisson(lam):
    p = [math.exp(-lam)]
    for k in range(1, 21):
        p.append(p[-1] * lam / k)
    return p


def total_pmf(lam_total, home_share):
    ph, pa = _poisson(lam_total * home_share), _poisson(lam_total * (1 - home_share))
    tot = [0.0] * 43
    home_win = 0.0
    for h in range(21):
        for a in range(21):
            p = ph[h] * pa[a]
            if h == a:
                tot[h + a + 1] += p
                home_win += p * home_share
            else:
                tot[h + a] += p
                if h > a:
                    home_win += p
    return tot, home_win


def over_prob(tot, x):
    win = push = 0.0
    for v, p in enumerate(tot):
        if v == x:
            push += p
        elif v > x:
            win += p
    return None if push >= 1 else win / (1 - push)


def fit(pt, fair, p_home=None):
    """(lam_total, home_share, pmf) with no-push Over(pt) == fair."""
    fair = min(0.97, max(0.03, fair))
    share, lam = 0.5, 6.0
    for _ in range(4):
        lo, hi = 1.5, 14.0
        for _ in range(40):
            m = (lo + hi) / 2
            if over_prob(total_pmf(m, share)[0], pt) < fair:
                lo = m
            else:
                hi = m
        lam = (lo + hi) / 2
        if p_home is None:
            break
        lo, hi = 0.15, 0.85
        for _ in range(40):
            m = (lo + hi) / 2
            if total_pmf(lam, m)[1] < p_home:
                lo = m
            else:
                hi = m
        share = (lo + hi) / 2
    return lam, share, total_pmf(lam, share)[0]


def over_at(pt_known, fair_over_known, pt_target, p_home=None):
    """Fair no-push Over at pt_target given the fair Over at pt_known, or None
    past MAX_GAP."""
    if abs(pt_target - pt_known) > MAX_GAP + 1e-9:
        return None
    _, _, tot = fit(pt_known, fair_over_known, p_home)
    return over_prob(tot, pt_target)
