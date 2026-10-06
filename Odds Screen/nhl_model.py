"""
NHL goal model (totals + puck lines) — the same math as the screen's
nhlGamePmf / nhlFit / nhlModelShift in static/index.html (change both
together). Each team's regulation goals are Poisson; a game tied after
regulation gets one more goal (OT or the shootout goal books count); a
one-goal regulation lead becomes two (one more total goal) with probability
`en`, the empty-net goal. Per game the total rate is fitted to the total's
no-push price, the home share to the moneyline, and `en` to the puck line
(else EN_DEFAULT). Used by clv.py to carry a close to a bet's own number.

Meant to be replaced by a lookup table of final totals/margins by closing line
once the NHL close capture (line_snapshots, source odds_api_close) has a season.
"""
import math

MAX_GAP = 0.5          # totals
SPREAD_MAX_GAP = 3.0   # puck lines: -1.5 <-> +1.5 / -2.5
EN_DEFAULT = 0.15   # median of the per-game fits on 10/6 (0.12-0.18)


def _poisson(lam):
    p = [math.exp(-lam)]
    for k in range(1, 21):
        p.append(p[-1] * lam / k)
    return p


def game_pmf(lam_total, home_share, en):
    """(tot, mar, home_win): tot[k] = P(total k); mar[m + 25] = P(home margin m)."""
    ph, pa = _poisson(lam_total * home_share), _poisson(lam_total * (1 - home_share))
    tot, mar = [0.0] * 45, [0.0] * 51
    home_win = 0.0
    for h in range(21):
        for a in range(21):
            p = ph[h] * pa[a]
            if h == a:
                tot[h + a + 1] += p
                mar[26] += p * home_share
                mar[24] += p * (1 - home_share)
                home_win += p * home_share
            elif abs(h - a) == 1:
                sg = 1 if h > a else -1
                tot[h + a] += p * (1 - en)
                mar[25 + sg] += p * (1 - en)
                tot[h + a + 1] += p * en
                mar[25 + 2 * sg] += p * en
                if h > a:
                    home_win += p
            else:
                tot[h + a] += p
                mar[25 + h - a] += p
                if h > a:
                    home_win += p
    return tot, mar, home_win


def over_prob(tot, x):
    win = push = 0.0
    for v, p in enumerate(tot):
        if v == x:
            push += p
        elif v > x:
            win += p
    return None if push >= 1 else win / (1 - push)


def away_cover(mar, x):
    """No-push chance the away side covers at away point x (home margin < x)."""
    win = push = 0.0
    for i, p in enumerate(mar):
        m = i - 25
        if m == x:
            push += p
        elif m < x:
            win += p
    return None if push >= 1 else win / (1 - push)


def _fit_inner(pt, fair, p_home, en):
    share, lam = 0.5, 6.0
    for _ in range(4):
        lo, hi = 1.5, 14.0
        for _ in range(36):
            m = (lo + hi) / 2
            if over_prob(game_pmf(m, share, en)[0], pt) < fair:
                lo = m
            else:
                hi = m
        lam = (lo + hi) / 2
        if p_home is None:
            break
        lo, hi = 0.15, 0.85
        for _ in range(36):
            m = (lo + hi) / 2
            if game_pmf(lam, m, en)[2] < p_home:
                lo = m
            else:
                hi = m
        share = (lo + hi) / 2
    return lam, share


def fit(pt, fair, p_home=None, pl=None):
    """pl = (away_pt, fair_away) from the puck line, or None.
    Returns dict lam, share, en, tot, mar."""
    fair = min(0.97, max(0.03, fair))
    en = EN_DEFAULT
    if pl and p_home is not None:
        def gap(e):
            lam, share = _fit_inner(pt, fair, p_home, e)
            return away_cover(game_pmf(lam, share, e)[1], pl[0]) - pl[1]
        lo, hi = 0.0, 0.6
        glo, ghi = gap(lo), gap(hi)
        if glo * ghi > 0:
            en = lo if abs(glo) < abs(ghi) else hi
        else:
            for _ in range(22):
                m = (lo + hi) / 2
                gm = gap(m)
                if gm * glo > 0:
                    lo, glo = m, gm
                else:
                    hi = m
            en = (lo + hi) / 2
    lam, share = _fit_inner(pt, fair, p_home, en)
    tot, mar, _ = game_pmf(lam, share, en)
    return {"lam": lam, "share": share, "en": en, "tot": tot, "mar": mar}


def over_at(pt_known, fair_over_known, pt_target, p_home=None, pl=None):
    """Fair no-push Over at pt_target given the fair Over at pt_known (None past MAX_GAP)."""
    if abs(pt_target - pt_known) > MAX_GAP + 1e-9:
        return None
    return over_prob(fit(pt_known, fair_over_known, p_home, pl)["tot"], pt_target)


def away_cover_at(total_pt, total_fair_over, p_home, away_pt_known, fair_away_known, away_pt_target):
    """Fair no-push away cover at away_pt_target given the known puck line (None past SPREAD_MAX_GAP)."""
    if abs(away_pt_target - away_pt_known) > SPREAD_MAX_GAP + 1e-9:
        return None
    f = fit(total_pt, total_fair_over, p_home, (away_pt_known, fair_away_known))
    return away_cover(f["mar"], away_pt_target)
