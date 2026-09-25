"""
Odds conversions and the count distributions used to turn a prop line + price
into an implied mean (and back again).

Pure Python (math module only) so there's no numpy/scipy dependency.
"""

import math


# ── Price conversions ─────────────────────────────────────────────────────────

def american_to_decimal(american) -> float:
    """-110 -> 1.909, +120 -> 2.20. Accepts int, float or strings like "+120"."""
    a = float(str(american).strip().replace("+", ""))
    if a == 0:
        raise ValueError("American odds cannot be 0")
    return 1 + a / 100 if a > 0 else 1 + 100 / -a


def decimal_to_american(decimal_odds: float) -> str:
    d = float(decimal_odds)
    if d <= 1.0:
        raise ValueError(f"Decimal odds must be > 1.0, got {d}")
    if d >= 2.0:
        return f"+{round((d - 1) * 100)}"
    return str(round(-100 / (d - 1)))


def no_vig_two_way(over_decimal: float, under_decimal: float) -> tuple[float, float]:
    """Strip the vig from a two-way market (multiplicative method)."""
    p_over = 1 / over_decimal
    p_under = 1 / under_decimal
    total = p_over + p_under
    return p_over / total, p_under / total


# ── Count distributions ───────────────────────────────────────────────────────

def count_pmf(k: int, mean: float, dispersion: float = 1.0) -> float:
    """
    P(X = k) for a count with the given mean.

    dispersion = variance / mean.  1.0 -> Poisson; > 1.0 -> negative binomial
    (fatter tails, which fits shots and saves better than a pure Poisson).
    """
    if k < 0 or mean <= 0:
        return 1.0 if (k == 0 and mean <= 0) else 0.0
    if dispersion <= 1.0 + 1e-9:
        return math.exp(k * math.log(mean) - mean - math.lgamma(k + 1))
    # Negative binomial parameterised by mean and variance.
    r = mean / (dispersion - 1)
    p = r / (r + mean)
    log_pmf = (math.lgamma(k + r) - math.lgamma(r) - math.lgamma(k + 1)
               + r * math.log(p) + k * math.log(1 - p))
    return math.exp(log_pmf)


def prob_over_under(line: float, mean: float, dispersion: float = 1.0) -> tuple[float, float, float]:
    """
    (P(over), P(under), P(push)) for a prop line.

    Half-point lines never push; whole-number lines push when X == line.
    """
    floor_line = math.floor(line)
    cdf_below = sum(count_pmf(k, mean, dispersion) for k in range(floor_line + 1))
    if line == floor_line:                     # whole number: X == line is a push
        p_push = count_pmf(floor_line, mean, dispersion)
        p_under = cdf_below - p_push
    else:
        p_push = 0.0
        p_under = cdf_below
    p_over = max(0.0, 1.0 - cdf_below)
    return p_over, p_under, p_push


def prob_over_no_push(line: float, mean: float, dispersion: float = 1.0) -> float:
    """P(over | the bet doesn't push) — what a de-vigged two-way price represents."""
    p_over, p_under, _ = prob_over_under(line, mean, dispersion)
    decided = p_over + p_under
    return p_over / decided if decided > 0 else 0.5


def implied_mean(line: float, fair_p_over: float, dispersion: float = 1.0,
                 lo: float = 0.01, hi: float = 150.0) -> float:
    """
    Find the mean that makes P(over line) equal the market's fair over probability.

    Example: goals line 2.5 with a fair over price of 55% -> Poisson mean ~2.9.
    P(over) rises steadily as the mean rises, so a simple bisection works.
    """
    fair_p_over = min(max(fair_p_over, 1e-4), 1 - 1e-4)
    for _ in range(100):
        mid = (lo + hi) / 2
        if prob_over_no_push(line, mid, dispersion) < fair_p_over:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-6:
            break
    return (lo + hi) / 2


def expected_value(p_win: float, p_lose: float, decimal_odds: float) -> float:
    """EV per 1 unit staked (pushes return the stake, so they add 0)."""
    return p_win * (decimal_odds - 1) - p_lose
