"""
NoVig price sanity checks, shared by app.py (board + alt lines) and
history_tracker.py (Telegram alerts).

NoVig is an exchange: every line has its own order book, and the Odds API
reports whatever is resting there. An empty book comes through as a
placeholder (-100000, -49900) and a thin one as two expensive sides that
can't both be right — Ohio -33.5 -147 / Stonehill +33.5 -174 (23% overround)
while -32.5 right next to it was -115/+100. Its alt ladders also carry stale
rungs out of order with their neighbors (FSU O53.5 -100000 between O50.5 -506
and O57.5 -245). None of those are prices you can get. Real NoVig markets run
1.4-2.6% overround (median, 2026-09-20..24); the rec books ~4.7%. Bad quotes
cluster far from kickoff: 5.3% of quotes 3+ days out, 0 of 60 inside 24h.
"""

NOVIG_BOOK = "novig"
MIN_DECIMAL = 1.01       # -10000 or worse: an empty order book
MAX_OVERROUND = 0.08     # both sides together; real NoVig markets sit near 2%
LADDER_TOL = 0.01        # probability slack before a rung counts as out of order


def pair_ok(price_a, price_b) -> bool:
    """Both sides of one NoVig line (decimal prices) look like a real market."""
    try:
        a, b = float(price_a), float(price_b)
    except (TypeError, ValueError):
        return False
    if a < MIN_DECIMAL or b < MIN_DECIMAL:
        return False
    return 1 / a + 1 / b - 1 <= MAX_OVERROUND


def monotone_ladder(rungs: list) -> list:
    """
    rungs: [(x, pa, pb, key)] — implied probabilities along one line axis, where
    pa must rise with x and pb must fall (totals: Under / Over; spreads keyed by
    the away point: away / home). Repeatedly drops the rung that breaks that
    order against the most others (ties: the wider market) until the rest are
    in order. Returns the keys that survive.
    """
    keep = sorted(rungs, key=lambda r: r[0])
    while keep:
        bad = [0] * len(keep)
        for i in range(len(keep)):
            for j in range(i + 1, len(keep)):
                if keep[i][1] > keep[j][1] + LADDER_TOL or keep[i][2] < keep[j][2] - LADDER_TOL:
                    bad[i] += 1
                    bad[j] += 1
        worst = max(range(len(keep)), key=lambda i: (bad[i], keep[i][1] + keep[i][2]))
        if bad[worst] == 0:
            break
        keep.pop(worst)
    return [r[3] for r in keep]
