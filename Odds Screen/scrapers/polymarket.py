"""
Polymarket US game markets (public gateway API, no key) for the odds board.

Every price is what a TAKER actually pays: the ask plus Polymarket US's taker
fee, feeCoefficient x p x (1-p) per share, read from each market (0.0695 on
football as of 2026-09-25; e.g. 52c -> +1.73c -> 53.7c, -108 shown as -116).
The raw ask is kept as *_prob so the board can show the number to click.

Each market is one question ("Will the Carolina Panthers cover -2.5 vs the
Cleveland Browns"): the long side is bought at the best ask, the other side
at 1 - best bid. Ladders run 10-40 strikes per game, so every strike becomes a
quote (alt_lines) and the most balanced one is shown as the main line.

The listing carries no size at the best price, so the bid-ask spread stands
in for liquidity: a market whose spread is wider than MAX_SPREAD is skipped
(an ask with nothing behind it usually sits far from the bid).

Output: game dicts shaped like the other scrapers' ({away_team, home_team,
markets, alt_lines}) plus "_date" (ET game date).
"""
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)

API = "https://gateway.polymarket.us"
DEFAULT_COEF = 0.0695
MAX_SPREAD = 0.05
CACHE_SECONDS = 60
_ET = ZoneInfo("America/New_York")

LEAGUES = {"americanfootball_nfl": "nfl", "americanfootball_ncaaf": "cfb"}
# Polymarket sportsMarketType -> Odds API market key. Team-points totals
# ("football_team_points_full_game_total", "football_team_first_half_total")
# are left out; "football_team_full_game_total" is the GAME total despite its name.
TYPES = {
    "football_team_full_game_winner":     "h2h",
    "football_team_full_game_spread":     "spreads",
    "football_team_full_game_total":      "totals",
    "football_team_first_half_spread":    "spreads_h1",
    "football_game_first_half_total":     "totals_h1",
    "football_team_first_quarter_spread": "spreads_q1",
    "football_game_first_quarter_total":  "totals_q1",
}

# Names that differ from the Odds API's (so the team matcher can pair them).
_NAME_FIX = {"Louisiana-Monroe": "UL Monroe", "Houston Christian": "Houston Baptist",
             "Southern Miss": "Southern Mississippi", "NM State": "New Mexico State"}

_cache: dict = {}   # sport -> (epoch, games)


def _american(decimal: float) -> str:
    return f"+{round((decimal - 1) * 100)}" if decimal >= 2 else str(round(-100 / (decimal - 1)))


def _buy(p: float, coef: float):
    """(net American odds, raw price) for buying at p, or (None, None)."""
    if not (0.02 <= p <= 0.98):
        return None, None
    return _american(1 / (p + coef * p * (1 - p))), round(p, 4)


def fetch_polymarket(sport_key: str) -> list:
    """Polymarket US games for one sport (cached CACHE_SECONDS). [] on failure."""
    hit = _cache.get(sport_key)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]
    league = LEAGUES.get(sport_key)
    if not league:
        return []
    try:
        events, offset = [], 0
        for _ in range(10):
            r = requests.get(f"{API}/v2/leagues/{league}/events",
                             params={"limit": 100, "offset": offset}, timeout=30)
            r.raise_for_status()
            page = r.json().get("events") or []
            events += page
            if len(page) < 100:
                break
            offset += 100
        games = [g for g in (_game(e) for e in events) if g]
    except Exception as e:
        logger.warning(f"polymarket {sport_key}: fetch failed: {type(e).__name__}: {e}")
        return hit[1] if hit else []
    _cache[sport_key] = (time.time(), games)
    logger.info(f"polymarket {sport_key}: {len(games)} games")
    return games


def _game(ev: dict):
    if ev.get("closed") or ev.get("ended") or ev.get("live") or not ev.get("active", True):
        return None
    raw_names = [t.get("name") for t in (ev.get("teams") or [])]
    if len(raw_names) != 2 or not all(raw_names):
        return None
    teams = [_NAME_FIX.get(n, n) for n in raw_names]
    try:
        date = datetime.fromisoformat(ev["startTime"].replace("Z", "+00:00")).astimezone(_ET).date()
    except Exception:
        date = None
    g = {"away_team": teams[0], "home_team": teams[1], "_date": date, "markets": {}, "alt_lines": {}}
    rungs: dict = {}
    for m in ev.get("markets") or []:
        mkt = TYPES.get(m.get("sportsMarketType"))
        if not mkt or m.get("closed") or m.get("hidden") or m.get("active") is False:
            continue
        try:
            bid = float((m.get("bestBidQuote") or {}).get("value"))
            ask = float((m.get("bestAskQuote") or {}).get("value"))
        except (TypeError, ValueError):
            continue
        if ask - bid > MAX_SPREAD:
            continue
        coef = float(m.get("feeCoefficient") or DEFAULT_COEF)
        sides = m.get("marketSides") or []
        long_s = next((s for s in sides if s.get("long")), None)
        short_s = next((s for s in sides if not s.get("long")), None)
        if not long_s or not short_s:
            continue
        long_q, long_p = _buy(ask, coef)
        short_q, short_p = _buy(1 - bid, coef)
        if mkt.startswith("totals"):
            x = float(m["line"])
            e = {"home_point": x, "away_point": x, "home_odds": long_q, "home_prob": long_p,
                 "away_odds": short_q, "away_prob": short_p}                     # home = Over
        else:
            quotes = {}
            for s, q, p in ((long_s, long_q, long_p), (short_s, short_q, short_p)):
                name = (s.get("team") or {}).get("name")
                side = "away" if name == raw_names[0] else "home" if name == raw_names[1] else None
                if not side:
                    break
                pt = None if mkt == "h2h" else float(s.get("description"))
                quotes[side] = (pt, q, p)
            if len(quotes) != 2:
                continue
            e = {f"{sd}_{k}": v for sd, (pt, q, p) in quotes.items()
                 for k, v in (("point", pt), ("odds", q), ("prob", p))}
        if mkt == "h2h":
            g["markets"]["h2h"] = e
            continue
        rungs.setdefault(mkt, []).append(e)
    for mkt, lst in rungs.items():
        g["alt_lines"][mkt] = lst
        two_sided = [e for e in lst if e.get("away_odds") and e.get("home_odds")]
        if two_sided:
            g["markets"][mkt] = min(two_sided, key=lambda e: abs(e["away_prob"] - e["home_prob"]))
    return g if g["markets"] else None
