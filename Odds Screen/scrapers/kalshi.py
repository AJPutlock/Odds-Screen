"""
Kalshi game markets (public market-data API, no key) for the odds board.

Every price is what a TAKER actually pays: the best ask plus Kalshi's taker
fee, 0.07 x p x (1-p) per contract x the series' fee multiplier (fee schedule
effective 2026-07-07; e.g. 52c -> +1.75c -> 53.75c, -108 shown as -116). The
raw ask is kept as *_prob so the board can show the number to click on Kalshi.

Kalshi lists a LADDER per game — spreads "CLE Browns wins by over 9.5" (YES =
CLE -9.5, NO = the other team +9.5) and totals "over 44.5" (YES = Over, NO =
Under) at 10-30 strikes each — so every strike becomes a quote (alt_lines)
and the most balanced one is shown as the main line.

A price with less than MIN_DEPTH_USD ($10) resting at the ask is dropped, so
the board shows the next best price instead — many college markets carry
$3-$10, and an edge you can put $5 on isn't an edge.

Output: game dicts shaped like the bet365/Bookmaker scrapers' ({away_team,
home_team, markets, alt_lines}) plus "_date" (ET game date from the ticker).
"""
import logging
import re
import time
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

API = "https://api.elections.kalshi.com/trade-api/v2"
TAKER_COEF = 0.07
MIN_DEPTH_USD = 10.0      # user rule 2026-09-28: under $10 at the price, don't show it
CACHE_SECONDS = 60

# Odds API market key -> Kalshi series. Winner-only period markets (1H/1Q
# "winner") are left out: a tied half settles differently than a 2-way line.
SERIES = {
    "americanfootball_nfl": {
        "h2h": "KXNFLGAME", "spreads": "KXNFLSPREAD", "totals": "KXNFLTOTAL",
        "spreads_h1": "KXNFL1HSPREAD", "totals_h1": "KXNFL1HTOTAL",
        "spreads_q1": "KXNFL1QSPREAD", "totals_q1": "KXNFL1QTOTAL",
    },
    "americanfootball_ncaaf": {
        "h2h": "KXNCAAFGAME", "spreads": "KXNCAAFSPREAD", "totals": "KXNCAAFTOTAL",
        "spreads_h1": "KXNCAAF1HSPREAD", "totals_h1": "KXNCAAF1HTOTAL",
        "spreads_q1": "KXNCAAF1QSPREAD", "totals_q1": "KXNCAAF1QTOTAL",
    },
}
_MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}

# Kalshi shortens same-city NFL teams to a letter; spell them out so the
# board's team matcher can't pair the Giants with the Jets.
# College names that differ from the Odds API's go here too.
_NAME_FIX = {"New York G": "New York Giants", "New York J": "New York Jets",
             "Los Angeles R": "Los Angeles Rams", "Los Angeles C": "Los Angeles Chargers",
             "Louisiana-Monroe": "UL Monroe", "Houston Christian": "Houston Baptist",
             "Southern Miss": "Southern Mississippi"}

_cache: dict = {}        # sport -> (epoch, games)
_fee_mult: dict = {}     # series -> taker multiplier (None = unsupported fee type)


def _get(path: str, **params) -> dict:
    r = requests.get(f"{API}{path}", params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def _all(path: str, key: str, **params) -> list:
    out, cursor = [], None
    for _ in range(20):
        d = _get(path, **params, **({"cursor": cursor} if cursor else {}))
        out += d.get(key) or []
        cursor = d.get("cursor")
        if not cursor:
            break
    return out


def _taker_mult(series: str):
    if series not in _fee_mult:
        try:
            s = _get(f"/series/{series}").get("series") or {}
            ok = str(s.get("fee_type", "")).startswith("quadratic")
            _fee_mult[series] = float(s.get("fee_multiplier") or 1) if ok else None
        except Exception as e:
            logger.warning(f"kalshi: fee lookup failed for {series}: {e}")
            return None
    return _fee_mult[series]


def _american(decimal: float) -> str:
    return f"+{round((decimal - 1) * 100)}" if decimal >= 2 else str(round(-100 / (decimal - 1)))


def _quote(ask, size, mult):
    """(net American odds, raw ask, $ resting at the ask) for buying at `ask`,
    or (None, None, None) if the ask is missing/extreme or too thin to bet."""
    try:
        p, n = float(ask), float(size or 0)
    except (TypeError, ValueError):
        return None, None, None
    if not (0.02 <= p <= 0.98) or n * p < MIN_DEPTH_USD:
        return None, None, None
    cost = p + TAKER_COEF * mult * p * (1 - p)
    return _american(1 / cost), p, round(n * p, 2)


def _event_date(suffix: str):
    m = re.match(r"(\d\d)([A-Z]{3})(\d\d)", suffix)
    if not m or m.group(2) not in _MONTHS:
        return None
    return datetime(2000 + int(m.group(1)), _MONTHS[m.group(2)], int(m.group(3))).date()


def fetch_kalshi(sport_key: str) -> list:
    """Kalshi games for one sport (cached CACHE_SECONDS). [] on failure."""
    hit = _cache.get(sport_key)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]
    series = SERIES.get(sport_key)
    if not series:
        return []
    try:
        games = _build(series)
    except Exception as e:
        logger.warning(f"kalshi {sport_key}: fetch failed: {type(e).__name__}: {e}")
        return hit[1] if hit else []
    _cache[sport_key] = (time.time(), games)
    logger.info(f"kalshi {sport_key}: {len(games)} games")
    return games


def _build(series: dict) -> list:
    # Game identity from the winner series' events: "Howard vs Rutgers" /
    # "HOW vs RUTG (Sep 25)" -> names (away vs home) and ticker codes.
    games: dict = {}
    for ev in _all("/events", "events", series_ticker=series["h2h"], status="open", limit=200):
        suffix = ev["event_ticker"].split("-", 1)[1]
        names = [_NAME_FIX.get(s.strip(), s.strip()) for s in (ev.get("title") or "").split(" vs ")]
        codes = re.findall(r"[A-Z0-9]+", (ev.get("sub_title") or "").split("(")[0].replace(" vs ", " "))
        if len(names) != 2 or len(codes) != 2:
            continue
        games[suffix] = {"away_team": names[0], "home_team": names[1], "_codes": codes,
                         "_date": _event_date(suffix), "markets": {}, "alt_lines": {}}

    for mkt, ser in series.items():
        mult = _taker_mult(ser)
        if mult is None:
            continue
        rungs: dict = {}   # suffix -> [entry]
        for m in _all("/markets", "markets", series_ticker=ser, status="open", limit=1000):
            suffix = m["event_ticker"].split("-", 1)[1]
            g = games.get(suffix)
            if not g:
                continue
            away_c, home_c = g["_codes"]
            yes, yes_p, yes_usd = _quote(m.get("yes_ask_dollars"), m.get("yes_ask_size_fp"), mult)
            no, no_p, no_usd = _quote(m.get("no_ask_dollars"), _no_size(m), mult)
            team = m["ticker"].rsplit("-", 1)[1]
            if mkt == "h2h":
                side = "away" if team == away_c else "home" if team == home_c else None
                if side and yes:
                    e = g["markets"].setdefault("h2h", {"away_point": None, "home_point": None})
                    e[f"{side}_odds"], e[f"{side}_prob"], e[f"{side}_usd"] = yes, yes_p, yes_usd
                continue
            x = m.get("floor_strike")
            if x is None or not (yes or no):
                continue
            x = float(x)
            if mkt.startswith("totals"):
                e = {"home_point": x, "away_point": x, "home_odds": yes, "home_prob": yes_p, "home_usd": yes_usd,
                     "away_odds": no, "away_prob": no_p, "away_usd": no_usd}  # home = Over
            else:
                team_code = re.sub(r"\d+$", "", team)
                if team_code == away_c:      # away -x (YES) / home +x (NO)
                    e = {"away_point": -x, "home_point": x, "away_odds": yes, "away_prob": yes_p, "away_usd": yes_usd,
                         "home_odds": no, "home_prob": no_p, "home_usd": no_usd}
                elif team_code == home_c:    # home -x (YES) / away +x (NO)
                    e = {"home_point": -x, "away_point": x, "home_odds": yes, "home_prob": yes_p, "home_usd": yes_usd,
                         "away_odds": no, "away_prob": no_p, "away_usd": no_usd}
                else:
                    continue
            rungs.setdefault(suffix, []).append(e)
        for suffix, lst in rungs.items():
            g = games[suffix]
            g["alt_lines"][mkt] = lst
            two_sided = [e for e in lst if e.get("away_odds") and e.get("home_odds")]
            if two_sided:   # main line: the most balanced two-sided strike
                g["markets"][mkt] = min(two_sided, key=lambda e: abs(e["away_prob"] - e["home_prob"]))
    return [g for g in games.values() if g["markets"] or g["alt_lines"]]


def _no_size(m: dict):
    """Contracts at the best NO ask = contracts at the best YES bid."""
    return m.get("no_ask_size_fp") or m.get("yes_bid_size_fp")
