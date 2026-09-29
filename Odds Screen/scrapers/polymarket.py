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

The listing carries no size at the best price, so a market whose bid-ask
spread is wider than MAX_SPREAD is skipped outright, and every quote carries a
ref ("slug|long" / "slug|short") the board uses to check the order book of
the prices it actually shows (depth()): one with less than MIN_DEPTH_USD
behind it is replaced by the price a $10 bet really fills at. Checking every
market instead isn't possible: ~4,000 football markets, and in practice the
public endpoints allow a burst of 5 requests and then answer 429 for ~11 s
(measured 2026-09-28, despite the documented 20/s) — about 18 books a minute.

Output: game dicts shaped like the other scrapers' ({away_team, home_team,
markets, alt_lines}) plus "_date" (ET game date).
"""
import logging
import threading
import time
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from scrapers import polymarket_ws

logger = logging.getLogger(__name__)

API = "https://gateway.polymarket.us"
DEFAULT_COEF = 0.0695
MAX_SPREAD = 0.05
CACHE_SECONDS = 60
MIN_DEPTH_USD = 10.0
RATE_BURST, RATE_WINDOW = 5, 16.0   # measured: 5 requests, then ~11 s of 429s
BOOKS_PER_CALL = 5                  # new books per depth_many() call (one burst)
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
_books: dict = {}   # slug -> (epoch, marketData)
_books_lock = threading.Lock()
_req_times: deque = deque()
_rate_lock = threading.Lock()


def _throttle():
    """Block until another request fits in RATE_BURST per RATE_WINDOW."""
    with _rate_lock:
        while True:
            now = time.time()
            while _req_times and now - _req_times[0] > RATE_WINDOW:
                _req_times.popleft()
            if len(_req_times) < RATE_BURST:
                _req_times.append(now)
                return
            time.sleep(RATE_WINDOW - (now - _req_times[0]) + 0.1)


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
        # With API credentials, stream every quoted market's book (polymarket_ws).
        polymarket_ws.watch({e[k].rsplit("|", 1)[0] for g in games
                             for lst in [list(g["markets"].values())] + list(g["alt_lines"].values())
                             for e in lst for k in ("away_ref", "home_ref") if e.get(k)})
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
        slug = m.get("slug")
        if mkt.startswith("totals"):
            x = float(m["line"])
            e = {"home_point": x, "away_point": x, "home_odds": long_q, "home_prob": long_p,
                 "away_odds": short_q, "away_prob": short_p,                    # home = Over
                 "home_ref": f"{slug}|long", "away_ref": f"{slug}|short"}
        else:
            quotes = {}
            for s, q, p, leg in ((long_s, long_q, long_p, "long"), (short_s, short_q, short_p, "short")):
                name = (s.get("team") or {}).get("name")
                side = "away" if name == raw_names[0] else "home" if name == raw_names[1] else None
                if not side:
                    break
                pt = None if mkt == "h2h" else float(s.get("description"))
                quotes[side] = (pt, q, p, f"{slug}|{leg}")
            if len(quotes) != 2:
                continue
            e = {f"{sd}_{k}": v for sd, (pt, q, p, ref) in quotes.items()
                 for k, v in (("point", pt), ("odds", q), ("prob", p), ("ref", ref))}
        e["_coef"] = coef
        _apply_streamed_depth(e, coef)
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


def _apply_streamed_depth(e: dict, coef: float) -> None:
    """
    The $10 rule, server-side, for sides whose book is streaming: a thin best
    price becomes the price a $10 bet fills at (or the side is dropped), and
    $ available is recorded. The ref is then cleared, so the page knows this
    side is already verified and doesn't queue a slow REST check for it.
    """
    for sd in ("away", "home"):
        ref = e.get(f"{sd}_ref")
        if not ref or not e.get(f"{sd}_odds"):
            continue
        slug, leg = ref.rsplit("|", 1)
        md = polymarket_ws.book(slug)
        if md is None:
            continue
        d = _depth_from_book(md, leg)
        if d["usd_best"] >= MIN_DEPTH_USD:
            e[f"{sd}_usd"] = d["usd_best"]
        elif d["px10"] is not None:
            e[f"{sd}_odds"], e[f"{sd}_prob"] = _buy(d["px10"], coef)
            e[f"{sd}_usd"] = d["usd_px10"]
        else:
            e[f"{sd}_odds"] = e[f"{sd}_prob"] = None
        e[f"{sd}_ref"] = None


def _book(slug: str):
    """A market's order book: streamed if available, else REST (cached
    CACHE_SECONDS, backing off on 429)."""
    md = polymarket_ws.book(slug)
    if md is not None:
        return md
    with _books_lock:
        hit = _books.get(slug)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]
    for attempt in range(2):
        _throttle()
        try:
            r = requests.get(f"{API}/v1/markets/{slug}/book", timeout=15)
        except requests.RequestException:
            return None
        if r.status_code == 429:
            time.sleep(12)
            continue
        if not r.ok:
            return None
        md = r.json().get("marketData") or {}
        with _books_lock:
            _books[slug] = (time.time(), md)
        return md
    return None


def depth(ref: str):
    """
    What buying one side of a market really gets: {usd_best: $ resting at the
    best price, px10: price (raw, of that side) a MIN_DEPTH_USD bet fills at —
    the best price itself when it's deep enough, None when the book can't fill
    it — usd_px10: $ available up to px10}. None if the book can't be read.
    Buying long lifts the offers; buying short hits the bids at 1 - bid.
    """
    try:
        slug, leg = ref.rsplit("|", 1)
    except ValueError:
        return None
    md = _book(slug)
    if md is None:
        return None
    return _depth_from_book(md, leg)


def _depth_from_book(md: dict, leg: str) -> dict:
    raw = md.get("offers") if leg == "long" else md.get("bids")
    levels = []
    for lv in raw or []:
        try:
            px, qty = float(lv["px"]["value"]), float(lv["qty"])
        except (KeyError, TypeError, ValueError):
            continue
        paid = px if leg == "long" else 1 - px
        if 0.02 <= paid <= 0.98 and qty > 0:
            levels.append((paid, qty))
    levels.sort()
    if not levels:
        return {"usd_best": 0.0, "px10": None, "usd_px10": 0.0}
    usd_best = levels[0][0] * levels[0][1]
    cum, px10 = 0.0, None
    for paid, qty in levels:
        cum += paid * qty
        if cum >= MIN_DEPTH_USD:
            px10 = paid
            break
    return {"usd_best": round(usd_best, 2), "px10": px10, "usd_px10": round(cum, 2)}


def depth_many(refs: list) -> dict:
    """
    depth() for refs in priority order, fetching at most BOOKS_PER_CALL new
    books (one rate-limit burst) per call — refs whose book is already cached
    are always answered. Refs left out are for the caller to ask again.
    """
    out, new = {}, set()
    for ref in dict.fromkeys(refs):
        slug = ref.rsplit("|", 1)[0]
        with _books_lock:
            hit = _books.get(slug)
        cached = hit and time.time() - hit[0] < CACHE_SECONDS
        if not cached:
            if slug not in new and len(new) >= BOOKS_PER_CALL:
                continue
            new.add(slug)
        out[ref] = depth(ref)
    return out
