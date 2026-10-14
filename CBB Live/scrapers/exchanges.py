"""
Exchange feeds for the CBB Live hub (forked from CFB Live): Polymarket US, Kalshi, and the NoVig
live-fee adjustment.

WHY NOT THE ODDS API
--------------------
The Odds API does carry kalshi, polymarket and novig for NCAAF, but it quotes
their RAW prices with no trading fee applied. On an exchange the taker fee is
part of what you actually pay, so a raw 52c ask is not -108, it is -116. A
board that shows the raw number invents edge that does not exist. Same reason
Odds Screen pulls these from their native feeds instead.

FEES (taker, i.e. what you pay)
-------------------------------
  Kalshi      0.07 * p * (1-p) * series fee_multiplier
  Polymarket  feeCoefficient * p * (1-p), read per market (0.0695 on football)
  NoVig       0.03 * p * (1-p) -- LIVE GAMES ONLY; pregame is fee-free

NoVig is the one that matters specially here: because its fee applies only
when a game is live, the Odds API's NoVig prices are already net for a pregame
board but are NOT net for this one. Everything in this app is in-play, so the
fee always has to come off.

BOTH UPSTREAM SCHEMAS CHANGED since Odds Screen's scrapers were written
(2026-09-28). Verified live 2026-10-10:

  Polymarket  `bestAsk`/`bestBid` are gone; prices are now in
              `bestAskQuote.value` / `bestBidQuote.value`, with a `marketSides`
              array carrying `line`, `long` and per-side price.
  Kalshi      `/markets` returns null for EVERY price field (yes_bid, yes_ask,
              yes_ask_size, last_price, volume). Prices moved to
              `/markets/{ticker}/orderbook` -> `orderbook_fp`, which lists
              resting bids per side as [price, dollars].

So this is a rewrite against the current APIs, not a copy of the old modules.
"""
import re
import time
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

TIMEOUT = 30
CACHE_SECONDS = 45

# Minimum dollars resting at the quoted price for it to count as real.
MIN_DEPTH_USD = 10.0

POLY_API = "https://gateway.polymarket.us"
POLY_DEFAULT_COEF = 0.0695

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_TAKER_COEF = 0.07
KALSHI_SERIES = {
    # verified 2026-10-14 via /series/{ticker}: all fee_multiplier 1
    "h2h": "KXNCAAMBGAME",
    "spreads": "KXNCAAMBSPREAD",
    "totals": "KXNCAAMBTOTAL",
}

NOVIG_LIVE_COEF = 0.03

# Polymarket sportsMarketType -> our market key. Full game only: this hub
# tracks game lines, and the per-period types multiply the request cost.
POLY_TYPES = {
    # UNVERIFIED for basketball: assumed by analogy with the football names.
    # Check against a live CBB board in November; if Polymarket uses other
    # strings, CBB markets are silently skipped (no wrong prices, just none).
    "basketball_team_full_game_winner": "h2h",
    "basketball_team_full_game_spread": "spreads",
    "basketball_team_full_game_total": "totals",
}

_cache = {}


# ---------------------------------------------------------------------------
# Pricing
# ---------------------------------------------------------------------------

def to_american(decimal_odds):
    """Decimal -> American, as an int."""
    if decimal_odds is None or decimal_odds <= 1:
        return None
    if decimal_odds >= 2:
        return int(round((decimal_odds - 1) * 100))
    return int(round(-100 / (decimal_odds - 1)))


def american_to_prob(price):
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if p == 0:
        return None
    return (-p) / ((-p) + 100.0) if p < 0 else 100.0 / (p + 100.0)


def net_from_ask(ask, coef):
    """
    Fee-adjusted American price for taking an exchange offer at `ask` (0-1).

    The fee is quadratic in the price, so it bites hardest on a coin flip and
    barely at all on a heavy favourite: at 50c a 0.07 coefficient costs 1.75c,
    at 95c it costs 0.33c.
    """
    try:
        p = float(ask)
    except (TypeError, ValueError):
        return None, None
    if not (0.02 <= p <= 0.98):
        return None, None
    cost = p + coef * p * (1.0 - p)
    if cost <= 0 or cost >= 1:
        return None, None
    return to_american(1.0 / cost), round(p, 4)


def novig_live_net(american_price):
    """
    Strip NoVig's live taker fee off an Odds API price.

    NoVig charges 0.03 * P * (1-P) on game markets ONLY when the game is live.
    Pregame prices need no adjustment, which is why Odds Screen's pregame board
    can use the Odds API feed as-is; every game on this board is in-play, so it
    always applies here.
    """
    p = american_to_prob(american_price)
    if p is None or not (0.02 <= p <= 0.98):
        return None
    cost = p + NOVIG_LIVE_COEF * p * (1.0 - p)
    if cost <= 0 or cost >= 1:
        return None
    return to_american(1.0 / cost)


def _get(url, **params):
    r = requests.get(url, params=params or None,
                     headers={"Accept": "application/json"}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Polymarket US
# ---------------------------------------------------------------------------

def fetch_polymarket(live_only=True):
    """
    Full-game CFB quotes from Polymarket US, net of fees.

    One request covers the whole league (~3.7 MB). Returns a list of dicts:
        {home_team, away_team, commence_time, market, side, point, price,
         raw_ask, book}
    """
    key = ("poly", live_only)
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]

    try:
        data = _get(POLY_API + "/v2/leagues/cfb/events")
    except Exception as e:
        logger.warning("polymarket fetch failed: %s", e)
        return []

    events = data if isinstance(data, list) else (data.get("events") or data.get("data") or [])
    out = []
    for ev in events:
        if live_only and not ev.get("live"):
            continue
        teams = _poly_event_teams(ev)
        if not teams:
            continue
        home, away = teams
        # Polymarket publishes a full alt ladder as separate markets - 40+
        # strikes per game. Only the MAIN line belongs on the board: these rows
        # feed the same best-line ranking as the sportsbooks, which prefers the
        # highest point, so a deep +30.5 alt would otherwise be crowned "best
        # spread" over a real -1.5. Keep the most balanced two-sided strike per
        # market type, the same rule Odds Screen uses.
        best = {}
        for m in ev.get("markets") or []:
            if m.get("status") != "MARKET_STATUS_OPEN":
                continue
            mkt = POLY_TYPES.get(m.get("sportsMarketType"))
            if not mkt:
                continue
            ask = _num((m.get("bestAskQuote") or {}).get("value"))
            bid = _num((m.get("bestBidQuote") or {}).get("value"))
            if ask is None or bid is None:
                continue
            balance = abs(((ask + bid) / 2.0) - 0.5)
            cur = best.get(mkt)
            if cur is None or balance < cur[0]:
                best[mkt] = (balance, m)

        for mkt, (_, m) in best.items():
            out.extend(_poly_market_quotes(m, mkt, home, away, ev))

    logger.info("polymarket: %d quotes from %d events", len(out), len(events))
    _cache[key] = (time.time(), out)
    return out


def _poly_event_teams(ev):
    """(home_name, away_name) from the first market's sides, or None."""
    for m in ev.get("markets") or []:
        names = {}
        for s in m.get("marketSides") or []:
            t = s.get("team") or {}
            if t.get("name"):
                names[t.get("id")] = t["name"]
        if len(names) == 2:
            vals = list(names.values())
            # Titles read "Away vs. Home" on this feed.
            title = ev.get("title") or ""
            if " vs. " in title:
                a, h = title.split(" vs. ", 1)
                a, h = a.strip(), h.strip()
                if a in vals and h in vals:
                    return h, a
            return vals[1], vals[0]
    return None


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _poly_market_quotes(m, mkt, home, away, ev):
    """
    Both sides of one Polymarket market, fee-adjusted.

    The feed gives one bid/ask pair describing the LONG side. Taking the long
    side costs `bestAsk`; taking the short side costs `1 - bestBid`. (Checked
    on a live game: ISU +28.5 bid 0.9850 / ask 0.9950, so BYU -28.5 costs
    1 - 0.9850 = 1.5c, which is the only sane price for that side.)
    """
    ask = _num((m.get("bestAskQuote") or {}).get("value"))
    bid = _num((m.get("bestBidQuote") or {}).get("value"))
    if ask is None and bid is None:
        return []
    coef = _num(m.get("feeCoefficient")) or POLY_DEFAULT_COEF
    line = _num(m.get("line"))

    long_side = short_side = None
    for s in m.get("marketSides") or []:
        if s.get("long"):
            long_side = s
        else:
            short_side = s
    if long_side is None:
        return []

    rows = []

    def add(side_label, point, raw):
        price, raw_ask = net_from_ask(raw, coef)
        if price is None:
            return
        rows.append({
            "book": "polymarket",
            "home_team": home, "away_team": away,
            "commence_time": ev.get("startTime"),
            "market": mkt, "side": side_label,
            "point": point, "price": price, "raw_ask": raw_ask,
        })

    if mkt == "totals":
        # Long side is the Over on Polymarket's total markets.
        if ask is not None:
            add("over", line, ask)
        if bid is not None:
            add("under", line, 1.0 - bid)
        return rows

    long_team = ((long_side.get("team") or {}).get("name"))
    long_is_home = long_team == home
    other_is_home = not long_is_home

    if mkt == "h2h":
        if ask is not None:
            add("home" if long_is_home else "away", None, ask)
        if bid is not None:
            add("home" if other_is_home else "away", None, 1.0 - bid)
        return rows

    # spreads: the long side holds `line`, the other side its negation
    if ask is not None:
        add("home" if long_is_home else "away", line, ask)
    if bid is not None and line is not None:
        add("home" if other_is_home else "away", -line, 1.0 - bid)
    return rows


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

_fee_mult = {}


def _kalshi_fee_mult(series):
    """Series fee multiplier; football game series are 1."""
    if series not in _fee_mult:
        try:
            s = _get(KALSHI_API + "/series/" + series).get("series") or {}
            ok = str(s.get("fee_type", "")).startswith("quadratic")
            _fee_mult[series] = float(s.get("fee_multiplier") or 1) if ok else 1.0
        except Exception as e:
            logger.warning("kalshi: fee lookup failed for %s: %s", series, e)
            _fee_mult[series] = 1.0
    return _fee_mult[series]


def kalshi_open_markets(series, date_tags):
    """
    Open markets for a series whose ticker carries one of `date_tags`
    (e.g. {"26OCT10"}). Paginates; prices are NOT here (see module docstring).
    """
    out, cursor = [], None
    for _ in range(10):
        params = {"series_ticker": series, "status": "open", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        try:
            j = _get(KALSHI_API + "/markets", **params)
        except Exception as e:
            logger.warning("kalshi markets fetch failed (%s): %s", series, e)
            break
        ms = j.get("markets") or []
        for m in ms:
            t = m.get("ticker") or ""
            if any(tag in t for tag in date_tags):
                out.append(m)
        cursor = j.get("cursor")
        if not cursor or not ms:
            break
    return out


def kalshi_best_ask(ticker):
    """
    Best YES ask and the dollars resting there, from the orderbook.

    `orderbook_fp` lists RESTING BIDS per side as [price, dollars], ascending.
    Buying YES means crossing the best NO bid, so:

        yes_ask = 1 - max(no_bid)

    Verified live: best no bid 0.70 against a best yes bid of 0.29, giving a
    yes ask of 0.30 and a sane 1c spread.
    """
    try:
        j = _get(KALSHI_API + "/markets/{0}/orderbook".format(ticker))
    except Exception as e:
        logger.debug("kalshi orderbook failed for %s: %s", ticker, e)
        return None, None
    ob = j.get("orderbook_fp") or j.get("orderbook") or {}
    no_levels = ob.get("no_dollars") or []
    if not no_levels:
        return None, None
    try:
        price, dollars = no_levels[-1]
        best_no = float(price)
        depth = float(dollars)
    except (ValueError, TypeError, IndexError):
        return None, None
    return round(1.0 - best_no, 4), depth


def fetch_kalshi(date_tags, max_markets=60):
    """
    Fee-adjusted Kalshi game-winner quotes for the given ticker date tags.

    Costs ONE request per contract because prices now live in the orderbook, so
    it is capped and meant to run on the slow odds cadence, not the scoreboard
    heartbeat.
    """
    key = ("kalshi", tuple(sorted(date_tags)))
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]

    series = KALSHI_SERIES["h2h"]
    mult = _kalshi_fee_mult(series)
    markets = kalshi_open_markets(series, date_tags)

    out = []
    for m in markets[:max_markets]:
        ask, depth = kalshi_best_ask(m.get("ticker"))
        if ask is None or depth is None or depth < MIN_DEPTH_USD:
            continue
        price, raw = net_from_ask(ask, KALSHI_TAKER_COEF * mult)
        if price is None:
            continue
        out.append({
            "book": "kalshi",
            "ticker": m.get("ticker"),
            "title": m.get("title"),
            "team": m.get("yes_sub_title") or m.get("title"),
            "market": "h2h",
            "price": price, "raw_ask": raw, "depth_usd": depth,
        })

    logger.info("kalshi: %d priced of %d markets", len(out), len(markets))
    _cache[key] = (time.time(), out)
    return out


def kalshi_games(quotes):
    """
    Group Kalshi contracts into two-sided games.

    Each contract is one team's "wins" market, and the ticker prefix pairs
    them: KXNCAAFGAME-26OCT10BSUFRES-{BSU,FRES}. Returns a list of
        {teams: [name, name], quotes: {name: quote}}
    Home/away orientation is deliberately NOT guessed here - the event matcher
    scores both orientations and picks the better one.
    """
    by_prefix = {}
    for q in quotes:
        t = q.get("ticker") or ""
        prefix = t.rsplit("-", 1)[0] if "-" in t else t
        by_prefix.setdefault(prefix, []).append(q)

    games = []
    for prefix, qs in by_prefix.items():
        named = {}
        for q in qs:
            name = (q.get("team") or "").strip()
            if name:
                named[name] = q
        if len(named) == 2:
            games.append({"prefix": prefix, "teams": list(named.keys()), "quotes": named})
    return games


def kalshi_date_tags(now=None):
    """Ticker date tags for today and yesterday ET, e.g. {'26OCT10','26OCT09'}."""
    now = now or datetime.now(timezone.utc)
    tags = set()
    for delta in (0, -1):
        d = now.timestamp() + delta * 86400
        dt = datetime.fromtimestamp(d, tz=timezone.utc)
        tags.add("{0:02d}{1}{2:02d}".format(
            dt.year % 100, dt.strftime("%b").upper(), dt.day))
    return tags


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("--- Polymarket (live CFB) ---")
    for q in fetch_polymarket(live_only=True)[:12]:
        print("  {0} @ {1:<22} {2:<8} {3:<5} {4:>6} {5:>6}  (raw {6})".format(
            q["away_team"], q["home_team"], q["market"], q["side"],
            q["point"] if q["point"] is not None else "-", q["price"], q["raw_ask"]))
    print()
    print("--- Kalshi ---")
    for q in fetch_kalshi(kalshi_date_tags())[:12]:
        print("  {0:<34} {1:>6}  (raw {2}, ${3:,.0f})".format(
            q["ticker"], q["price"], q["raw_ask"], q["depth_usd"]))
