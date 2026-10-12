"""
NoVig game markets from NoVig's own public catalog (no key) for the odds board.

Why not the Odds API's NoVig feed: it carries one line per market with no size,
so the board could never tell a real price from $3 of dust, and it gave no
outcome ids to bet with. NoVig publishes the whole ladder, the full order book
and real outcome ids, all unauthenticated (`Key: none` on /v3/public/...).

THREE things about NoVig's model that the prices here depend on:

1. COMPLEMENTARY BID BOOKS. /book lists resting BUY orders per outcome, not
   bids-and-asks. The ask for one side is 1 - the best bid on the other:
   SF bid .775 / WSH bid .19 -> ask SF .810, ask WSH .225 (a 3.5c spread).
   Lifting a bid of `qty` on the other side is what fills you, so the money
   available to BUY side A sits in side B's book.

2. qty IS IN CENTS OF PAYOUT. One contract pays 1c, so a level is worth
   price x qty x $0.01. A 300,000-qty order at .77 is $2,310 of risk, not
   $231k. Getting this wrong is a 100x stake error.

3. FEES ARE LIVE-ONLY on game markets. The per-market fee block reads
   {coefficient .03, makerCredit .5, charged WHEN_LIVE}, so a PREGAME ask is
   already the net price and needs no adjustment -- unlike Kalshi/Polymarket,
   where the taker fee is baked in here. The block is read per market, never
   assumed, so a schedule change shows up as a price change.

RATE LIMITS DECIDE WHAT THIS MODULE CAN COVER. There is no bulk book endpoint
and no ETag, and CloudFront meters the public routes to a ~9-request burst
that refills near 2/s (measured 2026-10-10: 2.0/s ran 40/40 clean, 4.0/s took
30% 429s; a sustained hammer earns `Retry-After: 60` on every public route,
the cheap catalog call included). A book costs one request, so at 2/s:

    NFL      1,318 markets / 29 events   -> 11 min for the full ladder
    NCAAF    5,000+ markets / 109 events -> 40+ min for the full ladder
    main lines only (MONEY + the rung either side of each ladder's middle,
    games inside HOT_HOURS)              -> ~1 min NFL, ~3 min NCAAF

So REST covers MAIN LINES on a 1-3 minute refresh, and the full alt ladder
only fills in slowly in the background (fine for NFL on a board that runs all
day, partial at best for a 90-game college slate). A background thread does
the fetching under a token bucket, hottest first, and fetch_novig() never
blocks on it -- it returns whatever is warm.

THE WEBSOCKET IS THE REAL FIX: /v3/ws has a `bbo` channel and takes 8,192
markets on one connection, so every strike in both leagues streams over a
single socket with no polling at all. It needs a `trading::read` key -- which
cannot place orders, cannot move money, and whose reads tolerate a stale
geolocation. Until that key exists, treat alt_lines here as best-effort.

Output: game dicts shaped like the Kalshi/bet365 scrapers' ({away_team,
home_team, markets, alt_lines}) plus "_date" (ET game date) and a "*_ref"
("marketId|outcomeId") on every quote so an order can be placed on it later.
"""
import logging
import re
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

import novig_filter

logger = logging.getLogger(__name__)

API = "https://api.novig.com/v3/public"
CACHE_SECONDS = 60
MIN_DEPTH_USD = 10.0       # user rule 2026-09-28: under $10 at the price, don't show it
MIN_PROB, MAX_PROB = 0.02, 0.98

# Public-route budget, measured 2026-10-10 (see the module docstring). 2.0/s
# is the ceiling that takes no 429s at all; going faster nets barely more good
# responses and risks the 60 s block on every public route.
REQ_PER_SEC = 2.0
BURST = 8

# NoVig league -> Odds API sport key. NoVig lists no 1Q football markets.
LEAGUES = {"americanfootball_nfl": "NFL", "americanfootball_ncaaf": "NCAAF"}

# NoVig marketType -> the board's market key. MONEY_1H is left out for the
# same reason Kalshi's 1H winner series is: a tied half voids to fair market
# value instead of settling as a 2-way line, so it isn't the same bet as the
# Odds API's h2h_h1.
TYPES = {"MONEY": "h2h", "SPREAD": "spreads", "TOTAL": "totals",
         "SPREAD_1H": "spreads_h1", "TOTAL_1H": "totals_h1"}

# Hot tier: MONEY for every open game, plus this many strikes either side of
# each ladder's middle for games kicking off inside HOT_HOURS. NoVig centres a
# ladder on its own market, so the middle rung is the main line often enough to
# pick it. Everything else refreshes on the cold cycle behind it.
HOT_RUNGS = 1
HOT_HOURS = 36

# NoVig team names that differ from the Odds API's.
_NAME_FIX = {"Louisiana-Monroe": "UL Monroe", "Houston Christian": "Houston Baptist",
             "Southern Miss": "Southern Mississippi"}

_ET = timezone(timedelta(hours=-4))     # ET game date; only used to pair games

_session = requests.Session()
_cache: dict = {}         # sport_key -> (epoch, games)
_books: dict = {}         # marketId -> (epoch, {outcomeId: [(price, qty)]})
_lock = threading.Lock()
_wanted: dict = {}        # league -> (hot marketIds, cold marketIds)
_cold_cursor = 0
_refresher: threading.Thread | None = None
_blocked_until = 0.0      # set from Retry-After on a 429
_tokens, _tokens_ts = float(BURST), time.time()


# ── public-route budget ───────────────────────────────────────────────────────

def _take_token() -> None:
    """Block until the token bucket allows one public request."""
    global _tokens, _tokens_ts
    while True:
        with _lock:
            now = time.time()
            _tokens = min(BURST, _tokens + (now - _tokens_ts) * REQ_PER_SEC)
            _tokens_ts = now
            wait = max(0.0, _blocked_until - now)
            if not wait and _tokens >= 1:
                _tokens -= 1
                return
            if not wait:
                wait = (1 - _tokens) / REQ_PER_SEC
        time.sleep(min(wait, 5.0))


def _get(path: str, **params):
    """One public GET under the budget. None on a throttle or transport error."""
    global _blocked_until
    _take_token()
    try:
        r = _session.get(f"{API}{path}", params=params, timeout=20)
    except requests.RequestException as e:
        logger.warning(f"novig: {path}: {type(e).__name__}: {e}")
        return None
    if r.status_code == 429:
        try:
            cool = float(r.headers.get("Retry-After") or 60)
        except ValueError:
            cool = 60.0
        with _lock:
            _blocked_until = time.time() + cool
        logger.warning(f"novig: throttled on {path}, holding off {cool:g}s")
        return None
    if not r.ok:
        logger.warning(f"novig: {path}: HTTP {r.status_code}")
        return None
    try:
        return r.json()
    except ValueError:
        logger.warning(f"novig: {path}: non-JSON body")
        return None


def _paged(path: str, **params) -> list:
    out, after = [], None
    for _ in range(20):
        d = _get(path, **params, **({"after": after} if after else {}))
        if not d:
            break
        out += d.get("items") or []
        after = d.get("next")
        if not after:
            break
    return out


# ── books ────────────────────────────────────────────────────────────────────

def _book(market_id: str):
    """{outcomeId: [(price, qty), ...]} of resting BIDS, or None."""
    d = _get(f"/catalog/markets/{market_id}/book")
    if not d:
        return None
    out = {}
    for oid, orders in (d.get("orders") or {}).items():
        lv = []
        for o in orders or []:
            try:
                lv.append((float(o["price"]), int(o["qty"])))
            except (KeyError, TypeError, ValueError):
                continue
        out[oid] = lv
    return out


COLD_PER_PASS = 60        # cold books between hot passes (~30 s of budget)


def _refresh_loop() -> None:
    """Refresh every hot market, then a slice of the cold ladder, forever.

    Interleaving matters: one pass over every market takes 11-40 minutes, so a
    single priority-ordered cycle would leave the main lines as stale as the
    far rungs. This keeps the hot tier on a ~1-2 minute heartbeat and lets the
    ladder fill in behind it, resuming where it left off each pass.
    """
    global _cold_cursor
    while True:
        with _lock:
            hot = [m for h, _ in _wanted.values() for m in h]
            cold = [m for _, c in _wanted.values() for m in c]
        if not hot and not cold:
            time.sleep(5)
            continue
        for mid in hot:
            _store(mid)
        if cold:
            with _lock:
                start = _cold_cursor % len(cold)
                _cold_cursor = start + COLD_PER_PASS
            for mid in (cold * 2)[start:start + COLD_PER_PASS]:
                _store(mid)
        time.sleep(1)


def _store(market_id: str) -> None:
    b = _book(market_id)
    if b is not None:
        with _lock:
            _books[market_id] = (time.time(), b)


def _ensure_refresher() -> None:
    global _refresher
    if _refresher is None or not _refresher.is_alive():
        _refresher = threading.Thread(target=_refresh_loop, name="novig-books", daemon=True)
        _refresher.start()


def _american(decimal: float) -> str:
    return f"+{round((decimal - 1) * 100)}" if decimal >= 2 else str(round(-100 / (decimal - 1)))


def _fill(other_levels):
    """(ask, $ available at or better) for buying a side, walking the OTHER
    side's bids until MIN_DEPTH_USD is covered.

    Same rule as the Polymarket path: a top level holding less than $10 is not
    a price you can bet, so the walk returns the price that actually fills $10
    and the board shows that instead. (None, 0) if the whole book is thinner
    than $10. qty is cents of payout, so a level is worth (1-q) x qty x $0.01.
    """
    worst, usd = None, 0.0
    for q, qty in sorted(other_levels or [], key=lambda l: -l[0]):
        ask = round(1.0 - q, 4)
        if not (MIN_PROB <= ask <= MAX_PROB):
            continue
        worst, usd = ask, usd + ask * qty * 0.01
        if usd >= MIN_DEPTH_USD:
            return ask, round(usd, 2)
    return (None, 0.0) if usd < MIN_DEPTH_USD else (worst, round(usd, 2))


def _quote(other_levels, fee_coef: float, live: bool):
    """(net American odds, ask, $ available) for BUYING a side, given the OTHER
    side's bids. Pregame charges no fee, so the ask is already the net price."""
    ask, usd = _fill(other_levels)
    if ask is None:
        return None, None, None
    cost = ask + (fee_coef * ask * (1 - ask) if live else 0.0)
    if cost >= 1:
        return None, None, None
    return _american(1 / cost), ask, usd


# ── catalog -> board rows ────────────────────────────────────────────────────

def _trailing_num(name: str):
    m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*$", name or "")
    return float(m.group(1)) if m else None


def _abbrevs(markets: list):
    """(away, home) abbreviations for one event. NoVig names outcomes by
    abbreviation ("SF -14.5") but describes events by full name, and a SPREAD
    strike is the HOME handicap -- so the outcome whose own number equals the
    strike is the home side. A TOTAL's description carries "AWY @ HOM"."""
    for m in markets:
        if m.get("marketType") == "TOTAL":
            d = re.match(r"^(\S+)\s+@\s+(\S+)\s", m.get("description") or "")
            if d:
                return d.group(1), d.group(2)
    for m in markets:
        if m.get("marketType") != "SPREAD" or m.get("strike") is None:
            continue
        try:
            strike = float(m["strike"])
        except (TypeError, ValueError):
            continue
        outs = m.get("outcomes") or []
        if len(outs) != 2:
            continue
        for i, o in enumerate(outs):
            if _trailing_num(o.get("name")) == strike:
                home = (o.get("name") or "").rsplit(" ", 1)[0]
                away = (outs[1 - i].get("name") or "").rsplit(" ", 1)[0]
                return away, home
    return None, None


# A main line has to look like one. With the cache partly warm the most
# balanced WARM rung can be far off the real line (Baltimore @ Atlanta, with
# 365 of 7,333 books warm, picked ATL +3.5 at a 0.45 prob gap while the true
# line was -3.5), and a bogus main line is worse than none: leaving it unset
# lets EXCHANGE_FEED_FALLBACK show the Odds API quote instead.
MAIN_LINE_MAX_GAP = 0.15
# Below this h2h gap the favourite is noise, so the spread sign proves nothing.
FAV_EDGE = 0.05


def _plausible_main(e: dict, mk: str, h2h) -> bool:
    """Is this rung believable as the main line, or an artefact of a cold cache?

    Two tests. The sides have to be near balanced, and -- for spreads -- the
    favourite has to agree with the game's own moneyline. The gap test alone
    was not enough: a half-warm Ravens/Falcons ladder offered ATL +1.5 at a
    0.19 gap while NoVig's own moneyline had Atlanta at .575, a 10-point
    contradiction that would have shown the favourite as a dog on the board.
    """
    if abs(e["away_prob"] - e["home_prob"]) > MAIN_LINE_MAX_GAP:
        return False
    if not mk.startswith("spreads") or e.get("home_point") is None:
        return True
    if not h2h or h2h.get("home_prob") is None or h2h.get("away_prob") is None:
        return True
    edge = h2h["home_prob"] - h2h["away_prob"]
    if abs(edge) < FAV_EDGE or e["home_point"] == 0:
        return True
    return (edge > 0) == (e["home_point"] < 0)

def _monotone(lst: list) -> list:
    """Drop rungs that contradict their own ladder, via novig_filter.

    NoVig's resting bids are not monotone across a ladder, and not only in
    thin games: on a liquid Ravens/Falcons board ATL -9.5 was bid .295 while
    the EASIER ATL -8.5 was bid .285. novig_filter already documents and
    handles exactly this for the Odds API NoVig feed, so the same checker runs
    here rather than a second algorithm with its own tolerance -- it keys
    spreads on the away point, where away_prob rises and home_prob falls,
    which is already this module's away/home convention (totals: away=Under,
    home=Over). One-sided rungs are left alone; there is nothing to compare a
    lone quote against, and it is still a price with $10 behind it.
    """
    two = [e for e in lst if e.get("home_point") is not None
           and e.get("away_prob") is not None and e.get("home_prob") is not None]
    if len(two) < 3:
        return lst
    rungs = [(e["away_point"], e["away_prob"], e["home_prob"], i) for i, e in enumerate(two)]
    keep = set(novig_filter.monotone_ladder(rungs))
    dropped = {id(e) for i, e in enumerate(two) if i not in keep}
    return [e for e in lst if id(e) not in dropped]


def _cold_order(by_event: dict, hot: set) -> list:
    """The non-hot ladder, ordered by how far a rung sits from its ladder's
    middle rather than grouped by game. Sweeping event-by-event would give the
    first few games complete ladders and the rest nothing; widening every
    game's ladder together is what the +EV scan can actually use.
    """
    ranked = []
    for ms in by_event.values():
        ladders: dict = {}
        for m in ms:
            if m["marketId"] in hot or m.get("strike") is None:
                continue
            ladders.setdefault(m["marketType"], []).append(m)
        for lst in ladders.values():
            lst.sort(key=lambda m: float(m["strike"]))
            mid = (len(lst) - 1) / 2
            for i, m in enumerate(lst):
                ranked.append((abs(i - mid), m["marketId"]))
    ranked.sort(key=lambda r: r[0])
    return [mid for _, mid in ranked]


def _sides(m: dict, away_abbr, home_abbr):
    """(away_outcome, home_outcome) for a market, or (None, None).
    TOTAL maps Over->home / Under->away, matching the board's convention."""
    outs = m.get("outcomes") or []
    if len(outs) != 2:
        return None, None
    mt = m.get("marketType")
    if mt in ("TOTAL", "TOTAL_1H"):
        over = next((o for o in outs if (o.get("name") or "").lower().startswith("over")), None)
        under = next((o for o in outs if (o.get("name") or "").lower().startswith("under")), None)
        return (under, over) if over and under else (None, None)
    if mt in ("SPREAD", "SPREAD_1H"):
        try:
            strike = float(m["strike"])
        except (KeyError, TypeError, ValueError):
            return None, None
        for i, o in enumerate(outs):
            if _trailing_num(o.get("name")) == strike:
                return outs[1 - i], o                 # (away, home); strike is home's
        return None, None
    # MONEY: match the abbreviation.
    by = {(o.get("name") or "").strip(): o for o in outs}
    if away_abbr in by and home_abbr in by:
        return by[away_abbr], by[home_abbr]
    return None, None


def _entry(m: dict, away_o, home_o, books, live: bool):
    """One board quote for a market, or None if neither side is bettable."""
    fee = m.get("fee") or {}
    try:
        coef = float(fee.get("coefficient") or 0)
    except (TypeError, ValueError):
        coef = 0.0
    charged = str(fee.get("charged") or "")
    chargeable = live and charged != "NEVER"
    b = books.get(m["marketId"]) or {}
    away_lv, home_lv = b.get(away_o["outcomeId"]) or [], b.get(home_o["outcomeId"]) or []
    # Buying one side means lifting the OTHER side's bids.
    a_odds, a_prob, a_usd = _quote(home_lv, coef, chargeable)
    h_odds, h_prob, h_usd = _quote(away_lv, coef, chargeable)
    if not (a_odds or h_odds):
        return None
    mid = m["marketId"]
    e = {"away_odds": a_odds, "away_prob": a_prob, "away_usd": a_usd,
         "home_odds": h_odds, "home_prob": h_prob, "home_usd": h_usd,
         "away_ref": f"{mid}|{away_o['outcomeId']}", "home_ref": f"{mid}|{home_o['outcomeId']}"}
    mt = m.get("marketType")
    if mt in ("TOTAL", "TOTAL_1H"):
        x = float(m["strike"])
        e["home_point"] = e["away_point"] = x      # home = Over
    elif mt in ("SPREAD", "SPREAD_1H"):
        x = float(m["strike"])                     # home handicap
        e["home_point"], e["away_point"] = x, -x
    else:
        e["home_point"] = e["away_point"] = None
    return e


def _hot(markets: list, events: dict) -> list:
    """Market ids worth refreshing first: MONEY for every open game, plus the
    middle HOT_RUNGS strikes of each ladder for games inside HOT_HOURS. At 2
    requests a second the hot tier has to stay near a minute's worth of work,
    so the far rungs and the far slate ride the cold cycle behind it."""
    cutoff = (time.time() + HOT_HOURS * 3600) * 1000
    out, ladders = [], {}
    for m in markets:
        if m.get("marketType") == "MONEY":
            out.append(m["marketId"])
            continue
        if m.get("strike") is None:
            continue
        starts = (events.get(m.get("eventId")) or {}).get("startsTs")
        if starts and starts <= cutoff:
            ladders.setdefault((m["eventId"], m["marketType"]), []).append(m)
    for lst in ladders.values():
        lst.sort(key=lambda m: float(m["strike"]))
        mid = len(lst) // 2
        lo, hi = max(0, mid - HOT_RUNGS), min(len(lst), mid + HOT_RUNGS + 1)
        out += [m["marketId"] for m in lst[lo:hi]]
    return out


def fetch_novig(sport_key: str) -> list:
    """NoVig games for one sport (cached CACHE_SECONDS). [] on failure.

    Never blocks on the book crawl: the catalog call is cheap and synchronous,
    prices come from whatever the background refresher has warmed. A cold start
    therefore fills in over the first minute or two.
    """
    hit = _cache.get(sport_key)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]
    league = LEAGUES.get(sport_key)
    if not league:
        return []
    try:
        games = _build(league)
    except Exception as e:
        logger.warning(f"novig {sport_key}: fetch failed: {type(e).__name__}: {e}")
        return hit[1] if hit else []
    _cache[sport_key] = (time.time(), games)
    priced = sum(1 for g in games if g["markets"])
    logger.info(f"novig {sport_key}: {len(games)} games, {priced} priced")
    return games


def _build(league: str) -> list:
    events = {e["eventId"]: e for e in _paged("/catalog/events", league=league, limit=500)}
    markets = _paged("/catalog/markets", league=league,
                     marketType=",".join(TYPES), limit=5000)
    if not events or not markets:
        return []

    # Tell the refresher what to keep warm, hottest first, and start it.
    by_event: dict = {}
    for m in markets:
        if m.get("status") == "OPEN" and m.get("eventId") in events:
            by_event.setdefault(m["eventId"], []).append(m)
    hot = _hot([m for ms in by_event.values() for m in ms], events)
    hot_set = set(hot)
    rest = _cold_order(by_event, hot_set)
    with _lock:
        _wanted[league] = (hot, rest)
        books = {mid: b for mid, (_, b) in _books.items()}
    _ensure_refresher()

    games = []
    for eid, ms in by_event.items():
        ev = events[eid]
        parts = (ev.get("description") or "").split(" @ ")
        if len(parts) != 2:
            continue
        away, home = (_NAME_FIX.get(p.strip(), p.strip()) for p in parts)
        live = not str(ev.get("status") or "").endswith("PREGAME")
        try:
            date = datetime.fromtimestamp(ev["startsTs"] / 1000, _ET).date()
        except (KeyError, TypeError, ValueError, OSError):
            date = None
        away_abbr, home_abbr = _abbrevs(ms)
        g = {"away_team": away, "home_team": home, "_date": date,
             "markets": {}, "alt_lines": {}}
        rungs: dict = {}
        for m in ms:
            mk = TYPES.get(m.get("marketType"))
            if not mk:
                continue
            away_o, home_o = _sides(m, away_abbr, home_abbr)
            if not (away_o and home_o):
                continue
            e = _entry(m, away_o, home_o, books, live)
            if not e:
                continue
            if mk == "h2h":
                g["markets"]["h2h"] = e
            else:
                rungs.setdefault(mk, []).append(e)
        for mk, lst in rungs.items():
            lst = _monotone(lst)
            if not lst:
                continue
            g["alt_lines"][mk] = lst
            two = [e for e in lst if e["away_odds"] and e["home_odds"]]
            if two:   # main line: the most balanced two-sided strike...
                best = min(two, key=lambda e: abs(e["away_prob"] - e["home_prob"]))
                if _plausible_main(best, mk, g["markets"].get("h2h")):
                    g["markets"][mk] = best
        if g["markets"] or g["alt_lines"]:
            games.append(g)
    return games


def status() -> dict:
    """Refresher health, for /api/novig/status."""
    with _lock:
        now = time.time()
        ages = [now - ts for ts, _ in _books.values()]
        hot = sum(len(h) for h, _ in _wanted.values())
        cold = sum(len(c) for _, c in _wanted.values())
        return {"hot": hot, "cold": cold, "cached": len(_books),
                "oldest_book_s": round(max(ages), 1) if ages else None,
                "median_book_s": round(sorted(ages)[len(ages) // 2], 1) if ages else None,
                "throttled_for_s": round(max(0.0, _blocked_until - now), 1),
                "running": bool(_refresher and _refresher.is_alive())}
