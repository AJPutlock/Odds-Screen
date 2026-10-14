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

(Those figures are the PUBLIC routes. With a read key the same reads go to the
signed routes at 12/s -- see SIGNED_REQ_PER_SEC -- so the tail the websocket
can't hold refreshes every ~3.5 min instead of ~20.)

So REST covers MAIN LINES on a 1-3 minute refresh, and the full alt ladder
only fills in slowly in the background (fine for NFL on a board that runs all
day, partial at best for a 90-game college slate). A background thread does
the fetching under a token bucket, hottest first, and fetch_novig() never
blocks on it -- it returns whatever is warm.

LIVE BOOKS (2026-10-12): with a trading::read key in .env, scrapers/novig_ws.py
streams the full order book for the most important ~2,000 game-line markets
per read key (the key's real cap is 2,048, not the docs' 8,192), most
important first: every MONEY, then the rungs nearest each ladder's middle.
Streamed books override polled ones here, the REST poller skips them and
spends its 2/s on the tail, and the catalog itself is re-read only every
CACHE_SECONDS while prices are rebuilt from the current books on every call.
The slate is ~4,600 game-line markets, so one read key covers ~43% by count
-- the part that matters -- and two more read keys would cover all of it.

Output: game dicts shaped like the Kalshi/bet365 scrapers' ({away_team,
home_team, markets, alt_lines}) plus "_date" (ET game date) and a "*_ref"
("marketId|outcomeId") on every quote so an order can be placed on it later.
"""
import logging
import re
import threading
import time
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone

import requests

import novig_filter

try:                       # live books over the websocket, when a read key exists
    from scrapers import novig_ws
except ImportError:
    novig_ws = None

logger = logging.getLogger(__name__)

API = "https://api.novig.com/v3/public"
CACHE_SECONDS = 60        # the catalog (events + market list): cheap, but it spends the 2/s budget
BUILD_SECONDS = 3         # built games: prices are rebuilt from the live books this often
MIN_DEPTH_USD = 10.0       # user rule 2026-09-28: under $10 at the price, don't show it
MIN_PROB, MAX_PROB = 0.02, 0.98

# Public-route budget, measured 2026-10-10 (see the module docstring). 2.0/s
# is the ceiling that takes no 429s at all; going faster nets barely more good
# responses and risks the 60 s block on every public route.
REQ_PER_SEC = 2.0
BURST = 8

# SIGNED-route budget. With a trading::read key, the same catalog and book reads
# exist as signed routes (/v3/catalog/...) that bill the KEY's `read` bucket
# (64 burst, 16/s refill) instead of CloudFront's per-IP 2/s. Measured
# 2026-10-12: 15/s sustained, zero 429s, identical response shapes. 12/s keeps
# headroom for the catalog refresh, and it stops this poller from starving every
# other process on the PC of NoVig's public API (which a 2/s poller did).
SIGNED_REQ_PER_SEC = 12.0
SIGNED_BURST = 32
SIGNED_COOLDOWN = 300      # after a 401/451 on the signed routes, use public for this long

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
_catalog: dict = {}       # league -> (epoch, events, markets)
_books: dict = {}         # marketId -> (epoch, {outcomeId: [(price, qty)]})
_lock = threading.Lock()
_wanted: dict = {}        # league -> (hot marketIds, cold marketIds)
_cold_cursor = 0
_refresher: threading.Thread | None = None
_blocked_until = 0.0      # set from Retry-After on a 429
_signed_off_until = 0.0   # signed routes paused (key refused / location check) until
_tls = threading.local()  # one signed client per thread (requests.Session isn't thread-safe)
_tokens, _tokens_ts = float(BURST), time.time()


# ── public-route budget ───────────────────────────────────────────────────────

def _signed_client():
    """This thread's signed client on the read key, or None to use public routes.
    Runs on every request (via _rate), so the key is read from .env at most once
    a minute per thread, never per call."""
    if time.time() < _signed_off_until:
        return None
    c = getattr(_tls, "client", None)
    if c is not None:
        return c
    if time.time() - getattr(_tls, "checked_at", 0.0) < 60:
        return None                                   # looked recently: no usable key
    _tls.checked_at = time.time()
    if not (novig_ws and novig_ws.available()):
        return None
    try:
        import novig_client as nc
        kid, key = novig_ws._read_keys()[0]
        c = _tls.client = nc.NovigClient(key_id=kid, key=key, env="prod")
    except Exception as e:
        logger.warning(f"novig: signed client unavailable ({type(e).__name__}); using public routes")
        return None
    return c


def _rate():
    return (SIGNED_REQ_PER_SEC, SIGNED_BURST) if _signed_client() else (REQ_PER_SEC, BURST)


def _take_token() -> None:
    """Block until the token bucket allows one request (signed or public budget)."""
    global _tokens, _tokens_ts
    while True:
        rate, burst = _rate()
        with _lock:
            now = time.time()
            _tokens = min(burst, _tokens + (now - _tokens_ts) * rate)
            _tokens_ts = now
            wait = max(0.0, _blocked_until - now)
            if not wait and _tokens >= 1:
                _tokens -= 1
                return
            if not wait:
                wait = (1 - _tokens) / rate
        time.sleep(min(wait, 5.0))


def _get(path: str, **params):
    """One catalog/book GET under the budget: the signed route on the read key
    when there is one, else the public route. None on a throttle or error."""
    global _blocked_until, _signed_off_until
    _take_token()
    c = _signed_client()
    if c is not None:
        import novig_client as nc
        try:
            return c.request("GET", f"/v3{path}", query=urlencode(params) if params else "")
        except nc.NovigError as e:
            if e.status == 429:
                with _lock:
                    _blocked_until = time.time() + 1.0
                logger.warning(f"novig: signed read throttled on {path}, holding off 1s")
                return None
            if e.status in (401, 403, 451):          # key refused or location check: go public for a while
                _signed_off_until = time.time() + SIGNED_COOLDOWN
                logger.warning(f"novig: signed reads refused ({e.status} {e.code}); public routes for "
                               f"{SIGNED_COOLDOWN // 60} min")
            else:
                logger.warning(f"novig: {path}: {e.status} {e.code}")
                return None
        except Exception as e:
            logger.warning(f"novig: signed {path}: {type(e).__name__}: {e}")
            return None
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
        # Markets the websocket is streaming are current to the second; spend the
        # 2/s REST budget only on the ones it can't hold.
        hot = [m for m in hot if not _streamed(m)]
        cold = [m for m in cold if not _streamed(m)]
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


def _streamed(market_id: str) -> bool:
    return bool(novig_ws and novig_ws.fresh(market_id))


def _watch_stream() -> None:
    """Hand every league's markets to the websocket, most important first: all
    leagues' hot tiers (MONEY + middle rungs of games inside HOT_HOURS), then
    the cold ladders interleaved so no league's alt lines starve the other's.
    The stream holds the top ~2,000 per read key; the REST poller covers the rest."""
    if not (novig_ws and novig_ws.available()):
        return
    with _lock:
        hot = [m for h, _ in _wanted.values() for m in h]
        colds = [list(c) for _, c in _wanted.values()]
    cold = []
    while any(colds):
        for c in colds:
            if c:
                cold.append(c.pop(0))
    novig_ws.watch(hot + cold)


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
    if hit and time.time() - hit[0] < BUILD_SECONDS:
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
    # The catalog (which games and markets exist) changes slowly and costs the
    # shared 2/s budget, so it refreshes every CACHE_SECONDS. Prices do NOT:
    # they are rebuilt from the current books on every call.
    hit = _catalog.get(league)
    if hit and time.time() - hit[0] < CACHE_SECONDS:
        events, markets = hit[1], hit[2]
    else:
        events = {e["eventId"]: e for e in _paged("/catalog/events", league=league, limit=500)}
        markets = _paged("/catalog/markets", league=league,
                         marketType=",".join(TYPES), limit=5000)
        if events and markets:
            _catalog[league] = (time.time(), events, markets)
        elif hit:                                   # throttled: keep the last good catalog
            events, markets = hit[1], hit[2]
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
    _watch_stream()
    if novig_ws:
        # A streamed book is current; a polled one can be minutes old. Every
        # check below (_fill's $10 walk, _monotone, _plausible_main) applies to
        # both alike -- streaming changes freshness, not the standard.
        for ms in by_event.values():
            for m in ms:
                lv = novig_ws.levels(m["marketId"])
                if lv is not None:
                    books[m["marketId"]] = lv

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
            m_live = live
            if novig_ws:
                if novig_ws.market_status(m["marketId"]) in ("CLOSED", "SETTLED"):
                    continue                        # closed since the catalog was read
                streamed_live = novig_ws.market_live(m["marketId"])
                if streamed_live is not None:       # GOLIVE/UNLIVE seen on the stream
                    m_live = streamed_live
            e = _entry(m, away_o, home_o, books, m_live)
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
        # Ages that matter: only markets the REST poller owns (wanted, NOT
        # streamed). The all-books figures above also count stale REST copies of
        # markets the stream took over, which the poller deliberately skips.
        owned = [m for h, c in _wanted.values() for m in (*h, *c) if not _streamed(m)]
        own = sorted(now - _books[m][0] for m in owned if m in _books)
        hot = sum(len(h) for h, _ in _wanted.values())
        cold = sum(len(c) for _, c in _wanted.values())
        return {"hot": hot, "cold": cold, "cached": len(_books),
                "oldest_book_s": round(max(ages), 1) if ages else None,
                "median_book_s": round(sorted(ages)[len(ages) // 2], 1) if ages else None,
                "poller_markets": len(owned), "poller_cached": len(own),
                "poller_median_s": round(own[len(own) // 2], 1) if own else None,
                "poller_oldest_s": round(own[-1], 1) if own else None,
                "throttled_for_s": round(max(0.0, _blocked_until - now), 1),
                "rest_mode": ("signed read key, %g/s" % SIGNED_REQ_PER_SEC) if _signed_client()
                             else ("public, %g/s" % REQ_PER_SEC),
                "running": bool(_refresher and _refresher.is_alive()),
                "stream": novig_ws.status() if novig_ws else {"available": False}}
