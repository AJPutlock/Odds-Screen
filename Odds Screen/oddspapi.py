"""
OddsPapi (oddspapi.io) comparison feed, 10/13 -- free-plan trial against the
current board (The Odds API + odds-api.io + the exchange feeds). Served by
oddspapi_dash.py (port 5066), never by app.py.

Free plan: 250 billable requests a month, every book (Pinnacle, Circa,
Bookmaker.eu, BetOnline, 4casters, sharp.ag, the US retail books, exchanges).
GET /v4/account reports request_limit / request_count and is free.

Measured on the first pulls (10/13), not all in the docs:
  * /v4/odds-by-tournaments takes ONE bookmaker (`bookmaker=`), not 3, and
    returns every fixture with participant names, mainLine flags, alt ladders
    and Pinnacle's limits. Billable.
  * /v4/historical-odds is FREE (never counts), up to 3 bookmakers per call,
    5 s cooldown, one fixture per call. Its last entry per outcome is the
    current price (Pinnacle's matched the live pull to the ms); a pulled line
    ends on an entry with active=false.
So prices cost nothing: one billable call per sport fetches the fixture list
(cached FIXTURE_HOURS; Pinnacle as the book, so it carries its main lines too),
then a free history call per fixture per 3 books builds the snapshot.

Market ids come from /v4/markets (cached): marketType moneyline / spreads /
totals, period "result" = full game incl. OT ("fulltime" is regulation, used by
hockey's 3-way lines -- skipped), "p1+p2" 1st half, "p1" Q1 / P1 / 1st inning,
"p1+..+p5" first five. participant1 is HOME; a spread market's handicap is the
home team's number (outcome "1" = home at +handicap, "2" = away at -handicap).

Snapshot shape: {fixture_id: {away_team, home_team, commence_time, books:
{slug: {market_key: [{away_odds, home_odds, away_point, home_point, away_dec,
home_dec, changed_at, limit}, ...]}}}} -- one row per active line, American
odds strings, totals with the Over in the home slot as on the main board.
"""
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
except ImportError:          # pragma: no cover
    load_dotenv = None

BASE = "https://api.oddspapi.io/v4"
DIR = Path(__file__).parent / "data" / "oddspapi"
REF_DAYS = 7
MAX_BOOKS = 3                # odds-by-tournaments accepts at most 3 bookmakers per call
COOLDOWN = 1.1               # seconds; the per-endpoint cooldowns are 0.5-2 s

_lock = threading.Lock()
_last_call = {"at": 0.0}


class FeedError(Exception):
    """An OddsPapi failure, described without the request URL (it carries the key)."""
    def __init__(self, status, path, text=""):
        super().__init__(f"OddsPapi HTTP {status} on {path}{': ' + text[:200] if text else ''}")
        self.status = status


def _key():
    if load_dotenv:
        load_dotenv(Path(__file__).parent / ".env", override=True)
    return os.environ.get("ODDSPAPI_KEY", "").strip()


def enabled() -> bool:
    return bool(_key())


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read(name, default=None):
    p = DIR / name
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def _write(name, obj):
    DIR.mkdir(parents=True, exist_ok=True)
    tmp = DIR / (name + ".tmp")
    tmp.write_text(json.dumps(obj), encoding="utf-8")
    tmp.replace(DIR / name)


_ledger_lock = threading.Lock()


def _log_request(path, billable, tag=None):
    with _ledger_lock:
        led = _read("ledger.json", {}) or {}
        day = datetime.now().strftime("%Y-%m-%d")
        d = led.setdefault(day, {"billable": 0, "free": 0, "calls": []})
        d["billable" if billable else "free"] += 1
        if billable:                     # free history calls are only counted
            d["calls"].append({"at": _now_iso(), "path": path, **({"by": tag} if tag else {})})
        _write("ledger.json", led)


def billable_this_month(tag=None):
    """Billable calls logged this calendar month (optionally only one caller's)."""
    month = datetime.now().strftime("%Y-%m")
    return sum(1 for day, d in ledger().items() if day.startswith(month)
               for c in d.get("calls") or [] if tag is None or c.get("by") == tag)


def _get(path, billable=True, tag=None, **params):
    """One API call, spaced COOLDOWN apart. 429/5xx are not retried: on a
    quota this small a blind retry costs a request (failed calls count too)."""
    key = _key()
    if not key:
        raise FeedError("no-key", path, "ODDSPAPI_KEY is not set in Odds Screen/.env")
    with _lock:
        wait = COOLDOWN - (time.time() - _last_call["at"])
        if wait > 0:
            time.sleep(wait)
        _last_call["at"] = time.time()
        try:
            r = requests.get(f"{BASE}{path}", params={"apiKey": key, **params}, timeout=30)
        except requests.exceptions.RequestException as e:
            raise FeedError("network", path, type(e).__name__)
        _log_request(path, billable, tag)
    if not r.ok:
        raise FeedError(r.status_code, path, r.text)
    return r.json()


# ── Account (free) ───────────────────────────────────────────────────────────

def account():
    """The active subscription: request_limit / request_count, the bookmakers
    and sports the plan covers. Free -- never counts against the quota."""
    acc = _get("/account", billable=False)
    subs = acc.get("subscriptions") or []
    cur = next((s for s in subs if s.get("subscription_id") == acc.get("current_subscription_id")), None)
    cur = cur or next((s for s in subs if s.get("is_active")), None) or (subs[0] if subs else {})
    out = {
        "request_limit": cur.get("request_limit"),
        "request_count": cur.get("request_count"),
        "rate_limit":    cur.get("rate_limit"),
        "valid_from":    cur.get("valid_from"),
        "valid_until":   cur.get("valid_until"),
        "last_request":  cur.get("last_request"),
        "bookmakers":    sorted((cur.get("bookmakers") or {}).keys()),
        "sport_ids":     cur.get("sport_ids") or [],
        "checked_at":    _now_iso(),
    }
    _write("account.json", out)
    return out


def account_cached():
    return _read("account.json")


def ledger():
    return _read("ledger.json", {}) or {}


# ── Reference data (billable, cached REF_DAYS) ───────────────────────────────

def _ref(name, path, refresh=False, **params):
    cached = _read(name)
    if cached and not refresh and time.time() - cached.get("at", 0) < REF_DAYS * 86400:
        return cached["data"]
    data = _get(path, **params)
    _write(name, {"at": time.time(), "data": data})
    return data


def ref_cached(name):
    c = _read(name)
    return c["data"] if c else None


def sports(refresh=False):
    return _ref("sports.json", "/sports", refresh)


def bookmakers(refresh=False):
    return _ref("bookmakers.json", "/bookmakers", refresh)


def markets(refresh=False):
    return _ref("markets.json", "/markets", refresh)


def tournaments(sport_id, refresh=False):
    return _ref(f"tournaments_{sport_id}.json", "/tournaments", refresh, sportId=sport_id)


def participants(sport_id, refresh=False):
    return _ref(f"participants_{sport_id}.json", "/participants", refresh, sportId=sport_id)


# ── Sports, books, market map ────────────────────────────────────────────────

# app sport key -> (OddsPapi sportId, tournament slugs to try, family, days ahead)
SPORTS = {
    "americanfootball_nfl":   (14, ["nfl"], "football", 8),
    "americanfootball_ncaaf": (14, ["ncaa-regular-season"], "football", 8),
    "basketball_nba":         (11, ["nba"], "basketball", 2),
    "basketball_nba_preseason": (11, ["nba-preseason", "nba-pre-season"], "basketball", 2),
    "basketball_ncaab":       (11, ["ncaa-regular-season", "ncaa-division-i"], "basketball", 2),
    "basketball_wnba":        (11, ["wnba"], "basketball", 2),
    "icehockey_nhl":          (15, ["nhl"], "hockey", 2),
    "baseball_mlb":           (13, ["mlb"], "baseball", 2),
}
KNOWN_TOURNAMENTS = {"americanfootball_nfl": 31, "americanfootball_ncaaf": 27653}

_PERIODS = {
    "football":   {"result": "", "p1+p2": "_h1", "p1": "_q1"},
    "basketball": {"result": "", "p1+p2": "_h1", "p1": "_q1"},
    "hockey":     {"result": "", "p1": "_p1"},
    "baseball":   {"result": "", "p1+p2+p3+p4+p5": "_1st_5_innings", "p1": "_1st_1_innings"},
}
_TYPES = {"moneyline": "h2h", "spreads": "spreads", "totals": "totals"}

# OddsPapi slug -> main-board book key (None: OddsPapi-only, e.g. Pinnacle)
BOARD_BOOK = {
    "pinnacle": None, "circasports": "circa", "bookmaker.eu": "bookmaker",
    "betonline.ag": "betonlineag", "draftkings": "draftkings", "fanduel": "fanduel",
    "caesars": "williamhill_us", "betmgm": "betmgm", "betrivers": "betrivers",
    "espnbet": "espnbet", "fanatics": "fanatics", "hardrockbet": "hardrockbet",
    "bet365-nj": "bet365", "novig.us": "novig", "kalshi": "kalshi", "polymarket.us": "polymarket",
}
# Offered on the dashboard: the board's books plus sharp / offshore extras.
BOOK_CHOICES = ["pinnacle", "circasports", "bookmaker.eu", "betonline.ag", "4casters", "sharp.ag",
                "ps3838", "betcris", "lowvig.ag", "draftkings", "fanduel", "caesars", "betmgm",
                "betrivers", "espnbet", "fanatics", "hardrockbet", "bet365-nj", "novig.us",
                "prophetx", "sx.bet", "kalshi", "polymarket.us"]
DEFAULT_BOOKS = ["pinnacle", "circasports", "bookmaker.eu", "betonline.ag", "draftkings", "fanduel"]
SHARP_REF = "pinnacle"
FIXTURE_HOURS = 6
HISTORY_COOLDOWN = 5.3

_market_map = {"at": 0, "map": None}


def market_map():
    """marketId (str) -> (board market key, handicap, {outcomeId(str): '1'|'2'|'over'|'under'})"""
    if _market_map["map"] is None:
        rows = ref_cached("markets.json") or markets()
        out = {}
        for m in rows:
            base = _TYPES.get(m.get("marketType"))
            if not base or m.get("playerProp") or m.get("marketLength") != 2:
                continue
            fam = {14: "football", 11: "basketball", 15: "hockey", 13: "baseball"}.get(m.get("sportId"))
            suffix = _PERIODS.get(fam, {}).get(m.get("period"))
            if suffix is None:
                continue
            out[str(m["marketId"])] = (fam, base + suffix, float(m.get("handicap") or 0),
                                       {str(o["outcomeId"]): o["outcomeName"].lower() for o in m["outcomes"]})
        _market_map["map"] = out
    return _market_map["map"]


def american(dec):
    if not dec or dec <= 1:
        return None
    return f"+{round((dec - 1) * 100)}" if dec >= 2 else f"{round(-100 / (dec - 1))}"


def dec_from_american(s):
    try:
        v = float(str(s).replace("+", ""))
    except (TypeError, ValueError):
        return None
    if v == 0:
        return None
    return 1 + v / 100 if v > 0 else 1 + 100 / -v


def tournament_id(sport_key):
    if sport_key in KNOWN_TOURNAMENTS:
        return KNOWN_TOURNAMENTS[sport_key]
    sid, slugs, _, _ = SPORTS[sport_key]
    ts = tournaments(sid)          # billable once a week per sport
    for usa_only in (True, False):          # the NHL may file under another category
        for slug in slugs:
            for t in ts:
                if t.get("tournamentSlug") == slug and (t.get("categorySlug") == "usa" or not usa_only):
                    return t["tournamentId"]
    raise FeedError("unmapped", "/tournaments", f"no USA tournament {slugs} for {sport_key}")


# ── Fixture list (1 billable call per sport per FIXTURE_HOURS) ───────────────

def _fixture(f):
    return {"fixture_id": f["fixtureId"], "home_team": f.get("participant1Name"),
            "away_team": f.get("participant2Name"), "commence_time": f.get("startTime"),
            "status": f.get("statusId")}


def fixtures(sport_key, refresh=False, max_age_hours=FIXTURE_HOURS, tag=None):
    """Upcoming fixtures for a sport. The list comes from odds-by-tournaments
    with Pinnacle as the book (the call takes one), cached max_age_hours.
    Every list also lands in the fixture registry (pinnacle_close.py reads it
    after kickoff, when the game has left this list)."""
    name = f"fixtures_{sport_key}.json"
    c = _read(name)
    if c and not refresh and time.time() - c.get("at", 0) < max_age_hours * 3600:
        return c["fixtures"], c
    raw = _get("/odds-by-tournaments", tag=tag, tournamentIds=str(tournament_id(sport_key)),
               bookmaker=SHARP_REF, oddsFormat="american", verbosity=3)
    fx = [_fixture(f) for f in (raw or [])]
    c = {"at": time.time(), "fetched_at": _now_iso(), "fixtures": fx}
    _write(name, c)
    register(sport_key, fx)
    return fx, c


def fixtures_between(sport_key, start, end, tag=None):
    """Fixtures (any status) kicking off between start and end (datetimes, < 10
    days apart) -- /v4/fixtures, 1 billable call. For backfilling closes of
    games that started before the registry knew them."""
    raw = _get("/fixtures", tag=tag, tournamentId=tournament_id(sport_key),
               **{"from": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "to": end.strftime("%Y-%m-%dT%H:%M:%SZ")})
    fx = [_fixture(f) for f in (raw or [])]
    register(sport_key, fx)
    return fx


# Every fixture any list has shown, kept REGISTRY_DAYS past kickoff.
REGISTRY_DAYS = 14
_reg_lock = threading.Lock()


def register(sport_key, fx):
    with _reg_lock:
        reg = _read("fixture_registry.json", {}) or {}
        cutoff = time.time() - REGISTRY_DAYS * 86400
        for f in fx:
            if f.get("fixture_id") and f.get("commence_time"):
                reg[f["fixture_id"]] = {**f, "sport_key": sport_key}
        reg = {k: v for k, v in reg.items() if (_ts(v.get("commence_time")) or 0) > cutoff}
        _write("fixture_registry.json", reg)


def registry():
    return _read("fixture_registry.json", {}) or {}


def fixtures_cached(sport_key):
    c = _read(f"fixtures_{sport_key}.json")
    return c


# ── History (free) -> current prices ─────────────────────────────────────────

_hist_lock = threading.Lock()
_hist_last = {"at": 0.0}


def history(fixture_id, books):
    with _hist_lock:
        wait = HISTORY_COOLDOWN - (time.time() - _hist_last["at"])
        if wait > 0:
            time.sleep(wait)
        try:
            return _get("/historical-odds", billable=False, fixtureId=fixture_id, bookmakers=",".join(books))
        finally:
            _hist_last["at"] = time.time()


def parse_history(hist, pick=None):
    """{slug: {market_key: [rows]}} from a historical-odds response: the last
    entry of each outcome. A row is active only when both sides' last entries
    are; inactive rows are kept so a pulled game reads as OFF, not missing.
    pick(entries) -> entry|None replaces "the last entry" (pinnacle_close
    picks the last one at or before kickoff)."""
    mm = market_map()
    out = {}
    for slug, bv in ((hist or {}).get("bookmakers") or {}).items():
        books = out.setdefault(slug, {})
        for mid, mv in (bv.get("markets") or {}).items():
            info = mm.get(str(mid))
            if not info:
                continue
            _, mkey, hcap, names = info
            last = {}
            for oid, ov in (mv.get("outcomes") or {}).items():
                arr = next(iter((ov.get("players") or {}).values()), None)
                e = (pick(arr) if pick else arr[-1]) if arr else None
                if e:
                    last[names.get(str(oid))] = e
            if mkey.startswith("totals"):
                a, h = last.get("under"), last.get("over")
                ap = hp = hcap
            else:
                a, h = last.get("2"), last.get("1")
                hp, ap = (hcap, -hcap) if mkey.startswith("spreads") else (None, None)
            if not a or not h:
                continue
            ad, hd = a.get("price"), h.get("price")
            if not ad or not hd or ad <= 1 or hd <= 1:
                continue
            books.setdefault(mkey, []).append({
                "away_odds": american(ad), "home_odds": american(hd),
                "away_point": ap, "home_point": hp, "away_dec": ad, "home_dec": hd,
                "changed_at": max(a.get("createdAt") or "", h.get("createdAt") or ""),
                "limit": max(a.get("limit") or 0, h.get("limit") or 0) or None,
                "active": bool(a.get("active") and h.get("active")),
            })
    return out


def live(rows):
    return [r for r in rows or [] if r.get("active", True)]


def off_at(rows):
    """When a book pulled a market: the latest change among its rows, if none is active."""
    if rows and not live(rows):
        return max(r["changed_at"] for r in rows)
    return None


def main_row(rows):
    """The main line of a ladder: the pair priced closest to even (smallest
    gap between the two sides' implied probabilities)."""
    rows = live(rows)
    if not rows:
        return None
    return min(rows, key=lambda r: abs(1 / r["away_dec"] - 1 / r["home_dec"]))


def row_at(rows, mkey, point):
    """The ladder row at the board's number (away point for spreads, the total
    for totals); moneylines have one row."""
    rows = live(rows)
    if not rows:
        return None
    if mkey.startswith("h2h"):
        return rows[0]
    for r in rows:
        if r["away_point"] is not None and point is not None and abs(r["away_point"] - point) < 1e-6:
            return r
    return None


# ── Snapshot job (free history calls, background) ────────────────────────────

_jobs = {}            # sport_key -> {state, done, total, started, error, books}


def job(sport_key):
    return dict(_jobs.get(sport_key) or {})


def snapshot(sport_key):
    return _read(f"snap_{sport_key}.json")


def refresh(sport_key, books, refresh_fixtures=False):
    """Start a background snapshot: fixtures (cached, else 1 billable call),
    then one FREE history call per fixture per 3 books. Returns at once."""
    if sport_key not in SPORTS:
        raise ValueError(f"unknown sport {sport_key}")
    if (_jobs.get(sport_key) or {}).get("state") == "running":
        return job(sport_key)
    books = [b for b in dict.fromkeys(books) if b] or list(DEFAULT_BOOKS)
    _jobs[sport_key] = {"state": "running", "done": 0, "total": 0, "started": _now_iso(),
                        "error": None, "books": books}
    threading.Thread(target=_run, args=(sport_key, books, refresh_fixtures), daemon=True,
                     name=f"oddspapi-{sport_key}").start()
    return job(sport_key)


def _run(sport_key, books, refresh_fixtures):
    j = _jobs[sport_key]
    try:
        fx, meta = fixtures(sport_key, refresh=refresh_fixtures)
        days = SPORTS[sport_key][3]
        now = datetime.now(timezone.utc)
        keep = []
        for f in fx:
            try:
                st = datetime.fromisoformat(f["commence_time"].replace("Z", "+00:00"))
            except (AttributeError, ValueError):
                continue
            if f.get("status") == 0 and now <= st and (st - now).total_seconds() < days * 86400:
                keep.append(f)
        chunks = [books[i:i + 3] for i in range(0, len(books), 3)]
        j["total"] = len(keep) * len(chunks)
        snap = {"sport_key": sport_key, "started": j["started"], "books": books,
                "fixtures_fetched_at": meta.get("fetched_at"), "games": {}, "errors": []}
        for f in keep:
            g = dict(f, books={})
            for ch in chunks:
                try:
                    g["books"].update(parse_history(history(f["fixture_id"], ch)))
                except FeedError as e:
                    snap["errors"].append(f"{f['away_team']} @ {f['home_team']} {ch}: {e}")
                    if e.status == 429:
                        raise
                j["done"] += 1
            snap["games"][f["fixture_id"]] = g
        snap["finished"] = _now_iso()
        _write(f"snap_{sport_key}.json", snap)
        j["state"] = "done"
    except Exception as e:                  # surfaced on the dashboard
        j.update(state="error", error=str(e))
    j["finished"] = _now_iso()


# ── Comparison against the main board ────────────────────────────────────────

import re
import unicodedata

_STOP = {"state", "st", "university", "college", "the"}


# OddsPapi short forms -> the board's (Odds API) long names
_ALIASES = {"fiu": "florida international", "utsa": "texas san antonio", "utep": "texas el paso",
            "uab": "alabama birmingham", "ucf": "central florida", "smu": "southern methodist",
            "tcu": "texas christian", "byu": "brigham young", "lsu": "louisiana state",
            "unlv": "nevada las vegas", "ul monroe": "louisiana monroe", "southern miss": "southern mississippi"}


def _norm(name):
    name = unicodedata.normalize("NFKD", (name or "").lower()).encode("ascii", "ignore").decode()
    words = name.split()
    for short, full in _ALIASES.items():
        sw = short.split()
        for i in range(len(words) - len(sw) + 1):
            if words[i:i + len(sw)] == sw:
                words[i:i + len(sw)] = full.split()
                break
    name = " ".join(words)
    name = name.replace("&", " and ").replace("(oh)", "ohio").replace("(fl)", "florida")
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    return set(name.split())


def _overlap(a, b):
    ta, tb = _norm(a) - _STOP, _norm(b) - _STOP
    if not ta or not tb:
        ta, tb = _norm(a), _norm(b)
    return len(ta & tb) / max(len(ta | tb), 1)


def _ts(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except (AttributeError, ValueError):
        return None


def match_games(board_games, op_games):
    """board game_id -> (op fixture, swapped). Both teams must overlap >= 0.3
    and kickoffs sit within 6 h; the orientation scoring higher wins."""
    out = {}
    for bg in board_games:
        bt = _ts(bg["commence_time"])
        best = None
        for og in op_games:
            ot = _ts(og.get("commence_time"))
            if bt and ot and abs(bt - ot) > 6 * 3600:
                continue
            s = (_overlap(bg["away_team"], og["away_team"]), _overlap(bg["home_team"], og["home_team"]))
            w = (_overlap(bg["away_team"], og["home_team"]), _overlap(bg["home_team"], og["away_team"]))
            swapped = sum(w) > sum(s)
            a, h = w if swapped else s
            if a < 0.3 or h < 0.3:
                continue
            if not best or a + h > best[0]:
                best = (a + h, og, swapped)
        if best:
            out[bg["game_id"]] = (best[1], best[2])
    return out


def _swap(r):
    return dict(r, away_odds=r["home_odds"], home_odds=r["away_odds"], away_point=r["home_point"],
                home_point=r["away_point"], away_dec=r["home_dec"], home_dec=r["away_dec"])


def _fair(row):
    """Multiplicative no-vig probabilities (away, home)."""
    ia, ih = 1 / row["away_dec"], 1 / row["home_dec"]
    return ia / (ia + ih), ih / (ia + ih)


def compare(sport_key, board, board_times=None):
    """Rows for the dashboard: every board market of a matched game with
    OddsPapi's main line per book, the same book's board quote at the same
    number, and the board's best price scored against Pinnacle's no-vig line
    at that number.

    board_times: board book key -> when the board last pulled that source.
    When the two feeds disagree (>= 0.35 pp), the newer side is the one whose
    time is later: OddsPapi's last change after the board's pull means the
    board is stale; the board pulling after OddsPapi's last change means
    OddsPapi missed a move."""
    board_times = board_times or {}
    snap = snapshot(sport_key) or {}
    op_games = list((snap.get("games") or {}).values())
    rows_in = board.get("data") or []
    board_games = {}
    for r in rows_in:
        board_games.setdefault(r["game_id"], r)
    matches = match_games(list(board_games.values()), op_games)
    books = snap.get("books") or []
    out, stats = [], {b: {"quotes": 0, "same_line": 0, "same_price": 0, "abs_pp": [], "op_only": 0,
                          "board_only": 0, "off": 0, "newer_oddspapi": 0, "newer_board": 0}
                      for b in books}
    for r in rows_in:
        m = matches.get(r["game_id"])
        if not m:
            continue
        og, swapped = m
        mkey = r["market"]
        row = {"game_id": r["game_id"], "away_team": r["away_team"], "home_team": r["home_team"],
               "commence_time": r["commence_time"], "market": mkey, "market_label": r.get("market_label"),
               "fixture_id": og["fixture_id"], "swapped": swapped, "books": {}}
        for slug in books:
            ladder = ((og.get("books") or {}).get(slug) or {}).get(mkey) or []
            if swapped and not mkey.startswith("totals"):      # Over/Under don't follow the teams
                ladder = [_swap(x) for x in ladder]
            main = main_row(ladder)
            bk = BOARD_BOOK.get(slug)
            bq = (r.get("books") or {}).get(bk) if bk else None
            cell = {"op": main, "board": bq, "board_key": bk, "off_at": off_at(ladder)}
            if bq and bq.get("away_odds") and bq.get("home_odds"):
                at = row_at(ladder, mkey, bq.get("away_point") if not mkey.startswith("totals") else bq.get("home_point"))
                cell["op_at_board"] = at
                st = stats[slug]
                if main:
                    st["quotes"] += 1
                    same_line = mkey.startswith("h2h") or (main["away_point"] == bq.get("away_point"))
                    st["same_line"] += same_line
                if at:
                    da, dh = dec_from_american(bq["away_odds"]), dec_from_american(bq["home_odds"])
                    if da and dh:
                        pp = max(abs(1 / da - 1 / at["away_dec"]), abs(1 / dh - 1 / at["home_dec"])) * 100
                        cell["diff_pp"] = round(pp, 2)
                        st["abs_pp"].append(pp)
                        st["same_price"] += pp < 0.35
                        bt = _ts(board_times.get(bk))
                        if pp >= 0.35 and bt:
                            newer = "oddspapi" if (_ts(at["changed_at"]) or 0) > bt else "board"
                            cell["newer"] = newer
                            st["newer_" + newer] += 1
                if not main:
                    st["board_only"] += 1
                    st["off"] += bool(cell["off_at"])
            elif main:
                stats[slug]["op_only"] += 1
            row["books"][slug] = cell
        # board best vs the sharp reference at the same number
        ref = ((og.get("books") or {}).get(SHARP_REF) or {}).get(mkey) or []
        if swapped and not mkey.startswith("totals"):
            ref = [_swap(x) for x in ref]
        for side in ("away", "home"):
            best = r.get(f"best_{side}") or {}
            dec = dec_from_american(best.get("odds"))
            pt = best.get("point")
            if mkey.startswith("spreads") and side == "home" and pt is not None:
                pt = -pt                                   # ladder is keyed by the away number
            refrow = row_at(ref, mkey, pt)
            ev = None
            if refrow and dec:
                fa, fh = _fair(refrow)
                ev = round(((fa if side == "away" else fh) * dec - 1) * 100, 2)
            row[f"best_{side}"] = {**best, "ev_vs_ref": ev}
        out.append(row)
    summary = {}
    for b, st in stats.items():
        n = len(st["abs_pp"])
        srt = sorted(st["abs_pp"])
        summary[b] = {"board_key": BOARD_BOOK.get(b), "quotes": st["quotes"], "same_line": st["same_line"],
                      "priced_same_number": n, "same_price": st["same_price"], "op_only": st["op_only"],
                      "board_only": st["board_only"], "off": st["off"],
                      "newer_oddspapi": st["newer_oddspapi"], "newer_board": st["newer_board"],
                      "board_time": board_times.get(BOARD_BOOK.get(b)),
                      "median_pp": round(srt[n // 2], 2) if n else None,
                      "p90_pp": round(srt[int(n * .9)], 2) if n else None}
    return {"sport_key": sport_key, "rows": out, "summary": summary, "books": books,
            "snapshot_started": snap.get("started"), "snapshot_finished": snap.get("finished"),
            "fixtures_fetched_at": snap.get("fixtures_fetched_at"), "errors": snap.get("errors") or [],
            "board_last_updated": board.get("last_updated"),
            "matched_games": len(matches), "board_games": len(board_games), "op_games": len(op_games),
            "unmatched_op": [f"{g['away_team']} @ {g['home_team']}" for g in op_games
                             if g["fixture_id"] not in {m[0]["fixture_id"] for m in matches.values()}]}


if __name__ == "__main__":       # python oddspapi.py  -> account (free)
    print(json.dumps({k: v for k, v in account().items() if k != "bookmakers"}, indent=1))
