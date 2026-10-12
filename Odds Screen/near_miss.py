"""
Alt-rule check: every football alt rung the +EV scan sees that clears
ALT_MIN_EDGE (1.5pp) on EITHER the sharp-price fair (Bookmaker ladder / sharp
curve) or the outcome model's fair, stored in data/bets.db table
alt_candidates. `passed` = it also cleared on the lower of the two (what the
+EV tab shows); passed = 0 is a near miss, with the reason:

  model_lower    sharp/ladder fair cleared, the model's didn't
  sharp_lower    the model's fair cleared, the sharp/ladder fair didn't
  nfl_no_sharp   NFL alt with no real sharp price at/beyond the line (model only)
  model_cant_check / no_sharp_at_line   the model couldn't check it

After kickoff each candidate is judged at its first-seen price against the
CLOSE AT ITS OWN NUMBER, which doesn't depend on our model:
  1. Bookmaker's ladder at that rung (archived within 12h of kickoff), else
  2. the median de-vigged close at that rung across the books that hang it
     (Odds API close capture, or a historical pull when the app wasn't running),
  3. BetOnline's main close carried by the model — stored too, but it's the
     model grading itself, so the summary keeps it apart.
Plus the ESPN result (won/lost/push), the unbiased but noisy check.

The passed rows are the control group: if near misses close as well as passes,
the lower-of-two rule is costing bets.
"""
import json
import sqlite3
import statistics
import threading
from datetime import datetime, timezone, timedelta

import requests

import credits   # every Odds API call counts toward the daily ledger

import bet_grader
import bets as bets_store
import clv
import history_tracker

_lock = threading.Lock()
MARKETS_OK = ("spreads", "spreads_h1", "spreads_q1", "totals", "totals_h1", "totals_q1")
HIST_CREDITS_PER_RUN = 120     # alt-close backfill for games the app missed at kickoff (~20-40 a game)
HIST_MAX_AGE = timedelta(days=4)
# Close at the candidate's own number (10/10, user): Circa's close first
# (odds-api.io, pulled on our own schedule), then Bookmaker only when a pull
# landed within 10 minutes of the start, then the median of the books.
BK_CLOSE_WINDOW = timedelta(minutes=10)
# Ten books bill as one region: 10 credits per market that returns data.
HIST_BOOKS = "draftkings,fanduel,betmgm,williamhill_us,espnbet,fanatics,hardrockbet,betrivers,novig,betonlineag"

FIELDS = ("sport_key", "event_id", "away", "home", "commence_time", "market", "side", "team", "point",
          "book_key", "odds", "sharp_fair", "model_fair", "trusted_fair", "passed", "reason", "backing",
          "anchor_point", "hours_to_kick")


def init():
    with _lock, bets_store._connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alt_candidates (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                key           TEXT NOT NULL UNIQUE,
                first_seen    TEXT NOT NULL,
                last_seen     TEXT NOT NULL,
                n_seen        INTEGER NOT NULL DEFAULT 1,
                sport_key     TEXT NOT NULL,
                event_id      TEXT NOT NULL,
                away          TEXT, home TEXT, commence_time TEXT,
                market        TEXT NOT NULL,
                side          TEXT NOT NULL,           -- away | home | over | under
                team          TEXT,
                point         REAL,
                book_key      TEXT NOT NULL,
                odds          INTEGER NOT NULL,        -- first-seen price (what it's judged at)
                odds_last     INTEGER,
                sharp_fair    REAL,                    -- sharp curve / ladder fair at the line
                model_fair    REAL,                    -- outcome model's fair at the line
                trusted_fair  REAL,                    -- lower of the two (null = none)
                passed        INTEGER NOT NULL,        -- cleared ALT_MIN_EDGE on trusted_fair
                ever_passed   INTEGER NOT NULL DEFAULT 0,
                reason        TEXT,
                backing       TEXT,
                anchor_point  REAL,
                hours_to_kick REAL,
                close_fair_bk    REAL,
                close_fair_mkt   REAL,
                close_mkt_books  INTEGER,
                close_fair_model REAL,
                clv           REAL,                    -- vs bk, else mkt (never the model)
                clv_model     REAL,
                clv_source    TEXT,
                close_done    INTEGER NOT NULL DEFAULT 0,
                hist_fetched  INTEGER NOT NULL DEFAULT 0,
                result        TEXT,                    -- won | lost | push | void
                result_detail TEXT
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_altc_event ON alt_candidates (event_id)")
        if "close_fair_circa" not in {r[1] for r in conn.execute("PRAGMA table_info(alt_candidates)")}:
            conn.execute("ALTER TABLE alt_candidates ADD COLUMN close_fair_circa REAL")


init()


def _side_ok(c):
    return c.get("side") in (("over", "under") if str(c.get("market", "")).startswith("totals") else ("away", "home"))


def record(items) -> dict:
    """Upsert candidates posted by the +EV scan. First sighting fixes the price
    and classification; later ones bump last_seen / odds_last / ever_passed."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    added = updated = 0
    with _lock, bets_store._connect() as conn:
        for it in items or []:
            c = {k: it.get(k) for k in FIELDS}
            if c["market"] not in MARKETS_OK or not _side_ok(c) or not c["event_id"] or c["odds"] in (None, "")                     or c["point"] is None:
                continue
            c["odds"] = int(c["odds"])
            c["passed"] = 1 if c["passed"] else 0
            key = f"{c['event_id']}|{c['market']}|{c['side']}|{c['point']}|{c['book_key']}"
            cur = conn.execute("SELECT id FROM alt_candidates WHERE key=?", (key,)).fetchone()
            if cur:
                conn.execute("""UPDATE alt_candidates SET last_seen=?, n_seen=n_seen+1, odds_last=?,
                                ever_passed=MAX(ever_passed, ?) WHERE id=?""", (now, c["odds"], c["passed"], cur[0]))
                updated += 1
            else:
                row = {**c, "key": key, "first_seen": now, "last_seen": now, "odds_last": c["odds"],
                       "ever_passed": c["passed"]}
                conn.execute(f"INSERT INTO alt_candidates ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
                             list(row.values()))
                added += 1
    return {"added": added, "updated": updated}


# ── Closing value at the candidate's own number ──────────────────────────────

def _names(c):
    if c["market"].startswith("totals"):
        return ("Over", "Under") if c["side"] == "over" else ("Under", "Over")
    return (c["away"], c["home"]) if c["side"] == "away" else (c["home"], c["away"])


def _rung_fairs(rows, c):
    """rows: (book, side, point, price) at one capture. De-vigged fair of the
    candidate's side at its exact number, per book that hangs both sides."""
    mine_n, other_n = _names(c)
    pt = float(c["point"])
    other_pt = pt if c["market"].startswith("totals") else -pt
    by_book = {}
    for bk, side, point, price in rows:
        if point is None or not price or price <= 1:
            continue
        if side == mine_n and abs(point - pt) < 1e-9:
            by_book.setdefault(bk, {})["m"] = price
        elif side == other_n and abs(point - other_pt) < 1e-9:
            by_book.setdefault(bk, {})["o"] = price
    return {bk: clv.power_devig(v["m"], v["o"]) for bk, v in by_book.items() if "m" in v and "o" in v}


def _circa_fair(conn, c):
    """Circa's closing fair at the candidate's own number (official close, else
    the kickoff - 5 min capture), or None when Circa didn't hang that number.
    Circa's games carry odds-api.io ids and team names: found by teams."""
    import circa_close
    g = circa_close._find_game(conn, c)
    if not g:
        return None
    eid, home, away = g
    rename = {away: c["away"], home: c["home"]}
    for src in (circa_close.SOURCE, circa_close.T5_SOURCE):
        rows = conn.execute("SELECT book_key, side, point, price FROM line_snapshots WHERE source=? AND event_id=? AND market=?",
                            (src, eid, c["market"])).fetchall()
        if rows:
            return _rung_fairs([(b, rename.get(s, s), p, pr) for b, s, p, pr in rows], c).get("circa")
    return None


def _latest_capture(conn, c, source, before, after=""):
    r = conn.execute("""SELECT MAX(captured_at) FROM line_snapshots
                        WHERE event_id=? AND market=? AND source=? AND captured_at < ? AND captured_at >= ?""",
                     (c["event_id"], c["market"], source, before, after)).fetchone()
    if not r or not r[0]:
        return []
    return conn.execute("""SELECT book_key, side, point, price FROM line_snapshots
                           WHERE event_id=? AND market=? AND source=? AND captured_at=?""",
                        (c["event_id"], c["market"], source, r[0])).fetchall()


def _fetch_hist_alts(api_key, c, kickoff, markets):
    """Every US book's alt + main lines at kickoff - 10 min for these markets,
    stored like the live close capture (base market, is_alt). Returns credits."""
    req = sorted({m for b in markets for m in (b, "alternate_" + b)})
    r = credits.odds_get(clv.HIST_URL.format(sport=c["sport_key"], event=c["event_id"]), params={
        "apiKey": api_key, "bookmakers": HIST_BOOKS, "markets": ",".join(req), "oddsFormat": "decimal",
        "date": (kickoff - clv.CLOSE_OFFSET).strftime("%Y-%m-%dT%H:%M:%SZ")}, timeout=20)
    spent = int(r.headers.get("x-requests-last") or 0)
    if r.status_code != 200:
        print(f"[near-miss] historical {c['away']} @ {c['home']}: HTTP {r.status_code} {r.text[:150]}")
        return spent
    body = r.json() or {}
    ev = body.get("data") or {}
    snap = body.get("timestamp") or (kickoff - clv.CLOSE_OFFSET).isoformat()
    rows = []
    for bk in ev.get("bookmakers", []):
        for m in bk.get("markets", []):
            key = m.get("key", "")
            is_alt = key.startswith("alternate_")
            base = key[len("alternate_"):] if is_alt else key
            for oc in m.get("outcomes", []):
                if oc.get("price") is not None:
                    rows.append((snap, "near_miss_hist_close", c["sport_key"], c["event_id"],
                                 ev.get("home_team", c["home"]), ev.get("away_team", c["away"]),
                                 ev.get("commence_time"), bk.get("key"), base, int(is_alt),
                                 oc.get("name"), oc.get("point"), oc.get("price")))
    history_tracker._insert_lines(rows)
    return spent


def _parse(ts):
    return clv._parse(ts)


def compute(api_key=None, now=None) -> dict:
    """Close + result for every candidate past kickoff that doesn't have them."""
    now = now or datetime.now(timezone.utc)
    with bets_store._connect() as bconn:
        todo = [dict(r) for r in bconn.execute(
            "SELECT * FROM alt_candidates WHERE close_done=0 OR result IS NULL")]
    todo = [c for c in todo if c.get("commence_time") and _parse(c["commence_time"]) <= now]
    if not todo:
        return {"closed": 0, "graded": 0, "credits": 0}
    hconn = sqlite3.connect(history_tracker.DB_PATH, timeout=30)

    # Games with no close capture at all: one historical pull for every market they need.
    credits = 0
    if api_key:
        need = {}
        for c in todo:
            k = _parse(c["commence_time"])
            if c["close_done"] or c["hist_fetched"] or now - k > HIST_MAX_AGE:
                continue
            if _latest_capture(hconn, c, "odds_api_close", k.isoformat()):
                continue
            need.setdefault(c["event_id"], (c, k, set()))[2].add(c["market"])
        for ev_id, (c, k, mkts) in need.items():
            if credits + 20 * len(mkts) > HIST_CREDITS_PER_RUN:
                break
            try:
                credits += _fetch_hist_alts(api_key, c, k, mkts)
            except requests.exceptions.RequestException as e:
                print(f"[near-miss] historical {c['away']} @ {c['home']}: {type(e).__name__}: {e}")
                continue
            with bets_store._connect() as bconn:
                bconn.execute("UPDATE alt_candidates SET hist_fetched=1 WHERE event_id=?", (ev_id,))
            for t in todo:
                if t["event_id"] == ev_id:
                    t["hist_fetched"] = 1

    closed = graded = 0
    boards = {}
    for c in todo:
        k = _parse(c["commence_time"])
        upd = {}
        if not c["close_done"]:
            before = k.isoformat()
            circa = _circa_fair(hconn, c)
            bk_rows = _latest_capture(hconn, c, "bookmaker", before, (k - BK_CLOSE_WINDOW).isoformat())
            bk = _rung_fairs(bk_rows, c).get("bookmaker")
            mkt_rows = _latest_capture(hconn, c, "odds_api_close", before) or \
                _latest_capture(hconn, c, "near_miss_hist_close", now.isoformat())
            fairs = [f for b, f in _rung_fairs(mkt_rows, c).items() if b != "bookmaker"]
            mkt = statistics.median(fairs) if fairs else None
            # BetOnline main close carried by the model (same path as the bet tracker's CLV)
            model = None
            pair = clv._from_close_capture(hconn, c) or clv._from_hourly(hconn, c, k)
            if pair:
                base = clv.power_devig(pair["price"], pair["other_price"])
                same = pair["point"] is None or abs(float(pair["point"]) - float(c["point"])) < 1e-9
                model = base if same else clv.cross_line_fair(hconn, c, pair, base)
            dec = clv._decimal(c["odds"])
            ref, src = ((circa, "circa") if circa is not None else (bk, "bookmaker") if bk is not None
                        else (mkt, f"alt_close_median_{len(fairs)}") if mkt is not None else (None, None))
            upd.update(close_fair_circa=circa, close_fair_bk=bk, close_fair_mkt=mkt, close_mkt_books=len(fairs) or None,
                       close_fair_model=model, clv=None if ref is None else round(ref * dec - 1, 5),
                       clv_model=None if model is None else round(model * dec - 1, 5), clv_source=src)
            # Done once a market close is in, or nothing more can arrive.
            if ref is not None or (c["hist_fetched"] or now - k > HIST_MAX_AGE or not api_key):
                upd["close_done"] = 1
                closed += 1
        if c.get("result") is None and now - k >= bet_grader.GRADE_AFTER:
            try:
                et_day = k.astimezone(bet_grader.ET).date()
                games = []
                for d in (et_day, et_day - timedelta(days=1), et_day + timedelta(days=1)):
                    key = (c["sport_key"], d.strftime("%Y%m%d"))
                    if key not in boards:
                        boards[key] = bet_grader._scoreboard(*key)
                    games += boards[key]
                game, oriented = bet_grader.find_game(c, games)
                if game:
                    status = game["status"] or ""
                    if status in ("STATUS_CANCELED", "STATUS_ABANDONED", "STATUS_FORFEIT"):
                        upd.update(result="void", result_detail=status)
                    elif game["completed"] and status.startswith("STATUS_FINAL"):
                        g, detail = bet_grader.grade_one(c, game, oriented)
                        if g:
                            upd.update(result=g, result_detail=detail)
                            graded += 1
            except requests.exceptions.RequestException as e:
                print(f"[near-miss] ESPN: {type(e).__name__}: {e}")
        if upd:
            with _lock, bets_store._connect() as bconn:
                bconn.execute(f"UPDATE alt_candidates SET {', '.join(f'{x}=?' for x in upd)} WHERE id=?",
                              [*upd.values(), c["id"]])
    hconn.close()
    return {"closed": closed, "graded": graded, "credits": credits}


# ── Report ───────────────────────────────────────────────────────────────────

def _profit(c):
    r = c.get("result")
    if r == "won":
        return clv._decimal(c["odds"]) - 1
    return -1.0 if r == "lost" else 0.0 if r in ("push", "void") else None


def summary(sport_key=None) -> dict:
    with bets_store._connect() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM alt_candidates ORDER BY first_seen DESC")]
    if sport_key:
        rows = [r for r in rows if r["sport_key"] == sport_key]
    groups = {}
    for r in rows:
        g = "passed" if r["passed"] else (r["reason"] or "near_miss")
        groups.setdefault((r["sport_key"], g), []).append(r)

    def stat(lst):
        clvs = [r["clv"] for r in lst if r["clv"] is not None]
        mclv = [r["clv_model"] for r in lst if r["clv_model"] is not None]
        res = [p for p in (_profit(r) for r in lst) if p is not None]
        dec_ev = lambda f, r: None if f is None else f * clv._decimal(r["odds"]) - 1
        claimed = [dec_ev(max(x for x in (r["sharp_fair"], r["model_fair"]) if x is not None), r) for r in lst
                   if r["sharp_fair"] is not None or r["model_fair"] is not None]
        return {"n": len(lst), "claimed_ev": round(statistics.mean(claimed), 4) if claimed else None,
                "clv_n": len(clvs), "clv_avg": round(statistics.mean(clvs), 4) if clvs else None,
                "clv_beat": sum(1 for x in clvs if x > 0),
                "model_clv_n": len(mclv), "model_clv_avg": round(statistics.mean(mclv), 4) if mclv else None,
                "graded": len(res), "roi": round(sum(res) / len(res), 4) if res else None}

    out = [{"sport_key": sk, "group": g, **stat(lst)} for (sk, g), lst in sorted(groups.items())]
    return {"groups": out, "total": len(rows), "recent": rows[:200]}
