"""
Pinnacle closing lines from OddsPapi (free /v4/historical-odds) and a
secondary CLV against them (10/13). Never replaces the BetOnline / Circa CLV.

start_if_due()  From the grader loop (every 15 min): a background pass that
        stores Pinnacle's close for each registered game 10 min to RECENT_DAYS
        after kickoff, one game at a time (5 s cooldown, oddspapi.history).
        The close is the last price at or before commence_time, so a late call
        still gets the true close, not a live price (close_entry). Main lines
        (is_alt=0, the pair priced closest to even) plus Pinnacle's ladder
        (is_alt=1), full game and 1H / Q1 / P1, into odds_history.db
        line_snapshots as source 'pinnacle_close', book 'pinnacle', event_id
        'op:<fixture id>', prices decimal, side = team name (Over / Under for
        totals) -- circa_close's shape. Team names are OddsPapi's (circa_close
        stores odds-api.io's); bets find their game by fuzzy team match.
compute()  Each bet past kickoff gets pinn_close_* and clv_pinn, judged at the
        bet's own number: Pinnacle's MAIN close at that number -> power de-vig,
        clv_pinn_source 'main'. Only an alt rung at that number -> 'alt', no
        CLV (Pinnacle alt no-vig is noise: ~50% of alt rows sat > 1.5 pp off
        Circa+BOL on 10/13). Neither -> 'off_number', no CLV.

Games are found through oddspapi's fixture registry: every fixture list
(odds-by-tournaments, Pinnacle) lands there. Billable budget: the list is
re-pulled at most once per LIST_HOURS per sport, and never once the account's
monthly request_count reaches REQUEST_GUARD (the job then logs and skips; the
free history calls carry on for registered games). Calls are tagged 'close'
in data/oddspapi/ledger.json.

Switch: PINNACLE_CLOSE_ENABLED in .env (default on; 0 turns it off).
"""
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bet_grader
import bets as bets_store
import clv
import history_tracker
import oddspapi as op

SOURCE = "pinnacle_close"
BOOK = "pinnacle"
TAG = "close"
REQUEST_GUARD = 150            # of the free plan's 250 / month
# Sports, how often their fixture list is re-pulled, and from when
LIST_HOURS = {"americanfootball_nfl": 48, "americanfootball_ncaaf": 24, "icehockey_nhl": 24,
              "basketball_nba": 24}
ACTIVE_FROM = {"basketball_nba": datetime(2026, 10, 20, tzinfo=timezone.utc)}
AFTER_KICKOFF = timedelta(minutes=10)
RECENT_DAYS = 3
SUSPEND_GRACE = 15 * 60        # an inactive entry this close to kickoff is the kickoff suspension
MAX_TRIES = 6
RETRY_AFTER = 20 * 60
RUN_EVERY = 10 * 60
STATE = "close_state.json"     # in data/oddspapi/: fixture id -> {tries, last, status}

_lock = threading.Lock()
_status = {"running": False, "last_run": 0.0, "last": None}


def enabled() -> bool:
    op._key()                                  # loads .env
    return os.environ.get("PINNACLE_CLOSE_ENABLED", "1").strip().lower() not in ("0", "false", "off", "no") \
        and op.enabled()


# ── Close selection ──────────────────────────────────────────────────────────

def close_entry(entries, commence_ts, grace=SUSPEND_GRACE):
    """The closing entry of one outcome: the last entry at or before kickoff.
    When that entry is inactive and posted within `grace` seconds of kickoff
    (Pinnacle suspending for the start), the close is the last active entry
    before it; an inactive entry earlier than that means the line was pulled
    (moved off the number) -> None."""
    prior = sorted((e for e in entries or [] if (op._ts(e.get("createdAt")) or 0) <= commence_ts),
                   key=lambda e: e.get("createdAt") or "")
    if not prior:
        return None
    last = prior[-1]
    if last.get("active"):
        return last
    if commence_ts - (op._ts(last.get("createdAt")) or 0) <= grace:
        active = [e for e in prior if e.get("active")]
        return active[-1] if active else None
    return None


def close_rows(sport_key, fixture, hist):
    """line_snapshots rows for one game's Pinnacle close (empty if none)."""
    ct = op._ts(fixture["commence_time"])
    parsed = op.parse_history(hist, pick=lambda arr: close_entry(arr, ct)).get(BOOK) or {}
    eid = f"op:{fixture['fixture_id']}"
    home, away, start = fixture["home_team"], fixture["away_team"], fixture["commence_time"]
    out = []
    for mkey, rows in parsed.items():
        rows = op.live(rows)
        main = op.main_row(rows)
        for r in rows:
            is_alt = 0 if r is main else 1
            at = r["changed_at"]
            base = (at, SOURCE, sport_key, eid, home, away, start, BOOK, mkey, is_alt)
            if mkey.startswith("totals"):
                out += [base + ("Over", r["home_point"], r["home_dec"]), base + ("Under", r["away_point"], r["away_dec"])]
            else:
                out += [base + (away, r["away_point"], r["away_dec"]), base + (home, r["home_point"], r["home_dec"])]
    return out


# ── Job ──────────────────────────────────────────────────────────────────────

def _state():
    return op._read(STATE, {}) or {}


def _have(event_ids):
    ids = list(event_ids)
    if not ids:
        return set()
    conn = history_tracker._connect()
    got = set()
    for i in range(0, len(ids), 500):
        part = ids[i:i + 500]
        got |= {r[0] for r in conn.execute(
            f"SELECT DISTINCT event_id FROM line_snapshots WHERE source=? AND event_id IN ({','.join('?' * len(part))})",
            [SOURCE, *part])}
    conn.close()
    return got


def _billable_ok():
    """Under the monthly guard? The account's own counter is the authority
    (free call); this month's ledger is the fallback."""
    try:
        used = op.account().get("request_count")
    except op.FeedError:
        used = None
    if used is None:
        used = op.billable_this_month()
    return used < REQUEST_GUARD, used


def _refresh_lists(now, log):
    ok, used = _billable_ok()
    for sport_key, hours in LIST_HOURS.items():
        if now < ACTIVE_FROM.get(sport_key, now):
            continue
        c = op.fixtures_cached(sport_key)
        if c and time.time() - c.get("at", 0) < hours * 3600:
            op.register(sport_key, c["fixtures"])          # the dashboard may have pulled it
            continue
        if not ok:
            log.append(f"{sport_key}: list skipped, {used} of {REQUEST_GUARD} monthly requests used")
            continue
        try:
            op.fixtures(sport_key, refresh=True, tag=TAG)
            used += 1
            ok = used < REQUEST_GUARD
            log.append(f"{sport_key}: fixture list pulled (1 request)")
        except op.FeedError as e:
            log.append(f"{sport_key}: list failed: {e}")


def due(now, reg=None, state=None, recent_days=RECENT_DAYS):
    """Registered games whose close is due and not yet stored or given up on."""
    reg = op.registry() if reg is None else reg
    state = _state() if state is None else state
    out = []
    for fid, f in reg.items():
        if f.get("sport_key") not in LIST_HOURS:
            continue
        k = op._ts(f.get("commence_time"))
        if not k or not (k + AFTER_KICKOFF.total_seconds() <= now.timestamp() <= k + recent_days * 86400):
            continue
        st = state.get(fid) or {}
        if st.get("status") in ("stored", "empty") or st.get("tries", 0) >= MAX_TRIES:
            continue
        if now.timestamp() - st.get("last", 0) < RETRY_AFTER:
            continue
        out.append(f)
    have = _have(f"op:{f['fixture_id']}" for f in out)
    return [f for f in out if f"op:{f['fixture_id']}" not in have]


LOCK = op.DIR / "close.lock"
LOCK_STALE = 45 * 60


def _acquire():
    """Cross-process lock (the app's background pass vs a command-line
    backfill), so a game is never fetched and stored twice at once."""
    op.DIR.mkdir(parents=True, exist_ok=True)
    try:
        if time.time() - LOCK.stat().st_mtime > LOCK_STALE:
            LOCK.unlink()
    except OSError:
        pass
    try:
        os.close(os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
        return True
    except FileExistsError:
        return False


def run_once(now=None, refresh_lists=True, only=None, recent_days=RECENT_DAYS) -> dict:
    """One pass: refresh due fixture lists, then store every due close.
    only: optional set of fixture ids (the backfill)."""
    if not _acquire():
        return {"skipped": "another close pass is running"}
    try:
        return _run_once(now, refresh_lists, only, recent_days)
    finally:
        try:
            LOCK.unlink()
        except OSError:
            pass


def _run_once(now, refresh_lists, only, recent_days):
    now = now or datetime.now(timezone.utc)
    log, stored, failed = [], 0, 0
    if refresh_lists:
        _refresh_lists(now, log)
    state = _state()
    games = due(now, state=state, recent_days=recent_days)
    if only is not None:
        games = [g for g in games if g["fixture_id"] in only]
    for f in games:
        fid = f["fixture_id"]
        st = state.setdefault(fid, {"tries": 0})
        st["tries"] += 1
        st["last"] = time.time()
        try:
            rows = close_rows(f["sport_key"], f, op.history(fid, [BOOK]))
        except op.FeedError as e:
            st["status"] = "error"
            st["error"] = str(e)[:200]
            failed += 1
        else:
            if rows:
                history_tracker._insert_lines(rows)
                st["status"] = "stored"
                stored += 1
            elif st["tries"] >= 2:                 # Pinnacle never priced it
                st["status"] = "empty"
        op._write(STATE, state)
    res = {"due": len(games), "stored": stored, "failed": failed, "log": log,
           "requests_month": op.billable_this_month(TAG)}
    if stored or failed or log:
        print(f"[pinnacle-close] {res}")
    return res


def _run():
    try:
        _status["last"] = run_once()
    except Exception as e:                      # never take the grader down
        _status["last"] = {"error": f"{type(e).__name__}: {e}"}
        print(f"[pinnacle-close] {type(e).__name__}: {e}")
    finally:
        _status["running"] = False


def start_if_due():
    """Kick off a background pass if one is due; returns at once."""
    if not enabled():
        return {"enabled": False}
    with _lock:
        if _status["running"] or time.time() - _status["last_run"] < RUN_EVERY:
            return {"running": _status["running"]}
        _status.update(running=True, last_run=time.time())
    threading.Thread(target=_run, daemon=True, name="pinnacle-close").start()
    return {"started": True}


def status():
    return {"enabled": enabled(), **_status, "requests_month": op.billable_this_month(TAG)}


def backfill(days=7) -> dict:
    """Closes for the last `days` of bets' games: one billable /v4/fixtures
    call per sport with bets, then free history calls for the bet games only."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    recent = [b for b in bets_store.list_bets() if b.get("commence_time") and b.get("market") != "parlay"
              and b.get("sport_key") in LIST_HOURS and start <= clv._parse(b["commence_time"]) <= now]
    want, nreq = set(), 0
    for sport_key in sorted({b["sport_key"] for b in recent}):
        ok, used = _billable_ok()
        if not ok:
            print(f"[pinnacle-close] backfill {sport_key} skipped: {used} requests used")
            continue
        fx = op.fixtures_between(sport_key, start - timedelta(hours=12), now, tag=TAG)
        nreq += 1
        for b in (b for b in recent if b["sport_key"] == sport_key):
            g = _match_fixture(b, fx)
            if g:
                want.add(g["fixture_id"])
    res = run_once(now, refresh_lists=False, only=want, recent_days=days + 1)
    return {**res, "games": len(want), "bets": len(recent), "list_requests": nreq}


# ── CLV vs Pinnacle's close ──────────────────────────────────────────────────

def _score(bet_name, name):
    return max(bet_grader._team_score(bet_name, {"name": name, "location": name, "short_name": name}),
               op._overlap(bet_name, name))


def _match_fixture(bet, fixtures):
    k = clv._parse(bet["commence_time"]).timestamp()
    best = None
    for f in fixtures:
        t = op._ts(f.get("commence_time"))
        if not t or abs(t - k) > 12 * 3600:
            continue
        straight = (_score(bet["away"], f["away_team"]), _score(bet["home"], f["home_team"]))
        flipped = (_score(bet["away"], f["home_team"]), _score(bet["home"], f["away_team"]))
        s = max(straight, flipped, key=sum)
        if min(s) >= 0.6 and (not best or sum(s) > best[0]):
            best = (sum(s), f)
    return best[1] if best else None


def _find_game(conn, bet):
    k = clv._parse(bet["commence_time"])
    rows = conn.execute("""SELECT DISTINCT event_id, home_team, away_team, commence_time FROM line_snapshots
                           WHERE source=? AND sport_key=? AND commence_time BETWEEN ? AND ?""",
                        (SOURCE, bet["sport_key"], (k - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         (k + timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ"))).fetchall()
    g = _match_fixture(bet, [{"fixture_id": eid, "home_team": h, "away_team": a, "commence_time": ct}
                             for eid, h, a, ct in rows])
    return (g["fixture_id"], g["home_team"], g["away_team"]) if g else None


def judge(rows, bet, mine, other):
    """(source, my price, other price, my point) from one market's close rows
    [(is_alt, side, point, price)] at the bet's own number: 'main' when
    Pinnacle's main line is there, 'alt' when only a ladder rung is,
    'off_number' otherwise (the main close is returned for display)."""
    def pair(alt):
        out = {}
        for is_alt, side, pt, price in rows:
            if is_alt == alt:
                out.setdefault(side, []).append((pt, price))
        return out

    main = pair(0)
    if not main.get(mine) or not main.get(other):
        return ("no_market", None, None, None)
    (mpt, mprice), = main[mine][:1]
    oprice = main[other][0][1]
    if bet.get("point") is None or mpt is None or abs(float(mpt) - float(bet["point"])) < 1e-9:
        return ("main", mprice, oprice, mpt)
    alts = pair(1)
    for pt, price in alts.get(mine) or []:
        if pt is not None and abs(float(pt) - float(bet["point"])) < 1e-9:
            return ("alt", mprice, oprice, mpt)
    return ("off_number", mprice, oprice, mpt)


def compute(now=None) -> dict:
    now = now or datetime.now(timezone.utc)
    conn = history_tracker._connect()
    filled = 0
    for b in bets_store.list_bets():
        if b.get("clv_pinn_source") or not b.get("commence_time") or b.get("sport_key") not in LIST_HOURS:
            continue
        if b.get("market") == "parlay" or str(b.get("market")).split("_")[0] not in ("h2h", "spreads", "totals"):
            continue
        k = clv._parse(b["commence_time"])
        if now < k + timedelta(minutes=30):
            continue
        g = _find_game(conn, b)
        if not g:
            # Only give up once the registry has dropped the game: a close can
            # land late (retries, a backfill), and 'none' is never revisited.
            if now - k > timedelta(days=op.REGISTRY_DAYS):
                bets_store.update_bet(b["id"], {"clv_pinn_source": "none"})
            continue
        eid, home, away = g
        if b["market"].startswith("totals"):
            mine, other = ("Over", "Under") if b["side"] == "over" else ("Under", "Over")
        else:
            team = b["away"] if b["side"] == "away" else b["home"]
            mine, other = (away, home) if _score(team, away) >= _score(team, home) else (home, away)
        rows = conn.execute("SELECT is_alt, side, point, price FROM line_snapshots WHERE source=? AND event_id=? AND market=?",
                            (SOURCE, eid, b["market"])).fetchall()
        src, price, oprice, pt = judge(rows, b, mine, other)
        if src == "no_market":
            if now - k > timedelta(days=1):
                bets_store.update_bet(b["id"], {"clv_pinn_source": "no_market"})
            continue
        upd = {"pinn_close_odds": clv._american(price), "pinn_close_point": pt, "clv_pinn_source": src}
        if src == "main":
            fair = clv.power_devig(price, oprice)
            upd.update(pinn_close_fair=round(fair, 5), clv_pinn=round(fair * clv._decimal(b["odds"]) - 1, 5))
        bets_store.update_bet(b["id"], upd)
        filled += 1
    conn.close()
    return {"filled": filled}


if __name__ == "__main__":       # python pinnacle_close.py backfill [days]
    import sys
    if sys.argv[1:2] == ["backfill"]:
        print(json.dumps(backfill(int(sys.argv[2]) if len(sys.argv) > 2 else 7), indent=1))
        print(json.dumps(compute(), indent=1))
    else:
        print(json.dumps(run_once(), indent=1))
