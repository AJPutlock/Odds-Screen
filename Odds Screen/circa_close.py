"""
Circa closing lines (odds-api.io /historical/closing-lines) and CLV against them.

pull()      Settled games' Circa closes into odds_history.db line_snapshots,
            source 'circa_close', book 'circa', event_id 'oio:<odds-api.io id>',
            markets in board keys (spreads_h1, totals_p1, ...), prices decimal.
            Every Circa row is kept — the main line plus any of Circa's few alt
            numbers (is_alt=1). Backfill once from the first Circa close
            (Feb 2026), then hourly over the last few days.
capture_live()  Circa's live prices for games about to start (the close pull's
            T-5 moment), source 'circa_t5': every market and number Circa
            hangs. compute() uses it when odds-api.io has no official close.
compute()   Each bet past kickoff gets circa_close_* and clv_circa: same number
            -> power de-vig of Circa's pair; a different number -> carried to
            the bet's number by the football outcome model (clv.py's) or the
            NHL goal model, centred on Circa's own close; else points only.

Bets carry The Odds API's event ids, odds-api.io games have their own, so a
bet finds its game by teams (bet_grader's fuzzy match) within 12 h of kickoff.

ON Sharp (10/12): odds-api.io's sharp consensus line (see oddsio.py) is stored
the same two ways under book 'onsharp' -- official closes as 'onsharp_close'
(backfilled from its Aug 2026 launch) and the T-5 capture as 'onsharp_t5' --
through the same functions with the book passed in. Circa's defaults are
unchanged, and compute() / near_miss still read only Circa's sources.
"""
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import requests

import bet_grader
import bets as bets_store
import clv
import history_tracker
import nhl_model
import oddsio

SOURCE = "circa_close"
T5_SOURCE = "circa_t5"      # live prices at kickoff - CLOSE_CAPTURE_MINUTES (capture_live)
# ON Sharp: (odds-api.io name, book key, official-close source, T-5 source, backfill start)
ONSHARP = ("ON Sharp", "onsharp", "onsharp_close", "onsharp_t5", datetime(2026, 8, 1, tzinfo=timezone.utc))
BACKFILL_FROM = datetime(2026, 1, 1, tzinfo=timezone.utc)   # first Circa closes are Feb 2026
RECENT_DAYS = 4
PULL_EVERY = 3600
MARKETS = {
    "american-football": "ML,Spread,Totals,ML HT,Spread HT,Totals HT,ML 1Q,Spread 1Q,Totals 1Q",
    "basketball": "ML,Spread,Totals,ML HT,Spread HT,Totals HT,ML 1Q,Spread 1Q,Totals 1Q",
    "ice-hockey": "ML,Spread,Totals,ML 1Q,Totals 1Q",
    "baseball": "ML,Spread,Totals,First 5 Innings ML,First 5 Innings Spread,First 5 Innings Totals,1st Inning Totals",
}
_last_pull = 0.0


def _have(event_ids, source=SOURCE):
    conn = history_tracker._connect()
    ids = list(event_ids)
    got = set()
    for i in range(0, len(ids), 500):
        part = ids[i:i + 500]
        got |= {r[0] for r in conn.execute(
            f"SELECT DISTINCT event_id FROM line_snapshots WHERE source=? AND event_id IN ({','.join('?' * len(part))})",
            [source, *part])}
    conn.close()
    return got


def _rows(sport_key, sport, ev, api_name="Circa", book="circa", source=SOURCE):
    """line_snapshots rows for one settled game's markets from one book (Circa)."""
    eid = f"oio:{ev['id']}"
    home, away = ev.get("home"), ev.get("away")
    out = []
    for m in (ev.get("bookmakers") or {}).get(api_name) or []:
        key = oddsio._market_key(sport, m.get("name") or "")
        if not key:
            continue
        base = key.split("_")[0]
        at = m.get("updatedAt") or ev.get("date")
        main, alts = oddsio._rows_to_market(base, m.get("odds") or [])
        if not main:
            continue
        dec = lambda am: clv._decimal(int(am)) if am else None
        lines = [(0, main)] + [(1, a) for a in alts]
        for is_alt, e in lines:
            if base == "h2h":
                out += [(at, source, sport_key, eid, home, away, ev.get("date"), book, key, 0, away, None, dec(e["away_odds"])),
                        (at, source, sport_key, eid, home, away, ev.get("date"), book, key, 0, home, None, dec(e["home_odds"]))]
            elif base == "spreads":
                out += [(at, source, sport_key, eid, home, away, ev.get("date"), book, key, is_alt, away, e["away_point"], dec(e["away_odds"])),
                        (at, source, sport_key, eid, home, away, ev.get("date"), book, key, is_alt, home, e["home_point"], dec(e["home_odds"]))]
            else:
                pt = e.get("home_point", e.get("point"))
                o, u = (e["home_odds"], e["away_odds"]) if "home_odds" in e else (e["over_odds"], e["under_odds"])
                out += [(at, source, sport_key, eid, home, away, ev.get("date"), book, key, is_alt, "Over", pt, dec(o)),
                        (at, source, sport_key, eid, home, away, ev.get("date"), book, key, is_alt, "Under", pt, dec(u))]
    return [r for r in out if r[-1]]


def pull(since=None, sports=None, api_name="Circa", book="circa", source=SOURCE) -> dict:
    """Store Circa's (or, with api_name/book/source, another odds-api.io book's)
    closes for games settled since `since` (default: the last RECENT_DAYS).
    Returns {sport_key: games stored, 'requests': n}."""
    if not oddsio.enabled():
        return {"error": "ODDS_API_IO_KEY isn't set"}
    now = datetime.now(timezone.utc)
    since = since or now - timedelta(days=RECENT_DAYS)
    out, nreq = {}, 0
    for sport_key, (sport, leagues, _) in oddsio.SPORTS.items():
        if sports and sport_key not in sports:
            continue
        rows, stored = [], 0
        start = since
        while start < now:
            end = min(now, start + timedelta(days=365))
            skip = 0
            while True:
                try:
                    evs = oddsio._get("/historical/closing-lines", sport=sport, leagues=",".join(leagues),
                                      markets=MARKETS[sport], bookmakers=api_name, limit=100, skip=skip,
                                      **{"from": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "to": end.strftime("%Y-%m-%dT%H:%M:%SZ")})
                except (oddsio.FeedError, ValueError) as e:
                    print(f"[circa-close] {sport_key}: {e}")
                    break
                nreq += 1
                evs = evs if isinstance(evs, list) else []
                have = _have((f"oio:{e['id']}" for e in evs), source)
                for ev in evs:
                    if f"oio:{ev['id']}" in have or not (ev.get("bookmakers") or {}).get(api_name):
                        continue
                    r = _rows(sport_key, sport, ev, api_name, book, source)
                    if r:
                        rows += r
                        stored += 1
                if len(evs) < 100:
                    break
                skip += 100
            start = end
        history_tracker._insert_lines(rows)
        out[sport_key] = stored
    out["requests"] = nreq
    return out


def _game_rows(sport_key, eid, g, at, source=T5_SOURCE, book="circa"):
    """line_snapshots rows (source T5_SOURCE) from one live feed game dict:
    American odds, totals with the Over in the home slot (oddsio's shape)."""
    home, away, ct = g.get("home_team"), g.get("away_team"), g.get("commence_time")

    def row(key, is_alt, side, pt, am):
        try:
            price = clv._decimal(int(str(am))) if am else None
        except ValueError:
            price = None
        return (at, source, sport_key, eid, home, away, ct, book, key, is_alt, side, pt, price)

    out = []
    for key, m in (g.get("markets") or {}).items():
        base = key.split("_")[0]
        if base == "h2h":
            out += [row(key, 0, away, None, m.get("away_odds")), row(key, 0, home, None, m.get("home_odds"))]
        elif base == "spreads":
            out += [row(key, 0, away, m.get("away_point"), m.get("away_odds")),
                    row(key, 0, home, m.get("home_point"), m.get("home_odds"))]
        else:
            out += [row(key, 0, "Over", m.get("home_point"), m.get("home_odds")),
                    row(key, 0, "Under", m.get("home_point"), m.get("away_odds"))]
    for key, alts in (g.get("alt_lines") or {}).items():
        base = key.split("_")[0]
        for a in alts:
            if base == "spreads":
                out += [row(key, 1, away, a.get("away_point"), a.get("away_odds")),
                        row(key, 1, home, a.get("home_point"), a.get("home_odds"))]
            elif base == "totals":
                out += [row(key, 1, "Over", a.get("point"), a.get("over_odds")),
                        row(key, 1, "Under", a.get("point"), a.get("under_odds"))]
    return [r for r in out if r[-1]]


def capture_live(sport_key, minutes, book="circa", source=T5_SOURCE) -> int:
    """Store Circa's current prices for games starting within `minutes` that
    haven't had a capture: the close at kickoff - CLOSE_CAPTURE_MINUTES, for
    every sport, every market and alt number Circa hangs. Returns rows stored.
    The cached feed says whether anything is due; if so Circa is pulled fresh
    (oddsio.get_fresh) and that is what gets stored."""
    def due(feed):
        now = datetime.now(timezone.utc)
        out = []
        for g in (feed or {}).get("data") or []:
            try:
                ct = datetime.fromisoformat(str(g.get("commence_time")).replace("Z", "+00:00"))
            except ValueError:
                continue
            if g.get("_oddsio_id") is not None and now - timedelta(minutes=1) <= ct <= now + timedelta(minutes=minutes):
                out.append(g)
        return out

    games = due(oddsio.get(sport_key, book))
    if not games:
        return 0
    have = _have((f"oio:{g['_oddsio_id']}" for g in games), source)
    if all(f"oio:{g['_oddsio_id']}" in have for g in games):
        return 0
    games = due(oddsio.get_fresh(sport_key, book))
    have = _have((f"oio:{g['_oddsio_id']}" for g in games), source)
    at = datetime.now(timezone.utc).isoformat()
    rows = []
    for g in games:
        eid = f"oio:{g['_oddsio_id']}"
        if eid not in have:
            rows += _game_rows(sport_key, eid, g, at, source, book)
    n = history_tracker._insert_lines(rows)
    if n:
        print(f"[close] {sport_key} {'Circa' if book == 'circa' else 'ON Sharp'} — {n} line(s) across {len({r[3] for r in rows})} game(s)")
    return n


def capture_live_onsharp(sport_key, minutes) -> int:
    """ON Sharp's T-5 close (it's pulled at kickoff, so this is its last price)."""
    return capture_live(sport_key, minutes, book=ONSHARP[1], source=ONSHARP[3])


def pull_onsharp(since=None, sports=None) -> dict:
    """ON Sharp's official closes; the first call backfills from its launch."""
    api_name, book, source, _, start = ONSHARP
    if since is None:
        conn = history_tracker._connect()
        have_any = conn.execute("SELECT 1 FROM line_snapshots WHERE source=? LIMIT 1", (source,)).fetchone()
        conn.close()
        since = None if have_any else start
    return pull(since, sports, api_name=api_name, book=book, source=source)


def pull_recent_if_due():
    global _last_pull
    if time.time() - _last_pull < PULL_EVERY:
        return None
    _last_pull = time.time()
    out = pull()
    try:
        out["onsharp"] = pull_onsharp()
    except Exception as e:                  # never let ON Sharp break Circa's hourly pull
        out["onsharp"] = {"error": f"{type(e).__name__}: {e}"}
    return out


# ── CLV vs Circa's close ─────────────────────────────────────────────────────

def _find_game(conn, bet):
    k = clv._parse(bet["commence_time"])
    rows = conn.execute("""SELECT DISTINCT event_id, home_team, away_team, commence_time FROM line_snapshots
                           WHERE source IN (?, ?) AND sport_key=? AND commence_time BETWEEN ? AND ?""",
                        (SOURCE, T5_SOURCE, bet["sport_key"], (k - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         (k + timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ"))).fetchall()
    best = None
    for eid, home, away, ct in rows:
        same = (bet_grader._team_score(bet["home"], {"name": home, "location": home, "short_name": home}),
                bet_grader._team_score(bet["away"], {"name": away, "location": away, "short_name": away}))
        if min(same) >= 0.6 and (not best or sum(same) > best[0]):
            best = (sum(same), eid, home, away)
    return best[1:] if best else None


def _pair(conn, eid, market, mine, other, point=None):
    """(my price, other price, my point) for Circa's MAIN line, or None: the
    official close when odds-api.io has one, else the T-5 live capture."""
    for src in (SOURCE, T5_SOURCE):
        rows = conn.execute("SELECT side, point, price FROM line_snapshots WHERE source=? AND event_id=? AND market=? AND is_alt=0",
                            (src, eid, market)).fetchall()
        m = next((r for r in rows if r[0] == mine), None)
        o = next((r for r in rows if r[0] == other), None)
        if m and o:
            return m[2], o[2], m[1]
    return None


def compute(now=None) -> dict:
    now = now or datetime.now(timezone.utc)
    conn = history_tracker._connect()
    filled = 0
    for b in bets_store.list_bets():
        if b.get("clv_circa_source") or not b.get("commence_time") or not b.get("sport_key"):
            continue
        k = clv._parse(b["commence_time"])
        if now < k + timedelta(minutes=30):
            continue
        if b["market"] not in history_tracker.TRACKED_MARKETS and not str(b["market"]).split("_")[0] in ("h2h", "spreads", "totals"):
            continue
        g = _find_game(conn, b)
        if not g:
            if now - k > timedelta(days=3):     # odds-api.io never posted it (or Circa had no line)
                bets_store.update_bet(b["id"], {"clv_circa_source": "none"})
            continue
        eid, home, away = g
        totals = b["market"].startswith("totals")
        if totals:
            mine, other = ("Over", "Under") if b["side"] == "over" else ("Under", "Over")
        else:
            mine, other = (away, home) if b["side"] == "away" else (home, away)
        pr = _pair(conn, eid, b["market"], mine, other)
        if not pr:
            if now - k > timedelta(days=3):
                bets_store.update_bet(b["id"], {"clv_circa_source": "no_market"})
            continue
        price, other_price, cpt = pr
        fair_close = clv.power_devig(price, other_price)
        upd = {"circa_close_odds": clv._american(price), "circa_close_point": cpt}
        same = b["market"].startswith("h2h") or cpt is None or b.get("point") is None or abs(float(cpt) - float(b["point"])) < 1e-9
        fair = fair_close if same else _carry(conn, b, eid, home, away, float(cpt), fair_close)
        if fair is not None:
            upd.update(circa_close_fair=round(fair, 5), clv_circa=round(fair * clv._decimal(b["odds"]) - 1, 5),
                       clv_circa_source="same_line" if same else "model")
        else:
            upd["clv_circa_source"] = "points"
        bets_store.update_bet(b["id"], upd)
        filled += 1
    conn.close()
    return {"filled": filled}


def _carry(conn, b, eid, home, away, cpt, fair_close):
    """Circa's closing fair at its number carried to the bet's number."""
    mkt, side, bpt = b["market"], b["side"], float(b["point"])
    if b["sport_key"] == "icehockey_nhl":
        ml = _pair(conn, eid, "h2h", home, away)
        p_home = clv.power_devig(ml[0], ml[1]) if ml else None
        if mkt == "totals":
            sp = _pair(conn, eid, "spreads", away, home)
            pl = (sp[2], clv.power_devig(sp[0], sp[1])) if sp else None
            over = side == "over"
            p = nhl_model.over_at(cpt, fair_close if over else 1 - fair_close, bpt, p_home, pl)
            return None if p is None else (p if over else 1 - p)
        if mkt == "spreads":
            tot = _pair(conn, eid, "totals", "Over", "Under")
            if not tot:
                return None
            away_side = side == "away"
            known, target = (cpt, bpt) if away_side else (-cpt, -bpt)
            p = nhl_model.away_cover_at(tot[2], clv.power_devig(tot[0], tot[1]), p_home, known,
                                        fair_close if away_side else 1 - fair_close, target)
            return None if p is None else (p if away_side else 1 - p)
        return None
    import re
    t_all = clv._model().get(b["sport_key"])
    m = re.match(r"^(spreads|totals)(_h1|_q1)?$", mkt)
    if not t_all or not m or not t_all.get(mkt):
        return None
    suffix = m.group(2) or ""
    if abs(bpt - cpt) > clv.FB_AVG_PTS_PER_PLAY * clv.FB_PERIOD_SHARE[suffix] ** 0.5:
        return None
    spreads = m.group(1) == "spreads"
    if spreads:
        ap = _pair(conn, eid, mkt, away, home)
        center = ap[2] if ap else None
    else:
        center = cpt
    if center is None:
        return None
    upper = side in ("away", "over")
    to_x = (lambda pt: pt if upper else -pt) if spreads else (lambda pt: pt)
    t = t_all[mkt]
    pb, pc = clv._upper_win(t, spreads, center, to_x(bpt)), clv._upper_win(t, spreads, center, to_x(cpt))
    if pb is None or pc is None:
        return None
    if not upper:
        pb, pc = 1 - pb, 1 - pc
    return min(clv.FB_TAIL[1], max(clv.FB_TAIL[0], fair_close + (pb - pc)))
