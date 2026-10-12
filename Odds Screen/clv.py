"""
Closing-line value for tracked bets, against BetOnline's close.

For each bet whose game has started, find BetOnline's closing price for the
bet's market (both sides, its main line), remove the margin (power de-vig, two
ways) and compare:

  same line as the bet  ->  clv = closing fair prob x decimal(bet odds) - 1
                            (the EV of the bet if the close was the truth;
                            same definition as the screen's +EV%)
  line moved            ->  clv_points = points gained vs the close (+ = beat
                            it: more points on a spread, a lower Over, a higher
                            Under); clv % isn't computed across a line move
                            (BetOnline posts no alt lines to price it at)

Where the close comes from, in order:
  1. line_snapshots 'odds_api_close': the app's own pull at kickoff - 10 min
     (free; only exists if the app was running then)
  2. The Odds API historical endpoint at kickoff - 10 min (10 credits per
     market; stored back into line_snapshots as 'odds_api_hist_close', so it's
     paid once). Capped per run by HIST_CREDIT_CAP.
  3. snapshots (hourly main-line capture): last BetOnline price before
     kickoff, only if within 2 h of it.
"""
import sqlite3
from datetime import datetime, timedelta, timezone

import requests

import bets as bets_store
import history_tracker
import nhl_model

BOOK = "betonlineag"
CLOSE_OFFSET = timedelta(minutes=10)
HOURLY_MAX_AGE = timedelta(hours=2)
HIST_CREDIT_CAP = 600             # per run; ~10 credits per market per game
HIST_URL = "https://api.the-odds-api.com/v4/historical/sports/{sport}/events/{event}/odds"


def _parse(ts):
    t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def _american(dec):
    if not dec or dec <= 1:
        return None
    return round((dec - 1) * 100) if dec >= 2 else round(-100 / (dec - 1))


def _decimal(american):
    a = int(american)
    return 1 + a / 100 if a > 0 else 1 + 100 / abs(a)


def power_devig(d_mine, d_other):
    p1, p2 = 1 / d_mine, 1 / d_other
    lo, hi = 1.0, 3.0
    for _ in range(60):
        k = (lo + hi) / 2
        lo, hi = (k, hi) if p1 ** k + p2 ** k > 1 else (lo, k)
    return p1 ** ((lo + hi) / 2)


def _outcome_names(bet):
    """(my outcome name, the other side's) as the Odds API names them."""
    if bet["market"].startswith("totals"):
        return ("Over", "Under") if bet["side"] == "over" else ("Under", "Over")
    return (bet["away"], bet["home"]) if bet["side"] == "away" else (bet["home"], bet["away"])


def _pair_from_rows(rows, bet):
    """rows: (side, point, price) for one book/market; my side and the other."""
    mine_n, other_n = _outcome_names(bet)
    mine = [r for r in rows if r[0] == mine_n]
    other = [r for r in rows if r[0] == other_n]
    if not mine or not other:
        return None
    return {"point": mine[0][1], "price": mine[0][2], "other_price": other[0][2]}


def _from_close_capture(conn, bet, sources=("odds_api_close", "odds_api_hist_close")):
    q = ",".join("?" * len(sources))
    row = conn.execute(f"""SELECT MAX(captured_at) FROM line_snapshots
                           WHERE event_id=? AND market=? AND book_key=? AND is_alt=0 AND source IN ({q})""",
                       (bet["event_id"], bet["market"], BOOK, *sources)).fetchone()
    if not row or not row[0]:
        return None
    rows = conn.execute("""SELECT side, point, price, source FROM line_snapshots
                           WHERE event_id=? AND market=? AND book_key=? AND is_alt=0 AND captured_at=?""",
                        (bet["event_id"], bet["market"], BOOK, row[0])).fetchall()
    pair = _pair_from_rows([r[:3] for r in rows], bet)
    if pair:
        pair["source"] = rows[0][3]
        pair["at"] = row[0]
    return pair


def _from_hourly(conn, bet, kickoff):
    if bet["market"] not in history_tracker.TRACKED_MARKETS:
        return None
    row = conn.execute("""SELECT MAX(captured_at) FROM snapshots
                          WHERE event_id=? AND market=? AND book_key=? AND captured_at < ?""",
                       (bet["event_id"], bet["market"], BOOK, kickoff.isoformat())).fetchone()
    if not row or not row[0] or kickoff - _parse(row[0]) > HOURLY_MAX_AGE:
        return None
    rows = conn.execute("""SELECT side, point, price FROM snapshots
                           WHERE event_id=? AND market=? AND book_key=? AND captured_at=?""",
                        (bet["event_id"], bet["market"], BOOK, row[0])).fetchall()
    pair = _pair_from_rows(rows, bet)
    if pair:
        pair.update(source="hourly", at=row[0])
    return pair


def _fetch_historical(api_key, bet, kickoff, markets):
    """BetOnline at kickoff - 10 min for these markets; stored to line_snapshots.
    Returns credits spent."""
    r = requests.get(HIST_URL.format(sport=bet["sport_key"], event=bet["event_id"]), params={
        "apiKey": api_key, "bookmakers": BOOK, "markets": ",".join(sorted(markets)),
        "oddsFormat": "decimal", "date": (kickoff - CLOSE_OFFSET).strftime("%Y-%m-%dT%H:%M:%SZ")}, timeout=20)
    spent = int(r.headers.get("x-requests-last") or 0)
    if r.status_code != 200:
        print(f"[clv] historical {bet['away']} @ {bet['home']}: HTTP {r.status_code} {r.text[:150]}")
        return spent
    ev = (r.json() or {}).get("data") or {}
    snap = (r.json() or {}).get("timestamp") or (kickoff - CLOSE_OFFSET).isoformat()
    rows = []
    for bk in ev.get("bookmakers", []):
        for m in bk.get("markets", []):
            for oc in m.get("outcomes", []):
                if oc.get("price") is None:
                    continue
                rows.append((snap, "odds_api_hist_close", bet["sport_key"], bet["event_id"], ev.get("home_team", bet["home"]),
                             ev.get("away_team", bet["away"]), ev.get("commence_time"), bk.get("key"), m.get("key"), 0,
                             oc.get("name"), oc.get("point"), oc.get("price")))
    history_tracker._insert_lines(rows)
    return spent


# ── Football cross-line: the screen's fitted outcome model ──────────────────
# When the bet's line differs from BetOnline's closing line, the closing fair is
# carried to the bet's line the same way the screen prices a cross-line edge:
# fair(bet line) = fair(close line) + [model(bet line) - model(close line)], the
# model being FB_EMPIRICAL (Normal/t scatter around the market line x
# key-number multipliers), read straight from static/index.html so the screen
# and CLV always use the same numbers. Upper axis: away points for spreads
# (the distribution is the HOME margin, centred on the away point), Over for
# totals. Past FB_AVG_PTS_PER_PLAY x sqrt(period share) points the model isn't
# trusted (same cutoff as fbMaxCrossLineGap): points only.

FB_PERIOD_SHARE = {"": 1.0, "_h1": 0.5, "_q1": 0.25}
FB_AVG_PTS_PER_PLAY = 5.89
FB_TAIL = (0.01, 0.99)
_fb_model = None


def _model():
    global _fb_model
    if _fb_model is None:
        import json, re
        from pathlib import Path
        page = (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")
        _fb_model = {}
        for sk in ("americanfootball_ncaaf", "americanfootball_nfl"):
            m = re.search(r"^\s*" + sk + r": (\{.*\}),?\s*$", page, re.M)
            if m:
                _fb_model[sk] = json.loads(m.group(1))
    return _fb_model


def _upper_win(t, spreads, center, x):
    """No-push win probability of the upper side at line x, or None in the tails."""
    import math
    span = len(t["mult"]) - 1
    offset = span if spreads else 0
    n = 2 * span + 1 if spreads else span + 1
    pmf = []
    for i in range(n):
        v = i - offset
        d = (v - center) / t["scale"]
        w = math.exp(-0.5 * d * d) if t["nu"] is None else (1 + d * d / t["nu"]) ** (-(t["nu"] + 1) / 2)
        pmf.append(w * t["mult"][abs(v)])
    z = sum(pmf)
    win = push = 0.0
    for i, w in enumerate(pmf):
        v = i - offset
        if v == x:
            push += w / z
        elif (v < x) if spreads else (v > x):
            win += w / z
    if push >= 1:
        return None
    p = win / (1 - push)
    return p if FB_TAIL[0] <= p <= FB_TAIL[1] else None


def _close_centers(conn, bet, pair, suffix):
    """(mu, tau): BetOnline's closing away spread and total for this period (full
    game x period share when the period line is missing)."""
    if pair["source"] == "hourly":
        q = "SELECT market, side, point FROM snapshots WHERE event_id=? AND book_key=? AND captured_at=?"
        rows = conn.execute(q, (bet["event_id"], BOOK, pair["at"])).fetchall()
    else:
        q = "SELECT market, side, point FROM line_snapshots WHERE event_id=? AND book_key=? AND is_alt=0 AND captured_at=?"
        rows = conn.execute(q, (bet["event_id"], BOOK, pair["at"])).fetchall()
    def line(market, side):
        return next((r[2] for r in rows if r[0] == market and r[1] == side and r[2] is not None), None)
    share = FB_PERIOD_SHARE[suffix]
    mu = line("spreads" + suffix, bet["away"])
    if mu is None and line("spreads", bet["away"]) is not None:
        mu = line("spreads", bet["away"]) * share
    tau = line("totals" + suffix, "Over")
    if tau is None and line("totals", "Over") is not None:
        tau = line("totals", "Over") * share
    return mu, tau


def _nhl_cross_fair(conn, bet, pair, close_fair):
    """NHL: the close carried to the bet's number by the goal model — totals
    half a goal, puck lines up to nhl_model.SPREAD_MAX_GAP. The model is fitted
    to BetOnline's total, moneyline and puck line at the same capture."""
    if bet.get("point") is None or pair.get("point") is None or bet["market"] not in ("totals", "spreads"):
        return None
    table = "snapshots" if pair["source"] == "hourly" else "line_snapshots"
    extra = "" if table == "snapshots" else " AND is_alt=0"
    rows = conn.execute(f"SELECT market, side, point, price FROM {table} WHERE event_id=? AND book_key=? "
                        f"AND captured_at=?{extra}", (bet["event_id"], BOOK, pair["at"])).fetchall()
    get = lambda mk, side: next(((r[2], r[3]) for r in rows if r[0] == mk and r[1] == side), (None, None))
    _, ph = get("h2h", bet["home"])
    _, pa = get("h2h", bet["away"])
    p_home = power_devig(ph, pa) if ph and pa else None
    tpt, to = get("totals", "Over")
    _, tu = get("totals", "Under")
    apt, sa = get("spreads", bet["away"])
    _, sh = get("spreads", bet["home"])
    pl = (apt, power_devig(sa, sh)) if apt is not None and sa and sh else None
    if bet["market"] == "totals":
        over = bet["side"] == "over"
        p = nhl_model.over_at(float(pair["point"]), close_fair if over else 1 - close_fair, float(bet["point"]), p_home, pl)
        return None if p is None else min(FB_TAIL[1], max(FB_TAIL[0], p if over else 1 - p))
    if tpt is None or not (to and tu):
        return None
    away = bet["side"] == "away"
    # puck-line points on the away axis: a home +1.5 is the away -1.5
    known = float(pair["point"]) if away else -float(pair["point"])
    target = float(bet["point"]) if away else -float(bet["point"])
    fair_away_known = close_fair if away else 1 - close_fair
    p = nhl_model.away_cover_at(float(tpt), power_devig(to, tu), p_home, known, fair_away_known, target)
    return None if p is None else min(FB_TAIL[1], max(FB_TAIL[0], p if away else 1 - p))


def cross_line_fair(conn, bet, pair, close_fair):
    """Closing fair carried to the bet's own line (football spreads/totals,
    NHL totals), or None."""
    if bet["sport_key"] == "icehockey_nhl":
        return _nhl_cross_fair(conn, bet, pair, close_fair)
    t_all = _model().get(bet["sport_key"])
    import re
    m = re.match(r"^(spreads|totals)(_h1|_q1)?$", bet["market"])
    if not t_all or not m or bet.get("point") is None or pair.get("point") is None:
        return None
    base, suffix = m.group(1), m.group(2) or ""
    t = t_all.get(bet["market"])
    if not t:
        return None
    if abs(float(bet["point"]) - float(pair["point"])) > FB_AVG_PTS_PER_PLAY * FB_PERIOD_SHARE[suffix] ** 0.5:
        return None
    mu, tau = _close_centers(conn, bet, pair, suffix)
    spreads = base == "spreads"
    center = mu if spreads else tau
    if center is None:
        return None
    upper = bet["side"] in ("away", "over")
    to_x = (lambda pt: float(pt) if upper else -float(pt)) if spreads else float
    pb, pc = _upper_win(t, spreads, center, to_x(bet["point"])), _upper_win(t, spreads, center, to_x(pair["point"]))
    if pb is None or pc is None:
        return None
    if not upper:
        pb, pc = 1 - pb, 1 - pc
    return min(FB_TAIL[1], max(FB_TAIL[0], close_fair + (pb - pc)))


def compute(api_key=None, now=None, allow_historical=True) -> dict:
    """Fill close/CLV on every bet that's past kickoff and doesn't have one yet."""
    now = now or datetime.now(timezone.utc)
    conn = sqlite3.connect(history_tracker.DB_PATH, timeout=30)
    todo = []
    for b in bets_store.list_bets():
        # Done: a CLV %, or a moved line the model can't bridge (non-football /
        # too far / tails) that was already tried. A football bet with only
        # points is re-tried so the cross-line conversion can fill it in.
        retry = b.get("clv_source") and b.get("clv") is None and b.get("sport_key", "").startswith("americanfootball")             and not str(b.get("clv_source")).endswith("+points")
        if (b.get("clv_source") and not retry) or not b.get("event_id") or not b.get("commence_time"):
            continue
        if b["market"] in ("team_totals",) or not b["market"].split("_")[0] in ("h2h", "spreads", "totals"):
            continue
        k = _parse(b["commence_time"])
        if now < k:
            continue
        todo.append((b, k))
    # Historical pulls: one call per game for every market its bets need.
    credits, filled, missing = 0, 0, []
    if allow_historical and api_key:
        need = {}
        for b, k in todo:
            if not _from_close_capture(conn, b):
                need.setdefault((b["sport_key"], b["event_id"]), (b, k, set()))[2].add(b["market"])
        for (sk, ev), (b, k, mkts) in need.items():
            if credits + 10 * len(mkts) > HIST_CREDIT_CAP:
                break
            try:
                credits += _fetch_historical(api_key, b, k, mkts)
            except requests.exceptions.RequestException as e:
                print(f"[clv] historical {b['away']} @ {b['home']}: {type(e).__name__}: {e}")
    for b, k in todo:
        pair = _from_close_capture(conn, b) or _from_hourly(conn, b, k)
        if not pair:
            missing.append(b["id"])
            continue
        upd = {"close_odds": _american(pair["price"]), "close_point": pair["point"],
               "clv_source": pair["source"], "clv_at": pair["at"], "close_fair": None, "clv": None, "clv_points": None}
        same_line = b["market"].startswith("h2h") or pair["point"] is None or b.get("point") is None \
            or abs(float(pair["point"]) - float(b["point"])) < 1e-9
        if same_line:
            fair = power_devig(pair["price"], pair["other_price"])
            upd["close_fair"] = round(fair, 5)
            upd["clv"] = round(fair * _decimal(b["odds"]) - 1, 5)
            upd["clv_points"] = 0.0
        else:
            d = float(b["point"]) - float(pair["point"])
            upd["clv_points"] = round(-d if b["side"] == "over" else d, 2)
            # Carry the closing fair to the bet's own line (football model); points only when it can't.
            fair_b = cross_line_fair(conn, b, pair, power_devig(pair["price"], pair["other_price"]))
            if fair_b is not None:
                upd["close_fair"] = round(fair_b, 5)
                upd["clv"] = round(fair_b * _decimal(b["odds"]) - 1, 5)
                upd["clv_source"] = pair["source"] + "+model"
            else:
                upd["clv_source"] = pair["source"] + "+points"
        bets_store.update_bet(b["id"], upd)
        filled += 1
    conn.close()
    return {"filled": filled, "missing": missing, "credits": credits}
