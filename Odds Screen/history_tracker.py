"""
Hourly line-history capture + off-market alerting.

Captures full-game h2h/spreads/totals across all Odds-API-sourced books
(including the betonlineag reference line) into a local SQLite DB every
time `capture_snapshot` is called, and flags any recreational book whose
price at BetOnline's own point is at least ALERT_EDGE_THRESHOLD off
BetOnline's no-vig fair probability. New qualifying lines are pushed to
Telegram; lines that stop qualifying are cleared silently.

Also archives alt ladders and 1H/Q1/team-total lines into `line_snapshots`
(Odds API close pull + every Bookmaker.eu collection) for CLV grading.
"""

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests

import novig_filter

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DB_PATH = Path(__file__).parent / "data" / "odds_history.db"
DB_PATH.parent.mkdir(exist_ok=True)

REFERENCE_BOOK = "betonlineag"
TRACKED_MARKETS = ("h2h", "spreads", "totals")
ALERT_EDGE_THRESHOLD = 0.01  # 1 percentage point of fair probability

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = _connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at   TEXT NOT NULL,
            sport_key     TEXT NOT NULL,
            event_id      TEXT NOT NULL,
            home_team     TEXT NOT NULL,
            away_team     TEXT NOT NULL,
            commence_time TEXT,
            book_key      TEXT NOT NULL,
            market        TEXT NOT NULL,
            side          TEXT NOT NULL,
            point         REAL,
            price         REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
        ON snapshots (event_id, market, side, book_key, captured_at)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS active_alerts (
            alert_key   TEXT PRIMARY KEY,
            sport_key   TEXT,
            event_id    TEXT,
            matchup     TEXT,
            commence_time TEXT,
            book_key    TEXT,
            market      TEXT,
            side        TEXT,
            point       REAL,
            price       REAL,
            edge_pct    REAL,
            first_seen  TEXT,
            last_seen   TEXT
        )
    """)
    # Closing-line archive for everything `snapshots` doesn't hold — alt ladders,
    # 1H/Q1, team totals — so alt and derivative bets can be graded on CLV.
    # Two sources: the Odds API per-event pull at kickoff minus
    # CLOSE_CAPTURE_MINUTES (source 'odds_api_close'; soft books + BetOnline's
    # main lines — the Odds API carries no BetOnline alts or periods), and every
    # Bookmaker.eu collection (source 'bookmaker'; its full ladders, the sharp
    # reference for derivative/alt closes). `market` is the base key
    # (spreads_h1, not alternate_spreads_h1) with is_alt marking ladder rungs;
    # price is decimal, as in `snapshots`.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS line_snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at   TEXT NOT NULL,
            source        TEXT NOT NULL,
            sport_key     TEXT NOT NULL,
            event_id      TEXT NOT NULL,
            home_team     TEXT NOT NULL,
            away_team     TEXT NOT NULL,
            commence_time TEXT,
            book_key      TEXT NOT NULL,
            market        TEXT NOT NULL,
            is_alt        INTEGER NOT NULL,
            side          TEXT NOT NULL,
            point         REAL,
            price         REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_line_snapshots_lookup
        ON line_snapshots (event_id, market, book_key, side, point, captured_at)
    """)
    conn.commit()
    conn.close()


init_db()


# ── odds math ────────────────────────────────────────────────────────────────

def _american_from_decimal(decimal_price):
    # decimal_price <= 1.0 isn't a real price (seen from novig's exchange feed as a
    # placeholder/no-offer value) — treat as missing rather than dividing by zero.
    if decimal_price is None or decimal_price <= 1.0:
        return None
    if decimal_price >= 2.0:
        return round((decimal_price - 1) * 100)
    return round(-100 / (decimal_price - 1))


def _implied_prob(american_price):
    if american_price is None:
        return None
    if american_price > 0:
        return 100.0 / (american_price + 100.0)
    return -american_price / (-american_price + 100.0)


def _devig_shin(pa: float, pb: float) -> tuple[float, float]:
    """Shin de-vig — same method as the odds screen (see devigShin in index.html):
    more of the margin comes off the longshot than proportional de-vig removes."""
    s = pa + pb
    if s <= 1:
        return pa / s, pb / s
    f = lambda q, z: ((z * z + 4 * (1 - z) * q * q / s) ** 0.5 - z) / (2 * (1 - z))
    lo, hi = 0.0, 0.5
    for _ in range(50):
        z = (lo + hi) / 2
        if f(pa, z) + f(pb, z) > 1:
            lo = z
        else:
            hi = z
    z = (lo + hi) / 2
    a, b = f(pa, z), f(pb, z)
    return a / (a + b), b / (a + b)


# ── snapshot capture ─────────────────────────────────────────────────────────

def capture_snapshot(sport_key: str, raw_games: list) -> int:
    """Insert one row per (event, book, market, outcome) for the current pull."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for game in raw_games or []:
        event_id = game.get("id")
        home = game.get("home_team")
        away = game.get("away_team")
        commence = game.get("commence_time")
        if not event_id or not home or not away:
            continue
        for bk in game.get("bookmakers", []):
            book_key = bk.get("key")
            for mkt in bk.get("markets", []):
                mkt_key = mkt.get("key")
                if mkt_key not in TRACKED_MARKETS:
                    continue
                for oc in mkt.get("outcomes", []):
                    price = oc.get("price")
                    if price is None:
                        continue
                    rows.append((
                        now, sport_key, event_id, home, away, commence,
                        book_key, mkt_key, oc.get("name"), oc.get("point"), price,
                    ))
    if not rows:
        return 0
    conn = _connect()
    conn.executemany(
        """INSERT INTO snapshots
           (captured_at, sport_key, event_id, home_team, away_team, commence_time,
            book_key, market, side, point, price)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    conn.close()
    return len(rows)


# ── closing-line archive (alts + derivatives) ────────────────────────────────

CLOSE_CAPTURE_MINUTES = 10   # per-event close pull fires this long before kickoff
# Billed per market that returns data (~11-12 credits per game). The Odds API
# has no Q1 for NFL and no 1H alts for NCAAF — missing markets cost nothing.
CLOSE_MARKETS = [
    "h2h", "spreads", "totals", "alternate_spreads", "alternate_totals",
    "h2h_h1", "spreads_h1", "totals_h1", "alternate_spreads_h1", "alternate_totals_h1",
    "h2h_q1", "spreads_q1", "totals_q1", "team_totals",
]


def _american_str_to_decimal(odds):
    """'+137' / '-182' (Bookmaker's scraped format) → decimal; None if unparseable."""
    try:
        n = int(str(odds).replace("+", ""))
    except (TypeError, ValueError):
        return None
    if n >= 100:
        return 1 + n / 100
    if n <= -100:
        return 1 + 100 / -n
    return None


def _insert_lines(rows: list) -> int:
    if not rows:
        return 0
    conn = _connect()
    conn.executemany(
        """INSERT INTO line_snapshots
           (captured_at, source, sport_key, event_id, home_team, away_team, commence_time,
            book_key, market, is_alt, side, point, price)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    conn.close()
    return len(rows)


def capture_close_event(sport_key: str, event: dict) -> int:
    """Store one per-event Odds API response (every book, market and rung)."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for bk in event.get("bookmakers", []):
        for mkt in bk.get("markets", []):
            key = mkt.get("key", "")
            is_alt = key.startswith("alternate_")
            base = key[len("alternate_"):] if is_alt else key
            for oc in mkt.get("outcomes", []):
                if oc.get("price") is None:
                    continue
                side = oc.get("name")
                if base == "team_totals":   # "Over"/"Under" alone doesn't say whose total
                    side = f"{oc.get('description', '')}|{side}"
                rows.append((now, "odds_api_close", sport_key, event["id"], event["home_team"],
                             event["away_team"], event.get("commence_time"), bk.get("key"),
                             base, int(is_alt), side, oc.get("point"), oc.get("price")))
    return _insert_lines(rows)


def closes_captured(event_ids) -> set:
    """Which of these events already have an 'odds_api_close' capture."""
    ids = list(event_ids)
    if not ids:
        return set()
    conn = _connect()
    got = {r[0] for r in conn.execute(
        f"SELECT DISTINCT event_id FROM line_snapshots WHERE source = 'odds_api_close' "
        f"AND event_id IN ({','.join('?' * len(ids))})", ids)}
    conn.close()
    return got


def capture_bookmaker(sport_key: str, matched: list) -> int:
    """
    Store a Bookmaker.eu collection: `matched` is [(api_game, bk_game)] with
    the Odds API event supplying event_id and team names. Main lines and every
    ladder rung, all periods. Totals: Bookmaker's home_* = Over, away_* = Under.
    """
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for api, bkg in matched:
        head = (now, "bookmaker", sport_key, api["id"], api["home_team"], api["away_team"],
                api.get("commence_time"), "bookmaker")

        def add(market, is_alt, side, point, odds):
            price = _american_str_to_decimal(odds)
            if price is not None:
                rows.append(head + (market, int(is_alt), side, point, price))

        for market, e in (bkg.get("markets") or {}).items():
            if market.startswith("totals"):
                add(market, False, "Over", e.get("home_point"), e.get("home_odds"))
                add(market, False, "Under", e.get("away_point"), e.get("away_odds"))
            else:
                add(market, False, api["away_team"], e.get("away_point"), e.get("away_odds"))
                add(market, False, api["home_team"], e.get("home_point"), e.get("home_odds"))
        for market, rungs in (bkg.get("alt_lines") or {}).items():
            for r in rungs:
                if market.startswith("totals"):
                    add(market, True, "Over", r.get("point"), r.get("over_odds"))
                    add(market, True, "Under", r.get("point"), r.get("under_odds"))
                else:
                    add(market, True, api["away_team"], r.get("away_point"), r.get("away_odds"))
                    add(market, True, api["home_team"], r.get("home_point"), r.get("home_odds"))
    return _insert_lines(rows)


def get_line_stats(sport_key: str = None) -> list:
    """Row/event counts per source and market — a quick read on what's been archived."""
    conn = _connect()
    q = """SELECT sport_key, source, market, is_alt, COUNT(*) AS n_rows,
                  COUNT(DISTINCT event_id) AS n_events, MAX(captured_at) AS last_capture
           FROM line_snapshots {} GROUP BY sport_key, source, market, is_alt
           ORDER BY sport_key, source, market, is_alt"""
    rows = (conn.execute(q.format("WHERE sport_key = ?"), (sport_key,)) if sport_key
            else conn.execute(q.format("")))
    out = [dict(r) for r in rows]
    conn.close()
    return out


# ── off-market detection ─────────────────────────────────────────────────────

def _fair_probs_for_market(mkt: dict):
    """No-vig fair probability per side, keyed by outcome name, for one book's market entry."""
    outcomes = mkt.get("outcomes") or []
    if len(outcomes) != 2:
        return None, None
    americans = [_american_from_decimal(oc.get("price")) for oc in outcomes]
    implieds = [_implied_prob(p) for p in americans]
    if any(ip is None for ip in implieds):
        return None, None
    fairs = _devig_shin(implieds[0], implieds[1])
    fair_by_side = {}
    point_by_side = {}
    for oc, fair in zip(outcomes, fairs):
        fair_by_side[oc.get("name")] = fair
        point_by_side[oc.get("name")] = oc.get("point")
    return fair_by_side, point_by_side


def find_off_market_lines(sport_key: str, raw_games: list) -> list:
    """
    Compare every recreational book's price (at BetOnline's own point) against
    BetOnline's no-vig fair probability. Returns qualifying lines (edge >= threshold).
    Only compares when the rec book's point exactly matches BetOnline's point for
    that market/side — cross-line (different point) discrepancies still only show
    up in the live +EV tab, which has the full key-number/ladder model.
    """
    results = []
    for game in raw_games or []:
        event_id = game.get("id")
        home = game.get("home_team")
        away = game.get("away_team")
        commence = game.get("commence_time")
        if not event_id:
            continue
        books_by_key = {bk.get("key"): bk for bk in game.get("bookmakers", [])}
        ref_book = books_by_key.get(REFERENCE_BOOK)
        if not ref_book:
            continue
        ref_markets = {m.get("key"): m for m in ref_book.get("markets", [])}

        for mkt_key in TRACKED_MARKETS:
            ref_mkt = ref_markets.get(mkt_key)
            if not ref_mkt:
                continue
            fair_by_side, point_by_side = _fair_probs_for_market(ref_mkt)
            if not fair_by_side:
                continue

            for book_key, bk in books_by_key.items():
                if book_key == REFERENCE_BOOK:
                    continue
                rec_mkt = next((m for m in bk.get("markets", []) if m.get("key") == mkt_key), None)
                if not rec_mkt:
                    continue
                if book_key == novig_filter.NOVIG_BOOK and (len(rec_mkt.get("outcomes") or []) != 2
                        or not novig_filter.pair_ok(*(oc.get("price") for oc in rec_mkt["outcomes"]))):
                    continue  # empty or stale NoVig order book — not a price you can get
                for oc in rec_mkt.get("outcomes", []):
                    side = oc.get("name")
                    if side not in fair_by_side:
                        continue
                    if mkt_key in ("spreads", "totals") and oc.get("point") != point_by_side.get(side):
                        continue  # different point than BetOnline — out of scope for this check
                    rec_price = oc.get("price")
                    rec_american = _american_from_decimal(rec_price)
                    rec_implied = _implied_prob(rec_american)
                    if rec_implied is None:
                        continue
                    edge = fair_by_side[side] - rec_implied
                    if edge >= ALERT_EDGE_THRESHOLD:
                        results.append({
                            "ev_pct": round((fair_by_side[side] / rec_implied - 1) * 100, 2),
                            "sport_key": sport_key,
                            "event_id": event_id,
                            "matchup": f"{away} @ {home}",
                            "commence_time": commence,
                            "book_key": book_key,
                            "market": mkt_key,
                            "side": side,
                            "point": oc.get("point"),
                            "price": rec_american,
                            "edge_pct": round(edge * 100, 2),
                        })
    return results


def _send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[telegram] not configured, would have sent: {text}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        print(f"[telegram] send failed: {e}")


MARKET_LABEL = {"h2h": "ML", "spreads": "Spread", "totals": "Total"}


def _fmt_price(p):
    return f"+{p}" if p is not None and p > 0 else str(p)


def _alert_message(row: dict) -> str:
    pt = f" {row['point']:+g}" if row.get("point") is not None else ""
    line = f"{row['side']}{pt}"
    return (
        f"🚨 Off-market line ({row['ev_pct']}% EV, {row['edge_pct']}pp edge)\n"
        f"{row['matchup']}\n"
        f"{MARKET_LABEL.get(row['market'], row['market'])}: {line} {_fmt_price(row['price'])} "
        f"({BOOK_DISPLAY.get(row['book_key'], row['book_key'])}) vs BetOnline fair line"
    )


# Populated by app.py via set_book_display() so messages use the app's display names.
BOOK_DISPLAY = {}


def set_book_display(display_map: dict):
    global BOOK_DISPLAY
    BOOK_DISPLAY = display_map


def compute_and_send_alerts(sport_key: str, raw_games: list) -> list:
    """Diff current off-market lines against active_alerts; alert on new ones, clear stale ones."""
    current = find_off_market_lines(sport_key, raw_games)
    now = datetime.now(timezone.utc).isoformat()
    current_by_key = {}
    for row in current:
        key = f"{row['event_id']}|{row['book_key']}|{row['market']}|{row['side']}|{row['point']}"
        current_by_key[key] = row

    conn = _connect()
    existing_keys = {r["alert_key"] for r in conn.execute(
        "SELECT alert_key FROM active_alerts WHERE sport_key = ?", (sport_key,)
    )}

    new_keys = set(current_by_key) - existing_keys
    stale_keys = existing_keys - set(current_by_key)

    for key in stale_keys:
        conn.execute("DELETE FROM active_alerts WHERE alert_key = ?", (key,))

    newly_alerted = []
    for key in new_keys:
        row = current_by_key[key]
        conn.execute(
            """INSERT INTO active_alerts
               (alert_key, sport_key, event_id, matchup, commence_time, book_key,
                market, side, point, price, edge_pct, first_seen, last_seen)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (key, sport_key, row["event_id"], row["matchup"], row["commence_time"],
             row["book_key"], row["market"], row["side"], row["point"], row["price"],
             row["edge_pct"], now, now),
        )
        newly_alerted.append(row)

    for key in current_by_key:
        if key not in new_keys:
            conn.execute("UPDATE active_alerts SET last_seen = ?, price = ?, edge_pct = ? WHERE alert_key = ?",
                         (now, current_by_key[key]["price"], current_by_key[key]["edge_pct"], key))

    conn.commit()
    conn.close()

    for row in newly_alerted:
        _send_telegram(_alert_message(row))

    return newly_alerted


def get_active_alerts(sport_key: str) -> list:
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM active_alerts WHERE sport_key = ? ORDER BY last_seen DESC", (sport_key,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_history(sport_key: str, event_id: str) -> list:
    conn = _connect()
    rows = conn.execute(
        """SELECT captured_at, book_key, market, side, point, price
           FROM snapshots WHERE sport_key = ? AND event_id = ?
           ORDER BY captured_at ASC""",
        (sport_key, event_id),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_tracked_events(sport_key: str) -> list:
    conn = _connect()
    rows = conn.execute(
        """SELECT DISTINCT event_id, home_team, away_team, commence_time
           FROM snapshots WHERE sport_key = ?
           ORDER BY commence_time ASC""",
        (sport_key,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
