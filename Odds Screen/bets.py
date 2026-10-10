"""
Bet tracker store — every bet the user places, in data/bets.db.

Replaces the old write-only Google Sheet log: the odds screen reads open
positions back from here (same-number hedges greyed out, polish middles
warned, same-side stakes counted against Kelly) and /tracker shows P&L.

A bet is stored in the screen's own terms so it can be matched to a live row:
sport_key + event_id (Odds API id) + market key (spreads_h1, not "1H Spread")
+ side ('away' | 'home' | 'over' | 'under') + point on the bet's OWN axis
(the backed team's points; the total for Over/Under; NULL for moneylines).
Manual entries may carry no event_id; the screen then matches by team names.
"""
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "bets.db"
_lock = threading.Lock()

STATUSES = ("open", "won", "lost", "push", "void")

# Columns a client may set (id / created_at are the server's).
FIELDS = ("placed_at", "sport_key", "sport_label", "event_id", "away", "home", "commence_time",
          "market", "side", "team", "point", "odds", "book_key", "book", "stake",
          "fair_prob", "edge", "ev", "kelly_rec", "is_alt", "player", "source", "status",
          "profit", "notes", "graded_at", "graded_by", "espn_event_id", "result_detail",
          "close_odds", "close_point", "close_fair", "clv", "clv_points", "clv_source", "clv_at",
          "circa_close_odds", "circa_close_point", "circa_close_fair", "clv_circa", "clv_circa_source",
          "now_fair", "now_ev", "now_src", "now_at")

# Added after the first release; init_db adds them to an existing bets.db.
LATER_COLUMNS = {"graded_at": "TEXT", "graded_by": "TEXT", "espn_event_id": "TEXT", "result_detail": "TEXT",
                 "close_odds": "INTEGER", "close_point": "REAL", "close_fair": "REAL", "clv": "REAL",
                 "clv_points": "REAL", "clv_source": "TEXT", "clv_at": "TEXT",
                 # 10/8: CLV vs Circa's close (circa_close.py, odds-api.io closing lines)
                 "circa_close_odds": "INTEGER", "circa_close_point": "REAL", "circa_close_fair": "REAL",
                 "clv_circa": "REAL", "clv_circa_source": "TEXT",
                 # 10/10: EV now — the odds screen prices open bets at the latest odds
                 # (set_now_ev); the last read before kickoff stays.
                 "now_fair": "REAL", "now_ev": "REAL", "now_src": "TEXT", "now_at": "TEXT"}


def _connect():
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _lock, _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bets (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at    TEXT NOT NULL,
                placed_at     TEXT NOT NULL,
                sport_key     TEXT,
                sport_label   TEXT,
                event_id      TEXT,
                away          TEXT NOT NULL,
                home          TEXT NOT NULL,
                commence_time TEXT,
                market        TEXT NOT NULL,
                side          TEXT NOT NULL,
                team          TEXT,
                point         REAL,
                odds          INTEGER NOT NULL,
                book_key      TEXT,
                book          TEXT,
                stake         REAL NOT NULL,
                fair_prob     REAL,
                edge          REAL,
                ev            REAL,
                kelly_rec     REAL,
                is_alt        INTEGER NOT NULL DEFAULT 0,
                player        TEXT,
                source        TEXT NOT NULL DEFAULT 'screen',
                status        TEXT NOT NULL DEFAULT 'open',
                profit        REAL,
                notes         TEXT
            )
        """)
        have = {r[1] for r in conn.execute("PRAGMA table_info(bets)")}
        for col, typ in LATER_COLUMNS.items():
            if col not in have:
                conn.execute(f"ALTER TABLE bets ADD COLUMN {col} {typ}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bets_event ON bets (event_id, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bets_teams ON bets (away, home, status)")


init_db()


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def win_profit(odds: int, stake: float) -> float:
    return stake * (odds / 100 if odds > 0 else 100 / abs(odds))


def settle_profit(status: str, odds: int, stake: float):
    if status == "won":
        return round(win_profit(odds, stake), 2)
    if status == "lost":
        return round(-stake, 2)
    if status in ("push", "void"):
        return 0.0
    return None


def _clean(data: dict) -> dict:
    out = {k: data[k] for k in FIELDS if k in data}
    if "odds" in out:
        out["odds"] = int(str(out["odds"]).replace("+", ""))
    for k in ("stake", "point", "fair_prob", "edge", "ev", "kelly_rec", "profit"):
        if k in out and out[k] in ("", None):
            out[k] = None
        elif k in out:
            out[k] = float(out[k])
    if "is_alt" in out:
        out["is_alt"] = 1 if out["is_alt"] else 0
    if "status" in out and out["status"] not in STATUSES:
        raise ValueError(f"status must be one of {STATUSES}")
    if "side" in out and out["side"] not in ("away", "home", "over", "under"):
        raise ValueError("side must be away | home | over | under")
    return out


def add_bet(data: dict) -> dict:
    row = _clean(data)
    for k in ("away", "home", "market", "side", "odds", "stake"):
        if row.get(k) in (None, ""):
            raise ValueError(f"missing {k}")
    if not row["stake"] > 0:
        raise ValueError("stake must be > 0")
    row.setdefault("placed_at", _now())
    row.setdefault("status", "open")
    if row["status"] != "open" and row.get("profit") is None:
        row["profit"] = settle_profit(row["status"], row["odds"], row["stake"])
    row["created_at"] = _now()
    cols = ", ".join(row)
    with _lock, _connect() as conn:
        cur = conn.execute(f"INSERT INTO bets ({cols}) VALUES ({', '.join('?' * len(row))})", list(row.values()))
        return get_bet(cur.lastrowid, conn)


def get_bet(bet_id: int, conn=None):
    own = conn is None
    conn = conn or _connect()
    try:
        r = conn.execute("SELECT * FROM bets WHERE id = ?", (bet_id,)).fetchone()
        return dict(r) if r else None
    finally:
        if own:
            conn.close()


def update_bet(bet_id: int, data: dict):
    changes = _clean(data)
    with _lock, _connect() as conn:
        cur = conn.execute("SELECT * FROM bets WHERE id = ?", (bet_id,)).fetchone()
        if not cur:
            return None
        merged = {**dict(cur), **changes}
        # Re-derive profit whenever the result, price or stake changes (unless given).
        if "profit" not in changes and ({"status", "odds", "stake"} & set(changes)):
            changes["profit"] = settle_profit(merged["status"], merged["odds"], merged["stake"])
        if changes:
            sets = ", ".join(f"{k} = ?" for k in changes)
            conn.execute(f"UPDATE bets SET {sets} WHERE id = ?", [*changes.values(), bet_id])
        return get_bet(bet_id, conn)


def set_now_ev(items: list) -> int:
    """[{id, fair, ev, src}] from the odds screen -> now_* on open bets whose
    game hasn't started (so the last read before kickoff is what stays)."""
    now = _now()
    n = 0
    with _lock, _connect() as conn:
        for it in items:
            try:
                bid, fair, ev = int(it["id"]), float(it["fair"]), float(it["ev"])
            except (KeyError, TypeError, ValueError):
                continue
            if not (0 < fair < 1) or abs(ev) > 5:
                continue
            n += conn.execute(
                """UPDATE bets SET now_fair = ?, now_ev = ?, now_src = ?, now_at = ?
                   WHERE id = ? AND status = 'open' AND (commence_time IS NULL OR commence_time > ?)""",
                (round(fair, 5), round(ev, 5), str(it.get("src") or "")[:20] or None, now, bid, now)).rowcount
    return n


def delete_bet(bet_id: int) -> bool:
    with _lock, _connect() as conn:
        return conn.execute("DELETE FROM bets WHERE id = ?", (bet_id,)).rowcount > 0


def list_bets(status: str = None, since: str = None) -> list:
    q, args = "SELECT * FROM bets WHERE 1=1", []
    if status == "open":
        q += " AND status = 'open'"
    elif status == "settled":
        q += " AND status != 'open'"
    if since:
        q += " AND placed_at >= ?"
        args.append(since)
    q += " ORDER BY placed_at DESC, id DESC"
    with _connect() as conn:
        return [dict(r) for r in conn.execute(q, args)]
