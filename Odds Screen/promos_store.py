"""
Saved sportsbook promos (data/bets.db, table promos), so boosts entered as they
arrive during the week persist and the screen can rank the best place to use
each one ("Best boosted options" in static/promos.js).

A promo: book, type ('profit' boost %, 'nosweat' refund as a bonus bet worth
refund_rate per $, 'bonus' bet of a fixed amount), the value (boost %),
max_stake, and eligibility: sports (list of sport keys, empty = any), markets
(base types 'h2h' / 'spreads' / 'totals', empty = any), periods ('full' /
'h1' / 'q1', empty = any), odds range (min_odds = "this or longer", e.g. -200;
max_odds optional), expiry, notes, bet_kind ('straight' / 'parlay' / 'any';
NULL = straight, the only kind before 10/10) and min_legs for parlays, where
min_odds applies to the whole parlay's price and min_leg_odds (10/11) to each
leg (books void a boosted parlay with a leg shorter than e.g. -200). Entered by hand on purpose: scraping logged-in
promo pages risks the account.
"""
import json
import threading
from datetime import datetime, timezone

import bets as bets_store

_lock = threading.Lock()
TYPES = ("profit", "nosweat", "bonus", "odds")
FIELDS = ("book_key", "type", "value", "max_stake", "refund_rate", "sports", "markets", "periods",
          "min_odds", "max_odds", "expires_at", "notes", "status", "title", "games", "bet_kind", "min_legs", "min_leg_odds")
KINDS = ("straight", "parlay", "any")
JSON_FIELDS = ("sports", "markets", "periods", "games")   # games: [{id, label, commence_time}], empty = any game


def init():
    with _lock, bets_store._connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS promos (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at  TEXT NOT NULL,
                title       TEXT,
                book_key    TEXT NOT NULL,
                type        TEXT NOT NULL,
                value       REAL,
                max_stake   REAL,
                refund_rate REAL,
                sports      TEXT,
                markets     TEXT,
                periods     TEXT,
                min_odds    INTEGER,
                max_odds    INTEGER,
                expires_at  TEXT,
                notes       TEXT,
                status      TEXT NOT NULL DEFAULT 'active'
            )""")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(promos)")}
        if "games" not in cols:      # 10/7: boosts limited to specific games
            conn.execute("ALTER TABLE promos ADD COLUMN games TEXT")
        if "bet_kind" not in cols:   # 10/10: parlay boosts (Add Boost wizard, parlay finder)
            conn.execute("ALTER TABLE promos ADD COLUMN bet_kind TEXT")
        if "min_legs" not in cols:
            conn.execute("ALTER TABLE promos ADD COLUMN min_legs INTEGER")
        if "min_leg_odds" not in cols:   # 10/11: shortest price allowed on any one leg
            conn.execute("ALTER TABLE promos ADD COLUMN min_leg_odds INTEGER")


init()


def _row(r):
    d = dict(r)
    for k in JSON_FIELDS:
        try:
            d[k] = json.loads(d[k]) if d.get(k) else []
        except ValueError:
            d[k] = []
    return d


def _clean(data):
    out = {k: data[k] for k in FIELDS if k in data}
    if "type" in out and out["type"] not in TYPES:
        raise ValueError(f"type must be one of {TYPES}")
    for k in ("value", "max_stake", "refund_rate"):
        if k in out:
            out[k] = None if out[k] in ("", None) else float(out[k])
    for k in ("min_odds", "max_odds", "min_leg_odds"):
        if k in out:
            out[k] = None if out[k] in ("", None) else int(str(out[k]).replace("+", ""))
    if "min_legs" in out:
        out["min_legs"] = None if out["min_legs"] in ("", None) else int(out["min_legs"])
    if out.get("bet_kind") is not None and out["bet_kind"] not in KINDS:
        raise ValueError(f"bet_kind must be one of {KINDS}")
    for k in JSON_FIELDS:
        if k in out:
            out[k] = json.dumps(list(out[k] or []))
    if "status" in out and out["status"] not in ("active", "used", "expired"):
        raise ValueError("status must be active | used | expired")
    return out


def list_promos(include_inactive=False):
    now = datetime.now(timezone.utc).isoformat()
    with bets_store._connect() as conn:
        rows = [_row(r) for r in conn.execute("SELECT * FROM promos ORDER BY id DESC")]
    for p in rows:   # past its expiry -> expired
        if p["status"] == "active" and p.get("expires_at") and p["expires_at"] < now:
            p["status"] = "expired"
    return rows if include_inactive else [p for p in rows if p["status"] == "active"]


def add_promo(data):
    row = _clean(data)
    if not row.get("book_key") or not row.get("type"):
        raise ValueError("book_key and type are required")
    row["created_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    row.setdefault("status", "active")
    with _lock, bets_store._connect() as conn:
        cur = conn.execute(f"INSERT INTO promos ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})", list(row.values()))
        return _row(conn.execute("SELECT * FROM promos WHERE id=?", (cur.lastrowid,)).fetchone())


def update_promo(pid, data):
    row = _clean(data)
    with _lock, bets_store._connect() as conn:
        if row:
            conn.execute(f"UPDATE promos SET {', '.join(f'{k}=?' for k in row)} WHERE id=?", [*row.values(), pid])
        r = conn.execute("SELECT * FROM promos WHERE id=?", (pid,)).fetchone()
        return _row(r) if r else None


def delete_promo(pid):
    with _lock, bets_store._connect() as conn:
        return conn.execute("DELETE FROM promos WHERE id=?", (pid,)).rowcount > 0
