"""
Player-prop line history capture.

Records a change-only time series of every book's posted line and prices for
each (event, player, market), so model output can later be scored against real
closing lines and real outcomes. Writes into the same SQLite file as
history_tracker so a prop quote can be joined to the contemporaneous game line
(the simulator needs the game total/spread as of the moment a prop was quoted).

Two deliberate choices:

- Change-only writes. A full hourly dump of an NFL slate is ~25k rows of which
  nearly all repeat the previous hour. Storing only transitions gives a true
  line-movement history — including price moves at an unchanged line, which is
  the signature of sharp money that hasn't yet moved the number.

- Tiered cadence by time-to-kickoff (CAPTURE_TIERS). Every event-market costs
  an Odds API call, and almost all backtest value sits near kickoff: the
  closing line is the scoreboard, and late moves are the news.

This module owns storage and scheduling only. Fetching lives in app.py, which
holds the API credentials and the response parser.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "odds_history.db"
DB_PATH.parent.mkdir(exist_ok=True)

# (hours_to_kickoff_below, min_minutes_between_captures)
CAPTURE_TIERS = [
    (2, 20),
    (6, 60),
    (24, 180),
    (48, 720),
]
MAX_HOURS_BEFORE_KICKOFF = 48


def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = _connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prop_snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at   TEXT NOT NULL,
            sport_key     TEXT NOT NULL,
            event_id      TEXT NOT NULL,
            home_team     TEXT,
            away_team     TEXT,
            commence_time TEXT,
            player_name   TEXT NOT NULL,
            market        TEXT NOT NULL,
            book_key      TEXT NOT NULL,
            line          REAL,
            over_odds     INTEGER,
            under_odds    INTEGER
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_prop_snap_key
        ON prop_snapshots (event_id, player_name, market, book_key, id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_prop_snap_sport_time
        ON prop_snapshots (sport_key, captured_at)
    """)

    # Every capture attempt, including ones that produced no changes — the
    # cadence scheduler can't use max(captured_at) from prop_snapshots because
    # a quiet event would look stale forever and get re-fetched every tick.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prop_capture_log (
            event_id    TEXT NOT NULL,
            sport_key   TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            rows_changed INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (event_id, captured_at)
        )
    """)

    # Graded actuals. Populated by a separate results step (nflverse et al.);
    # created here so the backtest schema is stable from day one.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prop_results (
            event_id     TEXT NOT NULL,
            player_name  TEXT NOT NULL,
            market       TEXT NOT NULL,
            actual_value REAL,
            played       INTEGER,
            graded_at    TEXT NOT NULL,
            source       TEXT,
            PRIMARY KEY (event_id, player_name, market)
        )
    """)

    # Closing line = last observation before kickoff, per book, and only for
    # games that have actually kicked off — the latest line on an upcoming game
    # is just the current line, not a close.
    #
    # julianday() rather than string comparison throughout: captured_at carries
    # a '+00:00' offset while the API's commence_time is 'Z'-suffixed, so these
    # are not lexicographically comparable.
    conn.execute("DROP VIEW IF EXISTS prop_closing_lines")
    conn.execute("""
        CREATE VIEW prop_closing_lines AS
        SELECT s.*
        FROM prop_snapshots s
        JOIN (
            SELECT MAX(id) AS mid
            FROM prop_snapshots
            WHERE commence_time IS NOT NULL
              AND julianday(captured_at) < julianday(commence_time)
              AND julianday(commence_time) < julianday('now')
            GROUP BY event_id, player_name, market, book_key
        ) m ON s.id = m.mid
    """)
    conn.commit()
    conn.close()


init_db()


def _to_american(odds):
    """Accept '+150' / '-110' / 150 / None from the app's prop rows."""
    if odds is None or odds == "":
        return None
    if isinstance(odds, (int, float)):
        return int(odds)
    try:
        return int(str(odds).replace("+", "").strip())
    except ValueError:
        return None


def _parse_iso(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


def _latest_state(conn, event_id):
    """Most recent (line, over, under) per (player, market, book) for one event."""
    rows = conn.execute("""
        SELECT s.player_name, s.market, s.book_key, s.line, s.over_odds, s.under_odds
        FROM prop_snapshots s
        JOIN (
            SELECT MAX(id) AS mid
            FROM prop_snapshots
            WHERE event_id = ?
            GROUP BY player_name, market, book_key
        ) m ON s.id = m.mid
    """, (event_id,)).fetchall()
    return {
        (r["player_name"], r["market"], r["book_key"]):
            (r["line"], r["over_odds"], r["under_odds"])
        for r in rows
    }


def capture_props(sport_key: str, rows: list, captured_at: str = None) -> dict:
    """
    Store the changed subset of one props pull.

    `rows` is the output of app.process_prop_event (one row per player/market,
    with a `books` dict). Returns {'changed': n, 'seen': n, 'events': n}.
    """
    now = captured_at or datetime.now(timezone.utc).isoformat()

    by_event = {}
    for row in rows or []:
        event_id = row.get("game_id")
        if not event_id:
            continue
        by_event.setdefault(event_id, []).append(row)
    if not by_event:
        return {"changed": 0, "seen": 0, "events": 0}

    conn = _connect()
    inserts, seen_total = [], 0

    for event_id, event_rows in by_event.items():
        previous = _latest_state(conn, event_id)
        observed = set()

        for row in event_rows:
            market = row.get("prop_market")
            player = row.get("player_name")
            if not market or not player:
                continue
            meta = (
                row.get("home_team"), row.get("away_team"), row.get("commence_time"),
            )
            for book_key, entry in (row.get("books") or {}).items():
                if not entry:
                    continue
                line = entry.get("line")
                if line is None:
                    continue
                over = _to_american(entry.get("over_odds"))
                under = _to_american(entry.get("under_odds"))
                key = (player, market, book_key)
                observed.add(key)
                seen_total += 1
                if previous.get(key) == (line, over, under):
                    continue
                inserts.append((
                    now, sport_key, event_id, meta[0], meta[1], meta[2],
                    player, market, book_key, line, over, under,
                ))

        # A book that previously had an offer and now doesn't has pulled the
        # market — usually news. Record the withdrawal once.
        for key, prev in previous.items():
            if key in observed or prev == (None, None, None):
                continue
            player, market, book_key = key
            sample = event_rows[0]
            inserts.append((
                now, sport_key, event_id, sample.get("home_team"),
                sample.get("away_team"), sample.get("commence_time"),
                player, market, book_key, None, None, None,
            ))

        conn.execute(
            """INSERT OR REPLACE INTO prop_capture_log
               (event_id, sport_key, captured_at, rows_changed) VALUES (?,?,?,?)""",
            (event_id, sport_key, now, 0),
        )

    if inserts:
        conn.executemany(
            """INSERT INTO prop_snapshots
               (captured_at, sport_key, event_id, home_team, away_team, commence_time,
                player_name, market, book_key, line, over_odds, under_odds)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            inserts,
        )
        changed_by_event = {}
        for ins in inserts:
            changed_by_event[ins[2]] = changed_by_event.get(ins[2], 0) + 1
        for event_id, n in changed_by_event.items():
            conn.execute(
                "UPDATE prop_capture_log SET rows_changed = ? WHERE event_id = ? AND captured_at = ?",
                (n, event_id, now),
            )

    conn.commit()
    conn.close()
    return {"changed": len(inserts), "seen": seen_total, "events": len(by_event)}


def events_due(sport_key: str, events: list, now: datetime = None) -> list:
    """
    Filter `events` (dicts with 'id' and 'commence_time') down to those whose
    time-to-kickoff tier says they're due for another capture.
    """
    now = now or datetime.now(timezone.utc)
    conn = _connect()
    last_by_event = {
        r["event_id"]: r["last_at"]
        for r in conn.execute(
            "SELECT event_id, MAX(captured_at) AS last_at FROM prop_capture_log "
            "WHERE sport_key = ? GROUP BY event_id",
            (sport_key,),
        )
    }
    conn.close()

    due = []
    for event in events or []:
        event_id = event.get("id")
        commence = _parse_iso(event.get("commence_time"))
        if not event_id or not commence:
            continue
        hours_out = (commence - now).total_seconds() / 3600.0
        if hours_out <= 0 or hours_out > MAX_HOURS_BEFORE_KICKOFF:
            continue

        interval_min = next(
            (mins for cutoff, mins in CAPTURE_TIERS if hours_out <= cutoff), None
        )
        if interval_min is None:
            continue

        last = _parse_iso(last_by_event.get(event_id))
        if last is None:
            due.append(event)
            continue
        if (now - last).total_seconds() / 60.0 >= interval_min:
            due.append(event)
    return due


def record_results(results: list, source: str = None) -> int:
    """Upsert graded actuals. Each item: event_id, player_name, market, actual_value, played."""
    if not results:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect()
    conn.executemany(
        """INSERT OR REPLACE INTO prop_results
           (event_id, player_name, market, actual_value, played, graded_at, source)
           VALUES (?,?,?,?,?,?,?)""",
        [(r["event_id"], r["player_name"], r["market"], r.get("actual_value"),
          1 if r.get("played", True) else 0, now, source) for r in results],
    )
    conn.commit()
    conn.close()
    return len(results)


def get_stats(sport_key: str = None) -> dict:
    """Coverage summary — what the backtest set currently holds."""
    conn = _connect()
    where, params = ("WHERE sport_key = ?", (sport_key,)) if sport_key else ("", ())

    row = conn.execute(f"""
        SELECT COUNT(*) AS snapshots,
               COUNT(DISTINCT event_id) AS events,
               COUNT(DISTINCT player_name || '|' || market) AS player_markets,
               MIN(captured_at) AS first_at,
               MAX(captured_at) AS last_at
        FROM prop_snapshots {where}
    """, params).fetchone()

    closing = conn.execute(f"""
        SELECT COUNT(*) AS n FROM prop_closing_lines {where}
    """, params).fetchone()

    graded = conn.execute("SELECT COUNT(*) AS n FROM prop_results").fetchone()

    recent = [dict(r) for r in conn.execute(f"""
        SELECT event_id, sport_key, MAX(captured_at) AS last_at,
               SUM(rows_changed) AS total_changes, COUNT(*) AS captures
        FROM prop_capture_log {where}
        GROUP BY event_id ORDER BY last_at DESC LIMIT 15
    """, params)]

    conn.close()
    return {
        "snapshots": row["snapshots"],
        "events": row["events"],
        "player_markets": row["player_markets"],
        "first_captured": row["first_at"],
        "last_captured": row["last_at"],
        "closing_lines": closing["n"],
        "graded_results": graded["n"],
        "recent_events": recent,
    }


def get_player_history(event_id: str, player_name: str, market: str) -> list:
    conn = _connect()
    rows = conn.execute("""
        SELECT captured_at, book_key, line, over_odds, under_odds
        FROM prop_snapshots
        WHERE event_id = ? AND player_name = ? AND market = ?
        ORDER BY captured_at ASC, book_key ASC
    """, (event_id, player_name, market)).fetchall()
    conn.close()
    return [dict(r) for r in rows]
