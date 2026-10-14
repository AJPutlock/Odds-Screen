"""
SQLite storage for the CBB Live hub (forked from CFB Live).

Three kinds of data live here:

  games          - current state of every game we've seen today (upserted in place)
  game_states    - append-only time series of score/clock/situation per game
  odds_snapshots - append-only time series of live odds, one row per book/market/side

The time series tables are the point of the hub: a live scoreboard is cheap to
get from ESPN at any moment, but "where was this line when the score was 17-7
with 2:23 left in the 2nd" only exists if we recorded it. That history is what
the model layer (and any backtest of it) will eventually read.

Game IDs are ESPN event IDs, which are also CFBD's game IDs - so rows here join
directly to `NCAAB Live/ncaa_betting_app/espn_historical.db` (ESPN event id == CBBD sourceId).
"""
import os
import json
import sqlite3
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cbb_live.db")

# Path to the historical research DB (read-only joins; may not exist on a fresh box)
HISTORICAL_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "NCAAB Live", "ncaa_betting_app", "espn_historical.db",
)


def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    # WAL keeps the refresh loop's writes from blocking the Flask request threads.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS games (
    game_id         TEXT PRIMARY KEY,      -- ESPN event id == CFBD game_id
    season          INTEGER,
    week            INTEGER,
    start_time      TEXT,                  -- ISO8601 UTC
    state           TEXT,                  -- pre | in | post
    status_detail   TEXT,
    period          INTEGER,
    clock           TEXT,
    clock_seconds   INTEGER,
    elapsed_seconds INTEGER,
    home_id         TEXT,
    home_team       TEXT,
    home_abbr       TEXT,
    home_score      INTEGER,
    home_rank       INTEGER,
    home_record     TEXT,
    home_color      TEXT,
    home_logo       TEXT,
    away_id         TEXT,
    away_team       TEXT,
    away_abbr       TEXT,
    away_score      INTEGER,
    away_rank       INTEGER,
    away_record     TEXT,
    away_color      TEXT,
    away_logo       TEXT,
    home_linescores TEXT,                  -- JSON array of per-quarter points
    away_linescores TEXT,
    down            INTEGER,
    distance        INTEGER,
    yard_line       INTEGER,               -- absolute: 0 = HOME goal line, 100 = AWAY goal line
    down_distance_text TEXT,               -- ESPN's own formatting, e.g. "1st & 10 at ARST 25"
    possession_id   TEXT,
    is_red_zone     INTEGER DEFAULT 0,
    home_timeouts   INTEGER,
    away_timeouts   INTEGER,
    last_play       TEXT,
    espn_home_wp    REAL,                  -- ESPN's own win probability, as a cross-check
    situation_at    TEXT,                  -- when field position was last actually reported
    venue           TEXT,
    broadcast       TEXT,
    neutral_site    INTEGER DEFAULT 0,
    pbp_available   INTEGER,               -- ESPN's own flag: is play-by-play flowing?
    timeouts_trusted INTEGER DEFAULT 1,    -- 0 when ESPN's counter has floored and info is lost
    to_anchor_half  INTEGER,               -- half the anchor below was taken in
    to_anchor_home  INTEGER,               -- ESPN's counter at the start of that half
    to_anchor_away  INTEGER,
    odds_event_id   TEXT,                  -- matched The Odds API event id, if any
    updated_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_games_state ON games(state);
CREATE INDEX IF NOT EXISTS idx_games_start ON games(start_time);

CREATE TABLE IF NOT EXISTS game_states (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id         TEXT NOT NULL,
    captured_at     TEXT NOT NULL,
    period          INTEGER,
    clock_seconds   INTEGER,
    elapsed_seconds INTEGER,
    home_score      INTEGER,
    away_score      INTEGER,
    down            INTEGER,
    distance        INTEGER,
    yard_line       INTEGER,
    possession_id   TEXT,
    home_timeouts   INTEGER,
    away_timeouts   INTEGER,
    espn_home_wp    REAL,
    last_play       TEXT,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE INDEX IF NOT EXISTS idx_states_game ON game_states(game_id, captured_at);

CREATE TABLE IF NOT EXISTS odds_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id         TEXT,                  -- ESPN id once matched; NULL if unmatched
    odds_event_id   TEXT,
    captured_at     TEXT NOT NULL,
    book            TEXT NOT NULL,
    market          TEXT NOT NULL,         -- spreads | totals | h2h
    side            TEXT NOT NULL,         -- home | away | over | under
    point           REAL,
    price           INTEGER,
    book_updated_at TEXT,                  -- the book's own last_update from the API
    is_live         INTEGER DEFAULT 0,     -- captured after kickoff?
    -- Game state at capture time, denormalized so a backtest does not need a join
    period          INTEGER,
    clock_seconds   INTEGER,
    home_score      INTEGER,
    away_score      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_odds_game ON odds_snapshots(game_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_odds_book ON odds_snapshots(game_id, book, market, captured_at);
-- Serves the MAX(id) GROUP BY that get_latest_odds_all uses to build the board.
CREATE INDEX IF NOT EXISTS idx_odds_latest ON odds_snapshots(game_id, book, market, side, id);

CREATE TABLE IF NOT EXISTS refresh_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT,
    finished_at     TEXT,
    kind            TEXT,                  -- scoreboard | summary | odds
    games_touched   INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'ok',
    detail          TEXT
);
"""


# Columns added after the first release, applied to existing databases on boot.
_MIGRATIONS = [
    ("games", "down_distance_text", "TEXT"),
    ("games", "situation_at", "TEXT"),
    ("games", "pbp_available", "INTEGER"),
    ("games", "timeouts_trusted", "INTEGER"),
    ("games", "to_anchor_half", "INTEGER"),
    ("games", "to_anchor_home", "INTEGER"),
    ("games", "to_anchor_away", "INTEGER"),
]


def init_db():
    conn = get_connection()
    try:
        conn.executescript(SCHEMA)
        for table, column, coltype in _MIGRATIONS:
            existing = {r[1] for r in conn.execute("PRAGMA table_info({0})".format(table))}
            if column not in existing:
                conn.execute("ALTER TABLE {0} ADD COLUMN {1} {2}".format(table, column, coltype))
                logger.info("migrated: added %s.%s", table, column)
        conn.commit()
        logger.info("Database ready at %s", DB_PATH)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

_GAME_COLS = [
    "game_id", "season", "week", "start_time", "state", "status_detail",
    "period", "clock", "clock_seconds", "elapsed_seconds",
    "home_id", "home_team", "home_abbr", "home_score", "home_rank", "home_record",
    "home_color", "home_logo",
    "away_id", "away_team", "away_abbr", "away_score", "away_rank", "away_record",
    "away_color", "away_logo",
    "home_linescores", "away_linescores",
    "down", "distance", "yard_line", "down_distance_text", "possession_id", "is_red_zone",
    "home_timeouts", "away_timeouts", "last_play", "espn_home_wp", "situation_at",
    "venue", "broadcast", "neutral_site", "pbp_available", "timeouts_trusted",
    "to_anchor_half", "to_anchor_home", "to_anchor_away", "updated_at",
]

# Written only when ESPN actually reported field position. When it drops the
# situation block - which it does often, mid-drive, for most of the board - these
# keep their previous values so the drive marker shows the last known spot
# instead of going blank.
# Field position: preserved as-is when ESPN omits the situation block, because
# the ball has not moved as far as we know.
_SITUATION_COLS = [
    "down", "distance", "yard_line", "down_distance_text", "possession_id",
    "is_red_zone", "situation_at",
]

# Timeouts are NOT in that list on purpose. They cannot simply be preserved -
# they reset at the half and in overtime, so they are always written, either
# from ESPN or from carry_timeouts(). See the comment there for the live case
# that exposed this.
_TIMEOUT_COLS = ["home_timeouts", "away_timeouts"]


def upsert_games(games):
    """Insert or update the current state of each game. `games` are normalized dicts."""
    if not games:
        return 0
    now = datetime.now(timezone.utc).isoformat()

    def row_for(g):
        h, a, s = g["home"], g["away"], g.get("situation") or {}
        return (
            g["game_id"], g.get("season"), g.get("week"), g.get("start_time"),
            g.get("state"), g.get("status_detail"),
            g.get("period"), g.get("clock"), g.get("clock_seconds"), g.get("elapsed_seconds"),
            h.get("id"), h.get("name"), h.get("abbr"), h.get("score"), h.get("rank"),
            h.get("record"), h.get("color"), h.get("logo"),
            a.get("id"), a.get("name"), a.get("abbr"), a.get("score"), a.get("rank"),
            a.get("record"), a.get("color"), a.get("logo"),
            json.dumps(h.get("linescores") or []), json.dumps(a.get("linescores") or []),
            s.get("down"), s.get("distance"), s.get("yard_line"),
            s.get("down_distance_text"), s.get("possession_id"),
            1 if s.get("is_red_zone") else 0,
            s.get("home_timeouts"), s.get("away_timeouts"),
            s.get("last_play"), s.get("espn_home_wp"),
            now if s.get("present") else None,
            g.get("venue"), g.get("broadcast"), 1 if g.get("neutral_site") else 0,
            1 if g.get("play_by_play_available") else 0,
            0 if g.get("_timeouts_stale") else 1,
            g.get("_to_anchor_half"), g.get("_to_anchor_home"), g.get("_to_anchor_away"),
            now,
        )

    placeholders = ",".join(["?"] * len(_GAME_COLS))
    insert_head = "INSERT INTO games ({0}) VALUES ({1}) ON CONFLICT(game_id) DO UPDATE SET ".format(
        ",".join(_GAME_COLS), placeholders
    )

    # odds_event_id is deliberately absent from both update lists - it is written
    # by the odds matcher, and an ESPN refresh must not clobber a match already made.
    full_update = ",".join(
        "{0}=excluded.{0}".format(c) for c in _GAME_COLS if c != "game_id"
    )
    # A game whose situation ESPN omitted: refresh score/clock, leave position alone.
    partial_update = ",".join(
        "{0}=excluded.{0}".format(c)
        for c in _GAME_COLS
        if c != "game_id" and c not in _SITUATION_COLS
    )

    reported = [g for g in games if (g.get("situation") or {}).get("present")]
    unreported = [g for g in games if not (g.get("situation") or {}).get("present")]

    conn = get_connection()
    try:
        # One lookup of stored state for every game being written: used both to
        # carry timeouts half-aware when ESPN is silent, and to keep the
        # "ESPN counter is stale" verdict sticky once it has been made.
        ids = [g["game_id"] for g in games]
        prev = {}
        for chunk in (ids[i:i + 400] for i in range(0, len(ids), 400)):
            q = ("SELECT game_id, period, home_timeouts, away_timeouts, timeouts_trusted, "
                 "to_anchor_half, to_anchor_home, to_anchor_away "
                 "FROM games WHERE game_id IN ({0})").format(",".join(["?"] * len(chunk)))
            for r in conn.execute(q, tuple(chunk)):
                prev[r["game_id"]] = (r["period"], r["home_timeouts"], r["away_timeouts"],
                                      r["timeouts_trusted"], r["to_anchor_half"],
                                      r["to_anchor_home"], r["to_anchor_away"])

        for g in games:
            s = g.get("situation") or {}
            p = prev.get(g["game_id"])
            cur_half = half_of(g.get("period"))
            stored_half = p[4] if p else None
            a_home, a_away = (p[5], p[6]) if p else (None, None)

            if s.get("present"):
                # Re-anchor the moment the game enters a new half: ESPN will not
                # reset its counter, so we capture where it stands and count our
                # own way down from 3.
                if cur_half is not None and stored_half != cur_half:
                    a_home = s.get("home_timeouts")
                    a_away = s.get("away_timeouts")
                    stored_half = cur_half

                h = derive_timeouts(a_home, s.get("home_timeouts"))
                a = derive_timeouts(a_away, s.get("away_timeouts"))
                s["home_timeouts"], s["away_timeouts"] = h, a
                g["_timeouts_stale"] = (h is None or a is None)
            else:
                # ESPN silent: keep whatever verdict we already had.
                g["_timeouts_stale"] = bool(p and p[3] == 0)

            g["_to_anchor_half"] = stored_half
            g["_to_anchor_home"] = a_home
            g["_to_anchor_away"] = a_away

        if unreported:
            for g in unreported:
                p = prev.get(g["game_id"])
                if p:
                    h, a = carry_timeouts(p[0], g.get("period"), p[1], p[2])
                else:
                    h = a = None
                sit = g.setdefault("situation", {})
                sit["home_timeouts"], sit["away_timeouts"] = h, a

        if reported:
            conn.executemany(insert_head + full_update, [row_for(g) for g in reported])
        if unreported:
            conn.executemany(insert_head + partial_update, [row_for(g) for g in unreported])
        conn.commit()
        return len(games)
    finally:
        conn.close()


def record_game_states(games):
    """
    Append a time-series row for each in-progress game, but only when something
    actually changed since the last capture. Polling every 20s for four hours
    would otherwise write ~700 identical rows per game.
    """
    live = [g for g in games if g.get("state") == "in"]
    if not live:
        return 0

    conn = get_connection()
    try:
        inserted = 0
        now = datetime.now(timezone.utc).isoformat()
        for g in live:
            s = g.get("situation") or {}
            prev = conn.execute(
                "SELECT elapsed_seconds, home_score, away_score, down, distance, yard_line, "
                "possession_id, home_timeouts, away_timeouts, period "
                "FROM game_states WHERE game_id=? ORDER BY id DESC LIMIT 1",
                (g["game_id"],),
            ).fetchone()

            # When ESPN omits the situation block, carry the last known position
            # forward rather than recording a row full of nulls - otherwise the
            # history alternates between real positions and holes.
            if s.get("present"):
                down, distance, yard_line = s.get("down"), s.get("distance"), s.get("yard_line")
                poss = s.get("possession_id")
                h_to, a_to = s.get("home_timeouts"), s.get("away_timeouts")
            elif prev:
                down, distance, yard_line = prev[3], prev[4], prev[5]
                poss = prev[6]
                # Position carries forward as-is, but timeouts reset on a half
                # or overtime boundary - see carry_timeouts().
                h_to, a_to = carry_timeouts(prev[9], g.get("period"), prev[7], prev[8])
            else:
                down = distance = yard_line = poss = h_to = a_to = None

            fingerprint = (
                g.get("elapsed_seconds"), g["home"].get("score"), g["away"].get("score"),
                down, distance, yard_line,
            )
            if prev and tuple(prev[:6]) == fingerprint:
                continue

            conn.execute(
                "INSERT INTO game_states (game_id, captured_at, period, clock_seconds, "
                "elapsed_seconds, home_score, away_score, down, distance, yard_line, "
                "possession_id, home_timeouts, away_timeouts, espn_home_wp, last_play) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    g["game_id"], now, g.get("period"), g.get("clock_seconds"),
                    g.get("elapsed_seconds"), g["home"].get("score"), g["away"].get("score"),
                    down, distance, yard_line, poss, h_to, a_to,
                    s.get("espn_home_wp"), s.get("last_play"),
                ),
            )
            inserted += 1
        conn.commit()
        return inserted
    finally:
        conn.close()


def record_odds(rows):
    """Append odds snapshot rows. Each row is a dict matching odds_snapshots columns."""
    if not rows:
        return 0
    cols = [
        "game_id", "odds_event_id", "captured_at", "book", "market", "side",
        "point", "price", "book_updated_at", "is_live",
        "period", "clock_seconds", "home_score", "away_score",
    ]
    sql = "INSERT INTO odds_snapshots ({0}) VALUES ({1})".format(
        ",".join(cols), ",".join(["?"] * len(cols))
    )
    conn = get_connection()
    try:
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def set_odds_event_id(game_id, odds_event_id):
    conn = get_connection()
    try:
        conn.execute("UPDATE games SET odds_event_id=? WHERE game_id=?", (odds_event_id, game_id))
        conn.commit()
    finally:
        conn.close()


def log_refresh(kind, started_at, games_touched=0, status="ok", detail=None):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO refresh_log (started_at, finished_at, kind, games_touched, status, detail) "
            "VALUES (?,?,?,?,?,?)",
            (started_at, datetime.now(timezone.utc).isoformat(), kind, games_touched, status, detail),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def get_games(state=None):
    conn = get_connection()
    try:
        if state:
            cur = conn.execute("SELECT * FROM games WHERE state=? ORDER BY start_time", (state,))
        else:
            cur = conn.execute("SELECT * FROM games ORDER BY start_time")
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_game(game_id):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM games WHERE game_id=?", (game_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_latest_odds(game_id, books=None):
    """
    Most recent price per (book, market, side) for one game.

    `books` filters on read. Rows written under an older book configuration stay
    in the table (they are still valid history), but must not leak into a board
    that no longer includes those books.
    """
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT book, market, side, point, price, book_updated_at, captured_at, is_live "
            "FROM odds_snapshots o WHERE game_id=? AND id = ("
            "  SELECT MAX(id) FROM odds_snapshots "
            "  WHERE game_id=o.game_id AND book=o.book AND market=o.market AND side=o.side"
            ") ORDER BY market, book, side",
            (game_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        if books is not None:
            keep = set(books)
            rows = [r for r in rows if r["book"] in keep]
        return rows
    finally:
        conn.close()


def half_of(period):
    """1 = first half, 2 = second half, 3+ = overtime period number."""
    if period is None:
        return None
    if period <= 2:
        return 1
    if period <= 4:
        return 2
    return period - 2          # P5 -> OT1, P6 -> OT2, ...


def carry_timeouts(prev_period, new_period, prev_home, prev_away):
    """
    Decide what a carried-forward timeout count should be when ESPN is not
    reporting one.

    Carrying the last known value blindly is WRONG across a half boundary:
    college football resets each team to 3 timeouts at the start of the second
    half. Observed live on ISU @ BYU - BYU burned all three in the 1st half,
    the app froze 0 through halftime, and showed "0 timeouts" for the first six
    minutes of Q3 while BYU actually had all three (they then took three more
    at 8:39, 5:41 and 4:46, which the app never recorded either).

    Overtime resets too, and differently: in CFB each team gets ONE timeout per
    overtime period, and unused regulation timeouts do not carry over.

    Within the same half, carrying forward is correct and still desirable.
    """
    ph, nh = half_of(prev_period), half_of(new_period)
    if ph is None or nh is None or ph == nh:
        return prev_home, prev_away          # same half: carry as-is
    if nh >= 3:                              # entered an overtime period
        return 1, 1
    return 3, 3                              # entered a new half of regulation


# A team ALWAYS has 3 timeouts at the moment a half begins. Observing fewer
# within the first 30 seconds of game clock means ESPN's counter did not reset
# and is stale for the rest of the game.
_HALF_START_CLOCK = 14 * 60 + 30


def derive_timeouts(anchor, espn_now):
    """
    Recover true timeouts-remaining-this-half from ESPN's non-resetting counter.

    ESPN's counter behaves as `3 - (timeouts used in the WHOLE GAME)`, floored
    at 0, and never resets at the half. Verified across 36 games in one night:
    at Q3 15:00 - before a single second-half snap, when every team must hold
    exactly 3 - ESPN was reporting 0s and 1s.

    The counter is useless as an absolute, but its DECREMENTS are still real.
    So anchor on its value at the moment the half starts and count down from 3:

        used_this_half = anchor - espn_now
        remaining      = 3 - used_this_half

    This is exact while the counter still has headroom. Once it floors at 0 it
    stops decrementing, further timeouts become invisible, and the true count
    is genuinely unrecoverable - that case returns None so the UI can say so
    rather than guess. (BYU hit exactly this: anchored at 0 entering the second
    half, then took one more that the counter could not express.)

    Returns remaining (0-3) or None when unknowable.
    """
    if espn_now is None:
        return None
    if anchor is None:
        # No anchor yet (first half, or we joined mid-game): ESPN's own value is
        # the best available and is correct in the first half.
        return max(0, min(3, espn_now))
    if espn_now <= 0:
        return None                      # floored - information lost
    used = anchor - espn_now
    if used < 0:                         # counter went UP: ESPN reset properly
        return max(0, min(3, espn_now))
    return max(0, min(3, 3 - used))


def espn_timeouts_look_stale(period, clock_seconds, home_to, away_to):
    """
    True when ESPN's timeout counter is demonstrably wrong for this game.

    ESPN gives no trustworthy way to count timeouts:

      * `situation.homeTimeouts` / `awayTimeouts` is an absolute counter that
        does NOT reset at halftime. Caught live on ISU @ BYU: BYU ended the
        first half at 0, and ESPN still reported 0 deep into the 4th quarter
        while BYU actually had 2 (they had used one in the second half).

      * The play-by-play cannot be used instead. ESPN tags TV and media
        stoppages with the same `type id 21, "Timeout"` and the same
        `teamParticipants ... "timeout": true` as a charged team timeout. The
        same game logged EIGHT "Timeout BYU" plays in the first half alone,
        against a real limit of three. There is no field that separates them.

    So rather than publishing a confident wrong number, detect the failure: at
    the start of a half both teams must hold 3. Fewer than that in the opening
    30 seconds means the counter is stale, and the UI marks it unknown.
    """
    if period is None or clock_seconds is None:
        return False
    if period not in (1, 3):                 # only the opening of a half
        return False
    if clock_seconds < _HALF_START_CLOCK:
        return False
    for v in (home_to, away_to):
        if v is not None and v < 3:
            return True
    return False


def get_latest_odds_all(books=None):
    """
    Latest price per (game, book, market, side) for EVERY game, in one query.

    The per-game version runs a correlated subquery, which is fine for a single
    game page but catastrophic for the scoreboard: 75 games x 2 derived values
    meant 150 connections and 150 correlated scans of a table that grows all
    season. That made /api/scoreboard take ~8.7s against 43k rows, and it was
    getting slower every hour.

    Here the grouping happens once, and the rows are bucketed by game in Python.

    Returns {game_id: [row, ...]}.
    """
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT game_id, book, market, side, point, price, book_updated_at, "
            "       captured_at, is_live "
            "FROM odds_snapshots WHERE id IN ("
            "  SELECT MAX(id) FROM odds_snapshots WHERE game_id IS NOT NULL "
            "  GROUP BY game_id, book, market, side"
            ")"
        )
        keep = set(books) if books is not None else None
        out = {}
        for r in cur.fetchall():
            if keep is not None and r["book"] not in keep:
                continue
            out.setdefault(r["game_id"], []).append(dict(r))
        return out
    finally:
        conn.close()


def purge_books(keep_books):
    """
    Drop snapshot rows for books no longer tracked.

    Called at startup so a change to the book list takes effect on the existing
    database instead of leaving old books rendering on the board.
    """
    if not keep_books:
        return 0
    placeholders = ",".join(["?"] * len(keep_books))
    conn = get_connection()
    try:
        cur = conn.execute(
            "DELETE FROM odds_snapshots WHERE book NOT IN ({0})".format(placeholders),
            tuple(keep_books),
        )
        removed = cur.rowcount
        conn.commit()
        if removed:
            logger.info("purged %d odds rows from untracked books", removed)
        return removed
    finally:
        conn.close()


def get_odds_history(game_id, market=None, book=None):
    sql = "SELECT * FROM odds_snapshots WHERE game_id=?"
    params = [game_id]
    if market:
        sql += " AND market=?"
        params.append(market)
    if book:
        sql += " AND book=?"
        params.append(book)
    sql += " ORDER BY captured_at"
    conn = get_connection()
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def get_state_history(game_id):
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT * FROM game_states WHERE game_id=? ORDER BY captured_at", (game_id,)
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_db_summary():
    conn = get_connection()
    try:
        out = {}
        for t in ("games", "game_states", "odds_snapshots"):
            out[t] = conn.execute("SELECT COUNT(*) FROM " + t).fetchone()[0]
        out["by_state"] = {
            r[0]: r[1]
            for r in conn.execute("SELECT state, COUNT(*) FROM games GROUP BY state").fetchall()
        }
        return out
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    print(json.dumps(get_db_summary(), indent=2))
