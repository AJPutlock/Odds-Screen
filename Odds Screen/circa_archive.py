"""
Hourly archive of Circa's full-game main lines for football (10/12).

capture(sport_key)  Circa's current full-game h2h / spread / total main lines
            for every upcoming game in the odds-api.io feed, into
            odds_history.db line_snapshots: source 'circa_hourly', book 'circa',
            event_id 'oio:<odds-api.io id>', the same row shape as
            circa_close._game_rows (decimal prices, totals Over/Under).
            captured_at is the time odds-api.io's feed was fetched, not when
            this ran, so a row says how old Circa's price really was.

Why: index.html's 1H/Q1 full-game shift (fbPeriodFgAdj) was backtested with
the full-game CLOSE standing in for Circa's line — future information at 10h.
This archive gives Circa's line as of each Bookmaker pull, so the shift can be
re-tested the way it runs live (3h / 6h / 10h / 24h+) before NFL is turned on
or the "provisional" label comes off. Called from app.py's _hourly_capture_loop.

ON Sharp (10/12, user-approved): the same capture for odds-api.io's consensus
line, source 'onsharp_hourly', book 'onsharp', every sport the feed covers --
for the pre-game lead/lag question (does ON move before Circa / the books, or
after?). ON stays out of the sharp blend (its closes scored the same as
Circa's on 926 shared lines, with no extra signal in the disagreements).
capture_all() is the one hourly call: Circa's football, then ON everywhere.
ON is pulled fresh (oddsio.get_fresh) because most of its sports aren't on
screen, and a cached feed for those would be an hour old.
"""
from datetime import datetime, timezone

import circa_close
import history_tracker
import oddsio

SOURCE = "circa_hourly"
SPORTS = ("americanfootball_nfl", "americanfootball_ncaaf")
FULL_GAME = ("h2h", "spreads", "totals")
# book -> (source, sports, pull fresh)
BOOKS = {
    "circa":   (SOURCE, SPORTS, False),
    "onsharp": ("onsharp_hourly", tuple(oddsio.SPORTS), True),
}
_LABEL = {"circa": "circa archive", "onsharp": "ON Sharp archive"}


def _stored(sport_key, at, source=SOURCE) -> bool:
    conn = history_tracker._connect()
    try:
        return conn.execute("SELECT 1 FROM line_snapshots WHERE source=? AND sport_key=? AND captured_at=? LIMIT 1",
                            (source, sport_key, at)).fetchone() is not None
    finally:
        conn.close()


def capture(sport_key, book="circa") -> int:
    """Store the book's full-game main lines for this sport's upcoming games. Returns rows stored."""
    source, sports, fresh = BOOKS[book]
    if sport_key not in sports:
        return 0
    feed = oddsio.get_fresh(sport_key, book, max_age=300) if fresh else oddsio.get(sport_key, book)
    if not feed or not feed.get("data"):
        return 0
    at = feed.get("last_updated") or datetime.now(timezone.utc).isoformat()
    if _stored(sport_key, at, source):  # feed not refreshed since the last capture
        return 0
    now = datetime.now(timezone.utc)
    rows = []
    for g in feed["data"]:
        if g.get("_oddsio_id") is None:
            continue
        try:
            if datetime.fromisoformat(str(g.get("commence_time")).replace("Z", "+00:00")) <= now:
                continue
        except ValueError:
            continue
        mains = {k: v for k, v in (g.get("markets") or {}).items() if k in FULL_GAME}
        if mains:
            rows += circa_close._game_rows(sport_key, f"oio:{g['_oddsio_id']}",
                                           {**g, "markets": mains, "alt_lines": {}}, at, source, book)
    n = history_tracker._insert_lines(rows)
    if n:
        print(f"[{_LABEL[book]}] {sport_key} — {n} line(s) across {len({r[3] for r in rows})} game(s), feed at {at}")
    return n


def capture_all() -> dict:
    """The hourly call: every book in BOOKS, every sport it covers. One sport's
    failure never stops the rest. Returns {(book, sport_key): rows}."""
    out = {}
    for book, (_, sports, _) in BOOKS.items():
        for sk in sports:
            try:
                out[(book, sk)] = capture(sk, book)
            except Exception as e:
                print(f"[{_LABEL[book]}] {sk} — capture failed: {type(e).__name__}: {e}")
    return out
