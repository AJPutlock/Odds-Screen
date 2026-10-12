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
"""
from datetime import datetime, timezone

import circa_close
import history_tracker
import oddsio

SOURCE = "circa_hourly"
SPORTS = ("americanfootball_nfl", "americanfootball_ncaaf")
FULL_GAME = ("h2h", "spreads", "totals")


def _stored(sport_key, at) -> bool:
    conn = history_tracker._connect()
    try:
        return conn.execute("SELECT 1 FROM line_snapshots WHERE source=? AND sport_key=? AND captured_at=? LIMIT 1",
                            (SOURCE, sport_key, at)).fetchone() is not None
    finally:
        conn.close()


def capture(sport_key) -> int:
    """Store Circa's full-game main lines for this sport's upcoming games. Returns rows stored."""
    if sport_key not in SPORTS:
        return 0
    feed = oddsio.get(sport_key, "circa")
    if not feed or not feed.get("data"):
        return 0
    at = feed.get("last_updated") or datetime.now(timezone.utc).isoformat()
    if _stored(sport_key, at):          # feed not refreshed since the last capture
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
                                           {**g, "markets": mains, "alt_lines": {}}, at, SOURCE, "circa")
    n = history_tracker._insert_lines(rows)
    if n:
        print(f"[circa archive] {sport_key} — {n} line(s) across {len({r[3] for r in rows})} game(s), feed at {at}")
    return n
