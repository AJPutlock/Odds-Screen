"""
Orchestration: fetch lines, run the checker, record snapshots and flags.
"""

import time
from datetime import datetime, timedelta, timezone

import requests

from . import config, tracking
from .checker import check_event
from .sources import OddsApiClient, load_goalie_teams, load_sog_lines, parse_event_odds
from .teams import team_abbrev


def run_scan(client: OddsApiClient | None = None, hours_ahead: float = 36,
             record: bool = True) -> dict:
    """
    Check every NHL game starting in the next `hours_ahead` hours.

    Costs ~2 Odds API credits per game (2 markets × 1 bookmaker group).
    Returns {"rows", "flags", "errors", "remaining_requests", "scanned_at"}.
    """
    client = client or OddsApiClient(config.get_api_key())
    errors = []

    events = client.events(hours_ahead)
    teams = {team_abbrev(e[side]) for e in events for side in ("home_team", "away_team")}
    goalie_teams = load_goalie_teams({t for t in teams if t})
    sog_lines = load_sog_lines()

    rows = []
    for event in events:
        try:
            odds = client.event_odds(event["id"])
        except requests.RequestException as e:
            errors.append(f"{event.get('away_team')} @ {event.get('home_team')}: {e}")
            continue
        rows.extend(check_event(odds, parse_event_odds(odds), goalie_teams, sog_lines))

    new_flags = []
    if record:
        tracking.record_snapshots(rows)
        new_flags = tracking.record_flags(rows)

    rows.sort(key=lambda r: (r["commence_time"], r["game"], r["goalie"],
                             -(r["ev"] if r["ev"] is not None else -9)))
    return {
        "rows": rows,
        "flags": [r for r in rows if r["flag"]],
        "new_flags": new_flags,
        "errors": errors,
        "remaining_requests": client.remaining_requests,
        "scanned_at": datetime.now(timezone.utc).isoformat(),
    }


def watch_closing(interval_minutes: float = 10, client: OddsApiClient | None = None) -> None:
    """
    Loop forever, snapshotting each game's saves lines just before puck drop
    so CLV has a closing price. Only games starting before the next check are
    fetched, so each game costs one extra fetch (~2 credits). Ctrl+C to stop.
    """
    client = client or OddsApiClient(config.get_api_key())
    captured: set[str] = set()
    window = timedelta(minutes=interval_minutes + 5)
    print(f"[nhl_props] watching for puck drops every {interval_minutes} min (Ctrl+C to stop)")

    while True:
        try:
            now = datetime.now(timezone.utc)
            for event in client.events(hours_ahead=window.total_seconds() / 3600):
                starts = datetime.fromisoformat(event["commence_time"].replace("Z", "+00:00"))
                if event["id"] in captured or starts - now > window:
                    continue
                odds = client.event_odds(event["id"])
                rows = [{"event_id": odds["id"], "commence_time": odds["commence_time"],
                         "team": None, **offer}
                        for offer in parse_event_odds(odds)["saves"]]
                for r in rows:
                    r["over_dec"], r["under_dec"] = r.pop("over"), r.pop("under")
                tracking.record_snapshots(rows)
                captured.add(event["id"])
                print(f"[nhl_props] closing snapshot: {event['away_team']} @ "
                      f"{event['home_team']} ({len(rows)} lines)")
        except requests.RequestException as e:
            print(f"[nhl_props] closing capture failed, will retry: {e}")
        time.sleep(interval_minutes * 60)
