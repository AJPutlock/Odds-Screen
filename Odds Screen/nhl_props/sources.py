"""
Data inputs for the saves checker:

  * The Odds API  — goalie saves props (player_total_saves) and team goal
                    totals (team_totals), fetched per event.
  * NHL web API   — current rosters, to know which team each goalie plays for.
  * Local CSVs    — team shots-on-goal lines (The Odds API doesn't carry them)
                    and optional goalie -> team overrides.
"""

import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from . import config
from .odds_math import american_to_decimal
from .teams import normalize, team_abbrev

try:
    from zoneinfo import ZoneInfo
    _EASTERN = ZoneInfo("America/New_York")
except Exception:                      # e.g. Windows without the tzdata package
    _EASTERN = timezone(timedelta(hours=-5))


def game_date(commence_time_iso: str) -> str:
    """NHL slates are dated in Eastern time: '2026-10-08T00:00:00Z' -> '2026-10-07'."""
    utc = datetime.fromisoformat(commence_time_iso.replace("Z", "+00:00"))
    return utc.astimezone(_EASTERN).date().isoformat()


# ── The Odds API ──────────────────────────────────────────────────────────────

class OddsApiClient:
    def __init__(self, api_key: str, bookmakers: list[str] | None = None,
                 base_url: str = config.ODDS_API_BASE):
        self.api_key = api_key
        self.bookmakers = bookmakers or config.BOOKMAKERS
        self.base_url = base_url
        self.remaining_requests = None
        self.used_requests = None

    def _get(self, path: str, params: dict):
        resp = requests.get(f"{self.base_url}{path}",
                            params={"apiKey": self.api_key, **params}, timeout=15)
        if not resp.ok:
            raise requests.HTTPError(f"{resp.status_code} {path}: {resp.text[:300]}")
        self.remaining_requests = resp.headers.get("x-requests-remaining", self.remaining_requests)
        self.used_requests = resp.headers.get("x-requests-used", self.used_requests)
        return resp.json()

    def events(self, hours_ahead: float = 36) -> list[dict]:
        """Upcoming NHL events. The /events endpoint doesn't use quota."""
        now = datetime.now(timezone.utc)
        return self._get(f"/sports/{config.SPORT_KEY}/events", {
            "dateFormat": "iso",
            "commenceTimeFrom": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "commenceTimeTo": (now + timedelta(hours=hours_ahead)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    def event_odds(self, event_id: str) -> dict:
        """
        Saves props + team totals for one game.
        Costs (number of markets) x (bookmakers / 10, rounded up) credits.
        """
        return self._get(f"/sports/{config.SPORT_KEY}/events/{event_id}/odds", {
            "markets": ",".join(config.MARKETS),
            "oddsFormat": "decimal",
            "dateFormat": "iso",
            "bookmakers": ",".join(self.bookmakers),
        })


def parse_event_odds(event: dict) -> dict:
    """
    Flatten an Odds API event-odds response into:

      {"saves": [{goalie, book, line, over, under}, ...],
       "team_goals": {team_abbrev: [{book, line, over, under}, ...]}}

    Prices are decimal odds. Only two-way (over AND under) offers are kept,
    since we need both sides to strip the vig.
    """
    saves: dict[tuple, dict] = {}
    team_goals: dict[tuple, dict] = {}

    for bm in event.get("bookmakers", []):
        book = bm.get("key")
        for market in bm.get("markets", []):
            key = market.get("key")
            if key not in ("player_total_saves", "team_totals"):
                continue
            for oc in market.get("outcomes", []):
                side = (oc.get("name") or "").lower()
                who = oc.get("description") or ""
                line = oc.get("point")
                price = oc.get("price")
                if side not in ("over", "under") or line is None or not price:
                    continue
                if key == "player_total_saves":
                    slot = saves.setdefault((who, book, line),
                                            {"goalie": who, "book": book, "line": line})
                else:
                    team = team_abbrev(who)
                    if not team:
                        continue
                    slot = team_goals.setdefault((team, book, line),
                                                 {"team": team, "book": book, "line": line})
                slot[side] = float(price)

    goals_by_team: dict[str, list] = {}
    for offer in team_goals.values():
        if "over" in offer and "under" in offer:
            goals_by_team.setdefault(offer.pop("team"), []).append(offer)

    return {
        "saves": [o for o in saves.values() if "over" in o and "under" in o],
        "team_goals": goals_by_team,
    }


# ── Goalie -> team ────────────────────────────────────────────────────────────

def fetch_team_goalies(team: str) -> list[str]:
    """Goalies on a team's current NHL roster (public NHL web API, no key)."""
    resp = requests.get(f"{config.NHL_API_BASE}/roster/{team}/current", timeout=15)
    resp.raise_for_status()
    names = []
    for g in resp.json().get("goalies", []):
        first = (g.get("firstName") or {}).get("default", "")
        last = (g.get("lastName") or {}).get("default", "")
        names.append(f"{first} {last}".strip())
    return names


def load_goalie_teams(teams: set[str], cache_dir: Path = config.CACHE_DIR,
                      overrides_csv: Path = config.GOALIE_OVERRIDES_CSV) -> dict[str, str]:
    """
    {normalized goalie name: team abbrev} for the given teams.

    Rosters are cached per day so repeat scans don't re-hit the NHL API.
    Rows in goalie_teams.csv (goalie,team) win over the roster lookup — use it
    for call-ups, trades, or a name the two sources spell differently.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"goalies_{datetime.now(_EASTERN).date().isoformat()}.json"
    cached = json.loads(cache_file.read_text()) if cache_file.exists() else {}

    for team in sorted(teams - cached.keys()):
        try:
            cached[team] = fetch_team_goalies(team)
        except requests.RequestException as e:
            print(f"[nhl_props] roster lookup failed for {team}: {e}")
    cache_file.write_text(json.dumps(cached, indent=1))

    mapping = {normalize(name): team for team, names in cached.items() for name in names}

    if overrides_csv.exists():
        with open(overrides_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                team = team_abbrev(row.get("team", ""))
                if row.get("goalie") and team:
                    mapping[normalize(row["goalie"])] = team
    return mapping


# ── Team shots-on-goal lines (manual CSV) ────────────────────────────────────

def load_sog_lines(path: Path = config.SOG_LINES_CSV) -> dict[tuple[str, str], list[dict]]:
    """
    Read team shots-on-goal lines you've entered by hand.

    CSV columns: date,team,line,over,under,book
      date   game date in Eastern time (YYYY-MM-DD)
      team   the team TAKING the shots (abbreviation or full name)
      over / under  American odds; leave blank to assume -110 / -110

    Returns {(date, team_abbrev): [{book, line, over, under}, ...]} with
    decimal prices.
    """
    lines: dict[tuple[str, str], list[dict]] = {}
    if not path.exists():
        return lines
    with open(path, newline="", encoding="utf-8") as f:
        for row_num, row in enumerate(csv.DictReader(f), start=2):
            try:
                team = team_abbrev(row["team"])
                if not team:
                    raise ValueError(f"unknown team {row['team']!r}")
                lines.setdefault((row["date"].strip(), team), []).append({
                    "book": (row.get("book") or "manual").strip(),
                    "line": float(row["line"]),
                    "over": american_to_decimal(row.get("over") or -110),
                    "under": american_to_decimal(row.get("under") or -110),
                })
            except (KeyError, ValueError) as e:
                print(f"[nhl_props] skipping {path.name} row {row_num}: {e}")
    return lines
