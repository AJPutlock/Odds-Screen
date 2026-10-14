"""
ESPN live feed for men's college basketball (forked from CFB Live).

Two endpoints, both public and unauthenticated:

  scoreboard  - ONE request returns every game for the day with score, clock,
                down/distance, yard line, possession, timeouts and red-zone flag.
                This is the cheap "poll everything" call, analogous to the NCAAB
                app's get_daily_games().

  summary     - per-game detail: full team/player box score, drive-by-drive,
                scoring plays, and ESPN's own win-probability series. Costs one
                request per game, so the caller decides which games deserve it.

Unlike stats.ncaa.org there is no Akamai bot wall here and no Selenium needed -
plain requests works. Throughput is still worth respecting (see REQUEST_DELAY),
but the "IP-level block" constraint that shaped the NCAAB scraper does not apply.

Event IDs returned here are the same integers CFBD uses as game_id, so live
games join straight onto the historical plays database.
"""
import time
import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball"

# groups=50 is Division I (verified 2026-10-14 against D1 scoreboards); None returns all divisions.
FBS_GROUP = "50"

REQUEST_DELAY = 0.20
TIMEOUT = 20

# Do NOT add a browser User-Agent here. site.api.espn.com returns 403 for
# Chrome-like UA strings and 200 for the default python-requests one - verified
# by isolating the header against the live endpoint. The usual "spoof a browser
# to avoid blocks" instinct breaks this scraper rather than helping it.
HEADERS = {
    "Accept": "application/json",
}

# Men's college basketball: two 20:00 halves, 5:00 overtimes. elapsed_seconds
# follows espn_historical.db's plays convention (OT continues past 2400).
HALF_SECONDS = 20 * 60
OT_SECONDS = 5 * 60
REGULATION_SECONDS = 2 * HALF_SECONDS


class ESPNError(Exception):
    pass


def _get(url, params=None):
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise ESPNError("request failed: {0}".format(e))
    if r.status_code != 200:
        raise ESPNError("HTTP {0} for {1}".format(r.status_code, r.url))
    try:
        return r.json()
    except ValueError as e:
        raise ESPNError("bad JSON: {0}".format(e))


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _clock_to_seconds(display_clock):
    """'2:23' -> 143. Returns None if unparseable."""
    if not display_clock:
        return None
    try:
        parts = str(display_clock).split(":")
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(float(parts[1]))
        return int(float(parts[0]))
    except (ValueError, TypeError):
        return None


def _elapsed_seconds(period, clock_seconds):
    """
    Seconds played. Matches the `elapsed_seconds` convention in
    NCAAB Live/espn_historical.db plays so live state compares to history.
    """
    if period is None or clock_seconds is None:
        return None
    if period >= 3:          # overtime periods
        return REGULATION_SECONDS + (period - 3) * OT_SECONDS + (OT_SECONDS - clock_seconds)
    return (period - 1) * HALF_SECONDS + (HALF_SECONDS - clock_seconds)


def _int_or_none(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _competitor(c):
    team = c.get("team") or {}
    rank = c.get("curatedRank") or {}
    rank_val = rank.get("current")
    # ESPN uses 99 to mean "unranked" rather than omitting the field.
    if rank_val in (0, 99):
        rank_val = None

    record = None
    for r in c.get("records") or []:
        # The 'total' record is the overall W-L; other entries are home/away/conf splits.
        if r.get("type") == "total" or r.get("name") == "overall":
            record = r.get("summary")
            break
    if record is None and c.get("records"):
        record = (c["records"][0] or {}).get("summary")

    return {
        "id": team.get("id"),
        "name": team.get("displayName"),
        "short_name": team.get("shortDisplayName"),
        "location": team.get("location"),
        "abbr": team.get("abbreviation"),
        "color": team.get("color"),
        "logo": team.get("logo"),
        "score": _int_or_none(c.get("score")),
        "rank": rank_val,
        "record": record,
        "linescores": [_int_or_none(ls.get("value")) for ls in (c.get("linescores") or [])],
    }


def _situation(comp, home_id):
    """
    Basketball in-game situation. No downs or field position: keeps the keys the
    football-era storage layer reads (all None) so the shared schema still works,
    and marks the block non-degenerate so the field-position repair never runs.
    """
    sit = comp.get("situation") or {}
    last = sit.get("lastPlay") or {}
    prob = last.get("probability") or {}
    try:
        home_wp = float(prob.get("homeWinPercentage")) if prob.get("homeWinPercentage") is not None else None
    except (TypeError, ValueError):
        home_wp = None
    poss = sit.get("possession")
    return {
        "present": bool(sit),
        "degenerate": False,
        "down": None,
        "distance": None,
        "yard_line": None,
        "down_distance_text": None,
        "possession_id": poss,
        "possession_is_home": (poss == home_id if poss else None),
        "is_red_zone": False,
        "home_timeouts": _int_or_none(sit.get("homeTimeouts")),
        "away_timeouts": _int_or_none(sit.get("awayTimeouts")),
        "last_play": last.get("text"),
        "last_play_type": (last.get("type") or {}).get("text"),
        "espn_home_wp": home_wp,
    }


def parse_event(event):
    """Turn one raw ESPN scoreboard event into the hub's normalized game dict."""
    comps = event.get("competitions") or []
    if not comps:
        return None
    comp = comps[0]

    home = away = None
    for c in comp.get("competitors") or []:
        if c.get("homeAway") == "home":
            home = _competitor(c)
        elif c.get("homeAway") == "away":
            away = _competitor(c)
    if not home or not away:
        return None

    status = event.get("status") or {}
    stype = status.get("type") or {}
    period = _int_or_none(status.get("period"))
    clock_seconds = _clock_to_seconds(status.get("displayClock"))

    broadcasts = []
    for b in comp.get("broadcasts") or []:
        broadcasts.extend(b.get("names") or [])

    venue = comp.get("venue") or {}
    addr = venue.get("address") or {}
    venue_str = venue.get("fullName")
    if venue_str and addr.get("city"):
        venue_str = "{0} - {1}, {2}".format(venue_str, addr.get("city"), addr.get("state") or "")
        venue_str = venue_str.rstrip(", ")

    state = stype.get("state")
    return {
        "game_id": str(event.get("id")),
        "season": (event.get("season") or {}).get("year"),
        "week": (event.get("week") or {}).get("number"),
        "start_time": event.get("date"),
        "state": state,
        "status_detail": stype.get("shortDetail") or stype.get("detail"),
        "completed": bool(stype.get("completed")),
        "period": period,
        "clock": status.get("displayClock"),
        "clock_seconds": clock_seconds,
        "elapsed_seconds": _elapsed_seconds(period, clock_seconds) if state == "in" else (
            REGULATION_SECONDS if state == "post" else 0
        ),
        "num_periods": period,
        "home": home,
        "away": away,
        "situation": _situation(comp, home.get("id")) if state == "in" else {},
        "venue": venue_str,
        "broadcast": ", ".join(broadcasts) if broadcasts else None,
        "neutral_site": bool(comp.get("neutralSite")),
        "play_by_play_available": bool(comp.get("playByPlayAvailable")),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_scoreboard(group=FBS_GROUP, date=None, limit=300):
    """
    One request covering every game for a day.

    `date` is a YYYYMMDD string; omit for today. Returns normalized game dicts.
    """
    params = {"limit": limit}
    if group:
        params["groups"] = group
    if date:
        params["dates"] = date

    data = _get(BASE + "/scoreboard", params=params)
    games = []
    for ev in data.get("events") or []:
        try:
            g = parse_event(ev)
            if g:
                games.append(g)
        except Exception as e:
            logger.warning("failed to parse event %s: %s", ev.get("id"), e)
    logger.info("scoreboard: %d games", len(games))
    return games


def spot_from_summary(summary):
    """
    Recover the true line of scrimmage from a game's drive data.

    Used when the scoreboard's situation block is degenerate (see `_situation`).
    Walks the plays newest-first and returns the end state of the last REAL
    scrimmage play, skipping non-snap events whose `end` is a placeholder:

        Punt     end: down 1, distance 10, yardLine 97   <- what we want
        Timeout  end: down 0, distance  2, yardLine 57   <- what ESPN shows

    Returns {down, distance, yard_line, team_id} or None.
    """
    drives = (summary or {}).get("drives") or []
    plays = []
    for d in drives:
        plays.extend(d.get("plays") or [])
    for p in reversed(plays):
        end = p.get("_end") or {}
        down = end.get("down")
        if down is not None and down >= 1:
            return {
                "down": down,
                "distance": end.get("distance"),
                "yard_line": end.get("yardLine"),
                "team_id": str(end.get("teamId")) if end.get("teamId") else None,
            }
    return None


def fetch_summary(game_id):
    """
    Per-game detail. Returns a dict with box score, drives, scoring plays and
    ESPN's win-probability series. One HTTP request.
    """
    time.sleep(REQUEST_DELAY)
    data = _get(BASE + "/summary", params={"event": game_id})
    return {
        "game_id": str(game_id),
        "boxscore": _parse_boxscore(data.get("boxscore") or {}),
        "drives": [],
        "plays": _parse_plays(data.get("plays") or []),
        "scoring_plays": _parse_scoring_plays(data.get("scoringPlays") or []),
        "win_probability": _parse_win_probability(data.get("winprobability") or []),
        "pregame_odds": _parse_pickcenter(data.get("pickcenter") or []),
        "leaders": data.get("leaders") or [],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def _parse_boxscore(box):
    """Team-level stats keyed by ESPN team id, plus raw player blocks."""
    teams = {}
    for t in box.get("teams") or []:
        team = t.get("team") or {}
        stats = {}
        for s in t.get("statistics") or []:
            key = s.get("name") or s.get("label")
            if key:
                stats[key] = s.get("displayValue")
        teams[str(team.get("id"))] = {
            "team_id": str(team.get("id")),
            "name": team.get("displayName"),
            "abbr": team.get("abbreviation"),
            "stats": stats,
        }
    return {"teams": teams, "players": box.get("players") or []}


def _parse_drives(drives):
    """Flatten ESPN's previous/current drive structure into one ordered list."""
    # `current` is normally ALSO the last entry in `previous`, just with fresher
    # play data - verified against a live game where both carried drive id
    # 40185668525. Appending it blindly renders the in-progress drive twice, so
    # dedupe by id and let the `current` copy win.
    items, seen = [], {}
    for d in list(drives.get("previous") or []):
        did = d.get("id")
        if did is not None and did in seen:
            items[seen[did]] = d
            continue
        if did is not None:
            seen[did] = len(items)
        items.append(d)

    current = drives.get("current")
    if current:
        cid = current.get("id")
        if cid is not None and cid in seen:
            items[seen[cid]] = current
        else:
            items.append(current)

    out = []
    for d in items:
        team = d.get("team") or {}
        start, end = d.get("start") or {}, d.get("end") or {}
        out.append({
            "id": d.get("id"),
            "team_id": str(team.get("id")) if team.get("id") else None,
            "team_abbr": team.get("abbreviation"),
            "description": d.get("description"),
            "result": d.get("displayResult") or d.get("result"),
            "is_score": bool(d.get("isScore")),
            "yards": d.get("yards"),
            "play_count": d.get("offensivePlays"),
            "time_elapsed": (d.get("timeElapsed") or {}).get("displayValue"),
            "start_period": (start.get("period") or {}).get("number"),
            "start_clock": (start.get("clock") or {}).get("displayValue"),
            "start_text": start.get("text"),
            "end_period": (end.get("period") or {}).get("number"),
            "end_clock": (end.get("clock") or {}).get("displayValue"),
            "end_text": end.get("text"),
            "plays": [
                {
                    "id": p.get("id"),
                    "text": p.get("text"),
                    "type": (p.get("type") or {}).get("text"),
                    "down": (p.get("start") or {}).get("down"),
                    "distance": (p.get("start") or {}).get("distance"),
                    "yard_line": (p.get("start") or {}).get("yardLine"),
                    # Kept raw so spot_from_summary can read the post-play spot;
                    # this is the only place the true live position survives
                    # when a timeout follows a punt or kickoff.
                    "_end": {
                        "down": (p.get("end") or {}).get("down"),
                        "distance": (p.get("end") or {}).get("distance"),
                        "yardLine": (p.get("end") or {}).get("yardLine"),
                        "teamId": ((p.get("end") or {}).get("team") or {}).get("id"),
                    },
                    "yards_gained": p.get("statYardage"),
                    "scoring_play": bool(p.get("scoringPlay")),
                    "clock": (p.get("clock") or {}).get("displayValue"),
                    "period": (p.get("period") or {}).get("number"),
                    "away_score": p.get("awayScore"),
                    "home_score": p.get("homeScore"),
                }
                for p in (d.get("plays") or [])
            ],
        })
    return out


def _parse_plays(plays):
    """Basketball play stream (no drives). Newest last; wallclock kept for odds alignment."""
    out = []
    for p in plays:
        team = p.get("team") or {}
        out.append({
            "id": p.get("id"),
            "seq": p.get("sequenceNumber"),
            "text": p.get("text"),
            "type": (p.get("type") or {}).get("text"),
            "team_id": str(team.get("id")) if team.get("id") else None,
            "period": (p.get("period") or {}).get("number"),
            "clock": (p.get("clock") or {}).get("displayValue"),
            "home_score": p.get("homeScore"),
            "away_score": p.get("awayScore"),
            "scoring_play": bool(p.get("scoringPlay")),
            "shooting_play": bool(p.get("shootingPlay")),
            "score_value": p.get("scoreValue"),
            "x": (p.get("coordinate") or {}).get("x"),
            "y": (p.get("coordinate") or {}).get("y"),
            "wallclock": p.get("wallclock"),
        })
    return out


def _parse_scoring_plays(plays):
    out = []
    for p in plays:
        team = p.get("team") or {}
        out.append({
            "id": p.get("id"),
            "team_id": str(team.get("id")) if team.get("id") else None,
            "team_abbr": team.get("abbreviation"),
            "type": (p.get("type") or {}).get("text"),
            "text": p.get("text"),
            "period": (p.get("period") or {}).get("number"),
            "clock": (p.get("clock") or {}).get("displayValue"),
            "away_score": p.get("awayScore"),
            "home_score": p.get("homeScore"),
            "scoring_type": (p.get("scoringType") or {}).get("displayName"),
        })
    return out


def _parse_win_probability(series):
    return [
        {
            "play_id": w.get("playId"),
            "home_wp": w.get("homeWinPercentage"),
            "tie_pct": w.get("tiePercentage"),
        }
        for w in series
    ]


def _parse_pickcenter(pc):
    """
    ESPN's embedded lines. NOTE: this is the PREGAME line, not a live one - it
    does not move during the game. Useful as an opening/closing reference only;
    live prices come from The Odds API.
    """
    out = []
    for p in pc:
        provider = p.get("provider") or {}
        out.append({
            "provider": provider.get("name"),
            "details": p.get("details"),
            "spread": p.get("spread"),
            "over_under": p.get("overUnder"),
            "over_odds": p.get("overOdds"),
            "under_odds": p.get("underOdds"),
            "home_ml": (p.get("homeTeamOdds") or {}).get("moneyLine"),
            "away_ml": (p.get("awayTeamOdds") or {}).get("moneyLine"),
            "home_spread_odds": (p.get("homeTeamOdds") or {}).get("spreadOdds"),
            "away_spread_odds": (p.get("awayTeamOdds") or {}).get("spreadOdds"),
        })
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    games = fetch_scoreboard()
    live = [g for g in games if g["state"] == "in"]
    print("{0} games, {1} live".format(len(games), len(live)))
    for g in live[:10]:
        sit = g["situation"]
        print("  {0:<18} {1:<22} {2}-{3}  {4}".format(
            g["away"]["abbr"] + " @ " + g["home"]["abbr"],
            g["status_detail"],
            g["away"]["score"], g["home"]["score"],
            sit.get("last_play") or "",
        ))
