"""
Build NFL Data/data/nfl_games.csv — results, quarter/half scores, and pregame
lines for every NFL game since 2015 — for tools/fit_nfl_outcomes.py.

- Results + lines: nflverse games.csv (github.com/nflverse/nfldata).
  spread_line = expected HOME margin (+ = home favored), total_line = total —
  the last line nflverse recorded before kickoff (updated through the week;
  the book isn't documented).
- Quarter scores: ESPN scoreboard linescores, one call per season-week,
  matched on nflverse's `espn` game id. Completed seasons are cached in
  NFL Data/data/linescores/ and never re-fetched; the current season always is.

2015 is the start because the extra point moved back that year, which changed
how often NFL games land on 1, 4, 7, 8 etc.

Usage: python tools/fetch_nfl_data.py
"""
import csv, json
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "NFL Data" / "data"
CACHE = OUT / "linescores"
GAMES_URL = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
ESPN = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={y}&seasontype={t}&week={w}&limit=100"
FIRST_SEASON = 2015


def get(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.content


def week_linescores(season, stype, week):
    out = {}
    for e in json.loads(get(ESPN.format(y=season, t=stype, w=week))).get("events", []):
        c = e["competitions"][0]
        if not c.get("status", {}).get("type", {}).get("completed"):
            continue
        q = {}
        for t in c["competitors"]:
            q[t["homeAway"]] = [int(l.get("value", 0) or 0) for l in t.get("linescores", [])]
        if len(q.get("home", [])) >= 4 and len(q.get("away", [])) >= 4:
            out[e["id"]] = q
    return out


def season_linescores(season, current):
    path = CACHE / f"{season}.json"
    if path.exists() and season < current:
        return json.loads(path.read_text())
    weeks = [(2, w) for w in range(1, 19)] + [(3, w) for w in range(1, 6)]
    merged = {}
    with ThreadPoolExecutor(6) as ex:
        for d in ex.map(lambda sw: week_linescores(season, *sw), weeks):
            merged.update(d)
    path.write_text(json.dumps(merged))
    return merged


def main():
    OUT.mkdir(parents=True, exist_ok=True); CACHE.mkdir(exist_ok=True)
    (OUT / "games.csv").write_bytes(get(GAMES_URL))
    games = [r for r in csv.DictReader(open(OUT / "games.csv", encoding="utf-8"))
             if int(r["season"]) >= FIRST_SEASON and r["home_score"] and r["away_score"]]
    today = date.today()
    current = today.year if today.month >= 8 else today.year - 1
    ls = {}
    for s in sorted({int(r["season"]) for r in games}):
        ls.update(season_linescores(s, current))
    rows, missing = [], 0
    for r in games:
        q = ls.get(r["espn"])
        if not q:
            missing += 1
            continue
        h, a = q["home"], q["away"]
        rows.append({"season": r["season"], "game_type": r["game_type"], "week": r["week"],
                     "home_team": r["home_team"], "away_team": r["away_team"],
                     "home_points": r["home_score"], "away_points": r["away_score"],
                     "home_1h": h[0] + h[1], "away_1h": a[0] + a[1], "home_q1": h[0], "away_q1": a[0],
                     "spread_line": r["spread_line"], "total_line": r["total_line"], "completed": "1"})
    with open(OUT / "nfl_games.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader(); w.writerows(rows)
    print(f"{len(rows)} games written to {OUT / 'nfl_games.csv'} ({missing} without ESPN linescores)")


if __name__ == "__main__":
    main()
