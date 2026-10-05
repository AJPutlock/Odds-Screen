"""
Automatic bet grading from ESPN's public scoreboard (no key).

Every open bet whose game has started is matched to its ESPN game and graded
won / lost / push / void from the final score, so the tracker fills itself in.
Supported: NFL, college football, NHL (more sports: add to ESPN_SPORTS and
PERIODS).

Grading rules
  full game     final score incl. overtime (NHL: incl. shootout, which ESPN's
                final score already counts as the deciding goal, as books do)
  _h1           quarters 1+2          _h2  quarters 3+4 + overtime
  _q1.._q4      that quarter          _p1.._p3 (NHL) that period
  moneyline     two-way: a tie pushes. NHL PERIOD moneylines are left open with
                a note on a tie: some books settle those three-way.
  spreads       margin + the bet's points: > 0 win, < 0 loss, = 0 push
  totals        vs the bet's total:  over/under, equal pushes
  cancelled     void.  postponed: void once 48 h past the scheduled start
Bets the user settled by hand are never touched (only status 'open' is read).
Unmatched or ungradable bets stay open with the reason in result_detail.

Matching is EVENT level (both teams together + kickoff within 4 h), adapted
from CFB Live/team_match.py, which was built after a team-by-team matcher paired
Grambling State/TCU with Ohio State/Texas through the shared word "state".

ESPN gotcha (CFB Live, 2026-09-19): site.api.espn.com returns 403 to
Chrome-like User-Agents and 200 to the default python-requests one. Don't add
a browser UA.
"""
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

import bets as bets_store

ET = ZoneInfo("America/New_York")
ESPN = "https://site.api.espn.com/apis/site/v2/sports/{path}/scoreboard"
ESPN_SPORTS = {
    "americanfootball_nfl":   ("football/nfl", {}),
    "americanfootball_ncaaf": ("football/college-football", {"groups": "80", "limit": "500"}),   # FBS (incl. FBS-FCS games)
    "icehockey_nhl":          ("hockey/nhl", {}),
}
# Market suffix -> the periods it covers (1-based; "ot" = everything after regulation).
PERIODS = {
    "football": {"": None, "_h1": [1, 2], "_h2": [3, 4, "ot"], "_q1": [1], "_q2": [2], "_q3": [3], "_q4": [4]},
    "hockey":   {"": None, "_p1": [1], "_p2": [2], "_p3": [3]},
}
REGULATION = {"football": 4, "hockey": 3}
GRADE_AFTER = timedelta(hours=2)          # don't ask ESPN before a game could be over
POSTPONED_VOID_AFTER = timedelta(hours=48)
SCOREBOARD_TTL = 300                      # seconds a fetched board is reused within one process

_boards: dict = {}                        # (sport_key, yyyymmdd) -> (fetched_at, games)


# ── ESPN boards ──────────────────────────────────────────────────────────────

def _scoreboard(sport_key: str, day: str) -> list:
    hit = _boards.get((sport_key, day))
    if hit and time.time() - hit[0] < SCOREBOARD_TTL:
        return hit[1]
    path, extra = ESPN_SPORTS[sport_key]
    r = requests.get(ESPN.format(path=path), params={"dates": day, **extra}, timeout=20)
    r.raise_for_status()
    games = []
    for e in r.json().get("events", []):
        c = (e.get("competitions") or [{}])[0]
        st = (c.get("status") or {}).get("type") or {}
        teams = {}
        for t in c.get("competitors", []):
            team = t.get("team") or {}
            teams[t.get("homeAway")] = {
                "name": team.get("displayName"), "location": team.get("location"),
                "short_name": team.get("shortDisplayName"),
                "score": int(float(t["score"])) if t.get("score") not in (None, "") else None,
                "lines": [int(float(ls.get("value", 0))) for ls in t.get("linescores", [])],
            }
        if "home" in teams and "away" in teams:
            games.append({"id": e.get("id"), "start": e.get("date"), "status": st.get("name"),
                          "completed": bool(st.get("completed")), "detail": st.get("detail"),
                          "home": teams["home"], "away": teams["away"]})
    _boards[(sport_key, day)] = (time.time(), games)
    return games


# ── Team matching (event level) ──────────────────────────────────────────────

ALIASES = {
    "app state": "appalachian state", "southern miss": "southern mississippi",
    "ul monroe": "louisiana monroe", "ul lafayette": "louisiana", "miami fl": "miami",
    "miami oh": "miami ohio", "ole miss": "mississippi", "usc": "southern california",
    "smu": "southern methodist", "tcu": "texas christian", "ucf": "central florida",
    "uconn": "connecticut", "utsa": "texas san antonio", "utep": "texas el paso",
    "unlv": "nevada las vegas", "fiu": "florida international", "fau": "florida atlantic",
    "umass": "massachusetts", "byu": "brigham young", "lsu": "louisiana state",
    "nc state": "north carolina state", "pitt": "pittsburgh", "hawai i": "hawaii",
}
STOPWORDS = {"the", "of", "at", "university", "college"}   # NOT "state": Ohio vs Ohio State
MIN_SIDE, MIN_BOTH, MAX_DRIFT_MIN = 0.60, 1.50, 240.0


def _fold(name):
    s = unicodedata.normalize("NFKD", str(name or ""))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", s)).strip()


def _canon(name):
    s = _fold(name)
    if s in ALIASES:
        return ALIASES[s]
    for a, full in ALIASES.items():
        if s.startswith(a + " "):
            return (full + s[len(a):]).strip()
    return s


def _tokens(name):
    return {t for t in _canon(name).split() if t not in STOPWORDS}


def _team_score(name, espn_team):
    o_canon, o_tok = _canon(name), _tokens(name)
    if not o_tok:
        return 0.0
    best = 0.0
    for cand in (espn_team.get("name"), espn_team.get("location"), espn_team.get("short_name")):
        if not cand:
            continue
        if _canon(cand) == o_canon:
            return 1.0
        c_tok = _tokens(cand)
        if not c_tok:
            continue
        overlap = len(o_tok & c_tok)
        smaller = min(len(o_tok), len(c_tok))
        score = 0.65 * (overlap / smaller) + 0.35 * (overlap / len(o_tok | c_tok))
        if overlap == smaller and smaller >= 2:
            score = max(score, 0.90)
        best = max(best, score)
    return best


def _drift_min(bet, game):
    try:
        a = datetime.fromisoformat(str(bet["commence_time"]).replace("Z", "+00:00"))
        b = datetime.fromisoformat(str(game["start"]).replace("Z", "+00:00"))
        return abs((a - b).total_seconds()) / 60
    except (KeyError, TypeError, ValueError):
        return None


def find_game(bet, games):
    """(game, bet_home_is_espn_home) for the bet's game, or (None, None)."""
    best = None
    for g in games:
        straight = (_team_score(bet["home"], g["home"]), _team_score(bet["away"], g["away"]))
        flipped = (_team_score(bet["home"], g["away"]), _team_score(bet["away"], g["home"]))
        same = sum(straight) >= sum(flipped)
        h, a = straight if same else flipped
        if min(h, a) < MIN_SIDE or h + a < MIN_BOTH:
            continue
        drift = _drift_min(bet, g)
        if drift is not None and drift > MAX_DRIFT_MIN:
            continue
        score = h + a - (0 if drift is None else min(drift / MAX_DRIFT_MIN, 1) * 0.1)
        if not best or score > best[0]:
            best = (score, g, same)
    return (best[1], best[2]) if best else (None, None)


# ── Grading ──────────────────────────────────────────────────────────────────

def _family(sport_key):
    return "hockey" if sport_key.startswith("icehockey") else "football"


def _split_market(market):
    m = re.match(r"^(h2h|spreads|totals)(_[a-z0-9]+)?$", market or "")
    return (m.group(1), m.group(2) or "") if m else (None, None)


def _points(team, periods, family):
    if periods is None:
        return team["score"]
    reg, total = REGULATION[family], 0
    for p in periods:
        if p == "ot":
            total += sum(team["lines"][reg:])
        elif p - 1 < len(team["lines"]):
            total += team["lines"][p - 1]
        else:
            return None                    # that period has no score (yet)
    return total


def grade_one(bet, game, bet_home_is_espn_home):
    """('won'|'lost'|'push', detail) or (None, reason) when it can't be graded."""
    family = _family(bet["sport_key"])
    base, suffix = _split_market(bet["market"])
    if base is None or suffix not in PERIODS[family]:
        return None, f"market {bet['market']} isn't auto-graded"
    periods = PERIODS[family][suffix]
    home_t = game["home"] if bet_home_is_espn_home else game["away"]
    away_t = game["away"] if bet_home_is_espn_home else game["home"]
    hp, ap = _points(home_t, periods, family), _points(away_t, periods, family)
    if hp is None or ap is None:
        return None, "period score missing"
    label = {"": "Final", "_h1": "1H", "_h2": "2H"}.get(suffix, suffix[1:].upper())
    detail = f"{label}: {bet['away']} {ap}, {bet['home']} {hp}" + \
             (f" ({game['detail']})" if periods is None and game.get("detail") not in (None, "Final") else "")
    if base == "totals":
        if bet.get("point") is None or bet["side"] not in ("over", "under"):
            return None, "total bet without a line"
        t = hp + ap
        diff = (t - bet["point"]) if bet["side"] == "over" else (bet["point"] - t)
        return ("won" if diff > 0 else "lost" if diff < 0 else "push"), f"{detail} · total {t}"
    if bet["side"] not in ("away", "home"):
        return None, "team bet without a team side"
    mine, theirs = (ap, hp) if bet["side"] == "away" else (hp, ap)
    if base == "h2h":
        if mine == theirs:
            if family == "hockey" and suffix:
                return None, f"{detail} · period tie: check how the book settles it (some are three-way)"
            return "push", detail + " · tie"
        return ("won" if mine > theirs else "lost"), detail
    if bet.get("point") is None:
        return None, "spread bet without a line"
    v = mine - theirs + bet["point"]
    return ("won" if v > 0 else "lost" if v < 0 else "push"), detail


def grade_open_bets(now=None, dry_run=False) -> dict:
    """Grade every open bet that can be graded now. Returns a summary with one
    entry per bet looked at."""
    now = now or datetime.now(timezone.utc)
    results, errors = [], []
    for b in bets_store.list_bets("open"):
        if b.get("sport_key") not in ESPN_SPORTS:
            continue
        try:
            start = datetime.fromisoformat(str(b["commence_time"]).replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError):
            results.append({"id": b["id"], "graded": False, "why": "no start time"})
            continue
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if now - start < GRADE_AFTER:
            continue
        et_day = start.astimezone(ET).date()
        games = []
        try:
            for d in (et_day, et_day - timedelta(days=1), et_day + timedelta(days=1)):
                games += _scoreboard(b["sport_key"], d.strftime("%Y%m%d"))
        except requests.exceptions.RequestException as e:
            errors.append(f"ESPN {b['sport_key']}: {type(e).__name__}: {e}")
            continue
        game, oriented = find_game(b, games)
        if not game:
            why = "no ESPN game matches these teams and start time"
            results.append({"id": b["id"], "graded": False, "why": why})
            if not dry_run and b.get("result_detail") != why:
                bets_store.update_bet(b["id"], {"result_detail": why})
            continue
        status = game["status"] or ""
        if status in ("STATUS_CANCELED", "STATUS_ABANDONED", "STATUS_FORFEIT") or \
                (status == "STATUS_POSTPONED" and now - start > POSTPONED_VOID_AFTER):
            grade, detail = "void", f"ESPN: {game.get('detail') or status} (void)"
        elif not (game["completed"] and status.startswith("STATUS_FINAL")):
            results.append({"id": b["id"], "graded": False, "why": f"not final ({game.get('detail') or status})"})
            continue
        else:
            grade, detail = grade_one(b, game, oriented)
        if grade is None:
            results.append({"id": b["id"], "graded": False, "why": detail})
            if not dry_run and b.get("result_detail") != detail:
                bets_store.update_bet(b["id"], {"result_detail": detail, "espn_event_id": game["id"]})
            continue
        row = {"id": b["id"], "graded": True, "status": grade, "detail": detail,
               "label": f"{b['away']} @ {b['home']} {b['market']} {b['side']} {b.get('point') if b.get('point') is not None else ''}".strip(),
               "stake": b["stake"], "odds": b["odds"]}
        if not dry_run:
            upd = bets_store.update_bet(b["id"], {
                "status": grade, "graded_at": now.isoformat(timespec="seconds"), "graded_by": "espn",
                "espn_event_id": game["id"], "result_detail": detail})
            row["profit"] = upd.get("profit") if upd else None
        else:
            row["profit"] = bets_store.settle_profit(grade, b["odds"], b["stake"])
        results.append(row)
    graded = [r for r in results if r.get("graded")]
    return {"graded": len(graded), "looked_at": len(results), "errors": errors, "results": results,
            "profit": round(sum(r.get("profit") or 0 for r in graded), 2)}
