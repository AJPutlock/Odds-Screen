"""
Matching The Odds API's events to ESPN's games.

ESPN is the spine of this app (its event id is also the CFBD game id), so odds
have to be attached to ESPN games rather than the other way round.

The important design choice: matching happens at the EVENT level, never
team-by-team. A team-level matcher that is "confident enough" on each side
independently can pair two entirely different games - the Odds Screen app hit
exactly this, matching Grambling State/TCU onto Ohio State/Texas through the
shared token "state". Here a candidate pairing must satisfy both sides at once,
clear a combined floor, and agree on kickoff time, before it is accepted.

Name differences between the two sources fall into three buckets:
  - punctuation/accents      "Louisiana Ragin' Cajuns" vs "Louisiana Ragin Cajuns"
  - dropped/added qualifiers "Nicholls Colonels"       vs "Nicholls State Colonels"
  - genuine abbreviations    "App State"               vs "Appalachian State"
The first is handled by folding, the second by token containment, and only the
third needs the alias table.
"""
import re
import logging
import unicodedata
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Genuine abbreviations that no amount of token math will bridge. Keys and values
# are both folded before use, so write them naturally.
ALIASES = {
    "app state": "appalachian state",
    "southern miss": "southern mississippi",
    "ul monroe": "louisiana monroe",
    "ul lafayette": "louisiana",
    "miami fl": "miami",
    "miami oh": "miami ohio",
    "ole miss": "mississippi",
    "usc": "southern california",
    "smu": "southern methodist",
    "tcu": "texas christian",
    "ucf": "central florida",
    "uconn": "connecticut",
    "utsa": "texas san antonio",
    "utep": "texas el paso",
    "unlv": "nevada las vegas",
    "fiu": "florida international",
    "fau": "florida atlantic",
    "umass": "massachusetts",
    "byu": "brigham young",
    "lsu": "louisiana state",
    "nc state": "north carolina state",
    "pitt": "pittsburgh",
    "hawai i": "hawaii",
    "san jose state": "san jose state",
}

# Words that carry no identifying signal on their own. "State" is deliberately
# NOT here - dropping it is what lets Ohio and Ohio State collapse together.
STOPWORDS = {"the", "of", "at", "university", "college"}


def fold(name):
    """Lowercase, strip accents and punctuation, collapse whitespace."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _expand_st(tokens):
    """
    "St." means State at the END of a school name and Saint at the START.

    Kalshi writes "Fresno St.", "Penn St.", "Iowa St."; ESPN writes "Fresno
    State Bulldogs". Folded naively those score 0.41 and fall under the match
    floor, which is why only 5 of 29 Kalshi games were pairing. Expanding a
    trailing "st" to "state" lifts them to a clean containment match.

    The first token is left alone so "St. Francis" / "St. John's" stay Saint.
    """
    if not tokens:
        return tokens
    out = list(tokens)
    for i in range(1, len(out)):
        if out[i] == "st":
            out[i] = "state"
    return out


def canonical(name):
    """Fold, then apply the alias table to the longest matching prefix."""
    s = fold(name)
    if not s:
        return ""
    if s in ALIASES:
        return ALIASES[s]
    # Alias the leading portion of a name, e.g. "app state mountaineers".
    for alias, full in ALIASES.items():
        if s == alias or s.startswith(alias + " "):
            return (full + s[len(alias):]).strip()
    return s


def tokens(name):
    return _expand_st([t for t in canonical(name).split() if t not in STOPWORDS])


def team_score(odds_name, espn_team):
    """
    Score one Odds API team name against one ESPN competitor, 0.0-1.0.

    `espn_team` is a normalized competitor dict from scrapers.espn.
    """
    candidates = [
        espn_team.get("name"),
        espn_team.get("location"),
        espn_team.get("short_name"),
    ]
    o_canon = canonical(odds_name)
    o_tokens = set(tokens(odds_name))
    if not o_tokens:
        return 0.0

    best = 0.0
    for cand in candidates:
        if not cand:
            continue
        c_canon = canonical(cand)
        if not c_canon:
            continue

        if o_canon == c_canon:
            return 1.0

        c_tokens = set(tokens(cand))
        if not c_tokens:
            continue

        # Containment: one name is the other plus a dropped qualifier, e.g.
        # "Nicholls Colonels" inside "Nicholls State Colonels".
        overlap = len(o_tokens & c_tokens)
        smaller = min(len(o_tokens), len(c_tokens))
        containment = overlap / smaller if smaller else 0.0
        jaccard = overlap / len(o_tokens | c_tokens)

        # Containment is the stronger signal but is easy to score 1.0 on a single
        # shared token, so it is blended with Jaccard rather than trusted alone.
        score = 0.65 * containment + 0.35 * jaccard
        if containment >= 1.0 and smaller >= 2:
            score = max(score, 0.90)
        best = max(best, score)

    return best


def _kickoff_delta_minutes(odds_event, espn_game):
    try:
        a = datetime.fromisoformat(str(odds_event["commence_time"]).replace("Z", "+00:00"))
        b = datetime.fromisoformat(str(espn_game["start_time"]).replace("Z", "+00:00"))
        if a.tzinfo is None:
            a = a.replace(tzinfo=timezone.utc)
        if b.tzinfo is None:
            b = b.replace(tzinfo=timezone.utc)
        return abs((a - b).total_seconds()) / 60.0
    except (KeyError, TypeError, ValueError):
        return None


# A pairing must clear all three of these to be accepted.
MIN_SIDE_SCORE = 0.60     # neither side may be a weak match
MIN_COMBINED = 1.50       # and together they must be strong
MAX_KICKOFF_DRIFT_MIN = 240.0


def match_events(odds_events, espn_games):
    """
    Pair Odds API events to ESPN games.

    Returns (matches, unmatched), where matches maps odds_event_id -> espn game_id
    and unmatched lists the odds events that found no confident home.

    Greedy by descending score with each ESPN game consumed at most once, so two
    similarly-named events cannot both claim the same game.
    """
    scored = []
    for oe in odds_events:
        for g in espn_games:
            # Odds API orients home/away the same way ESPN does; score that way
            # first, but also try the flip in case a neutral-site game disagrees.
            straight = (
                team_score(oe.get("home_team"), g["home"]),
                team_score(oe.get("away_team"), g["away"]),
            )
            flipped = (
                team_score(oe.get("home_team"), g["away"]),
                team_score(oe.get("away_team"), g["home"]),
            )
            h, a = straight if sum(straight) >= sum(flipped) else flipped
            if min(h, a) < MIN_SIDE_SCORE or (h + a) < MIN_COMBINED:
                continue

            drift = _kickoff_delta_minutes(oe, g)
            if drift is not None and drift > MAX_KICKOFF_DRIFT_MIN:
                continue

            # Prefer closer kickoffs among otherwise equal name matches.
            penalty = 0.0 if drift is None else min(drift / MAX_KICKOFF_DRIFT_MIN, 1.0) * 0.10
            scored.append((h + a - penalty, oe.get("id"), g["game_id"], h, a))

    scored.sort(reverse=True)
    matches, used_games, used_odds = {}, set(), set()
    for total, oid, gid, h, a in scored:
        if oid in used_odds or gid in used_games:
            continue
        matches[oid] = gid
        used_odds.add(oid)
        used_games.add(gid)

    unmatched = [
        {
            "id": oe.get("id"),
            "home_team": oe.get("home_team"),
            "away_team": oe.get("away_team"),
            "commence_time": oe.get("commence_time"),
        }
        for oe in odds_events
        if oe.get("id") not in matches
    ]
    if unmatched:
        logger.info("odds matcher: %d/%d matched, %d unmatched",
                    len(matches), len(odds_events), len(unmatched))
    return matches, unmatched
