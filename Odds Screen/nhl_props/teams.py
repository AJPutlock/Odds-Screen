"""
NHL team names <-> abbreviations, plus name normalisation shared by goalie and
team matching (The Odds API uses full names, the NHL API uses abbreviations).
"""

import re
import unicodedata

TEAM_ABBREVS = {
    "Anaheim Ducks": "ANA",
    "Boston Bruins": "BOS",
    "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY",
    "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ",
    "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET",
    "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA",
    "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN",
    "Montreal Canadiens": "MTL",
    "Nashville Predators": "NSH",
    "New Jersey Devils": "NJD",
    "New York Islanders": "NYI",
    "New York Rangers": "NYR",
    "Ottawa Senators": "OTT",
    "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT",
    "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA",
    "St Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL",
    "Toronto Maple Leafs": "TOR",
    "Utah Mammoth": "UTA",
    "Utah Hockey Club": "UTA",
    "Vancouver Canucks": "VAN",
    "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH",
    "Winnipeg Jets": "WPG",
}


def normalize(name: str) -> str:
    """'Montréal Canadiens' -> 'montreal canadiens', 'St. Louis' -> 'st louis'."""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    ascii_name = re.sub(r"[^a-z0-9 ]", " ", ascii_name.lower())
    return " ".join(ascii_name.split())


_BY_NORMALIZED = {normalize(full): abbr for full, abbr in TEAM_ABBREVS.items()}
_VALID_ABBREVS = set(TEAM_ABBREVS.values())


def team_abbrev(name: str) -> str | None:
    """Accepts a full name ('New York Rangers') or an abbreviation ('NYR')."""
    if not name:
        return None
    if name.strip().upper() in _VALID_ABBREVS:
        return name.strip().upper()
    return _BY_NORMALIZED.get(normalize(name))
