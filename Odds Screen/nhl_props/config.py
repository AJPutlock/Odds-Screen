"""
Settings for the NHL saves checker. The model constants are rough starting
points — phase 2 should calibrate them from play-by-play data.
"""

import os
from pathlib import Path

# ── APIs ──────────────────────────────────────────────────────────────────────
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
NHL_API_BASE = "https://api-web.nhle.com/v1"
SPORT_KEY = "icehockey_nhl"
MARKETS = ["player_total_saves", "team_totals"]

# Up to 10 books costs the same as 1 (The Odds API bills per 10 bookmakers).
BOOKMAKERS = [
    "novig", "draftkings", "fanduel", "williamhill_us", "hardrockbet",
    "fanatics", "espnbet", "betmgm", "betrivers", "betonlineag",
]


def get_api_key() -> str:
    """ODDS_API_KEY env var first, else the key already configured in app.py."""
    key = os.environ.get("ODDS_API_KEY")
    if key:
        return key
    from app import API_KEY   # imported lazily: app.py pulls in Flask etc.
    return API_KEY


# ── Files ─────────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "nhl_saves"
SOG_LINES_CSV = DATA_DIR / "team_sog_lines.csv"          # you fill this in
GOALIE_OVERRIDES_CSV = DATA_DIR / "goalie_teams.csv"     # optional
SNAPSHOTS_CSV = DATA_DIR / "snapshots.csv"               # every saves line seen
FLAGS_CSV = DATA_DIR / "flags.csv"                       # every play flagged
CACHE_DIR = DATA_DIR / "cache"

# ── Model constants ───────────────────────────────────────────────────────────
# variance / mean for each count. 1.0 = Poisson. Only used to turn a line +
# price into an implied mean, so small errors here matter little.
SOG_DISPERSION = 1.2
SAVES_DISPERSION = 1.2
GOALS_DISPERSION = 1.0

# Shots the starter never sees because they got pulled (≈ pull rate × shots the
# relief goalie faces). Rough prior: ~6% of starts × ~10 shots.
PULLED_SHOTS_ADJ = 0.6

# Team goal totals usually include the shootout winner as a goal, but a
# shootout goal is not a shot on goal. ≈ P(shootout) × P(team wins it).
SHOOTOUT_GOAL_ADJ = 0.03

# How much to trust the cross-market implied saves over the saves market itself
# when pricing a bet. 1.0 = treat the SOG/goals markets as exactly right; 0.5 =
# assume the truth is halfway between. Raise it once CLV shows the gaps close
# toward the implied number.
MODEL_WEIGHT = 0.5

# ── Flag thresholds ───────────────────────────────────────────────────────────
MIN_EV = 0.03            # expected value per unit at the offered price
MIN_SAVES_EDGE = 0.5     # implied saves mean − market saves mean, toward the bet side
