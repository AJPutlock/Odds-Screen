"""
Bookmaker.eu HAR Importer
=========================
Reads a HAR file exported from Charles Proxy while browsing bookmaker.eu
and extracts game odds from the GetGameView JSON response.

Usage (from app.py):
    from scrapers.parse_bookmaker_har import games_from_bookmaker_har
    games = games_from_bookmaker_har("/path/to/session.har")

Output format matches fetch_bet365() — a list of game dicts with a
'markets' key containing h2h / spreads / totals (+ partials).
"""

import json
import base64
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

GAMEVIEW_PATH = "BetslipProxy.aspx/GetGameView"

# gpd string → Odds API market suffix
_GPD_SUFFIX: dict[str, str] = {
    "game":              "",
    "first half":        "_h1",
    "1st half":          "_h1",
    "first quarter":     "_q1",
    "1st quarter":       "_q1",
    "first period":      "_p1",
    "1st period":        "_p1",
    "first 5 innings":   "_1st_5_innings",
    "first five innings":"_1st_5_innings",
    "f5":                "_1st_5_innings",
    "1st inning":        "_1st_1_innings",
    "first inning":      "_1st_1_innings",
}


def _fmt(val) -> str | None:
    """Format an American odds value — adds '+' prefix for positive numbers."""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s == "0":
        return None
    try:
        if int(float(s)) > 0 and not s.startswith("+"):
            return "+" + s
    except (ValueError, TypeError):
        pass
    return s or None


def _main_line(lines: list) -> dict | None:
    """Return the principal line from a Derivatives.line array.

    Prefer the entry with s_ml == 1 (moneyline present).
    Fall back to index == '0' or '0' numeric.
    """
    for l in lines:
        if l.get("s_ml") == 1:
            return l
    for l in lines:
        if str(l.get("index", "")) == "0":
            return l
    return lines[0] if lines else None


def _parse_gameview(gameview_text: str) -> list:
    """Parse a single GetGameView JSON response body into game dicts."""
    try:
        data = json.loads(gameview_text)
    except json.JSONDecodeError as e:
        logger.warning(f"bookmaker HAR: JSON parse error: {e}")
        return []

    raw_games = data.get("GameView", {}).get("game", [])
    if not raw_games:
        logger.warning("bookmaker HAR: GameView.game array is empty")
        return []

    # ── Pass 1: collect full-game entries (gp == 0, vtm != htm) ─────────────
    parent_map: dict[str, dict] = {}   # idgm -> game dict (full-game only)

    for g in raw_games:
        vtm  = g.get("vtm", "")
        htm  = g.get("htm", "")
        gp   = str(g.get("gp", ""))
        idgm = str(g.get("idgm", ""))
        idgp = str(g.get("idgp", ""))

        # Skip team-total props (same team on both sides)
        if vtm == htm or not vtm or not htm:
            continue

        if gp != "0":
            continue   # periods handled in pass 2

        lines = g.get("Derivatives", {}).get("line", [])
        ml    = _main_line(lines)
        if not ml:
            continue

        # Build commence_time from gmdt + gmtm
        gmdt = g.get("gmdt", "")   # "20260402"
        gmtm = g.get("gmtm", "")   # "18:45:00"
        try:
            commence = datetime.strptime(
                f"{gmdt} {gmtm}", "%Y%m%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            commence = None

        markets: dict = {}

        # Full-game ML
        if ml.get("voddst") or ml.get("hoddst"):
            markets["h2h"] = {
                "away_odds":  _fmt(ml.get("voddst")),
                "home_odds":  _fmt(ml.get("hoddst")),
                "away_point": None,
                "home_point": None,
            }

        # Full-game spread
        if ml.get("vsprdt") or ml.get("hsprdt"):
            try:
                vsp = float(ml["vsprdt"]) if ml.get("vsprdt") else None
                hsp = float(ml["hsprdt"]) if ml.get("hsprdt") else None
            except (ValueError, TypeError):
                vsp = hsp = None
            markets["spreads"] = {
                "away_odds":  _fmt(ml.get("vsprdoddst")),
                "home_odds":  _fmt(ml.get("hsprdoddst")),
                "away_point": vsp,
                "home_point": hsp,
            }

        # Full-game totals
        if ml.get("ovt") or ml.get("unt"):
            try:
                ov = float(ml["ovt"]) if ml.get("ovt") else None
                un = float(ml["unt"]) if ml.get("unt") else None
            except (ValueError, TypeError):
                ov = un = None
            markets["totals"] = {
                "away_odds":  _fmt(ml.get("ovoddst")),
                "home_odds":  _fmt(ml.get("unoddst")),
                "away_point": ov,
                "home_point": un,
            }

        if not markets:
            continue

        parent_map[idgm] = {
            "away_team":     vtm,
            "home_team":     htm,
            "commence_time": commence,
            "markets":       markets,
            "_idgm":         idgm,
        }

    # ── Pass 2: merge period markets into parent games ───────────────────────
    for g in raw_games:
        vtm  = g.get("vtm", "")
        htm  = g.get("htm", "")
        gp   = str(g.get("gp", ""))
        idgp = str(g.get("idgp", ""))
        gpd  = g.get("gpd", "").lower().strip()

        if vtm == htm or not vtm or not htm:
            continue
        if gp == "0":
            continue

        suffix = _GPD_SUFFIX.get(gpd)
        if suffix is None:
            continue   # unknown / unneeded period (2nd half, individual innings, etc.)

        parent = parent_map.get(idgp)
        if parent is None:
            continue

        lines = g.get("Derivatives", {}).get("line", [])
        ml    = _main_line(lines)
        if not ml:
            continue

        # Period ML
        if ml.get("voddst") or ml.get("hoddst"):
            parent["markets"][f"h2h{suffix}"] = {
                "away_odds":  _fmt(ml.get("voddst")),
                "home_odds":  _fmt(ml.get("hoddst")),
                "away_point": None,
                "home_point": None,
            }

        # Period spread
        if ml.get("vsprdt") or ml.get("hsprdt"):
            try:
                vsp = float(ml["vsprdt"]) if ml.get("vsprdt") else None
                hsp = float(ml["hsprdt"]) if ml.get("hsprdt") else None
            except (ValueError, TypeError):
                vsp = hsp = None
            parent["markets"][f"spreads{suffix}"] = {
                "away_odds":  _fmt(ml.get("vsprdoddst")),
                "home_odds":  _fmt(ml.get("hsprdoddst")),
                "away_point": vsp,
                "home_point": hsp,
            }

        # Period totals
        if ml.get("ovt") or ml.get("unt"):
            try:
                ov = float(ml["ovt"]) if ml.get("ovt") else None
                un = float(ml["unt"]) if ml.get("unt") else None
            except (ValueError, TypeError):
                ov = un = None
            parent["markets"][f"totals{suffix}"] = {
                "away_odds":  _fmt(ml.get("ovoddst")),
                "home_odds":  _fmt(ml.get("unoddst")),
                "away_point": ov,
                "home_point": un,
            }

    return list(parent_map.values())


def games_from_bookmaker_har(har_path: str) -> list:
    """
    Parse a Charles-exported HAR file from bookmaker.eu and return a list of
    game dicts in the same format as fetch_bet365().
    """
    try:
        with open(har_path, encoding="utf-8") as f:
            har = json.load(f)
    except Exception as e:
        logger.error(f"bookmaker HAR: could not read {har_path!r}: {e}")
        return []

    entries = har.get("log", {}).get("entries", [])
    logger.info(f"bookmaker HAR: {len(entries)} total entries in {har_path!r}")

    all_games: list = []
    found = 0

    for entry in entries:
        url    = entry.get("request", {}).get("url", "")
        status = entry.get("response", {}).get("status", 0)
        if GAMEVIEW_PATH not in url or status != 200:
            continue

        content = entry.get("response", {}).get("content", {})
        text    = content.get("text", "")
        if content.get("encoding") == "base64" and text:
            try:
                text = base64.b64decode(text).decode("utf-8", errors="replace")
            except Exception:
                continue

        if not text:
            continue

        found += 1
        games = _parse_gameview(text)
        # Merge into all_games, keyed by (away, home) to deduplicate
        existing = {(g["away_team"], g["home_team"]): g for g in all_games}
        for g in games:
            key = (g["away_team"], g["home_team"])
            if key in existing:
                existing[key]["markets"].update(g["markets"])
            else:
                existing[key] = g
        all_games = list(existing.values())

    logger.info(
        f"bookmaker HAR: {found} GetGameView response(s), "
        f"{len(all_games)} unique games parsed"
    )
    return all_games
