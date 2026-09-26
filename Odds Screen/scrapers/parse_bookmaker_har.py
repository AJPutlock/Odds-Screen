"""
Bookmaker.eu HAR Importer
=========================
Reads a HAR file exported from Charles Proxy while browsing bookmaker.eu
and extracts game odds from the GetGameView JSON response.

Usage (from app.py):
    from scrapers.parse_bookmaker_har import games_from_bookmaker_har, props_from_bookmaker_har
    games = games_from_bookmaker_har("/path/to/session.har", sport_key="baseball_mlb")
    props = props_from_bookmaker_har("/path/to/session.har", sport_key="baseball_mlb")

Output format:
  games_from_bookmaker_har → list of game dicts with a 'markets' key
  props_from_bookmaker_har → {market_key: {player_name: {over_odds, under_odds, line}}}
"""

import re
import json
import base64
import logging
import unicodedata
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

GAMEVIEW_PATH = "BetslipProxy.aspx/GetGameView"
# Bookmaker.eu's game-lines listing page now loads odds (including the full
# alt-line derivatives ladder) via GetSchedule instead of/in addition to
# GetGameView — verified against a real Charles capture (2026-09) that had
# zero GetGameView calls but a full Derivatives.line[] ladder sitting in every
# GetSchedule response. Per-game field names are identical between the two
# (vtm/htm/Derivatives.line/gpd/etc.); only the top-level nesting differs —
# see _extract_raw_games().
SCHEDULE_PATH = "BetslipProxy.aspx/GetSchedule"


def _extract_raw_games(data: dict) -> list:
    """Bookmaker.eu has used at least two response shapes carrying the same
    per-game fields:
      - GetGameView: {"GameView": {"game": [...]}}                       (flat)
      - GetSchedule: {"Schedule": {"Data": {"Leagues": {"League": [
            {"dateGroup": [{"game": [...]}, ...]}, ...
        ]}}}}                                                            (nested)
    Flatten either shape into one plain list of game dicts.
    """
    flat = data.get("GameView", {}).get("game", [])
    if flat:
        return flat
    games: list = []
    leagues = (((data.get("Schedule") or {}).get("Data") or {}).get("Leagues") or {}).get("League") or []
    for league in leagues:
        for dg in league.get("dateGroup", []) or []:
            games.extend(dg.get("game", []) or [])
    return games

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
    "inning 1":          "_1st_1_innings",   # Bookmaker.eu gpd literal
}

# prop-result keywords that appear as vtm/htm in player-prop game entries
_PROP_JUNK = frozenset({"yes", "no", "over", "under", "odd", "even"})


def _is_team_name(s: str) -> bool:
    """Return False for known prop-result values and entries containing digits."""
    if not s:
        return False
    if s.lower().strip() in _PROP_JUNK:
        return False
    if any(c.isdigit() for c in s):   # "Over 1.5 Runs", "Under 8.5 K's"
        return False
    return True


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


def _parse_gameview(gameview_text: str, sport_key: str = "") -> list:
    """Parse a single GetGameView/GetSchedule JSON response body into game
    dicts. Convenience wrapper around _parse_raw_games for the (rare, now
    that bookmaker.eu splits periods across separate tab requests — see
    games_from_bookmaker_har) case where one response is self-contained."""
    try:
        data = json.loads(gameview_text)
    except json.JSONDecodeError as e:
        logger.warning(f"bookmaker HAR: JSON parse error: {e}")
        return []
    return _parse_raw_games(_extract_raw_games(data), sport_key=sport_key)


def _parse_raw_games(raw_games: list, sport_key: str = "") -> list:
    """Core Pass 1 / Pass 2 parent-child resolution over an already-flattened
    list of raw game dicts. Bookmaker.eu's site now spreads full-game and
    period (half/quarter) odds across SEPARATE GetSchedule calls — one per
    tab the user visited — rather than one self-contained response, so the
    caller must accumulate raw games from every matching HAR entry into one
    list FIRST and call this once; running Pass 1/Pass 2 separately per
    response (the original design) silently drops every period, because a
    half/quarter-only response has no gp=='0' entries to seed parent_map with
    at all, so idgp lookups in Pass 2 never resolve to anything.
    """
    if not raw_games:
        logger.warning("bookmaker HAR: no games found in GameView or Schedule response")
        return []

    is_baseball = "baseball" in sport_key.lower()

    # ── Pass 1: collect full-game entries (gp == 0, vtm != htm) ─────────────
    parent_map: dict[str, dict] = {}   # idgm -> game dict (full-game only)

    for g in raw_games:
        vtm  = g.get("vtm", "")
        htm  = g.get("htm", "")
        gp   = str(g.get("gp", ""))
        idgm = str(g.get("idgm", ""))
        idgp = str(g.get("idgp", ""))

        # Skip prop entries: same team on both sides, player names, or prop keywords
        if vtm == htm or not _is_team_name(vtm) or not _is_team_name(htm):
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
        alt_spreads_full: list = []
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

            # Collect alternate spread lines (all non-main lines that have spread data)
            for ln in lines:
                if ln is ml:
                    continue
                try:
                    alp = float(ln["vsprdt"]) if ln.get("vsprdt") else None
                    hlp = float(ln["hsprdt"]) if ln.get("hsprdt") else None
                except (ValueError, TypeError):
                    alp = hlp = None
                if alp is None and hlp is None:
                    continue
                ao = _fmt(ln.get("vsprdoddst"))
                ho = _fmt(ln.get("hsprdoddst"))
                if ao or ho:
                    alt_spreads_full.append({
                        "away_point": alp,
                        "home_point": hlp,
                        "away_odds":  ao,
                        "home_odds":  ho,
                    })
            alt_spreads_full.sort(key=lambda x: (x["away_point"] if x["away_point"] is not None else 0))

        # Full-game totals  (home = Over, away = Under — matches Odds API convention)
        alt_totals_full: list = []
        if ml.get("ovt") or ml.get("unt"):
            try:
                ov = float(ml["ovt"]) if ml.get("ovt") else None
                un = float(ml["unt"]) if ml.get("unt") else None
            except (ValueError, TypeError):
                ov = un = None
            markets["totals"] = {
                "home_odds":  _fmt(ml.get("ovoddst")),  # Over  → home
                "away_odds":  _fmt(ml.get("unoddst")),  # Under → away
                "home_point": ov,                        # Over line  → home_point
                "away_point": un,                        # Under line → away_point
            }

            # Collect all alternate totals lines (every line except the main one)
            for ln in lines:
                if ln is ml:
                    continue
                try:
                    pt = float(ln["ovt"]) if ln.get("ovt") else None
                except (ValueError, TypeError):
                    pt = None
                if pt is None:
                    continue
                oo = _fmt(ln.get("ovoddst"))
                uo = _fmt(ln.get("unoddst"))
                if oo or uo:
                    alt_totals_full.append({
                        "point":      pt,
                        "over_odds":  oo,
                        "under_odds": uo,
                    })
            alt_totals_full.sort(key=lambda x: x["point"])

        if not markets:
            continue

        # Unified alt_lines dict holds both spreads and totals alt lines
        alt_lines_dict: dict = {}
        if alt_spreads_full:
            alt_lines_dict["spreads"] = alt_spreads_full
        if alt_totals_full:
            alt_lines_dict["totals"] = alt_totals_full

        parent_map[idgm] = {
            "away_team":     vtm,
            "home_team":     htm,
            "commence_time": commence,
            "markets":       markets,
            "alt_lines":     alt_lines_dict,
            "_idgm":         idgm,
        }

    # ── Pass 2: merge period markets into parent games ───────────────────────
    for g in raw_games:
        vtm  = g.get("vtm", "")
        htm  = g.get("htm", "")
        gp   = str(g.get("gp", ""))
        idgp = str(g.get("idgp", ""))
        gpd  = g.get("gpd", "").lower().strip()

        if vtm == htm or not _is_team_name(vtm) or not _is_team_name(htm):
            continue
        if gp == "0":
            continue

        suffix = _GPD_SUFFIX.get(gpd)
        if suffix is None:
            continue   # unknown / unneeded period (2nd half, individual innings, etc.)

        # For MLB, Bookmaker.eu calls the F5 market "First Half" — remap to _1st_5_innings
        if is_baseball and suffix == "_h1":
            suffix = "_1st_5_innings"

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

            # Alternate spread lines for this period
            alt_period_sp: list = []
            for ln in lines:
                if ln is ml:
                    continue
                try:
                    alp = float(ln["vsprdt"]) if ln.get("vsprdt") else None
                    hlp = float(ln["hsprdt"]) if ln.get("hsprdt") else None
                except (ValueError, TypeError):
                    alp = hlp = None
                if alp is None and hlp is None:
                    continue
                ao = _fmt(ln.get("vsprdoddst"))
                ho = _fmt(ln.get("hsprdoddst"))
                if ao or ho:
                    alt_period_sp.append({
                        "away_point": alp,
                        "home_point": hlp,
                        "away_odds":  ao,
                        "home_odds":  ho,
                    })
            alt_period_sp.sort(key=lambda x: (x["away_point"] if x["away_point"] is not None else 0))
            if alt_period_sp:
                parent.setdefault("alt_lines", {})[f"spreads{suffix}"] = alt_period_sp

        # Period totals  (home = Over, away = Under — matches Odds API convention)
        if ml.get("ovt") or ml.get("unt"):
            try:
                ov = float(ml["ovt"]) if ml.get("ovt") else None
                un = float(ml["unt"]) if ml.get("unt") else None
            except (ValueError, TypeError):
                ov = un = None
            parent["markets"][f"totals{suffix}"] = {
                "home_odds":  _fmt(ml.get("ovoddst")),  # Over  → home
                "away_odds":  _fmt(ml.get("unoddst")),  # Under → away
                "home_point": ov,                        # Over line  → home_point
                "away_point": un,                        # Under line → away_point
            }

            # Alternate totals lines for this period
            alt_period_tot: list = []
            for ln in lines:
                if ln is ml:
                    continue
                try:
                    pt = float(ln["ovt"]) if ln.get("ovt") else None
                except (ValueError, TypeError):
                    pt = None
                if pt is None:
                    continue
                oo = _fmt(ln.get("ovoddst"))
                uo = _fmt(ln.get("unoddst"))
                if oo or uo:
                    alt_period_tot.append({
                        "point":      pt,
                        "over_odds":  oo,
                        "under_odds": uo,
                    })
            alt_period_tot.sort(key=lambda x: x["point"])
            if alt_period_tot:
                parent.setdefault("alt_lines", {})[f"totals{suffix}"] = alt_period_tot

    return list(parent_map.values())


def games_from_bookmaker_har(har_path: str, sport_key: str = "") -> list:
    """
    Parse a Charles-exported HAR file from bookmaker.eu and return a list of
    game dicts in the same format as fetch_bet365().

    Thin wrapper over games_from_bookmaker_texts() — the live Playwright
    collector calls that directly with the same response bodies, so both
    routes share every parser in this module.

    Args:
        har_path:  Path to the HAR file.
        sport_key: Odds API sport key (e.g. "baseball_mlb").  Used to apply
                   sport-aware period mapping (e.g. MLB "First Half" → F5).
    """
    try:
        with open(har_path, encoding="utf-8") as f:
            har = json.load(f)
    except Exception as e:
        logger.error(f"bookmaker HAR: could not read {har_path!r}: {e}")
        return []

    entries = har.get("log", {}).get("entries", [])
    logger.info(f"bookmaker HAR: {len(entries)} total entries in {har_path!r}")

    responses: list = []
    for entry in entries:
        url = entry.get("request", {}).get("url", "")
        if entry.get("response", {}).get("status", 0) != 200:
            continue
        if GAMEVIEW_PATH not in url and SCHEDULE_PATH not in url:
            continue
        content = entry.get("response", {}).get("content", {})
        text    = content.get("text", "")
        if content.get("encoding") == "base64" and text:
            try:
                text = base64.b64decode(text).decode("utf-8", errors="replace")
            except Exception:
                continue
        if text:
            responses.append(text)

    return games_from_bookmaker_texts(responses, sport_key=sport_key)


def games_from_bookmaker_texts(responses: list, sport_key: str = "") -> list:
    """
    Turn captured GetGameView/GetSchedule response bodies into game dicts.

    responses: [body_text, ...] — raw JSON strings, in any order. The caller
    is responsible for having filtered to the two odds endpoints; anything
    that isn't parseable JSON carrying games is skipped.
    """
    # Accumulate raw games from EVERY matching response first, then resolve
    # parent/period linkage once over the combined list — see _parse_raw_games
    # docstring for why this can't be done per-response: a half/quarter-only
    # tab's response has no full-game (gp=='0') entries of its own to attach
    # period markets onto, so parsing each response in isolation silently
    # drops every period even though the full-game entry from a DIFFERENT tab
    # would have matched it correctly.
    all_raw_games: list = []
    found = 0

    for text in responses:
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            logger.warning(f"bookmaker: JSON parse error: {e}")
            continue

        raw = _extract_raw_games(data)
        if raw:
            found += 1
            all_raw_games.extend(raw)

    all_games = _parse_raw_games(all_raw_games, sport_key=sport_key)

    logger.info(
        f"bookmaker: {found} GetGameView/GetSchedule response(s), "
        f"{len(all_games)} unique games parsed"
    )
    return all_games


# ── Player props ──────────────────────────────────────────────────────────────

# evdesc value (after stripping "(Away) " / "(Home) " prefix) → Odds API market key
_EVDESC_TO_MARKET: dict[str, str] = {
    # MLB pitcher props
    "pitcher total strikeouts":    "pitcher_strikeouts",
    "pitcher total hits allowed":  "pitcher_hits_allowed",
    # MLB batter props
    "total bases":                 "batter_total_bases",
    "hits+runs+rbis":              "batter_hits_runs_rbis",
    # NBA
    "total pts":       "player_points",
    "total rebounds":  "player_rebounds",
    "total assists":   "player_assists",
}

# Regex to strip the "(Away) " / "(Home) " prefix from evdesc
_SIDE_PREFIX_RE = re.compile(r"^\((away|home)\)\s*", re.IGNORECASE)


def _normalize_player(name: str) -> str:
    """Lowercase, fix whitespace, strip accents and punctuation for fuzzy name matching."""
    # Fix non-breaking spaces and other weird whitespace
    name = re.sub(r"\s+", " ", name).strip()
    # Normalize accented characters
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    # Lowercase and remove periods (handles Jr. vs Jr, R.J. vs RJ)
    name = name.lower().replace(".", "")
    return name


def props_from_bookmaker_har(har_path: str, sport_key: str = "") -> dict:
    """
    Parse a Charles-exported HAR file from bookmaker.eu and extract player prop odds.

    Args:
        har_path:  Path to the HAR file.
        sport_key: Odds API sport key (e.g. "baseball_mlb") — reserved for future filtering.

    Returns:
        {
          market_key: {
            player_name: {"over_odds": str|None, "under_odds": str|None, "line": float|None}
          }
        }
        Player names are returned in their original form (pre-normalization); the caller
        should use _normalize_player() when doing lookups.
    """
    try:
        with open(har_path, encoding="utf-8") as f:
            har = json.load(f)
    except Exception as e:
        logger.error(f"bookmaker props HAR: could not read {har_path!r}: {e}")
        return {}

    entries = har.get("log", {}).get("entries", [])
    logger.info(f"bookmaker props HAR: {len(entries)} total entries in {har_path!r}")

    all_games: list = []
    for entry in entries:
        url    = entry.get("request", {}).get("url", "")
        status = entry.get("response", {}).get("status", 0)
        if (GAMEVIEW_PATH not in url and SCHEDULE_PATH not in url) or status != 200:
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
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        all_games.extend(_extract_raw_games(data))

    result: dict = {}   # market_key -> {player_name -> {over_odds, under_odds, line}}

    for g in all_games:
        vtm = g.get("vtm", "").strip()
        htm = g.get("htm", "").strip()

        # Player props have the same name on both sides
        if not vtm or vtm != htm:
            continue

        evdesc = g.get("evdesc", "")
        evdesc_clean = _SIDE_PREFIX_RE.sub("", evdesc).strip().lower()
        market_key = _EVDESC_TO_MARKET.get(evdesc_clean)
        if market_key is None:
            continue

        lines = g.get("Derivatives", {}).get("line", [])
        ml = _main_line(lines)
        if not ml:
            continue

        # Normalize whitespace/encoding in player name (but keep original casing for display)
        player_name = re.sub(r"\s+", " ", vtm).strip()

        try:
            line_val = float(ml["ovt"]) if ml.get("ovt") else None
        except (ValueError, TypeError):
            line_val = None

        entry = {
            "over_odds":  _fmt(ml.get("ovoddst")),
            "under_odds": _fmt(ml.get("unoddst")),
            "line":       line_val,
        }

        mkt_dict = result.setdefault(market_key, {})
        # If duplicate entries exist for the same player+market, keep the one
        # whose line matches the existing entry (prefer the first seen)
        if player_name not in mkt_dict:
            mkt_dict[player_name] = entry

    total = sum(len(v) for v in result.values())
    logger.info(
        f"bookmaker props HAR: {total} prop line(s) across {len(result)} market(s) — "
        + ", ".join(f"{mk}: {len(v)}" for mk, v in result.items())
    )
    return result
