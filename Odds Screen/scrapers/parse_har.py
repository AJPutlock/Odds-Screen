"""
Charles HAR Importer
====================
Reads a HAR file exported from Charles Proxy and extracts bet365 XHR
responses (game list + per-game coupons), feeding them through the same
parsers used by the Playwright scraper.

Also handles bet365 category/props pages (MG;SY=fe format) via
props_from_b365_har().

Usage (from app.py):
    from scrapers.parse_har import games_from_har, props_from_b365_har
    games = games_from_har("/path/to/session.har")
    props = props_from_b365_har("/path/to/session.har")
"""

import json
import logging
import base64
import re

logger = logging.getLogger(__name__)

GAME_LIST_PATH = "matchmarketscontentapi"
COUPON_PATH    = "matchbettingcontentapi/coupon"


def _entry_text(entry: dict) -> str:
    """Extract response body text from a HAR entry, handling base64 encoding."""
    content = entry.get("response", {}).get("content", {})
    text = content.get("text", "")
    encoding = content.get("encoding", "")
    if encoding == "base64" and text:
        try:
            text = base64.b64decode(text).decode("utf-8", errors="replace")
        except Exception:
            pass
    return text


# ── bet365 category-page market detection ────────────────────────────────────
# Maps the NA= value found in the navigation menu to an Odds API market key.
_B365_CATEGORY_MARKETS: dict[str, str] = {
    "pitcher strikeouts o/u": "pitcher_strikeouts",
}


def _parse_b365_fields(record: str) -> dict:
    """Parse semicolon-delimited key=value pairs from a bet365 pipe record."""
    fields: dict = {}
    for part in record.split(";"):
        if "=" in part:
            k, _, v = part.partition("=")
            fields[k.strip()] = v.strip()
    return fields


def _frac_to_american(frac: str) -> str | None:
    """
    Convert bet365 fractional odds (e.g. '6/5', '5/7') to American odds string.
    Returns e.g. '+120', '-140'.
    """
    try:
        num, den = frac.strip().split("/")
        decimal = 1.0 + int(num) / int(den)
        if decimal >= 2.0:
            american = round((decimal - 1.0) * 100)
            return f"+{american}"
        else:
            american = round(-100.0 / (decimal - 1.0))
            return str(american)
    except Exception:
        return None


def _detect_b365_market(text: str) -> str | None:
    """
    Detect which props market a bet365 category-page response contains by
    looking for the market name in the navigation MA records.
    """
    for raw_key, mkt_key in _B365_CATEGORY_MARKETS.items():
        if raw_key in text.lower():
            return mkt_key
    return None


def _parse_b365_category_props(text: str, market_key: str) -> dict:
    """
    Parse a bet365 category-page pipe-delimited response that uses
    MG;SY=fe game headers and PA;ID=P{id} pitcher profiles.

    Returns:
        {player_name: {"over_odds": str|None, "under_odds": str|None, "line": float|None}}
    """
    records = [r for r in text.split("|") if r.strip()]

    result: dict = {}  # player_name -> {over_odds, under_odds, line}

    # State machine
    pitchers: list[dict] = []      # [{id, name, team}]
    over_bets:  list[dict] = []    # [{id, line, odds_frac}]
    under_bets: list[dict] = []    # [{id, line, odds_frac}]
    current_section: str = ""      # "pitchers" | "over" | "under" | ""

    def flush_game():
        """Match pitchers to their over/under bets and save to result."""
        nonlocal pitchers, over_bets, under_bets, current_section
        for i, pitcher in enumerate(pitchers):
            name = pitcher.get("name")
            if not name:
                continue
            over  = over_bets[i]  if i < len(over_bets)  else {}
            under = under_bets[i] if i < len(under_bets) else {}
            line_val = None
            for b in (over, under):
                try:
                    line_val = float(b.get("line", ""))
                    break
                except (ValueError, TypeError):
                    pass
            result[name] = {
                "over_odds":  _frac_to_american(over.get("odds_frac",  "")) or None,
                "under_odds": _frac_to_american(under.get("odds_frac", "")) or None,
                "line":       line_val,
            }
        pitchers.clear()
        over_bets.clear()
        under_bets.clear()
        current_section = ""

    for raw in records:
        rec_type = raw.split(";")[0] if ";" in raw else raw.split("|")[0]
        f = _parse_b365_fields(raw)

        if rec_type == "MG":
            sy = f.get("SY", "")
            if sy == "fe":
                # Start of a new game block — flush previous if any
                if pitchers:
                    flush_game()
                current_section = ""
            continue

        if rec_type == "MA":
            sy = f.get("SY", "")
            na = f.get("NA", "")
            if sy == "dp":
                # "Player / Last 5" pitcher display block
                current_section = "pitchers"
            elif sy == "dc" and na.lower() == "over":
                current_section = "over"
            elif sy == "dc" and na.lower() == "under":
                current_section = "under"
            continue

        if rec_type == "PA":
            pid = f.get("ID", "")
            if current_section == "pitchers" and pid.startswith("P"):
                pitchers.append({
                    "id":   pid[1:],   # strip the P prefix
                    "name": f.get("NA", "").strip(),
                    "team": f.get("N2", "").strip(),
                })
            elif current_section in ("over", "under") and pid and not pid.startswith("P"):
                bet = {
                    "id":        pid,
                    "line":      f.get("HD", ""),
                    "odds_frac": f.get("OD", ""),
                }
                if current_section == "over":
                    over_bets.append(bet)
                else:
                    under_bets.append(bet)

    # Flush the last game block
    if pitchers:
        flush_game()

    return result


def props_from_b365_har(har_path: str) -> dict:
    """
    Parse a Charles-exported HAR file captured while browsing a bet365
    player-props category page (e.g. Pitcher Strikeouts O/U).

    Returns:
        {
          market_key: {
            player_name: {"over_odds": str|None, "under_odds": str|None, "line": float|None}
          }
        }
    """
    try:
        with open(har_path, encoding="utf-8") as fh:
            har = json.load(fh)
    except Exception as e:
        logger.error(f"bet365 props HAR: could not read {har_path!r}: {e}")
        return {}

    entries = har.get("log", {}).get("entries", [])
    logger.info(f"bet365 props HAR: {len(entries)} total entries")

    combined: dict = {}  # market_key -> {player_name -> {...}}

    for entry in entries:
        url    = entry.get("request", {}).get("url", "")
        status = entry.get("response", {}).get("status", 0)
        if status != 200:
            continue
        if GAME_LIST_PATH not in url:
            continue

        text = _entry_text(entry)
        if not text or not text.strip().startswith("F|"):
            continue

        # Only process category-page responses (have MG;SY=fe game headers)
        if "SY=fe" not in text:
            continue

        market_key = _detect_b365_market(text)
        if not market_key:
            logger.debug(f"bet365 props HAR: unrecognised market in {url[:80]}")
            continue

        props = _parse_b365_category_props(text, market_key)
        if props:
            combined.setdefault(market_key, {}).update(props)
            logger.info(
                f"bet365 props HAR: {len(props)} {market_key} entries from {url[:80]}"
            )

    return combined


# ── bet365 F5 innings parser ──────────────────────────────────────────────────

def _parse_b365_f5(text: str) -> dict:
    """
    Parse F5 innings odds from a bet365 matchmarketscontentapi response.
    Identifies this format by the presence of MA;SY=eb sections.

    Returns:
        {(away_lower, home_lower): {"h2h_1st_5_innings": {...}, "totals_1st_5_innings": {...}}}
    """
    records = text.split("|")

    # ── Step 1: build game list from PC-prefixed PA records ───────────────────
    games_by_id: dict = {}   # stripped_id → {away, home, markets:{}}

    for rec in records:
        rec = rec.strip()
        if not rec.startswith("PA;"):
            continue
        f = _parse_b365_fields(rec)
        if "FD" not in f or "BC" not in f:
            continue
        fd = f["FD"]
        if "@" not in fd:
            continue
        away, _, home = fd.partition("@")
        away, home = away.strip(), home.strip()
        raw_id = f.get("ID", "")
        # Strip "PC" prefix that distinguishes game-list PAs
        game_id = raw_id[2:] if raw_id.startswith("PC") else raw_id
        if game_id:
            games_by_id[game_id] = {"away": away, "home": home, "markets": {}}

    if not games_by_id:
        return {}

    # ── Step 2: parse SY=eb sections (Run Line = F5 ML, Total = F5 O/U) ─────
    # market_fi (from odds PAs) → game  — built while scanning Run Line section
    market_fi_map: dict = {}
    section = ""   # "ml" | "tot"

    for rec in records:
        rec = rec.strip()
        if not rec:
            continue

        if rec.startswith("MA;"):
            f = _parse_b365_fields(rec)
            if f.get("SY") == "eb":
                na = f.get("NA", "").lower()
                if "run line" in na or "money" in na:
                    section = "ml"
                elif "total" in na:
                    section = "tot"
                else:
                    section = ""
            # Don't reset section on non-SY=eb MAs (navigation records)
            continue

        if not rec.startswith("PA;") or section not in ("ml", "tot"):
            continue

        f = _parse_b365_fields(rec)
        if "OD" not in f:
            continue
        pa_id   = f.get("ID", "")
        mfi     = f.get("FI", "")
        american = _frac_to_american(f["OD"])
        if not american:
            continue

        if section == "ml":
            if pa_id in games_by_id:
                # Away team odds — ID matches the game's stripped PC id
                g = games_by_id[pa_id]
                if mfi:
                    market_fi_map[mfi] = g
                mkt = g["markets"].setdefault("h2h_1st_5_innings", {
                    "away_odds": None, "home_odds": None,
                    "away_point": None, "home_point": None,
                })
                mkt["away_odds"] = american
            elif mfi in market_fi_map:
                # Home team odds — same market FI, comes right after away
                g = market_fi_map[mfi]
                mkt = g["markets"].get("h2h_1st_5_innings", {})
                if mkt and mkt.get("home_odds") is None:
                    mkt["home_odds"] = american

        elif section == "tot":
            if mfi not in market_fi_map:
                continue
            g   = market_fi_map[mfi]
            hd  = f.get("HD", "")
            if hd.startswith("O "):
                try:
                    pt = float(hd[2:])
                    mkt = g["markets"].setdefault("totals_1st_5_innings", {
                        "away_odds": None, "home_odds": None,
                        "away_point": None, "home_point": None,
                    })
                    mkt["away_odds"]  = american
                    mkt["away_point"] = pt
                    mkt["home_point"] = pt
                except ValueError:
                    pass
            elif hd.startswith("U "):
                mkt = g["markets"].get("totals_1st_5_innings", {})
                if mkt and mkt.get("home_odds") is None:
                    mkt["home_odds"] = american

    return {
        (g["away"].lower(), g["home"].lower()): g["markets"]
        for g in games_by_id.values()
        if g["markets"]
    }


# ── bet365 1st-inning runs parser ─────────────────────────────────────────────

def _parse_b365_1st_inning(text: str) -> dict:
    """
    Parse 1st inning Over/Under totals (0.5 line only) from a bet365
    category-page response (MG;SY=fe + MA;SY=dg Over/Under format).

    Returns:
        {(away_lower, home_lower): {"totals_1st_1_innings": {...}}}
    """
    records = text.split("|")
    result:  dict = {}

    # Per-game mutable state
    game_away:   str  = ""
    game_home:   str  = ""
    lines:       dict = {}   # stripped_id → line_value_str  (e.g. "0.5")
    over_map:    dict = {}   # stripped_id → american_odds
    under_list:  list = []   # american odds in positional order

    section = ""   # "lines" | "over" | "under"

    def _flush():
        nonlocal lines, over_map, under_list, section
        if game_away and game_home:
            # Find the id associated with the 0.5 line
            half_id = next(
                (lid for lid, lv in lines.items()
                 if lv.strip() in ("0.5", ".5")),
                None
            )
            o_odds = over_map.get(half_id) if half_id else None
            u_odds = under_list[0] if under_list else None
            if o_odds or u_odds:
                result[(game_away.lower(), game_home.lower())] = {
                    "totals_1st_1_innings": {
                        "away_odds":  o_odds,
                        "home_odds":  u_odds,
                        "away_point": 0.5,
                        "home_point": 0.5,
                    }
                }
        lines.clear()
        over_map.clear()
        under_list.clear()
        section = ""

    for rec in records:
        rec = rec.strip()
        if not rec:
            continue

        if rec.startswith("MG;"):
            f = _parse_b365_fields(rec)
            if f.get("SY") == "fe":
                _flush()
                na = f.get("NA", "")
                if "@" in na:
                    game_away, _, game_home = na.partition("@")
                    game_away = game_away.strip()
                    game_home = game_home.strip()
                else:
                    game_away = game_home = ""
            continue

        if rec.startswith("MA;"):
            f = _parse_b365_fields(rec)
            sy = f.get("SY", "")
            na = f.get("NA", "").lower().strip()
            if sy == "dc":
                section = "lines"        # line-value definitions
            elif sy == "dg" and na == "over":
                section = "over"
            elif sy == "dg" and na == "under":
                section = "under"
            continue

        if rec.startswith("PA;"):
            f   = _parse_b365_fields(rec)
            pid = f.get("ID", "")

            if section == "lines" and pid.startswith("P"):
                lines[pid[1:]] = f.get("NA", "")    # stripped id → "0.5" / "1.5" etc.

            elif section == "over" and pid and not pid.startswith("P"):
                amer = _frac_to_american(f.get("OD", ""))
                if amer:
                    over_map[pid] = amer

            elif section == "under" and pid and not pid.startswith("P"):
                amer = _frac_to_american(f.get("OD", ""))
                if amer:
                    under_list.append(amer)

    _flush()   # flush last game block
    return result


# ── Main games_from_har ───────────────────────────────────────────────────────

def games_from_har(har_path: str) -> list:
    """
    Parse a Charles-exported HAR file and return a list of game dicts in the
    same format as fetch_bet365().

    Handles multiple matchmarketscontentapi response types in one HAR:
      - Main game list  (full-game h2h / spreads / totals)
      - F5 partial      (h2h_1st_5_innings / totals_1st_5_innings)  — SY=eb format
      - 1st inning runs (totals_1st_1_innings, 0.5 only)            — SY=dg format
      - Pitcher K props (SY=dp format) — skipped here, use props_from_b365_har
    """
    from scrapers.bet365 import parse_response, parse_coupon

    try:
        with open(har_path, encoding="utf-8") as f:
            har = json.load(f)
    except Exception as e:
        logger.error(f"HAR import: could not read {har_path!r}: {e}")
        return []

    entries = har.get("log", {}).get("entries", [])
    logger.info(f"HAR import: {len(entries)} total entries in {har_path!r}")

    # ── Classify each matchmarketscontentapi response ─────────────────────────
    game_list_texts: list[str] = []   # main game list (has ML= in game PAs)
    f5_texts:        list[str] = []   # F5 partial markets  (SY=eb sections)
    inning1_texts:   list[str] = []   # 1st inning category (SY=dg Over/Under)
    coupon_entries:  list[tuple] = [] # (url, text)

    for entry in entries:
        url    = entry.get("request", {}).get("url", "")
        status = entry.get("response", {}).get("status", 0)
        if status != 200:
            continue
        text = _entry_text(entry)
        if not text:
            continue

        if GAME_LIST_PATH in url and text.strip().startswith("F|"):
            if "SY=dp" in text:
                # Pitcher K / player-profile category — handled by props_from_b365_har
                logger.debug(f"HAR import: pitcher props page skipped — {url[:80]}")
            elif "SY=dg" in text and "SY=fe" in text:
                inning1_texts.append(text)
                logger.debug(f"HAR import: 1st-inning category ({len(text)} B) — {url[:80]}")
            elif ";ML=" in text or "MG;SY=cmx" in text:
                # Main game list: MLB/NFL etc. have ML= streaming IDs in game PAs;
                # NCAA baseball uses MG;SY=cmx as the container instead of ML= fields
                game_list_texts.append(text)
                logger.debug(f"HAR import: game list ({len(text)} B) — {url[:80]}")
            elif "SY=eb" in text:
                f5_texts.append(text)
                logger.debug(f"HAR import: F5 partial market ({len(text)} B) — {url[:80]}")
            else:
                game_list_texts.append(text)
                logger.debug(f"HAR import: game list (fallback) ({len(text)} B) — {url[:80]}")

        elif COUPON_PATH in url and "|MG;" in text:
            coupon_entries.append((url, text))
            logger.debug(f"HAR import: coupon found ({len(text)} B) — {url[:80]}")

    logger.info(
        f"HAR import: {len(game_list_texts)} main, {len(f5_texts)} F5, "
        f"{len(inning1_texts)} 1st-inn, {len(coupon_entries)} coupons"
    )

    if not game_list_texts:
        logger.warning(
            "HAR import: no main game-list responses found. "
            "Make sure you browsed the sport's main odds page in Charles."
        )
        return []

    # ── Parse main game list ──────────────────────────────────────────────────
    seen_matchups: set = set()
    games: list = []
    for gl_text in game_list_texts:
        for g in parse_response(gl_text, filter_past=False):
            key = (g.get("away_team", "").lower(), g.get("home_team", "").lower())
            if key not in seen_matchups:
                seen_matchups.add(key)
                games.append(g)
    logger.info(f"HAR import: {len(games)} unique games from main game list")

    # ── Merge coupon data ─────────────────────────────────────────────────────
    coupon_fi_map = {g["_coupon_fi"]: g for g in games if g.get("_coupon_fi")}
    merged = 0
    for coupon_url, coupon_text in coupon_entries:
        markets = parse_coupon(coupon_text)
        if not markets:
            continue
        target_game = _find_game_for_coupon(coupon_url, coupon_text, games, coupon_fi_map)
        if target_game is not None:
            target_game["markets"].update(markets)
            merged += 1
    if coupon_entries:
        logger.info(f"HAR import: coupon data merged for {merged}/{len(games)} games")

    # ── Merge F5 markets ──────────────────────────────────────────────────────
    if f5_texts:
        f5_map: dict = {}
        for txt in f5_texts:
            f5_map.update(_parse_b365_f5(txt))
        f5_merged = 0
        for game in games:
            key = (game.get("away_team", "").lower(), game.get("home_team", "").lower())
            if key in f5_map:
                game["markets"].update(f5_map[key])
                f5_merged += 1
        logger.info(f"HAR import: F5 markets merged for {f5_merged}/{len(games)} games")

    # ── Merge 1st-inning markets ──────────────────────────────────────────────
    if inning1_texts:
        in1_map: dict = {}
        for txt in inning1_texts:
            in1_map.update(_parse_b365_1st_inning(txt))
        in1_merged = 0
        for game in games:
            key = (game.get("away_team", "").lower(), game.get("home_team", "").lower())
            if key in in1_map:
                game["markets"].update(in1_map[key])
                in1_merged += 1
        logger.info(f"HAR import: 1st-inning markets merged for {in1_merged}/{len(games)} games")

    return games


def _find_game_for_coupon(coupon_url: str, coupon_text: str, games: list, coupon_fi_map: dict) -> dict | None:
    """
    Match a coupon to a game.
    Primary: extract the E<id> parameter from the coupon URL and look it up
             in the _coupon_fi map (most reliable).
    Fallback: team-name token overlap against EV;NA= field in the response body.
    """
    import re

    # Primary: URL contains E<coupon_fi> — e.g. ...#E25364073# or pd=...E25364073...
    url_fi_match = re.search(r'[#%23]E(\d+)[#%23F]', coupon_url)
    if not url_fi_match:
        # Try the pd= query param decoded form
        url_fi_match = re.search(r'E(\d{7,})', coupon_url)
    if url_fi_match:
        fi = url_fi_match.group(1)
        if fi in coupon_fi_map:
            return coupon_fi_map[fi]

    # Fallback: match by team name tokens in EV;NA= field
    ev_match = re.search(r'\|EV;[^|]*?;NA=([^;|]+)', coupon_text)
    ev_name  = ev_match.group(1).lower() if ev_match else ""
    if ev_name:
        for game in games:
            away = game.get("away_team", "").lower()
            home = game.get("home_team", "").lower()
            for token in away.split() + home.split():
                if len(token) > 3 and token in ev_name:
                    return game

    if len(games) == 1:
        return games[0]

    return None
