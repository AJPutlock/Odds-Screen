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


def games_from_har(har_path: str) -> list:
    """
    Parse a Charles-exported HAR file and return a list of game dicts in the
    same format as fetch_bet365().
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

    # ── Collect game-list and coupon responses ────────────────────────────────
    game_list_texts: list[str] = []
    coupon_entries:  list[tuple] = []  # (url, text)

    for entry in entries:
        url    = entry.get("request", {}).get("url", "")
        status = entry.get("response", {}).get("status", 0)
        if status != 200:
            continue
        text = _entry_text(entry)
        if not text:
            continue
        if GAME_LIST_PATH in url and text.strip().startswith("F|"):
            game_list_texts.append(text)
            logger.debug(f"HAR import: game list found ({len(text)} bytes) — {url[:100]}")
        elif COUPON_PATH in url and "|MG;" in text:
            coupon_entries.append((url, text))
            logger.debug(f"HAR import: coupon found ({len(text)} bytes) — {url[:100]}")

    logger.info(
        f"HAR import: {len(game_list_texts)} game-list response(s), "
        f"{len(coupon_entries)} coupon response(s)"
    )

    if not game_list_texts:
        logger.warning(
            "HAR import: no game-list responses found. "
            "Make sure you browsed the sport's main odds page in Charles."
        )
        return []

    # ── Parse all game list responses and merge (HAR may contain multiple sports) ─
    seen_matchups: set = set()
    games: list = []
    for gl_text in game_list_texts:
        for g in parse_response(gl_text, filter_past=False):
            key = (g.get("away_team", "").lower(), g.get("home_team", "").lower())
            if key not in seen_matchups:
                seen_matchups.add(key)
                games.append(g)
    logger.info(f"HAR import: {len(games)} unique games parsed across {len(game_list_texts)} game-list response(s)")

    # ── Build _coupon_fi → game lookup ───────────────────────────────────────
    coupon_fi_map = {g["_coupon_fi"]: g for g in games if g.get("_coupon_fi")}

    # ── Merge coupon data into games ──────────────────────────────────────────
    merged = 0
    for coupon_url, coupon_text in coupon_entries:
        markets = parse_coupon(coupon_text)
        if not markets:
            continue

        target_game = _find_game_for_coupon(coupon_url, coupon_text, games, coupon_fi_map)
        if target_game is not None:
            target_game["markets"].update(markets)
            merged += 1

    logger.info(f"HAR import: coupon data merged for {merged}/{len(games)} games")
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
