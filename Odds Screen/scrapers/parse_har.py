"""
Charles HAR Importer
====================
Reads a HAR file exported from Charles Proxy and extracts bet365 XHR
responses (game list + per-game coupons), feeding them through the same
parsers used by the Playwright scraper.

Usage (from app.py):
    from scrapers.parse_har import games_from_har
    games = games_from_har("/path/to/session.har")
"""

import json
import logging
import base64

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
