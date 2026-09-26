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
    "pitcher strikeouts o/u":    "pitcher_strikeouts",
    "pitcher hits allowed o/u":  "pitcher_hits_allowed",
    "batter total bases o/u":    "batter_total_bases",
    "batter hits+runs+rbis o/u": "batter_hits_runs_rbis",
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


# ── bet365 football league-page parsers ───────────────────────────────────────
#
# Football (and basketball) league pages come in three shapes, all served from
# matchmarketscontentapi.  Which one you are looking at is stated by the page
# itself: the nav block carries an `MA;...;LS=1` record naming the currently
# selected tab.  That is what we classify on — the tab names are stable across
# sports and leagues, whereas the URL's E<id> parameter differs per league and
# the SY= codes are reused between unrelated page types (every one of these
# pages contains SY=cmx, and the 1H Total page contains both SY=dg and SY=fe,
# so body-sniffing misroutes them — see games_from_har).
#
#   "Game Lines" / "1st Half" / "1st Quarter"
#       Main lines. Game-header PAs (FD=/BC=) followed by three MA;SY=eb
#       sections — Spread, Total, Money — holding every game's priced legs.
#
#   "Spread" / "1st Half Point Spread"
#       Alt spread ladder. One MG;SY=fd|fe per game, then two MA;SY=_a
#       sections named after the away and home team, each listing that side's
#       rungs as PA;NA=<point>;OD=<price>.
#
#   "Total" / "1st Half Total"
#       Alt total ladder. Per game, a repeating triple of
#       MA;SY=dr|dc  (rung *points*, as PA;ID=P<id>;NA=<point>)
#       MA;SY=nm|ds|dg NA=Over   (prices, PA;ID=<id> — the point's id, P stripped)
#       MA;SY=nm|ds|dg NA=Under  (prices, positional within the section)
#       The Over price joins its point by id; Under joins by position.
#
# Tab name (lowercased) → (page kind, Odds-API market-key suffix)
_B365_FB_TABS: dict[str, tuple[str, str]] = {
    "game lines":                 ("main",       ""),
    "1st half":                   ("main",       "_h1"),
    "1st quarter":                ("main",       "_q1"),
    "spread":                     ("alt_spread", ""),
    "1st half point spread":      ("alt_spread", "_h1"),
    "1st quarter point spread":   ("alt_spread", "_q1"),
    "total":                      ("alt_total",  ""),
    "1st half total":             ("alt_total",  "_h1"),
    "1st quarter total":          ("alt_total",  "_q1"),
}

# MA;SY=eb section name → Odds API base market type
_B365_EB_SECTIONS: dict[str, str] = {
    "spread":    "spreads",
    "handicap":  "spreads",
    "run line":  "spreads",
    "puck line": "spreads",
    "total":     "totals",
    "totals":    "totals",
    "money":     "h2h",
    "moneyline": "h2h",
}


def _b365_selected_tab(text: str) -> str:
    """Return the market tab this response represents, e.g. '1st half total'.

    The nav carries several LS=1 ('currently selected') records — the top-level
    section, the conference filter (whose NA is blank for 'All'), and finally
    the market tab.  The last non-blank one is the market tab.
    """
    selected = ""
    for rec in text.split("|"):
        if rec.startswith("MA;") and ";LS=1" in rec:
            na = _parse_b365_fields(rec[3:]).get("NA", "").strip()
            if na:
                selected = na
    return selected.lower()


def _b365_game_key(name: str) -> tuple | None:
    """'Liberty @ Coastal Carolina' → ('liberty', 'coastal carolina')."""
    if "@" not in name:
        return None
    away, _, home = name.partition("@")
    away, home = away.strip(), home.strip()
    if not away or not home:
        return None
    return (away.lower(), home.lower())


def _parse_b365_main_lines(text: str, suffix: str) -> dict:
    """Parse a 'Game Lines' / '1st Half' / '1st Quarter' league page.

    Returns {(away_lower, home_lower): {"away": str, "home": str, "bc": str,
                                        "markets": {...}}}
    with market keys h2h/spreads/totals + the period suffix.
    """
    records = [r for r in text.split("|") if r.strip()]

    # ── Pass 1: game headers ─────────────────────────────────────────────────
    by_pc:  dict = {}   # game-header ID with the 'PC' prefix stripped → game
    by_oi:  dict = {}   # parent-event OI → game
    games:  dict = {}
    for rec in records:
        if not rec.startswith("PA;"):
            continue
        f = _parse_b365_fields(rec[3:])
        fd = f.get("FD", "")
        if "FD" not in f or "BC" not in f or "@" not in fd:
            continue
        away, _, home = fd.partition("@")
        key = (away.strip().lower(), home.strip().lower())
        # The coupon endpoint keys on the E<id> inside PD, which is a different
        # number from FI — same extraction parse_response() does, so per-game
        # coupons captured in the same HAR still merge onto these games.
        coupon_m = re.search(r"#E(\d+)#", f.get("PD", ""))
        fi = f.get("FI") or f.get("ID", "")
        game = {
            "away":       away.strip(),
            "home":       home.strip(),
            "bc":         f.get("BC", ""),
            "fi":         fi,
            "coupon_fi":  coupon_m.group(1) if coupon_m else fi,
            "markets":    {},
        }
        raw_id = f.get("ID", "")
        # Register every header so its odds resolve, but a matchup that appears
        # twice on one page (bet365 serves two slates together when a series
        # spans dates) keeps its FIRST occurrence — same as parse_response()'s
        # dedupe, and the earlier game is the one about to be played. The later
        # duplicate's odds land on an orphan dict and are discarded.
        by_pc[raw_id[2:] if raw_id.startswith("PC") else raw_id] = game
        games.setdefault(key, game)

    if not games:
        return {}

    # ── Pass 2: the three SY=eb odds sections ────────────────────────────────
    state = {"type": None, "game": None, "legs": []}

    def commit() -> None:
        mtype, game, legs = state["type"], state["game"], state["legs"]
        if not mtype or game is None or len(legs) < 2:
            return
        key = mtype + suffix
        if mtype == "h2h":
            game["markets"][key] = {
                "away_odds":  legs[0]["odds"],
                "home_odds":  legs[1]["odds"],
                "away_point": None,
                "home_point": None,
            }
        elif mtype == "spreads":
            game["markets"][key] = {
                "away_odds":  legs[0]["odds"],
                "away_point": legs[0].get("point"),
                "home_odds":  legs[1]["odds"],
                "home_point": legs[1].get("point"),
            }
        elif mtype == "totals":
            over  = next((l for l in legs if l.get("side") == "over"),  None)
            under = next((l for l in legs if l.get("side") == "under"), None)
            if over and under:
                # Over = home slot, Under = away slot — the convention used by
                # parse_coupon(), parse_response() and the frontend.
                game["markets"][key] = {
                    "home_odds":  over["odds"],
                    "home_point": over.get("point"),
                    "away_odds":  under["odds"],
                    "away_point": under.get("point"),
                }

    for rec in records:
        if rec.startswith("MA;"):
            f = _parse_b365_fields(rec[3:])
            if f.get("SY") == "eb":
                commit()
                state.update(type=_B365_EB_SECTIONS.get(f.get("NA", "").strip().lower()),
                             game=None, legs=[])
            continue

        if state["type"] is None or not rec.startswith("PA;"):
            continue
        f = _parse_b365_fields(rec[3:])
        if "OD" not in f:
            continue
        american = _frac_to_american(f["OD"])
        if not american:
            continue

        # Which game is this leg for?  In the Spread section a leg's ID equals
        # the game header's ID with 'PC' stripped; in the Total and Money
        # sections the IDs are unrelated, but OI always carries the parent
        # event id, which the Spread section already taught us.
        oi, rid = f.get("OI", ""), f.get("ID", "")
        target = by_pc.get(rid) or by_oi.get(oi)
        if target is None:
            continue
        if oi and oi not in by_oi:
            by_oi[oi] = target
        if target is not state["game"]:
            commit()
            state.update(game=target, legs=[])

        hd = f.get("HD", "").strip()
        leg: dict = {"odds": american}
        if hd.startswith("O ") or hd.startswith("U "):
            try:
                leg["point"] = float(hd[2:])
                leg["side"]  = "over" if hd.startswith("O ") else "under"
            except ValueError:
                pass
        elif hd:
            try:
                leg["point"] = float(hd)
            except ValueError:
                pass
        state["legs"].append(leg)
        if len(state["legs"]) == 2:     # a market is always a two-way pair
            commit()
            state["legs"] = []
    commit()

    # Games with no priced markets are kept — parse_response() keeps them too,
    # and app.py's team-matching/debug endpoints expect the full board.
    return games


def _parse_b365_alt_spread(text: str, suffix: str) -> dict:
    """Parse a 'Spread' / '1st Half Point Spread' alt-ladder page.

    Returns {(away_lower, home_lower): {"bc": str, "rungs": [
        {"away_point", "home_point", "away_odds", "home_odds"}, ...]}}
    """
    records = [r for r in text.split("|") if r.strip()]
    out: dict = {}
    state = {"key": None, "bc": "", "side": None, "legs": {}}

    def commit() -> None:
        key, legs = state["key"], state["legs"]
        if not key or not legs.get("away") or not legs.get("home"):
            return
        # bet365 lists each side's ladder independently; pair them on the point
        # (away -7.5 is the same rung as home +7.5).
        home_by_pt = {r["point"]: r["odds"] for r in legs["home"]}
        rungs = []
        for r in legs["away"]:
            ho = home_by_pt.get(-r["point"])
            rungs.append({
                "away_point": r["point"],
                "home_point": -r["point"],
                "away_odds":  r["odds"],
                "home_odds":  ho,
            })
        rungs.sort(key=lambda x: x["away_point"])
        if rungs:
            out[key] = {"bc": state["bc"], "rungs": rungs}

    for rec in records:
        if rec.startswith("MG;"):
            f = _parse_b365_fields(rec[3:])
            if f.get("SY") in ("fd", "fe"):
                key = _b365_game_key(f.get("NA", ""))
                if key:
                    commit()
                    state.update(key=key, bc=f.get("BC", ""), side=None, legs={})
            continue

        if rec.startswith("MA;"):
            f = _parse_b365_fields(rec[3:])
            if f.get("SY") == "_a" and state["key"]:
                na = f.get("NA", "").strip().lower()
                # The two sections are named after the teams, so we never have
                # to assume an ordering.
                side = ("away" if na == state["key"][0]
                        else "home" if na == state["key"][1] else None)
                state["side"] = side
                if side:
                    state["legs"][side] = []
            else:
                state["side"] = None
            continue

        if rec.startswith("PA;") and state["side"] and state["key"]:
            f = _parse_b365_fields(rec[3:])
            american = _frac_to_american(f.get("OD", ""))
            raw_pt = f.get("HA") or f.get("NA", "")
            if not american or not raw_pt:
                continue
            try:
                state["legs"][state["side"]].append(
                    {"point": float(raw_pt), "odds": american})
            except ValueError:
                pass
    commit()
    return out


def _parse_b365_alt_total(text: str, suffix: str) -> dict:
    """Parse a 'Total' / '1st Half Total' alt-ladder page.

    Returns {(away_lower, home_lower): {"bc": str, "rungs": [
        {"point", "over_odds", "under_odds"}, ...]}}
    """
    records = [r for r in text.split("|") if r.strip()]
    out: dict = {}
    state = {"key": None, "bc": "", "sec": None,
             "points": [], "over": {}, "under": []}

    def commit_chunk() -> None:
        key, points = state["key"], state["points"]
        if not key or not points:
            return
        rungs = out.setdefault(key, {"bc": state["bc"], "rungs": []})["rungs"]
        for i, (pid, point) in enumerate(points):
            over  = state["over"].get(pid)                                  # by id
            under = state["under"][i] if i < len(state["under"]) else None  # by position
            if over or under:
                rungs.append({"point": point, "over_odds": over, "under_odds": under})

    for rec in records:
        if rec.startswith("MG;"):
            f = _parse_b365_fields(rec[3:])
            if f.get("SY") in ("fd", "fe"):
                key = _b365_game_key(f.get("NA", ""))
                if key:
                    commit_chunk()
                    state.update(key=key, bc=f.get("BC", ""), sec=None,
                                 points=[], over={}, under=[])
            continue

        if rec.startswith("MA;"):
            f = _parse_b365_fields(rec[3:])
            sy, na = f.get("SY", ""), f.get("NA", "").strip().lower()
            if sy in ("dr", "dc"):
                # A new block of rung points — flush whatever came before it.
                commit_chunk()
                state.update(sec="points", points=[], over={}, under=[])
            elif sy in ("nm", "ds", "dg"):
                state["sec"] = ("over"  if na == "over"
                                else "under" if na == "under" else None)
            else:
                state["sec"] = None
            continue

        if not rec.startswith("PA;") or not state["key"] or not state["sec"]:
            continue
        f = _parse_b365_fields(rec[3:])
        rid = f.get("ID", "")
        if state["sec"] == "points":
            if rid.startswith("P"):
                try:
                    state["points"].append((rid[1:], float(f.get("NA", ""))))
                except ValueError:
                    pass
        else:
            american = _frac_to_american(f.get("OD", ""))
            if not american:
                continue
            if state["sec"] == "over":
                state["over"][rid] = american
            else:
                state["under"].append(american)
    commit_chunk()

    for entry in out.values():
        entry["rungs"].sort(key=lambda x: x["point"])
    return out


def _merge_b365_football_pages(fb_pages: list) -> tuple:
    """Assemble main-line + alt-ladder football pages into game dicts.

    fb_pages: [(kind, suffix, text), ...] as classified by games_from_har.
    Returns (games, unparsed_full_game_texts):
      games    — the list-of-game-dicts shape fetch_bet365() produces, plus an
                 "alt_lines" key matching parse_bookmaker_har.py's format.
      unparsed — full-game main pages this parser could not read (an older
                 bet365 layout with no SY=eb sections), for the caller to hand
                 to parse_response() instead.
    """
    from scrapers.bet365 import parse_bc

    games: dict = {}     # (away_lower, home_lower) → game dict
    ladders: dict = {}   # (away_lower, home_lower) → {market_key: [rungs]}
    unparsed: list = []  # full-game main pages the SY=eb parser couldn't read

    def touch(key, away, home, bc) -> dict:
        g = games.get(key)
        if g is None:
            g = games[key] = {
                "_fi":           "",
                "_coupon_fi":    "",
                "away_team":     away,
                "home_team":     home,
                "commence_time": parse_bc(bc) or "",
                "markets":       {},
                "alt_lines":     {},
            }
        elif not g["commence_time"] and bc:
            g["commence_time"] = parse_bc(bc) or ""
        return g

    # ── Main lines first: they define the game list ──────────────────────────
    for kind, suffix, text in fb_pages:
        if kind != "main":
            continue
        parsed = _parse_b365_main_lines(text, suffix)
        priced = sum(1 for g in parsed.values() if g["markets"])
        if not priced:
            # No SY=eb sections at all — an older bet365 layout.
            if suffix == "":
                unparsed.append(text)
            else:
                logger.warning(
                    f"HAR import: '{suffix}' main-line page yielded no odds — "
                    f"unrecognised layout, those markets will be missing"
                )
            continue
        for key, g in parsed.items():
            game = touch(key, g["away"], g["home"], g["bc"])
            game["markets"].update(g["markets"])
            if not game["_coupon_fi"] and g.get("coupon_fi"):
                game["_fi"]        = g.get("fi", "")
                game["_coupon_fi"] = g["coupon_fi"]
        logger.info(
            f"HAR import: main lines{suffix or ' (full game)'} — "
            f"{priced}/{len(parsed)} games priced"
        )

    # ── Alt ladders ──────────────────────────────────────────────────────────
    for kind, suffix, text in fb_pages:
        if kind == "alt_spread":
            parsed = _parse_b365_alt_spread(text, suffix)
            mkey   = "spreads" + suffix
        elif kind == "alt_total":
            parsed = _parse_b365_alt_total(text, suffix)
            mkey   = "totals" + suffix
        else:
            continue
        for key, entry in parsed.items():
            # An alt page can name a game the main pages never showed (or the
            # user may have captured alt pages only) — keep it either way.
            touch(key, key[0].title(), key[1].title(), entry.get("bc", ""))
            ladders.setdefault(key, {})[mkey] = entry["rungs"]
        logger.info(
            f"HAR import: {kind}{suffix or ' (full game)'} — {len(parsed)} games, "
            f"{sum(len(e['rungs']) for e in parsed.values())} rungs"
        )

    # ── Attach ladders, dropping the rung that duplicates the main line ──────
    # parse_bookmaker_har.py excludes the main line from alt_lines and the
    # frontend merges it back in (fullBookmakerLadder) — match that exactly, or
    # the main line would be counted twice.
    for key, by_market in ladders.items():
        game = games.get(key)
        if game is None:
            continue
        for mkey, rungs in by_market.items():
            main = game["markets"].get(mkey)
            if mkey.startswith("totals"):
                main_pt = (main or {}).get("home_point")
                kept = [r for r in rungs
                        if main_pt is None or abs(r["point"] - main_pt) > 1e-9]
            else:
                main_pt = (main or {}).get("away_point")
                kept = [r for r in rungs
                        if main_pt is None or abs(r["away_point"] - main_pt) > 1e-9]
            if kept:
                game["alt_lines"][mkey] = kept

    results = sorted(games.values(),
                     key=lambda g: (g["commence_time"], g["away_team"]))
    mkt_total = sum(len(g["markets"]) for g in results)
    lad_total = sum(len(v) for g in results for v in g["alt_lines"].values())
    logger.info(
        f"HAR import: {len(results)} league-page games, {mkt_total} markets, "
        f"{lad_total} alt-line rungs"
    )
    return results, unparsed


# ── Entry points ──────────────────────────────────────────────────────────────

def games_from_har(har_path: str) -> list:
    """Parse a Charles-exported HAR file — thin wrapper over games_from_texts().

    Kept as the manual-import path. The live Playwright collector calls
    games_from_texts() directly with the same response bodies, so both routes
    share every parser below.
    """
    try:
        with open(har_path, encoding="utf-8") as f:
            har = json.load(f)
    except Exception as e:
        logger.error(f"HAR import: could not read {har_path!r}: {e}")
        return []

    entries = har.get("log", {}).get("entries", [])
    logger.info(f"HAR import: {len(entries)} total entries in {har_path!r}")

    responses: list = []
    for entry in entries:
        if entry.get("response", {}).get("status", 0) != 200:
            continue
        text = _entry_text(entry)
        if text:
            responses.append((entry.get("request", {}).get("url", ""), text))
    return games_from_texts(responses)


def games_from_texts(responses: list) -> list:
    """
    Turn captured bet365 response bodies into game dicts, in the same format
    fetch_bet365() returns.

    responses: [(url, body_text), ...] — any mix of the page types below, in
    any order. Only the URL's path is used (to tell game/coupon endpoints
    apart); everything else is decided from the body.

    Handles multiple matchmarketscontentapi response types at once:
      - Football/basketball league pages, classified by their own selected-tab
        marker (see _B365_FB_TABS):
          "Game Lines" / "1st Half" / "1st Quarter"         → main lines
          "Spread"     / "1st Half Point Spread"            → alt spread ladder
          "Total"      / "1st Half Total"                   → alt total ladder
      - Main game list  (full-game h2h / spreads / totals)
      - F5 partial      (h2h_1st_5_innings / totals_1st_5_innings)  — SY=eb format
      - 1st inning runs (totals_1st_1_innings, 0.5 only)            — SY=dg format
      - Pitcher K props (SY=dp format) — skipped here, use props_from_b365_har

    Games carry an "alt_lines" dict in the same shape parse_bookmaker_har.py
    produces — {"spreads": [...], "totals_h1": [...]} — with the main line
    excluded, so the frontend's ladder code can consume either source.
    """
    from scrapers.bet365 import parse_response, parse_coupon

    # ── Classify each matchmarketscontentapi response ─────────────────────────
    game_list_texts: list[str] = []   # main game list (has ML= in game PAs)
    f5_texts:        list[str] = []   # F5 partial markets  (SY=eb sections)
    inning1_texts:   list[str] = []   # 1st inning category (SY=dg Over/Under)
    coupon_entries:  list[tuple] = [] # (url, text)
    fb_pages:        list[tuple] = [] # (kind, suffix, text) — football league pages

    for url, text in responses:
        if not text:
            continue

        if GAME_LIST_PATH in url and text.strip().startswith("F|"):
            # The page states which market tab it is — trust that over body
            # sniffing, which cannot tell these pages apart (all of them carry
            # SY=cmx, and the 1H Total page also carries SY=dg + SY=fe, so it
            # would otherwise be handed to the baseball 1st-inning parser).
            tab = _b365_selected_tab(text)
            route = _B365_FB_TABS.get(tab)
            if route:
                kind, suffix = route
                fb_pages.append((kind, suffix, text))
                logger.debug(
                    f"HAR import: {kind} page, tab={tab!r} → suffix={suffix!r} "
                    f"({len(text)} B) — {url[:80]}"
                )
            elif "SY=dp" in text:
                # Pitcher K / player-profile category — handled by props_from_b365_har
                logger.debug(f"HAR import: pitcher props page skipped — {url[:80]}")
            elif "SY=dg" in text and "SY=fe" in text:
                inning1_texts.append(text)
                logger.debug(f"HAR import: 1st-inning category ({len(text)} B) — {url[:80]}")
            elif ";ML=" in text or ";SY=cmx" in text:
                # Main game list: MLB/NFL etc. have ML= streaming IDs in game PAs;
                # NCAA baseball uses SY=cmx as the game-list container instead of ML= fields
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
        f"{len(inning1_texts)} 1st-inn, {len(coupon_entries)} coupons, "
        f"{len(fb_pages)} football league pages"
    )

    # ── Football/basketball league pages ─────────────────────────────────────
    # These seed the game list, then fall through to the shared coupon / F5 /
    # 1st-inning merging below — a HAR can legitimately hold both (e.g. NBA
    # league pages plus per-game coupons).
    fb_games: list = []
    if fb_pages:
        fb_games, fb_unparsed = _merge_b365_football_pages(fb_pages)
        # Older captures serve the full-game page in the pre-SY=eb layout,
        # where odds sit inline on the game-list PAs. parse_response() reads
        # that shape, so hand those pages back to it rather than losing them.
        if fb_unparsed:
            logger.info(
                f"HAR import: {len(fb_unparsed)} full-game page(s) had no SY=eb "
                f"sections — falling back to the game-list parser"
            )
            game_list_texts.extend(fb_unparsed)

    if not game_list_texts and not fb_games:
        logger.warning(
            "HAR import: no main game-list responses found. "
            "Make sure you browsed the sport's main odds page in Charles."
        )
        return []

    # ── Parse main game list ──────────────────────────────────────────────────
    seen_matchups: set = set()
    games: list = []
    for g in fb_games:
        key = (g.get("away_team", "").lower(), g.get("home_team", "").lower())
        seen_matchups.add(key)
        games.append(g)
    fb_by_key = {(g["away_team"].lower(), g["home_team"].lower()): g for g in fb_games}
    for gl_text in game_list_texts:
        for g in parse_response(gl_text, filter_past=False):
            key = (g.get("away_team", "").lower(), g.get("home_team", "").lower())
            if key in seen_matchups:
                # Already seeded from a league page — keep the richer entry and
                # only fill in markets it doesn't have, plus the coupon id the
                # league pages don't carry.
                existing = fb_by_key.get(key)
                if existing is not None:
                    for mk, mv in (g.get("markets") or {}).items():
                        existing["markets"].setdefault(mk, mv)
                    if not existing.get("_coupon_fi") and g.get("_coupon_fi"):
                        existing["_fi"]        = g.get("_fi", "")
                        existing["_coupon_fi"] = g["_coupon_fi"]
                continue
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
