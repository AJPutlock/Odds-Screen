"""
bet365 Scraper — Ohio (www.oh.bet365.com)
=========================================
Uses Playwright (headless Chromium) to bypass Cloudflare bot protection.

Flow
----
1. Navigate to the sport's game-list page and intercept the
   matchmarketscontentapi XHR → team names, commence times, and event IDs.
2. For each upcoming game, fire the matchbettingcontentapi/coupon endpoint
   (all requests launched in parallel via Promise.all inside the browser)
   → full game h2h / spreads / totals + partial markets (1H, Q1, 1P, F5, …).
3. Merge coupon data into the game dict; fall back to game-list odds for
   h2h / spreads if the coupon call fails for a specific game.

Output format
-------------
Each game dict:
    {
        "_fi":           str,          # bet365 internal event ID
        "away_team":     str,
        "home_team":     str,
        "commence_time": ISO-8601 str,
        "markets": {
            "h2h":              {"away_odds": str, "home_odds": str, "away_point": None, "home_point": None},
            "spreads":          {"away_odds": str, "home_odds": str, "away_point": float, "home_point": float},
            "totals":           {"away_odds": str, "home_odds": str, "away_point": float, "home_point": float},
            # partial markets added when available:
            "h2h_h1":           {...},  # basketball / football 1st half
            "spreads_h1":       {...},
            "totals_h1":        {...},
            "h2h_q1":           {...},  # basketball / football 1st quarter
            "spreads_q1":       {...},
            "totals_q1":        {...},
            "h2h_p1":           {...},  # hockey 1st period
            "spreads_p1":       {...},
            "totals_p1":        {...},
            "h2h_1st_5_innings":    {...},  # baseball F5
            "spreads_1st_5_innings":{...},
            "totals_1st_5_innings": {...},
            "h2h_1st_1_innings":    {...},  # baseball 1st inning
            ...
        }
    }

Requirements:
    pip install playwright --break-system-packages
    python -m playwright install chromium
"""

import re
import random
import logging
import pathlib
import threading
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Sport URLs ────────────────────────────────────────────────────────────────
SPORT_URLS = {
    "basketball_ncaab":        "https://www.oh.bet365.com/#/AC/B18/C21097732/D48/E1453/F10/",
    "basketball_nba":          "https://www.oh.bet365.com/#/AC/B18/C20604387/D48/E1453/F10/",
    "americanfootball_nfl":    "https://www.oh.bet365.com/#/AC/B13/C20901140/D47/E1453/F10/",
    "americanfootball_ncaaf":  "https://www.oh.bet365.com/#/AC/B13/C20901141/D47/E1453/F10/",
    "baseball_mlb":            "https://www.oh.bet365.com/#/AC/B14/C20866150/D47/E1453/F10/",
    "baseball_ncaa":           "https://www.oh.bet365.com/#/AC/B16/C21129592/D48/E1096/F10/",
    "icehockey_nhl":           "https://www.oh.bet365.com/#/AC/B12/C20869654/D47/E1453/F10/",
    # Uncomment when in season (URLs to be confirmed):
    # "basketball_wnba":       "https://www.oh.bet365.com/#/AC/B18/C.../",
}

API_PATH    = "/matchmarketscontentapi/markets"
COUPON_BASE = "https://www.oh.bet365.com/matchbettingcontentapi/coupon"

# ── Market group name → market key suffix ─────────────────────────────────────
# Maps the lowercased bet365 MG NA= value to the suffix appended to the base
# market type (h2h / spreads / totals) to produce the final Odds API key.
# Full game has suffix "" → "h2h", "spreads", "totals".
# Partial markets get a suffix → "h2h_h1", "spreads_q1", etc.
MG_TO_SUFFIX: dict[str, str] = {
    # Full game
    "game lines":                   "",
    "game":                         "",
    "match lines":                  "",
    "apuestas al partido":          "",   # Spanish: "game bets / game lines"
    # Basketball / American football — 1st half
    "1st half lines":               "_h1",
    "first half lines":             "_h1",
    "1st half":                     "_h1",
    "first half":                   "_h1",
    "half time":                    "_h1",
    "1° tiempo":                    "_h1",   # Spanish: "1st half"
    "primer tiempo":                "_h1",   # Spanish alternative
    # Basketball / American football — 1st quarter
    "1st quarter lines":            "_q1",
    "first quarter lines":          "_q1",
    "1st quarter":                  "_q1",
    "first quarter":                "_q1",
    "1° cuarto":                    "_q1",   # Spanish: "1st quarter"
    "primer cuarto":                "_q1",   # Spanish alternative
    # Hockey — 1st period
    "1st period lines":             "_p1",
    "first period lines":           "_p1",
    "1st period":                   "_p1",
    "first period":                 "_p1",
    # Baseball — 1st 5 innings
    "1st 5 innings":                "_1st_5_innings",
    "first 5 innings":              "_1st_5_innings",
    "f5 innings":                   "_1st_5_innings",
    "first 5 innings lines":        "_1st_5_innings",
    "1st 5 innings lines":          "_1st_5_innings",
    # Baseball — 1st inning
    "1st inning":                   "_1st_1_innings",
    "first inning":                 "_1st_1_innings",
    "1st inning lines":             "_1st_1_innings",
    "1st inning moneyline":         "_1st_1_innings",
}

# Maps the lowercased bet365 MA NA= value to the Odds API base type prefix
MA_TO_TYPE: dict[str, str] = {
    "money":            "h2h",
    "moneyline":        "h2h",
    "match betting":    "h2h",
    "1x2":              "h2h",
    "ganador":          "h2h",      # Spanish: winner / moneyline
    "spread":           "spreads",
    "handicap":         "spreads",
    "asian handicap":   "spreads",
    "run line":         "spreads",
    "puck line":        "spreads",
    "total":            "totals",
    "over/under":       "totals",
    "totals":           "totals",
    "runs":             "totals",
}

# ── Cache ─────────────────────────────────────────────────────────────────────
_cache: dict = {}
_lock = threading.Lock()


# ── Helpers ───────────────────────────────────────────────────────────────────

def fractional_to_american(frac_str: str) -> Optional[str]:
    """'10/11' → '-110',  '11/10' → '+110',  'EVS' → '+100'"""
    if not frac_str:
        return None
    s = frac_str.strip()
    try:
        if s in ("EVS", "EVE"):
            decimal = 2.0
        elif "/" in s:
            num, den = s.split("/", 1)
            decimal = float(num) / float(den) + 1.0
        else:
            decimal = float(s)
        if decimal <= 1.0:
            return None
        if decimal >= 2.0:
            return f"+{int(round((decimal - 1) * 100))}"
        else:
            return str(int(round(-100 / (decimal - 1))))
    except (ValueError, ZeroDivisionError):
        return None


def parse_fields(segment: str) -> dict:
    fields = {}
    for part in segment.split(";"):
        if "=" in part:
            k, _, v = part.partition("=")
            fields[k.strip()] = v.strip()
    return fields


def parse_bc(bc: str) -> Optional[str]:
    """YYYYMMDDHHmmss → ISO 8601 UTC"""
    if not bc or len(bc) < 14:
        return None
    try:
        dt = datetime(
            int(bc[0:4]), int(bc[4:6]),  int(bc[6:8]),
            int(bc[8:10]), int(bc[10:12]), int(bc[12:14]),
            tzinfo=timezone.utc,
        )
        return dt.isoformat()
    except ValueError:
        return None


def _extract_bc(sport_url: str) -> tuple[Optional[str], Optional[str]]:
    """Extract B and C path parameters from a SPORT_URLS entry.
    e.g. '.../B18/C20604387/...' → ('18', '20604387')"""
    m = re.search(r"/B(\d+)/C(\d+)/", sport_url)
    if m:
        return m.group(1), m.group(2)
    return None, None


def _build_coupon_url(b: str, c: str, event_id: str) -> str:
    """Build the matchbettingcontentapi/coupon URL for one game."""
    # pd = '#AC#B{b}#C{c}#D19#E{event_id}#F19#I0#' — URL-encoded
    pd = f"%23AC%23B{b}%23C{c}%23D19%23E{event_id}%23F19%23I0%23"
    return (
        f"{COUPON_BASE}?lid=32&zid=0&pd={pd}"
        f"&cid=198&cgid=3&ctid=198&csid=52"
    )


# ── Game list parser ───────────────────────────────────────────────────────────

def parse_response(text: str, filter_past: bool = True) -> list:
    """
    Parse the pipe-delimited bet365 matchmarketscontentapi response (F| format).

    Extracts:
      - away_team / home_team / commence_time / _fi (event ID)
      - Full-game h2h and spreads as fallback (totals not on this page)

    filter_past: if True (default), drop games whose commence_time is in the past.
                 Pass False when importing from a HAR file to keep in-progress games.

    Returns list of game dicts sorted by commence_time.
    """
    records  = text.split("|")
    games    = {}    # fi -> game dict
    last_fi  = None

    for rec in records:
        rec = rec.strip()
        if not rec:
            continue

        tag, _, body = rec.partition(";")
        fields = parse_fields(body)

        if tag == "MG":
            if fields.get("SY") == "cmx" and "OI" in fields:
                last_fi = fields["OI"]
            continue

        if tag != "PA":
            continue

        # ── Game-header PA: has FD= matchup + BC= time ──────────────────────
        if "FD" in fields and "BC" in fields:
            fd = fields["FD"]
            if "@" in fd:
                away, _, home = fd.partition("@")
                away, home = away.strip(), home.strip()
            else:
                away = fields.get("NA", "")
                home = fields.get("N2", "")

            fi = fields.get("FI") or fields.get("ID", "")
            if not fi:
                continue

            # Extract the coupon event ID from the PD field (e.g. "#AC#B18#C20604387#D19#E25347036#F19#I0#").
            # This differs from FI and is what the matchbettingcontentapi/coupon endpoint expects.
            pd_field  = fields.get("PD", "")
            coupon_id_m = re.search(r'#E(\d+)#', pd_field)
            coupon_fi = coupon_id_m.group(1) if coupon_id_m else fi

            oi = fields.get("OI", "")
            games[fi] = {
                "_fi":        fi,
                "_coupon_fi": coupon_fi,
                "away_team":     away,
                "home_team":     home,
                "commence_time": parse_bc(fields.get("BC", "")),
                "_ml":  [],
                "_sp":  [],
                "_tot": [],
            }
            # Some game-list formats (e.g. NCAA Baseball) link moneyline odds
            # via the game's OI value rather than FI — add an alias so the
            # odds lookup below can find the game either way.
            if oi and oi != fi:
                games[oi] = games[fi]
            last_fi = fi
            continue

        # ── Odds PA: has OD= price ───────────────────────────────────────────
        if "OD" not in fields:
            continue

        fi = fields.get("OI") or fields.get("FI") or last_fi
        if fi not in games:
            continue

        american = fractional_to_american(fields["OD"])
        if not american:
            continue

        hd_str = fields.get("HD", "").strip()
        # Some formats (e.g. NCAA Baseball game list) put the handicap in NA=
        # instead of HD= (e.g. NA=+1.5 for a run line).  Fall back to NA when
        # HD is absent and NA looks like a numeric point value.
        if not hd_str:
            na_val = fields.get("NA", "").strip()
            if na_val and (na_val[0].isdigit() or na_val[0] in ('+', '-')):
                try:
                    float(na_val)   # verify it really is numeric
                    hd_str = na_val
                except ValueError:
                    pass

        if hd_str.startswith("O ") or hd_str.startswith("U "):
            side = "over" if hd_str.startswith("O ") else "under"
            try:
                point = float(hd_str[2:])
                games[fi]["_tot"].append({"side": side, "point": point, "odds": american})
            except ValueError:
                pass
        elif hd_str:
            try:
                hd_val = float(hd_str)
                games[fi]["_sp"].append({"point": hd_val, "odds": american})
            except ValueError:
                pass
        else:
            games[fi]["_ml"].append({"odds": american})

    # ── Consolidate into output dicts ─────────────────────────────────────────
    cutoff_str = datetime.now(timezone.utc).isoformat() if filter_past else ""
    results    = []

    for fi, game in games.items():
        ct = game["commence_time"] or ""
        if filter_past and ct < cutoff_str:
            continue

        markets: dict = {}

        # h2h — away first, home second
        ml_away = game["_ml"][0]["odds"] if len(game["_ml"]) > 0 else None
        ml_home = game["_ml"][1]["odds"] if len(game["_ml"]) > 1 else None
        if ml_away or ml_home:
            markets["h2h"] = {
                "away_odds":  ml_away,
                "home_odds":  ml_home,
                "away_point": None,
                "home_point": None,
            }

        # spreads — bet365 lists away first, home second by position
        if len(game["_sp"]) >= 2:
            markets["spreads"] = {
                "away_odds":  game["_sp"][0]["odds"],
                "away_point": game["_sp"][0]["point"],
                "home_odds":  game["_sp"][1]["odds"],
                "home_point": game["_sp"][1]["point"],
            }
        elif len(game["_sp"]) == 1:
            o = game["_sp"][0]
            if o["point"] > 0:
                markets["spreads"] = {
                    "away_point": o["point"], "away_odds": o["odds"],
                    "home_point": None,       "home_odds": None,
                }
            else:
                markets["spreads"] = {
                    "away_point": None,       "away_odds": None,
                    "home_point": o["point"], "home_odds": o["odds"],
                }

        # totals (rarely present on game-list page, but capture if available)
        tot_list = game["_tot"]
        over  = next((t for t in tot_list if t["side"] == "over"),  None)
        under = next((t for t in tot_list if t["side"] == "under"), None)
        if over or under:
            markets["totals"] = {
                "home_odds":  (over  or {}).get("odds"),   # over  = home slot
                "home_point": (over  or {}).get("point"),
                "away_odds":  (under or {}).get("odds"),   # under = away slot
                "away_point": (under or {}).get("point"),
            }

        results.append({
            "_fi":        fi,
            "_coupon_fi": game.get("_coupon_fi", fi),
            "away_team":     game["away_team"],
            "home_team":     game["home_team"],
            "commence_time": ct,
            "markets":       markets,
        })

    results.sort(key=lambda g: (g["commence_time"], g["away_team"]))
    return results


# ── Coupon parser (per-game, all markets) ─────────────────────────────────────

def parse_coupon(text: str) -> dict:
    """
    Parse a bet365 matchbettingcontentapi/coupon response (MG/MA/PA format).

    Returns a markets dict keyed by Odds API-style names:
        'h2h', 'spreads', 'totals',
        'h2h_h1', 'spreads_h1', 'totals_h1',
        'h2h_q1', 'spreads_q1', 'totals_q1', ...

    All values use the same shape: {away_odds, home_odds, away_point, home_point}.
    For h2h, points are None.  For totals, home=over, away=under.
    """
    if not text or not text.strip():
        return {}

    import unicodedata as _ud

    def _ascii_fold(s: str) -> str:
        """Strip diacritics and non-ASCII for robust MA_TO_TYPE lookup.
        e.g. 'hándicap' (any encoding) → 'handicap', 'ganador' → 'ganador'."""
        nfkd = _ud.normalize("NFKD", s)
        return "".join(c for c in nfkd if _ud.category(c) != "Mn" and ord(c) < 128)

    records  = text.split("|")
    result   = {}
    suffix   = None   # current MG suffix (None = skip / unknown group)
    mtype    = None   # current MA type ('h2h' / 'spreads' / 'totals' / None)
    outcomes: list = []

    def commit() -> None:
        """Flush the current MA's accumulated outcomes into result."""
        if suffix is None or mtype is None or not outcomes:
            return
        key = mtype + suffix

        if mtype == "h2h":
            result[key] = {
                "away_odds":  outcomes[0]["odds"] if len(outcomes) > 0 else None,
                "home_odds":  outcomes[1]["odds"] if len(outcomes) > 1 else None,
                "away_point": None,
                "home_point": None,
            }
        elif mtype == "spreads":
            result[key] = {
                "away_odds":  outcomes[0].get("odds")  if len(outcomes) > 0 else None,
                "away_point": outcomes[0].get("point") if len(outcomes) > 0 else None,
                "home_odds":  outcomes[1].get("odds")  if len(outcomes) > 1 else None,
                "home_point": outcomes[1].get("point") if len(outcomes) > 1 else None,
            }
        elif mtype == "totals":
            over  = next((o for o in outcomes if o.get("side") == "over"),  None)
            under = next((o for o in outcomes if o.get("side") == "under"), None)
            result[key] = {
                "home_odds":  (over  or {}).get("odds"),   # over  = home slot
                "home_point": (over  or {}).get("point"),
                "away_odds":  (under or {}).get("odds"),   # under = away slot
                "away_point": (under or {}).get("point"),
            }

    for rec in records:
        rec = rec.strip()
        if not rec:
            continue

        tag, _, body = rec.partition(";")
        fields = parse_fields(body)

        if tag == "MG":
            commit()
            outcomes.clear()
            mtype  = None
            na     = fields.get("NA", "").strip().lower()
            suffix = MG_TO_SUFFIX.get(na)   # None if unrecognised group
            if suffix is None and na:
                logger.debug(f"bet365 coupon: skipping unrecognised MG NA={na!r}")

        elif tag == "MA":
            commit()
            outcomes.clear()
            if suffix is None:
                mtype = None
                continue
            na    = fields.get("NA", "").strip().lower()
            mtype = MA_TO_TYPE.get(na) or MA_TO_TYPE.get(_ascii_fold(na))
            if mtype is None and na:
                logger.debug(f"bet365 coupon: skipping unrecognised MA NA={na!r} (under MG suffix={suffix!r})")

        elif tag == "PA" and "OD" in fields:
            if suffix is None or mtype is None:
                continue
            american = fractional_to_american(fields["OD"])
            if not american:
                continue
            hd = fields.get("HD", "").strip()
            outcome: dict = {"odds": american}
            if hd.startswith("O "):
                try:
                    outcome.update({"point": float(hd[2:]), "side": "over"})
                except ValueError:
                    pass
            elif hd.startswith("U "):
                try:
                    outcome.update({"point": float(hd[2:]), "side": "under"})
                except ValueError:
                    pass
            elif hd:
                try:
                    outcome["point"] = float(hd)
                except ValueError:
                    pass
            outcomes.append(outcome)

    commit()  # flush final MA
    return result


# ── Playwright fetch ───────────────────────────────────────────────────────────

LOGIN_URL = "https://www.oh.bet365.com/#/HO/"


def fetch_bet365(sport_key: str, timeout: int = 60, login_event=None) -> Optional[list]:
    """
    Launch a visible Chrome window, optionally wait for the user to log in,
    then navigate bet365 to collect odds.

    Step 1 — Game list: intercept matchmarketscontentapi XHR → team names,
             commence times, event IDs, and fallback h2h/spreads.
    Step 2 — Coupons: fire all per-game coupon requests in parallel inside the
             browser (same session/cookies → no extra Cloudflare challenge).
             Merges full game totals + partial markets into each game dict.
    """
    url = SPORT_URLS.get(sport_key)
    if not url:
        logger.warning(f"bet365: unsupported sport {sport_key!r}")
        return None

    b_param, c_param = _extract_bc(url)
    if not b_param or not c_param:
        logger.error(f"bet365: cannot parse B/C params from URL {url!r}")
        return None

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error(
            "bet365: playwright not installed — "
            "run: pip install playwright && python -m playwright install chromium"
        )
        return None

    captured_text: Optional[str] = None
    games: list = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                timezone_id="America/New_York",
                viewport={"width": 1440, "height": 900},
            )
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = context.new_page()

            # ── Wait for user login if requested ─────────────────────────────
            if login_event is not None:
                page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
                logger.info("bet365: browser open at login page — waiting for confirmation")
                login_event.wait(timeout=300)  # up to 5 min
                if not login_event.is_set():
                    logger.warning("bet365: login confirmation timed out")
                    browser.close()
                    return None
                logger.info("bet365: login confirmed — starting scrape")

            # ── Step 1: Game list ─────────────────────────────────────────────
            with page.expect_response(
                lambda r: API_PATH in r.url and r.status == 200,
                timeout=timeout * 1000,
            ) as response_info:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)

            captured_text = response_info.value.text()
            logger.info(
                f"bet365: game list captured ({len(captured_text)} bytes) for {sport_key}"
            )

            if not captured_text.strip().startswith("F|"):
                logger.warning(
                    f"bet365: unexpected game list format "
                    f"(preview: {captured_text[:80]!r})"
                )
                browser.close()
                return None

            games = parse_response(captured_text)
            logger.info(f"bet365: {sport_key}: {len(games)} upcoming games from game list")

            # ── Step 2: Per-game coupons — navigate to each game's detail page ──
            # We intercept the matchbettingcontentapi/coupon XHR that bet365's
            # own JS fires on each game page.  Hash-based navigation is fast
            # (no full reload) so this adds only a few seconds per game.
            fi_list = [g["_fi"] for g in games if g.get("_fi")]
            if fi_list:
                try:
                    # Map each _fi to its coupon text as responses arrive
                    fi_to_coupon: dict = {}

                    for game in games:
                        fi        = game.get("_fi")
                        coupon_fi = game.get("_coupon_fi", fi)
                        if not fi:
                            continue

                        nav_url = (
                            f"https://www.oh.bet365.com/"
                            f"#/AC/B{b_param}/C{c_param}/D19/E{coupon_fi}/F19/"
                        )
                        try:
                            # Collect ALL coupon responses during this navigation —
                            # bet365 sometimes fires a bare CL-header response first
                            # and the actual market data arrives in a second response.
                            collected: list[str] = []
                            all_urls: list[str]  = []

                            def _on_response(resp, _fi=fi):
                                all_urls.append(f"{resp.status} {resp.url[:120]}")
                                if "matchbettingcontentapi/coupon" in resp.url and resp.status == 200:
                                    try:
                                        collected.append(resp.text())
                                    except Exception:
                                        pass

                            page.on("response", _on_response)
                            try:
                                page.goto(nav_url, wait_until="domcontentloaded", timeout=30_000)
                                # Human-paced wait: 3–6 s so bet365 sees normal browsing
                                delay_ms = int(random.uniform(3_000, 6_000))
                                page.wait_for_timeout(delay_ms)
                            finally:
                                page.remove_listener("response", _on_response)

                            # Pick the response that actually contains MG market records
                            best = max(collected, key=lambda t: t.count("|MG;"), default=None)
                            if best and "|MG;" in best:
                                fi_to_coupon[fi] = best
                                logger.debug(f"bet365: coupon fi={fi} ({len(best)} chars, {len(collected)} responses)")
                            elif collected:
                                logger.info(f"bet365: coupon for {game.get('away_team')} @ {game.get('home_team')} had no MG records "
                                            f"(responses: {len(collected)}, sizes: {[len(t) for t in collected]}, "
                                            f"previews: {[t[:80] for t in collected]})")
                                logger.info(f"bet365: all URLs during nav: {all_urls}")
                        except Exception as ge:
                            logger.info(f"bet365: coupon nav failed for {game.get('away_team')} @ {game.get('home_team')} (fi={fi}): {type(ge).__name__}: {str(ge)[:120]}")

                    merged = 0
                    for game in games:
                        fi = game.get("_fi")
                        if not fi:
                            continue
                        coupon_text = fi_to_coupon.get(fi)
                        label = f"{game.get('away_team')} @ {game.get('home_team')}"
                        if not coupon_text:
                            logger.info(f"bet365: no coupon text for {label} (fi={fi})")
                            continue
                        coupon_markets = parse_coupon(coupon_text)
                        if coupon_markets:
                            game["markets"].update(coupon_markets)
                            merged += 1
                            logger.debug(
                                f"bet365: fi={fi} markets={sorted(coupon_markets)}"
                            )

                    logger.info(
                        f"bet365: {sport_key}: coupon data merged for "
                        f"{merged}/{len(fi_list)} games"
                    )

                except Exception as e:
                    logger.warning(
                        f"bet365: coupon batch failed ({sport_key}): {e} — "
                        f"continuing with game-list odds only"
                    )

            browser.close()

    except Exception as e:
        logger.error(f"bet365: Playwright error ({sport_key}): {e}")
        return None

    with _lock:
        _cache[sport_key] = {
            "data":         games,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "error":        None,
        }

    logger.info(f"bet365 {sport_key}: cached {len(games)} games")
    return games


def get_cached(sport_key: str) -> dict:
    with _lock:
        return _cache.get(sport_key, {"data": [], "last_updated": None, "error": None})


# ── Standalone test ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    import sys
    sport = sys.argv[1] if len(sys.argv) > 1 else "basketball_ncaab"
    print(f"Testing bet365 scraper — {sport}")
    print("=" * 60)
    games = fetch_bet365(sport)

    if not games:
        print("Failed — see log above.")
        sys.exit(1)

    print(f"\nFound {len(games)} upcoming games\n")
    for g in games[:10]:
        mkts = g.get("markets", {})
        h2h  = mkts.get("h2h")    or {}
        sp   = mkts.get("spreads") or {}
        tot  = mkts.get("totals")  or {}
        h1   = mkts.get("h2h_h1") or {}
        q1   = mkts.get("h2h_q1") or {}
        p1   = mkts.get("h2h_p1") or {}
        f5   = mkts.get("h2h_1st_5_innings") or {}
        print(f"  {g['away_team']} @ {g['home_team']}  [{g['commence_time']}]  fi={g['_fi']}")
        print(f"    ML:      away={h2h.get('away_odds')}  home={h2h.get('home_odds')}")
        print(f"    Spread:  away={sp.get('away_point')} {sp.get('away_odds')}  "
              f"home={sp.get('home_point')} {sp.get('home_odds')}")
        print(f"    Total:   O{tot.get('home_point')} {tot.get('home_odds')}  "
              f"U{tot.get('away_point')} {tot.get('away_odds')}")
        if h1:
            print(f"    1H ML:   away={h1.get('away_odds')}  home={h1.get('home_odds')}")
        if q1:
            print(f"    Q1 ML:   away={q1.get('away_odds')}  home={q1.get('home_odds')}")
        if p1:
            print(f"    P1 ML:   away={p1.get('away_odds')}  home={p1.get('home_odds')}")
        if f5:
            print(f"    F5 ML:   away={f5.get('away_odds')}  home={f5.get('home_odds')}")
        all_keys = sorted(mkts)
        print(f"    Markets: {all_keys}")
        print()
