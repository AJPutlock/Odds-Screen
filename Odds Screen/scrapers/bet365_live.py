"""
bet365 Live Collector (10/12)
=============================
odds-api.io's bet365 runs behind the site, so the board's bet365 column comes
from the user's own logged-in session: a real Chrome on the bet365_profile/
profile walks the league pages a person would click through and keeps the
matchmarketscontentapi bodies the site fetches for itself. Those go to
parse_har.games_from_texts(), so a live pull, a Charles HAR and the old
scraper share every parser.

bet365 allows ONE session per account and the user bets on their phone, so
unlike Bookmaker this collector never stays signed in (user, 10/12):
  * One browser and one sign-in per run (run_session): sign in, collect every
    sport in the plan, then LOG OUT in a `finally` (Account -> Log Out), check
    that the header shows "Log In" again, retry once. The caller is told when
    the logout couldn't be confirmed (app.py sends a Telegram warning).
  * Signed in for at most MAX_SIGNED_IN_SECONDS; the run stops between tabs
    when it's up, and still logs out.
  * Nothing that would carry the login across runs: no session-cookie
    snapshot, no --restore-last-session. The profile is kept only for bet365's
    remember-this-device cookie, so 2FA stays rare.
  * Signing in: the 365 LOGIN window (this profile on-screen, the user types),
    or BET365_USERNAME / BET365_PASSWORD from .env typed by the code (never
    logged; once per 30 min at most, paused 6 h after a failure).
The header probes (login state, Log Out entry) and the login-form selectors
are informed guesses from a signed-out page until the supervised capture run;
a probe miss reports 'unknown', never a false 'logged_out'.

Pages per sport are the league page's tabs, each a `pd` path. The game-lines
response lists the tabs it offers (MA records with NA= and PD=), and those win
over the codes below, read off the 9/21-9/23 captures. A league whose C= id
moves (a new season) needs its id updated in SPORTS.
"""

import os
import re
import time
import random
import logging
import pathlib
import threading
from typing import Optional

logger = logging.getLogger(__name__)

SITE = "https://www.oh.bet365.com"
HOME_URL = f"{SITE}/#/HO/"
ODDS_PATH = "matchmarketscontentapi"

PROFILE_DIR = pathlib.Path(__file__).resolve().parent.parent / "bet365_profile"
MAX_SIGNED_IN_SECONDS = 4 * 60

# Tab label (as the page's nav names it, lowercased) -> pd tail after the league's #AC#B#C#.
_FOOTBALL_TABS = [
    ("game lines",            "D48#E1441#F36#N0#"),
    ("spread",                "D47#E120504#F47#N0#"),
    ("total",                 "D47#E120508#F47#N0#"),
    ("1st half",              "D48#E120595#F21#N4#"),
    ("1st half point spread", "D47#E121202#F47#N4#"),
    ("1st half total",        "D47#E121201#F47#N4#"),
    ("1st quarter",           "D48#E120659#F21#N4#"),
]
_NBA_TABS = [
    ("game lines",  "D48#E1453#F10#"),
    ("1st half",    "D48#E928#F40#N4#"),
    ("1st quarter", "D48#E941#F30#N0#"),
]
SPORTS: dict = {
    "americanfootball_nfl":     {"league": "#AC#B12#C20426855#", "tabs": _FOOTBALL_TABS},
    "americanfootball_ncaaf":   {"league": "#AC#B12#C20437885#", "tabs": _FOOTBALL_TABS},
    "basketball_nba":           {"league": "#AC#B18#C20604387#", "tabs": _NBA_TABS},
    "basketball_nba_preseason": {"league": "#AC#B18#C20604387#", "tabs": _NBA_TABS},   # preseason sits on the NBA page
    "basketball_ncaab":         {"league": "#AC#B18#C21097732#", "tabs": [("game lines", "D48#E1453#F10#")]},   # id from the old scraper, unconfirmed
    "icehockey_nhl":            {"league": "#AC#B17#C20836572#", "tabs": [("game lines", "D48#E972#F10#")]},
    "baseball_mlb":             {"league": "#AC#B16#C20525425#", "tabs": [
        ("game lines",      "D48#E1096#F10#"),
        ("5 innings",       "D48#E160025#F20#N3#"),
        ("1st inning runs", "D29#E163105#F29#N3#"),
    ]},
}
MAIN_TABS = ("game lines",)   # main_only pulls: every other tab is alts / periods

_cache: dict = {}             # sport_key -> {"login_state", "last_updated", "games"}
_lock = threading.Lock()
_browser_lock = threading.Lock()   # one Chrome per profile


# ── Login state / logout ─────────────────────────────────────────────────────
# Signed out, the header shows "Join" and "Log In" buttons (hrm-* classes,
# seen 10/12 on a fresh profile). With the header drawn and no Log In button
# it reads as signed in; before the header draws, 'unknown'.
_LOGIN_STATE_JS = """() => {
  const vis = e => !!(e.offsetWidth || e.offsetHeight);
  const header = [...document.querySelectorAll('[class*="hrm-"]')].filter(vis);
  if (!header.length) return 'unknown';
  const loginBtn = [...document.querySelectorAll('button')].some(b => vis(b) && /^log ?in$/i.test((b.innerText || '').trim()));
  return loginBtn ? 'logged_out' : 'logged_in';
}"""

# The account menu that holds "Log Out": the first visible header control that
# looks like the member / account / balance area. Unverified (step 2 capture).
_OPEN_ACCOUNT_MENU_JS = """() => {
  const vis = e => !!(e.offsetWidth || e.offsetHeight);
  const cands = [...document.querySelectorAll('[class*="hrm-"], header *')].filter(e =>
    vis(e) && /member|account|balance|user|profile/i.test((e.className || '').toString() + ' ' + (e.getAttribute('aria-label') || '')));
  if (!cands.length) return false;
  cands[0].click();
  return true;
}"""
_LOGOUT_TEXT = re.compile(r"^\s*Log ?Out\s*$", re.I)


def _login_state(page) -> str:
    try:
        return page.evaluate(_LOGIN_STATE_JS)
    except Exception as e:
        logger.debug(f"bet365: login-state probe failed: {e}")
        return "unknown"


def _wait_ready(page, max_ms=20_000) -> None:
    """Until bet365's full-page preloader has gone (it blocks clicks and the
    league page's own requests while it's up)."""
    waited = 0
    while waited < max_ms:
        try:
            if page.evaluate("() => { const p = document.getElementById('__-plContainer');"
                             " return !p || getComputedStyle(p).display === 'none'; }"):
                return
        except Exception:
            pass
        page.wait_for_timeout(500)
        waited += 500


def _click_logout_entry(page) -> bool:
    try:
        entry = page.get_by_text(_LOGOUT_TEXT).first
        if entry.count() and entry.is_visible():
            entry.click(timeout=3000)
            return True
    except Exception:
        pass
    return False


def _logout(page) -> bool:
    """Account -> Log Out, then wait for the header's Log In button; one retry
    from a fresh home page. True only when the signed-out header is seen."""
    for attempt in (1, 2):
        try:
            if attempt == 2:
                page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30_000)
                _wait_ready(page)
            if _login_state(page) == "logged_out":
                return True
            if not _click_logout_entry(page):
                page.evaluate(_OPEN_ACCOUNT_MENU_JS)
                page.wait_for_timeout(1200)
                _click_logout_entry(page)
            for _ in range(20):                      # up to 10 s for the header to flip
                page.wait_for_timeout(500)
                if _login_state(page) == "logged_out":
                    return True
        except Exception as e:
            print(f"[bet365] log out attempt {attempt} failed: {type(e).__name__}", flush=True)
    return _login_state(page) == "logged_out"


# ── Automatic sign-in from .env (optional) ──────────────────────────────────
AUTO_LOGIN_MIN_GAP = 30 * 60
AUTO_LOGIN_FAIL_PAUSE = 6 * 3600
_auto_login = {"last": 0.0, "failed_at": 0.0}


def _credentials():
    try:
        from dotenv import load_dotenv
        load_dotenv(PROFILE_DIR.parent / ".env", override=True)
    except ImportError:
        pass
    return os.environ.get("BET365_USERNAME", "").strip(), os.environ.get("BET365_PASSWORD", "")


def auto_login_configured() -> bool:
    user, pw = _credentials()
    return bool(user and pw)


def _try_auto_login(page, why: str) -> bool:
    """Open bet365's Log In form and submit the .env credentials. True when
    the page is signed in afterwards. Never logs the values."""
    user, pw = _credentials()
    if not (user and pw):
        return False
    now = time.time()
    if now - _auto_login["failed_at"] < AUTO_LOGIN_FAIL_PAUSE:
        print("[bet365] auto sign-in paused after a failed attempt — use 365 LOGIN", flush=True)
        return False
    if now - _auto_login["last"] < AUTO_LOGIN_MIN_GAP:
        return False
    _auto_login["last"] = now
    print(f"[bet365] auto sign-in ({why})", flush=True)
    try:
        _wait_ready(page)
        page.locator("button", has_text=re.compile(r"^\s*Log ?In\s*$", re.I)).first.click(timeout=10_000)
        pwd = page.locator("input[type=password]:visible").first
        pwd.wait_for(state="visible", timeout=15_000)
        # The username box is the visible text/email field before the password.
        acct = page.locator("input:not([type=password]):not([type=hidden]):not([type=checkbox]):visible").first
        acct.click()
        acct.press_sequentially(user, delay=60)
        pwd.click()
        pwd.press_sequentially(pw, delay=60)
        pwd.press("Enter")
        for _ in range(40):                      # up to ~20 s
            page.wait_for_timeout(500)
            if _login_state(page) == "logged_in":
                break
    except Exception as e:
        print(f"[bet365] auto sign-in couldn't use the form: {type(e).__name__}", flush=True)
    ok = _login_state(page) == "logged_in"
    if ok:
        print("[bet365] auto sign-in OK", flush=True)
    else:
        _auto_login["failed_at"] = now
        print("[bet365] auto sign-in FAILED (wrong .env values, a 2FA prompt, or a changed form) — "
              "paused 6 h; 365 LOGIN still works by hand", flush=True)
    return ok


# ── Collection ───────────────────────────────────────────────────────────────

def _nav_pds(text: str) -> dict:
    """{tab label lowercased: pd} from a league page's nav block."""
    out = {}
    for rec in text.split("|"):
        if not rec.startswith("MA;") or "PD=" not in rec:
            continue
        f = {}
        for part in rec[3:].split(";"):
            k, _, v = part.partition("=")
            f[k.strip()] = v.strip()
        na, pd = f.get("NA", "").strip().lower(), f.get("PD", "")
        if na and pd.startswith("#AC#") and na not in out:
            out[na] = pd
    return out


def _hash_for(pd: str) -> str:
    """'#AC#B12#C1#D48#E1441#F36#N0#' -> '#/AC/B12/C1/D48/E1441/F36/N0/'"""
    return "#" + pd.replace("#", "/")


def _collect_with(page, sport_key: str, main_only: bool, deadline: float, timeout: int = 30) -> Optional[list]:
    """Walk one sport's tabs in a signed-in page, stopping between tabs once
    `deadline` (epoch) passes; game dicts or None."""
    cfg = SPORTS.get(sport_key)
    if not cfg:
        logger.warning(f"bet365: no league configured for {sport_key!r}")
        return None
    from scrapers.parse_har import games_from_texts, _b365_selected_tab

    tabs = [(t, cfg["league"] + tail) for t, tail in cfg["tabs"] if not main_only or t in MAIN_TABS]
    bodies: list = []          # (url, text)

    def _on_response(resp):
        if resp.status != 200 or ODDS_PATH not in resp.url:
            return
        try:
            bodies.append((resp.url, resp.text()))
        except Exception as e:
            logger.debug(f"bet365: could not read response body: {e}")

    page.on("response", _on_response)
    got_tabs = []
    try:
        discovered: dict = {}
        for i, (tab, pd) in enumerate(tabs):
            if time.time() > deadline:
                print(f"[bet365] {sport_key}: signed-in time cap reached — stopping before {tab!r}", flush=True)
                break
            pd = discovered.get(tab, pd)
            for attempt in (1, 2):
                n_before = len(bodies)
                try:
                    if attempt == 1:
                        page.evaluate("h => { location.hash = h; }", _hash_for(pd)[1:])
                    else:          # the in-page route change brought nothing: load the URL outright
                        page.goto(SITE + "/" + _hash_for(pd), wait_until="domcontentloaded", timeout=timeout * 1000)
                        _wait_ready(page)
                except Exception as e:
                    logger.warning(f"bet365: navigation failed for {sport_key} {tab}: {type(e).__name__}: {str(e)[:120]}")
                    continue
                for _ in range(30):                      # up to 15 s for this tab's own response
                    if len(bodies) > n_before:
                        break
                    page.wait_for_timeout(500)
                page.wait_for_timeout(1200)              # its follow-up chunks, if any
                new = bodies[n_before:]
                if new:
                    got = {_b365_selected_tab(t) for _, t in new}
                    got_tabs.append(tab if tab in got else f"{tab}→{'/'.join(sorted(g for g in got if g)) or '?'}")
                    for _, t in new:
                        for name, npd in _nav_pds(t).items():
                            discovered.setdefault(name, npd)
                    break
            else:
                print(f"[bet365] {sport_key} {tab}: nothing came back", flush=True)
                if i == 0:
                    break                                # no league page: the other tabs won't load either
            page.wait_for_timeout(int(random.uniform(1500, 3500)))
    finally:
        try:
            page.remove_listener("response", _on_response)
        except Exception:
            pass

    login_state = _login_state(page)
    with _lock:
        _cache[sport_key] = {**(_cache.get(sport_key) or {}), "login_state": login_state}
    if not bodies:
        print(f"[bet365] {sport_key}: nothing captured (login={login_state})", flush=True)
        return None
    games = games_from_texts(bodies)
    print(f"[bet365] {sport_key}: {len(bodies)} response(s) [{', '.join(got_tabs)}] → {len(games)} games "
          f"(login={login_state})", flush=True)
    with _lock:
        _cache[sport_key].update(last_updated=time.time(), games=len(games))
    return games or None


def _launch(pw, onscreen: bool):
    PROFILE_DIR.mkdir(exist_ok=True)
    context = pw.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=False,                           # a real window: the lower-risk shape, as for Bookmaker
        channel="chrome",
        args=["--no-sandbox", "--disable-blink-features=AutomationControlled",
              "--window-position=60,40" if onscreen else "--window-position=-2400,-2400",
              "--window-size=1440,900"],
        locale="en-US",
        timezone_id="America/New_York",
        **({"no_viewport": True} if onscreen else {"viewport": {"width": 1440, "height": 900}}),
    )
    context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    page = context.pages[0] if context.pages else context.new_page()
    for extra in context.pages[1:]:
        extra.close()
    return context, page


def _move_offscreen(context, page) -> None:
    try:
        cdp = context.new_cdp_session(page)
        win = cdp.send("Browser.getWindowForTarget")
        cdp.send("Browser.setWindowBounds", {"windowId": win["windowId"],
                                              "bounds": {"left": -2400, "top": -2400, "width": 1440, "height": 900}})
    except Exception:
        pass


def _raise_window() -> bool:
    try:
        from scrapers.bookmaker_live import _raise_window as raise_by_title
        return raise_by_title("bet365")
    except Exception:
        return False


LOGIN_WINDOW_MAX_SECONDS = 15 * 60
_focus_request = threading.Event()
_window_closed = threading.Event()


def request_login_window_focus() -> None:
    _focus_request.set()


def _wait_for_manual_sign_in(context, page) -> bool:
    """365 LOGIN: the window is on-screen; wait for the user to sign in (or
    close it). True once the header reads signed in."""
    from playwright.sync_api import TimeoutError as PWTimeout
    deadline = time.time() + LOGIN_WINDOW_MAX_SECONDS
    last = None
    while time.time() < deadline:
        state = _login_state(page)
        if state != last:
            print(f"[bet365] login window: {state}", flush=True)
            last = state
        if state == "logged_in":
            return True
        try:
            context.wait_for_event("close", timeout=1000)
            _window_closed.set()
            print("[bet365] login window: closed by the user", flush=True)
            return False
        except PWTimeout:
            pass
        if not context.pages:
            _window_closed.set()
            return False
        if _focus_request.is_set():
            _focus_request.clear()
            _raise_window()
    print("[bet365] login window: nobody signed in within 15 min", flush=True)
    return False


def run_session(plan, on_result=None, manual_login: bool = False) -> dict:
    """
    One sign-in for a whole run: open the profile (on-screen for manual_login,
    the 365 LOGIN window; off-screen otherwise, signing in from .env), collect
    each (sport_key, main_only) in `plan` -- calling on_result(sport_key,
    games or None) as each lands -- for at most MAX_SIGNED_IN_SECONDS, then
    log out (always, in a finally) and close Chrome.

    Returns {"signed_in": bool, "logged_out": bool | None, "error": str | None,
    "done": [sport_key, ...]}. logged_out is None when it never signed in,
    False when the logout couldn't be confirmed.
    """
    out = {"signed_in": False, "logged_out": None, "error": None, "done": []}
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        out["error"] = "playwright not installed"
        return out
    _window_closed.clear()
    with _browser_lock:
        try:
            with sync_playwright() as pw:
                context, page = _launch(pw, onscreen=manual_login)
                try:
                    if manual_login:
                        page.bring_to_front()
                        _raise_window()
                    page.goto(HOME_URL, wait_until="domcontentloaded", timeout=60_000)
                    _wait_ready(page)
                    page.wait_for_timeout(1500)
                    state = _login_state(page)
                    if state == "logged_in":
                        print("[bet365] already signed in when the browser opened", flush=True)
                    if state != "logged_in" and auto_login_configured():
                        if manual_login:
                            _auto_login["last"] = 0.0     # an explicit click may try right away
                        _try_auto_login(page, "365 LOGIN" if manual_login else "365 NOW")
                        state = _login_state(page)
                    if state != "logged_in" and manual_login:
                        _raise_window()
                        _focus_request.clear()
                        state = "logged_in" if _wait_for_manual_sign_in(context, page) else state
                    if state != "logged_in":
                        out["error"] = ("Not signed in — the window was closed" if _window_closed.is_set()
                                        else "Not signed in — use 365 LOGIN" if not auto_login_configured() or manual_login
                                        else "Automatic sign-in failed — use 365 LOGIN")
                        return out
                    out["signed_in"] = True
                    deadline = time.time() + MAX_SIGNED_IN_SECONDS
                    if manual_login:
                        page.wait_for_timeout(1500)
                        _move_offscreen(context, page)
                    print(f"[bet365] signed in — collecting {[sk for sk, _ in plan]} "
                          f"(cap {MAX_SIGNED_IN_SECONDS // 60} min)", flush=True)
                    for sk, main_only in plan:
                        if time.time() > deadline:
                            print(f"[bet365] signed-in time cap reached — skipping {sk} and the rest", flush=True)
                            break
                        games = None
                        try:
                            games = _collect_with(page, sk, main_only, deadline)
                        except Exception as e:
                            print(f"[bet365] {sk}: collection error {type(e).__name__}: {str(e)[:160]}", flush=True)
                        out["done"].append(sk)
                        if on_result:
                            try:
                                on_result(sk, games)
                            except Exception as e:
                                print(f"[bet365] {sk}: result handler error {type(e).__name__}: {e}", flush=True)
                    return out
                finally:
                    # Never leave the PC signed in: bet365 allows one session and
                    # the user bets on their phone. Attempted whenever the page
                    # isn't plainly signed out (a half-finished sign-in included).
                    if not _window_closed.is_set():
                        try:
                            if out["signed_in"] or _login_state(page) != "logged_out":
                                ok = _logout(page)
                                out["logged_out"] = ok
                                print(f"[bet365] log out {'confirmed' if ok else 'NOT confirmed'}", flush=True)
                        except Exception as e:
                            out["logged_out"] = False
                            print(f"[bet365] log out failed: {type(e).__name__}", flush=True)
                    elif out["signed_in"]:
                        out["logged_out"] = False      # closed while signed in: can't check
                    try:
                        page.goto("about:blank", timeout=10_000)
                    except Exception:
                        pass
                    try:
                        context.close()
                    except Exception:
                        pass
        except Exception as e:
            out["error"] = f"browser error {type(e).__name__}: {str(e)[:160]}"
            print(f"[bet365] {out['error']}", flush=True)
            if out["signed_in"] and out["logged_out"] is None:
                out["logged_out"] = False
            return out


def last_login_state(sport_key: str = None) -> str:
    """Login state seen on this sport's last pull (any sport's when None)."""
    with _lock:
        if sport_key:
            return (_cache.get(sport_key) or {}).get("login_state", "unknown")
        seen = sorted(((v.get("last_updated") or 0, v.get("login_state")) for v in _cache.values()), reverse=True)
        return seen[0][1] if seen else "unknown"
