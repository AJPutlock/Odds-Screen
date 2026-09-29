"""
Bookmaker.eu Live Collector
===========================
Replaces the manual Charles-HAR flow for bookmaker.eu: drives a real Chrome
instance through the same sport tabs you would browse by hand, captures the
GetSchedule / GetGameView XHRs the site fires on its own, and feeds the raw
bodies to the existing parsers in parse_bookmaker_har.py.

Nothing about the parsing changes — games_from_bookmaker_texts() is the shared
entry point, so the HAR importer and this collector produce byte-identical
output from the same responses.

Why navigate rather than replay the POST directly: the request body carries a
LeaguesIdList that differs per sport and has been seen to change (NCAA alone
used 2 / 108 / 12072 / 13483 / 13484 across three captures). Letting the page
issue its own request keeps this working when those ids move, and keeps the
cookies/headers exactly as the site expects.

Setup (once):
    python login_bookmaker.py       # log in, session saved to bookmaker_profile/

Usage:
    from scrapers.bookmaker_live import fetch_bookmaker
    games = fetch_bookmaker("americanfootball_ncaaf")
"""

import os
import time
import random
import logging
import pathlib
import threading
from typing import Optional

logger = logging.getLogger(__name__)

BASE = "https://be.bookmaker.eu/en/sports"

# Sport → the tab pages worth loading, in order. Each one fires its own
# GetSchedule; the parser merges period markets onto the full-game entries
# afterwards, which is why the game-lines tab must always come first (see
# _parse_raw_games' docstring — a halves-only response has no gp=='0' entries
# of its own to attach to).
SPORT_PAGES: dict[str, list[str]] = {
    "americanfootball_ncaaf": [
        f"{BASE}/football/ncaa/game-lines/",
        f"{BASE}/football/ncaa/1st-halves/",
        f"{BASE}/football/ncaa/quarters/",
        f"{BASE}/football/ncaa/extra-games/",
    ],
    "americanfootball_nfl": [
        f"{BASE}/football/nfl/game-lines/",
        f"{BASE}/football/nfl/1st-halves/",
        f"{BASE}/football/nfl/quarters/",
    ],
    "baseball_mlb": [
        f"{BASE}/baseball/mlb/game-lines/",
        f"{BASE}/baseball/mlb/1st-5-innings/",
        f"{BASE}/baseball/mlb/innings/",
    ],
    "basketball_nba": [
        f"{BASE}/basketball/nba/game-lines/",
    ],
    "icehockey_nhl": [
        f"{BASE}/hockey/nhl/game-lines/",
    ],
    # NCAAB tab slug unconfirmed — no capture on hand. Add once verified.
}

ODDS_PATHS = ("BetslipProxy.aspx/GetSchedule", "BetslipProxy.aspx/GetGameView")

PROFILE_DIR = pathlib.Path(__file__).resolve().parent.parent / "bookmaker_profile"
LOGIN_URL   = f"{BASE}/football/nfl/game-lines/"

# The login only outlasts the browser when "Remember me" is ticked — without
# it the session rides on ASP_NET_SessionId, a session cookie Chrome deletes on
# close, so every run opened to the Log In form. Signed-out pages show the
# login form's own text; element-class probes matched it too and reported
# "logged_in" on a login page, so read the text instead.
_LOGGED_OUT_TEXT = ("Can't access your account", "Can’t access your account")

_cache: dict = {}
_lock = threading.Lock()
# One Chrome per profile at a time — a second launch on the same profile dies
# at once (exit code 21), so a manual pull during a scheduled one waits here.
_browser_lock = threading.Lock()


# bookmaker.eu has no remember-me: the login is ASP_NET_SessionId, a session
# cookie Chrome drops on every new launch unless it restores the last session.
# The --restore-last-session switch makes it keep them (and reopen last run's
# tab, which fetch_bookmaker closes). Writing "restore_on_startup" into the
# profile's Preferences instead does NOT hold: it's a protected setting and
# Chrome silently reset it (2026-09-25), logging the collector out.
SESSION_ARGS = ["--restore-last-session"]


def _login_state(page) -> str:
    """'logged_out' when the login form is showing, else 'unknown' — never raises.
    fetch_bookmaker upgrades 'unknown' to 'logged_in' once odds actually arrive."""
    try:
        text = page.inner_text("body", timeout=3000)
        if any(t in text for t in _LOGGED_OUT_TEXT):
            return "logged_out"
    except Exception as e:
        logger.debug(f"bookmaker: login-state probe failed: {e}")
    return "unknown"


def fetch_bookmaker(sport_key: str, timeout: int = 60,
                    headless: bool = False) -> Optional[list]:
    """Collect one sport's odds; serialized — see _browser_lock."""
    with _browser_lock:
        return _fetch_bookmaker(sport_key, timeout, headless)


def _fetch_bookmaker(sport_key: str, timeout: int = 60,
                     headless: bool = False) -> Optional[list]:
    """
    Collect bookmaker.eu odds for one sport. Returns game dicts in the same
    shape games_from_bookmaker_har() produces, or None on failure.

    Runs a normal (headed) Chrome window parked off-screen: Cloudflare in front
    of bookmaker.eu blocks headless Chrome outright (403 on every odds request,
    "The action cannot be completed at this time"), while the same profile in a
    regular window loads normally.
    """
    pages = SPORT_PAGES.get(sport_key)
    if not pages:
        logger.warning(f"bookmaker: no page list for sport {sport_key!r}")
        return None

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error(
            "bookmaker: playwright not installed — "
            "run: pip install playwright && python -m playwright install chromium"
        )
        return None

    from scrapers.parse_bookmaker_har import games_from_bookmaker_texts

    bodies: list = []
    login_state = "unknown"

    try:
        with sync_playwright() as pw:
            PROFILE_DIR.mkdir(exist_ok=True)
            context = pw.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                headless=headless,
                channel="chrome",
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled",
                      # off-screen so the scheduled runs don't pop a window up
                      "--window-position=-2400,-2400", "--window-size=1440,900", *SESSION_ARGS],
                # No user_agent override: installed Chrome sends its real UA; a fixed old
                # one (it said Chrome 124) got 'unsupported browser' pages and doesn't
                # match the version the browser reports everywhere else.
                locale="en-US",
                timezone_id="America/New_York",
                viewport={"width": 1440, "height": 900},
            )
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = context.pages[0] if context.pages else context.new_page()
            # --restore-last-session (see SESSION_ARGS) also reopens last
            # run's tabs — close them so only one page loads.
            for extra in context.pages[1:]:
                extra.close()

            def _on_response(resp):
                if resp.status != 200:
                    return
                if not any(p in resp.url for p in ODDS_PATHS):
                    return
                try:
                    bodies.append(resp.text())
                except Exception as e:
                    logger.debug(f"bookmaker: could not read response body: {e}")

            page.on("response", _on_response)

            try:
                for i, url in enumerate(pages):
                    try:
                        page.goto(url, wait_until="domcontentloaded",
                                  timeout=timeout * 1000)
                    except Exception as e:
                        logger.warning(
                            f"bookmaker: navigation failed for {url}: "
                            f"{type(e).__name__}: {str(e)[:120]}"
                        )
                        continue
                    # Wait for this tab's own GetSchedule: it lands 5-6 s after
                    # domcontentloaded on a logged-in page (the old fixed 2.5-4.5 s
                    # wait left before the quarters response arrived, so Q1 was
                    # missing), then a short jitter so the cadence isn't metronomic.
                    # A tab Bookmaker hasn't posted yet (college 1H/quarters early
                    # in the week) redirects to /en/not-found/ — skip it at once
                    # instead of waiting the full 15 s for odds that never come.
                    n_before = len(bodies)
                    not_posted = False
                    for _ in range(30):
                        if len(bodies) > n_before:
                            break
                        if "/not-found" in page.url:
                            not_posted = True
                            break
                        page.wait_for_timeout(500)
                    if not_posted:
                        logger.info(f"bookmaker: {url} isn't posted yet (site shows not-found) — skipped")
                        continue
                    page.wait_for_timeout(int(random.uniform(800, 2_000)))
                    if i == 0:
                        login_state = _login_state(page)
                        if login_state == "logged_out":
                            logger.warning(
                                "bookmaker: NOT LOGGED IN — odds may be stale. "
                                "Run login_bookmaker.py to refresh the session."
                            )
            finally:
                page.remove_listener("response", _on_response)

            # Leave on a blank page: --restore-last-session reopens whatever was
            # open last, and a not-found tab would greet the next BK LOGIN.
            try:
                page.goto("about:blank", timeout=10_000)
            except Exception:
                pass
            context.close()

    except Exception as e:
        logger.error(f"bookmaker: Playwright error ({sport_key}): {e}")
        return None

    if bodies and login_state == "unknown":
        login_state = "logged_in"
    if not bodies:
        with _lock:
            _cache.setdefault(sport_key, {})["login_state"] = login_state
        logger.warning(
            f"bookmaker: no GetSchedule/GetGameView responses captured for "
            f"{sport_key} — page layout may have changed, or the session expired"
        )
        return None

    games = games_from_bookmaker_texts(bodies, sport_key=sport_key)
    logger.info(
        f"bookmaker {sport_key}: {len(bodies)} responses → {len(games)} games "
        f"(login={login_state})"
    )

    with _lock:
        _cache[sport_key] = {
            "data":         games,
            "last_updated": time.time(),
            "login_state":  login_state,
        }
    return games


def last_login_state(sport_key: str) -> str:
    with _lock:
        return (_cache.get(sport_key) or {}).get("login_state", "unknown")


LOGIN_WINDOW_MAX_SECONDS = 15 * 60     # auto-close so a forgotten window can't block pulls
_focus_request = threading.Event()     # BK LOGIN clicked while the window is already open


def _raise_window(title_part: str = "bookmaker") -> bool:
    """
    Put the Bookmaker Chrome window in front of everything. Windows won't let a
    background process (the app server) take the foreground, so a plain launch
    opened the window BEHIND the user's browser with no sign it existed — BK
    NOW then queued behind it and a second BK LOGIN did nothing (2026-09-27).
    A synthetic Alt tap is the documented way to be allowed SetForegroundWindow;
    the taskbar button flashes too in case Windows still refuses.
    """
    if os.name != "nt":
        return False
    import ctypes
    from ctypes import wintypes
    user32 = ctypes.windll.user32
    found = []

    @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    def _each(hwnd, _):
        if user32.IsWindowVisible(hwnd):
            buf = ctypes.create_unicode_buffer(256)
            user32.GetWindowTextW(hwnd, buf, 256)
            if title_part in buf.value.lower() and "chrome" in buf.value.lower():
                found.append(hwnd)
        return True

    user32.EnumWindows(_each, 0)
    if not found:
        return False
    hwnd = found[0]
    # Borrow the foreground window's input queue so Windows treats this as the
    # active app, pin the window on top for an instant, then take focus.
    fg_thread = user32.GetWindowThreadProcessId(user32.GetForegroundWindow(), None)
    me = ctypes.windll.kernel32.GetCurrentThreadId()
    attached = bool(fg_thread and fg_thread != me and user32.AttachThreadInput(me, fg_thread, True))
    try:
        user32.keybd_event(0x12, 0, 0, 0)      # Alt tap: unlocks SetForegroundWindow
        user32.keybd_event(0x12, 0, 2, 0)
        user32.ShowWindow(hwnd, 9)             # SW_RESTORE
        flags = 0x0001 | 0x0002 | 0x0040       # SWP_NOSIZE | SWP_NOMOVE | SWP_SHOWWINDOW
        user32.SetWindowPos(hwnd, -1, 0, 0, 0, 0, flags)   # HWND_TOPMOST
        user32.SetWindowPos(hwnd, -2, 0, 0, 0, 0, flags)   # HWND_NOTOPMOST
        user32.BringWindowToTop(hwnd)
        user32.SetForegroundWindow(hwnd)
    finally:
        if attached:
            user32.AttachThreadInput(me, fg_thread, False)
    ok = user32.GetForegroundWindow() == hwnd

    class FLASHWINFO(ctypes.Structure):
        _fields_ = [("cbSize", wintypes.UINT), ("hwnd", wintypes.HWND), ("dwFlags", wintypes.DWORD),
                    ("uCount", wintypes.UINT), ("dwTimeout", wintypes.DWORD)]
    fi = FLASHWINFO(ctypes.sizeof(FLASHWINFO), hwnd, 0x3 | 0xC, 0, 0)   # FLASHW_ALL | FLASHW_TIMERNOFG
    user32.FlashWindowEx(ctypes.byref(fi))
    return ok


def request_login_window_focus() -> None:
    """Bring an already-open BK LOGIN window back to the front."""
    _focus_request.set()


def open_login_window(start_url: str = LOGIN_URL) -> None:
    """
    Open the collector's own Chrome profile ON-SCREEN, in front, and block until
    the user closes the window (or LOGIN_WINDOW_MAX_SECONDS pass) — the BK LOGIN
    button. Log in there, or just browse lines: it's the same session the
    collector uses, so nothing gets kicked (logging in on another browser does
    kick it — bookmaker.eu allows one session per account). Pulls wait on
    _browser_lock while it's open.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    with _browser_lock:
        with sync_playwright() as pw:
            PROFILE_DIR.mkdir(exist_ok=True)
            context = pw.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                headless=False,
                channel="chrome",
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled",
                      "--window-position=60,40", "--window-size=1440,900", *SESSION_ARGS],
                locale="en-US",
                timezone_id="America/New_York",
                no_viewport=True,
            )
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = context.pages[0] if context.pages else context.new_page()
            for extra in context.pages[1:]:
                extra.close()
            page.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
            # Session-restored tabs can open late — close any that did.
            page.wait_for_timeout(1500)
            for extra in [p for p in context.pages if p != page]:
                extra.close()
            page.bring_to_front()
            _raise_window()
            _focus_request.clear()
            deadline = time.time() + LOGIN_WINDOW_MAX_SECONDS
            # Return once the user closes the window (the browser exits).
            while time.time() < deadline:
                try:
                    context.wait_for_event("close", timeout=1000)
                    return
                except PWTimeout:
                    pass
                if not context.pages:
                    break
                if _focus_request.is_set():
                    _focus_request.clear()
                    _raise_window()
            logger.info("bookmaker: login window closed automatically after 15 min")
            context.close()
