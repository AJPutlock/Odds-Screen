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
import json
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
    # Period tabs (10/12): the board's NBA 1H/Q1 and NHL P1 rows never got a
    # Bookmaker price because only game-lines was pulled. NHL's tab is
    # /periods/ (seen in the game-lines nav); NBA's halves/quarters aren't
    # posted in preseason, so their slugs follow NFL/NCAAF's -- a tab that
    # isn't up redirects to not-found and is skipped at once.
    "basketball_nba": [
        f"{BASE}/basketball/nba/game-lines/",
        f"{BASE}/basketball/nba/1st-halves/",
        f"{BASE}/basketball/nba/quarters/",
    ],
    "icehockey_nhl": [
        f"{BASE}/hockey/nhl/game-lines/",
        f"{BASE}/hockey/nhl/periods/",
    ],
    "basketball_wnba": [
        f"{BASE}/basketball/wnba/game-lines/",
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

# Chrome 154 (auto-updated 2026-10-01) stopped carrying the login across
# launches even with --restore-last-session: after BK LOGIN the profile held
# only analytics/Cloudflare cookies and every pull opened logged out. So the
# login window now snapshots bookmaker.eu's cookies (session ones included)
# while it's open, and each pull adds back any the profile lost. The file holds
# a live login token: it lives in the git-ignored profile folder.
SESSION_FILE = PROFILE_DIR / "bk_session_cookies.json"


def _save_session_cookies(context) -> None:
    try:
        cookies = [c for c in context.cookies() if "bookmaker.eu" in c.get("domain", "")]
        if cookies:
            tmp = SESSION_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(cookies), encoding="utf-8")
            os.replace(tmp, SESSION_FILE)
    except Exception as e:
        logger.debug(f"bookmaker: cookie snapshot failed: {e}")


def _restore_session_cookies(context) -> int:
    """Put the saved bookmaker.eu cookies back; returns how many were set.
    Session cookies (the login, ASP_NET_SessionId) always overwrite: the tab
    --restore-last-session reopens starts loading the moment Chrome launches
    and is handed a fresh, signed-out session id before this runs."""
    try:
        saved = json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return 0
    have = {(c["name"], c["domain"], c["path"]) for c in context.cookies()}
    put = [c for c in saved
           if c.get("expires", -1) == -1 or (c["name"], c["domain"], c["path"]) not in have]
    if put:
        try:
            context.add_cookies(put)
        except Exception as e:
            logger.warning(f"bookmaker: could not restore saved login cookies: {e}")
            return 0
    return len(put)


def _signed_in_now(context, page) -> bool:
    """The login window's page is past the login form and holds a session id."""
    try:
        if not any(c["name"] == "ASP_NET_SessionId" for c in context.cookies()):
            return False
        text = page.inner_text("body", timeout=1000)
        return not any(t in text for t in _LOGGED_OUT_TEXT)
    except Exception:
        return False


# ── Automatic sign-in (10/8) ────────────────────────────────────────────────
# BOOKMAKER_USERNAME / BOOKMAKER_PASSWORD in the app's .env (git-ignored; the
# user enters them). Used by the collector's own Chrome when BK LOGIN opens or
# a pull finds the session expired. Never logged. At most one attempt per
# AUTO_LOGIN_MIN_GAP, and a failed attempt pauses it for AUTO_LOGIN_FAIL_PAUSE
# so a wrong password can't lock the account with repeated tries.
AUTO_LOGIN_MIN_GAP = 30 * 60
AUTO_LOGIN_FAIL_PAUSE = 6 * 3600
_auto_login = {"last": 0.0, "failed_at": 0.0}


def _credentials():
    try:
        from dotenv import load_dotenv
        load_dotenv(PROFILE_DIR.parent / ".env", override=True)
    except ImportError:
        pass
    return os.environ.get("BOOKMAKER_USERNAME", "").strip(), os.environ.get("BOOKMAKER_PASSWORD", "")


def auto_login_configured() -> bool:
    user, pw = _credentials()
    return bool(user and pw)


def _try_auto_login(context, page, why: str) -> bool:
    """Fill Bookmaker's header sign-in form from .env and submit it. True when
    the page is signed in afterwards."""
    user, pw = _credentials()
    if not (user and pw):
        return False
    now = time.time()
    if now - _auto_login["failed_at"] < AUTO_LOGIN_FAIL_PAUSE:
        print("[bookmaker] auto sign-in paused after a failed attempt — sign in with BK LOGIN", flush=True)
        return False
    if now - _auto_login["last"] < AUTO_LOGIN_MIN_GAP:
        return False
    _auto_login["last"] = now
    print(f"[bookmaker] auto sign-in ({why})", flush=True)
    try:
        acct = page.get_by_placeholder("Account").first
        acct.wait_for(state="visible", timeout=10_000)
        acct.click()
        acct.press_sequentially(user, delay=60)
        pwd = page.get_by_placeholder("Password").first
        pwd.click()
        pwd.press_sequentially(pw, delay=60)
        page.get_by_role("button", name="Log In").first.click()
        for _ in range(30):                      # up to ~15 s for the session to come up
            page.wait_for_timeout(500)
            if _signed_in_now(context, page):
                break
    except Exception as e:
        print(f"[bookmaker] auto sign-in couldn't use the form: {type(e).__name__}", flush=True)
    ok = _signed_in_now(context, page)
    if ok:
        _save_session_cookies(context)
        print("[bookmaker] auto sign-in OK", flush=True)
    else:
        _auto_login["failed_at"] = now
        print("[bookmaker] auto sign-in FAILED — check BOOKMAKER_USERNAME / BOOKMAKER_PASSWORD in .env "
              "(paused 6 h; BK LOGIN still works by hand)", flush=True)
    return ok


def _window_log(msg: str) -> None:
    # print, not logger.info: the app only writes stdout/stderr to odds_screen.log
    print(f"[bookmaker] login window: {msg}", flush=True)


def _period_labels(texts) -> set:
    """The period labels (gpd) of the non-full-game entries in these
    GetSchedule/GetGameView bodies — empty when a tab returned no period odds."""
    out = set()

    def walk(o):
        if isinstance(o, dict):
            if "gp" in o and "vtm" in o and str(o.get("gp")) != "0":
                out.add(str(o.get("gpd") or o.get("gp")).strip().lower())
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    for t in texts:
        try:
            walk(json.loads(t))
        except ValueError:
            pass
    return out


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


FULL_GAME_TABS = ("game-lines", "extra-games")


def fetch_bookmaker(sport_key: str, timeout: int = 60,
                    headless: bool = False, main_only: bool = False) -> Optional[list]:
    """Collect one sport's odds; serialized — see _browser_lock. main_only:
    the full-game tabs only (BK AUTO overnight), no 1H/quarter/inning tabs."""
    with _browser_lock:
        return _fetch_bookmaker(sport_key, timeout, headless, main_only)


def _collect_with(context, page, sport_key: str, timeout: int = 60, main_only: bool = False) -> Optional[list]:
    """Collect one sport's tabs in an already-open Chrome page (the collector's
    own off-screen window, or the BK LOGIN window right after it signs in).
    Returns game dicts, or None when nothing came back."""
    pages = SPORT_PAGES.get(sport_key)
    if not pages:
        logger.warning(f"bookmaker: no page list for sport {sport_key!r}")
        return None
    if main_only:
        pages = [u for u in pages if u.rstrip("/").split("/")[-1] in FULL_GAME_TABS]
    from scrapers.parse_bookmaker_har import games_from_bookmaker_texts

    bodies: list = []
    login_state = "unknown"
    no_games = False          # the game-lines page itself isn't posted: no games today

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
    # Odds requests still in flight. A late response from the previous
    # tab used to satisfy the next tab's "new response" wait, so the
    # collector moved on before that tab's own GetSchedule landed and the
    # navigation cancelled it (10/7: every NCAAF game came back with no
    # 1H). Each tab now starts with nothing in flight and waits for its
    # own requests to finish; a tab with no period odds is retried once.
    inflight = set()
    aborted = []
    is_odds = lambda req: any(p in req.url for p in ODDS_PATHS)
    on_req = lambda req: inflight.add(req) if is_odds(req) else None
    on_done = lambda req: inflight.discard(req)
    def on_fail(req):
        if is_odds(req):
            inflight.discard(req)
            aborted.append(req.url)
    page.on("request", on_req)
    page.on("requestfinished", on_done)
    page.on("requestfailed", on_fail)

    def settle(max_ms=8000):
        """Wait until no odds request is in flight (up to max_ms)."""
        waited = 0
        while inflight and waited < max_ms:
            page.wait_for_timeout(250)
            waited += 250

    try:
        for i, url in enumerate(pages):
          tab = url.rstrip("/").split("/")[-1]
          for attempt in (1, 2):
            settle()
            n_aborted = len(aborted)
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
                print(f"[bookmaker] {sport_key} {tab}: not posted yet (site shows not-found) — skipped", flush=True)
                if i == 0:
                    no_games = True
                break
            settle()                      # this tab's own requests, all of them
            page.wait_for_timeout(int(random.uniform(800, 2_000)))
            if i == 0:
                login_state = _login_state(page)
                if login_state == "logged_out":
                    if attempt == 1 and _try_auto_login(context, page, "session expired during a pull"):
                        login_state = "unknown"
                        continue          # reload this tab signed in
                    logger.warning(
                        "bookmaker: NOT LOGGED IN — odds may be stale. "
                        "Click BK LOGIN (or set BOOKMAKER_USERNAME / BOOKMAKER_PASSWORD in .env)."
                    )
                    break
            new = bodies[n_before:]
            periods = _period_labels(new)
            lost = len(aborted) - n_aborted
            # Period tabs must bring period odds; full-game tabs (game lines,
            # extra games) just need a response.
            full_game_tab = tab in FULL_GAME_TABS
            ok = bool(new) and (full_game_tab or bool(periods))
            print(f"[bookmaker] {sport_key} {tab}: {len(new)} response(s)"
                  + ("" if full_game_tab else f", periods {sorted(periods)}")
                  + (f", {lost} cancelled" if lost else "")
                  + ("" if ok else (" — retrying" if attempt == 1 else " — still nothing, moving on")), flush=True)
            if ok:
                break
    finally:
        page.remove_listener("response", _on_response)
        for ev, fn in (("request", on_req), ("requestfinished", on_done), ("requestfailed", on_fail)):
            try:
                page.remove_listener(ev, fn)
            except Exception:
                pass

    # Keep the saved login current when this pull was signed in.
    if bodies and login_state != "logged_out":
        _save_session_cookies(context)

    if bodies and login_state == "unknown":
        login_state = "logged_in"
    with _lock:
        _cache.setdefault(sport_key, {})["no_games"] = no_games and not bodies
    if not bodies:
        with _lock:
            _cache.setdefault(sport_key, {})["login_state"] = login_state
        if no_games:
            return None          # nothing posted for this sport — not a failure
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
            "no_games":     False,
        }
    return games



def _fetch_bookmaker(sport_key: str, timeout: int = 60,
                     headless: bool = False, main_only: bool = False) -> Optional[list]:
    """
    Collect bookmaker.eu odds for one sport in its own Chrome. Returns game
    dicts in the same shape games_from_bookmaker_har() produces, or None.

    Runs a normal (headed) Chrome window parked off-screen: Cloudflare in front
    of bookmaker.eu blocks headless Chrome outright (403 on every odds request,
    "The action cannot be completed at this time"), while the same profile in a
    regular window loads normally.
    """
    if not SPORT_PAGES.get(sport_key):
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
            n_restored = _restore_session_cookies(context)
            if n_restored:
                logger.info(f"bookmaker: restored {n_restored} saved login cookie(s)")
            try:
                return _collect_with(context, page, sport_key, timeout, main_only)
            finally:
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


def last_no_games(sport_key: str) -> bool:
    """True when the last pull found the sport's game-lines page not posted."""
    with _lock:
        return bool((_cache.get(sport_key) or {}).get("no_games"))


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


def _move_offscreen(context, page) -> None:
    """Park the window off-screen (it keeps rendering, unlike a minimized one)."""
    try:
        cdp = context.new_cdp_session(page)
        win = cdp.send("Browser.getWindowForTarget")
        cdp.send("Browser.setWindowBounds", {"windowId": win["windowId"],
                                              "bounds": {"left": -2400, "top": -2400, "width": 1440, "height": 900}})
    except Exception:
        pass


def _collect_here(context, page, on_signed_in) -> bool:
    """Signed in: hand the window to on_signed_in(fetch) to pull every sport in
    it (fetch(sport_key) -> games), so nothing closes and reopens."""
    _move_offscreen(context, page)
    try:
        on_signed_in(lambda sk: _collect_with(context, page, sk))
    except Exception as e:
        print(f"[bookmaker] collecting in the login window failed: {type(e).__name__}: {e}", flush=True)
        return False
    return True


def open_login_window(start_url: str = LOGIN_URL, on_signed_in=None):
    """
    Open the collector's own Chrome profile ON-SCREEN, in front, and block until
    the user closes the window (or LOGIN_WINDOW_MAX_SECONDS pass) — the BK LOGIN
    button. Log in there, or just browse lines: it's the same session the
    collector uses, so nothing gets kicked (logging in on another browser does
    kick it — bookmaker.eu allows one session per account). Pulls wait on
    _browser_lock while it's open.

    on_signed_in(fetch): once the window is signed in (automatically from .env,
    or by hand), the window moves off-screen and on_signed_in pulls every sport
    in it with fetch(sport_key), then the window closes. Returns True when that
    happened, so the caller doesn't start a second Chrome for the same pull.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    t0 = time.time()
    # With credentials in .env the window never needs to be seen: it starts
    # off-screen, signs in, collects, closes. It comes on-screen only if the
    # automatic sign-in fails and you have to type (10/8: an on-screen launch sat
    # ~25 s behind other windows while Chrome and Bookmaker's page loaded).
    hidden = auto_login_configured() and time.time() - _auto_login["failed_at"] >= AUTO_LOGIN_FAIL_PAUSE
    with _browser_lock:
        _window_log(f"opening ({'off-screen, automatic sign-in' if hidden else 'on-screen'})"
                    + (f" after waiting {time.time() - t0:.0f} s for another pull" if time.time() - t0 > 2 else ""))
        with sync_playwright() as pw:
            PROFILE_DIR.mkdir(exist_ok=True)
            context = pw.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                headless=False,
                channel="chrome",
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled",
                      "--window-position=-2400,-2400" if hidden else "--window-position=60,40",
                      "--window-size=1440,900", *SESSION_ARGS],
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
            # A login that's still alive on Bookmaker's side comes straight back.
            _restore_session_cookies(context)

            # What Bookmaker answers while the user logs in, so a refused login
            # shows up in odds_screen.log: every failed request, and the
            # status (never the body) of anything login/auth-shaped.
            def _on_resp(resp):
                try:
                    from urllib.parse import urlparse
                    u = urlparse(resp.url)
                    if "bookmaker" not in u.netloc:
                        return
                    path = u.path[:100]
                    if resp.status >= 400 or any(k in path.lower() for k in ("login", "logon", "auth", "signin")):
                        _window_log(f"HTTP {resp.status} {resp.request.method} {u.netloc}{path}")
                except Exception:
                    pass
            page.on("response", _on_resp)
            _window_log(f"Chrome up in {time.time() - t0:.1f} s")
            if not hidden:
                page.bring_to_front()
                _raise_window()          # in front while Bookmaker loads, not after
            page.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
            _window_log(f"Bookmaker loaded in {time.time() - t0:.1f} s")
            # Session-restored tabs can open late — close any that did.
            page.wait_for_timeout(1500)
            for extra in [p for p in context.pages if p != page]:
                extra.close()
            if not hidden:
                page.bring_to_front()
                _raise_window()
            _focus_request.clear()
            # With credentials in .env the window signs itself in and closes, so
            # BK LOGIN is one click; without them (or if it fails) it waits for you.
            if not _signed_in_now(context, page) and auto_login_configured():
                _auto_login["last"] = 0.0             # an explicit click may try right away
                if _try_auto_login(context, page, "BK LOGIN"):
                    _window_log(f"signed in {time.time() - t0:.1f} s after BK LOGIN")
                    page.wait_for_timeout(1500)
                    _window_log("signed in automatically" + (" — collecting every sport here" if on_signed_in else " — closing"))
                    done = _collect_here(context, page, on_signed_in) if on_signed_in else False
                    try:
                        page.goto("about:blank", timeout=10_000)
                    except Exception:
                        pass
                    context.close()
                    return done
            if hidden and not _signed_in_now(context, page):
                try:
                    cdp = context.new_cdp_session(page)
                    win = cdp.send("Browser.getWindowForTarget")
                    cdp.send("Browser.setWindowBounds", {"windowId": win["windowId"],
                                                          "bounds": {"left": 60, "top": 40, "width": 1440, "height": 900}})
                except Exception:
                    pass
                page.bring_to_front()
                _raise_window()
                _window_log("automatic sign-in didn't work — window shown for you to sign in")
            deadline = time.time() + LOGIN_WINDOW_MAX_SECONDS
            last_state = None
            # Return once the user closes the window (the browser exits).
            # Snapshot the cookies every pass while signed in — after the close
            # they're gone. A signed-out window never overwrites the last good
            # login (it used to, every second, the moment the window opened).
            while time.time() < deadline:
                signed_in = _signed_in_now(context, page)
                if signed_in:
                    _save_session_cookies(context)
                    if on_signed_in:             # signed in by hand: collect right here too
                        page.wait_for_timeout(2000)
                        _window_log("SIGNED IN — collecting every sport in this window")
                        done = _collect_here(context, page, on_signed_in)
                        try:
                            page.goto("about:blank", timeout=10_000)
                        except Exception:
                            pass
                        context.close()
                        return done
                try:
                    from urllib.parse import urlparse
                    state = (signed_in, urlparse(page.url).netloc + urlparse(page.url).path,
                             any(c["name"] == "ASP_NET_SessionId" for c in context.cookies()))
                except Exception:
                    state = last_state
                if state != last_state:
                    _window_log(f"{'SIGNED IN' if state[0] else 'signed out'} · {state[1]} · "
                                f"session cookie {'present' if state[2] else 'MISSING'}")
                    last_state = state
                try:
                    context.wait_for_event("close", timeout=1000)
                    _window_log("closed by the user" + ("" if last_state and last_state[0] else
                                " while still signed out — the saved login was left as it was"))
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
