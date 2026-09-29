"""
Bookmaker.eu Login Helper
=========================
Opens a real Chrome window using a dedicated browser profile.
Log in to bookmaker.eu, then press Enter — the session is stored in the
profile and the collector reuses it automatically on every run.

Run once:
    python login_bookmaker.py

Re-run if the odds screen reports the bookmaker session as logged out.

bookmaker.eu has no "remember me": the login is a session cookie Chrome would
delete when this window closes. This window and the collector both launch
Chrome with --restore-last-session, which keeps that cookie, so the
collector's browser opens already logged in. Nothing is stored in code or
.env — the credentials only ever go into the browser.

NOTE: You do NOT need to close your regular Chrome before running this.
"""

import pathlib
from playwright.sync_api import sync_playwright

from scrapers.bookmaker_live import SESSION_ARGS

PROFILE_DIR = pathlib.Path(__file__).parent / "bookmaker_profile"
LOGIN_URL   = "https://be.bookmaker.eu/en/sports/football/nfl/game-lines/"


def main():
    PROFILE_DIR.mkdir(exist_ok=True)
    print(f"Using profile: {PROFILE_DIR}")
    print("Opening bookmaker.eu in Chrome...\n")

    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            channel="chrome",
            headless=False,
            args=[
                "--no-sandbox",
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
                *SESSION_ARGS,
            ],
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
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)

        input("Log in to bookmaker.eu, then press Enter here to save your session... ")

        context.close()

    print("\nSession saved to bookmaker_profile/")
    print("The collector will now use this session automatically.")


if __name__ == "__main__":
    main()
