"""
bet365 Login Helper
===================
Opens a real Chrome window using a dedicated browser profile.
Log in to bet365, then press Enter — the session is stored in the profile
and the scraper will reuse it automatically on every run.

Run once:
    python login_bet365.py

Re-run if you ever get logged out.

NOTE: You do NOT need to close your regular Chrome before running this.
"""

import pathlib
from playwright.sync_api import sync_playwright

PROFILE_DIR = pathlib.Path(__file__).parent / "bet365_profile"
LOGIN_URL   = "https://www.oh.bet365.com/?_h=YFSlKWT5aYaLEwz9Ddw2Pg%3D%3D&btsffd=1#/HO/"


def main():
    PROFILE_DIR.mkdir(exist_ok=True)
    print(f"Using profile: {PROFILE_DIR}")
    print("Opening bet365 in Chrome...\n")

    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            channel="chrome",
            headless=False,
            args=[
                "--no-sandbox",
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
            ],
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
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)

        input("Log in to bet365, then press Enter here to save your session... ")

        context.close()

    print("\nSession saved to bet365_profile/")
    print("The scraper will now use this session automatically.")


if __name__ == "__main__":
    main()
