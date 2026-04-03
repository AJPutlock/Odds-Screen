"""
auto_capture.py — Automated bet365 HAR capture helper
======================================================
Navigates Chrome through every game page for a sport so that Charles Proxy
captures the per-game coupon XHRs (which carry the actual odds data).

Workflow
--------
1. Open Charles Proxy and start recording.
2. In Chrome, browse to the sport's main bet365 page and wait for it to load
   (this captures the game list).
3. Export that initial HAR from Charles and import it in the Odds Screen app
   (this caches the event IDs the script needs).
4. Run:  python auto_capture.py <sport_key>
         e.g.  python auto_capture.py baseball_ncaa
5. Make sure Chrome is visible on screen.  The script will start navigating
   in 5 seconds — click on the Chrome window before it begins.
6. When the script beeps and prints "DONE", go to Charles → File → Export →
   save as .har, then import it in the Odds Screen app.

Requirements
------------
    pip install pyautogui pygetwindow requests
"""

import sys
import time
import winsound
import requests
import pyautogui
import pygetwindow as gw

# ── Config ────────────────────────────────────────────────────────────────────
APP_URL      = "http://localhost:5000"
PAGE_DELAY   = 5        # seconds to wait on each game page (Charles needs time to capture)
START_DELAY  = 5        # seconds before the first navigation (time to click into Chrome)
TYPING_SPEED = 0.04     # seconds between keystrokes (pyautogui typewrite interval)

# ── Helpers ───────────────────────────────────────────────────────────────────

def beep_done():
    """Play a short ascending tone to signal completion."""
    for freq, dur in [(800, 150), (1000, 150), (1300, 300)]:
        winsound.Beep(freq, dur)
        time.sleep(0.05)


def beep_error():
    winsound.Beep(400, 600)


def focus_chrome():
    """Bring the Chrome window to the foreground."""
    wins = [w for w in gw.getAllWindows()
            if "chrome" in w.title.lower() or "bet365" in w.title.lower()]
    if wins:
        try:
            wins[0].activate()
            time.sleep(0.4)
            return True
        except Exception:
            pass
    return False


def navigate_to(url: str):
    """Type a URL into Chrome's address bar and press Enter."""
    pyautogui.hotkey("ctrl", "l")          # focus address bar
    time.sleep(0.3)
    pyautogui.hotkey("ctrl", "a")          # select all existing text
    time.sleep(0.1)
    pyautogui.typewrite(url, interval=TYPING_SPEED)
    time.sleep(0.1)
    pyautogui.press("enter")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    sport_key = sys.argv[1] if len(sys.argv) > 1 else None
    if not sport_key:
        print("Usage: python auto_capture.py <sport_key>")
        print("       e.g.  python auto_capture.py baseball_ncaa")
        print()
        print("Available sport keys:")
        try:
            r = requests.get(f"{APP_URL}/api/sports", timeout=5)
            for sk in r.json().get("sports", []):
                meta = r.json().get("sport_meta", {}).get(sk, {})
                print(f"  {sk:30s}  ({meta.get('label', '')})")
        except Exception:
            print("  (could not reach app — is it running?)")
        sys.exit(1)

    # ── Fetch game URLs from the app ─────────────────────────────────────────
    print(f"\nFetching game URLs for {sport_key!r} from Odds Screen app...")
    try:
        r = requests.get(f"{APP_URL}/api/bet365/game-urls/{sport_key}", timeout=10)
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot reach Odds Screen app.  Is it running on localhost:5000?")
        beep_error()
        sys.exit(1)

    data = r.json()
    if "error" in data:
        print(f"ERROR: {data['error']}")
        print("Tip: Import the sport's game-list HAR first so the app knows the event IDs.")
        beep_error()
        sys.exit(1)

    games = data["games"]
    if not games:
        print("No games found in cache.")
        beep_error()
        sys.exit(1)

    print(f"Found {len(games)} games to capture:\n")
    for g in games:
        print(f"  {g['game']}")

    # ── Confirm before starting ───────────────────────────────────────────────
    print(f"\nStarting in {START_DELAY} seconds.")
    print(f">> Click on the Chrome window now! <<\n")
    time.sleep(START_DELAY)

    # ── Navigate through each game page ──────────────────────────────────────
    if not focus_chrome():
        print("Warning: could not auto-focus Chrome — make sure it is in the foreground.")

    total   = len(games)
    success = 0

    for i, g in enumerate(games):
        label = g["game"]
        url   = g["url"]
        print(f"[{i+1}/{total}]  {label}")

        try:
            navigate_to(url)
            time.sleep(PAGE_DELAY)
            success += 1
        except Exception as e:
            print(f"         WARNING: navigation error — {e}")

    # ── Done ─────────────────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"  DONE — navigated {success}/{total} game pages.")
    print(f"{'='*55}")
    print()
    print("Next steps:")
    print("  1. In Charles Proxy: File → Export Session → save as .har")
    print(f"  2. In Odds Screen app: go to the {sport_key} tab and click 'BET365 HAR'")
    print("  3. Select the .har file you just saved")
    print()
    beep_done()


if __name__ == "__main__":
    main()
