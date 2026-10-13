"""
bet365 supervised capture run (step 2, 10/12)
=============================================
Run WITH the user at the PC, phone in hand. It opens the collector's own
profile (bet365_profile/) on-screen and walks through each stage, saving what
the page looks like so bet365_live.py's guesses (login form, 2FA prompt,
signed-in header, account menu / Log Out, the login-state probe) can be
replaced with real selectors. It also records what bet365 does with two
sessions: does a new login kick the old one, or is it refused, in both
directions (PC then phone, phone then PC).

    python bet365_capture.py

Saved to data/bet365_capture/<timestamp>/ (data/ is git-ignored):
  NN_<stage>.html   the page DOM with every input's value stripped and
                    scripts removed: typed values are never saved
  NN_<stage>.png    a screenshot, taken BEFORE you type anything at that stage
  results.json      per stage: time, URL, what the probes in bet365_live.py
                    report, header/menu candidates, cookie NAMES (no values),
                    auth-shaped requests (method, path, status; no bodies),
                    and your answers about the phone
The signed-in screenshots show your header (balance, maybe your name), and
results.json keeps up to 60 characters of header/menu text per element, which
can include the username and balance. The script says so and asks before it
starts. The folder stays on this PC; delete it when the selectors are done.

Don't press 365 NOW / 365 LOGIN in the app while this runs: one Chrome per profile.
"""
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrapers import bet365_live as b   # noqa: E402

OUT = Path(__file__).resolve().parent / "data" / "bet365_capture" / datetime.now().strftime("%Y%m%d_%H%M%S")

# DOM copy without input values or scripts (typed values live in .value, not
# the HTML, but strip every value-ish attribute anyway).
_SCRUBBED_DOM_JS = """() => {
  const root = document.documentElement.cloneNode(true);
  root.querySelectorAll('script, noscript').forEach(e => e.remove());
  root.querySelectorAll('input, textarea').forEach(e => { e.removeAttribute('value'); e.textContent = ''; });
  return '<!doctype html>\\n' + root.outerHTML;
}"""

# What could be the header's account area / a Log In / Log Out / 2FA control.
_CANDIDATES_JS = """() => {
  const vis = e => !!(e.offsetWidth || e.offsetHeight);
  const desc = e => ({tag: e.tagName, cls: (e.className || '').toString().slice(0, 120),
                      aria: e.getAttribute('aria-label'), text: (e.innerText || '').trim().slice(0, 60)});
  const all = [...document.querySelectorAll('body *')].filter(vis);
  return {
    header: [...document.querySelectorAll('[class*="hrm-"]')].filter(vis).filter(e => e.children.length < 4).slice(0, 40).map(desc),
    account_like: all.filter(e => /member|account|balance|user|profile/i.test((e.className || '').toString() + ' ' + (e.getAttribute('aria-label') || ''))).slice(0, 25).map(desc),
    log_in_out: all.filter(e => e.children.length < 3 && /^(log ?in|log ?out|sign ?out|join)$/i.test((e.innerText || '').trim())).map(desc),
    inputs: [...document.querySelectorAll('input')].filter(vis).map(e => ({type: e.type, name: e.name, id: e.id,
             placeholder: e.placeholder, autocomplete: e.autocomplete, inputmode: e.inputMode, maxlength: e.maxLength,
             cls: (e.className || '').toString().slice(0, 80)})),
    code_like_text: all.filter(e => e.children.length < 2 && /verif|code|two.?step|2fa|authenticat|one.?time/i.test(e.innerText || '')).slice(0, 15).map(desc),
    frames: [...document.querySelectorAll('iframe')].map(f => f.src.slice(0, 120)),
  };
}"""

results = {"started": datetime.now().isoformat(), "stages": [], "phone": {}}
auth_log = []


# --chat: answers come from files instead of the terminal (10/12: the user's
# typing wasn't reaching the Terminal tab). Each prompt is written to
# OUT/prompt.txt as "<k>\t<text>"; the run waits for OUT/answer_<k>.txt, which
# Claude writes with the user's reply from the chat.
CHAT = "--chat" in sys.argv
_asked = [0]


def ask(prompt):
    if not CHAT:
        return input(f"\n>>> {prompt}\n    ").strip()
    _asked[0] += 1
    k = _asked[0]
    (OUT / "prompt.txt").write_text(f"{k}\t{prompt}", encoding="utf-8")
    print(f"\n>>> [{k}] {prompt}\n    (waiting for answer_{k}.txt)", flush=True)
    f = OUT / f"answer_{k}.txt"
    while not f.exists():
        time.sleep(1)
    time.sleep(0.3)
    ans = f.read_text(encoding="utf-8").strip()
    f.unlink()
    print(f"    answer: {ans}", flush=True)
    return ans


def capture(page, context, n, stage, note=""):
    t = time.time()
    try:
        b._wait_ready(page, 8000)
    except Exception:
        pass
    entry = {"n": n, "stage": stage, "note": note, "at": datetime.now().isoformat(), "url": page.url}
    try:
        (OUT / f"{n:02d}_{stage}.html").write_text(page.evaluate(_SCRUBBED_DOM_JS), encoding="utf-8")
    except Exception as e:
        entry["dom_error"] = f"{type(e).__name__}: {e}"
    try:
        page.screenshot(path=str(OUT / f"{n:02d}_{stage}.png"))
    except Exception as e:
        entry["png_error"] = f"{type(e).__name__}: {e}"
    entry["probe_login_state"] = b._login_state(page)
    try:
        entry["candidates"] = page.evaluate(_CANDIDATES_JS)
    except Exception as e:
        entry["candidates_error"] = str(e)
    entry["cookies"] = sorted({(c["name"], c["domain"], "session" if c.get("expires", -1) == -1 else "persistent")
                               for c in context.cookies() if "bet365" in c.get("domain", "")})
    entry["auth_requests_since_last"] = auth_log[:]
    auth_log.clear()
    entry["seconds"] = round(time.time() - t, 1)
    results["stages"].append(entry)
    save()
    print(f"    saved {n:02d}_{stage}  (probe says: {entry['probe_login_state']})")
    return entry


def save():
    (OUT / "results.json").write_text(json.dumps(results, indent=1, default=str), encoding="utf-8")


def league_check(page, n):
    """Does a signed-in page get the NFL league odds? (A fresh profile got none.)"""
    hits = []
    on = lambda r: hits.append((r.status, urlparse(r.url).path, len(r.body()) if r.status == 200 else 0)) \
        if b.ODDS_PATH in r.url else None
    page.on("response", on)
    cfg = b.SPORTS["americanfootball_nfl"]
    pd = cfg["league"] + cfg["tabs"][0][1]
    t = time.time()
    page.evaluate("h => { location.hash = h; }", b._hash_for(pd)[1:])
    for _ in range(30):
        if hits:
            break
        page.wait_for_timeout(500)
    via = "hash"
    if not hits:
        page.goto(b.SITE + "/" + b._hash_for(pd), wait_until="domcontentloaded", timeout=30_000)
        b._wait_ready(page)
        page.wait_for_timeout(8000)
        via = "full load"
    page.remove_listener("response", on)
    results["league_check"] = {"via": via, "responses": hits, "seconds": round(time.time() - t, 1)}
    save()
    print(f"    NFL game lines: {len(hits)} odds response(s) via {via}")
    capture(page, page.context, n, "nfl_game_lines")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    print(f"Saving to {OUT}")
    print("Heads-up: the signed-in screenshots, and the header text kept in results.json (up to 60\n"
          "characters per element), can include your bet365 username and balance. Typed values\n"
          "(password, 2FA code) are never saved. Everything stays on this PC in a git-ignored\n"
          "folder; delete it once the selectors are done.")
    if ask("Continue? (y/n)").lower() not in ("y", "yes"):
        print("Not started.")
        return
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        context, page = b._launch(pw, onscreen=True)
        try:
            _stages(context, page)
        except (KeyboardInterrupt, Exception) as e:
            print(f"\nStopped: {type(e).__name__}: {e}")
            results["stopped"] = f"{type(e).__name__}: {e}"
            save()
        finally:
            try:                                  # never leave the PC signed in
                if b._login_state(page) == "logged_in":
                    print("Signed in — running the collector's logout before closing...")
                    print("logout", "confirmed" if b._logout(page) else "NOT confirmed — log out by hand")
            except Exception:
                pass
            try:
                context.close()
            except Exception:
                pass
    print(f"\nSaved {len(results['stages'])} stages to {OUT}")


def _stages(context, page):
    """The capture stages, in order (main() owns the browser and the final logout)."""

    def on_resp(r):
        u = urlparse(r.url)
        if "bet365" in u.netloc and any(k in u.path.lower() for k in ("login", "logon", "auth", "logout", "session", "member", "verif", "otp")):
            auth_log.append({"t": datetime.now().strftime("%H:%M:%S"), "method": r.request.method,
                             "path": u.path[:120], "status": r.status})
    page.on("response", on_resp)

    page.goto(b.HOME_URL, wait_until="domcontentloaded", timeout=60_000)
    b._wait_ready(page)
    page.wait_for_timeout(2000)
    n = 1
    capture(page, context, n, "home_on_open", "as the profile opens (signed out expected)")

    if b._login_state(page) == "logged_in":
        ask("The profile opened SIGNED IN. Press Enter and I'll log it out with the collector's logout first.")
        results["logout_on_open"] = b._logout(page)
        n += 1; capture(page, context, n, "after_collector_logout_on_open")

    ask("Click 'Log In' in the bet365 window so the form shows. DON'T type yet. Press Enter here.")
    n += 1; capture(page, context, n, "login_form_empty")

    ask("Now type your username and password in the window and submit. If bet365 then asks for a "
        "verification code, press Enter here BEFORE typing the code. Otherwise press Enter once signed in.")
    n += 1; e = capture(page, context, n, "after_submit")
    if e["probe_login_state"] != "logged_in":
        ans = ask("Is a 2FA / verification-code prompt showing? (y/n)")
        results["two_factor_prompt"] = ans.lower().startswith("y")
        if results["two_factor_prompt"]:
            ask("Type the code in the window and finish signing in. Press Enter once signed in.")
            n += 1; capture(page, context, n, "after_2fa")
    t_signed = time.time()
    n += 1; capture(page, context, n, "signed_in_home", "header while signed in")

    n += 1; league_check(page, n)

    ask("Open the account menu in the window (whatever shows Log Out), don't click Log Out. Press Enter.")
    n += 1; capture(page, context, n, "account_menu_open")
    page.keyboard.press("Escape")

    # ── Two sessions: PC first, then phone ──────────────────────────────
    ask("PHONE TEST 1: the PC is signed in. Now sign in to bet365 on your PHONE. Press Enter when you've tried.")
    results["phone"]["pc_first__phone_login_ok"] = ask("Did the phone sign-in succeed? (y / n / describe)")
    page.goto(b.HOME_URL, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4000)
    n += 1; e = capture(page, context, n, "pc_after_phone_login", "PC reloaded after the phone signed in")
    results["phone"]["pc_first__pc_state_after"] = e["probe_login_state"]
    results["phone"]["pc_first__pc_message"] = ask("Does the PC window show any message (kicked / session ended)? Describe or Enter for none.")

    # ── Then phone first, PC second ─────────────────────────────────────
    if b._login_state(page) != "logged_in":
        ask("PHONE TEST 2: with the phone signed in, sign in again in the PC window (Log In, type, submit; "
            "answer 2FA if asked). Press Enter when done or refused.")
    else:
        ask("PHONE TEST 2: the PC still shows signed in. Make sure the phone is signed in, then press Enter.")
    n += 1; e = capture(page, context, n, "pc_after_second_login")
    results["phone"]["phone_first__pc_state"] = e["probe_login_state"]
    results["phone"]["phone_first__phone_kicked"] = ask("On the PHONE now: still signed in, or logged out / error? Describe.")

    # ── The collector's own logout ──────────────────────────────────────
    if b._login_state(page) == "logged_in":
        ask("Last step: I'll run the collector's logout (Account → Log Out → check for 'Log In'). Press Enter.")
        t = time.time()
        ok = b._logout(page)
        results["collector_logout"] = {"confirmed": ok, "seconds": round(time.time() - t, 1)}
        n += 1; capture(page, context, n, "after_collector_logout")
        print(f"    collector logout {'CONFIRMED' if ok else 'NOT confirmed — log out by hand in the window'}")
    results["seconds_signed_in_total"] = round(time.time() - t_signed, 1)
    results["finished"] = datetime.now().isoformat()
    save()
    ask("Done. Check the window shows signed out (log out by hand if not), then press Enter to close it.")


if __name__ == "__main__":
    main()
