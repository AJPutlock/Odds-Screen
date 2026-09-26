"""
Circa Recording Watcher
=========================
Watches OddsRecordings/ (the iCloud Drive folder the iPhone Shortcut saves
into — see memory: odds_screen_recording_ingestion) for new Circa screen
recordings, parses each one with parse_circa_recording, and POSTs the result
to the Odds Screen app's /api/circa/import-parsed/<sport_key> endpoint so it
shows up in the table automatically — no manual upload step.

Run with the PLAIN project interpreter, NOT the OCR venv — this script no
longer imports pytesseract/opencv directly (see below for why). Either
interpreter technically works since this script's own deps are stdlib-only,
but the OCR venv is reserved for the actual parsing subprocess.

    python circa_watcher.py

The Odds Screen app (app.py) must already be running on APP_BASE_URL for
imports to succeed — this script only calls the lightweight
/api/circa/import-parsed endpoint, it never touches the Odds API itself.

Uses simple polling rather than a filesystem-event watcher (e.g. `watchdog`)
deliberately: cloud-sync folders (iCloud Drive here) don't reliably fire
native OS file-system events for files that arrive via sync rather than a
local write, and polling avoids that whole class of platform/sync-client
quirk for a script that's expected to run continuously in the background.

Each video is parsed in its OWN subprocess (parse_circa_recording.py --json),
not in-process, with a hard timeout that kills and skips it if exceeded.
This was a real, repeated production failure, not a hypothetical: a single
OCR/CV call has been observed to hang indefinitely — near-zero CPU, no
exception, nothing to catch — which froze the entire watcher and every video
still waiting behind it. Two different fixes at the OCR-call level (a
pytesseract-level timeout, then forcing the file fully local before handing
it to OpenCV) were tried and neither reliably resolved it. Subprocess
isolation with an outer timeout fixes the operational problem regardless of
which of those (or something else) the true root cause turns out to be, and
is the right pattern for a long-running service either way — plus each video
gets a fully fresh process, so nothing can leak or accumulate across runs.
"""

import sys
import time
import json
import subprocess
from pathlib import Path
from collections import Counter
from urllib import request as urlrequest
from urllib.error import URLError, HTTPError

HERE          = Path(__file__).parent.resolve()
VENV_PYTHON   = HERE / ".venv-ocr" / "Scripts" / "python.exe"
PARSE_SCRIPT  = HERE / "scrapers" / "parse_circa_recording.py"

WATCH_DIR      = Path(r"C:\Users\ajput\iCloudDrive\OddsRecordings")
PROCESSED_DIR  = WATCH_DIR / "_processed"
FAILED_DIR     = WATCH_DIR / "_failed"   # unmappable/unparseable videos land here, not retried forever
APP_BASE_URL   = "http://localhost:5000"
POLL_INTERVAL  = 10    # seconds between folder scans
STABLE_CHECKS  = 2     # consecutive polls with unchanged file size before processing (iCloud sync safety —
                        # a file mid-sync will keep growing; this avoids processing a half-written video)
SUBPROCESS_TIMEOUT = 300  # seconds — generous per-video ceiling; a hang gets killed, not left to freeze the queue

VIDEO_EXTS = {".mov", ".mp4"}

# Circa's on-screen sport header -> Odds Screen sport_key. Extend this as
# recordings for more sports come in — an unrecognized header gets logged
# clearly rather than silently dropped, and the video is moved to _failed/
# so it doesn't get retried forever, but you can move it back to the watch
# folder to retry once the mapping below is updated.
HEADER_TO_SPORT_KEY = {
    "NCAA FB": "americanfootball_ncaaf",
    "NCAAF":   "americanfootball_ncaaf",
    "NFL":     "americanfootball_nfl",
}


def _guess_sport_key(games: list) -> str | None:
    headers = [g.get("_sport_header") for g in games if g.get("_sport_header")]
    if not headers:
        return None
    header = Counter(headers).most_common(1)[0][0]
    if header in HEADER_TO_SPORT_KEY:
        return HEADER_TO_SPORT_KEY[header]
    header_upper = header.upper()
    for k, v in HEADER_TO_SPORT_KEY.items():
        if k.upper() in header_upper or header_upper in k.upper():
            return v
    return None


def _post_json(url: str, payload: dict, timeout: float = 15.0) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urlrequest.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _parse_video_isolated(path: Path) -> list | None:
    """Runs the parser in its own subprocess with a hard timeout. Returns the
    games list, or None if it timed out or crashed (both logged either way)."""
    try:
        result = subprocess.run(
            [str(VENV_PYTHON), str(PARSE_SCRIPT), "--json", str(path)],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        print(f"[circa_watcher]   ERROR: parsing {path.name} exceeded {SUBPROCESS_TIMEOUT}s and was killed "
              f"(a single OCR/CV call has been observed to hang in practice — see this script's module "
              f"docstring). Filing as unmappable so it doesn't block the queue; move it back from "
              f"_failed/ to retry.")
        return None

    if result.returncode != 0:
        print(f"[circa_watcher]   ERROR parsing {path.name}: subprocess exited {result.returncode}\n"
              f"{result.stderr[-2000:]}")
        return None

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        print(f"[circa_watcher]   ERROR: could not parse subprocess output for {path.name}: {e!r}\n"
              f"stdout tail: {result.stdout[-500:]!r}")
        return None


def process_video(path: Path) -> str:
    """Returns 'ok', 'unmappable', or 'error' — caller decides where to file the video."""
    print(f"[circa_watcher] Processing {path.name} ...")
    games = _parse_video_isolated(path)

    if games is None:
        return "unmappable"   # covers both "timed out" and "crashed" — neither worth retrying automatically

    if not games:
        print(f"[circa_watcher]   No games parsed from {path.name} (empty/unreadable recording) — filing as unmappable")
        return "unmappable"

    sport_key = _guess_sport_key(games)
    if not sport_key:
        headers = sorted({g.get("_sport_header") for g in games if g.get("_sport_header")})
        print(f"[circa_watcher]   Could not map header(s) {headers} to a sport_key — "
              f"add it to HEADER_TO_SPORT_KEY in circa_watcher.py, then move this file back from _failed/ to retry.")
        return "unmappable"

    total_warnings = sum(len(g.get("_warnings", [])) for g in games)
    print(f"[circa_watcher]   Parsed {len(games)} game(s) for {sport_key}, {total_warnings} warning(s) total")

    try:
        result = _post_json(f"{APP_BASE_URL}/api/circa/import-parsed/{sport_key}", {"games": games})
        print(f"[circa_watcher]   Imported OK: {result}")
        return "ok"
    except (URLError, HTTPError) as e:
        print(f"[circa_watcher]   ERROR posting to app: {e!r} — is the Odds Screen app running on {APP_BASE_URL}?")
        return "error"


def main():
    if not VENV_PYTHON.exists():
        print(f"[circa_watcher] FATAL: {VENV_PYTHON} not found — set up the OCR venv first "
              f"(see parse_circa_recording.py's module docstring).")
        sys.exit(1)

    PROCESSED_DIR.mkdir(exist_ok=True)
    FAILED_DIR.mkdir(exist_ok=True)
    seen_sizes: dict = {}  # filename -> (size, stable_count)

    print(f"[circa_watcher] Watching {WATCH_DIR} — polling every {POLL_INTERVAL}s")
    print(f"[circa_watcher] Posting to {APP_BASE_URL} — make sure the Odds Screen app is running")
    print(f"[circa_watcher] Each video parsed in an isolated subprocess, {SUBPROCESS_TIMEOUT}s timeout")

    while True:
        try:
            for f in sorted(WATCH_DIR.iterdir()):
                if not f.is_file() or f.suffix.lower() not in VIDEO_EXTS:
                    continue

                size = f.stat().st_size
                prev_size, stable_count = seen_sizes.get(f.name, (None, 0))
                stable_count = stable_count + 1 if size == prev_size else 0
                seen_sizes[f.name] = (size, stable_count)

                if stable_count < STABLE_CHECKS:
                    continue

                outcome = process_video(f)
                seen_sizes.pop(f.name, None)
                dest_dir = PROCESSED_DIR if outcome == "ok" else (FAILED_DIR if outcome == "unmappable" else None)
                if dest_dir is not None:
                    try:
                        f.rename(dest_dir / f.name)
                    except OSError as e:
                        print(f"[circa_watcher]   Could not move {f.name} to {dest_dir.name}/: {e!r}")
                # outcome == "error" (network failure posting to the app): leave in place, retried next
                # poll — this is the one case worth retrying automatically, since the app might just not
                # be running yet.
        except Exception as e:
            print(f"[circa_watcher] Unexpected error in poll loop: {e!r}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
