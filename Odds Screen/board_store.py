"""
Board state that survives a server restart (10/12). Every .py save restarts
the Flask server (14 restarts on 10/12), which wiped the board, the Bookmaker
pull and the per-game period pull times, so the next BK NOW re-pulled every
game's 1H/Q1 lines (~500 credits). Saved after each refresh, loaded at startup:

  data/board_cache/<sport>.json     the board (_caches[sport]: rows, raw games, times)
  data/board_cache/bk_<sport>.json  the last Bookmaker pull (_bkmkr_caches[sport])
  data/period_pulled.json           Odds API event id -> epoch of its last period pull

Games that have started are dropped from the board on load (by the Odds
API's kickoff; the Bookmaker cache is kept whole, see load_bookmaker). Every row keeps its own
periods_pulled_at and each source its capture time, so the page's stale-source
gates still keep old prices from counting as +EV after a reload.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

_DIR = Path(__file__).resolve().parent / "data"
_BOARD_DIR = _DIR / "board_cache"
_PERIOD_FILE = _DIR / "period_pulled.json"
BOARD_FIELDS = ("data", "raw_games", "last_updated", "full_updated", "periods_updated")


def _write(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, default=str), encoding="utf-8")
    os.replace(tmp, path)            # atomic: a crash mid-write never leaves half a file


def _read(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return None


def _future(item: dict, now: datetime) -> bool:
    try:
        return datetime.fromisoformat(str(item.get("commence_time")).replace("Z", "+00:00")) > now
    except ValueError:
        return True                  # no usable kickoff: keep it


def save_board(sport_key: str, cache: dict) -> None:
    _write(_BOARD_DIR / f"{sport_key}.json", {k: cache.get(k) for k in BOARD_FIELDS})


def save_bookmaker(sport_key: str, cache: dict) -> None:
    _write(_BOARD_DIR / f"bk_{sport_key}.json",
           {k: cache.get(k) for k in ("data", "last_updated", "status")})


def save_period_pulled(pulled: dict) -> None:
    _write(_PERIOD_FILE, pulled)


def load_board(sport_key: str) -> dict | None:
    """The saved board with started games dropped, or None."""
    saved = _read(_BOARD_DIR / f"{sport_key}.json")
    if not saved:
        return None
    now = datetime.now(timezone.utc)
    saved["data"] = [r for r in saved.get("data") or [] if _future(r, now)]
    saved["raw_games"] = [g for g in saved.get("raw_games") or [] if _future(g, now)]
    return saved if saved["data"] else None


def load_bookmaker(sport_key: str) -> dict | None:
    # Not filtered by kickoff: the Bookmaker parser stamps its local (ET) game
    # time as UTC, so tonight's games looked 4 h old and were dropped (10/12).
    # A started game has no board row left to match, and the next pull
    # replaces this cache anyway.
    saved = _read(_BOARD_DIR / f"bk_{sport_key}.json")
    return saved if saved and saved.get("data") else None


def load_period_pulled(live_event_ids: set) -> dict:
    """Saved period pull times for events still on a board (not yet started)."""
    saved = _read(_PERIOD_FILE) or {}
    return {eid: ts for eid, ts in saved.items() if eid in live_event_ids}
