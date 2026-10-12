"""
Daily Odds API credit ledger (10/12). The user's budget is ~3,000 credits a
day; a Saturday was running ~4,000. Every Odds API call in app.py goes through
odds_get(), which reads the response's x-requests-last (this call's cost) and
x-requests-used (the account's running total, so credits spent by other
scripts -- clv.py, near_miss.py -- count too) into today's ET total, kept in
data/credits_today.json across restarts.

Spending priorities once a pull would push the day past DAILY_BUDGET:
  1. the kickoff -5 BetOnline close capture always runs;
  2. BK NOW / BK LOGIN run (the user pressed a button);
  3. BK AUTO's scheduled 1H/period reloads are skipped (app._refresh_board_periods).
"""
import json
import threading
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

DAILY_BUDGET = 3000
_ET = ZoneInfo("America/New_York")
_FILE = Path(__file__).resolve().parent / "data" / "credits_today.json"
_lock = threading.Lock()
_state: dict = {}


def _today() -> str:
    return datetime.now(_ET).date().isoformat()


def _load():
    global _state
    if _state.get("date") == _today():
        return
    try:
        s = json.loads(_FILE.read_text())
    except (FileNotFoundError, ValueError):
        s = {}
    if s.get("date") != _today():
        s = {"date": _today(), "summed": 0, "day_start_used": None, "last_used": None}
    _state = s


def _save():
    try:
        _FILE.parent.mkdir(exist_ok=True)
        _FILE.write_text(json.dumps(_state))
    except OSError:
        pass


def _int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def record(resp) -> None:
    """Add one Odds API response's cost to today's ledger."""
    last, used = _int(resp.headers.get("x-requests-last")), _int(resp.headers.get("x-requests-used"))
    if last is None and used is None:
        return
    with _lock:
        _load()
        _state["summed"] += last or 0
        if used is not None:
            if _state["day_start_used"] is None or used < _state["day_start_used"]:
                # First call of the ET day, or the monthly counter reset. Other
                # scripts' spend before this call isn't seen; after it, it is.
                _state["day_start_used"] = used - (last or 0)
            _state["last_used"] = used
        _save()


def odds_get(*args, **kwargs):
    """requests.get for The Odds API, recorded in the ledger."""
    resp = requests.get(*args, **kwargs)
    try:
        record(resp)
    except Exception as e:          # the ledger must never break a pull
        print(f"[credits] ledger update failed: {type(e).__name__}: {e}")
    return resp


def used_today() -> int:
    with _lock:
        _load()
        by_counter = (_state["last_used"] - _state["day_start_used"]) \
            if _state.get("day_start_used") is not None and _state.get("last_used") is not None else 0
        return max(_state["summed"], by_counter)


def would_exceed(cost: int) -> bool:
    """True when spending `cost` more credits would take today past the budget."""
    return used_today() + max(0, cost) > DAILY_BUDGET


def today() -> dict:
    return {"date": _today(), "used": used_today(), "budget": DAILY_BUDGET,
            "remaining": max(0, DAILY_BUDGET - used_today())}
