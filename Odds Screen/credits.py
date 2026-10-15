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
import contextlib
import contextvars
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

# Who is spending (lead session, 10/12: so the next spike explains itself):
# bk_now, bk_login, bk_auto, reload:<sport>, close_capture, history, props,
# clv_hist, near_miss, alt_lines; "other" when no caller was set.
_caller = contextvars.ContextVar("odds_caller", default="other")


@contextlib.contextmanager
def caller(name: str):
    """Tag every Odds API call made inside this block (same thread)."""
    token = _caller.set(name)
    try:
        yield
    finally:
        _caller.reset(token)


def bound(fn):
    """fn carrying the current caller tag into a worker thread
    (ThreadPoolExecutor threads don't inherit context variables)."""
    name = _caller.get()

    def run(*args, **kwargs):
        with caller(name):
            return fn(*args, **kwargs)
    return run


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
        if last:
            by = _state.setdefault("by_caller", {})
            by[_caller.get()] = by.get(_caller.get(), 0) + last
        if used is not None:
            if _state["day_start_used"] is None or used < _state["day_start_used"]:
                # First call of the ET day, or the monthly counter reset. Other
                # scripts' spend before this call isn't seen; after it, it is.
                _state["day_start_used"] = used - (last or 0)
            _state["last_used"] = used
        remaining = _int(resp.headers.get("x-requests-remaining"))
        if remaining is not None:
            _state["month_remaining"] = remaining      # the account's own monthly counter
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
    used = used_today()
    with _lock:
        by = dict(sorted((_state.get("by_caller") or {}).items(), key=lambda kv: -kv[1]))
        outside = used - _state.get("summed", 0)
        untagged = _state.get("summed", 0) - sum(by.values())
    if untagged > 0:
        by["untagged"] = untagged       # spent before caller tags existed (10/12)
    if outside > 0:
        by["outside_app"] = outside     # account counter moved without an app call: another script
    return {"date": _today(), "used": used, "budget": DAILY_BUDGET,
            "remaining": max(0, DAILY_BUDGET - used), "by_caller": by,
            "month_remaining": _state.get("month_remaining"), "month_used": _state.get("last_used")}
