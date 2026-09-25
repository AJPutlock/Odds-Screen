"""
Logging for closing-line-value (CLV) tracking.

  snapshots.csv  every saves line seen, each time a scan or closing capture runs
  flags.csv      every play the checker flagged, at the price when first flagged

CLV answers "did the market move toward my side after I flagged it?". It's a
far faster read on whether the signal is real than win/loss over a few
hundred bets.
"""

import csv
from datetime import datetime, timezone
from pathlib import Path

from . import config
from .odds_math import implied_mean, no_vig_two_way, prob_over_under

SNAPSHOT_FIELDS = ["captured_at", "event_id", "commence_time", "goalie", "team",
                   "book", "line", "over_dec", "under_dec"]

FLAG_FIELDS = ["flagged_at", "event_id", "commence_time", "game", "goalie", "team",
               "opponent", "book", "side", "line", "price_dec", "ev",
               "implied_saves_mean", "market_saves_mean", "saves_edge", "line_gap",
               "sog_line", "goals_line"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _append(path: Path, fields: list[str], rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


def _read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def record_snapshots(rows: list[dict], path: Path | None = None,
                     captured_at: str | None = None) -> None:
    captured_at = captured_at or _now_iso()
    _append(path or config.SNAPSHOTS_CSV, SNAPSHOT_FIELDS,
            [{**r, "captured_at": captured_at} for r in rows])


def record_flags(rows: list[dict], path: Path | None = None,
                 flagged_at: str | None = None) -> list[dict]:
    """Log newly flagged plays. A play already logged (same game, goalie, book,
    side and line) isn't logged again, so CLV is measured from first sight."""
    path = path or config.FLAGS_CSV
    seen = {(f["event_id"], f["goalie"], f["book"], f["side"], float(f["line"]))
            for f in _read(path)}
    flagged_at = flagged_at or _now_iso()

    new = []
    for r in rows:
        key = (r["event_id"], r["goalie"], r["book"], r["side"], float(r["line"]))
        if r.get("flag") and key not in seen:
            seen.add(key)
            price = r["over_dec"] if r["side"] == "over" else r["under_dec"]
            new.append({**r, "flagged_at": flagged_at, "price_dec": price})
    _append(path, FLAG_FIELDS, new)
    return new


def closing_snapshot(flag: dict, snapshots: list[dict]) -> dict | None:
    """Last snapshot of the same goalie at the same book before puck drop."""
    candidates = [s for s in snapshots
                  if s["event_id"] == flag["event_id"]
                  and s["goalie"] == flag["goalie"]
                  and s["book"] == flag["book"]
                  and s["captured_at"] <= flag["commence_time"]]
    return max(candidates, key=lambda s: s["captured_at"]) if candidates else None


def clv_for_flag(flag: dict, close: dict) -> dict:
    """
    Compare the flagged price with the fair closing price.

    If the line moved (say 27.5 -> 28.5), the closing line + price is turned
    into an implied mean and re-priced at the line that was flagged.
    clv_ev = fair closing probability of the flagged side × flagged price − 1.
    """
    line = float(flag["line"])
    close_line = float(close["line"])
    fair_over, _ = no_vig_two_way(float(close["over_dec"]), float(close["under_dec"]))
    close_mean = implied_mean(close_line, fair_over, config.SAVES_DISPERSION)

    p_over, p_under, _ = prob_over_under(line, close_mean, config.SAVES_DISPERSION)
    decided = p_over + p_under
    p_side = (p_over if flag["side"] == "over" else p_under) / decided

    return {
        "close_line": close_line,
        "close_mean": round(close_mean, 2),
        "close_fair_prob": round(p_side, 4),
        "clv_ev": round(p_side * float(flag["price_dec"]) - 1, 4),
    }


def clv_report(flags_path: Path | None = None, snapshots_path: Path | None = None,
               now: str | None = None) -> list[dict]:
    """CLV for every flagged play whose game has started and has a closing snapshot."""
    snapshots = _read(snapshots_path or config.SNAPSHOTS_CSV)
    now = now or _now_iso()
    report = []
    for flag in _read(flags_path or config.FLAGS_CSV):
        if flag["commence_time"] > now:
            continue                                   # not closed yet
        close = closing_snapshot(flag, snapshots)
        if close is None or close["captured_at"] <= flag["flagged_at"]:
            continue                                   # no later price to compare to
        report.append({**flag, **clv_for_flag(flag, close)})
    return report
