"""
Command line:

  python -m nhl_props scan            check upcoming games, log flags + snapshots
  python -m nhl_props scan --all      also print rows that aren't flagged
  python -m nhl_props watch-close     snapshot closing lines near puck drop
  python -m nhl_props clv             closing line value of everything flagged
"""

import argparse

from . import tracking
from .scan import run_scan, watch_closing


def _fmt_pct(x) -> str:
    return f"{x * 100:+.1f}%" if isinstance(x, (int, float)) else "-"


def print_scan(result: dict, show_all: bool) -> None:
    rows = result["rows"] if show_all else result["flags"]
    header = (f"{'Game':<12}{'Goalie':<22}{'Book':<15}{'Line':>5} {'Over':>6} {'Under':>6}"
              f"{'Mkt':>7}{'Model':>7}{'Gap':>6}  {'Side':<6}{'EV':>7}")
    print(header)
    print("-" * len(header))
    for r in rows:
        if r["status"] != "ok":
            if show_all:
                print(f"{r['game']:<12}{r['goalie']:<22}{r['book']:<15}{r['line']:>5}  {r['status']}")
            continue
        mark = " *" if r["flag"] else ""
        print(f"{r['game']:<12}{r['goalie']:<22}{r['book']:<15}{r['line']:>5} "
              f"{r['over']:>6} {r['under']:>6}{r['market_saves_mean']:>7}"
              f"{r['implied_saves_mean']:>7}{r['line_gap']:>6}  "
              f"{r['side']:<6}{_fmt_pct(r['ev']):>7}{mark}")

    skipped = {r["status"] for r in result["rows"] if r["status"] != "ok"}
    print(f"\n{len(result['flags'])} flagged ({len(result['new_flags'])} new), "
          f"{len(result['rows'])} offers checked. Credits remaining: {result['remaining_requests']}")
    for s in sorted(skipped):
        print(f"  not checked: {s}")
    for e in result["errors"]:
        print(f"  error: {e}")
    print("Mkt = market-implied saves, Model = implied from SOG − goals, "
          "Gap = SOG line − saves line − goals line")


def print_clv(report: list[dict]) -> None:
    if not report:
        print("No flagged plays with a closing snapshot yet. "
              "Run `watch-close` on game days to capture closing lines.")
        return
    print(f"{'Date':<12}{'Goalie':<22}{'Book':<15}{'Side':<6}{'Line':>5}{'Close':>7}{'CLV':>8}")
    for r in report:
        print(f"{r['commence_time'][:10]:<12}{r['goalie']:<22}{r['book']:<15}{r['side']:<6}"
              f"{r['line']:>5}{r['close_line']:>7}{_fmt_pct(r['clv_ev']):>8}")
    avg = sum(r["clv_ev"] for r in report) / len(report)
    beat = sum(r["clv_ev"] > 0 for r in report)
    print(f"\n{len(report)} plays, average CLV {_fmt_pct(avg)}, beat the close {beat}/{len(report)}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="nhl_props", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    scan = sub.add_parser("scan", help="check upcoming games")
    scan.add_argument("--hours", type=float, default=36, help="look-ahead window (default 36)")
    scan.add_argument("--all", action="store_true", help="print every offer, not just flags")
    scan.add_argument("--no-record", action="store_true", help="don't write snapshots/flags")

    watch = sub.add_parser("watch-close", help="capture closing lines near puck drop")
    watch.add_argument("--interval", type=float, default=10, help="minutes between checks")

    sub.add_parser("clv", help="closing line value report")

    args = parser.parse_args()
    if args.command == "scan":
        print_scan(run_scan(hours_ahead=args.hours, record=not args.no_record), args.all)
    elif args.command == "watch-close":
        try:
            watch_closing(args.interval)
        except KeyboardInterrupt:
            pass
    elif args.command == "clv":
        print_clv(tracking.clv_report())


if __name__ == "__main__":
    main()
