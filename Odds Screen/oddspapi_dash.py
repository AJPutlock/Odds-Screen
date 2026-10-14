"""
OddsPapi comparison dashboard (10/13): http://localhost:5066

A standalone page next to the main board (port 5000) so the trial never
touches app.py. It reads the board from /api/cache/<sport> -- the in-memory
board, no Odds API credits -- and lines it up against OddsPapi snapshots built
by oddspapi.py (fixture list: 1 billable request per sport per 6 h; prices:
free historical-odds calls).

Run:  python oddspapi_dash.py
"""
import requests
from flask import Flask, jsonify, request, send_from_directory

import oddspapi as op

BOARD = "http://localhost:5000"
app = Flask(__name__, static_folder="static")


@app.route("/")
def index():
    return send_from_directory("static", "oddspapi.html")


@app.route("/api/meta")
def meta():
    return jsonify({"sports": list(op.SPORTS), "books": op.BOOK_CHOICES, "default_books": op.DEFAULT_BOOKS,
                    "board_book": op.BOARD_BOOK, "sharp_ref": op.SHARP_REF, "enabled": op.enabled()})


@app.route("/api/account")
def account():
    try:
        acc = op.account() if request.args.get("live") == "1" else (op.account_cached() or op.account())
    except op.FeedError as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({**{k: v for k, v in acc.items() if k != "bookmakers"}, "ledger": op.ledger(),
                    "month_billable": op.billable_this_month(), "month_close": op.billable_this_month("close")})


@app.route("/api/refresh/<sport_key>", methods=["POST"])
def refresh(sport_key):
    body = request.get_json(silent=True) or {}
    try:
        return jsonify(op.refresh(sport_key, body.get("books") or op.DEFAULT_BOOKS,
                                  refresh_fixtures=bool(body.get("refresh_fixtures"))))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/job/<sport_key>")
def job(sport_key):
    fx = op.fixtures_cached(sport_key) or {}
    return jsonify({**op.job(sport_key), "fixtures_fetched_at": fx.get("fetched_at")})


@app.route("/api/compare/<sport_key>")
def compare(sport_key):
    if sport_key not in op.SPORTS:
        return jsonify({"error": f"unknown sport {sport_key}"}), 404
    try:
        board = requests.get(f"{BOARD}/api/cache/{sport_key}", timeout=15).json()
        board_err = None
    except (requests.exceptions.RequestException, ValueError) as e:
        board, board_err = {"data": []}, f"main board unreachable on :5000 ({type(e).__name__})"
    times = board_times(sport_key, board) if not board_err else {}
    out = op.compare(sport_key, board, times)
    out["board_error"] = board_err or board.get("error")
    out["board_times"] = times
    return jsonify(out)


ODDS_API_BOOKS = ["draftkings", "fanduel", "williamhill_us", "betmgm", "betrivers", "espnbet",
                  "fanatics", "hardrockbet"]


def board_times(sport_key, board):
    """When the board last pulled each source -- read-only status routes, no
    credits: the Odds API pull, odds-api.io (Circa, BetOnline), the exchange
    refresher, the Bookmaker collector."""
    times = {bk: board.get("last_updated") for bk in ODDS_API_BOOKS}
    try:
        fs = requests.get(f"{BOARD}/api/feeds/status", timeout=10).json()
        oi = (fs.get("oddsio") or {}).get(sport_key) or {}
        for bk in ("circa", "betonlineag"):
            if oi.get(f"{'betonline' if bk == 'betonlineag' else bk}_games"):
                times[bk] = oi.get("fetched_at")
        ex = (fs.get("exchanges") or {}).get(sport_key) or {}
        for bk, n in (ex.get("games") or {}).items():
            if n:
                times[bk] = ex.get("at")
    except (requests.exceptions.RequestException, ValueError):
        pass
    try:
        bs = requests.get(f"{BOARD}/api/bookmaker/status/{sport_key}", timeout=10).json()
        if bs.get("last_updated"):
            times["bookmaker"] = bs["last_updated"]
    except (requests.exceptions.RequestException, ValueError):
        pass
    return times


if __name__ == "__main__":
    app.run(port=5066, debug=False, threaded=True)
