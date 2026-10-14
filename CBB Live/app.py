"""
Flask server for the CBB Live hub (men's college basketball; forked from CFB Live).

Runs on port 5057 to stay clear of Odds Screen and the NCAAB app (5000), CFB Live
(5055), SGO (5065) and OddsPapi (5066).

    python app.py
    http://127.0.0.1:5057
"""
import os
import logging
import pathlib
from datetime import datetime, timezone

from flask import Flask, render_template, jsonify, request

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=pathlib.Path(__file__).parent / ".env")
except ImportError:
    pass

import database as db
from game_manager import GameManager
from scrapers import espn
from scrapers.odds_api import REC_BOOKS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# The Odds API key comes from CBB Live/.env (ODDS_API_KEY, gitignored). No
# fallback literal: the key must never be committed.
ODDS_API_KEY = os.environ.get("ODDS_API_KEY")
# Odds polling is OFF unless CBB_ODDS=1: the key is shared with Odds Screen and the
# idle loop alone costs ~430 credits/day. Turn it on in .env once the season starts.
if os.environ.get("CBB_ODDS", "0") != "1":
    ODDS_API_KEY = None

app = Flask(__name__)
db.init_db()
# Drop snapshots from books no longer in REC_BOOKS, so changing the book list
# takes effect on an existing database instead of leaving removed books on the
# board. (Caught live: bovada kept rendering after it was taken off the list.)
db.purge_books(REC_BOOKS)
gm = GameManager(odds_api_key=ODDS_API_KEY)


# ---- Pages ----

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/game/<game_id>")
def game_page(game_id):
    return render_template("game.html", game_id=game_id)


# ---- API ----

@app.route("/api/scoreboard")
def api_scoreboard():
    games = gm.get_scoreboard()
    # Counts always describe the whole board, so the tab badges stay correct
    # even when the response itself is filtered down to one state.
    counts = _counts(games)
    state_filter = request.args.get("state")
    if state_filter:
        games = [g for g in games if g.get("state") == state_filter]
    return jsonify({
        "games": games,
        "counts": counts,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "last_scoreboard_at": gm.last_scoreboard_at,
        "last_odds_at": gm.last_odds_at,
    })


def _counts(games):
    out = {"pre": 0, "in": 0, "post": 0}
    for g in games:
        if g.get("state") in out:
            out[g["state"]] += 1
    return out


@app.route("/api/game/<game_id>")
def api_game(game_id):
    include_summary = request.args.get("summary", "1") != "0"
    # ?at=<elapsed seconds> prices the model at that point of the game (replay).
    detail = gm.get_game_detail(game_id, include_summary=include_summary,
                                at=request.args.get("at", type=int))
    if not detail:
        return jsonify({"error": "game not found"}), 404
    return jsonify(detail)


@app.route("/api/game/<game_id>/summary")
def api_summary(game_id):
    try:
        return jsonify(gm.get_summary(game_id, force=request.args.get("force") == "1"))
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/game/<game_id>/pace")
def api_pace(game_id):
    """
    Play counter and live fair value for one game.

    Costs one ESPN summary request (TTL-cached), no Odds API credits. The model
    and its limits are documented in pace.py.
    """
    detail = gm.get_game_detail(game_id)
    if not detail:
        return jsonify({"error": "game not found"}), 404
    view = detail.get("pace")
    if view is None:
        return jsonify({"error": "no pace view (game has not kicked off)"}), 409
    return jsonify(view)


@app.route("/api/pace")
def api_pace_board():
    """
    Every live game ranked by how far the market is from the model's fair total.

    Reads the local database for state and odds but needs one ESPN summary
    request per live game for the play count, so it is user-triggered, not part
    of any background loop.
    """
    rows = []
    for g in gm.get_scoreboard():
        if g.get("state") != "in":
            continue
        detail = gm.get_game_detail(g["game_id"])
        view = (detail or {}).get("pace")
        if not view:
            continue
        rows.append({
            "game_id": g["game_id"],
            "matchup": "{0} @ {1}".format(g.get("away_abbr"), g.get("home_abbr")),
            "status": g.get("status_detail"),
            "score": "{0}-{1}".format(g.get("away_score"), g.get("home_score")),
            "plays_run": view["plays_run"],
            "plays_expected": view["plays_expected"],
            "pace_z": view["pace_z"],
            "pregame_total": view.get("pregame_total"),
            "fair_total": view.get("fair_total"),
            "live_total": view.get("live_total"),
            "total_edge": view.get("total_edge"),
            "fair_spread_home": view.get("fair_spread_home"),
            "live_spread_home": view.get("live_spread_home"),
            "spread_edge": view.get("spread_edge"),
        })
    rows.sort(key=lambda r: abs(r["total_edge"] or 0), reverse=True)
    return jsonify({"count": len(rows), "games": rows})


@app.route("/api/game/<game_id>/all_odds")
def api_all_odds(game_id):
    """
    Every market for one game: alternate ladders, halves, quarters, team totals.

    User-triggered only - it costs one API credit per market.
    """
    try:
        payload = gm.get_all_odds(game_id, force=request.args.get("force") == "1")
        if payload is None:
            return jsonify({"error": "game not found or odds client unavailable"}), 404
        return jsonify(payload)
    except Exception as e:
        logger.error("all_odds failed for %s: %s", game_id, e)
        return jsonify({"error": str(e)}), 502


@app.route("/api/game/<game_id>/odds_history")
def api_odds_history(game_id):
    return jsonify({
        "game_id": game_id,
        "market": request.args.get("market"),
        "rows": db.get_odds_history(
            game_id, market=request.args.get("market"), book=request.args.get("book")
        ),
    })


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    what = (request.json or {}).get("what", "all") if request.is_json else "all"
    done = []
    try:
        if what in ("all", "scoreboard"):
            gm.refresh_scoreboard()
            done.append("scoreboard")
        if what in ("all", "odds"):
            gm.refresh_odds()
            done.append("odds")
        return jsonify({"status": "ok", "refreshed": done})
    except Exception as e:
        logger.error("manual refresh failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/status")
def api_status():
    return jsonify(gm.get_status())


@app.route("/api/debug/unmatched")
def api_unmatched():
    """Odds API events that found no ESPN game. Usually next week's slate."""
    return jsonify({"unmatched": gm.unmatched_odds, "count": len(gm.unmatched_odds)})


@app.route("/api/debug/scoreboard_raw")
def api_scoreboard_raw():
    """Straight from ESPN, bypassing the database - for diagnosing parse issues."""
    return jsonify({"games": espn.fetch_scoreboard()})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5057"))
    print("\n" + "=" * 62)
    print("  CBB Live Hub")
    print("=" * 62)
    print("  Server:    http://127.0.0.1:{0}".format(port))
    print("             (use 127.0.0.1, NOT localhost - see README)")
    print("  Scores:    ESPN (public API, no key)")
    print("  Odds:      The Odds API  [key {0}]".format("set" if ODDS_API_KEY else "MISSING"))
    print("  Database:  {0}".format(db.DB_PATH))
    print("=" * 62 + "\n")

    gm.start()
    try:
        # use_reloader=False: the reloader would start a second copy of both
        # background loops, doubling odds-API spend.
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    finally:
        gm.stop()
