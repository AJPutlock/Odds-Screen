"""
Orchestration for the CBB Live hub (forked from CFB Live).

Runs two background loops on different cadences, for different reasons:

  scoreboard (fast, ~20s)  ESPN, ONE request covering every game on the board.
                           Free and cheap, so it sets the app's heartbeat.
  odds       (slow, ~60s)  The Odds API, costs len(markets) credits per call.
                           Deliberately slower than the scoreboard so that quota
                           is spent on line MOVEMENT, not on re-reading a board
                           that mostly has not changed.

Per-game ESPN summaries (box score, drives, win-prob series) cost one request
each, so they are fetched lazily when a game detail page is opened and held in a
short TTL cache, rather than polled for all 75 games.

Note the contrast with the NCAAB app: there, throughput was capped by
stats.ncaa.org's IP-level Akamai blocking, which is what forced the elaborate
event-triggered priority tiers. ESPN's public API has no such wall, so the
budget here is governed by The Odds API quota alone - a much simpler constraint.
"""
import os
import pathlib
import time
import logging
import threading
from datetime import datetime, timezone

import database as db
import pace
from scrapers import espn, exchanges
from scrapers.odds_api import (
    OddsAPIClient, OddsAPIError, to_snapshot_rows, dedupe_against_previous,
    group_event_odds, ALL_MARKETS, CORE_MARKETS, REC_BOOKS, ENABLED_MARKET_GROUPS,
)
from scrapers.exchanges import novig_live_net
from team_match import match_events, team_score

logger = logging.getLogger(__name__)

# ESPN is free and caches ~9s, so the scoreboard is polled fast enough that the
# score is essentially current play-to-play. The UI then reads the database
# every 30s, which costs nothing.
# ESPN sets cache-control: max-age=9 on the scoreboard, so polling faster than
# ~10s just re-reads the same cached payload. 10s is the floor worth paying for.
SCOREBOARD_INTERVAL = int(os.environ.get("SCOREBOARD_INTERVAL", "10"))

# Odds are metered, so they run on a much slower cadence: 3 markets per poll
# (full-game spreads, totals, moneyline) = 3 credits, once every 3 minutes.
# That is 60 credits/hour while games are live, against 180 at the old 60s.
# The tradeoff is line-movement granularity - a move that happens and reverses
# inside one 3-minute window is not captured.
ODDS_INTERVAL = int(os.environ.get("ODDS_INTERVAL", "180"))
SUMMARY_TTL = int(os.environ.get("SUMMARY_TTL", "45"))

# Full per-event odds are user-triggered and cost a credit per market, so the
# cache window is generous: re-opening the same game shortly after should not
# re-spend. An explicit refresh click bypasses it.
ALL_ODDS_TTL = int(os.environ.get("ALL_ODDS_TTL", "120"))

# When nothing is live, back off hard - there is no reason to poll a board of
# finished and not-yet-started games every 20 seconds.
IDLE_SCOREBOARD_INTERVAL = int(os.environ.get("IDLE_SCOREBOARD_INTERVAL", "120"))
IDLE_ODDS_INTERVAL = int(os.environ.get("IDLE_ODDS_INTERVAL", "600"))


def _historical_close(game_id):
    """(total, home_spread) from espn_historical.db's CBBD `lines` (spread is the home
    number there). ESPN drops pickcenter from finished games, so replays need this.
    Same provider priority as the model's calibration."""
    import sqlite3
    path = db.HISTORICAL_DB_PATH
    if not game_id or not os.path.exists(path):
        return None, None
    try:
        conn = sqlite3.connect("file:{0}?mode=ro".format(pathlib.Path(path).as_posix()), uri=True, timeout=5)
        row = conn.execute("""
            SELECT over_under, spread FROM lines WHERE game_id = ? AND spread IS NOT NULL
            ORDER BY CASE provider WHEN 'ESPN BET' THEN 0 WHEN 'Draft Kings' THEN 1
                     WHEN 'Bovada' THEN 2 WHEN 'consensus' THEN 3 ELSE 4 END LIMIT 1""",
            (str(game_id),)).fetchone()
        conn.close()
    except sqlite3.Error as e:
        logger.debug("historical close lookup failed for %s: %s", game_id, e)
        return None, None
    return (row[0], row[1]) if row else (None, None)


def _state_at(plays, at):
    """Score at `at` elapsed seconds from the ESPN play stream (for replay pricing)."""
    best = {"elapsed_seconds": at, "home_score": 0, "away_score": 0, "period": 1, "clock": "20:00"}
    for p in plays:
        e = espn._elapsed_seconds(p.get("period"), espn._clock_to_seconds(p.get("clock")))
        if e is None or e > at:
            continue
        if p.get("home_score") is not None:
            best.update(home_score=int(p["home_score"]), away_score=int(p["away_score"]),
                        period=p.get("period"), clock=p.get("clock"))
    return best


def _as_float(v):
    """Tolerant float parse. ESPN sends odds fields as both numbers and strings."""
    try:
        return float(str(v).strip().replace("+", ""))
    except (TypeError, ValueError, AttributeError):
        return None


def _american_to_prob(price):
    """American odds -> implied probability (vig included)."""
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if p == 0:
        return None
    return (-p) / ((-p) + 100.0) if p < 0 else 100.0 / (p + 100.0)


def _quote_rank(market, side, quote):
    """
    Sort key for "which book has the best version of this bet".

    Ranking on price alone would be wrong: a book at -3.5 (-105) is not beating
    one at -2.5 (-110), because the point moved against you by more than the
    price made up. Point dominates, price breaks the tie.

    Direction of "better" depends on the market:
      spreads  higher point is better for whichever side is quoted
      totals   Over wants a LOWER number, Under wants a higher one
      h2h      no point at all, price decides
    """
    point = quote.get("point")
    price = quote.get("price")
    price_key = price if price is not None else float("-inf")

    if market == "h2h" or point is None:
        return (0.0, price_key)
    if market == "totals":
        point_key = -point if side == "over" else point
    else:
        point_key = point
    return (point_key, price_key)


def _group_by_side(quotes):
    out = {}
    for q in quotes:
        out.setdefault(q["side"], []).append(q)
    return out


def _no_vig_pair(price_a, price_b):
    """Two-way de-vig. Returns (prob_a, prob_b) or (None, None)."""
    pa, pb = _american_to_prob(price_a), _american_to_prob(price_b)
    if pa is None or pb is None:
        return None, None
    total = pa + pb
    if total <= 0:
        return None, None
    return pa / total, pb / total


class GameManager:
    def __init__(self, odds_api_key=None):
        self.odds_client = OddsAPIClient(odds_api_key) if odds_api_key else None
        self._threads = []
        self._stop = threading.Event()

        self._summary_cache = {}        # game_id -> (fetched_at_epoch, payload)
        self._summary_lock = threading.Lock()

        self._all_odds_cache = {}       # game_id -> (fetched_at_epoch, payload)
        self._all_odds_lock = threading.Lock()

        # (game_id, book, market, side) -> (point, price), to suppress no-op rows.
        self._last_odds = {}
        self._odds_lock = threading.Lock()

        self.last_scoreboard_at = None
        self.last_odds_at = None
        self.last_error = None
        self.unmatched_odds = []

    # -- lifecycle ---------------------------------------------------------

    def start(self):
        if self._threads:
            return
        self._stop.clear()
        for name, fn in (("scoreboard", self._scoreboard_loop), ("odds", self._odds_loop)):
            t = threading.Thread(target=fn, name=name, daemon=True)
            t.start()
            self._threads.append(t)
        logger.info("background loops started (scoreboard %ss / odds %ss)",
                    SCOREBOARD_INTERVAL, ODDS_INTERVAL)

    def stop(self):
        self._stop.set()
        for t in self._threads:
            t.join(timeout=5)
        self._threads = []

    def is_running(self):
        return any(t.is_alive() for t in self._threads)

    def _has_live_games(self):
        try:
            return any(g["state"] == "in" for g in db.get_games())
        except Exception:
            return False

    def _scoreboard_loop(self):
        while not self._stop.is_set():
            try:
                self.refresh_scoreboard()
            except Exception as e:
                self.last_error = "scoreboard: {0}".format(e)
                logger.error("scoreboard refresh failed: %s", e)
            interval = SCOREBOARD_INTERVAL if self._has_live_games() else IDLE_SCOREBOARD_INTERVAL
            self._stop.wait(interval)

    def _odds_loop(self):
        # Let the scoreboard populate first so odds have games to match onto.
        self._stop.wait(3)
        while not self._stop.is_set():
            try:
                self.refresh_odds()
            except Exception as e:
                self.last_error = "odds: {0}".format(e)
                logger.error("odds refresh failed: %s", e)
            interval = ODDS_INTERVAL if self._has_live_games() else IDLE_ODDS_INTERVAL
            self._stop.wait(interval)

    # -- refreshes ---------------------------------------------------------

    # Cap how many per-game summaries one cycle may pull to repair degenerate
    # situations. ESPN is free, but this runs every poll and should not turn
    # into 30 extra requests whenever half the board is in a TV timeout.
    MAX_SPOT_REPAIRS = 12

    def _repair_degenerate_spots(self, games):
        """
        Fill in the real line of scrimmage for live games whose situation block
        ESPN has clobbered with a non-snap event (see espn._situation).

        Without this the board freezes mid-drive: after a punt followed by a TV
        timeout, ESPN rewinds the spot to where the punt was kicked from and
        drops the down entirely, so the card keeps showing the punting team's
        4th-and-2 instead of the receiving team's new 1st-and-10.
        """
        candidates = [
            g for g in games
            if g.get("state") == "in" and (g.get("situation") or {}).get("degenerate")
        ]
        repaired = 0
        for g in candidates[:self.MAX_SPOT_REPAIRS]:
            try:
                summary = self.get_summary(g["game_id"])      # TTL-cached
                spot = espn.spot_from_summary(summary)
            except Exception as e:
                logger.debug("spot repair failed for %s: %s", g["game_id"], e)
                continue
            if not spot or spot.get("yard_line") is None:
                continue
            s = g.setdefault("situation", {})
            s["down"] = spot["down"]
            s["distance"] = spot["distance"]
            s["yard_line"] = spot["yard_line"]
            if spot.get("team_id"):
                s["possession_id"] = spot["team_id"]
                s["possession_is_home"] = spot["team_id"] == g["home"].get("id")
            s["down_distance_text"] = None   # ESPN's text describes the stale spot
            s["present"] = True
            s["repaired"] = True
            repaired += 1
        if repaired:
            logger.debug("repaired field position for %d of %d degenerate games",
                         repaired, len(candidates))
        return repaired

    def refresh_scoreboard(self):
        started = datetime.now(timezone.utc).isoformat()
        # CBB_DATE=YYYYMMDD pins the board to a past date (UI testing off-season).
        games = espn.fetch_scoreboard(date=os.environ.get("CBB_DATE") or None)
        db.upsert_games(games)
        changed = db.record_game_states(games)
        self.last_scoreboard_at = datetime.now(timezone.utc).isoformat()
        db.log_refresh("scoreboard", started, games_touched=len(games),
                       detail="{0} state rows".format(changed))
        logger.debug("scoreboard: %d games, %d state changes", len(games), changed)
        return games

    def refresh_odds(self):
        if not self.odds_client:
            return []
        started = datetime.now(timezone.utc).isoformat()

        # Match against the ESPN board as the app currently understands it.
        rows = db.get_games()
        espn_games = [
            {
                "game_id": r["game_id"],
                "start_time": r["start_time"],
                "home": {"name": r["home_team"], "location": None, "short_name": r["home_abbr"]},
                "away": {"name": r["away_team"], "location": None, "short_name": r["away_abbr"]},
            }
            for r in rows
        ]
        states = {
            r["game_id"]: {
                "state": r["state"], "period": r["period"],
                "clock_seconds": r["clock_seconds"],
                "home_score": r["home_score"], "away_score": r["away_score"],
            }
            for r in rows
        }

        try:
            events = self.odds_client.fetch_odds()
        except OddsAPIError as e:
            db.log_refresh("odds", started, status="error", detail=str(e))
            raise

        matches, unmatched = match_events(events, espn_games)
        self.unmatched_odds = unmatched
        for oid, gid in matches.items():
            db.set_odds_event_id(gid, oid)

        snapshot_rows = to_snapshot_rows(events, matches, states)

        # NoVig's game-market fee applies ONLY when a game is live, so the Odds
        # API's NoVig prices are already net for a pregame board but not for
        # this one. Take the fee off every in-play NoVig quote.
        novig_adjusted = 0
        for r in snapshot_rows:
            if r["book"] == "novig" and r.get("is_live"):
                net = novig_live_net(r.get("price"))
                if net is not None:
                    r["price"] = net
                    novig_adjusted += 1
        if novig_adjusted:
            logger.debug("applied NoVig live fee to %d quotes", novig_adjusted)

        # Exchanges come from their own feeds, never the Odds API, because the
        # Odds API strips their trading fees. See scrapers/exchanges.py.
        try:
            snapshot_rows.extend(self._exchange_rows(rows, states))
        except Exception as e:
            logger.warning("exchange feeds failed: %s", e)

        with self._odds_lock:
            fresh = dedupe_against_previous(snapshot_rows, self._last_odds)
            for r in snapshot_rows:
                self._last_odds[(r["game_id"], r["book"], r["market"], r["side"])] = (
                    r.get("point"), r.get("price")
                )

        written = db.record_odds(fresh)
        self.last_odds_at = datetime.now(timezone.utc).isoformat()
        db.log_refresh("odds", started, games_touched=len(matches),
                       detail="{0} price changes of {1} quotes".format(written, len(snapshot_rows)))
        logger.debug("odds: %d matched, %d changed lines", len(matches), written)
        return fresh

    def _exchange_rows(self, game_rows, states):
        """
        Polymarket + Kalshi quotes, matched to ESPN games and shaped as
        odds_snapshots rows.

        Both are matched with the same event-level matcher the Odds API uses,
        so a pairing still has to satisfy both teams at once.
        """
        espn_games = [
            {
                "game_id": r["game_id"],
                "start_time": r["start_time"],
                "home": {"name": r["home_team"], "location": None, "short_name": r["home_abbr"]},
                "away": {"name": r["away_team"], "location": None, "short_name": r["away_abbr"]},
            }
            for r in game_rows
        ]
        captured_at = datetime.now(timezone.utc).isoformat()
        out = []

        def emit(gid, book, market, side, point, price):
            st = states.get(gid) or {}
            out.append({
                "game_id": gid, "odds_event_id": None, "captured_at": captured_at,
                "book": book, "market": market, "side": side,
                "point": point, "price": price,
                "book_updated_at": captured_at,
                "is_live": 1 if st.get("state") == "in" else 0,
                "period": st.get("period"), "clock_seconds": st.get("clock_seconds"),
                "home_score": st.get("home_score"), "away_score": st.get("away_score"),
            })

        # --- Polymarket: quotes already carry both team names ---
        poly = exchanges.fetch_polymarket(live_only=False)
        if poly:
            keyed = {}
            for q in poly:
                keyed.setdefault((q["home_team"], q["away_team"], q.get("commence_time")), []).append(q)
            pseudo = [
                {"id": i, "home_team": k[0], "away_team": k[1], "commence_time": k[2]}
                for i, k in enumerate(keyed)
            ]
            pmatch, _ = match_events(pseudo, espn_games)
            keys = list(keyed)
            for idx, gid in pmatch.items():
                for q in keyed[keys[idx]]:
                    emit(gid, "polymarket", q["market"], q["side"], q.get("point"), q["price"])

        # --- Kalshi: contracts paired into games by ticker prefix ---
        kal = exchanges.fetch_kalshi(exchanges.kalshi_date_tags())
        games = exchanges.kalshi_games(kal)
        if games:
            pseudo = [
                {"id": i, "home_team": g["teams"][1], "away_team": g["teams"][0],
                 "commence_time": None}
                for i, g in enumerate(games)
            ]
            kmatch, _ = match_events(pseudo, espn_games)
            by_id = {r["game_id"]: r for r in game_rows}
            for idx, gid in kmatch.items():
                row = by_id.get(gid)
                if not row:
                    continue
                for name, q in games[idx]["quotes"].items():
                    # Resolve which ESPN side this contract's team is.
                    side = None
                    if team_score(name, {"name": row["home_team"], "short_name": row["home_abbr"]}) >= 0.6:
                        side = "home"
                    if team_score(name, {"name": row["away_team"], "short_name": row["away_abbr"]}) >= 0.6:
                        side = "away" if side is None else side
                    if side:
                        emit(gid, "kalshi", "h2h", side, None, q["price"])

        if out:
            logger.info("exchanges: %d quotes attached", len(out))
        return out

    def get_all_odds(self, game_id, force=False):
        """
        Every market this app tracks for one game, from the per-event endpoint.

        Costs len(ALL_MARKETS) credits, so it is only ever triggered by the user
        opening (or explicitly refreshing) a game - never by a timer. Cached so
        re-rendering the same page does not re-spend.
        """
        if not self.odds_client:
            return None
        game = db.get_game(game_id)
        if not game:
            return None
        event_id = game.get("odds_event_id")
        if not event_id:
            return {
                "groups": [], "books": [], "book_display": {}, "market_count": 0,
                "note": "No Odds API event is matched to this game yet.",
            }

        now = time.time()
        with self._all_odds_lock:
            hit = self._all_odds_cache.get(game_id)
            if hit and not force and (now - hit[0]) < ALL_ODDS_TTL:
                payload = dict(hit[1])
                payload["cached"] = True
                return payload

        event = self.odds_client.fetch_event_odds(event_id)
        payload = group_event_odds(event, books=REC_BOOKS)
        payload["fetched_at"] = datetime.now(timezone.utc).isoformat()
        payload["credits_spent"] = len(ALL_MARKETS)
        payload["cached"] = False

        with self._all_odds_lock:
            self._all_odds_cache[game_id] = (now, payload)
        return payload

    def get_summary(self, game_id, force=False):
        """ESPN per-game detail, TTL-cached so repeated page loads cost one request."""
        now = time.time()
        with self._summary_lock:
            hit = self._summary_cache.get(game_id)
            if hit and not force and (now - hit[0]) < SUMMARY_TTL:
                return hit[1]
        payload = espn.fetch_summary(game_id)
        with self._summary_lock:
            self._summary_cache[game_id] = (now, payload)
        return payload

    # -- reads for the API -------------------------------------------------

    def get_scoreboard(self):
        """
        Every game with its latest line attached, shaped for the UI.

        Odds for the whole board are fetched in ONE query and bucketed by game.
        Doing it per game meant 150 correlated scans of a table that grows all
        season - measured at ~8.7s for 75 games against 43k rows.
        """
        games = db.get_games()
        odds_by_game = db.get_latest_odds_all(books=REC_BOOKS)
        out = []
        for g in games:
            row = dict(g)
            rows = odds_by_game.get(g["game_id"], [])
            row["odds"] = self._best_lines_from(rows)
            row["market_home_wp"] = self._market_home_wp_from(rows)
            out.append(row)
        return out

    def get_game_detail(self, game_id, include_summary=True, at=None):
        game = db.get_game(game_id)
        if not game:
            return None
        detail = dict(game)
        detail["odds"] = db.get_latest_odds(game_id, books=REC_BOOKS)
        detail["best_lines"] = self._best_lines(game_id)
        detail["market_home_wp"] = self._market_home_wp(game_id)
        detail["state_history"] = db.get_state_history(game_id)
        detail["odds_history"] = db.get_odds_history(game_id)
        if include_summary:
            try:
                detail["summary"] = self.get_summary(game_id)
            except Exception as e:
                logger.warning("summary fetch failed for %s: %s", game_id, e)
                detail["summary"] = None
        if at is not None and detail.get("summary"):
            detail["replay"] = _state_at(detail["summary"].get("plays") or [], at)
        try:
            detail["pace"] = self._pace_view(detail)
        except Exception as e:
            logger.warning("pace view failed for %s: %s", game_id, e)
            detail["pace"] = None
        return detail

    # -- live fair value ---------------------------------------------------

    def _pregame_anchor(self, detail):
        """
        The pregame number this game opened at, as (total, home_spread).

        Source is ESPN pickcenter, which does NOT move during the game - exactly
        what the model wants, since it prices a DEVIATION from the opening
        number rather than trying to out-forecast the live line.

        The sign of pickcenter's `spread` field is read from the `details`
        string ("ALA -14.5") rather than trusted directly, because the raw field
        is stated relative to the favourite in some payloads and relative to the
        home team in others. Getting it backwards silently doubles every margin
        error, so derive it from the abbreviation whenever one is present.
        """
        summary = detail.get("summary") or {}
        total = home_spread = None
        home_abbr = (detail.get("home_abbr") or "").upper()
        away_abbr = (detail.get("away_abbr") or "").upper()

        for p in summary.get("pregame_odds") or []:
            if total is None and p.get("over_under") is not None:
                total = _as_float(p.get("over_under"))
            if home_spread is not None:
                continue
            raw = _as_float(p.get("spread"))
            details = (p.get("details") or "").strip().upper()
            if details and " " in details:
                abbr, _, num = details.partition(" ")
                val = _as_float(num)
                if val is not None and abbr in (home_abbr, away_abbr):
                    # `val` is the favourite's number (negative). Express it as
                    # the home side.
                    home_spread = val if abbr == home_abbr else -val
                    continue
            if raw is not None:
                home_spread = raw
        if total is None or home_spread is None:
            h_total, h_spread = _historical_close(detail.get("game_id"))
            total = total if total is not None else h_total
            home_spread = home_spread if home_spread is not None else h_spread
        return total, home_spread

    def _pace_view(self, detail):
        """
        Live fair total / spread / team totals from game state plus the pregame
        line. See pace.py for the model, the coefficients and their limits.

        Returns None before kickoff - there is no deviation to price yet.
        """
        replay = detail.get("replay")
        if detail.get("state") == "pre" and not replay:
            return None

        summary = detail.get("summary") or {}
        pre_total, pre_spread = self._pregame_anchor(detail)
        state = replay or {"elapsed_seconds": detail.get("elapsed_seconds"),
                           "home_score": detail.get("home_score"),
                           "away_score": detail.get("away_score")}

        best = detail.get("best_lines") or {}
        live_total = (best.get("totals") or {}).get("median_point")
        live_spread = (best.get("spreads") or {}).get("median_point")

        if replay:
            live_total = live_spread = None     # live quotes are not historical; don't mix
        view = pace.evaluate(
            elapsed_seconds=state["elapsed_seconds"],
            home_score=state["home_score"],
            away_score=state["away_score"],
            drives=summary.get("drives") or [],
            pregame_total=pre_total,
            pregame_spread_home=pre_spread,
            live_total=live_total,
            live_spread_home=live_spread,
        )
        view["replay"] = replay
        view["pregame_total"] = pre_total
        view["pregame_spread_home"] = pre_spread
        view["live_total"] = live_total
        view["live_spread_home"] = live_spread
        if pre_total is None:
            view["notes"].append("no pregame line from ESPN pickcenter - fair values unavailable")
        return view

    def _best_lines(self, game_id):
        """
        Consensus view of the current market: median point per market plus the
        best available price on each side.

        Deliberately simple. This is a hub, not a pricing engine - the sharp-
        anchored cross-line machinery from Odds Screen is a later layer.
        """
        return self._best_lines_from(db.get_latest_odds(game_id, books=REC_BOOKS))

    def _best_lines_from(self, rows):
        if not rows:
            return {}

        by_market = {}
        for r in rows:
            by_market.setdefault(r["market"], []).append(r)

        out = {}
        for market, quotes in by_market.items():
            sides = {}
            for q in quotes:
                side = q["side"]
                cur = sides.get(side)
                if cur is None or _quote_rank(market, side, q) > _quote_rank(market, side, cur):
                    sides[side] = {
                        "book": q["book"], "point": q["point"], "price": q["price"],
                        "updated": q["book_updated_at"], "is_live": q["is_live"],
                    }
            # Medians must be taken PER SIDE. A spread board quotes the home side
            # at +7.5 and the away side at -7.5; pooling both and taking the
            # median returns 0, which is not a line anyone is offering.
            median_by_side = {}
            for side, side_quotes in _group_by_side(quotes).items():
                points = sorted(q["point"] for q in side_quotes if q["point"] is not None)
                if not points:
                    continue
                mid = len(points) // 2
                median_by_side[side] = (
                    points[mid] if len(points) % 2
                    else (points[mid - 1] + points[mid]) / 2.0
                )

            # The single headline number, read the way a board is read: the home
            # side for spreads, the total for totals, nothing for a moneyline.
            if market == "spreads":
                median_point = median_by_side.get("home")
            elif market == "totals":
                median_point = median_by_side.get("over", median_by_side.get("under"))
            else:
                median_point = None

            out[market] = {
                "sides": sides,
                "median_point": median_point,
                "median_by_side": median_by_side,
                "book_count": len({q["book"] for q in quotes}),
            }
        return out

    def _market_home_wp(self, game_id):
        """
        De-vigged home win probability from the current moneyline consensus.

        Shown next to ESPN's own win probability as a rough cross-check: a large
        gap between the two is the kind of thing worth a look. It is NOT a model
        output and carries no edge claim on its own.
        """
        return self._market_home_wp_from(db.get_latest_odds(game_id, books=REC_BOOKS))

    def _market_home_wp_from(self, all_rows):
        rows = [r for r in all_rows if r["market"] == "h2h"]
        if not rows:
            return None
        by_book = {}
        for r in rows:
            by_book.setdefault(r["book"], {})[r["side"]] = r["price"]
        probs = []
        for book, sides in by_book.items():
            if "home" in sides and "away" in sides:
                ph, _ = _no_vig_pair(sides["home"], sides["away"])
                if ph is not None:
                    probs.append(ph)
        if not probs:
            return None
        probs.sort()
        mid = len(probs) // 2
        return probs[mid] if len(probs) % 2 else (probs[mid - 1] + probs[mid]) / 2.0

    def get_status(self):
        status = {
            "running": self.is_running(),
            "last_scoreboard_at": self.last_scoreboard_at,
            "last_odds_at": self.last_odds_at,
            "last_error": self.last_error,
            "unmatched_odds_count": len(self.unmatched_odds),
            "intervals": {
                "scoreboard": SCOREBOARD_INTERVAL,
                "odds": ODDS_INTERVAL,
                "summary_ttl": SUMMARY_TTL,
            },
            # Credit accounting, surfaced so spend is visible rather than inferred.
            "credits": {
                "per_odds_poll": len(CORE_MARKETS),
                "per_game_open": len(ALL_MARKETS),
                "market_groups": ENABLED_MARKET_GROUPS,
                "odds_poll_per_hour_live": round(3600 / ODDS_INTERVAL) * len(CORE_MARKETS),
                "odds_poll_per_hour_idle": round(3600 / IDLE_ODDS_INTERVAL) * len(CORE_MARKETS),
            },
            "books": REC_BOOKS,
            "db": db.get_db_summary(),
        }
        if self.odds_client:
            status["odds_quota"] = self.odds_client.get_quota_status()
        return status
