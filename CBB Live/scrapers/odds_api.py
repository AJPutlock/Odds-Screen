"""
The Odds API client for live NCAAF lines.

The plain /odds endpoint serves in-play prices for games already underway - no
separate live endpoint is needed. Verified against a live Saturday board: games
that kicked off 90 minutes earlier were still listed, with bookmaker
last_update timestamps seconds old.

Quota matters. One call covers every game on the board, and the cost scales with
the number of markets and regions requested, not the number of games:

    cost = len(markets) * len(regions)

So spreads+totals+h2h for us = 3 credits per refresh. At a 60s cadence that is
180/hour, which is why the manager polls odds on a slower loop than scores.
"""
import os
import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BASE = "https://api.the-odds-api.com/v4"
SPORT = "basketball_ncaab"
TIMEOUT = 25

DEFAULT_MARKETS = ("spreads", "totals", "h2h")
DEFAULT_REGIONS = ("us",)

# Recreational books only, mirroring Odds Screen's list minus the sharp
# references. This app bets rec books; it does not use a sharp anchor, so
# BetOnline/LowVig are deliberately absent. bet365, Bookmaker and Circa are
# HAR/OCR-scraped in Odds Screen and have no Odds API feed at all.
REC_BOOKS = [
    "novig",
    "draftkings",
    "fanduel",
    "williamhill_us",
    "hardrockbet",
    "fanatics",
    "espnbet",
    "betmgm",
    "betrivers",
    # Exchanges priced from their OWN feeds (scrapers/exchanges.py), never the
    # Odds API, whose quotes omit the trading fee. Listed here so they render
    # as books on the board and survive purge_books(). NoVig is already above:
    # it DOES come from the Odds API, but its live fee is stripped off in
    # game_manager.refresh_odds.
    "kalshi",
    "polymarket",
]

BOOKS_OF_INTEREST = set(REC_BOOKS)

# Display names, matching Odds Screen so the two apps read the same.
BOOK_DISPLAY = {
    "novig": "NoVig",
    "draftkings": "DraftKings",
    "fanduel": "FanDuel",
    "williamhill_us": "Caesars",
    "hardrockbet": "Hard Rock",
    "fanatics": "Fanatics",
    "espnbet": "theScore",
    "betmgm": "BetMGM",
    "betrivers": "BetRivers",
    "kalshi": "Kalshi",
    "polymarket": "Polymarket",
}

# ---------------------------------------------------------------------------
# Markets
# ---------------------------------------------------------------------------
# The bulk /odds endpoint only serves the three core markets. Everything else -
# alternate ladders, halves, quarters, team totals, player props - is available
# only from the per-event endpoint.
#
# This corrects an older project note that 1H/1Q "are not available for NCAAF
# (returns 422)". That is true of the BULK endpoint; per-event they return data
# for any game books are still pricing. Verified live against LSU @ Ole Miss:
# spreads_h1 7 books, spreads_q1 6 books, alternate_spreads 284 outcomes.
#
# Empty for a given game usually means the book pulled the market (late in a
# blowout, or a game nearly over), not that the market does not exist.

CORE_MARKETS = ["h2h", "spreads", "totals"]

# Every group this app knows how to fetch and render. Which ones are actually
# requested is controlled by ENABLED_MARKET_GROUPS below - a group left out
# costs nothing, since billing is per market requested.
AVAILABLE_MARKET_GROUPS = {
    "game": ("Game Lines", ["h2h", "spreads", "totals"]),
    "alternates": ("Alternate Lines", ["alternate_spreads", "alternate_totals"]),
    "team_totals": ("Team Totals", ["team_totals", "alternate_team_totals"]),
    "h1": ("1st Half", ["h2h_h1", "spreads_h1", "totals_h1"]),
    "q1": ("1st Quarter", ["h2h_q1", "spreads_q1", "totals_q1"]),
}

# Default: full game + 1st half, which is what actually gets bet here.
#
# Off by default, both still defined above so they can be switched back on
# without a code change:
#   team_totals  - not bet here; dropped on request
#   q1           - quarters are available from these books, but full game and
#                  1H are what get bet
#
# Re-enable either with e.g.
#   ODDS_MARKET_GROUPS="game,alternates,team_totals,h1,q1"
DEFAULT_GROUP_ORDER = ["game", "alternates", "h1"]

_env_groups = os.environ.get("ODDS_MARKET_GROUPS", "").strip()
ENABLED_MARKET_GROUPS = (
    [g.strip() for g in _env_groups.split(",") if g.strip() in AVAILABLE_MARKET_GROUPS]
    if _env_groups else list(DEFAULT_GROUP_ORDER)
)

MARKET_GROUPS = [AVAILABLE_MARKET_GROUPS[g] for g in ENABLED_MARKET_GROUPS]

# Player props are intentionally NOT fetched for CFB. They exist on the API
# (player_rush_yds, player_anytime_td and friends all return data) but are out
# of scope for this app, and each one would add a credit per game opened.

ALL_MARKETS = [m for _, markets in MARKET_GROUPS for m in markets]

MARKET_DISPLAY = {
    "h2h": "Moneyline",
    "spreads": "Spread",
    "totals": "Total",
    "alternate_spreads": "Alt Spread",
    "alternate_totals": "Alt Total",
    "team_totals": "Team Total",
    "alternate_team_totals": "Alt Team Total",
    "h2h_h1": "1H Moneyline",
    "spreads_h1": "1H Spread",
    "totals_h1": "1H Total",
    "h2h_q1": "Q1 Moneyline",
    "spreads_q1": "Q1 Spread",
    "totals_q1": "Q1 Total",
}

# Ladder markets quote many points per book, so they render as point-per-row
# rather than one row per book.
LADDER_MARKETS = {"alternate_spreads", "alternate_totals", "alternate_team_totals"}

# Markets whose outcomes carry a `description` naming the team the line belongs
# to, with `name` holding Over/Under.
TEAM_SCOPED_MARKETS = {"team_totals", "alternate_team_totals"}


class OddsAPIError(Exception):
    pass


class OddsAPIClient:
    def __init__(self, api_key, books=None):
        self.api_key = api_key
        self.books = set(books) if books else set(BOOKS_OF_INTEREST)
        self.quota_remaining = None
        self.quota_used = None
        self.last_error = None

    def _get(self, path, params):
        params = dict(params or {})
        params["apiKey"] = self.api_key
        try:
            r = requests.get(BASE + path, params=params, timeout=TIMEOUT)
        except requests.RequestException as e:
            self.last_error = str(e)
            raise OddsAPIError("request failed: {0}".format(e))

        # These headers come back on every response, including errors.
        self.quota_remaining = r.headers.get("x-requests-remaining")
        self.quota_used = r.headers.get("x-requests-used")

        if r.status_code != 200:
            self.last_error = "HTTP {0}: {1}".format(r.status_code, r.text[:300])
            raise OddsAPIError(self.last_error)

        self.last_error = None
        return r.json()

    def fetch_odds(self, markets=DEFAULT_MARKETS, regions=DEFAULT_REGIONS):
        """
        Every NCAAF game currently offered, pregame and in-play.

        Returns the raw event list; pass it to `to_snapshot_rows` to flatten.
        """
        data = self._get(
            "/sports/{0}/odds".format(SPORT),
            {
                "regions": ",".join(regions),
                "markets": ",".join(markets),
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
        )
        logger.info(
            "odds api: %d events, quota remaining %s",
            len(data), self.quota_remaining,
        )
        return data

    def fetch_event_odds(self, event_id, markets=None, regions=DEFAULT_REGIONS):
        """
        Every market for ONE game, from the per-event endpoint.

        This is the only way to get alternate ladders, halves, quarters, team
        totals and player props - the bulk endpoint serves core markets only.

        Cost is len(markets) * len(regions) credits, so this is called on an
        explicit user action (opening a game), never on a timer.
        """
        markets = list(markets or ALL_MARKETS)
        data = self._get(
            "/sports/{0}/events/{1}/odds".format(SPORT, event_id),
            {
                "regions": ",".join(regions),
                "markets": ",".join(markets),
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
        )
        logger.info(
            "event odds %s: %d markets requested, quota remaining %s",
            event_id, len(markets), self.quota_remaining,
        )
        return data

    def get_quota_status(self):
        return {
            "remaining": self.quota_remaining,
            "used": self.quota_used,
            "last_error": self.last_error,
        }


def _side_for_outcome(outcome, market_key, home_team, away_team):
    """
    Map an outcome name onto a stable side label.

    Totals use Over/Under; spreads and moneylines name the team, which we resolve
    against the event's own home/away names rather than guessing.
    """
    name = (outcome.get("name") or "").strip()
    if market_key == "totals":
        low = name.lower()
        if low.startswith("over"):
            return "over"
        if low.startswith("under"):
            return "under"
        return None
    if name == home_team:
        return "home"
    if name == away_team:
        return "away"
    return None


def to_snapshot_rows(odds_events, matches, game_states, books=None,
                     captured_at=None):
    """
    Flatten the API response into odds_snapshots rows.

    `matches`     maps odds_event_id -> espn game_id (from team_match).
    `game_states` maps espn game_id -> dict with period/clock_seconds/scores/state,
                  used to stamp each price with the game situation it was quoted
                  under. That stamp is what makes the table usable for backtests
                  later - without it a row is just a price with no context.
    """
    captured_at = captured_at or datetime.now(timezone.utc).isoformat()
    keep = set(books) if books else set(BOOKS_OF_INTEREST)
    rows = []

    for ev in odds_events:
        oid = ev.get("id")
        gid = matches.get(oid)
        state = (game_states or {}).get(gid) or {}
        is_live = 1 if state.get("state") == "in" else 0
        home_team, away_team = ev.get("home_team"), ev.get("away_team")

        for bk in ev.get("bookmakers") or []:
            book = bk.get("key")
            if keep and book not in keep:
                continue
            for mkt in bk.get("markets") or []:
                mkey = mkt.get("key")
                for oc in mkt.get("outcomes") or []:
                    side = _side_for_outcome(oc, mkey, home_team, away_team)
                    if side is None:
                        continue
                    rows.append({
                        "game_id": gid,
                        "odds_event_id": oid,
                        "captured_at": captured_at,
                        "book": book,
                        "market": mkey,
                        "side": side,
                        "point": oc.get("point"),
                        "price": oc.get("price"),
                        "book_updated_at": mkt.get("last_update") or bk.get("last_update"),
                        "is_live": is_live,
                        "period": state.get("period"),
                        "clock_seconds": state.get("clock_seconds"),
                        "home_score": state.get("home_score"),
                        "away_score": state.get("away_score"),
                    })
    return rows


def group_event_odds(event, books=None):
    """
    Shape a per-event response into tables the UI can render directly.

    Books are always the COLUMNS, matching how Odds Screen presents a board.
    What forms a ROW depends on the market:

      standard   one row per side          (Away / Home, or Over / Under)
      ladder     one row per side+point    (every rung of an alt ladder)
      team-total one row per team+side     (LSU Over, LSU Under, ...)

    Cells hold the point and price a given book quotes for that row. For
    standard markets the point lives in the cell, because books disagree on it;
    for ladders the point IS the row, so the cell is price only.
    """
    keep = set(books) if books else set(REC_BOOKS)
    home_team, away_team = event.get("home_team"), event.get("away_team")

    # market key -> {"rows": {row_key: row}, "books": set()}
    collected = {}

    for bk in event.get("bookmakers") or []:
        book = bk.get("key")
        if book not in keep:
            continue
        for mkt in bk.get("markets") or []:
            mkey = mkt.get("key")
            slot = collected.setdefault(mkey, {"rows": {}, "books": set()})
            for oc in mkt.get("outcomes") or []:
                name = (oc.get("name") or "").strip()
                desc = (oc.get("description") or "").strip()
                point = oc.get("point")
                price = oc.get("price")

                if mkey in TEAM_SCOPED_MARKETS:
                    label, sublabel = desc, name
                else:
                    label, sublabel = name, ""

                is_ladder = mkey in LADDER_MARKETS
                row_key = (label, sublabel, point if is_ladder else None)

                row = slot["rows"].get(row_key)
                if row is None:
                    row = {
                        "label": label,
                        "sublabel": sublabel,
                        "point": point if is_ladder else None,
                        "cells": {},
                        # Sorting hints so the UI does not have to re-derive them.
                        "is_home": label == home_team,
                        "is_away": label == away_team,
                    }
                    slot["rows"][row_key] = row

                row["cells"][book] = {
                    "point": None if is_ladder else point,
                    "price": price,
                }
                slot["books"].add(book)
                if mkt.get("last_update"):
                    slot.setdefault("last_update", mkt["last_update"])
                    if mkt["last_update"] > slot["last_update"]:
                        slot["last_update"] = mkt["last_update"]

    def sort_key(row):
        # Away before home; Over before Under; then by point.
        side_rank = 0 if row["is_away"] else (1 if row["is_home"] else 2)
        name_rank = {"Over": 0, "Under": 1, "Yes": 0, "No": 1}.get(
            row["sublabel"] or row["label"], side_rank
        )
        point = row["point"]
        return (row["label"], name_rank, point if point is not None else 0)

    groups = []
    for title, market_keys in MARKET_GROUPS:
        markets = []
        for mkey in market_keys:
            slot = collected.get(mkey)
            if not slot or not slot["rows"]:
                continue
            rows = sorted(slot["rows"].values(), key=sort_key)
            markets.append({
                "key": mkey,
                "label": MARKET_DISPLAY.get(mkey, mkey),
                "is_ladder": mkey in LADDER_MARKETS,
                "books": sorted(slot["books"], key=lambda b: REC_BOOKS.index(b)
                                if b in REC_BOOKS else 99),
                "rows": rows,
                "last_update": slot.get("last_update"),
            })
        if markets:
            groups.append({"title": title, "markets": markets})

    all_books = sorted(
        {b for g in groups for m in g["markets"] for b in m["books"]},
        key=lambda b: REC_BOOKS.index(b) if b in REC_BOOKS else 99,
    )
    return {
        "groups": groups,
        "books": all_books,
        "book_display": {b: BOOK_DISPLAY.get(b, b) for b in all_books},
        "market_count": sum(len(g["markets"]) for g in groups),
    }


def dedupe_against_previous(rows, previous_index):
    """
    Drop rows identical to the last stored price for the same key.

    The API returns the full board every call, but most lines have not moved
    between polls. `previous_index` maps (game_id, book, market, side) ->
    (point, price); rows matching it are discarded so the table records line
    MOVES rather than one row per poll per line.
    """
    if not previous_index:
        return rows
    out = []
    for r in rows:
        key = (r["game_id"], r["book"], r["market"], r["side"])
        prev = previous_index.get(key)
        if prev and prev == (r.get("point"), r.get("price")):
            continue
        out.append(r)
    return out


if __name__ == "__main__":
    import os
    import sys

    logging.basicConfig(level=logging.INFO)
    key = os.environ.get("ODDS_API_KEY")
    if not key:
        print("set ODDS_API_KEY to run this smoke test")
        sys.exit(1)

    client = OddsAPIClient(key)
    events = client.fetch_odds()
    print("quota:", client.get_quota_status())
    now = datetime.now(timezone.utc)
    live = [
        e for e in events
        if datetime.fromisoformat(e["commence_time"].replace("Z", "+00:00")) < now
    ]
    print("{0} events, {1} already underway".format(len(events), len(live)))
    if live:
        e = live[0]
        print("\n{0} @ {1}".format(e["away_team"], e["home_team"]))
        for b in (e.get("bookmakers") or [])[:4]:
            for m in b.get("markets") or []:
                outs = ", ".join(
                    "{0} {1} {2}".format(o.get("name"), o.get("point"), o.get("price"))
                    for o in m.get("outcomes") or []
                )
                print("  {0:<14} {1:<8} {2}".format(b["key"], m["key"], outs))
