# CFB Live — College Football Live Hub

Live scores, stats and odds for FBS college football, in one board.

Sibling to `Odds Screen` (pregame cross-book comparison) and `NCAAB Live`
(basketball live app). Reads historical context from `CFB Data/cfb_betting.db`.

```bash
python app.py          # http://127.0.0.1:5055
```

**Use `127.0.0.1`, not `localhost`.** On Windows `localhost` resolves to `::1`
first; this server binds IPv4 only, so every request waits ~2 seconds for the
IPv6 attempt to time out before falling back. Measured on this machine:

| Host | /api/scoreboard |
|------|-----------------|
| `localhost:5055` | **2.083s** |
| `127.0.0.1:5055` | **0.057s** |

The desktop shortcut already opens the right one.

Ports in this repo: Odds Screen 5000, NCAAB Live 5000, **CFB Live 5055**.

---

## What it does

Two background loops write into SQLite; the web UI reads from SQLite. Nothing
in the UI blocks on a network call.

| Loop | Source | Cadence | Cost |
|------|--------|---------|------|
| Scoreboard | ESPN public API | 20s live / 120s idle | free, 1 request for the whole board |
| Odds (core) | The Odds API | **3 min** live / 10 min idle | 3 credits per poll |
| Summary | ESPN public API | on game open, 45s TTL | free, 1 request per game viewed |
| Full odds board | The Odds API | on game open, 120s TTL | up to 8 credits per game opened |

The scoreboard loop is the heartbeat because one request covers every game. The
odds loop is deliberately slower — quota should be spent on line *movement*, not
on re-reading a board that mostly hasn't changed.

**The screen auto-refreshes every 30s** while the tab is visible (paused when
hidden, and pulled immediately on return). This costs **no quota**:
`/api/scoreboard` and `/api/game/<id>` read the local database, which the
background loops keep current — they do not call ESPN or The Odds API.

The one thing that does NOT auto-refresh is the full odds board on a game page,
because that costs a credit per market. It reloads only on Refresh or
"Reload odds".

The header's age indicator reports when **this page** last fetched, not when the
server last polled. Those are different numbers, and showing the server's was a
real bug — the header would read "just now" while the cards on screen were
minutes old.

---

## Data sources

### ESPN (scores + stats) — free, no key

`site.api.espn.com/apis/site/v2/sports/football/college-football`

- `/scoreboard` — every game: score, clock, period, down, distance, yard line,
  possession, timeouts, red-zone flag, quarter linescores, ESPN's win probability.
- `/summary?event=<id>` — box score, drive-by-drive, scoring plays,
  win-probability series, pregame lines (`pickcenter`).

**Do not add a browser User-Agent.** This API returns **403** for Chrome-like UA
strings and **200** for the default `python-requests` one. Verified by isolating
the header against the live endpoint. The usual "spoof a browser to avoid being
blocked" instinct breaks this scraper rather than helping it.

#### ESPN drops play-by-play, and there is no way around it

ESPN intermittently stops serving the `situation` block (down, distance, yard
line, possession) **and** the `drives` block for live games. Observed going from
~22 of 25 live games having full data to **4 of 30** over about 50 minutes on one
Saturday evening, with individual games losing it mid-game.

`playByPlayAvailable` on the competition is an honest flag for this — it
correlates perfectly with whether the data is present (checked across a full live
board: 4 true/4 with data, 26 false/26 without). The app stores it as
`pbp_available` and the UI says "ESPN play-by-play offline for this game" rather
than pretending there's no drive.

**It is upstream, not an API-surface problem.** All three ESPN paths show the
identical gap for the same games, because they share one source:

| Path | Result |
|------|--------|
| `site.api.espn.com/.../scoreboard` | `situation: {}` |
| `sports.core.api.espn.com/.../situation` | bare `$ref`, no fields |
| `cdn.espn.com/core/college-football/scoreboard` (ESPN's own site feed) | `pbp False`, no situation |
| `site.api.espn.com/.../summary` | `drives` key absent entirely |

Mitigations in place: the last known field position is **carried forward** rather
than nulled (`_SITUATION_COLS` in `database.py`), so a gap freezes the marker
instead of blanking it, and `game_states` carries position forward too rather
than recording holes.

If this turns out to be chronic rather than a bad evening, the genuinely
independent options are CFBD's Patreon tier (`/live/plays` — same game IDs, so it
would drop straight in) or stats.ncaa.org (richest fields, but Akamai-blocked
historically — see the NCAAB project's IP-block incident). NCAA's public
`data.ncaa.com/casablanca` paths all 404'd when tried.

There's no Akamai bot wall here, so the elaborate event-triggered priority
tiering that `NCAAB Live` needed for stats.ncaa.org isn't required. The binding
constraint in this app is Odds API quota, not scrape throughput.

### The Odds API (odds) — paid, key already in use

The plain `/odds` endpoint serves **in-play** prices; there is no separate live
endpoint. Verified against a live Saturday board — games that kicked off 90
minutes earlier were still quoted, with `last_update` timestamps seconds old.

#### Billing model (measured, not assumed)

**cost = (markets that RETURN DATA) × regions.** Markets you request that come
back empty are free. Verified against the live key using `/v4/sports` as a
zero-cost meter:

| Call | Requested | Returned data | Charged |
|------|-----------|---------------|---------|
| bulk, 1 market, 1 region | 1 | 1 | **1** (43 games) |
| bulk, 3 markets, 1 region | 3 | 3 | **3** (43 games) |
| bulk, 2 markets, 2 regions | 2 | 2 | **4** (43 games) |
| per-event, 10 markets, pregame | 10 | 10 | **10** (1 game) |
| per-event, 10 markets, late-game | 10 | 7 | **7** (1 game) |
| per-event, market nobody offers | 1 | 0 | **0** |

Two consequences worth internalising:

1. **Game count is free; markets are what cost.** One bulk call priced 43 games
   for 3 credits. A single per-event call priced 1 game for 10. The per-event
   endpoint is ~140× more expensive per game — which is exactly why it is
   user-triggered rather than polled.
2. **Requesting a market that isn't offered costs nothing.** Enabling the `q1`
   group doesn't cost 3 credits per game; it costs 3 only on games where books
   are actually pricing quarters, and 0 everywhere else.

`/v4/sports` returns the quota headers and is itself free — use it as a meter.

**Books: recreational only.** Mirrors Odds Screen's list minus the sharp
references, since this app has no sharp-anchor step:

`novig`, `draftkings`, `fanduel`, `williamhill_us` (Caesars), `hardrockbet`,
`fanatics`, `espnbet` (theScore), `betmgm`, `betrivers`

BetOnline and LowVig are deliberately excluded. bet365, Bookmaker and Circa are
HAR/OCR-scraped in Odds Screen and have no Odds API feed at all.

**Two endpoints, two purposes.** The bulk `/odds` endpoint serves only the three
core markets — that's what the background loop polls for the scoreboard strip.
Everything else (alternate ladders, halves, quarters, team totals, player props)
is available *only* from the per-event endpoint, which is what a game page hits.

This **corrects an older project note** that 1H/1Q "are not available for NCAAF
(returns 422)". That's true of the bulk endpoint; per-event they return data for
any game books are still pricing. Verified live against LSU @ Ole Miss:
`spreads_h1` 7 books, `spreads_q1` 6 books, `alternate_spreads` 284 outcomes.
An empty market usually means the book pulled it (late in a blowout, or a game
nearly over), not that it doesn't exist.

**Market groups** — set with `ODDS_MARKET_GROUPS`, default `game,alternates,h1`:

| Group | Markets | Credits | Default |
|-------|---------|---------|---------|
| `game` | h2h, spreads, totals | 3 | on |
| `alternates` | alternate_spreads, alternate_totals | 2 | on |
| `team_totals` | team_totals, alternate_team_totals | 2 | **off** |
| `h1` | h2h_h1, spreads_h1, totals_h1 | 3 | on |
| `q1` | h2h_q1, spreads_q1, totals_q1 | 3 | **off** |

Quarters are available from these books but off by default — full game and 1H
are what get bet here, so quarters would cost 3 credits per game opened for
markets nobody prices a bet on. Player props are not fetched at all.

### CFBD — historical only

CFBD's `/scoreboard` and live-plays endpoints now require a Patreon Tier 1+
subscription (401 without one). Not needed: ESPN covers live. CFBD remains the
historical backfill in `CFB Data/`.

---

## The ID that makes this work

**CFBD game IDs are ESPN event IDs.** Verified directly against three 2025 games.

So a live game on this board joins to the 6,319-game / 1.1M-play historical
database on a primary key — no fuzzy team-name matching between live state and
history. `scrapers/espn.py` also computes `elapsed_seconds` on the same
convention as `CFB Data`'s `plays` table, so a live game state can be compared
to historical states directly.

---

## Odds ↔ ESPN matching

`team_match.py`. Odds API and ESPN agree on ~92% of team names exactly; the rest
differ in three ways:

| Kind | Example | Handled by |
|------|---------|-----------|
| accents / punctuation | `San José State` vs `San Jose State` | folding |
| dropped qualifier | `Nicholls Colonels` vs `Nicholls State Colonels` | token containment |
| real abbreviation | `App State` vs `Appalachian State` | `ALIASES` table |

**Matching is event-level, never team-by-team.** A matcher confident on each side
independently can pair two entirely different games — Odds Screen hit exactly
this, matching Grambling State/TCU onto Ohio State/Texas through the shared token
"state". Here a pairing must satisfy both sides at once, clear a combined floor,
agree on kickoff time, and claim each game at most once.

Measured on a live 44-event board: 41 matched, and the 3 misses were next
Saturday's games that weren't on the ESPN board at all. `/api/debug/unmatched`
shows the current misses.

`"State"` is deliberately **not** a stopword — dropping it is what collapses Ohio
and Ohio State together.

---

## Schema

`cfb_live.db`

- **games** — current state, one row per game, upserted in place.
- **game_states** — append-only time series, written only when the game state
  actually changes (otherwise a 4-hour game at 20s polling writes ~700 identical rows).
- **odds_snapshots** — append-only, one row per book/market/side, written only
  when a price *moves*. Each row is stamped with the score, period and clock it
  was quoted under, so it's usable for a backtest without a join.

That stamping is the point of the app. A live line is easy to read right now and
impossible to reconstruct later.

---

## API

| Endpoint | Purpose |
|----------|---------|
| `GET /api/scoreboard` | whole board + best lines + win probabilities |
| `GET /api/game/<id>` | one game: odds, history, box score, drives |
| `GET /api/game/<id>/all_odds` | every market, book-per-column (`?force=1` bypasses cache) |
| `GET /api/game/<id>/summary` | ESPN detail only |
| `GET /api/game/<id>/odds_history` | line movement, filterable by market/book |
| `POST /api/refresh` | manual poll, `{"what": "all"\|"scoreboard"\|"odds"}` |
| `GET /api/status` | loop health, quota, row counts |
| `GET /api/debug/unmatched` | odds events with no ESPN game |
| `GET /api/debug/scoreboard_raw` | straight from ESPN, bypasses the DB |

---

## Config

`.env` or environment:

```
ODDS_API_KEY=...            # falls back to the Odds Screen key
ODDS_MARKET_GROUPS=game,alternates,h1
SCOREBOARD_INTERVAL=20
ODDS_INTERVAL=180           # 3 min; biggest lever on credit spend
IDLE_SCOREBOARD_INTERVAL=120
IDLE_ODDS_INTERVAL=600
SUMMARY_TTL=45
ALL_ODDS_TTL=120
PORT=5055
```

### Credit budget

`GET /api/status` reports this live. At the defaults:

| Activity | Credits |
|----------|---------|
| Background odds poll (3 markets, whole board) | 3 |
| Per hour, games live (3 min) | 60 |
| Per hour, nothing live (600s) | 18 |
| Opening one game | up to 8, less when markets are empty |

"Opening one game" is a ceiling, not a flat rate — see the billing model above.
A pregame with everything posted charges the full 8; a game late in the 4th
where books have pulled 1H charges less.

A full Saturday with ~10 hours of live football runs roughly **600 credits**
from the background loop, plus ≤8 per game you open. `ODDS_INTERVAL` is the knob
that matters: at the old 60s it was ~1,800. The tradeoff is line-movement
granularity — a move that happens and reverses inside one 3-minute window is
never seen.

**Manual Refresh forces both sides at once** — a fresh ESPN pull (free) and a
fresh bulk odds poll (3 credits), ahead of whatever the loops had scheduled.

`use_reloader=False` is deliberate — Flask's reloader would start a second copy
of both background loops and double the odds spend. **Python changes need a
server restart.**

---

## What this is not

This is the plumbing layer. `_market_home_wp` (de-vigged moneyline consensus,
shown beside ESPN's win probability) is a rough cross-check, **not** a model, and
carries no edge claim.

Not built yet:

- The Kalman-filter / Brownian win-probability model designed for NCAAB Live,
  ported to football and fed by the historical join.
- Sharp-book anchoring. Out of scope by decision, not oversight — this app bets
  rec books and doesn't use a sharp fair-value reference.
- Player props. Available on the API, deliberately not fetched.
- Any bet-placement or alerting.
