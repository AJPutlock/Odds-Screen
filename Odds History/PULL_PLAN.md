# Historical odds pull — Month 1 plan (The Odds API, 5M-credit plan)

Status: **PLAN — awaiting the user's go.** Nothing runs until the 5M plan is active and the user
approves this document. Month 2 (2021-22 → 2022-23 for every sport, plus MLB) follows the same
machinery once month 1 is complete.

## 1. What we are buying and why

Live (in-play) odds history, pre-game opens and closes, and derivative-market lines at fixed game
moments, so every sport can get the same treatment NCAAB got: align each quoted line with the game
state at that moment and compare it with our own fair-value models. The Odds API terms allow
storing the data indefinitely and training models on it; only redistributing or reselling the raw
data is prohibited. (SportsGameOdds was ruled out: no in-play history, and its terms require
deleting all data within 30 days of cancelling.)

Verified on 10/14 with ~5 days of pilot pulls (~25K credits):
- Full-board historical snapshots every 5 min (10 min before Sept 2022) include in-play lines
  (DK on 36/36 live NCAAB games, FD 34/36 on 2/15/25).
- **A named list of up to 10 books costs the same as one region (30 credits per snapshot for 3
  markets).** So every snapshot carries DraftKings, FanDuel, BetMGM, Caesars, BetRivers, Fanatics,
  Bovada, **BetOnline, LowVig, Pinnacle** at no extra cost. Pinnacle and BetOnline are mostly
  pre-game: Pinnacle quoted 104/129 pre-game NCAAB events but only 4/36 live ones.
- Derivative markets (1H/2H/1Q/F5/P1) exist historically, in play too, but only on the per-event
  endpoint (10 credits per market per event per call). Live 1H lines at 8 min into 2/15/25
  Arkansas @ Texas A&M came back from DK/FD/Caesars/Bovada/MGM.

## 2. What gets pulled

Books on every call: `draftkings, fanduel, betmgm, williamhill_us, betrivers, fanatics, bovada,
betonlineag, lowvig, pinnacle`.

| Block | What | How |
|---|---|---|
| **A. Live boards** | Full-game ML/spread/total, every book, every 5 min while any game is live | Bulk historical endpoint on a 5-min grid from 20 min before the first tip to the last game's end + 5 min, per day |
| **B. Openers** | First time each book quotes each game | Pre-game board every 3 h from 48 h before each game day. Opener = earliest snapshot a book appears in (±3 h) |
| **C. Closes** | Two closes per game: **close_t5** = last snapshot at or before tip − 5 min (lands 5–10 min before tip); **close_t0** = last snapshot before tip (0–5 min before) | One bulk request per distinct scheduled tip time (−5 and −0); most are already in Block A's grid |
| **D. Derivative closes** | 1H (and 1Q / F5 / P1 by sport) closes at tip − 5 | Per-event call per game |
| **E. Derivative moments** | Live period lines at fixed moments, in games where DK quoted live | Per-event call at each moment, located by ESPN play wall-clock times |

**Moments (Block E):**

| Sport | Moments | Markets |
|---|---|---|
| NCAAB | tip−5 (1H close) · 12:00 left in 1H (8 min played) · halftime (≈3 min after the last 1H play) · 12:00 left in 2H | h2h/spreads/totals for `_h1` (first two), `_h2` (last two) |
| NFL / CFB | tip−5 (1H + 1Q close) · end of Q1 · halftime | `_q1`, `_h1`, `_h2` |
| NBA | tip−5 · end of Q1 · halftime | `_q1`, `_h1`, `_h2` |
| NHL | puck drop−5 · 1st intermission | `_p1`, then live full-game |

Market keys are confirmed with one pilot call per sport before each block runs.

## 3. Scope and budget (month 1)

Most recent seasons first, so the most relevant data lands first if anything stops early.

| Order | Block(s) | Seasons | Est. credits |
|---|---|---|---|
| 0 | Pilot day per sport (all blocks, verify parse + matching) | 1 day each | ~25K |
| 1 | NCAAB A+B+C | 2025-26, 2024-25, 2023-24 | ~1.79M |
| 2 | NCAAB D+E | 2025-26, 2024-25 | ~0.90M |
| 3 | NFL A–E | 2025, 2024, 2023 | ~0.45M |
| 4 | CFB A–E | 2025, 2024, 2023 | ~0.35M |
| 5 | NBA A–E | 2025-26, 2024-25 | ~0.85M |
| 6 | NHL A–E | 2025-26 (+ 2024-25 if credits allow) | ~0.40–0.80M |
| — | **Reserve for Odds Screen's daily use** | | **≥ 200K at all times** |
| | **Total** | | **~4.8M** |

These are estimates. Before each block runs, `plan` prints its exact call and credit count from
the real schedule. If the true total exceeds what's left above the reserve, NHL 2024-25 is the
first thing dropped (it moves to month 2).

Month 2: 2021-22 + 2022-23 for NCAAB, NFL, CFB, NBA, NHL; MLB (2023–2025); more seasons of
derivative moments.

## 4. Storage

Raw responses are what we pay for, so they are saved **before** parsing and backed up. Everything
parsed can be rebuilt from raw for free.

```
C:\Users\ajput\OddsHistoryData\          (local, OUTSIDE OneDrive — no sync churn while writing)
  raw\<sport>_<season>.db                 every API response as a gzip blob, keyed by request
  odds_history.db                         parsed, all sports (schema below)
  logs\                                   per-block run logs
OneDrive\...\Odds History\raw_backup\     copy of each COMPLETED raw DB (cloud backup; gitignored)
OneDrive\...\Odds History\                code (in git): pull.py, sports.py, parse.py, validate.py
```

Measured on the pilot: ~23 KB per snapshot gzipped, ~360 parsed rows per snapshot → month 1 ≈
**4 GB raw + 3–5 GB parsed** (80 GB free on C:).

Parsed schema (`odds_history.db`), one database for all sports:
- `requests` — every call: sport, season, block, requested_at, snapshot_ts, credits_used, credits_left, http_status
- `events` — Odds API event id, sport, season, commence_time, home, away
- `event_map` — event id → ESPN game id (+ home/away flip), match score and method
- `quote_runs` — one row per run of an unchanged price per (event, book, market):
  first_seen, last_seen, book last_update, home/over point+price, away/under point+price.
  A market missing from a snapshot ends the run, so suspended stretches show as gaps. Full-game
  AND derivative markets (market key carries `_h1`, `_q1`, etc.)
- `moment_quotes` — Block E rows keyed by (event, moment, book, market) with the game state
  (period, clock, score) at the moment
- `opens_closes` — derived view per (event, book, market): open, close_t5, close_t0
- `ledger` — per (sport, season, block): planned calls, done calls, credits spent, status — what
  makes every block resumable

## 5. How it runs

1. **Pilot (order 0):** one big day per sport, every block. A validation report checks: snapshots
   returned, % events matched to ESPN games (≥ 97% target), live coverage by book, Pinnacle /
   BetOnline open + close present, moment calls landing at the right game time. **I report the
   pilot results to the user before the bulk run.**
2. **Bulk run**, in the order above, unattended in the background: ~1–2 requests/s, ~170K calls ≈
   1–2 days of wall time. Resumable at any point (the ledger skips what's done); safe to stop and
   restart.
3. **Safeguards:** hard stop if credits remaining < 200K (the Odds Screen shares the key); stop on
   any non-200 response and log it; per-block credit cap = 110% of its `plan` estimate; raw saved
   before parse; a monitor checks progress and restarts a crashed run.
4. **After each block:** validation report (coverage by day, gaps, match rate, book coverage),
   raw DB copied to the OneDrive backup, ledger marked done, and a one-paragraph update to the user.

## 6. Game schedules and game state per sport

Schedules and wall-clock play times come from ESPN's public API (free), which also provides the
ESPN game id each odds event is matched to:

| Sport | Schedule + ESPN ids | Play wall-clock for moments |
|---|---|---|
| NCAAB | `espn_historical.db` (have) | `live_backtest.db` play_wallclock (have; Feb–Mar 2025 +5 h fix applied) |
| NFL / CFB / NBA / NHL | ESPN scoreboard by date (pulled during the pilot) | ESPN summary `plays[].wallclock` (checked per sport in the pilot; the same +5 h bug is tested for) |

The models that use this data for the other sports (their own game-state databases) are separate,
later work. The odds are the part that can only be bought while subscribed.

## 7. Decisions for the user before the pull starts

1. Approve this plan (or change the order or scope).
2. Confirm the 200K reserve for the Odds Screen is enough for a 1–2 day run.
3. Confirm the backup location (OneDrive `Odds History/raw_backup/`, about 4 GB) is acceptable.
