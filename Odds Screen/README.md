# OddsScreen · NCAAB

Local web-based betting odds dashboard powered by The Odds API.

## Quick Start

```bash
pip install -r requirements.txt
python app.py
# → open http://localhost:5000
```

## Layout

| Column | Description |
|---|---|
| **Game / Market** | Teams, tip-off time, market chip (Moneyline / Spread / Total) |
| **Best Available** | Best odds across all listed books for each side, with book name |
| **No-Vig Line** | Fair line placeholder — will be populated once Circa odds feed is added |
| **Per-book columns** | Each book's odds; **N/A** if the book has no data; best odds highlighted green |

## Controls

- **RELOAD** — manual fetch (costs 1 API call)
- **AUTO** toggle — 30-second auto-refresh (disabled by default to conserve free-tier quota)
- Quota counter in header shows remaining API calls

## Sportsbook API Keys

| Display | API key | Notes |
|---|---|---|
| NoVig | `novig` | May not be in free tier |
| DraftKings | `draftkings` | Full coverage |
| FanDuel | `fanduel` | Full coverage |
| Caesars | `caesars` | Full coverage |
| bet365 | `bet365` | Full coverage |
| Hard Rock | `hardrockbet` | Varies by state |
| Fanatics | `fanatics` | Varies |
| theScore | `thescore` | May be limited |
| BetMGM | `betmgm` | Full coverage |
| BetRivers | `betrivers` | Not OH-specific — all states |

If a book returns N/A consistently it means The Odds API doesn't carry it —
a custom scraper would need to be built for those books.

## Adding Circa / No-Vig

When you're ready to wire in Circa odds for the no-vig fair line:

1. Add `"circa"` to `BOOKMAKERS` in `app.py` (or handle Circa separately)
2. In `process_games()`, un-comment and populate `novig_home` / `novig_away`
   using the `novig_fair_line()` helper already in the file

## Changing Sport

Edit `SPORT_KEY` in `app.py`:

| Sport | Key |
|---|---|
| NCAAB (Men's) | `basketball_ncaab` |
| NBA | `basketball_nba` |
| NFL | `americanfootball_nfl` |
| MLB | `baseball_mlb` |
| NHL | `icehockey_nhl` |

## NHL Saves Checker

Flags goalie saves props that don't line up with the opponent's shots-on-goal
line and team goal total. It rests on one identity:

```
opponent shots on goal = goalie saves + opponent goals + shots after the goalie is pulled
```

Empty-net goals are both shots and goals, so using the opponent's **team goal
total** (which includes empty-netters) cancels them out exactly. No "+1" fudge
is needed. Every line + price is first converted to an implied mean, so a 2.5
goals line at −160 counts differently from 2.5 at +110.

### Inputs

| Input | Source |
|---|---|
| Goalie saves lines | The Odds API `player_total_saves` (automatic) |
| Team goal totals | The Odds API `team_totals` (automatic) |
| Goalie → team | NHL public API rosters, cached daily (automatic) |
| **Team shots-on-goal lines** | **You enter them** in `data/nhl_saves/team_sog_lines.csv`, since The Odds API doesn't carry them |
| Goalie overrides (optional) | `data/nhl_saves/goalie_teams.csv` for call-ups/trades |

Copy the `.example.csv` files to get started. In the SOG file, `team` is the
team **taking** the shots, `date` is the Eastern-time game date, and blank
odds mean −110.

### Running it

```bash
python -m nhl_props scan          # check games in the next 36h, log flags + snapshots
python -m nhl_props scan --all    # also show unflagged offers and what's missing
python -m nhl_props watch-close   # leave running on game days: snapshots closing lines
python -m nhl_props clv           # closing line value of everything flagged
```

Or run `python app.py` and open http://localhost:5000/nhl-saves.

Each scan costs about 2 Odds API credits per game. `watch-close` only fetches
games that are about to start, about 2 more credits per game.

### Reading the output

| Column | Meaning |
|---|---|
| Mkt | Saves mean implied by the saves line + price |
| Implied | Opponent SOG mean − opponent goals mean − pulled-goalie adjustment |
| Edge | Implied − Mkt (negative leans under) |
| Gap | SOG line − saves line − goals line. This is your original line-only rule |
| EV | Expected value at that book's price, using a blend of Mkt and Implied |

Settings are in `nhl_props/config.py`: `MODEL_WEIGHT` (how far to trust
Implied over Mkt), `PULLED_SHOTS_ADJ`, `MIN_EV`, `MIN_SAVES_EDGE` and the
dispersion constants. These are rough starting values. Tune them once `clv`
has a few hundred plays.

### Tests

```bash
pip install pytest
python -m pytest tests
```
