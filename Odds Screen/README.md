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
