# Project Notes

## What This Project Is
An NFL spread and totals prediction model that combines machine learning with real-time sportsbook line data to find edges on NFL spreads and totals.

## What's Working Right Now

### web-api/
- Flask app deployed on Render
- Scrapes BookmakersReview for historical sportsbook line data (spreads, totals, moneylines, consensus %)
- Pulls from multiple books: DraftKings, FanDuel, Pinnacle, and others
- Two endpoints:
  - `/odds/<year>/<week>` — preview data in browser
  - `/combined/<year>/<week>` — scrape and save that week's data to GitHub
- Uses a GitHub token (stored in Render environment variables) to write CSVs to this repo

### data/odds/
- Historical odds CSVs organized by year and week: `data/odds/{year}/week_{n}.csv`
- Coverage: 2018–2025 seasons
- Each file contains per-team line data: opening lines, current lines by book, consensus betting %

## What Needs to Be Built

### Ingestion (starting from scratch)
- Read and combine all CSVs from `data/odds/`
- Join with game results (nflverse is a good source: `https://github.com/nflverse/nfldata`)
- Output a clean model-ready table: one row per game with odds + result columns

### Model (starting from scratch)
- Train an ML model on the ingestion output
- Goal: identify edges vs the closing line (beat the closer = positive expected value)
- Target variables: did the home team cover the spread? did the game go over/under?

## Repo Structure (in progress — not yet refactored)
Current structure is messy. Plan is to reorganize but still being decided. Key idea:
- `scrapers/` as parent folder with `lines_scraper/` as a subfolder (leaving room for future scrapers)
- `ingestion/` for the data pipeline
- `model/` for the ML model
- `data/` stays as-is
- `Ingestion/` and `Model/` folders at the root are getting deleted — they are old and broken

## Environment
- Mac Mini, new machine
- Git SSH auth is configured
- VS Code installed with `code` CLI command working
- GitHub: https://github.com/kcdavs/NFL_Perdiction_Model
