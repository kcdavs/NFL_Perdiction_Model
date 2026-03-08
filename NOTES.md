# Project Notes

## What This Project Is
An NFL spread and totals prediction model that combines machine learning with real-time sportsbook line data to find edges on NFL spreads and totals.

## What's Working Right Now

### scrapers/lines_scraper/scrape.py
- Clean standalone scraper (rewrite of old web-api/app.py)
- Scrapes BookmakersReview for sportsbook line data (spreads, totals, moneylines, consensus %)
- Pulls from multiple books: DraftKings, FanDuel, Pinnacle, and others
- Saves to `data/odds/{year}/week_{week}.csv`
- Run locally: `python3 scrapers/lines_scraper/scrape.py 2024 1`
- Preview without saving: `python3 scrapers/lines_scraper/scrape.py 2024 1 --preview`

### .github/workflows/
- `scrape-odds.yml` — manually triggered from GitHub UI, inputs: year + week. Scrapes and commits one week of data.
- `rebuild-historical.yml` — re-scrapes all years/weeks, skips files that already exist. Used to normalize schema across all historical data.

### data/odds/
- Historical odds CSVs: `data/odds/{year}/week_{week}.csv`
- Coverage: 2018–2025 (all weeks, new consistent schema)
- Schema: one row per team per game. Key columns: game_id, team_id, team, date, time, score, outcome, rotation, opening_moneyline, opening_spread, opening_spread_odds, opening_total, opening_total_odds, consensus bet %, per-sportsbook current lines (8_moneyline, 8_spread, etc.)
- NOTE: old data (pre-rebuild) used a different schema (op_ml, op_spr, etc.) — all files have been rebuilt with the new schema

### index.html (GitHub Pages)
- Live at: https://kcdavs.github.io/NFL_Perdiction_Model/
- Select year + week → fetches CSV from repo and renders full raw data table in browser
- Playoff week labels are year-aware (2018-2020 had 17 regular season weeks, 2021+ have 18)
- This will evolve into the full strategy/recommendation UI over time

### web-api/ (OLD — do not use)
- Old Flask app that ran on Render, now replaced by the standalone scraper + GitHub Actions
- Kept for reference only

## What to Build Next

### 1. Combine all CSVs into one table
- Read all `data/odds/**/*.csv` and concatenate
- All files now have the same schema so this should be straightforward
- One row per team per game across all seasons

### 2. Simple first model
- Target: did the team cover the spread? (compute from score + opening_spread)
- Features to start: opening_spread, opening_moneyline, consensus bet %, maybe line movement
- Start with logistic regression or random forest — just get something working end to end
- Beating 52.4% accuracy = break-even at -110 odds, that's the real bar

### 3. Add complexity later
- Additional data sources (nflverse for game stats, weather, injuries, etc.)
- Feature engineering
- Model tuning

## Repo Structure
- `scrapers/lines_scraper/` — odds scraper
- `data/odds/` — historical CSVs
- `index.html` — GitHub Pages web viewer
- `.github/workflows/` — GitHub Actions for scraping
- `web-api/` — old Flask app, ignore
- `Ingestion/` and `Model/` — old broken folders, can be deleted eventually

## Environment
- Mac Mini, new machine
- Python 3 via system python3, packages installed via pip3
- Git SSH auth configured
- VS Code with Python extension
- GitHub: https://github.com/kcdavs/NFL_Perdiction_Model
- GitHub Pages: https://kcdavs.github.io/NFL_Perdiction_Model/