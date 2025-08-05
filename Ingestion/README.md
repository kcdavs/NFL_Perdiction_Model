🧩 Ingestion Overview
The ingestion/ folder will contain all logic for collecting, transforming, and aggregating raw football data from multiple sources into a single unified table — one row per NFL game — that can be used for modeling and analysis.

🔄 What It Will Do
This layer acts as the data processing pipeline. Its job is to:

Pull raw data from multiple sources, including:

NFL game metadata and statistics from the nflverse GitHub repository or the nfl_data_py Python package

Sportsbook odds collected by our custom web scraper (stored in odds/{year}/week{week}.csv)

Transform and clean the data to standardize formats, resolve team naming issues, etc.

Join everything into a single, wide-format dataset, where:

Each row represents one game

Columns contain game stats, team info, final scores, betting lines, and public betting data (e.g., opening spread, odds, consensus %)

🧠 Design Goals
The ingestion code should be fully reproducible: running it from scratch should rebuild the complete dataset from raw sources.

It should also support incremental updates:

A check will be in place to detect whether the latest week of data has already been ingested.

If not, the pipeline will fetch just the new data and append it to the existing master table.

📁 Output
The final output will be a cleaned, analysis-ready dataset stored as a CSV (or Parquet) — one row per game — and serve as the single source of truth for the model.
