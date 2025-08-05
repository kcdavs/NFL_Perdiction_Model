# NFL-Gambling-Addiction-ml
Use ml in conjunction with our existing ball knowledge to gain an edge on major sports gambling sites 

📦 Components
1. Webscraping
Responsible for collecting sportsbook line data (e.g., spreads, odds) each week and saving it in the repository. This serves as one of the core data sources for the model.

2. Ingestion & Transformation
This module pulls from multiple data sources—including the scraped odds and league-wide stats—and combines them into a single, clean dataset with one row per game.

The ingestion process is designed to be fully reproducible and can rebuild the entire dataset from scratch if needed.

To optimize efficiency, checks are in place to prevent duplication. On routine runs, only new weekly data will be ingested and appended to the existing dataset.

3. Modeling
The final dataset (one row per game) serves as input to a predictive model. This model analyzes trends, evaluates signals from sportsbook lines, and outputs insights or decisions that can inform weekly betting strategy or game evaluations.

