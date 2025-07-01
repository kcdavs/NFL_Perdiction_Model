# web-api/app.py

import os
import sys
import traceback
from flask import Flask, Response

# 1) Add the repo root to Python’s import path so we can import scraper.py
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

# 2) Now import your scraper entry-point
from scraper import scrape_all_seasons

app = Flask(__name__)

@app.route("/run-scraper")
def run_scraper():
    try:
        # 3) Define the same seasons list you already have in scraper.py
        seasons = [
            (4494, 2018),
            (5703, 2019),
            (8582, 2020),
            (29178, 2021),
            (38109, 2022),
            (38292, 2023),
            (42499, 2024),
        ]
        # 4) Kick off the scrape. This blocks until it's done.
        df = scrape_all_seasons(seasons)
        return Response("✅ Scrape complete: fetched "
                        f"{len(df)} rows of data.",
                        mimetype="text/plain")
    except Exception:
        # 5) On any error, log it and return a 500
        print(traceback.format_exc())
        return Response("scraper had an error", status=500,
                        mimetype="text/plain")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
