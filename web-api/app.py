import os
import sys
import traceback
from flask import Flask, Response

# 1) Make sure we can import scraper.py from the repo root
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

# 2) Import your existing scraper entry-point
from scraper import scrape_all_seasons

app = Flask(__name__)

@app.route("/")
def home():
    return "🟢 API is live. Use /run-scraper to kick off the scraper."

@app.route("/run-scraper")
def run_scraper():
    try:
        # 3) Define the seasons you want to scrape
        seasons = [
            (4494, 2018)
        ]
        # 4) Run your full scraper (blocks until done)
        df = scrape_all_seasons(seasons)
        return Response(
            f"✅ Scrape complete: fetched {len(df)} rows of data.",
            mimetype="text/plain"
        )
    except Exception as e:
        # 5) On error, return the exception and full traceback
        tb = traceback.format_exc()
        return Response(
            f"🛑 Scraper error:\n{e}\n\nTraceback:\n{tb}",
            status=500,
            mimetype="text/plain"
        )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
