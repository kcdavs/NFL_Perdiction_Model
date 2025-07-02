import os, sys, traceback
from flask import Flask, Response

# Make scraper.py importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

# Import the requests-based single-week scraper
from scraper import scrape_one_week_requests

app = Flask(__name__)

@app.route("/")
def home():
    return "🟢 API live. Use /scrape/<egid>/<season> to fetch week data."

@app.route("/scrape/<int:egid>/<int:season>")
def scrape_week(egid, season):
    try:
        df = scrape_one_week_requests(egid, season)
        return Response(f"<pre>{df.to_string(index=False)}</pre>",
                        mimetype="text/html")
    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Scraper error:\n{e}\n\n{tb}",
                        status=500, mimetype="text/plain")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
