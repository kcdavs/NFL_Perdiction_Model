import os, sys, traceback
import requests
from flask import Flask, Response

# 1) Make scraper.py importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

# 2) Import your existing single‐week scraper and week‐label helper
from scraper import scrape_one_week_requests, get_week_label

app = Flask(__name__)

@app.route("/")
def home():
    return "🟢 API live.  Use /scrape/<egid>/<season> or /raw-html/<egid>/<season>."

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

@app.route("/raw-html/<int:egid>/<int:season>")
def raw_html(egid, season):
    try:
        # reuse your week‐label logic if you need it
        week = get_week_label(egid, season)
        url  = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={season}"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/115.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        return Response(resp.text, mimetype="text/html")
    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error fetching HTML:\n{e}\n\n{tb}",
                        status=500, mimetype="text/plain")

if __name__=="__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
