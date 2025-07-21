# web-api/app.py

import os
import traceback
from flask import Flask, Response, request, jsonify
from scraper import scrape_one_week_requests
import requests

app = Flask(__name__)

@app.route("/")
def home():
    return (
        "🟢 NFL Odds Proxy Live!\n\n"
        "Use GET /fetch-json?url=<FULL_FETCH_URL>\n"
        "Example:\n"
        "  /fetch-json?url=https://cache.bmr.bbsi.com/odds/getLines"
        "?seid=4494&egid=10&market=all&period=reg\n"
    )

@app.route("/scrape/<int:egid>/<int:season>")
def scrape_week(egid, season):
    try:
        df = scrape_one_week_requests(egid, season)
        return df.to_json(orient="records")  # JSON array of row objects
    except Exception as e:
        tb = traceback.format_exc()
        return Response(
            f"❌ Error scraping:\n{e}\n\n{tb}",
            status=500,
            mimetype="text/plain"
        )

@app.route("/fetch-json")
def fetch_json():
    # 1) Grab the URL to proxy
    fetch_url = request.args.get("url", "")
    if not fetch_url:
        return Response(
            "❌ Missing required parameter: url\n"
            "Usage: /fetch-json?url=<FULL_FETCH_URL>",
            status=400,
            mimetype="text/plain"
        )

    # 2) Minimal headers to mimic a browser
    headers = {
        "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Referer":          "https://odds.bookmakersreview.com/nfl/",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        # 3) Fetch the remote JSON
        resp = requests.get(fetch_url, headers=headers, timeout=10)
        resp.raise_for_status()
        # 4) Return it verbatim
        return Response(resp.text, mimetype="application/json")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(
            f"❌ Error proxying URL:\n{e}\n\n{tb}",
            status=500,
            mimetype="text/plain"
        )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
