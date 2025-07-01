import os, traceback
from flask import Flask, Response
import sys

# allow importing scraper.py
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from scraper import fetch_week_data

app = Flask(__name__)

@app.route("/")
def home():
    return "🟢 API is live – call /scrape/egid/season"

@app.route("/scrape/<int:egid>/<int:season>")
def scrape(egid, season):
    try:
        df = fetch_week_data(egid, season)
        return Response(f"<pre>{df.to_string(index=False)}</pre>", mimetype="text/html")
    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")

if __name__=="__main__":
    port = int(os.environ.get("PORT",3000))
    app.run(host="0.0.0.0", port=port)
