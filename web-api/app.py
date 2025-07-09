import os
import sys
import traceback

import requests
from bs4 import BeautifulSoup
from flask import Flask, Response
from requests_html import HTMLSession    # ← NEW

# 1) Make scraper.py importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

# 2) Import your BS4-only scraper and helpers
from scraper import scrape_one_week_requests, get_week_label

app = Flask(__name__)

@app.route("/")
def home():
    return (
        "🟢 API live.\n\n"
        "Use:\n"
        "- /scrape/&lt;egid&gt;/&lt;season&gt;     (static BS4, no odds)\n"
        "- /raw-html/&lt;egid&gt;/&lt;season&gt;  (static HTML dump)\n"
        "- /check-static-odds/&lt;egid&gt;/&lt;season&gt;  (find empty placeholders)\n"
        "- /scrape-js/&lt;egid&gt;/&lt;season&gt; (JS-rendered with odds!)"
    )

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
        url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={season}"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        return Response(resp.text, mimetype="text/plain")
    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error fetching HTML:\n{e}\n\n{tb}",
                        status=500, mimetype="text/plain")

@app.route("/check-static-odds/<int:egid>/<int:season>")
def check_static_odds(egid, season):
    try:
        url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={season}"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        spans = soup.select("span.odd-2T5by")
        texts = [s.get_text(strip=True) for s in spans]
        report = [f"Found {len(spans)} <span class='odd-2T5by'> elements"]
        report += texts[:20] or ["(none)"]
        if len(texts) > 20:
            report.append("…(only showing first 20)")
        return Response("\n".join(report), mimetype="text/plain")
    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}",
                        status=500, mimetype="text/plain")

@app.route("/scrape-js/<int:egid>/<int:season>")
def scrape_js(egid, season):
    try:
        week = get_week_label(egid, season)
        url  = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={season}"
        session = HTMLSession()
        r = session.get(url, headers={"User-Agent":"Mozilla/5.0"})
        r.html.render(timeout=20)

        # now parse exactly as you did before
        soup = BeautifulSoup(r.html.html, "html.parser")
        all_data = []
        for grid in soup.find_all("div", class_="gridContainer-O4ezT"):
            for table in grid.find_all("table", class_="tableGrid-2PF6A"):
                rows = table.find_all("tr", class_="participantRow--z17q")
                for i in range(0, len(rows), 2):
                    r1, r2 = rows[i], rows[i+1]
                    # (you can copy & paste your extract_row_data logic here)
                    # for brevity, let’s just grab spreads from the spans:
                    spans = r1.select("span.odd-2T5by") + r2.select("span.odd-2T5by")
                    texts = [s.get_text(strip=True) for s in spans]
                    all_data.append(texts)

        # Return the first few rows so you can see the odds
        report = ["<pre>"] + [" | ".join(row[:10]) for row in all_data[:10]] + ["</pre>"]
        return Response("\n".join(report), mimetype="text/html")
    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ JS-render error:\n{e}\n\n{tb}",
                        status=500, mimetype="text/plain")

if __name__=="__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
