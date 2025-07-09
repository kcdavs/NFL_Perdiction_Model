import os, sys, traceback
from flask import Flask, Response
# ensure scraper.py is importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from scraper import get_week_label  # we’ll reuse your week logic
from requests_html import HTMLSession

app = Flask(__name__)

# … your existing / and /scrape endpoints …

@app.route("/scrape-js/<int:egid>/<int:season>")
def scrape_js(egid, season):
    try:
        # 1) build the URL
        week = get_week_label(egid, season)
        url  = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={season}"

        # 2) fire up the tiny headless browser
        session = HTMLSession()
        r = session.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })
        r.html.render(timeout=20)   # runs the JS

        # 3) parse exactly the same way you do now:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.html.html, "html.parser")
        # Copy‐paste your extract logic here (e.g. gridContainer→tables→rows)
        all_data = []
        grid_containers = soup.find_all("div", class_="gridContainer-O4ezT")
        for grid in grid_containers:
            # … same loops and extract_row_data as in scrape_bmr_spread_requests …
            pass

        # Build a DataFrame, etc.
        import pandas as pd
        df = pd.DataFrame(all_data, columns=[…your columns…])

        return Response(f"<pre>{df.to_string(index=False)}</pre>",
                        mimetype="text/html")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ JS‐render error:\n{e}\n\n{tb}",
                        status=500, mimetype="text/plain")

if __name__=="__main__":
    port = int(os.environ.get("PORT",3000))
    app.run(host="0.0.0.0", port=port)
