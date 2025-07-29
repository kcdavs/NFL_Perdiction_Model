import os
import traceback
import requests
import tempfile
import pandas as pd
from flask import Flask, Response, request
from tabulate import load_and_pivot_acl
from github_writer import push_csv_to_github  # ✅ move this here

app = Flask(__name__)

@app.route("/")
def home():
    return (
        "🟢 NFL Odds Scraper Running!\n\n"
        "Visit /tabulate to see tabulated odds using raw URLs from GitHub.\n"
        "Make sure your GitHub repo has: urls/10gameURL.txt and 6gameURL.txt\n"
    )

@app.route("/tabulate")
def tabulate_from_github():
    try:
        GITHUB_BASE = "https://raw.githubusercontent.com/kcdavs/NFL-Gambling-Addiction-ml/main/urls/"
        files = [("10gameURL.txt", "10games"), ("6gameURL.txt", "6games")]
        dfs = []

        for filename, label in files:
            url = GITHUB_BASE + filename
            raw_url = requests.get(url).text.strip()

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Referer": "https://odds.bookmakersreview.com/nfl/",
                "X-Requested-With": "XMLHttpRequest",
            }
            resp = requests.get(raw_url, headers=headers, timeout=10)
            resp.raise_for_status()

            with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
                tmp.write(resp.text)
                tmp_path = tmp.name

            df = load_and_pivot_acl(tmp_path, label)
            dfs.append(df)

        final_df = pd.concat(dfs, ignore_index=True).sort_values(["eid", "partid"])
        return Response(final_df.to_csv(index=False), mimetype="text/csv")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")


# ✅ SCRAPE ROUTE GOES HERE — not after __main__
@app.route("/scrape/<int:year>/<int:week>")
def scrape_and_push_week(year, week):
    try:
        base_eid = 3452654  # Adjust this if needed
        eids = list(range(base_eid + 16 * (week - 1), base_eid + 16 * week))

        eid_list = ",".join(map(str, eids))
        paid_list = ",".join(map(str, [8, 9, 10, 123, 44, 29, 16, 130, 54, 82, 36, 20, 127, 28, 84]))

        query = (
            f"{{"
            f"A_BL: bestLines(catid: 338 eid: [{eid_list}] mtid: 401) "
            f"A_CL: currentLines(paid: [{paid_list}], eid: [{eid_list}], mtid: 401) "
            f"A_OL: openingLines(paid: 8, eid: [{eid_list}], mtid: 401) "
            f"A_CO: consensus(eid: [{eid_list}], mtid: 401) "
            f"{{ eid mtid boid partid sbid paid lineid wag perc vol tvol sequence tim }} "
            f"maxSequences {{ linesMaxSequence }} "
            f"}}"
        )
        url = "https://ms.production-us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service?query=" + query

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://odds.bookmakersreview.com/nfl/",
            "X-Requested-With": "XMLHttpRequest",
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
            tmp.write(resp.text)
            tmp_path = tmp.name

        df = load_and_pivot_acl(tmp_path, f"{year}_week{week}")
        csv_data = df.to_csv(index=False)

        push_csv_to_github(csv_data, year, week)
        return Response(f"✅ Week {week} ({year}) pushed to GitHub", mimetype="text/plain")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")


# ✅ Keep this at the bottom
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
