import os
import traceback
import requests
import tempfile
import pandas as pd
from flask import Flask, Response, request
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from tabulate import load_and_pivot_acl
from github_writer import push_csv_to_github
from fetch_capture import fetch_odds_json_urls

app = Flask(__name__)

@app.route("/")
def home():
    return (
        "🟢 NFL Odds Scraper Running!\n\n"
        "Endpoints:\n"
        "/tabulate – uses URLs from GitHub\n"
        "/scrape/<year>/<week> – fetch and push to GitHub\n"
        "/fetch/<year>/<week> – display JSON fetch URLs in browser\n"
        "/games/<year>/<week> – display scraped HTML table in browser\n"
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

@app.route("/scrape/<int:year>/<int:week>")
def scrape_and_push_week(year, week):
    try:
        if year == 2018 and week == 1:
            eids = [3452654, 3452656, 3452658, 3452660, 3452662] + list(range(3452663, 3452674))
        else:
            season_start_eids = {
                2018: 3452674,
                2019: 3452942,
                2020: 3453230,
                2021: 3453518,
                2022: 3453806,
                2023: 3454094
            }
            base_eid = season_start_eids.get(year)
            if base_eid is None:
                return Response(f"❌ No base EID defined for season {year}", status=400)
            eids = list(range(base_eid + 16 * (week - 2), base_eid + 16 * (week - 1)))

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

@app.route("/fetch/<int:year>/<int:week>")
def fetch_urls_for_week(year, week):
    try:
        seid_map = {
            2018: 4494,
            2019: 4520,
            2020: 4546,
            2021: 4572,
            2022: 4598,
            2023: 4624
        }
        seid = seid_map.get(year)
        egid = 10 + (week - 1)

        if seid is None:
            return Response(f"No SEID for year {year}", status=400)

        urls = fetch_odds_json_urls(seid, egid)
        if not urls:
            return Response("No fetch URLs found.", mimetype="text/plain")

        return Response("\n\n".join(urls), mimetype="text/plain")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")

@app.route("/games/<int:year>/<int:week>")
def show_grid_container(year, week):
    try:
        seid_map = {
            2018: 4494,
            2019: 4520,
            2020: 4546,
            2021: 4572,
            2022: 4598,
            2023: 4624
        }
        seid = seid_map.get(year)
        if seid is None:
            return Response("❌ Unknown SEID for that year", status=400)

        egid = 10 + (week - 1)
        url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"

        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()

        soup = BeautifulSoup(res.text, "html.parser")

        # Find only the container you care about
        grid = soup.find("div", class_="gridContainer-O4ezT")
        if not grid:
            return Response("❌ gridContainer-O4ezT not found", status=404)

        # Remove scripts/styles from just this part
        for tag in grid(["script", "style", "noscript"]):
            tag.decompose()

        cleaned = str(grid)

        return Response(f"<html><body>{cleaned}</body></html>", mimetype="text/html")

    except Exception as e:
        return Response(f"❌ Error: {str(e)}", status=500, mimetype="text/plain")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
