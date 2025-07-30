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

@app.route("/eids/<int:year>/<int:week>")
def get_eids(year, week):
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
            return Response(f"❌ Unknown SEID for year {year}", status=400)

        egid = 10 + (week - 1)
        url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"

        resp = requests.get(url)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        a_tags = soup.find_all("a", class_="wrapper-2OSHA")

        eids = []
        for a in a_tags:
            href = a.get("href", "")
            parsed = urlparse(href)
            params = parse_qs(parsed.query)
            eid = params.get("eid")
            if eid:
                eids.append(eid[0])

        if not eids:
            return Response("No EIDs found.", mimetype="text/plain")

        return Response("\n".join(eids), mimetype="text/plain")

    except Exception as e:
        return Response(f"❌ Error: {str(e)}", status=500, mimetype="text/plain")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
