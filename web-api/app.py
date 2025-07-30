import os
import traceback
import requests
import tempfile
import pandas as pd
from flask import Flask, Response
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from tabulate import load_and_pivot_acl

app = Flask(__name__)

# Your existing /eids/<year>/<week> route here (or just copy scrape logic into this helper):

def get_eids(year, week):
    seid_map = {
        2018: 4494, 2019: 4520, 2020: 4546,
        2021: 4572, 2022: 4598, 2023: 4624
    }
    seid = seid_map.get(year)
    if seid is None:
        raise ValueError(f"Unknown SEID for year {year}")

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
        raise ValueError("No EIDs found for this year/week")
    return eids


@app.route("/fetch-and-tabulate/<int:year>/<int:week>")
def fetch_and_tabulate(year, week):
    try:
        eids = get_eids(year, week)
        eid_list = ",".join(eids)
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
        return Response(csv_data, mimetype="text/csv")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")

@app.route("/fetch-and-save/<int:year>/<int:week>")
def fetch_and_save_to_github(year, week):
    try:
        # Map seid and calculate egid like before
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
            return Response(f"❌ Unknown season ID (seid) for year {year}", status=400)

        egid = 10 + (week - 1)
        url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "html.parser")

        # Extract all eids from 'a.wrapper-2OSHA' anchors
        eids = []
        for a in soup.select("a.wrapper-2OSHA[href*='eid=']"):
            href = a.get("href", "")
            parsed = urlparse(href)
            params = parse_qs(parsed.query)
            eid = params.get("eid", [None])[0]
            if eid and eid.isdigit():
                eids.append(int(eid))

        if not eids:
            return Response("❌ No EIDs found for that year/week.", status=400)

        # Compose the GraphQL query with these eids
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
        fetch_url = "https://ms.production-us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service?query=" + query

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://odds.bookmakersreview.com/nfl/",
            "X-Requested-With": "XMLHttpRequest",
        }
        resp = requests.get(fetch_url, headers=headers, timeout=10)
        resp.raise_for_status()

        # Save the JSON temporarily
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
            tmp.write(resp.text)
            tmp_path = tmp.name

        # Process the JSON into dataframe
        df = load_and_pivot_acl(tmp_path, f"{year}_week{week}")

        # Convert df to CSV string
        csv_data = df.to_csv(index=False)

        # Compose the path for GitHub: odds/{year}/week{week:02d}.csv
        github_path = f"odds/{year}/week{week:02d}.csv"

        # Push CSV to GitHub
        push_csv_to_github(csv_data, year, week, github_path=github_path)

        return Response(f"✅ Week {week} ({year}) data saved to GitHub at {github_path}", mimetype="text/plain")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
