import os
import traceback
import requests
import tempfile
import pandas as pd
from flask import Flask, Response
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from tabulate import load_and_pivot_acl
from github_writer import push_csv_to_github

app = Flask(__name__)

# Your existing /eids/<year>/<week> route here (or just copy scrape logic into this helper):
# Static partid-to-team mapping used for verification
TEAM_MAP = {
    1536: "Philadelphia", 1546: "Atlanta", 1541: "Minnesota", 1547: "San Francisco",
    1525: "New England", 1530: "Houston", 1521: "Baltimore", 1526: "Buffalo",
    1529: "Jacksonville", 1535: "N.Y. Giants", 1527: "Indianapolis", 1522: "Cincinnati",
    1531: "Kansas City", 75380: "L.A. Chargers", 1543: "New Orleans", 1544: "Tampa Bay",
    1523: "N.Y. Jets", 1539: "Detroit", 1540: "Chicago", 1542: "Green Bay",
    1533: "Oakland", 1550: "L.A. Rams", 1538: "Dallas", 1545: "Carolina",
    1534: "Denver", 1548: "Seattle", 1537: "Washington", 1549: "Arizona",
    1524: "Miami", 1528: "Tennessee", 1519: "Pittsburgh", 1520: "Cleveland"
}

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

@app.route("/fetch_and_save/<int:year>/<int:week>")
def fetch_and_save_to_github(year, week):
    try:
        seid_map = {
            2018: 4494,
            2019: 5703,
            2020: 8582,
            2021: 29178,
            2022: 38109,
            2023: 38292,
            2024: 42499,
            2025: 59654
        }
        seid = seid_map.get(year)
        egid = 10 + (week - 1)

        if not seid:
            return Response(f"No SEID found for year {year}", status=400)

        # Step 1: Get ordered EIDs from the page
        url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "html.parser")
        a_tags = soup.find_all("a", class_="wrapper-2OSHA", href=True)

        eids = []
        for a in a_tags:
            href = a["href"]
            if "eid=" in href:
                parsed = urlparse(href)
                query = parse_qs(parsed.query)
                eid = query.get("eid", [None])[0]
                if eid and eid.isdigit():
                    eids.append(int(eid))

        if not eids:
            return Response("No EIDs found on the page", status=500)

        # Step 2: Construct GraphQL query using ordered EIDs
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
        graphql_url = "https://ms.production-us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service?query=" + query
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://odds.bookmakersreview.com/nfl/",
            "X-Requested-With": "XMLHttpRequest",
        }

        resp = requests.get(graphql_url, headers=headers, timeout=10)
        resp.raise_for_status()

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
            tmp.write(resp.text)
            tmp_path = tmp.name

        df = load_and_pivot_acl(tmp_path, f"{year}_week{week}")

        # Preserve row order based on original EID order
        df["eid_order"] = df["eid"].apply(lambda x: eids.index(x) if x in eids else -1)
        df = df.sort_values(["eid_order", "partid"]).drop(columns="eid_order")

        csv_data = df.to_csv(index=False)
        push_csv_to_github(csv_data, year, week)

        return Response(f"✅ Data pushed for {year} Week {week}", mimetype="text/plain")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")

@app.route("/metadata/<int:year>/<int:week>")
def metadata_test(year, week):
    try:
        SEID_MAP = {
            2018: 4494, 2019: 5703, 2020: 8582,
            2021: 29178, 2022: 38109, 2023: 38292,
            2024: 42499, 2025: 59654
        }

        seid = SEID_MAP.get(year)
        if not seid:
            return Response(f"No SEID found for year {year}", status=400)

        egid = 10 + (week - 1)
        url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "html.parser")

        rows = []
        for game in soup.find_all("a", class_="wrapper-2OSHA", href=True):
            row = {}
            # Extract EID
            parsed = urlparse(game["href"])
            query = parse_qs(parsed.query)
            eid = query.get("eid", [None])[0]
            row["eid"] = eid

            # Game time (day/time)
            time_tag = game.find("div", class_="dateBox-3kF8a")
            row["time"] = time_tag.text.strip() if time_tag else None

            # Team names and scores
            teams = game.find_all("div", class_="teamWrapper-11a8T")
            if len(teams) == 2:
                for i, t in enumerate(["away", "home"]):
                    name_tag = teams[i].find("span", class_="teamName-1xh5H")
                    score_tag = teams[i].find("span", class_="scoreWrapper-2r60i")
                    row[f"{t}_team"] = name_tag.text.strip() if name_tag else None
                    row[f"{t}_score"] = score_tag.text.strip() if score_tag else None

            rows.append(row)

        df = pd.DataFrame(rows)
        return Response(df.to_csv(index=False), mimetype="text/csv")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
