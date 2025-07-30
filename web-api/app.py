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
        # Step 1: Get EIDs
        eids = get_eids(year, week)
        eid_list = ",".join(eids)
        paid_list = ",".join(map(str, [8, 9, 10, 123, 44, 29, 16, 130, 54, 82, 36, 20, 127, 28, 84]))

        # Step 2: Build GraphQL query
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

        # Step 3: Parse odds data
        df = load_and_pivot_acl(tmp_path, f"{year}_week{week}")

        # Step 4: Scrape HTML metadata
        seid_map = {
            2018: 4494, 2019: 5703, 2020: 8582,
            2021: 29178, 2022: 38109, 2023: 38292,
            2024: 42499, 2025: 59654
        }
        seid = seid_map.get(year)
        egid = 10 + (week - 1)
        url_html = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
        html_resp = requests.get(url_html, headers=headers)
        html_resp.raise_for_status()

        soup = BeautifulSoup(html_resp.text, "html.parser")
        all_data = []

        grid_containers = soup.find_all("div", class_="gridContainer-O4ezT")
        for grid in grid_containers:
            tables = grid.find_all("table", class_="tableGrid-2PF6A")
            for table in tables:
                rows = table.find_all("tr", class_="participantRow--z17q")
                for i in range(0, len(rows), 2):
                    row1, row2 = rows[i], rows[i + 1]

                    def extract_info(r, skip_time=False):
                        tds = r.find_all("td")
                        if skip_time:
                            tds = tds[1:]
                        row = {"season": year, "week": week}
                        time_td = r.find("td", class_="timeContainer-3yNjf")
                        if time_td:
                            outcome = time_td.find("span", class_="eventStatusBox-19ZbY")
                            date = time_td.find("div", class_="time-3gPvd")
                            row["outcome"] = outcome.get_text(strip=True) if outcome else None
                            if date:
                                ds = date.find("span")
                                tp = date.find("p")
                                row["date"] = ds.get_text(strip=True) if ds else None
                                row["time"] = tp.get_text(strip=True) if tp else None
                        team_td = tds[1] if len(tds) > 1 else None
                        score_td = tds[2] if len(tds) > 2 else None
                        row["team"] = team_td.get_text(strip=True) if team_td else None
                        row["score"] = score_td.get_text(strip=True) if score_td else None
                        return row

                    all_data.append(extract_info(row1, skip_time=True))
                    all_data.append(extract_info(row2, skip_time=False))

        html_df = pd.DataFrame(all_data)
        html_df = html_df.dropna(subset=["team"])

        # Step 5: Merge odds data with HTML metadata
        merged = df.merge(html_df, on=["season", "week", "team"], how="left")

        csv_data = merged.to_csv(index=False)
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
