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

@app.route("/fetch-and-tabulate/<int:year>/<int:week>")
def fetch_and_tabulate(year, week):
    try:
        seid_lookup = {2018: 4494, 2019: 4520, 2020: 4546, 2021: 4572, 2022: 4598, 2023: 4624, 2024: 42499, 2025: 59654}
        seid = seid_lookup.get(year)
        egid = 10 + (week - 1)
        url_html = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
        html_resp = requests.get(url_html)
        html_resp.raise_for_status()

        soup = BeautifulSoup(html_resp.text, "html.parser")
        a_tags = soup.find_all("a", class_="wrapper-2OSHA", href=True)

        metadata_rows = []
        eid_order = []
        for a in a_tags:
            href = a.get("href")
            parsed = urlparse(href)
            eid = parse_qs(parsed.query).get("eid", [None])[0]
            if not eid or not eid.isdigit():
                continue
            eid = int(eid)
            eid_order.append(eid)

            game_rows = a.find_all("tr", class_="participantRow--z17q")
            date = time = outcome = None
            for r in game_rows:
                tds = r.find_all("td")
                meta = {"eid": eid, "season": year, "week": week}
                time_td = r.find("td", class_="timeContainer-3yNjf")
                if time_td:
                    status = time_td.find("span", class_="eventStatusBox-19ZbY")
                    when = time_td.find("div", class_="time-3gPvd")
                    if status: outcome = status.get_text(strip=True)
                    if when:
                        ds = when.find("span")
                        tp = when.find("p")
                        if ds: date = ds.get_text(strip=True)
                        if tp: time = tp.get_text(strip=True)
                meta["date"] = date
                meta["time"] = time
                meta["outcome"] = outcome
                meta["team"] = tds[1].get_text(strip=True) if len(tds) > 1 else None
                meta["score"] = tds[2].get_text(strip=True) if len(tds) > 2 else None
                metadata_rows.append(meta)

        meta_df = pd.DataFrame(metadata_rows)
        if "team" not in meta_df.columns or "eid" not in meta_df.columns:
            return Response("❌ Error: Missing required 'team' or 'eid' in metadata", status=500, mimetype="text/plain")

        meta_df = meta_df.dropna(subset=["team", "eid"])
        meta_df["eid"] = meta_df["eid"].astype(int)

        eid_list = ",".join(map(str, eid_order))
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
        gql_url = "https://ms.production-us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service?query=" + query
        resp = requests.get(gql_url)
        resp.raise_for_status()

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
            tmp.write(resp.text)
            tmp_path = tmp.name

        df = load_and_pivot_acl(tmp_path, f"{year}_week{week}")
        df["mapped_team"] = df["partid"].map(TEAM_MAP)
        df["season"] = year
        df["week"] = week

        if "eid" not in df.columns or df["eid"].isnull().all():
            return Response("❌ Error: No valid EIDs in JSON data", status=500, mimetype="text/plain")

        inconsistencies = []
        for eid in df["eid"].unique():
            odds_teams = df[df["eid"] == eid]["mapped_team"].dropna().tolist()
            meta_teams = meta_df[meta_df["eid"] == eid]["team"].dropna().tolist()
            if sorted(odds_teams) != sorted(meta_teams):
                inconsistencies.append((eid, odds_teams, meta_teams))

        if inconsistencies:
            error_msg = "Team mismatch for EIDs:\n"
            for eid, json_teams, html_teams in inconsistencies:
                error_msg += f"EID {eid}:\n  JSON: {json_teams}\n  HTML: {html_teams}\n\n"
            return Response(error_msg, status=400, mimetype="text/plain")

        meta_df["meta_idx"] = meta_df.groupby("eid").cumcount()
        df["meta_idx"] = df.groupby("eid").cumcount()

        merged = df.merge(meta_df, on=["eid", "season", "week", "meta_idx"], how="left")
        merged.drop(columns=["meta_idx", "mapped_team"], inplace=True)

        meta_cols = ["season", "week", "date", "time", "team", "score", "outcome"]
        betting_cols = [c for c in merged.columns if c.startswith("adj_") or c.startswith("ap_") or c in ["perc", "opening_adj", "opening_ap"]]
        id_cols = ["eid", "partid"]
        final_cols = meta_cols + betting_cols + id_cols
        merged = merged[final_cols]

        csv_data = merged.to_csv(index=False)
        return Response(csv_data, mimetype="text/csv")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
