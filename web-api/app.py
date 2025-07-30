from flask import Flask, Response
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse, parse_qs
import json
import tempfile
import os
import base64
import re

app = Flask(__name__)

# Utility to extract metadata from HTML
def extract_metadata(year, week):
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
    if seid is None:
        raise ValueError("Unknown SEID")

    egid = 10 + (week - 1)
    url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")

    metadata = []
    for row in soup.find_all("tr", class_="participantRow--z17q"):
        eid = None
        eid_tag = row.find("a", class_="link-1Vzcm")
        if eid_tag:
            parsed = urlparse(eid_tag["href"])
            qs = parse_qs(parsed.query)
            eid = qs.get("eid", [None])[0]

        date_tag = row.find("div", class_="time-3gPvd")
        date = date_tag.find("span").get_text(strip=True) if date_tag else ""
        time = date_tag.find("p").get_text(strip=True) if date_tag else ""

        team_tag = row.find("div", class_="participantName-3CqB8")
        team = team_tag.get_text(strip=True) if team_tag else ""

        score_tag = row.find("span", class_="score-3EWei")
        score = score_tag.get_text(strip=True) if score_tag else ""

        rotation_tag = row.find("td", class_="rotation-3JAfZ")
        rotation = rotation_tag.get_text(strip=True) if rotation_tag else ""

        outcome_tag = row.find("span", class_="eventStatusBox-19ZbY")
        outcome = outcome_tag.get_text(strip=True) if outcome_tag else ""

        metadata.append({
            "eid": eid,
            "rotation": rotation,
            "date": date,
            "time": time,
            "team": team,
            "score": score,
            "outcome": outcome
        })

    return metadata

# Utility to get JSON data using EIDs
def get_json_df(eids, label):
    query = (
        f"{{"
        f"A_BL: bestLines(catid: 338 eid: [{','.join(eids)}] mtid: 401) "
        f"A_CL: currentLines(paid: [8,9,10,123,44,29,16,130,54,82,36,20,127,28,84], eid: [{','.join(eids)}], mtid: 401) "
        f"A_OL: openingLines(paid: 8, eid: [{','.join(eids)}], mtid: 401) "
        f"A_CO: consensus(eid: [{','.join(eids)}], mtid: 401) "
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

    return load_and_pivot_acl(tmp_path, label)

# Parsing odds data
def load_and_pivot_acl(filepath, label):
    team_map = {
        1536: "Philadelphia", 1546: "Atlanta", 1541: "Minnesota", 1547: "San Francisco",
        1525: "New England", 1530: "Houston", 1521: "Baltimore", 1526: "Buffalo",
        1529: "Jacksonville", 1535: "N.Y. Giants", 1527: "Indianapolis", 1522: "Cincinnati",
        1531: "Kansas City", 75380: "L.A. Chargers", 1543: "New Orleans", 1544: "Tampa Bay",
        1523: "N.Y. Jets", 1539: "Detroit", 1540: "Chicago", 1542: "Green Bay",
        1533: "Oakland", 1550: "L.A. Rams", 1538: "Dallas", 1545: "Carolina",
        1534: "Denver", 1548: "Seattle", 1537: "Washington", 1549: "Arizona",
        1524: "Miami", 1528: "Tennessee", 1519: "Pittsburgh", 1520: "Cleveland"
    }

    with open(filepath, "r") as f:
        data = json.load(f)

    cl = pd.DataFrame(data["data"]["A_CL"])[["eid", "partid", "paid", "adj", "ap"]]
    co = pd.DataFrame(data["data"]["A_CO"])[["eid", "partid", "perc"]].drop_duplicates()
    ol = pd.DataFrame(data["data"]["A_OL"])[["eid", "partid", "adj", "ap"]]
    ol.columns = ["eid", "partid", "opening_adj", "opening_ap"]

    cl_adj = cl.pivot_table(index=["eid", "partid"], columns="paid", values="adj").add_prefix("adj_")
    cl_ap = cl.pivot_table(index=["eid", "partid"], columns="paid", values="ap").add_prefix("ap_")
    pivot_df = cl_adj.join(cl_ap).reset_index()

    df = pivot_df.merge(co, on=["eid", "partid"], how="left").merge(ol, on=["eid", "partid"], how="left")
    df["team"] = df["partid"].map(team_map)
    df["jsons"] = label

    metadata = ["jsons", "eid", "partid", "team", "perc", "opening_adj", "opening_ap"]
    adj_cols = [col for col in df.columns if col.startswith("adj_") and col != "opening_adj"]
    ap_cols = [col for col in df.columns if col.startswith("ap_") and col != "opening_ap"]
    suffixes = sorted(set(re.sub(r"^\D+_", "", col) for col in adj_cols + ap_cols), key=lambda x: int(x) if x.isdigit() else x)

    interleaved_cols = []
    for suffix in suffixes:
        adj = f"adj_{suffix}"
        ap = f"ap_{suffix}"
        if adj in df.columns:
            interleaved_cols.append(adj)
        if ap in df.columns:
            interleaved_cols.append(ap)

    final_columns = metadata + interleaved_cols
    return df[final_columns]

# Combine and render
@app.route("/combined/<int:year>/<int:week>")
def combined_view(year, week):
    try:
        metadata = extract_metadata(year, week)
        eids = [m["eid"] for m in metadata if m["eid"] is not None]
        json_df = get_json_df(eids, label=f"{year}_week{week}")

        meta_df = pd.DataFrame(metadata)
        meta_df["eid"] = meta_df["eid"].astype(str)
        json_df["eid"] = json_df["eid"].astype(str)

        merged = pd.merge(meta_df, json_df, on=["eid", "team"], how="left")
        return Response(merged.to_csv(index=False), mimetype="text/csv")

    except Exception as e:
        return Response(f"❌ Error: {e}", status=500, mimetype="text/plain")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
