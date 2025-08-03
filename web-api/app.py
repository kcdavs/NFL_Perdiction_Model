#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask API for scraping NFL weekly odds from BookmakersReview.com,
merging with fixture metadata, and pushing CSVs to a GitHub repo.

Endpoints:
  GET /healthz                  Health check (returns status).
  GET /combined/<year>/<week>   Scrape, merge, and upload data.

Environment:
  GITHUB_TOKEN: GitHub PAT with `contents:write` for the target repo.
  PORT (optional): Flask port (default: 3000).
"""

import os
import json
import tempfile
import base64
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd
from bs4 import BeautifulSoup
from flask import Flask, jsonify, Response

app = Flask(__name__)

# GitHub settings
GITHUB_REPO   = os.getenv("GITHUB_REPO", "kcdavs/NFL-Gambling-Addiction-ml")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN")

# Lookup maps
SEID_MAP = {
    2018: 4494, 2019: 5703, 2020: 8582,
    2021: 29178, 2022: 38109, 2023: 38292,
    2024: 42499, 2025: 59654
}
EGID_MAP = {}
for yr in range(2018, 2021):
    for wk in range(1, 18):
        EGID_MAP[(yr, wk)] = 9 + wk
    for idx, egid in enumerate([28,29,30,31], start=18):
        EGID_MAP[(yr, idx)] = egid
for yr in range(2021, 2026):
    for wk in range(1, 18):
        EGID_MAP[(yr, wk)] = 9 + wk
    EGID_MAP[(yr, 18)] = 33573
    for idx, egid in enumerate([28,29,30,31], start=19):
        EGID_MAP[(yr, idx)] = egid

# JSON partid → team mapping
TEAM_MAP = {
    1536: "Philadelphia", 1546: "Atlanta", 1541: "Minnesota", 1547: "San Francisco",
    1525: "New England", 1530: "Houston", 1521: "Baltimore", 1526: "Buffalo",
    1529: "Jacksonville", 1535: "N.Y. Giants", 1527: "Indianapolis", 1522: "Cincinnati",
    1531: "Kansas City", 75380: "L.A. Chargers", 1543: "New Orleans", 1544: "Tampa Bay",
    1523: "N.Y. Jets", 1539: "Detroit", 1540: "Chicago", 1542: "Green Bay",
    1533: "Las Vegas", 1550: "L.A. Rams", 1538: "Dallas", 1545: "Carolina",
    1534: "Denver", 1548: "Seattle", 1537: "Washington", 1549: "Arizona",
    1524: "Miami", 1528: "Tennessee", 1519: "Pittsburgh", 1520: "Cleveland"
}

PAID_IDS   = [8,9,10,123,44,29,16,130,54,82,36,20,127,28,84]
USER_AGENT = "Mozilla/5.0"

def push_csv_to_github(csv_content: str, year: int, week: int) -> None:
    if not GITHUB_TOKEN:
        raise RuntimeError("GITHUB_TOKEN environment variable is missing")
    path    = f"data/odds/{year}/week{week:02d}.csv"
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    # fetch existing sha
    resp = requests.get(api_url, headers=headers)
    sha  = resp.json().get("sha") if resp.status_code == 200 else None

    payload = {
        "message": f"Add odds for {year} week {week}",
        "content": base64.b64encode(csv_content.encode("utf-8")).decode("utf-8"),
        "branch": GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha

    res = requests.put(api_url, headers=headers, json=payload)
    res.raise_for_status()

def extract_metadata(year: int, week: int) -> list[dict]:
    seid = SEID_MAP.get(year)
    egid = EGID_MAP.get((year, week))
    if not seid or not egid:
        raise ValueError(f"Unknown SEID/EGID for {year} Week {week}")

    url  = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    metadata = []
    for row in soup.select("tr.participantRow--z17q"):
        eid_tag = row.select_one("a.link-1Vzcm")
        eid = parse_qs(urlparse(eid_tag["href"]).query).get("eid", [None])[0] if eid_tag else None

        date_tag = row.select_one("div.time-3gPvd")
        date = date_tag.select_one("span").get_text(strip=True) if date_tag else ""
        time = date_tag.select_one("p").get_text(strip=True) if date_tag else ""

        raw_team = row.select_one("div.participantName-3CqB8").get_text(strip=True)
        # unify Oakland → Las Vegas
        team = "Las Vegas" if "Oakland" in raw_team else raw_team

        score    = row.select_one("span.score-3EWei").get_text(strip=True) if row.select_one("span.score-3EWei") else ""
        rotation = row.select_one("td.rotation-3JAfZ").get_text(strip=True) if row.select_one("td.rotation-3JAfZ") else ""
        outcome  = row.select_one("span.eventStatusBox-19ZbY").get_text(strip=True) if row.select_one("span.eventStatusBox-19ZbY") else ""

        metadata.append({
            "eid": eid, "rotation": rotation, "season": year, "week": week,
            "date": date, "time": time, "team": team, "score": score, "outcome": outcome
        })

    return metadata

def load_and_pivot_acl(filepath: str) -> pd.DataFrame:
    with open(filepath, "r") as f:
        data = json.load(f)
    cl = pd.DataFrame(data["data"].get("A_CL", []))
    cl = cl[["eid","partid","paid","adj","ap"]]
    co = pd.DataFrame(data["data"].get("A_CO", [])).loc[:, ["eid","partid","perc"]].drop_duplicates()
    ol = pd.DataFrame(data["data"].get("A_OL", [])).loc[:, ["eid","partid","adj","ap"]]
    ol.columns = ["eid","partid","opening_adj","opening_ap"]

    adj = cl.pivot_table(index=["eid","partid"], columns="paid", values="adj").add_prefix("adj_")
    ap  = cl.pivot_table(index=["eid","partid"], columns="paid", values="ap").add_prefix("ap_")
    df  = adj.join(ap).reset_index().merge(co, on=["eid","partid"], how="left").merge(ol, on=["eid","partid"], how="left")
    df["team"] = df["partid"].map(TEAM_MAP)
    return df

def get_json_df(eids: list[str]) -> pd.DataFrame:
    q = (
        f"{{A_BL: bestLines(catid:338 eid:[{','.join(eids)}] mtid:401) "
        f"A_CL: currentLines(paid:{PAID_IDS} eid:[{','.join(eids)}] mtid:401) "
        f"A_OL: openingLines(paid:8 eid:[{','.join(eids)}] mtid:401) "
        f"A_CO: consensus(eid:[{','.join(eids)}] mtid:401) "
        f"{{eid mtid boid partid sbid paid lineid wag perc vol tvol sequence tim}} "
        f"maxSequences {{linesMaxSequence}}}}"
    )
    url  = "https://ms.production-us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service?query=" + q
    resp = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=10)
    resp.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
        tmp.write(resp.text)
        tmp_path = tmp.name

    return load_and_pivot_acl(tmp_path)

def run_scrape(year: int, week: int) -> str:
    meta    = extract_metadata(year, week)
    eids    = [m["eid"] for m in meta if m.get("eid")]
    odds_df = get_json_df(eids)
    meta_df = pd.DataFrame(meta)
    merged  = pd.merge(meta_df, odds_df, on=["eid","team"], how="left")
    csv     = merged.to_csv(index=False)
    push_csv_to_github(csv, year, week)
    return csv

@app.route("/healthz")
def healthz():
    return jsonify(status="ok"), 200

@app.route("/combined/<int:year>/<int:week>")
def combined(year: int, week: int):
    try:
        run_scrape(year, week)
        return Response(f"✅ {year} Week {week} processed", mimetype="text/plain"), 200
    except Exception as e:
        return Response(f"❌ Error: {e}", status=500, mimetype="text/plain")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
