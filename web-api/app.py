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

def push_csv_to_github(csv_content, year, week,
                       repo="kcdavs/NFL-Gambling-Addiction-ml",
                       branch="main"):
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise Exception("Missing GITHUB_TOKEN environment variable")

    filename = f"odds/{year}/week{str(week).zfill(2)}.csv"
    api_url = f"https://api.github.com/repos/{repo}/contents/{filename}"

    response = requests.get(api_url, headers={"Authorization": f"token {token}"})
    sha = response.json().get("sha") if response.status_code == 200 else None

    payload = {
        "message": f"Add odds for {year} week {week}",
        "content": base64.b64encode(csv_content.encode("utf-8")).decode("utf-8"),
        "branch": branch
    }
    if sha:
        payload["sha"] = sha

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }
    res = requests.put(api_url, json=payload, headers=headers)
    res.raise_for_status()
    return res.json()

# SEID and EGID maps
SEID_MAP = {2018: 4494, 2019: 5703, 2020: 8582, 2021: 29178,
            2022: 38109, 2023: 38292, 2024: 42499, 2025: 59654}
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


def extract_metadata(year: int, week: int) -> list[dict]:
    seid = SEID_MAP.get(year)
    egid = EGID_MAP.get((year, week))
    if not seid or not egid:
        raise ValueError(f"Unknown SEID/EGID for {year} Week {week}")

    url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
    res = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    rows = []
    for row in soup.select("tr.participantRow--z17q"):
        # Extract the raw HTML team label
        tag = row.select_one("div.participantName-3CqB8")
        raw_team = tag.get_text(strip=True) if tag else ""

        # Build a helper column for merging: 
        # if this is Raiders (Oakland or Las Vegas), always use "Las Vegas"
        merge_team = "Las Vegas" if "Raiders" in raw_team else raw_team

        # Gather the rest of the metadata
        eid = None
        if (a := row.select_one("a.link-1Vzcm")):
            eid = parse_qs(urlparse(a["href"]).query).get("eid", [None])[0]

        date_tag = row.select_one("div.time-3gPvd")
        date = date_tag.select_one("span").get_text(strip=True) if date_tag else ""
        time = date_tag.select_one("p").get_text(strip=True) if date_tag else ""

        score = (row.select_one("span.score-3EWei") or "").get_text(strip=True)
        rotation = (row.select_one("td.rotation-3JAfZ") or "").get_text(strip=True)
        outcome = (row.select_one("span.eventStatusBox-19ZbY") or "").get_text(strip=True)

        rows.append({
            "eid":        eid,
            "season":     year,
            "week":       week,
            "date":       date,
            "time":       time,
            "team":       raw_team,    # keep original for display
            "merge_team": merge_team,  # use for join
            "rotation":   rotation,
            "score":      score,
            "outcome":    outcome
        })

    return rows


def load_and_pivot_acl(fp: str) -> pd.DataFrame:
    with open(fp, "r") as f:
        data = json.load(f)

    cl = pd.DataFrame(data["data"].get("A_CL", []))
    cl = cl[["eid", "partid", "paid", "adj", "ap"]]

    co = pd.DataFrame(data["data"].get("A_CO", [])).loc[:, ["eid", "partid", "perc"]].drop_duplicates()
    ol = pd.DataFrame(data["data"].get("A_OL", [])).loc[:, ["eid", "partid", "adj", "ap"]]
    ol.columns = ["eid", "partid", "opening_adj", "opening_ap"]

    adj = cl.pivot_table(index=["eid", "partid"], columns="paid", values="adj").add_prefix("adj_")
    ap  = cl.pivot_table(index=["eid", "partid"], columns="paid", values="ap").add_prefix("ap_")
    df  = adj.join(ap).reset_index().merge(co, on=["eid", "partid"], how="left").merge(ol, on=["eid", "partid"], how="left")

    # Map partid → JSON team name (always "Las Vegas" for 1533)
    df["merge_team"] = df["partid"].map(TEAM_MAP)
    return df


def get_json_df(eids: list[str], label: str) -> pd.DataFrame:
    q = (
        f"{{A_BL: bestLines(catid:338 eid:[{','.join(eids)}] mtid:401) "
        f"A_CL: currentLines(paid:{PAID_IDS} eid:[{','.join(eids)}] mtid:401) "
        f"A_OL: openingLines(paid:8 eid:[{','.join(eids)}] mtid:401) "
        f"A_CO: consensus(eid:[{','.join(eids)}] mtid:401) "
        f"{{eid mtid boid partid sbid paid lineid wag perc vol tvol sequence tim}} "
        f"maxSequences{{linesMaxSequence}}}}"
    )
    url = "https://ms.production-us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service?query=" + q
    resp = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=10)
    resp.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
        tmp.write(resp.text)
        fp = tmp.name

    return load_and_pivot_acl(fp)


@app.route("/combined/<int:year>/<int:week>")
def combined_view(year: int, week: int):
    try:
        meta_rows = extract_metadata(year, week)
        meta_df   = pd.DataFrame(meta_rows)
        eids      = [r["eid"] for r in meta_rows if r["eid"]]

        odds_df   = get_json_df(eids, label=f"{year}_week{week}")

        # merge on eid AND our helper merge_team column
        merged = pd.merge(
            meta_df,
            odds_df,
            on=["eid", "merge_team"],
            how="left"
        )

        # drop the helper and reorder
        merged = merged.drop(columns=["merge_team"])
        csv = merged.to_csv(index=False)

        push_csv_to_github(csv, year, week)
        return Response(f"✅ {year} Week {week} uploaded to GitHub", mimetype="text/plain")

    except Exception as ex:
        return Response(f"❌ Error: {ex}", status=500, mimetype="text/plain")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
