from flask import Flask, Response, request
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

# ================================
# CONFIGURATION: SEID & EGID MAPPING
# ================================

# Season IDs by year
SEASON_ID_MAP = {
    2018: 4494, 2019: 5703, 2020: 8582, 2021: 29178,
    2022: 38109, 2023: 38292, 2024: 42499, 2025: 59654
}

# Event Group IDs by (year, week)
EVENT_GROUP_ID_MAP = {}
for year in range(2018, 2021):
    for week in range(1, 18):
        EVENT_GROUP_ID_MAP[(year, week)] = 9 + week
    for week, egid in enumerate([28, 29, 30, 31], start=18):
        EVENT_GROUP_ID_MAP[(year, week)] = egid
for year in range(2021, 2026):
    for week in range(1, 18):
        EVENT_GROUP_ID_MAP[(year, week)] = 9 + week
    EVENT_GROUP_ID_MAP[(year, 18)] = 33573
    for week, egid in enumerate([28, 29, 30, 31], start=19):
        EVENT_GROUP_ID_MAP[(year, week)] = egid


# ================================
# STEP 1: METADATA SCRAPER
# ================================

def extract_metadata(year: int, week: int) -> pd.DataFrame:
    """
    Scrape basic game metadata (EIDs, teams, date/time, scores, etc.)
    from the bookmakersreview.com odds page for a given year/week.
    """
    seid = SEASON_ID_MAP.get(year)
    egid = EVENT_GROUP_ID_MAP.get((year, week))
    if seid is None or egid is None:
        raise ValueError(f"Unknown SEID or EGID for {year} Week {week}")

    url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")

    metadata_records = []
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

        metadata_records.append({
            "eid": eid,
            "rotation": rotation,
            "season": year,
            "week": week,
            "date": date,
            "time": time,
            "team": team,
            "score": score,
            "outcome": outcome
        })

    meta_df = pd.DataFrame(metadata_records)

    # Map team names to partid IDs
    reverse_team_map = {
        "CAROLINA": 1545, "DALLAS": 1538, "L.A. RAMS": 1550, "PITTSBURGH": 1519,
        "CLEVELAND": 1520, "BALTIMORE": 1521, "CINCINNATI": 1522, "N.Y. JETS": 1523,
        "MIAMI": 1524, "NEW ENGLAND": 1525, "BUFFALO": 1526, "INDIANAPOLIS": 1527,
        "TENNESSEE": 1528, "JACKSONVILLE": 1529, "HOUSTON": 1530, "KANSAS CITY": 1531,
        "DENVER": 1534, "N.Y. GIANTS": 1535, "PHILADELPHIA": 1536, "WASHINGTON": 1537,
        "DETROIT": 1539, "CHICAGO": 1540, "MINNESOTA": 1541, "GREEN BAY": 1542,
        "NEW ORLEANS": 1543, "TAMPA BAY": 1544, "ATLANTA": 1546, "SAN FRANCISCO": 1547,
        "SEATTLE": 1548, "ARIZONA": 1549, "L.A. CHARGERS": 75380,
        "OAKLAND": 1533, "LAS VEGAS": 1533
    }
    meta_df["partid"] = meta_df["team"].str.strip().str.upper().map(reverse_team_map)

    return meta_df


# ================================
# STEP 2: FETCH JSON ONCE
# ================================

def fetch_odds_json(eids: list) -> dict:
    """
    Fetch the odds JSON (OL, CL, CO data) for a given list of EIDs.
    Returns the parsed JSON as a Python dict.
    """
    query = (
        f"{{"
        f"A_CL: currentLines(paid: [8,9,10,123,44,29,16,130,54,82,36,20,127,28,84], eid: [{','.join(eids)}], mtid: [83,401]) "
        f"A_OL: openingLines(paid: 8, eid: [{','.join(eids)}], mtid: [83,401]) "
        f"A_CO: consensus(eid: [{','.join(eids)}], mtid: [83,401]) "
        f"{{ eid mtid boid partid sbid paid lineid wag perc vol tvol sequence tim }} "
        f"}}"
    )
    url = "https://ms.production-us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service?query=" + query
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://odds.bookmakersreview.com/nfl/",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json" # Add Content-Type header
    }
    # Send the query in the request body as JSON
    resp = requests.get(url, headers=headers) # Use POST and data parameter
    resp.raise_for_status()
    return resp.json()


# ================================
# STEP 3: PARSE FUNCTIONS
# ================================

def parse_opening_lines(data: dict) -> pd.DataFrame:
    """Parse A_OL section into op_ml_odds, op_spr, op_spr_odds."""
    ol_df = pd.DataFrame(data["data"].get("A_OL", []))
    if ol_df.empty:
        return pd.DataFrame(columns=["eid", "partid", "op_ml_odds", "op_spr", "op_spr_odds"])

    ol_df['op_ml_odds'] = ol_df.apply(lambda r: r['ap'] if r['mtid'] == 83 else None, axis=1)
    ol_df['op_spr'] = ol_df.apply(lambda r: r['adj'] if r['mtid'] == 401 else None, axis=1)
    ol_df['op_spr_odds'] = ol_df.apply(lambda r: r['ap'] if r['mtid'] == 401 else None, axis=1)

    ol_df = ol_df[['eid', 'partid', 'op_ml_odds', 'op_spr', 'op_spr_odds']].groupby(
        ['eid', 'partid'], as_index=False).first()
    return ol_df


def parse_current_lines(data: dict) -> pd.DataFrame:
    """Parse A_CL section into bookmaker-specific columns."""
    cl_df = pd.DataFrame(data["data"].get("A_CL", []))
    if cl_df.empty:
        return pd.DataFrame()

    cl_df['ml'] = cl_df.apply(lambda r: r['ap'] if r['mtid'] == 83 else None, axis=1)
    cl_df['spr_odds'] = cl_df.apply(lambda r: r['ap'] if r['mtid'] == 401 else None, axis=1)
    cl_df['spr'] = cl_df.apply(lambda r: r['adj'] if r['mtid'] == 401 else None, axis=1)

    grouped = cl_df.groupby(['eid', 'partid', 'paid']).agg({
        'ml': 'first', 'spr': 'first', 'spr_odds': 'first'
    }).reset_index()

    pivoted = grouped.pivot(index=['eid', 'partid'], columns='paid')
    pivoted.columns = [f"{paid}_{metric}" for metric, paid in pivoted.columns]
    pivoted.reset_index(inplace=True)
    return pivoted


def parse_consensus(data: dict) -> pd.DataFrame:
    """Parse A_CO section into ml_perc, ml_wag, spr_perc, spr_wag."""
    co_df = pd.DataFrame(data["data"].get("A_CO", []))
    if co_df.empty:
        return pd.DataFrame(columns=["eid", "partid", "ml_perc", "ml_wag", "spr_perc", "spr_wag"])

    co_df['ml_perc'] = co_df.apply(lambda r: r['perc'] if r['mtid'] == 83 else None, axis=1)
    co_df['ml_wag'] = co_df.apply(lambda r: r['wag'] if r['mtid'] == 83 else None, axis=1)
    co_df['spr_perc'] = co_df.apply(lambda r: r['perc'] if r['mtid'] == 401 else None, axis=1)
    co_df['spr_wag'] = co_df.apply(lambda r: r['wag'] if r['mtid'] == 401 else None, axis=1)

    grouped = co_df.groupby(['eid', 'partid']).first().reset_index()
    return grouped


# ================================
# STEP 4: MERGE
# ================================

def merge_all(ol_df: pd.DataFrame, cl_df: pd.DataFrame, co_df: pd.DataFrame) -> pd.DataFrame:
    """Merge OL, CL, and CO DataFrames into a final dataset."""
    merged = co_df.merge(ol_df, on=['eid', 'partid'], how='outer')
    merged = merged.merge(cl_df, on=['eid', 'partid'], how='outer')

    op_cols = [c for c in merged.columns if c.startswith('op_')]
    exclude = ['eid', 'partid', 'ml_perc', 'ml_wag', 'spr_perc', 'spr_wag'] + op_cols
    cl_cols = [c for c in merged.columns if c not in exclude]

    ordered_cols = ['eid', 'partid', 'ml_perc', 'ml_wag', 'spr_perc', 'spr_wag'] + op_cols + cl_cols
    return merged[ordered_cols]


# ================================
# STEP 5: ORCHESTRATOR
# ================================

def get_weekly_odds(year: int, week: int) -> pd.DataFrame:
    """
    Full pipeline:
    1. Scrape metadata
    2. Fetch JSON once
    3. Parse OL, CL, CO
    4. Merge and return final DataFrame
    """
    meta_df = extract_metadata(year, week)
    eids = meta_df["eid"].dropna().unique().tolist()

    odds_json = fetch_odds_json(eids)

    ol_df = parse_opening_lines(odds_json)
    cl_df = parse_current_lines(odds_json)
    co_df = parse_consensus(odds_json)

    final_df = merge_all(ol_df, cl_df, co_df)
    return final_df

@app.route("/combined/<int:year>/<int:week>")
def combined_view(year, week):
    try:
        df = get_weekly_odds(year, week)
        return df.to_html(classes="table table-striped", index=False)
    except Exception as e:
        return f"<h3>Error: {str(e)}</h3>"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
