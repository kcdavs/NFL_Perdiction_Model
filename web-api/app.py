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
from github import Github, UnknownObjectException
import io

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
        f"A_CL: currentLines(paid: [8,9,10,123,44,29,16,130,54,82,36,20,127,28,84], eid: [{','.join(eids)}], mtid: [83,401,402]) "
        f"A_OL: openingLines(paid: 8, eid: [{','.join(eids)}], mtid: [83,401,402]) "
        f"A_CO: consensus(eid: [{','.join(eids)}], mtid: [83,401,402]) "
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
    ol_df = pd.DataFrame(data["data"].get("A_OL", []))
    if ol_df.empty:
        return pd.DataFrame(columns=["eid", "partid", "op_ml", "op_spr", "op_spr_odds", "op_ou", "op_ou_odds"])

    ol_df['op_ml'] = ol_df.apply(lambda r: r['ap']  if r['mtid'] == 83 else None, axis=1)

    ol_df['op_spr']      = ol_df.apply(lambda r: r['adj'] if r['mtid'] == 401 else None, axis=1)
    ol_df['op_spr_odds'] = ol_df.apply(lambda r: r['ap']  if r['mtid'] == 401 else None, axis=1)

    ol_df['op_ou']      = ol_df.apply(lambda r: r['adj'] if r['mtid'] == 402 else None, axis=1)
    ol_df['op_ou_odds'] = ol_df.apply(lambda r: r['ap']  if r['mtid'] == 402 else None, axis=1)

    ol_df = ol_df[['eid', 'partid', 'op_ml', 'op_spr', 'op_spr_odds', 'op_ou', 'op_ou_odds']].groupby(
        ['eid', 'partid'], as_index=False).first()

    return ol_df

def parse_current_lines(data: dict) -> pd.DataFrame:
    cl_df = pd.DataFrame(data["data"].get("A_CL", []))
    if cl_df.empty:
        return pd.DataFrame()

    cl_df['ml']        = cl_df.apply(lambda r: r['ap']  if r['mtid'] == 83 else None, axis=1)
    cl_df['spr']       = cl_df.apply(lambda r: r['adj'] if r['mtid'] == 401 else None, axis=1)
    cl_df['spr_odds']  = cl_df.apply(lambda r: r['ap']  if r['mtid'] == 401 else None, axis=1)
    cl_df['ou']        = cl_df.apply(lambda r: r['adj'] if r['mtid'] == 402 else None, axis=1)
    cl_df['ou_odds']   = cl_df.apply(lambda r: r['ap']  if r['mtid'] == 402 else None, axis=1)

    grouped = cl_df.groupby(['eid', 'partid', 'paid']).agg({
        'ml': 'first',
        'spr': 'first',
        'spr_odds': 'first',
        'ou': 'first',
        'ou_odds': 'first'
    }).reset_index()

    pivoted = grouped.pivot(index=['eid', 'partid'], columns='paid')
    pivoted.columns = [f"{paid}_{metric}" for metric, paid in pivoted.columns]
    pivoted.reset_index(inplace=True)
    return pivoted

def parse_consensus(data: dict) -> pd.DataFrame:
    co_df = pd.DataFrame(data["data"].get("A_CO", []))
    if co_df.empty:
        return pd.DataFrame(columns=["eid", "partid", "ml_perc", "ml_wag", "spr_perc", "spr_wag", "ou_perc", "ou_wag"])

    co_df['ml_perc'] = co_df.apply(lambda r: r['perc'] if r['mtid'] == 83 else None, axis=1)
    co_df['ml_wag']  = co_df.apply(lambda r: r['wag'] if r['mtid'] == 83 else None, axis=1)

    co_df['spr_perc'] = co_df.apply(lambda r: r['perc'] if r['mtid'] == 401 else None, axis=1)
    co_df['spr_wag']  = co_df.apply(lambda r: r['wag'] if r['mtid'] == 401 else None, axis=1)

    co_df['ou_perc'] = co_df.apply(lambda r: r['perc'] if r['mtid'] == 402 else None, axis=1)
    co_df['ou_wag']  = co_df.apply(lambda r: r['wag'] if r['mtid'] == 402 else None, axis=1)

    co_df = co_df[['eid', 'partid', 'ml_perc', 'ml_wag', 'spr_perc', 'spr_wag', 'ou_perc', 'ou_wag']]

    co_df = co_df.groupby(['eid', 'partid']).first().reset_index()
    return co_df


# ================================
# STEP 4: MERGE
# ================================

def normalize_keys(df):
    for col in ['eid', 'partid']:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df

def merge_all(meta_df, ol_df, cl_df, co_df):
    # --- Step 0: Add parity to meta_df ---
    meta_df['rotation'] = pd.to_numeric(meta_df['rotation'], errors='coerce')
    meta_df['parity'] = meta_df['rotation'] % 2

    # --- Step 1: Split original dfs into ML/SPR-only and OU-only ---
    def split_ml_spr_ou(df):
        ou_cols = [c for c in df.columns if 'ou' in c]
        base_cols = ['eid', 'partid']
        ml_spr_df = df[[c for c in df.columns if c not in ou_cols]].copy()
        ou_df = df[ou_cols + base_cols].copy()
        return ml_spr_df, ou_df

    ol_df_ml, ol_df_ou = split_ml_spr_ou(ol_df)
    cl_df_ml, cl_df_ou = split_ml_spr_ou(cl_df)
    co_df_ml, co_df_ou = split_ml_spr_ou(co_df)

    # --- Step 2: Add parity to OU dfs ---
    for df_ in [ol_df_ou, cl_df_ou, co_df_ou]:
        df_['partid'] = pd.to_numeric(df_['partid'], errors='coerce')
        df_['parity'] = df_['partid'] % 2

    # --- Step 3: Merge ML/SPR columns normally ---
    merged = meta_df.merge(ol_df_ml, on=['eid', 'partid'], how='left')
    merged = merged.merge(cl_df_ml, on=['eid', 'partid'], how='left')
    merged = merged.merge(co_df_ml, on=['eid', 'partid'], how='left')

    keep_partids = {15143, 15144}
    co_df_ou = co_df_ou[co_df_ou['partid'].isin(keep_partids)]
    ol_df_ou = ol_df_ou[ol_df_ou['partid'].isin(keep_partids)]
    cl_df_ou = cl_df_ou[cl_df_ou['partid'].isin(keep_partids)]


    # --- Step 4: Merge OU columns using parity ---
    merged = merged.merge(co_df_ou.drop(columns='partid'), on=['eid', 'parity'], how='left')
    merged = merged.merge(ol_df_ou.drop(columns='partid'), on=['eid', 'parity'], how='left')
    merged = merged.merge(cl_df_ou.drop(columns='partid'), on=['eid', 'parity'], how='left')

    # --- Step 5: Drop helper col ---
    merged.drop(columns='parity', inplace=True)

    # Base metadata columns
    base_cols = ['eid', 'partid'] + [c for c in meta_df.columns if c not in ['eid', 'partid']]

    # Detect paid IDs numerically
    paid_ids = sorted({int(col.split('_')[0]) for col in merged.columns if col.split('_')[0].isdigit()})

    # Opening line columns
    op_cols = ['op_ml', 'op_spr', 'op_spr_odds', 'op_ou', 'op_ou_odds']

    # Consensus columns (placed before OL columns)
    co_cols = ['ml_perc', 'ml_wag', 'spr_perc', 'spr_wag', 'ou_perc', 'ou_wag']

    # Current line columns grouped by paid ID
    cl_cols = []
    for p in paid_ids:
        cl_cols += [f"{p}_ml", f"{p}_spr", f"{p}_spr_odds", f"{p}_ou", f"{p}_ou_odds"]

    # Build final preferred order
    preferred_order = base_cols + co_cols + op_cols + cl_cols

    # Keep only existing columns
    existing_preferred = [c for c in preferred_order if c in merged.columns]

    # Add any extra columns we didn’t explicitly list
    remaining_cols = [c for c in merged.columns if c not in existing_preferred]

    # Final column order
    final_order = existing_preferred + remaining_cols

    return merged[final_order]
    # return cl_df_ou

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

    meta_df = normalize_keys(meta_df)
    ol_df   = normalize_keys(ol_df)
    cl_df   = normalize_keys(cl_df)
    co_df   = normalize_keys(co_df)

    final_df = merge_all(meta_df, ol_df, cl_df, co_df)

    return final_df

def save_to_github(year, week):
    df = get_weekly_odds(year, week)  # your existing pipeline

    repo_name = "kcdavs/NFL_Perdiction_Model"
    path_in_repo = f"data/odds/{year}/week_{week}.csv"

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError("GITHUB_TOKEN not found in environment")

    g = Github(token)
    repo = g.get_repo(repo_name)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode()

    try:
        contents = repo.get_contents(path_in_repo)
        repo.update_file(
            path_in_repo,
            message=f"Update odds data for {year} week {week}",
            content=csv_bytes,
            sha=contents.sha
        )
        print(f"Updated {path_in_repo} on GitHub.")
    except UnknownObjectException:
        repo.create_file(
            path_in_repo,
            message=f"Add odds data for {year} week {week}",
            content=csv_bytes
        )
        print(f"Created {path_in_repo} on GitHub.")

@app.route("/combined/<int:year>/<int:week>")
def combined_view(year, week):
    try:
        save_to_github(year, week)
        return f"<h3>Saved odds data for {year} week {week} to GitHub</h3>"
    except Exception as e:
        return f"<h3>Error: {str(e)}</h3>"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
