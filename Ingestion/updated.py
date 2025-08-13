import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse, parse_qs
import json
import tempfile

# Mapping values
SEID_MAP = {
    2018: 4494, 2019: 5703, 2020: 8582, 2021: 29178,
    2022: 38109, 2023: 38292, 2024: 42499, 2025: 59654
}

EGID_MAP = {}
for year in range(2018, 2021):
    for i in range(1, 18):
        EGID_MAP[(year, i)] = 9 + i
    for i, egid in enumerate([28, 29, 30, 31], start=18):
        EGID_MAP[(year, i)] = egid
for year in range(2021, 2026):
    for i in range(1, 18):
        EGID_MAP[(year, i)] = 9 + i
    EGID_MAP[(year, 18)] = 33573
    for i, egid in enumerate([28, 29, 30, 31], start=19):
        EGID_MAP[(year, i)] = egid

def extract_metadata(year, week):
    seid = SEID_MAP.get(year)
    egid = EGID_MAP.get((year, week))
    if seid is None or egid is None:
        raise ValueError(f"Unknown SEID or EGID for {year} Week {week}")

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
            "season": year,
            "week": week,
            "date": date,
            "time": time,
            "team": team,
            "score": score,
            "outcome": outcome
        })

    return metadata

def get_json_df(eids):
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
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        tmp.write(resp.text)
        tmp_path = tmp.name

    return url

def get_combined_df(year, week):
    metadata = extract_metadata(year, week)
    meta_df = pd.DataFrame(metadata)

    eids = [m["eid"] for m in metadata if m["eid"] is not None]

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

    json_df = get_json_df(eids)

    return json_df

def parse_odds_json(url):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://odds.bookmakersreview.com/nfl/",
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    ol_data = data["data"].get("A_OL", [])
    ol_df = pd.DataFrame(ol_data)

    # Select only relevant columns
    ol_df = ol_df[['mtid', 'eid', 'partid', 'ap', 'adj']]

    # Create separate columns for moneyline and spread odds with 'op_' prefix
    ol_df['op_ml_odds'] = ol_df.apply(lambda row: row['ap'] if row['mtid'] == 83 else None, axis=1)
    ol_df['op_spr'] = ol_df.apply(lambda row: row['adj'] if row['mtid'] == 401 else None, axis=1)
    ol_df['op_spr_odds'] = ol_df.apply(lambda row: row['ap'] if row['mtid'] == 401 else None, axis=1)

    # Drop unneeded columns
    ol_df = ol_df.drop(columns=['ap', 'adj', 'mtid'])

    # Group by eid and partid to combine the different odds columns into one row
    result = ol_df.groupby(['eid', 'partid'], as_index=False).agg({
        'op_ml_odds': 'first',
        'op_spr': 'first',
        'op_spr_odds': 'first',
    })

    return result

def parse_cl_json(url):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://odds.bookmakersreview.com/nfl/",
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    cl_data = data["data"].get("A_CL", [])
    cl_df = pd.DataFrame(cl_data)

    # Keep only needed columns
    cl_df = cl_df[['mtid', 'eid', 'partid', 'paid', 'ap', 'adj']]

    # ml_odds = ap when mtid == 83
    cl_df['ml'] = cl_df.apply(lambda row: row['ap'] if row['mtid'] == 83 else None, axis=1)
    # spr_odds = ap when mtid == 401
    cl_df['spr_odds'] = cl_df.apply(lambda row: row['ap'] if row['mtid'] == 401 else None, axis=1)
    # spr = adj when mtid == 401
    cl_df['spr'] = cl_df.apply(lambda row: row['adj'] if row['mtid'] == 401 else None, axis=1)

    # Group by eid, partid, paid and aggregate ml, spr_odds, spr taking first non-null
    grouped = cl_df.groupby(['eid', 'partid', 'paid']).agg({
        'ml': 'first',
        'spr': 'first',
        'spr_odds': 'first'
    }).reset_index()

    # Pivot so paid becomes prefix for columns
    pivoted = grouped.pivot(index=['eid', 'partid'], columns='paid')

    # Flatten multiindex columns: e.g. ('ml', 8) -> '8_ml'
    pivoted.columns = [f"{paid}_{metric}" for metric, paid in pivoted.columns]

    pivoted.reset_index(inplace=True)

    return pivoted

def parse_co_json(url):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://odds.bookmakersreview.com/nfl/",
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    co_data = data["data"].get("A_CO", [])
    co_df = pd.DataFrame(co_data)

    # Keep only needed columns
    co_df = co_df[['eid', 'partid', 'mtid', 'perc', 'wag']]

    # Rename perc and wag based on mtid
    co_df['ml_perc'] = co_df.apply(lambda r: r['perc'] if r['mtid'] == 83 else None, axis=1)
    co_df['ml_wag'] = co_df.apply(lambda r: r['wag'] if r['mtid'] == 83 else None, axis=1)
    co_df['spr_perc'] = co_df.apply(lambda r: r['perc'] if r['mtid'] == 401 else None, axis=1)
    co_df['spr_wag'] = co_df.apply(lambda r: r['wag'] if r['mtid'] == 401 else None, axis=1)

    # Group by eid and partid to combine rows for different mtids
    grouped = co_df.groupby(['eid', 'partid']).agg({
        'ml_perc': 'first',
        'ml_wag': 'first',
        'spr_perc': 'first',
        'spr_wag': 'first',
    }).reset_index()

    return grouped

def merge_all(ol_df, cl_df, co_df):
    # Merge CL and OL data
    merged = ol_df.merge(cl_df, on=['eid', 'partid'], how='outer')

    # Merge transformed CO data
    merged = co_df.merge(merged, on=['eid', 'partid'], how='outer')

    # Reorder columns:
    cols = merged.columns.tolist()

    # Columns starting with 'op_' (OL data)
    op_cols = [c for c in cols if c.startswith('op_')]
    # Columns that belong to CL (everything except eid, partid, ml_perc, ml_wag, spr_perc, spr_wag, and op_)
    exclude = ['eid', 'partid', 'ml_perc', 'ml_wag', 'spr_perc', 'spr_wag'] + op_cols
    cl_cols = [c for c in cols if c not in exclude]

    ordered_cols = ['eid', 'partid', 'ml_perc', 'ml_wag', 'spr_perc', 'spr_wag'] + op_cols + cl_cols

    return merged[ordered_cols]

# Usage example
url = get_combined_df(2018, 1)
ol_df = parse_odds_json(url)
cl_df = parse_cl_json(url)
co_df = parse_co_json(url)

final_df = merge_all(ol_df, cl_df, co_df)
final_df
