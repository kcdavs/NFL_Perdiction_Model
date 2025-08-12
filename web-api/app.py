from flask import Flask, Response
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse, parse_qs, quote_plus
import json
import tempfile
import os

app = Flask(__name__)

SEID_MAP = {
    2018: 4494,
    2019: 5703,
    2020: 8582,
    2021: 29178,
    2022: 38109,
    2023: 38292,
    2024: 42499,
    2025: 59654
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
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    metadata = []
    for row in soup.find_all("tr", class_="participantRow--z17q"):
        eid = None
        eid_tag = row.find("a", class_="link-1Vzcm")
        if eid_tag and eid_tag.has_attr("href"):
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


# Books: keep your working set/order (no need to include dupes unless you want)
PAID = [8,9,10,16,20,28,29,36,44,54,82,123,127,130]
BASE = "https://ms.production-us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service"

def _dedupe_eids_preserve_order(eids):
    seen = set(); out = []
    for e in eids:
        s = str(e)
        if s.isdigit() and s not in seen:
            seen.add(s); out.append(s)
    return out

def get_json_df(eids, label):
    # Build the exact query shape you wanted: aliases + mtid arrays + one selection at end
    eids = _dedupe_eids_preserve_order(eids)
    eid_list = ", ".join(eids)
    paid_list = ", ".join(str(p) for p in PAID)
    # Include mtid in fields so we can split spreads vs moneylines
    fields = "eid mtid boid partid sbid paid lineid wag perc vol tvol sequence tim adj ap"

    query = (
        "{ "
        f"A_BL: bestLines(catid: 338 eid: [{eid_list}] mtid: [83, 401]) "
        f"A_CL: currentLines(paid: [{paid_list}], eid: [{eid_list}], mtid: [83, 401]) "
        f"A_OL: openingLines(paid: 8, eid: [{eid_list}], mtid: [83, 401]) "
        f"A_CO: consensus(eid: [{eid_list}], mtid: [83, 401]) "
        f"{{ {fields} }} "
        "maxSequences { linesMaxSequence } "
        "}"
    )

    # Encode query in the URL (matches the style you showed/pasteable)
    url = f"{BASE}?query={quote_plus(query)}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://odds.bookmakersreview.com/nfl/",
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()

    # Save then parse (keeps the rest of your flow)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        tmp.write(resp.text)
        tmp_path = tmp.name

    return load_and_pivot_mixed(tmp_path, label)


def load_and_pivot_mixed(filepath, label):
    team_map = {
        1536: "Philadelphia", 1546: "Atlanta", 1541: "Minnesota", 1547: "San Francisco",
        1525: "New England", 1530: "Houston", 1521: "Baltimore", 1526: "Buffalo",
        1529: "Jacksonville", 1535: "N.Y. Giants", 1527: "Indianapolis", 1522: "Cincinnati",
        1531: "Kansas City", 75380: "L.A. Chargers", 1543: "New Orleans", 1544: "Tampa Bay",
        1523: "N.Y. Jets", 1539: "Detroit", 1540: "Chicago", 1542: "Green Bay",
        1550: "L.A. Rams", 1538: "Dallas", 1545: "Carolina",
        1534: "Denver", 1548: "Seattle", 1537: "Washington", 1549: "Arizona",
        1524: "Miami", 1528: "Tennessee", 1519: "Pittsburgh", 1520: "Cleveland"
    }

    with open(filepath, "r") as f:
        data = json.load(f)

    # Combined arrays (both mtids in each)
    cl = pd.DataFrame(data.get("data", {}).get("A_CL", []))
    co = pd.DataFrame(data.get("data", {}).get("A_CO", []))
    ol = pd.DataFrame(data.get("data", {}).get("A_OL", []))

    if cl.empty or not {"eid","partid","paid","ap","mtid"}.issubset(cl.columns):
        # Return minimal frame so route still renders CSV
        return pd.DataFrame(columns=["eid", "partid", "team"])

    SPREAD_MID = 401
    MONEYLINE_MID = 83

    # Spreads: adj_* and ap_*
    cl_spread = cl[cl["mtid"] == SPREAD_MID].copy()
    spread_adj = spread_ap = None
    if not cl_spread.empty and cl_spread["adj"].notna().any():
        spread_adj = cl_spread.pivot_table(index=["eid","partid"], columns="paid", values="adj").add_prefix("adj_")
    if not cl_spread.empty and cl_spread["ap"].notna().any():
        spread_ap = cl_spread.pivot_table(index=["eid","partid"], columns="paid", values="ap").add_prefix("ap_")

    # Moneylines: ap only -> ml_<paid>
    cl_ml = cl[cl["mtid"] == MONEYLINE_MID].copy()
    ml_ap = None
    if not cl_ml.empty and cl_ml["ap"].notna().any():
        ml_ap = cl_ml.pivot_table(index=["eid","partid"], columns="paid", values="ap").add_prefix("ml_")

    # Join pivots
    pieces = [p for p in [spread_adj, spread_ap, ml_ap] if p is not None]
    if not pieces:
        pivot_df = pd.DataFrame(columns=["eid","partid"])
    else:
        pivot_df = pieces[0]
        for p in pieces[1:]:
            pivot_df = pivot_df.join(p, how="outer")
    pivot_df = pivot_df.reset_index()

    # Consensus: keep spread perc (rename to 'perc' to match your prior CSV)
    if not co.empty and {"eid","partid","perc","mtid"}.issubset(co.columns):
        co_spread = co[co["mtid"] == SPREAD_MID][["eid","partid","perc"]].drop_duplicates()
        pivot_df = pivot_df.merge(co_spread, on=["eid","partid"], how="left")

    # Opening: keep spread opening_adj/opening_ap as before
    if not ol.empty and {"eid","partid","adj","ap","mtid"}.issubset(ol.columns):
        ol_spread = ol[ol["mtid"] == SPREAD_MID][["eid","partid","adj","ap"]].copy()
        ol_spread.columns = ["eid","partid","opening_adj","opening_ap"]
        pivot_df = pivot_df.merge(ol_spread, on=["eid","partid"], how="left")

    pivot_df["team"] = pivot_df["partid"].map(team_map)
    pivot_df["eid"] = pivot_df["eid"].astype(str)
    return pivot_df


@app.route("/combined/<int:year>/<int:week>")
def combined_view(year, week):
    import traceback
    try:
        metadata = extract_metadata(year, week)
        meta_df = pd.DataFrame(metadata)

        if "team" not in meta_df.columns:
            return Response("❌ Error: 'team' column missing in metadata", status=500, mimetype="text/plain")

        # partid reverse map (handles OAK/LV)
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

        meta_df["partid"] = meta_df["team"].fillna("").str.strip().str.upper().map(reverse_team_map)

        # Build clean, de-duplicated EID list
        eids = [m["eid"] for m in metadata if m.get("eid")]
        eids = _dedupe_eids_preserve_order(eids)

        json_df = get_json_df(eids, label=f"{year}_week{week}")
        if json_df.empty:
            return Response("eid,partid,team\n", mimetype="text/csv")

        json_df["eid"] = json_df["eid"].astype(str)
        json_df = json_df[pd.to_numeric(json_df["partid"], errors="coerce").notna()].copy()
        json_df["partid"] = json_df["partid"].astype(int)

        merged = pd.merge(meta_df, json_df, on=["eid", "partid"], how="left")

        # Fix team column after merge (handle suffixes)
        if "team_x" in merged.columns and "team_y" in merged.columns:
            merged = merged.rename(columns={"team_x": "team"}).drop(columns=["team_y"])
        elif "team" not in merged.columns:
            merged["team"] = meta_df["team"]

        # Return CSV directly to the browser
        csv = merged.to_csv(index=False)
        return Response(csv, mimetype="text/csv")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"Error processing {year} week {week}:\n{tb}")
        return Response(f"❌ Error: {e}\n\n{tb}", status=500, mimetype="text/plain")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
