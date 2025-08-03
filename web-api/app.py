from flask import Flask, Response
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse, parse_qs
import json
import tempfile
import os
import base64

app = Flask(__name__)

# GitHub upload function
def push_csv_to_github(csv_content, year, week,
                       repo="kcdavs/NFL-Gambling-Addiction-ml",
                       branch="main"):
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise Exception("Missing GITHUB_TOKEN environment variable")
    path = f"data/odds/{year}/week{week:02d}.csv"
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {"Authorization": f"token {token}",
               "Accept": "application/vnd.github+json"}
    resp = requests.get(url, headers=headers)
    sha = resp.json().get("sha") if resp.status_code == 200 else None
    payload = {
        "message": f"Add odds for {year} week {week}",
        "content": base64.b64encode(csv_content.encode()).decode(),
        "branch": branch
    }
    if sha:
        payload["sha"] = sha
    res = requests.put(url, headers=headers, json=payload)
    res.raise_for_status()
    return res.json()

# SEID & EGID lookup
def build_egid_map():
    seid_map = {2018:4494,2019:5703,2020:8582,2021:29178,2022:38109,2023:38292,2024:42499,2025:59654}
    egid_map = {}
    for yr in range(2018,2021):
        for wk in range(1,18): egid_map[(yr,wk)] = 9+wk
        for idx,egid in enumerate([28,29,30,31],start=18): egid_map[(yr,idx)] = egid
    for yr in range(2021,2026):
        for wk in range(1,18): egid_map[(yr,wk)] = 9+wk
        egid_map[(yr,18)] = 33573
        for idx,egid in enumerate([28,29,30,31],start=19): egid_map[(yr,idx)] = egid
    return seid_map, egid_map

SEID_MAP, EGID_MAP = build_egid_map()

USER_AGENT = "Mozilla/5.0"
PAID_IDS = [8,9,10,123,44,29,16,130,54,82,36,20,127,28,84]

# Scrape HTML metadata
def extract_metadata(year, week):
    seid = SEID_MAP.get(year)
    egid = EGID_MAP.get((year, week))
    if not seid or not egid:
        raise ValueError(f"Unknown SEID/EGID for {year} Week {week}")
    url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={seid}"
    res = requests.get(url, headers={"User-Agent":USER_AGENT}, timeout=10)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    rows = []
    for tr in soup.select("tr.participantRow--z17q"):
        eid = None
        a = tr.select_one("a.link-1Vzcm")
        if a:
            eid_val = parse_qs(urlparse(a["href"]).query).get("eid", [None])[0]
            eid = str(eid_val) if eid_val else None
        dt = tr.select_one("div.time-3gPvd")
        date = dt.select_one("span").get_text(strip=True) if dt else ""
        time = dt.select_one("p").get_text(strip=True) if dt else ""
        tag = tr.select_one("div.participantName-3CqB8")
        team = tag.get_text(strip=True) if tag else ""
        sc = tr.select_one("span.score-3EWei")
        score = sc.get_text(strip=True) if sc else ""
        rt = tr.select_one("td.rotation-3JAfZ")
        rotation = rt.get_text(strip=True) if rt else ""
        ot = tr.select_one("span.eventStatusBox-19ZbY")
        outcome = ot.get_text(strip=True) if ot else ""

        rows.append({
            "eid": eid,
            "season": year,
            "week": week,
            "date": date,
            "time": time,
            "team": team,
            "rotation": rotation,
            "score": score,
            "outcome": outcome
        })
    return rows

# Fetch & pivot JSON odds
def get_json_df(eids):
    query = (
        f"{{A_BL:bestLines(catid:338,eid:[{','.join(eids)}],mtid:401) "
        f"A_CL:currentLines(paid:{PAID_IDS},eid:[{','.join(eids)}],mtid:401) "
        f"A_OL:openingLines(paid:8,eid:[{','.join(eids)}],mtid:401) "
        f"A_CO:consensus(eid:[{','.join(eids)}],mtid:401) "
        f"{{eid,partid,paid,adj,ap,perc,opening_adj,opening_ap}}}}"
    )
    url = "https://ms.production-us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service?query=" + query
    resp = requests.get(url, headers={'User-Agent':USER_AGENT}, timeout=10)
    resp.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(mode="w+",suffix=".json",delete=False)
    tmp.write(resp.text)
    tmp.flush()
    tmp.close()
    data = json.load(open(tmp.name))
    cl = pd.DataFrame(data['data']['A_CL'])[['eid','partid','paid','adj','ap']]
    co = pd.DataFrame(data['data']['A_CO'])[['eid','partid','perc']].drop_duplicates()
    ol = pd.DataFrame(data['data']['A_OL'])[['eid','partid','adj','ap']]
    ol.columns = ['eid','partid','opening_adj','opening_ap']
    cl_adj = cl.pivot_table(index=['eid','partid'],columns='paid',values='adj').add_prefix('adj_')
    cl_ap  = cl.pivot_table(index=['eid','partid'],columns='paid',values='ap').add_prefix('ap_')
    df = cl_adj.join(cl_ap).reset_index().merge(co,on=['eid','partid'],how='left').merge(ol,on=['eid','partid'],how='left')
    df['eid'] = df['eid'].astype(str)
    return df

@app.route('/combined/<int:year>/<int:week>')
def combined_view(year, week):
    try:
        meta = extract_metadata(year, week)
        df_meta = pd.DataFrame(meta)
        # dedupe eids and preserve order
        eids = list(dict.fromkeys(r['eid'] for r in meta if r['eid']))
        df_odds = get_json_df(eids)
        merged = pd.merge(df_meta, df_odds, on='eid', how='left')
        csv = merged.to_csv(index=False)
        push_csv_to_github(csv, year, week)
        return Response(f"✅ {year} Week {week} uploaded", mimetype='text/plain')
    except Exception as e:
        return Response(f"❌ Error: {e}", status=500, mimetype='text/plain')

if __name__=='__main__':
    port=int(os.getenv('PORT',3000))
    app.run(host='0.0.0.0',port=port)
