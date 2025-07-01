# scraper.py

import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://odds.bookmakersreview.com"

def get_week_label(egid, season):
    egid = int(egid); season = int(season)
    postseason = {28:"Wild Card",29:"Divisional",30:"Conference",31:"Super Bowl"}
    if egid in postseason: return postseason[egid]
    if egid==33573 and season>=2021: return "18"
    return str(egid - 9)

def fetch_week_data(egid, season):
    week = get_week_label(egid, season)
    url = f"{BASE}/nfl/?egid={egid}&seid={season}"
    # polite delay
    time.sleep(1)
    resp = requests.get(url, headers={"User-Agent":"MyNFLBot/1.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    rows = []
    for grid in soup.select("div.gridContainer-O4ezT"):
        for table in grid.select("table.tableGrid-2PF6A"):
            pts = table.select("tr.participantRow--z17q")
            for i in range(0, len(pts), 2):
                a, b = pts[i], pts[i+1]
                cells1 = [td.get_text(strip=True) for td in a.select("td")[1:]]
                cells2 = [td.get_text(strip=True) for td in b.select("td")]
                rows.append({
                    "season": season,
                    "week": week,
                    "team1": cells1[0],
                    "spread1": cells1[1],
                    "odds1":   cells1[2],
                    "team2": cells2[0],
                    "spread2": cells2[1],
                    "odds2":   cells2[2],
                })
    return pd.DataFrame(rows)
