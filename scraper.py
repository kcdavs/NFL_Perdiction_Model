# scraper.py

import pandas as pd
import requests
import time
from bs4 import BeautifulSoup

# —————————————————————————————
# your existing helpers, unchanged
def get_week_label(egid, season):
    egid = int(egid); season = int(season)
    postseason = {28: "Wild Card", 29: "Divisional", 30: "Conference", 31: "Super Bowl"}
    if egid in postseason: 
        return postseason[egid]
    if egid == 33573 and season >= 2021: 
        return "18"
    return str(egid - 9)

def convert_spread(spread_str):
    if pd.isna(spread_str) or spread_str in ["-", ""]:
        return None
    spread_str = spread_str.replace("½", ".5").replace("Â", "")
    if spread_str.startswith("+"):
        spread_str = spread_str[1:]
    try:
        return float(spread_str)
    except ValueError:
        return None
# —————————————————————————————


def scrape_bmr_spread_requests(url, season, week):
    """
    Fetch the page via HTTP, parse with BeautifulSoup,
    and extract exactly the same rows & columns as your Selenium version.
    """
    print(f"🟢 Fetching (requests) season {season}, week {week}")
    # Use a desktop UA so the site gives you the full grid
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        )
    }
    # polite delay
    time.sleep(1)
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    all_data = []

    # exactly your old loops, swapped to requests/BS4
    grid_containers = soup.find_all("div", class_="gridContainer-O4ezT")
    for grid in grid_containers:
        tables = grid.find_all("table", class_="tableGrid-2PF6A")
        for table in tables:
            rows = table.find_all("tr", class_="participantRow--z17q")
            for i in range(0, len(rows), 2):
                row1, row2 = rows[i], rows[i+1]

                # pull out the time/outcome cells
                time_td = row1.find("td", class_="timeContainer-3yNjf")
                outcome = date = time_str = "-"
                if time_td:
                    outcome_tag = time_td.find("span", class_="eventStatusBox-19ZbY")
                    date_tag    = time_td.find("div",  class_="time-3gPvd")
                    if outcome_tag: outcome = outcome_tag.text.strip()
                    if date_tag:
                        ds = date_tag.find("span"); tp = date_tag.find("p")
                        if ds: date = ds.text.strip()
                        if tp: time_str = tp.text.strip()

                def extract_row_data(r, skip_time=False):
                    tds = r.find_all("td")
                    if skip_time:
                        tds = tds[1:]  # drop the time cell
                    row_data = [season, week, outcome, date, time_str]
                    for td in tds:
                        span = td.find("span", class_="odd-2T5by")
                        if span:
                            row_data.append(span.get_text(strip=True))
                        else:
                            row_data.append(td.get_text(strip=True))
                    return row_data

                all_data.append(extract_row_data(row1, skip_time=True))
                all_data.append(extract_row_data(row2, skip_time=False))

    # pad to uniform length
    max_len = max(len(r) for r in all_data)
    for r in all_data:
        while len(r) < max_len:
            r.append("-")

    base_cols = [
        "rotation", "team", "score", "wagers",
        "opener", "BETONLINE", "BOVODA",
        "BookMaker", "BAS", "Heritage",
        "everygame", "JustBET", "bet105","WAGERWEB"
    ]
    # season, week, outcome, date, time + the first (max_len-5) of base_cols
    cols = ["season", "week", "outcome", "date", "time"] \
           + base_cols[: max_len - 5]
    df = pd.DataFrame(all_data, columns=cols
    return df


def scrape_one_week_requests(egid, season_id):
    """Wrapper that returns a DataFrame exactly as scrape_multiple_weeks_for_season did."""
    week_label = get_week_label(egid, season_id)
    url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={season_id}"
    return scrape_bmr_spread_requests(url, season_id, week_label)


# If you still want your multi-week function, just swap in the new one:
def scrape_multiple_weeks_for_season_requests(season_id, season_year):
    all_dfs = []
    egid_map = {eg: str(eg-9) for eg in range(10,27)}
    if season_year>=2021: egid_map[33573]="18"
    egid_map.update({28:"Wild Card",29:"Divisional",30:"Conference",31:"Super Bowl"})

    for egid, week in egid_map.items():
        df = scrape_one_week_requests(egid, season_year)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)
