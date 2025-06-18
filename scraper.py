import pandas as pd

def get_week_label(egid, season):
    egid = int(egid)
    season = int(season)

    postseason = {
        28: "Wild Card",
        29: "Divisional",
        30: "Conference",
        31: "Super Bowl"
    }

    if egid in postseason:
        return postseason[egid]
    elif egid == 33573 and season >= 2021:
        return "18"
    else:
        return str(egid - 9)

def convert_spread(spread_str):
    if pd.isna(spread_str) or spread_str in ["-", ""]:
        return None
    spread_str = spread_str.replace("½", ".5").replace("Â", "")
    # Handle signs before the number, e.g., +3.5 or -3.5
    try:
        # Sometimes the string has a plus sign +, remove for float conversion
        if spread_str.startswith("+"):
            spread_str = spread_str[1:]
        return float(spread_str)
    except ValueError:
        return None

def scrape_bmr_spread_with_selenium(driver, url, season, week):
    from bs4 import BeautifulSoup
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time

    print(f"🟢 Currently scraping season {season}, week {week}")
    driver.get(url)
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CLASS_NAME, "gridContainer-O4ezT"))
    )
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    all_data = []
    grid_containers = soup.find_all("div", class_="gridContainer-O4ezT")

    for grid in grid_containers:
        tables = grid.find_all("table", class_="tableGrid-2PF6A")

        for table in tables:
            rows = table.find_all("tr", class_="participantRow--z17q")

            for i in range(0, len(rows), 2):
                row1 = rows[i]
                row2 = rows[i + 1]

                time_td = row1.find("td", class_="timeContainer-3yNjf")
                outcome, date, time_str = "-", "-", "-"

                if time_td:
                    outcome_tag = time_td.find("span", class_="eventStatusBox-19ZbY")
                    date_tag = time_td.find("div", class_="time-3gPvd")
                    if outcome_tag:
                        outcome = outcome_tag.text.strip()
                    if date_tag:
                        date_span = date_tag.find("span")
                        time_p = date_tag.find("p")
                        if date_span:
                            date = date_span.text.strip()
                        if time_p:
                            time_str = time_p.text.strip()

                def extract_row_data(row, skip_time=False):
                    cells = row.find_all("td")
                    if skip_time:
                        cells = cells[1:]
                    row_data = [season, week, outcome, date, time_str]
                    for td in cells:
                        span = td.find("span", class_="odd-2T5by")
                        if span:
                            row_data.append(span.get_text(strip=True))
                        else:
                            row_data.append(td.get_text(strip=True))
                    return row_data

                all_data.append(extract_row_data(row1, skip_time=True))
                all_data.append(extract_row_data(row2, skip_time=False))

    max_len = max(len(row) for row in all_data)
    for row in all_data:
        while len(row) < max_len:
            row.append("-")

    base_columns = [
        "rotation", "team", "score", "wagers", "opener", "BETONLINE", "BOVODA",
        "BookMaker", "BAS", "Heritage", "everygame", "JustBET", "bet105", "WAGERWEB"
    ]
    columns = ["season", "week", "outcome", "date", "time"] + base_columns[:max_len - 5]
    df = pd.DataFrame(all_data, columns=columns)
    return df

def scrape_multiple_weeks_for_season(driver, season_id, season_year):
    all_dfs = []
    egid_week_mapping = {}

    # Weeks 1–17
    for egid in range(10, 27):
        egid_week_mapping[egid] = str(egid - 9)

    # Week 18 only if season >= 2021
    if season_year >= 2021:
        egid_week_mapping[33573] = "18"

    # Postseason weeks
    egid_week_mapping[28] = "Wild Card"
    egid_week_mapping[29] = "Divisional"
    egid_week_mapping[30] = "Conference"
    egid_week_mapping[31] = "Super Bowl"

    for egid, week_label in egid_week_mapping.items():
        url = f"https://odds.bookmakersreview.com/nfl/?egid={egid}&seid={season_id}"
        print(f"\n🔄 Scraping Season {season_year}, Week: {week_label} ({url})")
        try:
            df = scrape_bmr_spread_with_selenium(driver, url, season_year, week_label)
            all_dfs.append(df)
        except Exception as e:
            print(f"❌ Failed on Week {week_label} — Error: {e}")

    if not all_dfs:
        print(f"⚠️ No data scraped for season {season_year} ({season_id})")
        return pd.DataFrame()

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Normalize sportsbook columns (spread + odds splitting)
    books = ["BETONLINE", "BOVODA", "BookMaker", "BAS", "Heritage", "everygame", "JustBET", "bet105", "WAGERWEB"]
    for book in books:
        if book in combined_df.columns:
            spreads = []
            odds = []
            for val in combined_df[book]:
                if isinstance(val, str) and " " in val:
                    parts = val.split(" ", 1)
                    spreads.append(convert_spread(parts[0]))
                    odds.append(parts[1])
                else:
                    spreads.append(convert_spread(val))
                    odds.append(None)
            combined_df[f"{book}_spread"] = spreads
            combined_df[f"{book}_odds"] = odds
            combined_df.drop(columns=[book], inplace=True)

    # Convert opener column as well
    spreads = []
    odds = []
    for val in combined_df["opener"]:
        if isinstance(val, str) and " " in val:
            parts = val.split(" ", 1)
            spreads.append(convert_spread(parts[0]))
            odds.append(parts[1])
        else:
            spreads.append(convert_spread(val))
            odds.append(None)
    combined_df["opener_spread"] = spreads
    combined_df["opener_odds"] = odds
    combined_df.drop(columns=["opener"], inplace=True)

    return combined_df

def scrape_all_seasons(seasons):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager

    options = Options()
    # options.add_argument("--headless")  # Uncomment to run headless
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    all_seasons_df = []

    for season_id, season_year in seasons:
        print(f"\n=== Starting season {season_year} ===")
        df_season = scrape_multiple_weeks_for_season(driver, season_id, season_year)
        if not df_season.empty:
            all_seasons_df.append(df_season)

    driver.quit()

    if not all_seasons_df:
        raise ValueError("No data scraped for any season!")

    combined = pd.concat(all_seasons_df, ignore_index=True)
    combined.to_csv("nfl_spreads_all_seasons.csv", index=False)
    print("\n✅ All seasons data saved to nfl_spreads_all_seasons.csv")

    return combined

if __name__ == "__main__":
    seasons = [
        (4494, 2018),
        (5703, 2019),
        (8582, 2020),
        (29178, 2021),
        (38109, 2022),
        (38292, 2023),
        (42499, 2024),
    ]
    combined_df = scrape_all_seasons(seasons)
    print(combined_df.head(50))
