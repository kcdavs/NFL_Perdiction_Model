from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time

def scrape_bmr_spread_with_selenium(url):
    # Set up Selenium options
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())  # Optional: specify path to chromedriver here if needed

    # Launch browser
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)

    # Wait for at least one gridContainer to appear
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CLASS_NAME, "gridContainer-O4ezT"))
    )

    # Let everything fully render (odds, logos, etc.)
    time.sleep(2)

    # Get page source and pass to BeautifulSoup
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    driver.quit()

    # Parse HTML
    all_data = []
    grid_containers = soup.find_all("div", class_="gridContainer-O4ezT")

    for grid in grid_containers:
        tables = grid.find_all("table", class_="tableGrid-2PF6A")

        for table in tables:
            rows = table.find_all("tr", class_="participantRow--z17q")

            # Loop through rows two at a time (each game has 2 rows: team1 and team2)
            for i in range(0, len(rows), 2):
                row1 = rows[i]
                row2 = rows[i + 1]

                time_td = row1.find("td", class_="timeContainer-3yNjf")
                game_time = time_td.get_text(strip=True) if time_td else "-"

                def extract_row_data(row, game_time, skip_first_td=False):
                    cells = row.find_all("td")
                    if skip_first_td:
                        cells = cells[1:]  # skip the time cell
                    row_data = [game_time]  # first col is time for both rows

                    for td in cells:
                        span = td.find("span", class_="odd-2T5by")
                        if span:
                            row_data.append(span.get_text(strip=True))
                        else:
                            row_data.append(td.get_text(strip=True))

                    return row_data
                  
            all_data.append(extract_row_data(row1, game_time, skip_first_td=True))
            all_data.append(extract_row_data(row2, game_time, skip_first_td=False))

    # Normalize lengths
    max_len = max(len(row) for row in all_data)
    for row in all_data:
        while len(row) < max_len:
            row.append("-")

    # Create DataFrame
    columns = [f"col{i+1}" for i in range(max_len)]
    df = pd.DataFrame(all_data, columns=columns)

    return df


url = "https://odds.bookmakersreview.com/nfl/?egid=10&seid=42499"
df = scrape_bmr_spread_with_selenium(url)
print(df.head(10))
