# test_soup.py

import requests
from bs4 import BeautifulSoup

def main():
    url = "https://odds.bookmakersreview.com/nfl/?egid=10&seid=4494"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        )
    }

    print(f"Fetching {url}…")
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    html = soup.prettify()

    # 1) Print to console
    print(html)

    # 2) —or— write to a local file so you can open it in your browser:
    with open("page.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("Wrote full HTML to page.html")

if __name__ == "__main__":
    main()
