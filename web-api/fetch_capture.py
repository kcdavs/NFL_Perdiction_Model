from playwright.sync_api import sync_playwright

def fetch_odds_json_urls(seid, egid):
    odds_url = f"https://odds.bookmakersreview.com/nfl/?seid={seid}&egid={egid}"
    pattern = "ms-odds-v2/odds-v2-service?query="

    matches = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        def handle_request(request):
            if pattern in request.url:
                matches.append(request.url)

        page.on("request", handle_request)
        page.goto(odds_url, wait_until="networkidle")
        browser.close()

    return matches
