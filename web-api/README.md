📡 Odds Scraper Overview
This web scraper is designed to extract both static and dynamically loaded sportsbook odds data from odds.bookmakersreview.com for every NFL game, week, and season. The goal is to replicate the data you see on the live site — and persist it week-by-week in a structured, queryable format (CSV stored in GitHub).

🔍 What the Scraper Does
1. Extracts Static HTML Metadata
Using BeautifulSoup, the scraper pulls key information that is rendered directly in the HTML when the page loads:

Team name

Score

Rotation number

Kickoff date and time

Outcome label (e.g. Final, Scheduled)

eid: A crucial game identifier embedded in anchor tags.

2. Identifies the eid for Each Game
The eid (Event ID) is a unique identifier for each game. This value is hidden within anchor tag URLs in the static HTML. By parsing this from each row, we can later pair metadata with dynamic odds data via the site's backend API.

3. Intercepts & Replicates the Fetch Call
The betting odds and line data (spreads, moneyline, public percentage, etc.) are not in the static HTML. They’re fetched dynamically by the site using an API call.
We reverse engineered this API by inspecting the Network tab in browser devtools during page load.

With this knowledge, we can:

Reconstruct the API call with the appropriate eid and other parameters

Retrieve the full odds JSON for all sportsbooks (e.g., BetMGM, DraftKings, FanDuel, etc.)

4. Combines Static and Dynamic Data
The JSON odds data is unpacked and pivoted into a wide format (one row per team per game), then:

Joined with the static metadata using eid and partid (team ID)

Cleaned and reformatted to match the structure of the site

5. Uploads to GitHub
Once the data is combined:

A CSV file is created per year-week (odds/{year}/week{week}.csv)

The file is pushed directly to GitHub via the REST API using a personal access token

🕓 Historical Access
Because the API and HTML structure have been stable over time, this process can be repeated across all seasons from 2018 onward, allowing the creation of a complete historical repository of weekly odds data.
