"""
NFL Odds Scraper
----------------
Scrapes NFL betting odds from BookmakersReview.com for a given season and week,
then saves the result as a CSV to data/odds/{year}/week_{week}.csv.

The pipeline runs in five steps:
    1. scrape_game_metadata()  — pull team names, scores, and game IDs from HTML
    2. fetch_odds_json()       — fetch opening lines, current lines, and consensus from the API
    3. parse_opening_lines()   — extract the opening line for each game/team from the JSON
       parse_current_lines()   — extract current lines per sportsbook from the JSON
       parse_consensus()       — extract public betting percentages from the JSON
    4. merge_all_data()        — join all four DataFrames into one wide table
    5. save_to_disk()          — write the final CSV

Usage:
    python scrape.py <year> <week>

Example:
    python scrape.py 2024 1
"""

import os
import sys

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# BookmakersReview assigns an internal ID to each NFL season.
# These are required URL parameters when loading the odds page.
SEASON_IDS = {
    2018: 4494,
    2019: 5703,
    2020: 8582,
    2021: 29178,
    2022: 38109,
    2023: 38292,
    2024: 42499,
    2025: 59654,
}

# Each week within a season also has its own internal ID (event group ID).
# Regular season weeks 1–17 follow a simple formula; playoff weeks have fixed IDs.
def _build_event_group_ids() -> dict:
    ids = {}
    for year in range(2018, 2021):
        for week in range(1, 18):
            ids[(year, week)] = 9 + week
        for week, egid in enumerate([28, 29, 30, 31], start=18):
            ids[(year, week)] = egid
    for year in range(2021, 2026):
        for week in range(1, 18):
            ids[(year, week)] = 9 + week
        ids[(year, 18)] = 33573
        for week, egid in enumerate([28, 29, 30, 31], start=19):
            ids[(year, week)] = egid
    return ids

EVENT_GROUP_IDS = _build_event_group_ids()

# BookmakersReview uses internal numeric IDs for each NFL team.
# These are used to join odds data (which references teams by ID) back to team names.
TEAM_IDS = {
    "ARIZONA":       1549,
    "ATLANTA":       1546,
    "BALTIMORE":     1521,
    "BUFFALO":       1526,
    "CAROLINA":      1545,
    "CHICAGO":       1540,
    "CINCINNATI":    1522,
    "CLEVELAND":     1520,
    "DALLAS":        1538,
    "DENVER":        1534,
    "DETROIT":       1539,
    "GREEN BAY":     1542,
    "HOUSTON":       1530,
    "INDIANAPOLIS":  1527,
    "JACKSONVILLE":  1529,
    "KANSAS CITY":   1531,
    "L.A. CHARGERS": 75380,
    "L.A. RAMS":     1550,
    "LAS VEGAS":     1533,
    "MIAMI":         1524,
    "MINNESOTA":     1541,
    "NEW ENGLAND":   1525,
    "NEW ORLEANS":   1543,
    "N.Y. GIANTS":   1535,
    "N.Y. JETS":     1523,
    "OAKLAND":       1533,
    "PHILADELPHIA":  1536,
    "PITTSBURGH":    1519,
    "SAN FRANCISCO": 1547,
    "SEATTLE":       1548,
    "TAMPA BAY":     1544,
    "TENNESSEE":     1528,
    "WASHINGTON":    1537,
}

# The API uses numeric IDs to identify bet market types.
MARKET_MONEYLINE = 83   # straight win/loss bet
MARKET_SPREAD    = 401  # point spread bet
MARKET_TOTAL     = 402  # over/under total points bet

# Over/under totals are not tied to a specific team in the API.
# Instead they use two special participant IDs (one for "over", one for "under").
# We filter to just these when processing total lines.
TOTAL_PARTICIPANT_IDS = {15143, 15144}

# Sportsbook IDs to request current lines from.
# Each number corresponds to a specific book (DraftKings, FanDuel, Pinnacle, etc.).
SPORTSBOOK_IDS = [8, 9, 10, 16, 20, 28, 29, 36, 44, 54, 82, 84, 123, 127, 130]

# Output path (relative to the repo root, where GitHub Actions checks out the code)
OUTPUT_DIR = "data/odds"


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: SCRAPE GAME METADATA FROM HTML
# ─────────────────────────────────────────────────────────────────────────────

def scrape_game_metadata(year: int, week: int) -> pd.DataFrame:
    """
    Scrapes the BookmakersReview NFL odds page for a given season and week.

    The page renders basic game info as static HTML, which we parse with
    BeautifulSoup. The most important field here is `game_id` — a unique
    identifier embedded in anchor tag URLs that we use in Step 2 to fetch
    odds from the API.

    Args:
        year: NFL season year (e.g. 2024)
        week: Week number, including playoffs (e.g. 1–22)

    Returns:
        DataFrame with one row per team per game (so 2 rows per game).
        Columns: game_id, season, week, team, team_id, date, time,
                 score, outcome, rotation
    """
    season_id      = SEASON_IDS.get(year)
    event_group_id = EVENT_GROUP_IDS.get((year, week))

    if season_id is None or event_group_id is None:
        raise ValueError(f"No season or event group ID found for {year} Week {week}")

    url      = f"https://odds.bookmakersreview.com/nfl/?egid={event_group_id}&seid={season_id}"
    response = requests.get(url)
    soup     = BeautifulSoup(response.text, "html.parser")

    rows = []
    for row in soup.find_all("tr", class_="participantRow--z17q"):

        # game_id is buried in an anchor tag's URL as a query parameter
        game_id  = None
        link_tag = row.find("a", class_="link-1Vzcm")
        if link_tag:
            parsed_url = urlparse(link_tag["href"])
            game_id    = parse_qs(parsed_url.query).get("eid", [None])[0]

        date_tag     = row.find("div",  class_="time-3gPvd")
        team_tag     = row.find("div",  class_="participantName-3CqB8")
        score_tag    = row.find("span", class_="score-3EWei")
        rotation_tag = row.find("td",   class_="rotation-3JAfZ")
        outcome_tag  = row.find("span", class_="eventStatusBox-19ZbY")

        rows.append({
            "game_id":  game_id,
            "season":   year,
            "week":     week,
            "team":     team_tag.get_text(strip=True)                    if team_tag     else "",
            "date":     date_tag.find("span").get_text(strip=True)       if date_tag     else "",
            "time":     date_tag.find("p").get_text(strip=True)          if date_tag     else "",
            "score":    score_tag.get_text(strip=True)                   if score_tag    else "",
            "outcome":  outcome_tag.get_text(strip=True)                 if outcome_tag  else "",
            "rotation": rotation_tag.get_text(strip=True)                if rotation_tag else "",
        })

    metadata          = pd.DataFrame(rows)
    metadata["team_id"] = metadata["team"].str.strip().str.upper().map(TEAM_IDS)

    # The HTML only populates date/outcome on the first row of each game pair.
    # Forward-fill within each game so both rows have values.
    metadata[["date", "time", "outcome"]] = (
        metadata.groupby("game_id")[["date", "time", "outcome"]]
        .transform(lambda col: col.replace("", pd.NA).ffill().bfill())
    )

    return metadata


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: FETCH RAW ODDS FROM THE API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_odds_json(game_ids: list) -> dict:
    """
    Fetches opening lines, current lines, and consensus betting percentages
    from the BookmakersReview API for all games in a given week.

    The site loads odds data dynamically via this internal API — we replicate
    the same request the browser makes (found by inspecting the Network tab
    in browser devtools). One call returns all three data types for all games.

    Args:
        game_ids: List of game ID strings from scrape_game_metadata()

    Returns:
        Raw JSON response as a Python dict with three keys under data{}:
            A_OL — opening lines (one sportsbook, the opener)
            A_CL — current lines (all sportsbooks)
            A_CO — consensus public betting percentages
    """
    ids    = ",".join(game_ids)
    books  = ",".join(str(b) for b in SPORTSBOOK_IDS)
    markets = "[83,401,402]"  # moneyline, spread, total

    query = (
        f"{{ "
        f"A_CL: currentLines(paid: [{books}], eid: [{ids}], mtid: {markets}) "
        f"A_OL: openingLines(paid: 8,          eid: [{ids}], mtid: {markets}) "
        f"A_CO: consensus(                      eid: [{ids}], mtid: {markets}) "
        f"{{ eid mtid boid partid sbid paid lineid wag perc vol tvol sequence tim }} "
        f"}}"
    )

    url = "https://ms.virginia.us-east-1.bookmakersreview.com/ms-odds-v2/odds-v2-service?query=" + query
    headers = {
        "User-Agent":        "Mozilla/5.0",
        "Accept":            "application/json",
        "Referer":           "https://odds.bookmakersreview.com/nfl/",
        "X-Requested-With":  "XMLHttpRequest",
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: PARSE THE RAW JSON INTO DATAFRAMES
# ─────────────────────────────────────────────────────────────────────────────

def parse_opening_lines(odds_json: dict) -> pd.DataFrame:
    """
    Extracts opening line data from the raw odds JSON.

    The API returns one record per game/team/market combination (e.g. one row
    for the KC Chiefs moneyline, another for their spread, etc.). We unpack
    each market type into its own column so the result is one row per game/team.

    In the raw API response:
        `ap`  (american price) = the odds, e.g. -110 or +150
        `adj` (adjusted line)  = the point spread or total number, e.g. -3.5 or 44.5

    Args:
        odds_json: Raw API response from fetch_odds_json()

    Returns:
        DataFrame with one row per game/team. Columns:
            game_id, team_id,
            opening_moneyline,
            opening_spread, opening_spread_odds,
            opening_total, opening_total_odds
    """
    records = odds_json["data"].get("A_OL", [])
    if not records:
        return pd.DataFrame(columns=[
            "game_id", "team_id",
            "opening_moneyline",
            "opening_spread", "opening_spread_odds",
            "opening_total",  "opening_total_odds",
        ])

    df = pd.DataFrame(records)

    df["opening_moneyline"]   = df.apply(lambda r: r["ap"]  if r["mtid"] == MARKET_MONEYLINE else None, axis=1)
    df["opening_spread"]      = df.apply(lambda r: r["adj"] if r["mtid"] == MARKET_SPREAD    else None, axis=1)
    df["opening_spread_odds"] = df.apply(lambda r: r["ap"]  if r["mtid"] == MARKET_SPREAD    else None, axis=1)
    df["opening_total"]       = df.apply(lambda r: r["adj"] if r["mtid"] == MARKET_TOTAL     else None, axis=1)
    df["opening_total_odds"]  = df.apply(lambda r: r["ap"]  if r["mtid"] == MARKET_TOTAL     else None, axis=1)

    return (
        df[["eid", "partid",
            "opening_moneyline",
            "opening_spread", "opening_spread_odds",
            "opening_total",  "opening_total_odds"]]
        .rename(columns={"eid": "game_id", "partid": "team_id"})
        .groupby(["game_id", "team_id"], as_index=False)
        .first()
    )


def parse_current_lines(odds_json: dict) -> pd.DataFrame:
    """
    Extracts current line data from the raw odds JSON, broken out by sportsbook.

    Unlike opening lines (one book), current lines come from every sportsbook
    we requested. We pivot the data so there is one row per game/team, with a
    group of columns for each sportsbook:
        {book_id}_moneyline, {book_id}_spread, {book_id}_spread_odds,
        {book_id}_total, {book_id}_total_odds

    Args:
        odds_json: Raw API response from fetch_odds_json()

    Returns:
        DataFrame with one row per game/team and sportsbook columns grouped by book ID.
    """
    records = odds_json["data"].get("A_CL", [])
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    df["moneyline"]   = df.apply(lambda r: r["ap"]  if r["mtid"] == MARKET_MONEYLINE else None, axis=1)
    df["spread"]      = df.apply(lambda r: r["adj"] if r["mtid"] == MARKET_SPREAD    else None, axis=1)
    df["spread_odds"] = df.apply(lambda r: r["ap"]  if r["mtid"] == MARKET_SPREAD    else None, axis=1)
    df["total"]       = df.apply(lambda r: r["adj"] if r["mtid"] == MARKET_TOTAL     else None, axis=1)
    df["total_odds"]  = df.apply(lambda r: r["ap"]  if r["mtid"] == MARKET_TOTAL     else None, axis=1)

    # Collapse to one row per (game, team, sportsbook), then pivot sportsbooks into columns
    grouped = (
        df.groupby(["eid", "partid", "paid"])
        [["moneyline", "spread", "spread_odds", "total", "total_odds"]]
        .first()
        .reset_index()
    )

    pivoted = grouped.pivot(index=["eid", "partid"], columns="paid")
    pivoted.columns = [f"{book_id}_{metric}" for metric, book_id in pivoted.columns]
    pivoted = pivoted.reset_index().rename(columns={"eid": "game_id", "partid": "team_id"})

    return pivoted


def parse_consensus(odds_json: dict) -> pd.DataFrame:
    """
    Extracts public betting consensus data from the raw odds JSON.

    Consensus data shows how the public is betting — what percentage of bets
    and what percentage of dollars are on each side. This is a market-wide
    figure (not specific to any one sportsbook).

    In the raw API response:
        `perc` = percentage of bets placed on this side
        `wag`  = percentage of dollars wagered on this side

    Args:
        odds_json: Raw API response from fetch_odds_json()

    Returns:
        DataFrame with one row per game/team. Columns:
            game_id, team_id,
            moneyline_bet_pct, moneyline_dollar_pct,
            spread_bet_pct,    spread_dollar_pct,
            total_bet_pct,     total_dollar_pct
    """
    records = odds_json["data"].get("A_CO", [])
    if not records:
        return pd.DataFrame(columns=[
            "game_id", "team_id",
            "moneyline_bet_pct", "moneyline_dollar_pct",
            "spread_bet_pct",    "spread_dollar_pct",
            "total_bet_pct",     "total_dollar_pct",
        ])

    df = pd.DataFrame(records)

    df["moneyline_bet_pct"]    = df.apply(lambda r: r["perc"] if r["mtid"] == MARKET_MONEYLINE else None, axis=1)
    df["moneyline_dollar_pct"] = df.apply(lambda r: r["wag"]  if r["mtid"] == MARKET_MONEYLINE else None, axis=1)
    df["spread_bet_pct"]       = df.apply(lambda r: r["perc"] if r["mtid"] == MARKET_SPREAD    else None, axis=1)
    df["spread_dollar_pct"]    = df.apply(lambda r: r["wag"]  if r["mtid"] == MARKET_SPREAD    else None, axis=1)
    df["total_bet_pct"]        = df.apply(lambda r: r["perc"] if r["mtid"] == MARKET_TOTAL     else None, axis=1)
    df["total_dollar_pct"]     = df.apply(lambda r: r["wag"]  if r["mtid"] == MARKET_TOTAL     else None, axis=1)

    return (
        df[["eid", "partid",
            "moneyline_bet_pct", "moneyline_dollar_pct",
            "spread_bet_pct",    "spread_dollar_pct",
            "total_bet_pct",     "total_dollar_pct"]]
        .rename(columns={"eid": "game_id", "partid": "team_id"})
        .groupby(["game_id", "team_id"])
        .first()
        .reset_index()
    )


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: MERGE EVERYTHING INTO ONE TABLE
# ─────────────────────────────────────────────────────────────────────────────

def merge_all_data(
    metadata:      pd.DataFrame,
    opening_lines: pd.DataFrame,
    current_lines: pd.DataFrame,
    consensus:     pd.DataFrame,
) -> pd.DataFrame:
    """
    Joins metadata, opening lines, current lines, and consensus into one wide table.

    Most columns join straightforwardly on (game_id, team_id). Totals (over/under)
    are the exception: the API does not attach total lines to a specific team.
    Instead it uses two special participant IDs (15143 and 15144) for the over
    and under sides. To match each total row to the right game row, we use
    rotation number parity (odd vs even) as a proxy — home and away teams always
    have consecutive rotation numbers, so odd/even reliably identifies the pairing.

    Args:
        metadata:      Output of scrape_game_metadata()
        opening_lines: Output of parse_opening_lines()
        current_lines: Output of parse_current_lines()
        consensus:     Output of parse_consensus()

    Returns:
        Merged DataFrame — one row per team per game, all columns combined.
    """

    # Ensure join keys are consistent strings across all DataFrames
    for df in [metadata, opening_lines, current_lines, consensus]:
        for col in ["game_id", "team_id"]:
            if col in df.columns:
                df[col] = df[col].astype(str)

    # Rotation numbers alternate odd/even between the two teams in each game.
    # We use this parity to match total lines (which have no team ID) to the right row.
    metadata["rotation"]        = pd.to_numeric(metadata["rotation"], errors="coerce")
    metadata["rotation_parity"] = metadata["rotation"] % 2

    # ── Separate total columns from moneyline/spread columns ──────────────────
    # Totals join on rotation_parity; everything else joins on team_id directly.

    def split_totals(df: pd.DataFrame):
        total_cols  = [c for c in df.columns if "total" in c]
        base_cols   = ["game_id", "team_id"]
        non_totals  = df[[c for c in df.columns if c not in total_cols]].copy()
        totals      = df[base_cols + total_cols].copy()
        return non_totals, totals

    opening_non_total, opening_totals   = split_totals(opening_lines)
    current_non_total, current_totals   = split_totals(current_lines)
    consensus_non_total, consensus_totals = split_totals(consensus)

    # Add rotation_parity to each totals DataFrame so we can join on it
    for totals_df in [opening_totals, current_totals, consensus_totals]:
        totals_df["team_id"]         = pd.to_numeric(totals_df["team_id"], errors="coerce")
        totals_df["rotation_parity"] = totals_df["team_id"] % 2

    # Keep only the two special total participant IDs
    opening_totals  = opening_totals[opening_totals["team_id"].isin(TOTAL_PARTICIPANT_IDS)]
    current_totals  = current_totals[current_totals["team_id"].isin(TOTAL_PARTICIPANT_IDS)]
    consensus_totals = consensus_totals[consensus_totals["team_id"].isin(TOTAL_PARTICIPANT_IDS)]

    # ── Merge moneyline/spread columns (standard join on game_id + team_id) ──
    merged = metadata.copy()
    merged = merged.merge(opening_non_total,   on=["game_id", "team_id"], how="left")
    merged = merged.merge(current_non_total,   on=["game_id", "team_id"], how="left")
    merged = merged.merge(consensus_non_total, on=["game_id", "team_id"], how="left")

    # ── Merge total columns (join on game_id + rotation_parity) ──────────────
    merged = merged.merge(opening_totals.drop(columns="team_id"),   on=["game_id", "rotation_parity"], how="left")
    merged = merged.merge(current_totals.drop(columns="team_id"),   on=["game_id", "rotation_parity"], how="left")
    merged = merged.merge(consensus_totals.drop(columns="team_id"), on=["game_id", "rotation_parity"], how="left")

    merged.drop(columns="rotation_parity", inplace=True)

    # ── Order columns for readability ─────────────────────────────────────────
    meta_cols      = ["game_id", "team_id", "season", "week", "team", "date", "time", "score", "outcome", "rotation"]
    opening_cols   = ["opening_moneyline", "opening_spread", "opening_spread_odds", "opening_total", "opening_total_odds"]
    consensus_cols = ["moneyline_bet_pct", "moneyline_dollar_pct", "spread_bet_pct", "spread_dollar_pct", "total_bet_pct", "total_dollar_pct"]

    book_ids     = sorted({int(c.split("_")[0]) for c in merged.columns if c.split("_")[0].isdigit()})
    current_cols = [f"{b}_{m}" for b in book_ids for m in ["moneyline", "spread", "spread_odds", "total", "total_odds"]]

    preferred = meta_cols + opening_cols + consensus_cols + current_cols
    ordered   = [c for c in preferred if c in merged.columns]
    remaining = [c for c in merged.columns if c not in ordered]

    return merged[ordered + remaining]


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: SAVE TO DISK
# ─────────────────────────────────────────────────────────────────────────────

def save_to_disk(year: int, week: int, df: pd.DataFrame) -> str:
    """
    Saves the final DataFrame as a CSV to data/odds/{year}/week_{week}.csv.

    This writes to the local filesystem. When running inside GitHub Actions,
    the workflow then handles the git commit and push to the repository.

    Args:
        year: NFL season year
        week: Week number
        df:   Final merged DataFrame from merge_all_data()

    Returns:
        The file path where the CSV was saved.
    """
    output_path = os.path.join(OUTPUT_DIR, str(year), f"week_{week}.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}  ({len(df)} rows, {len(df.columns)} columns)")
    return output_path


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def build_weekly_odds(year: int, week: int) -> pd.DataFrame:
    """
    Runs the full scraping pipeline for one NFL week and returns the final table.

    Calls each step in order, passing the output of one into the next:
        scrape_game_metadata -> fetch_odds_json -> parse_* -> merge_all_data

    Args:
        year: NFL season year (e.g. 2024)
        week: Week number (1–22, including playoffs)

    Returns:
        Final merged DataFrame — one row per team per game.
    """
    print(f"[1/4] Scraping game metadata for {year} Week {week}...")
    metadata = scrape_game_metadata(year, week)

    game_ids = metadata["game_id"].dropna().unique().tolist()
    print(f"[2/4] Found {len(game_ids)} games. Fetching odds from API...")
    odds_json = fetch_odds_json(game_ids)

    print("[3/4] Parsing opening lines, current lines, and consensus...")
    opening_lines = parse_opening_lines(odds_json)
    current_lines = parse_current_lines(odds_json)
    consensus     = parse_consensus(odds_json)

    print("[4/4] Merging all data...")
    return merge_all_data(metadata, opening_lines, current_lines, consensus)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = [a for a in sys.argv[1:] if a != "--preview"]
    preview = "--preview" in sys.argv

    if len(args) != 2:
        print("Usage:   python scrape.py <year> <week> [--preview]")
        print("Example: python scrape.py 2024 1")
        print("         python scrape.py 2024 1 --preview")
        sys.exit(1)

    year = int(args[0])
    week = int(args[1])

    df = build_weekly_odds(year, week)

    if preview:
        cols = ["team", "date", "score", "outcome", "opening_spread", "opening_moneyline", "opening_total"]
        print(df[cols].to_string(index=False))
    else:
        save_to_disk(year, week, df)


if __name__ == "__main__":
    main()
