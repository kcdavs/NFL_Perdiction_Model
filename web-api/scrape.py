import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
import pandas as pd
from flask import Blueprint, jsonify

scrape = Blueprint('scrape', __name__)

@scrape.route('/<int:year>/<int:week>')
def get_weekly_games(year, week):
    url = f"https://www.bookmakersreview.com/sports/nfl/week-{week}-{year}/"
    try:
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "html.parser")

        games_data = []
        game_divs = soup.find_all("div", class_="container-AYhX9")

        for game in game_divs:
            game_info = {}
            game_info["date"] = game.get("data-grid-date", "")

            time_tag = game.find("time")
            game_info["time"] = time_tag.get_text(strip=True) if time_tag else ""

            a_tag = game.find("a", href=True)
            if a_tag and "eid=" in a_tag['href']:
                parsed_url = urlparse(a_tag['href'])
                params = parse_qs(parsed_url.query)
                game_info["eid"] = params.get("eid", [None])[0]
                game_info["egid"] = params.get("egid", [None])[0]
                game_info["seid"] = params.get("seid", [None])[0]
                game_info["game_url"] = a_tag['href']
            else:
                continue

            teams = game.find_all("div", class_="participantName-3CqB8")
            scores = game.find_all("span", class_="score-3EWei")

            if len(teams) == 2:
                game_info["team_1"] = teams[0].get_text(strip=True)
                game_info["team_2"] = teams[1].get_text(strip=True)
            if len(scores) == 2:
                game_info["score_1"] = scores[0].get_text(strip=True)
                game_info["score_2"] = scores[1].get_text(strip=True)

            games_data.append(game_info)

        df_games = pd.DataFrame(games_data)
        return df_games.to_html(index=False, classes="table table-striped")

    except Exception as e:
        return jsonify({"error": str(e)}), 500
