import os
import traceback
import requests
import tempfile
import pandas as pd
from flask import Flask, Response, request
from tabulate import load_and_pivot_acl  # your Pandas-based function

app = Flask(__name__)

@app.route("/")
def home():
    return (
        "🟢 NFL Odds Scraper Running!\n\n"
        "Visit /tabulate to see tabulated odds using raw URLs from GitHub.\n"
        "Make sure your GitHub repo has: urls/10gameURL.txt and 6gameURL.txt\n"
    )

@app.route("/tabulate")
def tabulate_from_github():
    try:
        # 🔁 Update this to match your GitHub username/repo
        GITHUB_BASE = "https://raw.githubusercontent.com/kcdavs/NFL-Gambling-Addiction-ml/main/urls/"

        files = [("10gameURL.txt", "10games"), ("6gameURL.txt", "6games")]
        dfs = []

        for filename, label in files:
            url = GITHUB_BASE + filename
            raw_url = requests.get(url).text.strip()

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Referer": "https://odds.bookmakersreview.com/nfl/",
                "X-Requested-With": "XMLHttpRequest",
            }
            resp = requests.get(raw_url, headers=headers, timeout=10)
            resp.raise_for_status()

            with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
                tmp.write(resp.text)
                tmp_path = tmp.name

            df = load_and_pivot_acl(tmp_path, label)
            dfs.append(df)

        # ✅ Merge both sets using Pandas
        final_df = pd.concat(dfs, ignore_index=True)
        final_df = final_df.sort_values(["eid", "partid"])

        # ✅ Return as CSV
        return Response(final_df.to_csv(index=False), mimetype="text/csv")

    except Exception as e:
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")

# 🔚 Required for local testing
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
