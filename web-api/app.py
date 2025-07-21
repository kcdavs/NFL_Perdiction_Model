import requests
from tabulate import load_and_pivot_acl
import tempfile

@app.route("/tabulate")
def tabulate_from_github():
    try:
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

        from functools import reduce
        final_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
        pdf = final_df.orderBy("eid", "partid").toPandas()

        return Response(pdf.to_csv(index=False), mimetype="text/csv")

    except Exception as e:
        return Response(f"❌ Error: {e}", status=500)

