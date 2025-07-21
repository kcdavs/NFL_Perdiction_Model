import os
import traceback
from flask import Flask, Response, request
from urllib.parse import unquote
import requests
import tempfile
from tabulate import load_and_pivot_acl  # <- uses tabulate.py

app = Flask(__name__)

@app.route("/")
def home():
    return (
        "🟢 NFL Odds Proxy Live!\n\n"
        "Use GET /tabulate-json?url1=<ENCODED_10GAME>&url2=<ENCODED_6GAME>\n\n"
        "Get each encoded URL from Chrome DevTools and paste them into the URL like this:\n\n"
        "https://nfl-gambling-addiction-ml.onrender.com/tabulate-json?\n"
        "url1=<ENCODED_10GAME_URL>&url2=<ENCODED_6GAME_URL>\n"
    )

@app.route("/tabulate-json")
def tabulate_json():
    try:
        url1 = request.args.get("url1", "")
        url2 = request.args.get("url2", "")

        if not url1 and not url2:
            return Response("❌ At least one of `url1` or `url2` must be provided", status=400)

        urls = []
        if url1:
            urls.append((unquote(url1), "10games"))
        if url2:
            urls.append((unquote(url2), "6games"))

        dfs = []
        for url, label in urls:
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Referer": "https://odds.bookmakersreview.com/nfl/",
                "X-Requested-With": "XMLHttpRequest",
            }
            resp = requests.get(url, headers=headers, timeout=10)
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
        tb = traceback.format_exc()
        return Response(f"❌ Error:\n{e}\n\n{tb}", status=500, mimetype="text/plain")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
