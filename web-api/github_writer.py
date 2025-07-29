import requests
import os
import base64

def push_csv_to_github(csv_content, year, week, repo="kcdavs/NFL-Gambling-Addiction-ml", branch="main"):
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise Exception("Missing GITHUB_TOKEN environment variable")

    filename = f"odds/{year}/week{str(week).zfill(2)}.csv"
    api_url = f"https://api.github.com/repos/{repo}/contents/{filename}"

    # Check if file exists to get the SHA
    response = requests.get(api_url, headers={"Authorization": f"token {token}"})
    sha = response.json().get("sha") if response.status_code == 200 else None

    message = f"Add odds for {year} week {week}"
    encoded_content = base64.b64encode(csv_content.encode("utf-8")).decode("utf-8")

    payload = {
        "message": message,
        "content": encoded_content,
        "branch": branch
    }
    if sha:
        payload["sha"] = sha  # Overwrite existing file

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }

    res = requests.put(api_url, json=payload, headers=headers)
    if res.status_code not in [200, 201]:
        raise Exception(f"GitHub upload failed: {res.status_code} – {res.text}")
    return res.json()
