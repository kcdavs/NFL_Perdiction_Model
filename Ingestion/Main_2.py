# -*- coding: utf-8 -*-
"""
Combine all weekly odds CSVs (2018–2024) from:
  https://github.com/kcdavs/NFL_Perdiction_Model/tree/main/data/odds/<year>/

Output:
  - Local file: odds_combined_2018_2024.csv
  - (Optional) Push to GitHub: Model/model_input.csv (set GITHUB_TOKEN)
"""

import os
import re
import base64
import pandas as pd
import requests
from io import StringIO

# ----------------------------
# Config
# ----------------------------
OWNER = "kcdavs"
REPO  = "NFL_Perdiction_Model"

# New layout (note the leading "data/")
API_BASE = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/data/odds"
RAW_BASE = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/main/data/odds"

YEARS = [str(y) for y in range(2018, 2025)]  # 2018..2024 inclusive

# Output locations
LOCAL_OUT_CSV = "odds_combined_2018_2024.csv"
GITHUB_OUT_PATH = "Model/model_input.csv"  # change if you want a different target

# Optional: use a GitHub token for higher rate limits / push
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()

# ----------------------------
# Helpers
# ----------------------------
session = requests.Session()
if GITHUB_TOKEN:
    session.headers.update({"Authorization": f"token {GITHUB_TOKEN}"})

def gh_get(url):
    r = session.get(url)
    r.raise_for_status()
    return r.json()

def parse_week_from_filename(name: str) -> int:
    """
    Extract week number from variations like:
      week_1.csv, week1.csv, week01.csv, Week-17.csv
    """
    m = re.search(r'week[_\-]?(\d+)', name, re.IGNORECASE)
    if not m:
        raise ValueError(f"Could not parse week from filename: {name}")
    return int(m.group(1))

def read_week_csv(year: str, filename: str) -> pd.DataFrame:
    raw_url = f"{RAW_BASE}/{year}/{filename}"
    text = session.get(raw_url)
    text.raise_for_status()
    df = pd.read_csv(StringIO(text.text))
    # Ensure season/week exist (fill if missing)
    if "season" not in df.columns:
        df["season"] = int(year)
    if "week" not in df.columns:
        df["week"] = parse_week_from_filename(filename)
    return df

def safe_concat(dfs):
    if not dfs:
        return pd.DataFrame()
    # Union of columns across all weeks/years
    out = pd.concat(dfs, ignore_index=True, sort=False)
    # Optional: drop complete duplicate rows if they exist
    out = out.drop_duplicates()
    return out

def push_to_github_csv(repo_owner, repo_name, path_in_repo, csv_df, message="Upload combined odds CSV", branch="main"):
    """
    Create or update a file in GitHub via the contents API.
    Requires GITHUB_TOKEN.
    """
    if not GITHUB_TOKEN:
        print("No GITHUB_TOKEN detected; skipping GitHub upload.")
        return

    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{path_in_repo}"

    # If file exists, we need its current SHA to update
    sha = None
    get_resp = session.get(url, params={"ref": branch})
    if get_resp.status_code == 200:
        sha = get_resp.json().get("sha")

    content_b64 = base64.b64encode(csv_df.to_csv(index=False).encode()).decode()
    payload = {
        "message": message,
        "content": content_b64,
        "branch": branch
    }
    if sha:
        payload["sha"] = sha

    put_resp = session.put(url, json=payload)
    try:
        put_resp.raise_for_status()
        print(f"✅ Pushed to GitHub: {path_in_repo}")
    except Exception as e:
        print("❌ Failed to push to GitHub.")
        print("Status:", put_resp.status_code)
        print("Body:", put_resp.text)
        raise

# ----------------------------
# Main
# ----------------------------
all_dfs = []

# List year folders from /data/odds
root_listing = gh_get(API_BASE)
year_dirs = [item["name"] for item in root_listing
             if item["type"] == "dir" and item["name"] in YEARS]
year_dirs = sorted(year_dirs, key=int)

for year in year_dirs:
    print(f"Year {year} …")
    year_url = f"{API_BASE}/{year}"
    year_listing = gh_get(year_url)

    # Weekly files that look like "week_1.csv", "week01.csv", etc.
    week_files = [f["name"] for f in year_listing
                  if f["type"] == "file"
                  and f["name"].lower().endswith(".csv")
                  and f["name"].lower().startswith("week")]

    # Sort by parsed week number so you get deterministic order
    week_files = sorted(week_files, key=parse_week_from_filename)

    for fname in week_files:
        try:
            df = read_week_csv(year, fname)
            # (Optional) normalize whitespace-only strings to NaN
            df = df.replace(r"^\s*$", pd.NA, regex=True)
            # (Optional) keep a breadcrumb of source (handy for debugging)
            df["_src_year"] = int(year)
            df["_src_file"] = fname
            all_dfs.append(df)
            print(f"  - {fname}: rows={len(df)}")
        except Exception as ex:
            print(f"  ! Skipped {fname} due to error: {ex}")

combined = safe_concat(all_dfs)

# Persist locally
combined.to_csv(LOCAL_OUT_CSV, index=False)
print(f"\n✅ Wrote {LOCAL_OUT_CSV} with {len(combined):,} rows and {combined.shape[1]} columns.")

# Quick sanity summary by season/week count (optional)
try:
    summary = (combined.groupby(["season", "week"])
                        .size()
                        .reset_index(name="rows")
                        .sort_values(["season", "week"]))
    print(summary.head(20).to_string(index=False))
except Exception:
    pass

# (Optional) push to GitHub
push_to_github_csv(OWNER, REPO, GITHUB_OUT_PATH, combined,
                   message="chore: add combined odds (2018–2024) for model input",
                   branch="main")
