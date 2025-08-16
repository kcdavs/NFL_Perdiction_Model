"""
NFL Betting Pipeline — end‑to‑end (training + weekly pick generator)
==================================================================

Revision **Aug 2025‑03b** – completed script
-------------------------------------------
1. Market‑movement, context, rolling EPA features.
2. Hyper‑param tuned XGBoost with Platt calibration.
3. Edge & bet‑selection (half‑Kelly) for upcoming games.
4. Train vs Score modes selectable via CLI flag.
5. Artifacts: feature_importance.png · predictions_YYYY.csv · weekly_picks.csv · bankroll_history.csv.

Run examples
------------
```bash
python nfl_pipeline.py --train          # retrain through latest settled week
python nfl_pipeline.py --score --date 2025-09-10 --bankroll 1500
```

`--score` expects that the model pickle already exists and that fresh odds
(including openers) for upcoming games are appended to the CSVs.

| Scenario                                        | Config settings                                                           |
| ----------------------------------------------- | ------------------------------------------------------------------------- |
| **Current balanced** (half‑Kelly, edges ≥ 0.05) | `SELECTION_MODE="edge"`<br>`STAKE_MODE="kelly"`                           |
| **Aggressive test** (top‑3, flat \$100)         | `SELECTION_MODE="top3"`<br>`STAKE_MODE="flat"`<br>`FLAT_STAKE = 100.0`    |
| **Ultra‑aggressive** (top‑3, full Kelly)        | `SELECTION_MODE="top3"`<br>`STAKE_MODE="kelly"`<br>`KELLY_FRACTION = 1.0` |

"""
import argparse, sys, pickle, warnings, datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

import nfl_data_py as nfl
from xgboost import XGBClassifier
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.metrics import log_loss, accuracy_score
from sklearn.calibration import CalibratedClassifierCV

warnings.filterwarnings("ignore", "deprecated")

# 1. Load raw files

# --- paths
DATA_DIR   = Path("C:/Users/User/IdeaProjects/NFL_Prediction_Model")
ODDS_CSV   = DATA_DIR / "odds_combined_2018_2024.csv"
GAMES_CSV  = DATA_DIR / "games.csv"
SQLITE_URI = "sqlite:///nfl_betting.db"
MODEL_PKL  = DATA_DIR / "xgb_ats.pkl"
ATS_PKL = DATA_DIR / "xgb_ats.pkl"
ML_PKL  = DATA_DIR / "xgb_ml.pkl"
# --- parameters
RUN_MODE         = "backtest"   # "train" or "score"
TODAY            = pd.to_datetime(dt.date.today())
SEASON_MIN       = 2018
# Selection & Staking
FAIR_THRESHOLD = 0.02   # require ≥2pp advantage vs *vig-free* market probability
EDGE_THRESHOLD   = 0.05       # 5 pp ATS edge
KELLY_FRACTION   = 1.0
BANKROLL_START   = 1_000.0
SELECTION_MODE = "top3"     # "edge"  → all edges ≥ EDGE_THRESHOLD
                            # "top3"  → top‑3 edges each week

STAKE_MODE     = "flat"    # "kelly" → half‑Kelly using KELLY_FRACTION
                            # "flat"  → FLAT_STAKE per bet (no bankroll roll)

FLAT_STAKE     = 100.0      # used only when STAKE_MODE == "flat"
# --- team mapping
TEAM_MAP = {
    # NFC
    "Arizona": "ARI", "Atlanta": "ATL", "Carolina": "CAR", "Chicago": "CHI",
    "Dallas": "DAL", "Detroit": "DET", "Green Bay": "GB", "L.A. Rams": "LA",
    "Minnesota": "MIN", "New Orleans": "NO", "N.Y. Giants": "NYG",
    "Philadelphia": "PHI", "San Francisco": "SF", "Seattle": "SEA",
    "Tampa Bay": "TB", "Washington": "WAS",
    # AFC
    "Baltimore": "BAL", "Buffalo": "BUF", "Cincinnati": "CIN", "Cleveland": "CLE",
    "Denver": "DEN", "Houston": "HOU", "Indianapolis": "IND", "Jacksonville": "JAX",
    "Kansas City": "KC", "Las Vegas": "LV", "Oakland": "OAK", "St. Louis": "STL",
    "L.A. Chargers": "LAC", "Miami": "MIA", "N.Y. Jets": "NYJ", "New England": "NE",
    "Pittsburgh": "PIT", "Tennessee": "TEN",
}

# 2. Helper functions (patched)

def american_to_prob(odds: float) -> float:
    if pd.isna(odds):
        return np.nan
    return 100 / (odds + 100) if odds > 0 else abs(odds) / (abs(odds) + 100)

def american_to_profit_multiple(odds: float) -> float:
    # profit per $1 stake (excludes stake)
    if pd.isna(odds):
        return np.nan
    return odds / 100.0 if odds > 0 else 100.0 / abs(odds)

def kelly_fraction(p_win: float, odds: float) -> float:
    # CAP Kelly to [0, 1] to prevent >100% stakes
    b = american_to_profit_multiple(odds)
    if pd.isna(b) or b <= 0:
        return 0.0
    f = (b * p_win - (1 - p_win)) / b
    return max(0.0, min(1.0, f))

def kelly_stake_from_prob(p_win: float, bankroll: float, odds: float) -> float:
    return bankroll * KELLY_FRACTION * kelly_fraction(p_win, odds)

def stake_amount(edge: float, bankroll: float, p_win: float = None, odds: float = None) -> float:
    if STAKE_MODE == "kelly":
        if p_win is None or odds is None:
            raise ValueError("Kelly staking requires p_win and odds.")
        return kelly_stake_from_prob(p_win, bankroll, odds)
    elif STAKE_MODE == "flat":
        return FLAT_STAKE
    else:
        raise ValueError("STAKE_MODE must be 'kelly' or 'flat'")

def devig_pair(odds_home: float, odds_away: float):
    """
    Return (p_fair_home, p_fair_away, p_imp_home, p_imp_away)
    where p_imp_* are raw implied probs from the *priced* odds
    and p_fair_* are no-vig probabilities normalized to sum to 1.
    """
    p_h = american_to_prob(odds_home)
    p_a = american_to_prob(odds_away)
    if pd.isna(p_h) or pd.isna(p_a):
        return np.nan, np.nan, p_h, p_a
    s = p_h + p_a
    if not np.isfinite(s) or s <= 0:
        return np.nan, np.nan, p_h, p_a
    return p_h / s, p_a / s, p_h, p_a

def select_bets_week(cand_df: pd.DataFrame) -> pd.DataFrame:
    """
    Weekly selection using *both* tests:
      1) Positive EV vs the *priced* break-even (edge_ev >= EDGE_THRESHOLD)
      2) Advantage vs the *vig-free* fair prob (edge_fair >= FAIR_THRESHOLD)
    Then choose top-3 by edge_fair (or all if SELECTION_MODE='edge').
    """
    d = cand_df.copy()
    d = d[d["edge_ev"].notna() & d["edge_fair"].notna()]
    d = d[(d["edge_ev"] >= EDGE_THRESHOLD) & (d["edge_fair"] >= FAIR_THRESHOLD)]
    if SELECTION_MODE == "top3":
        return d.sort_values("edge_fair", ascending=False).head(3)
    elif SELECTION_MODE == "edge":
        return d.sort_values("edge_fair", ascending=False)
    else:
        raise ValueError("SELECTION_MODE must be 'edge' or 'top3'")

from pathlib import Path
import shutil

# Your project folder
DATA_DIR = Path("C:/Users/User/IdeaProjects/NFL_Prediction_Model")

# Candidate locations to search (add/remove as needed)
candidates = [
    DATA_DIR / "odds_combined_2018_2024.csv",          # expected project location
    Path.cwd() / "odds_combined_2018_2024.csv",        # next to the notebook
]

# Find the file
ODDS_CSV = None
for p in candidates:
    if p.exists():
        ODDS_CSV = p
        break

# If not found, do a quick recursive search starting from the project root (fast enough)
if ODDS_CSV is None and DATA_DIR.exists():
    hits = list(DATA_DIR.rglob("odds_combined_2018_2024.csv"))
    if hits:
        ODDS_CSV = hits[0]

if ODDS_CSV is None:
    raise FileNotFoundError(
        "Couldn't find 'odds_combined_2018_2024.csv'. "
        "If you know the path, set ODDS_CSV = Path(r'full\\path\\to\\odds_combined_2018_2024.csv')."
    )

print("✅ Using odds file:", ODDS_CSV)

# (Optional) copy into the project so future runs just work
target = DATA_DIR / "odds_combined_2018_2024.csv"
if ODDS_CSV.resolve() != target.resolve():
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ODDS_CSV, target)
    ODDS_CSV = target
    print("📦 Copied to project:", ODDS_CSV)

# --- Build dataset (helpers + function) ---

import re

def _row_mean_safe(r, cols):
    v = pd.to_numeric(r[cols], errors="coerce")
    return v.mean() if len(cols) else np.nan

def _row_std_safe(r, cols):
    v = pd.to_numeric(r[cols], errors="coerce")
    return v.std() if len(cols) else np.nan

def _ensure_team_cols_lines(df: pd.DataFrame) -> pd.DataFrame:
    # prefer already-normalized names if present
    if "home_team" not in df.columns:
        for c in ["team_abbr_home", "team_home", "home", "homeTeam"]:
            if c in df.columns:
                df = df.rename(columns={c: "home_team"})
                break
    if "away_team" not in df.columns:
        for c in ["team_abbr_away", "team_away", "away", "awayTeam"]:
            if c in df.columns:
                df = df.rename(columns={c: "away_team"})
                break
    return df

def _ensure_games_cols(df: pd.DataFrame) -> pd.DataFrame:
    # team columns
    if "home_team" not in df.columns:
        for c in ["team_home", "home", "home_abbr", "HomeTeam"]:
            if c in df.columns:
                df = df.rename(columns={c: "home_team"})
                break
    if "away_team" not in df.columns:
        for c in ["team_away", "away", "away_abbr", "AwayTeam"]:
            if c in df.columns:
                df = df.rename(columns={c: "away_team"})
                break
    # date column
    if "game_date" not in df.columns:
        for c in ["gameday", "gamedate", "date"]:
            if c in df.columns:
                df = df.rename(columns={c: "game_date"})
                break
    df["game_date"] = pd.to_datetime(df.get("game_date"), errors="coerce")
    return df

def build_dataset() -> pd.DataFrame:
    """Return merged odds + games DataFrame (leak-free rolling EPA) and persist to SQLite."""
    # --- 1) Read odds ---
    odds = pd.read_csv(ODDS_CSV)

    # team abbreviations (if not already present)
    if "team_abbr" not in odds.columns and "team" in odds.columns:
        odds["team_abbr"] = odds["team"].map(TEAM_MAP)
    elif "team_abbr" not in odds.columns:
        raise ValueError("Odds CSV must have either 'team_abbr' or 'team' to map from.")

    # Identify book columns in the combined odds file
    cols = odds.columns.tolist()
    ml_cols       = [c for c in cols if re.fullmatch(r"\d+_ml", c)]
    spr_cols      = [c for c in cols if re.fullmatch(r"\d+_spr", c)]
    spr_odds_cols = [c for c in cols if re.fullmatch(r"\d+_spr_odds", c)]
    ou_cols       = [c for c in cols if re.fullmatch(r"\d+_ou", c)]  # totals may exist

    keep_cols = ["eid","season","week","rotation","team_abbr"] \
                + [c for c in ["ml_perc","ml_wag","spr_perc","spr_wag","ou_perc","ou_wag"] if c in cols] \
                + ml_cols + spr_cols + spr_odds_cols + ou_cols

    # Pair home/away via rotation: away = odd, home = even (= odd + 1)
    away = odds.loc[odds["rotation"] % 2 == 1, keep_cols].copy()
    home = odds.loc[odds["rotation"] % 2 == 0, keep_cols].copy()
    away["rotation"] = away["rotation"] + 1  # align to home rotation

    # ✅ Make team columns explicit BEFORE the merge
    home = home.rename(columns={"team_abbr": "home_team"})
    away = away.rename(columns={"team_abbr": "away_team"})

    # Merge home/away rows for the same game
    lines = pd.merge(
        home, away,
        on=["eid", "season", "week", "rotation"],
        suffixes=("_home", "_away"),
        how="inner",
    )

    # Sanity: ensure team cols exist
    assert {"home_team", "away_team"}.issubset(lines.columns), f"lines missing team cols: {set(lines.columns)}"


    # Consensus across books
    lines["ml_home_cons"]  = lines.apply(lambda r: _row_mean_safe(r, [f"{c}_home" for c in ml_cols]), axis=1)
    lines["ml_away_cons"]  = lines.apply(lambda r: _row_mean_safe(r, [f"{c}_away" for c in ml_cols]), axis=1)
    lines["spr_home_cons"] = lines.apply(lambda r: _row_mean_safe(r, [f"{c}_home" for c in spr_cols]), axis=1)
    lines["spr_away_cons"] = lines.apply(lambda r: _row_mean_safe(r, [f"{c}_away" for c in spr_cols]), axis=1)
    lines["spr_odds_home_cons"] = lines.apply(lambda r: _row_mean_safe(r, [f"{c}_home" for c in spr_odds_cols]), axis=1)
    lines["spr_odds_away_cons"] = lines.apply(lambda r: _row_mean_safe(r, [f"{c}_away" for c in spr_odds_cols]), axis=1)

    # Totals (OU) → implied team totals
    ou_home_cols = [f"{c}_home" for c in ou_cols if f"{c}_home" in lines.columns]
    ou_away_cols = [f"{c}_away" for c in ou_cols if f"{c}_away" in lines.columns]
    ou_any = ou_home_cols + ou_away_cols
    lines["total_cons"] = lines.apply(lambda r: _row_mean_safe(r, ou_any), axis=1) if ou_any else np.nan
    lines["itt_home"] = lines["total_cons"]/2 - lines["spr_home_cons"]/2
    lines["itt_away"] = lines["total_cons"]/2 + lines["spr_home_cons"]/2

    # Public split diffs (if present)
    for name in ["ml_perc","spr_perc","ou_perc","ml_wag","spr_wag","ou_wag"]:
        h, a = f"{name}_home", f"{name}_away"
        if h in lines.columns and a in lines.columns:
            lines[f"{name}_diff"] = pd.to_numeric(lines[h], errors="coerce") - pd.to_numeric(lines[a], errors="coerce")

    # Book disagreement (std across books)
    lines["spr_home_std"] = lines.apply(lambda r: _row_std_safe(r, [f"{c}_home" for c in spr_cols]), axis=1)
    lines["spr_away_std"] = lines.apply(lambda r: _row_std_safe(r, [f"{c}_away" for c in spr_cols]), axis=1)
    lines["ml_home_std"]  = lines.apply(lambda r: _row_std_safe(r, [f"{c}_home" for c in ml_cols]), axis=1)
    lines["ml_away_std"]  = lines.apply(lambda r: _row_std_safe(r, [f"{c}_away" for c in ml_cols]), axis=1)

    lines["join_key"] = (lines["season"].astype(str) + "_" +
                         lines["week"].astype(int).astype(str).str.zfill(2) + "_" +
                         lines["home_team"] + "_" + lines["away_team"])

    # --- 2) Read games (local csv or nfl_data_py fallback) ---
    try:
        if GAMES_CSV.exists():
            games = pd.read_csv(GAMES_CSV)
        else:
            raise FileNotFoundError
    except Exception:
        years = list(range(SEASON_MIN, 2025))
        games = nfl.import_schedules(years)

    # Standardize key columns on GAMES
    for col in ["game_date","gameday","gamedate","date"]:
        if col in games.columns:
            games = games.rename(columns={col: "game_date"})
            break
    games = _ensure_games_cols(games)
    missing = {"home_team","away_team"} - set(games.columns)
    if missing:
        raise ValueError(f"GAMES missing columns: {missing}. Have: {sorted(games.columns)}")

    games["join_key"] = (games["season"].astype(str) + "_" +
                         games["week"].astype(int).astype(str).str.zfill(2) + "_" +
                         games["home_team"] + "_" + games["away_team"])

    # --- 3) Merge to dataset (CREATE dataset FIRST) ---
    keep_from_lines = [
    "join_key","eid",
    "ml_home_cons","ml_away_cons",
    "spr_home_cons","spr_away_cons",
    "spr_odds_home_cons","spr_odds_away_cons",
    "total_cons","itt_home","itt_away",
    "ml_perc_diff","spr_perc_diff","ou_perc_diff",
    "ml_wag_diff","spr_wag_diff","ou_wag_diff",
    "spr_home_std","spr_away_std","ml_home_std","ml_away_std",
    # ⛔️ intentionally NOT including 'home_team'/'away_team' from lines
    ]
    keep_from_lines = [c for c in keep_from_lines if c in lines.columns]

    dataset = games.merge(
        lines[keep_from_lines],
        on="join_key",
        how="inner",
        suffixes=("", "_L")   # keep games' columns as the canonical ones
    )

    # sanity: ensure teams present (from games)
    assert {"home_team","away_team"}.issubset(dataset.columns), \
        f"Teams missing after merge. Have: {sorted(dataset.columns)}"
    # Outcomes
    dataset["margin"]   = pd.to_numeric(dataset.get("home_score"), errors="coerce") - pd.to_numeric(dataset.get("away_score"), errors="coerce")
    dataset["home_win"] = (dataset["margin"] > 0).astype("Int64")
    dataset["home_cover"] = (pd.to_numeric(dataset["margin"], errors="coerce") + pd.to_numeric(dataset["spr_home_cons"], errors="coerce") > 0).astype("Int64")

    # --- 4) Rest-days & weather FLAGS (now that dataset exists) ---
    if {"home_team","away_team","game_date"}.issubset(dataset.columns):
        for side in ("home","away"):
            key = f"{side}_team"
            dataset = dataset.sort_values([key, "game_date"])
            dataset[f"{side}_last_game"] = dataset.groupby(key)["game_date"].shift(1)
            dataset[f"{side}_rest"] = (dataset["game_date"] - dataset[f"{side}_last_game"]).dt.days
    else:
        print("⚠️ Skipping rest-days: required columns not present.")

    dataset["is_roof_closed"] = (dataset["roof"].astype(str).str.lower() == "closed").astype("Int64")
    dataset["is_cold"]        = (pd.to_numeric(dataset.get("temp"), errors="coerce") <= 40).astype("Int64")
    dataset["is_windy"]       = (pd.to_numeric(dataset.get("wind"), errors="coerce") >= 12).astype("Int64")
    dataset["is_outdoor"]     = (~dataset["is_roof_closed"].astype(bool)).astype("Int64")
    dataset["windy_outdoor"]  = (dataset["is_windy"].fillna(0).astype(bool) & dataset["is_outdoor"].astype(bool)).astype("Int64")

    # --- 5) Leak-free rolling EPA from pbp (per-team, per-week, shifted) ---
    years = sorted(dataset.season.dropna().unique().astype(int).tolist())
    if years:
        pbp = nfl.import_pbp_data(years)
        pbp = pbp[pbp.season >= SEASON_MIN].copy()

        off_wk = (pbp.groupby(["season","week","posteam"], as_index=False)["epa"]
                     .mean().rename(columns={"posteam":"team","epa":"off_epa_g"}))
        def_wk = (pbp.groupby(["season","week","defteam"], as_index=False)["epa"]
                     .mean().rename(columns={"defteam":"team","epa":"def_epa_g"}))

        off_wk = off_wk.sort_values(["team","season","week"])
        off_wk["off_epa_3g"] = off_wk.groupby("team")["off_epa_g"].apply(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
        def_wk = def_wk.sort_values(["team","season","week"])
        def_wk["def_epa_3g"] = def_wk.groupby("team")["def_epa_g"].apply(lambda s: s.shift(1).rolling(3, min_periods=1).mean())

        dataset = dataset.merge(off_wk[["season","week","team","off_epa_3g"]],
                                left_on=["season","week","home_team"], right_on=["season","week","team"], how="left") \
                         .rename(columns={"off_epa_3g":"off_epa_3g_home"}).drop(columns="team")
        dataset = dataset.merge(off_wk[["season","week","team","off_epa_3g"]],
                                left_on=["season","week","away_team"], right_on=["season","week","team"], how="left") \
                         .rename(columns={"off_epa_3g":"off_epa_3g_away"}).drop(columns="team")
        dataset = dataset.merge(def_wk[["season","week","team","def_epa_3g"]],
                                left_on=["season","week","home_team"], right_on=["season","week","team"], how="left") \
                         .rename(columns={"def_epa_3g":"def_epa_3g_home"}).drop(columns="team")
        dataset = dataset.merge(def_wk[["season","week","team","def_epa_3g"]],
                                left_on=["season","week","away_team"], right_on=["season","week","team"], how="left") \
                         .rename(columns={"def_epa_3g":"def_epa_3g_away"}).drop(columns="team")

        dataset["off_epa_diff"] = dataset["off_epa_3g_home"] - dataset["def_epa_3g_away"]
        dataset["def_epa_diff"] = dataset["def_epa_3g_home"] - dataset["off_epa_3g_away"]
    else:
        print("⚠️ No seasons found for EPA merge; skipping EPA features.")

    # --- 6) Persist & return ---
    engine = create_engine(SQLITE_URI, echo=False)
    dataset.to_sql("model_dataset", engine, if_exists="replace", index=False)
    print(f"✅ Dataset rows: {len(dataset):,}")
    return dataset

dataset = build_dataset()
dataset[["season","week","home_team","away_team","spr_home_cons","ml_home_cons","total_cons"]].head()
dataset.groupby("season").size()

# 4⃣ Train / Score selector

# --- utility: filter training features (avoid using spread odds as a predictor) ---
def filter_train_features(cols):
    cols = list(dict.fromkeys(cols))  # de-dupe but preserve order
    drop = {"spr_odds_home_cons", "spr_odds_away_cons"}  # keep odds for EV, not for training
    return [c for c in cols if c not in drop]

if RUN_MODE == "train":
    dataset = build_dataset()
    df = dataset.copy()

    FEATURES = [
        # market (keep spreads & ML; drop spread-odds later for training)
        "spr_home_cons", "spr_away_cons", "spr_odds_home_cons", "spr_odds_away_cons",
        "ml_home_cons", "ml_away_cons",
        # public / sentiment
        "ml_perc_diff", "spr_perc_diff", "ou_perc_diff", "ml_wag_diff", "spr_wag_diff", "ou_wag_diff",
        # totals & implied team totals
        "total_cons", "itt_home", "itt_away",
        # book disagreement
        "spr_home_std","spr_away_std","ml_home_std","ml_away_std",
        # context
        "home_rest", "away_rest", "is_roof_closed", "is_cold", "is_windy", "is_outdoor", "windy_outdoor",
        # form (leak-free rolling)
        "off_epa_3g_home", "off_epa_3g_away", "def_epa_3g_home", "def_epa_3g_away",
        "off_epa_diff", "def_epa_diff",
    ]
    FEATURES = [f for f in FEATURES if f in df.columns]
    TRAIN_FEATURES = filter_train_features(FEATURES)

    df = df.dropna(subset=TRAIN_FEATURES + ["home_cover", "home_win"]).sort_values(["season", "week"])
    X = df[TRAIN_FEATURES].values

    # --- ATS model (home_cover) ---
    y_ats = df["home_cover"].values
    base = XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=42)
    param_grid = {
        "n_estimators": [400, 800, 1200],
        "max_depth": [3, 4, 5],
        "learning_rate": [0.03, 0.05, 0.07],
        "subsample": [0.7, 0.8, 0.9],
        "colsample_bytree": [0.7, 0.8, 0.9],
    }
    search_ats = RandomizedSearchCV(base, param_grid, cv=3, n_iter=20, scoring="neg_log_loss", verbose=0)
    search_ats.fit(X, y_ats)
    best_ats = search_ats.best_estimator_

    # Calibrate on last season
    last_season = df.season.max()
    tr = df[df.season < last_season]
    va = df[df.season == last_season]
    best_ats.fit(tr[TRAIN_FEATURES], tr["home_cover"])
    ats = CalibratedClassifierCV(best_ats, method="sigmoid", cv="prefit")
    ats.fit(va[TRAIN_FEATURES], va["home_cover"])
    with open(ATS_PKL, "wb") as f:
        pickle.dump({"model": ats, "features": TRAIN_FEATURES}, f)
    print("📝  ATS model saved →", ATS_PKL)

    # --- ML model (home_win) ---
    y_ml = df["home_win"].values
    search_ml = RandomizedSearchCV(base, param_grid, cv=3, n_iter=20, scoring="neg_log_loss", verbose=0)
    search_ml.fit(X, y_ml)
    best_ml = search_ml.best_estimator_

    best_ml.fit(tr[TRAIN_FEATURES], tr["home_win"])
    ml = CalibratedClassifierCV(best_ml, method="sigmoid", cv="prefit")
    ml.fit(va[TRAIN_FEATURES], va["home_win"])
    with open(ML_PKL, "wb") as f:
        pickle.dump({"model": ml, "features": TRAIN_FEATURES}, f)
    print("📝  ML model saved →", ML_PKL)

elif RUN_MODE == "score":
    # Load models
    with open(ATS_PKL, "rb") as f:
        ats_bundle = pickle.load(f)
    with open(ML_PKL, "rb") as f:
        ml_bundle = pickle.load(f)
    ats, ml = ats_bundle["model"], ml_bundle["model"]
    FEATURES = [c for c in ats_bundle["features"] if c in ml_bundle["features"]]

    dataset = build_dataset()
    upcoming = dataset[dataset.game_date >= TODAY].copy()
    upcoming = upcoming.dropna(subset=FEATURES)

    # ---- Find books present in upcoming
    book_ids = find_book_ids(upcoming.columns)

    # ---- Build candidates with real line shopping + de-vig gating
    cands = []
    for _, r in upcoming.iterrows():
        for bid in book_ids:
            # pull this book's quotes (skip if missing)
            cols = {
                "ml_home": f"{bid}_ml_home",
                "ml_away": f"{bid}_ml_away",
                "spr_home": f"{bid}_spr_home",
                "spr_away": f"{bid}_spr_away",
                "spr_odds_home": f"{bid}_spr_odds_home",
                "spr_odds_away": f"{bid}_spr_odds_away",
            }
            if not all(c in upcoming.columns for c in cols.values()):
                continue

            mlh, mla   = r.get(cols["ml_home"]), r.get(cols["ml_away"])
            sh, sa     = r.get(cols["spr_home"]), r.get(cols["spr_away"])
            shod, saod = r.get(cols["spr_odds_home"]), r.get(cols["spr_odds_away"])
            if pd.isna(mlh) or pd.isna(mla) or pd.isna(sh) or pd.isna(sa) or pd.isna(shod) or pd.isna(saod):
                continue

            # Build a feature row for THIS book by overwriting the consensus fields
            feat = r[FEATURES].to_frame().T.copy()
            overwrite = {
                "ml_home_cons": mlh, "ml_away_cons": mla,
                "spr_home_cons": sh, "spr_away_cons": sa,
                # odds are NOT used by the trained model (we filtered them out), but keep in FEATURES if present
                "spr_odds_home_cons": shod, "spr_odds_away_cons": saod,
            }
            for name, val in overwrite.items():
                if name in feat.columns:
                    feat.iloc[0, feat.columns.get_loc(name)] = val

            # Predict with calibrated models at this book's line/price
            p_home_cover = ats.predict_proba(feat[FEATURES])[:,1][0]
            p_home_win   = ml.predict_proba(feat[FEATURES])[:,1][0]

            # De-vig edges for ATS and ML using this book's two-sided prices
            p_fair_ml_home, p_fair_ml_away, p_be_ml_home, p_be_ml_away = devig_pair(mlh, mla)
            p_fair_ats_home, p_fair_ats_away, p_be_ats_home, p_be_ats_away = devig_pair(shod, saod)

            rows = [
                # ATS HOME
                {"bet_type":"ATS","side":"HOME","odds":shod,"p_win":p_home_cover,
                 "edge_ev": p_home_cover - p_be_ats_home, "edge_fair": p_home_cover - p_fair_ats_home,
                 "line": sh, "book_id": bid},
                # ATS AWAY
                {"bet_type":"ATS","side":"AWAY","odds":saod,"p_win":1-p_home_cover,
                 "edge_ev": (1-p_home_cover) - p_be_ats_away, "edge_fair": (1-p_home_cover) - p_fair_ats_away,
                 "line": sa, "book_id": bid},
                # ML HOME
                {"bet_type":"ML","side":"HOME","odds":mlh,"p_win":p_home_win,
                 "edge_ev": p_home_win - p_be_ml_home, "edge_fair": p_home_win - p_fair_ml_home,
                 "line": np.nan, "book_id": bid},
                # ML AWAY
                {"bet_type":"ML","side":"AWAY","odds":mla,"p_win":1-p_home_win,
                 "edge_ev": (1-p_home_win) - p_be_ml_away, "edge_fair": (1-p_home_win) - p_fair_ml_away,
                 "line": np.nan, "book_id": bid},
            ]
            for rr in rows:
                cands.append({
                    "season": r.season, "week": r.week, "game_date": r.game_date,
                    "home_team": r.home_team, "away_team": r.away_team,
                    **rr
                })

    cand_df = pd.DataFrame(cands)

    # One bet per game: keep the single highest edge_fair across books/sides/markets
    if not cand_df.empty:
        cand_df["game_key"] = (cand_df["season"].astype(str) + "_" +
                               cand_df["week"].astype(int).astype(str).zfill(2) + "_" +
                               cand_df["home_team"] + "_" + cand_df["away_team"])
        cand_df = (cand_df.sort_values("edge_fair", ascending=False)
                           .groupby("game_key", as_index=False).head(1))

    # Weekly selection (top-3 or threshold) using dual gate inside select_bets_week
    picks = (cand_df.groupby(["season","week"], as_index=False, group_keys=False)
                    .apply(select_bets_week))

    # Stake (flat or Kelly)
    if STAKE_MODE == "kelly":
        picks["stake"] = picks.apply(lambda s: stake_amount(s.edge_ev, BANKROLL_START, s.p_win, s.odds), axis=1)
    else:
        picks["stake"] = FLAT_STAKE

    outfile = DATA_DIR / f"recommended_bets_{TODAY:%Y%m%d}.csv"
    picks.to_csv(outfile, index=False)
    print(f"💸  {len(picks)} picks written → {outfile}")

else:
    pass


# 5⃣ Walk-forward back-test 2018-2024 (FINAL with de-vig and strict selection)

ONE_BET_PER_GAME = True   # set False to allow both ATS and ML on same game
DEBUG_SAMPLE_WEEKS = 3    # print first N weeks' picks for sanity

if RUN_MODE == "backtest":
    print("🔄  Walk-forward back-test …")
    dataset = build_dataset()

    FEATURES = [
        "spr_home_cons", "spr_away_cons", "spr_odds_home_cons", "spr_odds_away_cons",
        "ml_home_cons", "ml_away_cons",
        "ml_perc_diff", "spr_perc_diff",
        "home_rest", "away_rest", "is_roof_closed",
        "off_epa_3g_home", "off_epa_3g_away", "def_epa_3g_home", "def_epa_3g_away",
        "off_epa_diff", "def_epa_diff",
    ]
    FEATURES = [f for f in FEATURES if f in dataset.columns]

    bankroll = BANKROLL_START
    curve, weekly_logs, weekly_count = [], [], 0
    picks_all = []

    seasons = sorted(dataset.season.unique())
    for season in seasons:
        weeks = sorted(dataset[dataset.season == season].week.unique())
        for wk in weeks:
            train = dataset[(dataset.season < season) |
                            ((dataset.season == season) & (dataset.week < wk))]
            test  = dataset[(dataset.season == season) & (dataset.week == wk)]

            train = train.dropna(subset=FEATURES + ["home_cover", "home_win"])
            test  = test.dropna(subset=FEATURES + [
                "spr_odds_home_cons","spr_odds_away_cons","ml_home_cons","ml_away_cons"
            ])

            if len(train) < 200 or test.empty:
                curve.append((season, wk, bankroll))
                continue

            # Fit ATS & ML and calibrate (3-fold)
            X_train = train[FEATURES]
            y_ats   = train["home_cover"]
            y_ml    = train["home_win"]

            model_ats = XGBClassifier(
                n_estimators=400, max_depth=3, learning_rate=0.03,
                subsample=0.9, colsample_bytree=0.7,
                objective="binary:logistic", eval_metric="logloss",
                random_state=42
            ).fit(X_train, y_ats)
            calib_ats = CalibratedClassifierCV(model_ats, method="sigmoid", cv=3).fit(X_train, y_ats)

            model_ml = XGBClassifier(
                n_estimators=400, max_depth=3, learning_rate=0.03,
                subsample=0.9, colsample_bytree=0.7,
                objective="binary:logistic", eval_metric="logloss",
                random_state=42
            ).fit(X_train, y_ml)
            calib_ml = CalibratedClassifierCV(model_ml, method="sigmoid", cv=3).fit(X_train, y_ml)

            T = test.copy()
            T["p_home_cover"] = calib_ats.predict_proba(T[FEATURES])[:,1]
            T["p_home_win"]   = calib_ml.predict_proba(T[FEATURES])[:,1]

            # ---- Build candidates (ML & ATS, both sides) with de-vig edges
            cands = []
            for _, r in T.iterrows():
                # ML de-vig
                p_fair_ml_home, p_fair_ml_away, p_be_ml_home, p_be_ml_away = devig_pair(r.ml_home_cons, r.ml_away_cons)
                p_model_ml_home = r.p_home_win
                p_model_ml_away = 1 - r.p_home_win

                # ATS de-vig (uses spread odds)
                p_fair_ats_home, p_fair_ats_away, p_be_ats_home, p_be_ats_away = devig_pair(r.spr_odds_home_cons, r.spr_odds_away_cons)
                p_model_ats_home = r.p_home_cover
                p_model_ats_away = 1 - r.p_home_cover

                rows = [
                    # ATS HOME
                    {"bet_type":"ATS","side":"HOME","odds":r.spr_odds_home_cons,"p_win":p_model_ats_home,
                     "edge_ev": p_model_ats_home - p_be_ats_home, "edge_fair": p_model_ats_home - p_fair_ats_home,
                     "line": r.spr_home_cons},
                    # ATS AWAY
                    {"bet_type":"ATS","side":"AWAY","odds":r.spr_odds_away_cons,"p_win":p_model_ats_away,
                     "edge_ev": p_model_ats_away - p_be_ats_away, "edge_fair": p_model_ats_away - p_fair_ats_away,
                     "line": r.spr_away_cons},
                    # ML HOME
                    {"bet_type":"ML","side":"HOME","odds":r.ml_home_cons,"p_win":p_model_ml_home,
                     "edge_ev": p_model_ml_home - p_be_ml_home, "edge_fair": p_model_ml_home - p_fair_ml_home,
                     "line": np.nan},
                    # ML AWAY
                    {"bet_type":"ML","side":"AWAY","odds":r.ml_away_cons,"p_win":p_model_ml_away,
                     "edge_ev": p_model_ml_away - p_be_ml_away, "edge_fair": p_model_ml_away - p_fair_ml_away,
                     "line": np.nan},
                ]
                for rr in rows:
                    cands.append({
                        "season":r.season,"week":r.week,"game_date":r.game_date,
                        "home_team":r.home_team,"away_team":r.away_team,
                        "margin": r.margin, **rr
                    })

            cand_df = pd.DataFrame(cands)

            # Optionally restrict to ONE best bet per game (highest edge_fair)
            if ONE_BET_PER_GAME and not cand_df.empty:
                cand_df["game_key"] = (cand_df["season"].astype(str) + "_" +
                                       cand_df["week"].astype(int).astype(str).str.zfill(2) + "_" +
                                       cand_df["home_team"] + "_" + cand_df["away_team"])
                cand_df = (cand_df.sort_values("edge_fair", ascending=False)
                                   .groupby("game_key", as_index=False).head(1))

            # Weekly selection using edge vs priced and vig-free (needs select_bets_week from Cell 2)
            picks_week = select_bets_week(cand_df)

            # ---- Stake + settle
            rows = []
            for _, s in picks_week.iterrows():
                stake = stake_amount(edge=s.edge_ev, bankroll=bankroll, p_win=s.p_win, odds=s.odds)
                pm = american_to_profit_multiple(s.odds)

                if s.bet_type == "ATS":
                    # For AWAY ATS, compare away margin (+line_away): away_margin = -margin
                    if s.side == "HOME":
                        val = float(s.margin) + float(s.line)
                    else:  # AWAY
                        val = -float(s.margin) + float(s.line)

                    if np.isclose(val, 0.0, atol=1e-9):
                        payout = 0.0
                    else:
                        won = val > 0
                        payout = stake * (pm if won else -1.0)

                else:  # ML
                    if np.isclose(s.margin, 0.0, atol=1e-9):
                        payout = 0.0
                    else:
                        won = (s.margin > 0) if s.side == "HOME" else (s.margin < 0)
                        payout = stake * (pm if won else -1.0)

                rows.append({**s.to_dict(), "stake": stake, "payout": payout})

            if rows:
                B = pd.DataFrame(rows)
                bankroll += B["payout"].sum()
                picks_all.append(B)

            curve.append((season, wk, bankroll))

            # Debug sample
            if weekly_count < DEBUG_SAMPLE_WEEKS:
                weekly_logs.append({
                    "season": season, "week": wk,
                    "num_candidates": len(cand_df),
                    "num_picks": 0 if not rows else len(rows),
                    "picks_preview": [] if not rows else B[["bet_type","side","odds","edge_ev","edge_fair","line","payout"]].round(3).to_dict("records"),
                })
                weekly_count += 1

    curve_df = pd.DataFrame(curve, columns=["season", "week", "bankroll"])
    print(f"📈  Ending bankroll: ${bankroll:,.2f}  (start ${BANKROLL_START:,.0f})")

    # Save and plot
    if picks_all:
        picks_all = pd.concat(picks_all, ignore_index=True)
        picks_all.to_csv(DATA_DIR / "backtest_bets.csv", index=False)
        print("💾 backtest_bets.csv written.")

    if not curve_df.empty:
        plt.figure(figsize=(8,4))
        plt.plot(curve_df.bankroll)
        plt.title("Walk-forward bankroll growth")
        plt.xlabel("Week index")
        plt.ylabel("Bankroll ($)")
        plt.tight_layout()
        plt.show()

    # --- Quick audits ---
    if picks_all is not None and not picks_all.empty:
        print("\n🔍 Weekly debug (first few weeks):")
        for w in weekly_logs:
            print(w)

        pw = picks_all.groupby(["season","week"]).size()
        print("\nBets per week → min/median/max:",
              int(pw.min()), int(pw.median()), int(pw.max()),
              "| total bets:", len(picks_all))
        if SELECTION_MODE == "top3" and pw.max() > 3:
            print("⚠️ Found weeks with >3 bets (should not happen).")

print("STAKE_MODE:", STAKE_MODE, "| SELECTION_MODE:", SELECTION_MODE)
print("EDGE_THRESHOLD:", EDGE_THRESHOLD, "| FLAT_STAKE:", FLAT_STAKE)
if 'picks_all' in globals() and isinstance(picks_all, pd.DataFrame) and not picks_all.empty:
    n_bets = len(picks_all)
    bets_per_season = picks_all.groupby('season').size()
    bets_per_week   = picks_all.groupby(['season','week']).size().rename('bets_in_week')
    print(f"Total bets: {n_bets}")
    print("Bets per season:\n", bets_per_season.to_string())
    print("Bets per week (sample):\n", bets_per_week.head(10).to_string())

    total_staked = picks_all['stake'].sum()
    avg_odds     = picks_all['odds'].mean()
    win_rate     = (picks_all['payout'] > 0).mean()
    units        = picks_all['payout'].sum() / (FLAT_STAKE if STAKE_MODE=='flat' else 100)
    print(f"\nTotal staked: ${total_staked:,.2f} | Avg odds: {avg_odds:+.0f} | Hit rate: {win_rate:.3f} | Units: {units:.1f}")

    # sanity: for TOP3 + 7 seasons you should be ~ 3 bets/week * ~21 weeks * 7 ≈ ~441 bets (give or take)
else:
    print("No picks_all dataframe to audit.")

