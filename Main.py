# Databricks notebook source
# MAGIC %md
# MAGIC ## # Importing Data (will differ based on your setup)

# COMMAND ----------

import pandas as pd

odds = pd.read_csv("nfl_spreads_all_seasons.csv")

games = pd.read_csv("games.csv")

# Removing data from before 2018 becasue we dont have any odds data from before then 
games = games[games['season'] >= 2018]

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Transforming data from having two rows per game to one to match the schema of the main dataset "games" for ingestion to the model

# COMMAND ----------

# === TEAM NAME MAP (renaming team names to match that of the games table) ===
team_name_map = {
    'Arizona': 'ARI', 'Atlanta': 'ATL', 'Baltimore': 'BAL', 'Buffalo': 'BUF', 'Carolina': 'CAR',
    'Chicago': 'CHI', 'Cincinnati': 'CIN', 'Cleveland': 'CLE', 'Dallas': 'DAL', 'Denver': 'DEN',
    'Detroit': 'DET', 'Green Bay': 'GB', 'Houston': 'HOU', 'Indianapolis': 'IND', 'Jacksonville': 'JAX',
    'Kansas City': 'KC', 'Las Vegas': 'LV', 'L.A. Chargers': 'LAC', 'L.A. Rams': 'LAR',
    'Miami': 'MIA', 'Minnesota': 'MIN', 'New England': 'NE', 'New Orleans': 'NO',
    'N.Y. Giants': 'NYG', 'N.Y. Jets': 'NYJ', 'Philadelphia': 'PHI', 'Pittsburgh': 'PIT',
    'San Francisco': 'SF', 'Seattle': 'SEA', 'Tampa Bay': 'TB', 'Tennessee': 'TEN',
    'Washington': 'WAS', 'Oakland': 'OAK', 'St. Louis': 'STL'
}

# === STEP 1: Prepare odds dataset
odds_sorted = odds.sort_values(by=['season', 'week', 'rotation']).reset_index(drop=True)
odds_sorted['game_id_index'] = odds_sorted.index // 2
odds_sorted['team'] = odds_sorted['team'].replace(team_name_map)

# === STEP 2: Define fields and reshape (making two rows into one by renaming every row with home and away)
fields_to_pivot = [
    'score', 'wagers',
    'BETONLINE_spread', 'BETONLINE_odds',
    'BOVODA_spread', 'BOVODA_odds',
    'BookMaker_spread', 'BookMaker_odds',
    'BAS_spread', 'BAS_odds',
    'Heritage_spread', 'Heritage_odds',
    'everygame_spread', 'everygame_odds',
    'JustBET_spread', 'JustBET_odds',
    'bet105_spread', 'bet105_odds',
    'WAGERWEB_spread', 'WAGERWEB_odds',
    'opener_spread', 'opener_odds'
]

home_df = odds_sorted[odds_sorted['rotation'] % 2 == 0][['game_id_index', 'season', 'week', 'team'] + fields_to_pivot].copy()
away_df = odds_sorted[odds_sorted['rotation'] % 2 == 1][['game_id_index', 'season', 'week', 'team'] + fields_to_pivot].copy()

home_df = home_df.rename(columns={col: f"{col}_home" for col in fields_to_pivot})
away_df = away_df.rename(columns={col: f"{col}_away" for col in fields_to_pivot})
home_df = home_df.rename(columns={'team': 'team_home', 'season': 'season_home', 'week': 'week_home'})
away_df = away_df.rename(columns={'team': 'team_away', 'season': 'season_away', 'week': 'week_away'})

merged_clean = pd.merge(home_df, away_df, on='game_id_index')

# === STEP 3: Remap string weeks like 'Wild Card' to numeric (weeks were not numbered numerically as they are in games table)
def remap_week(row):
    season = row['season_home']
    week = row['week_home']

    if pd.isna(week):
        return None

    if isinstance(week, str):
        week = week.strip().lower()
        if week == 'wild card':
            return 18 if season < 2021 else 19
        elif week == 'divisional':
            return 19 if season < 2021 else 20
        elif week == 'conference':
            return 20 if season < 2021 else 21
        elif week == 'super bowl':
            return 21 if season < 2021 else 22
        elif week.isdigit():
            return int(week)
        else:
            return None
    elif isinstance(week, (int, float)):
        return int(week)
    else:
        return None

merged_clean['adjusted_week'] = merged_clean.apply(remap_week, axis=1)

# === STEP 4: Drop rows with unmapped weeks
merged_clean = merged_clean[merged_clean['adjusted_week'].notnull()].copy()

# === STEP 5: Build join_key to match games['game_id']
merged_clean['season_str'] = merged_clean['season_home'].astype(str).str.zfill(4)
merged_clean['adjusted_week_str'] = merged_clean['adjusted_week'].astype(int).astype(str).str.zfill(2)
merged_clean['join_key'] = (
    merged_clean['season_str'] + '_' +
    merged_clean['adjusted_week_str'] + '_' +
    merged_clean['team_away'] + '_' +
    merged_clean['team_home']
)

# === STEP 6: Finalize and show
merged_clean = merged_clean.rename(columns={'season_home': 'season', 'adjusted_week': 'week'})
display(merged_clean[['join_key', 'season', 'week', 'team_home', 'team_away', 'score_home', 'score_away']].head(500))


# COMMAND ----------

# === STEP 1: Ensure join_key is present in both tables
games['join_key'] = games['game_id']  # if not already set

# === STEP 2: Perform a clean left join of all odds data
games_final = pd.merge(
    games,
    merged_clean,  # contains all odds, spreads, wagers, team names, etc.
    on='join_key',
    how='left',
    suffixes=('', '_odds')
)

# === STEP 3: Preview final dataset structure
print(f"✅ Final merged dataset shape: {games_final.shape}")
display(games_final.head(600))


# COMMAND ----------

# MAGIC %md
# MAGIC # Actually ingest into model and see if we get a better outcome with given data (havent completed yet)

# COMMAND ----------

# import pandas as pd
# import matplotlib.pyplot as plt
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.metrics import accuracy_score

# Load dataset
games = pd.read_csv("games.csv")

# Filter to seasons 2010 and later
games = games[games['season'] >= 2010]

# Label: home team win
games['home_win'] = games['home_score'] > games['away_score']

# Step 1: Automatically encode all object (string) columns
for col in games.columns:
    if games[col].dtype == 'object':
        games[col] = games[col].astype('category').cat.codes

# Step 2: Define usable features (exclude label and irrelevant targets/IDs)
exclude = [
    'home_win', 'home_score', 'away_score',
    'margin', 'total', 'result', 'old_game_id', 'gsis',
    'nfl_detail_id', 'pfr', 'pff', 'espn', 'ftn',
    'gameday', 'gametime', 'matchup', 'stadium', 'stadium_id'
]
features = [col for col in games.columns if col not in exclude]

# Step 3: Drop incomplete rows
games_clean = games[features + ['home_win']].dropna()

# Step 4: Train/test split
latest_season = games_clean['season'].max()
train_df = games_clean[games_clean['season'] < latest_season]
test_df = games_clean[games_clean['season'] == latest_season]

X_train = train_df[features]
y_train = train_df['home_win']
X_test = test_df[features]
y_test = test_df['home_win']

# Step 5: Train model
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Step 6: Evaluate model
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f"Model test accuracy on {latest_season}: {accuracy:.2%}")

# Step 7: Plot feature importances
importances = model.feature_importances_
feature_importance_df = pd.DataFrame({
    'Feature': features,
    'Importance': importances
}).sort_values(by='Importance', ascending=True)

plt.figure(figsize=(10, len(features) * 0.3))  # Adjust plot height for all features
plt.barh(feature_importance_df['Feature'], feature_importance_df['Importance'])
plt.xlabel('Importance Score')
plt.title('Feature Importances (Random Forest) — All Features')
plt.tight_layout()
plt.grid(True, axis='x', linestyle='--', alpha=0.5)
plt.show()


# COMMAND ----------

