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

import pandas as pd

# === Load data ===
games = pd.read_csv("games.csv")
odds = pd.read_csv("nfl_spreads_all_seasons.csv")

# Only keep games with available odds data
games = games[games['season'] >= 2018]

# === TEAM NAME MAP (odds → games naming) ===
team_name_map = {
    'Arizona': 'ARI', 'Atlanta': 'ATL', 'Baltimore': 'BAL', 'Buffalo': 'BUF', 'Carolina': 'CAR',
    'Chicago': 'CHI', 'Cincinnati': 'CIN', 'Cleveland': 'CLE', 'Dallas': 'DAL', 'Denver': 'DEN',
    'Detroit': 'DET', 'Green Bay': 'GB', 'Houston': 'HOU', 'Indianapolis': 'IND', 'Jacksonville': 'JAX',
    'Kansas City': 'KC', 'Las Vegas': 'LV', 'L.A. Chargers': 'LAC', 'L.A. Rams': 'LA',
    'Miami': 'MIA', 'Minnesota': 'MIN', 'New England': 'NE', 'New Orleans': 'NO',
    'N.Y. Giants': 'NYG', 'N.Y. Jets': 'NYJ', 'Philadelphia': 'PHI', 'Pittsburgh': 'PIT',
    'San Francisco': 'SF', 'Seattle': 'SEA', 'Tampa Bay': 'TB', 'Tennessee': 'TEN',
    'Washington': 'WAS', 'Oakland': 'OAK', 'St. Louis': 'STL'
}

# === Sort and reshape odds ===
odds_sorted = odds.sort_values(by=['season', 'week', 'rotation']).reset_index(drop=True)
odds_sorted['game_id_index'] = odds_sorted.index // 2
odds_sorted['team'] = odds_sorted['team'].replace(team_name_map)

fields_to_pivot = [
    'score', 'wagers',
    'BETONLINE_spread', 'BETONLINE_odds',
    'BOVODA_spread', 'BOVODA_odds',
    'BookMaker_spread', 'BookMaker_odds',
    'Heritage_spread', 'Heritage_odds',
    'everygame_spread', 'everygame_odds',
    'JustBET_spread', 'JustBET_odds',
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

# === Map playoff labels (or strings) to week numbers ===
def remap_week(row):
    season = row['season_home']
    week = row['week_home']
    if pd.isna(week):
        return None
    if isinstance(week, str):
        week = week.strip().lower()
        if week == 'wild card': return 18 if season < 2021 else 19
        if week == 'divisional': return 19 if season < 2021 else 20
        if week == 'conference': return 20 if season < 2021 else 21
        if week == 'super bowl': return 21 if season < 2021 else 22
        if week.isdigit(): return int(week)
        return None
    elif isinstance(week, (int, float)):
        return int(week)
    return None

merged_clean['adjusted_week'] = merged_clean.apply(remap_week, axis=1)
merged_clean = merged_clean[merged_clean['adjusted_week'].notnull()].copy()

# === Build join_key to match games['game_id']
merged_clean['season_str'] = merged_clean['season_home'].astype(str).str.zfill(4)
merged_clean['adjusted_week_str'] = merged_clean['adjusted_week'].astype(int).astype(str).str.zfill(2)
merged_clean['join_key'] = (
    merged_clean['season_str'] + '_' +
    merged_clean['adjusted_week_str'] + '_' +
    merged_clean['team_away'] + '_' +
    merged_clean['team_home']
)
merged_clean = merged_clean.rename(columns={'season_home': 'season', 'adjusted_week': 'week'})

# === Apply PK spread fix: if spread is null but odds is not null, set spread to 0
spread_cols = [col for col in merged_clean.columns if '_spread_' in col]
for spread_col in spread_cols:
    odds_col = spread_col.replace('spread', 'odds')
    if odds_col in merged_clean.columns:
        condition = merged_clean[spread_col].isna() & merged_clean[odds_col].notna()
        merged_clean.loc[condition, spread_col] = 0.0

# === Join odds to games
games['join_key'] = games['game_id']
games_final = pd.merge(
    games,
    merged_clean,
    on='join_key',
    how='left',
    suffixes=('', '_odds')
)

display(games_final.head(500))

# COMMAND ----------

# MAGIC %md
# MAGIC # Actually ingest into model and see if we get a better outcome with given data (havent completed yet)

# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pandas as pd

# === Use enriched dataset ===
games = games_final.copy()

# Step 1: Recalculate label (home team win) for 2024
games['home_win'] = games['home_score'] > games['away_score']

# Step 2: Encode categorical features
for col in games.columns:
    if games[col].dtype == 'object':
        games[col] = games[col].astype('category').cat.codes

# Step 3: Define usable features (exclude labels and targets)
exclude = [
    'home_win', 'home_score', 'away_score',
    'margin', 'total', 'result', 'old_game_id', 'gsis',
    'nfl_detail_id', 'pfr', 'pff', 'espn', 'ftn',
    'gameday', 'gametime', 'matchup', 'stadium', 'stadium_id',
    'game_id', 'join_key', 'score_home', 'score_away'
]
features = [col for col in games.columns if col not in exclude]

# Step 4: Drop missing rows
games_clean = games[features + ['home_win']].dropna()

# Step 5: Train/test split
latest_season = games_clean['season'].max()
train_df = games_clean[games_clean['season'] < latest_season]
test_df = games_clean[games_clean['season'] == latest_season]

X_train = train_df[features]
y_train = train_df['home_win']
X_test = test_df[features]
y_test = test_df['home_win']

# Step 6: Train the model
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Step 7: Evaluate
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f"Model test accuracy on {latest_season}: {accuracy:.2%}")

# Step 8: Feature importances
importances = model.feature_importances_
feature_importance_df = pd.DataFrame({
    'Feature': features,
    'Importance': importances
}).sort_values(by='Importance', ascending=True)

# Step 9: Plot
plt.figure(figsize=(10, len(features) * 0.3))
plt.barh(feature_importance_df['Feature'], feature_importance_df['Importance'])
plt.xlabel('Importance Score')
plt.title('Feature Importances (Random Forest) — Games + Odds Features')
plt.tight_layout()
plt.grid(True, axis='x', linestyle='--', alpha=0.5)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### ## # Simulation

# COMMAND ----------

# Create a DataFrame with test results and predictions
sim_df = test_df.copy()
sim_df['predicted_home_win'] = y_pred

# Determine which team we bet on
sim_df['bet_team'] = sim_df['predicted_home_win'].map({True: 'home', False: 'away'})

# Pull the appropriate odds
sim_df['bet_odds'] = sim_df.apply(
    lambda row: row['BETONLINE_odds_home'] if row['bet_team'] == 'home' else row['BETONLINE_odds_away'],
    axis=1
)

# Convert American odds to decimal payout multiplier
def american_to_multiplier(odds):
    if pd.isna(odds):
        return None
    if odds > 0:
        return 1 + odds / 100
    else:
        return 1 + 100 / abs(odds)

sim_df['multiplier'] = sim_df['bet_odds'].apply(american_to_multiplier)

# Determine if the bet was correct
sim_df['bet_won'] = (
    (sim_df['predicted_home_win'] == sim_df['home_win'])
)

# Calculate profit/loss for each bet
sim_df['payout'] = sim_df.apply(
    lambda row: 100 * (row['multiplier'] - 1) if row['bet_won'] else -100,
    axis=1
)

# Summary
total_profit = sim_df['payout'].sum()
num_games = len(sim_df)
roi = total_profit / (num_games * 100)

print(f"📈 Total profit from betting $100 per game: ${total_profit:.2f}")
print(f"🏈 Total games bet: {num_games}")
print(f"💰 ROI: {roi:.2%}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### ## If we only bet on ravens games

# COMMAND ----------

# Use known encoding map from original data
team_codes = pd.read_csv("games.csv")['home_team'].astype('category').cat.categories
team_map = dict(enumerate(team_codes))

# Decode team names
ravens_df['home_team_name'] = ravens_df['home_team'].map(team_map)
ravens_df['away_team_name'] = ravens_df['away_team'].map(team_map)

# Add opponent name
ravens_df['opponent'] = ravens_df.apply(
    lambda row: row['away_team_name'] if row['home_team'] == 2 else row['home_team_name'],
    axis=1
)

# Calculate cumulative balance
ravens_df = ravens_df.sort_values(by=['season', 'week']).reset_index(drop=True)
ravens_df['balance'] = ravens_df['payout'].cumsum()

# Summary stats
total_profit = ravens_df['payout'].sum()
num_bets = len(ravens_df)
roi = total_profit / (num_bets * 100) if num_bets else 0

print(f"🟣 Ravens games bet: {num_bets}")
print(f"💰 Total profit: ${total_profit:.2f}")
print(f"📈 ROI: {roi:.2%}")
print(f"💼 Final balance: ${ravens_df['balance'].iloc[-1]:.2f}")

# Display detailed results
display(ravens_df[[
    'season', 'week',
    'home_team_name', 'away_team_name', 'opponent',
    'bet_team', 'bet_odds', 'bet_won', 'payout', 'balance'
]])


# COMMAND ----------

# MAGIC %md
# MAGIC
