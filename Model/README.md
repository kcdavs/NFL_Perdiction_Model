🧠 Model Overview
The model/ folder contains all code related to training, evaluating, and improving the machine learning models used to generate insights from the NFL gambling dataset.

🔍 Purpose
This module is focused on using the clean, per-game dataset produced by the ingestion pipeline to:

Identify patterns in betting lines vs. actual outcomes

Generate actionable insights based on model predictions

Surface edge cases, trends, or opportunities that could inform weekly decision-making

🧰 Structure
train/ – scripts and notebooks for training models (e.g., logistic regression, XGBoost, etc.)

predict/ – code for running predictions on the current or upcoming week's data

evaluate/ – tools for testing model performance across historical data

Accuracy vs. spread

Profit/loss simulations

Calibration plots

features/ – feature engineering logic, e.g., calculating recent team performance, public betting sentiment, etc.

🔄 Iterative Development
As the model evolves:

Results from each version will be logged for comparison

Evaluation code will help determine whether new features or architectures are genuinely improving predictive performance

Goal is not just to fit historical data but to build a model that generalizes to new weeks and adapts over time

