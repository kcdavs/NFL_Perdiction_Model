import os
from flask import Flask, request, jsonify
import joblib, pandas as pd
from datetime import datetime

app = Flask(__name__)

# Load your trained model once at startup
model = joblib.load("model/model.pkl")

@app.route("/predict", methods=["POST"])
def predict():
    """
    Expects JSON: { "data": [ {feature1: val, feature2: val, …}, … ] }
    Returns JSON: { "predictions": [0,1,0,…] }
    """
    payload = request.get_json(force=True)
    df = pd.DataFrame(payload["data"])
    preds = model.predict(df).astype(int).tolist()
    return jsonify({"predictions": preds})

@app.route("/", methods=["GET"])
def home():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"Hi, Kasey’s API is live. Server time: {now}"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
