# web-api/app.py
from flask import Flask
from datetime import datetime
import os

app = Flask(__name__)

@app.route("/")
def hello():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"hi this kasey trying to test out a remote server the time is {now}"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
