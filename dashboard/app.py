from flask import Flask, render_template, jsonify, request
import os, json, time
import pandas as pd
from datetime import datetime, timezone

app = Flask(__name__, template_folder="templates", static_folder="static")

# Paths to bot logs (as mounted/visible to this service)
BOT_LOGS_DIR = os.environ.get("BOT_LOGS_DIR", "/opt/render/project/src/bot/logs")
EQUITY_CSV = os.path.join(BOT_LOGS_DIR, "equity_curve.csv")
TRADES_CSV = os.path.join(BOT_LOGS_DIR, "trades.csv")

# Path for bot control/status file (shared volume or same repo if simple)
STATE_DIR = os.environ.get("STATE_DIR", "/opt/render/project/src/state")
os.makedirs(STATE_DIR, exist_ok=True)
BOT_STATE_JSON = os.path.join(STATE_DIR, "bot_state.json")

def read_bot_state():
    if os.path.exists(BOT_STATE_JSON):
        try:
            with open(BOT_STATE_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"status": "STOPPED", "updated_at": None}

def write_bot_state(status):
    payload = {"status": status, "updated_at": datetime.now(timezone.utc).isoformat()}
    with open(BOT_STATE_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/data")
def data():
    # Read trades
    trades = []
    try:
        if os.path.exists(TRADES_CSV):
            df = pd.read_csv(TRADES_CSV)
            # Ensure columns exist
            for col in ["time","connector","symbol","type","side","price","qty","pnl","equity"]:
                if col not in df.columns:
                    df[col] = ""
            # Show latest first
            df = df.tail(500).iloc[::-1]
            trades = df.to_dict(orient="records")
    except Exception:
        trades = []

    # Read equity
    equity_points = []
    try:
        if os.path.exists(EQUITY_CSV):
            edf = pd.read_csv(EQUITY_CSV)
            for _, r in edf.iterrows():
                equity_points.append({
                    "t": r.get("time"),
                    "y": float(r.get("equity", 0))
                })
    except Exception:
        equity_points = []

    # Bot state
    state = read_bot_state()

    return jsonify({
        "last_refresh_utc": datetime.now(timezone.utc).isoformat(),
        "status": state.get("status", "STOPPED"),
        "status_updated_at": state.get("updated_at"),
        "equity": equity_points,
        "trades": trades
    })

@app.route("/pause", methods=["POST"])
def pause():
    write_bot_state("STOPPED")
    return jsonify({"ok": True})

@app.route("/resume", methods=["POST"])
def resume():
    write_bot_state("RUNNING")
    return jsonify({"ok": True})

if __name__ == "__main__":
    # For local debug
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)
