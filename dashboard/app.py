# dashboard/app.py
# ================================================================
# Trading Dashboard PRO — מעוצב עם Bootstrap + תצוגות מתקדמות
# ================================================================

import os
import pandas as pd
from datetime import datetime
from flask import Flask, render_template, request, send_file
import pytz

app = Flask(__name__)

# נתיבי קבצים
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.join(BASE_DIR, "..", "bot")
LOG_DIR = os.path.join(BOT_DIR, "logs")
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")

TZ = pytz.timezone("Asia/Jerusalem")  # תיקון אזור זמן

def read_trades():
    if not os.path.exists(TRADES_CSV):
        return pd.DataFrame(columns=["time","connector","symbol","type","side","price","qty","pnl","equity"])
    df = pd.read_csv(TRADES_CSV)
    df["time"] = pd.to_datetime(df["time"]).dt.tz_convert(TZ)
    df["PnL%"] = ((df["pnl"].fillna(0) / (df["equity"].shift(1).fillna(df["equity"]))) * 100).round(2)
    df["status"] = df["type"].apply(lambda x: "Open" if x == "ENTER" else "Closed")
    return df

def read_equity():
    if not os.path.exists(EQUITY_CSV):
        return pd.DataFrame(columns=["time","equity"])
    df = pd.read_csv(EQUITY_CSV)
    df["time"] = pd.to_datetime(df["time"]).dt.tz_convert(TZ)
    return df

@app.route("/", methods=["GET", "POST"])
def dashboard():
    df = read_trades()
    eq = read_equity()

    # ===== פילטרים =====
    start = request.args.get("start")
    end = request.args.get("end")
    symbol = request.args.get("symbol")
    side = request.args.get("side")

    if start:
        df = df[df["time"] >= pd.to_datetime(start)]
    if end:
        df = df[df["time"] <= pd.to_datetime(end)]
    if symbol and symbol != "ALL":
        df = df[df["symbol"] == symbol]
    if side and side != "ALL":
        df = df[df["side"].str.lower() == side.lower()]

    # ===== סיכום עסקאות פתוחות =====
    open_trades = df[df["type"] == "ENTER"]
    closed_trades = df[df["type"].isin(["SL", "TP1", "TP2", "TIME"])]
    open_positions = len(open_trades) - len(closed_trades)
    unrealized_pnl = df[df["type"] == "ENTER"]["pnl"].sum() if not df.empty else 0
    exposure = df[df["type"] == "ENTER"]["qty"].sum() if not df.empty else 0

    return render_template(
        "dashboard.html",
        trades=df.sort_values("time", ascending=False),
        equity=eq,
        symbols=sorted(df["symbol"].unique()) if not df.empty else [],
        open_positions=open_positions,
        exposure=exposure,
        unrealized_pnl=unrealized_pnl,
        start=start,
        end=end,
        side=side or "ALL",
        symbol=symbol or "ALL"
    )

@app.route("/download")
def download_csv():
    return send_file(TRADES_CSV, as_attachment=True, download_name="trades.csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
