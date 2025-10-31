# dashboard/app.py
<<<<<<< HEAD
import os, csv, io
from datetime import datetime, timezone
from flask import Flask, render_template, jsonify, send_file

APP_TZ = timezone.utc  # מציגים הכל ב-UTC בדשבורד
BASE_DIR = os.path.dirname(__file__)
# נתיב לוגים של הבוט (ניתן לשנות עם ENV בשם LOG_DIR)
LOG_DIR = os.getenv("LOG_DIR", os.path.abspath(os.path.join(BASE_DIR, "..", "bot", "logs")))
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")

app = Flask(__name__, template_folder="templates", static_folder="static")

def _read_csv(path, limit=None):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    if limit:
        return rows[-limit:]
    return rows

def _parse_iso(ts):
    # תומך גם ב־...+00:00 וגם בלי timezone
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def _bot_status():
    """
    קובע RUNNING/STOPPED לפי ה־heartbeat בקובץ equity_curve.csv:
    אם השורה האחרונה חדשה מ־90 שניות — RUNNING, אחרת STOPPED.
    """
    eq = _read_csv(EQUITY_CSV, limit=1)
    now = datetime.now(APP_TZ)
    if not eq:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None}
    last_ts = _parse_iso(eq[-1]["time"])
    if not last_ts:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None}
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=APP_TZ)
    age = (now - last_ts).total_seconds()
    status = "RUNNING" if age <= 90 else "STOPPED"
    return {"status": status, "last_equity_ts": last_ts.isoformat(), "age_sec": int(age)}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/data")
def data():
    # שולפים טריידים ואקוויטי לתצוגה + סטטוס
    trades = _read_csv(TRADES_CSV)  # סנן בצד הלקוח
    equity = _read_csv(EQUITY_CSV)  # לצייר גרף
    st = _bot_status()
    return jsonify({
        "status": st["status"],
        "last_equity_ts": st["last_equity_ts"],
        "age_sec": st["age_sec"],
        "now_utc": datetime.now(APP_TZ).isoformat(),
        "trades": trades,
        "equity": equity,
    })

@app.route("/export/trades.csv")
def export_trades():
    # הורדה ישירה של trades.csv (אם אין—קובץ ריק עם כותרות)
    if not os.path.exists(TRADES_CSV):
        output = io.StringIO()
        w = csv.writer(output)
        w.writerow(["time","connector","symbol","type","side","price","qty","pnl","equity"])
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="trades.csv",
        )
    return send_file(TRADES_CSV, mimetype="text/csv", as_attachment=True, download_name="trades.csv")

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
=======
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
>>>>>>> parent of 4b323cb (Update app.py)
