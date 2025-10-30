# dashboard/app.py
# Trading Dashboard PRO — Bootstrap UI, filters, CSV export, tz fixes

import os
import pandas as pd
from datetime import datetime
from flask import Flask, render_template, request, send_file, abort
import pytz
import traceback

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.join(BASE_DIR, "..", "bot")
LOG_DIR = os.path.join(BOT_DIR, "logs")
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")

TZ = pytz.timezone("Asia/Jerusalem")  # show everything in Israel time

def _read_csv_safe(path, cols):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols)
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"[ERROR] reading {path}: {e}")
        return pd.DataFrame(columns=cols)

def _tz_series_jerusalem(s):
    # make sure timestamps are parsed and localized to UTC, then convert to IL time
    try:
        dt = pd.to_datetime(s, utc=True, errors="coerce")
        return dt.dt.tz_convert(TZ)
    except Exception:
        # fallback: try localize first if they’re naive
        dt = pd.to_datetime(s, errors="coerce")
        try:
            dt = dt.dt.tz_localize("UTC").dt.tz_convert(TZ)
        except Exception:
            pass
        return dt

def read_trades():
    df = _read_csv_safe(
        TRADES_CSV,
        ["time","connector","symbol","type","side","price","qty","pnl","equity"]
    )
    if df.empty:
        return df

    # Ensure required columns exist
    for c in ["pnl","equity","qty","price"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["time"] = _tz_series_jerusalem(df["time"])

    # PnL% (פשוט/גס): pnl חלקי equity קודם (או נוכחי אם אין)
    prev_eq = df["equity"].shift(1).fillna(df["equity"])
    with pd.option_context("mode.use_inf_as_na", True):
        df["PnL%"] = ((df["pnl"].fillna(0) / prev_eq.replace(0, pd.NA)) * 100).round(2).fillna(0)

    # Status: ENTER = Open, אחרת Closed (פשוט למען UI — אפשר לשפר בעתיד)
    df["status"] = df["type"].apply(lambda x: "Open" if str(x).upper() == "ENTER" else "Closed")

    # סידור נתונים
    df = df.sort_values("time", ascending=False).reset_index(drop=True)
    return df

def read_equity():
    df = _read_csv_safe(EQUITY_CSV, ["time","equity"])
    if df.empty:
        return df
    df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
    df["time"] = _tz_series_jerusalem(df["time"])
    df = df.sort_values("time").reset_index(drop=True)
    return df

@app.route("/")
def dashboard():
    try:
        df = read_trades()
        eq = read_equity()

        # ---- Filters ----
        start = request.args.get("start")  # yyyy-mm-dd
        end = request.args.get("end")
        symbol = request.args.get("symbol")
        side = request.args.get("side")

        filtered = df.copy()
        if not filtered.empty:
            if start:
                try:
                    start_dt = TZ.localize(datetime.fromisoformat(start + " 00:00:00"))
                    filtered = filtered[filtered["time"] >= start_dt]
                except Exception:
                    pass
            if end:
                try:
                    end_dt = TZ.localize(datetime.fromisoformat(end + " 23:59:59"))
                    filtered = filtered[filtered["time"] <= end_dt]
                except Exception:
                    pass
            if symbol and symbol != "ALL":
                filtered = filtered[filtered["symbol"] == symbol]
            if side and side != "ALL":
                filtered = filtered[filtered["side"].str.lower() == side.lower()]

        # ---- Open positions card (simple heuristic) ----
        if filtered.empty:
            open_positions = 0
            exposure = 0.0
            unrealized_pnl = 0.0
            symbols_list = []
        else:
            enters = filtered[filtered["type"].str.upper() == "ENTER"]
            closes = filtered[filtered["type"].str.upper().isin(["SL","TP1","TP2","TIME"])]
            open_positions = max(len(enters) - len(closes), 0)
            exposure = float(enters["qty"].fillna(0).sum())
            # הסתברות ל־unrealized PnL לא באמת מדויקת בלי מחיר שוק חי — נציג סכום pnl של ENTER בלבד כקירוב 0
            unrealized_pnl = 0.0
            symbols_list = sorted(df["symbol"].dropna().unique().tolist())

        now_jeru = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

        return render_template(
            "dashboard.html",
            trades=filtered,
            equity=eq,
            symbols=symbols_list,
            open_positions=open_positions,
            exposure=exposure,
            unrealized_pnl=unrealized_pnl,
            start=start,
            end=end,
            side=(side or "ALL"),
            symbol=(symbol or "ALL"),
            now_jeru=now_jeru
        )
    except Exception as e:
        print("[ERROR] / handler crashed:", e)
        traceback.print_exc()
        return abort(500)

@app.route("/download")
def download_csv():
    if not os.path.exists(TRADES_CSV):
        return abort(404)
    return send_file(TRADES_CSV, as_attachment=True, download_name="trades.csv")

@app.route("/health")
def health():
    return {"ok": True, "time": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")}

if __name__ == "__main__":
    # local run
    app.run(host="0.0.0.0", port=5000, debug=True)
