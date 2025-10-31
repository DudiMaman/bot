# dashboard/app.py
import os
import io
import csv
from datetime import datetime, timezone
from flask import Flask, render_template, jsonify, send_file, abort

# ===== הגדרות כלליות =====
APP_TZ = timezone.utc  # מציגים הכל ב-UTC בדשבורד כברירת מחדל
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# נתיב לוגים של הבוט (ניתן לשנות עם ENV בשם LOG_DIR)
DEFAULT_LOG_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "bot", "logs"))
LOG_DIR = os.getenv("LOG_DIR", DEFAULT_LOG_DIR)

TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")

app = Flask(__name__, template_folder="templates", static_folder="static")


# ===== יצירת תיקיות/קבצים חסרים אוטומטית =====
def _ensure_logs_and_headers():
    os.makedirs(LOG_DIR, exist_ok=True)
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"]
            )
    if not os.path.exists(EQUITY_CSV):
        with open(EQUITY_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["time", "equity"])

_ensure_logs_and_headers()


# ===== עזרי קבצים =====
def _read_csv(path, limit=None):
    rows = []
    if not os.path.exists(path):
        return rows
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                rows.append({(k.strip() if isinstance(k, str) else k): v for k, v in row.items()})
    except Exception:
        return []
    if limit:
        try:
            limit = int(limit)
        except Exception:
            limit = None
    return rows[-limit:] if limit else rows


def _parse_iso(ts: str):
    if not ts or not isinstance(ts, str):
        return None
    ts = ts.strip()
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                return datetime.strptime(ts, fmt).replace(tzinfo=APP_TZ)
            except Exception:
                continue
    return None


def _last_timestamp(row: dict):
    if not isinstance(row, dict):
        return None
    for key in ("time", "timestamp", "ts", "datetime"):
        if key in row and row[key]:
            return _parse_iso(row[key])
    return None


# ===== לוגיקת סטטוס =====
def _bot_status():
    eq_last = _read_csv(EQUITY_CSV, limit=1)
    now = datetime.now(APP_TZ)
    if not eq_last:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None}
    last_ts = _last_timestamp(eq_last[-1])
    if not last_ts:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None}
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=APP_TZ)
    age = (now - last_ts).total_seconds()
    status = "RUNNING" if age <= 90 else "STOPPED"
    return {"status": status, "last_equity_ts": last_ts.isoformat(), "age_sec": int(age)}


# ===== בחירת תבנית דשבורד =====
def _pick_dashboard_template():
    templates_dir = os.path.join(BASE_DIR, "templates")
    if os.path.exists(os.path.join(templates_dir, "index.html")):
        return "index.html"
    if os.path.exists(os.path.join(templates_dir, "dashboard.html")):
        return "dashboard.html"
    return None


# ===== ראוטים =====
@app.route("/")
def index():
    tmpl = _pick_dashboard_template()
    if tmpl:
        return render_template(tmpl)
    st = _bot_status()
    return (
        f"<h1>Trading Dashboard</h1>"
        f"<p>Status: <b>{st['status']}</b></p>"
        f"<p>Last equity timestamp: {st['last_equity_ts']}</p>"
        f"<p>Age (sec): {st['age_sec']}</p>"
        f"<p>LOG_DIR: {LOG_DIR}</p>",
        200,
        {"Content-Type": "text/html; charset=utf-8"},
    )


@app.route("/data")
def data():
    trades = _read_csv(TRADES_CSV)
    equity = _read_csv(EQUITY_CSV)
    st = _bot_status()
    return jsonify(
        {
            "status": st["status"],
            "last_equity_ts": st["last_equity_ts"],
            "age_sec": st["age_sec"],
            "now_utc": datetime.now(APP_TZ).isoformat(),
            "trades": trades,
            "equity": equity,
        }
    )


@app.route("/export/trades.csv")
def export_trades():
    if not os.path.exists(TRADES_CSV):
        output = io.StringIO()
        csv.writer(output).writerow(
            ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"]
        )
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="trades.csv",
        )
    return send_file(TRADES_CSV, mimetype="text/csv", as_attachment=True, download_name="trades.csv")


@app.route("/export/equity_curve.csv")
def export_equity():
    if not os.path.exists(EQUITY_CSV):
        output = io.StringIO()
        csv.writer(output).writerow(["time", "equity"])
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="equity_curve.csv",
        )
    return send_file(EQUITY_CSV, mimetype="text/csv", as_attachment=True, download_name="equity_curve.csv")


@app.route("/download")
def download_csv_alias():
    if os.path.exists(TRADES_CSV):
        return send_file(TRADES_CSV, as_attachment=True, download_name="trades.csv")
    abort(404, description="trades.csv not found")


@app.route("/health")
def health():
    st = _bot_status()
    return jsonify(
        {
            "ok": os.path.exists(TRADES_CSV) or os.path.exists(EQUITY_CSV),
            "has_trades_csv": os.path.exists(TRADES_CSV),
            "has_equity_csv": os.path.exists(EQUITY_CSV),
            "status": st["status"],
            "last_equity_ts": st["last_equity_ts"],
            "age_sec": st["age_sec"],
            "log_dir": LOG_DIR,
        }
    ), 200


# ===== DEBUG (מאובטח ב־ENV) =====
def _debug_enabled():
    return os.getenv("ENABLE_DEBUG", "0") == "1"

@app.route("/debug/seed", methods=["POST", "GET"])
def debug_seed():
    if not _debug_enabled():
        abort(404)
    os.makedirs(LOG_DIR, exist_ok=True)
    # seed trades
    with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"])
        w.writerow(["2025-10-31T12:00:00Z", "Bybit", "BTCUSDT", "MARKET", "BUY", "68000", "0.01", "3.2", "100003.2"])
    # seed equity
    with open(EQUITY_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time", "equity"])
        w.writerow(["2025-10-31T12:00:05Z", "100000"])
    return jsonify({"ok": True, "message": "seeded"}), 200

@app.route("/debug/pulse", methods=["POST", "GET"])
def debug_pulse():
    if not _debug_enabled():
        abort(404)
    now = datetime.now(APP_TZ).isoformat()
    # append equity heartbeat
    with open(EQUITY_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([now, "100000"])
    return jsonify({"ok": True, "now_utc": now}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
