# dashboard/app.py
import os
import io
import csv
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from flask import Flask, render_template, jsonify, send_file, abort, request

# ===== הגדרות כלליות =====
APP_TZ = timezone.utc  # מציגים הכל ב-UTC בדשבורד כברירת מחדל
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# נתיב לוגים של הבוט (ניתן לשנות עם ENV בשם LOG_DIR)
DEFAULT_LOG_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "bot", "logs"))
LOG_DIR = os.getenv("LOG_DIR", DEFAULT_LOG_DIR)

TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")

app = Flask(__name__, template_folder="templates", static_folder="static")

# ===== עזרי DB (psycopg v3) — לא מחייב, עובד רק אם מוגדר DATABASE_URL =====
_PSYCOPG_OK = False
try:
    import psycopg as _psycopg
    from psycopg.rows import dict_row as _dict_row
    _PSYCOPG_OK = True
except Exception:
    _PSYCOPG_OK = False

def _db_available() -> bool:
    return _PSYCOPG_OK and bool(os.getenv("DATABASE_URL"))

def _get_conn():
    if not _db_available():
        raise RuntimeError("DATABASE_URL missing or psycopg not installed")
    return _psycopg.connect(os.getenv("DATABASE_URL"))

def _to_primitive(v):
    if isinstance(v, Decimal):
        return float(v)
    return v

def _rows_to_list(rows):
    return [{k: _to_primitive(v) for k, v in r.items()} for r in rows]

def db_last_equity(limit: int = 1):
    """קריאה מהטבלה equity_curve (אם זמינה)"""
    with _get_conn() as conn, conn.cursor(row_factory=_dict_row) as cur:
        cur.execute('SELECT time, equity FROM "equity_curve" ORDER BY "time" DESC LIMIT %s;', (limit,))
        return cur.fetchall()

def db_last_trades(limit: int = 50):
    """קריאה מהטבלה trades (אם זמינה)"""
    with _get_conn() as conn, conn.cursor(row_factory=_dict_row) as cur:
        cur.execute('SELECT * FROM "trades" ORDER BY "time" DESC LIMIT %s;', (limit,))
        return cur.fetchall()

def current_status_db(quiet_sec: int | None = None):
    """סטטוס מה-DB לפי עדכון אחרון ב-equity/trades. ברירת מחדל: חלון שקט 900 שניות."""
    if not _db_available():
        return {"status": "STOPPED", "last_update": None, "age_sec": None, "source": "db"}
    if quiet_sec is None:
        quiet_sec = int((os.getenv("DASH_QUIET_SEC") or "900").strip())
    now_utc = datetime.now(timezone.utc)

    last_ts = None
    try:
        eq = db_last_equity(limit=1)
        if eq:
            t = eq[0]["time"]
            if last_ts is None or t > last_ts:
                last_ts = t
    except Exception:
        pass

    try:
        tr = db_last_trades(limit=1)
        if tr:
            t = tr[0]["time"]
            if last_ts is None or t > last_ts:
                last_ts = t
    except Exception:
        pass

    if not last_ts:
        return {"status": "STOPPED", "last_update": None, "age_sec": None, "source": "db"}

    age = (now_utc - last_ts).total_seconds()
    return {
        "status": "RUNNING" if age <= quiet_sec else "STOPPED",
        "last_update": last_ts.astimezone(timezone.utc).isoformat(),
        "age_sec": int(age),
        "source": "db",
    }

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

# ===== לוגיקת סטטוס (CSV) =====
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

# ===== ראוטים (קיימים) =====
@app.route("/")
def index():
    tmpl = _pick_dashboard_template()
    # סטטוס מאוחד: DB אם זמין; אחרת CSV
    if _db_available():
        try:
            uni = current_status_db()
        except Exception:
            s = _bot_status()
            uni = {"status": s["status"], "last_update": s["last_equity_ts"], "age_sec": s["age_sec"], "source": "csv"}
    else:
        s = _bot_status()
        uni = {"status": s["status"], "last_update": s["last_equity_ts"], "age_sec": s["age_sec"], "source": "csv"}

    if tmpl:
        return render_template(tmpl, unified_status=uni)

    return (
        f"<h1>Trading Dashboard</h1>"
        f"<p>Status: <b>{uni['status']}</b> <small>(source: {uni['source']})</small></p>"
        f"<p>Last update: {uni['last_update']}</p>"
        f"<p>Age (sec): {uni['age_sec']}</p>"
        f"<p>LOG_DIR: {LOG_DIR}</p>"
        f"<p>Unified status API: <code>/api/status</code></p>",
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

# ===== ראוטים חדשים – DB =====
@app.route("/api/status_db")
def api_status_db():
    return jsonify(current_status_db())

@app.route("/api/equity_db")
def api_equity_db():
    if not _db_available():
        return jsonify({"error": "DB not available"}), 503
    try:
        limit = int(request.args.get("limit", "200"))
    except Exception:
        limit = 200
    rows = _rows_to_list(db_last_equity(limit=limit)[::-1])  # chronological asc לגרפים
    return jsonify(rows)

@app.route("/api/trades_db")
def api_trades_db():
    if not _db_available():
        return jsonify({"error": "DB not available"}), 503
    try:
        limit = int(request.args.get("limit", "100"))
    except Exception:
        limit = 100
    rows = _rows_to_list(db_last_trades(limit=limit))
    return jsonify(rows)

# ===== Unified status endpoint (prefers DB, falls back to CSV) =====
def _status_csv_only():
    st = _bot_status()
    return {
        "status": st["status"],
        "last_update": st["last_equity_ts"],
        "age_sec": st["age_sec"],
        "source": "csv",
    }

@app.route("/api/status")
def api_status_unified():
    # אם יש DB ו-psycopg — נעדיף אותו
    if _db_available():
        try:
            return jsonify(current_status_db())
        except Exception:
            pass
    # נפילה חכמה ל-CSV (קיים כבר)
    return jsonify(_status_csv_only())

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
