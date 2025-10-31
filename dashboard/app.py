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
    """
    דואג שתיקיית הלוגים וקבצי ה-CSV קיימים.
    אם קובץ חסר—ייווצר עם כותרות בלבד.
    לא מוסיף שורות נתונים פיקטיביות.
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"])

    if not os.path.exists(EQUITY_CSV):
        with open(EQUITY_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["time", "equity"])


# נקרא פעם אחת בעת עליית המודול/האפליקציה
_ensure_logs_and_headers()


# ===== עזרי קבצים =====
def _read_csv(path, limit=None):
    """קורא CSV כ-list[dict]. אם limit סופק, יחזיר רק את הסוף. חסין לשגיאות קלות."""
    rows = []
    if not os.path.exists(path):
        return rows
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                clean = {(k.strip() if isinstance(k, str) else k): v for k, v in row.items()}
                rows.append(clean)
    except Exception:
        # במקרה של קובץ שבור/קידוד לא תקין – נחזיר ריק ולא נפיל את השרת
        return []
    if limit:
        try:
            limit = int(limit)
        except Exception:
            limit = None
    return rows[-limit:] if limit else rows


def _parse_iso(ts: str):
    """ממיר טקסט לזמן. תומך ב־Z, +00:00, וגם בלי אזור."""
    if not ts or not isinstance(ts, str):
        return None
    ts = ts.strip()
    if not ts:
        return None
    try:
        # normalize "Z"
        ts_norm = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(ts_norm)
    except Exception:
        # נסיון פורמטים נפוצים
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(ts, fmt)
                return dt.replace(tzinfo=APP_TZ)
            except Exception:
                continue
    return None


def _last_timestamp(row: dict):
    """מחלץ timestamp מתוך שורה לפי מספר שמות מקובלים."""
    if not isinstance(row, dict):
        return None
    for key in ("time", "timestamp", "ts", "datetime"):
        if key in row and row[key]:
            return _parse_iso(row[key])
    return None


# ===== לוגיקת סטטוס =====
def _bot_status():
    """
    קובע RUNNING/STOPPED לפי ה־heartbeat בקובץ equity_curve.csv:
    אם השורה האחרונה חדשה מ־90 שניות — RUNNING, אחרת STOPPED.
    """
    eq_last = _read_csv(EQUITY_CSV, limit=1)
    now = datetime.now(APP_TZ)

    if not eq_last:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None}

    last_ts = _last_timestamp(eq_last[-1])
    if not last_ts:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None}

    # אם חסר tzinfo נוסיף UTC
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=APP_TZ)

    age = (now - last_ts).total_seconds()
    status = "RUNNING" if age <= 90 else "STOPPED"
    return {"status": status, "last_equity_ts": last_ts.isoformat(), "age_sec": int(age)}


# ===== בחירת תבנית דשבורד =====
def _pick_dashboard_template():
    """מחפש index.html או dashboard.html בתיקיית templates."""
    templates_dir = os.path.join(BASE_DIR, "templates")
    idx = os.path.join(templates_dir, "index.html")
    dash = os.path.join(templates_dir, "dashboard.html")
    if os.path.exists(idx):
        return "index.html"
    if os.path.exists(dash):
        return "dashboard.html"
    return None


# ===== ראוטים =====
@app.route("/")
def index():
    tmpl = _pick_dashboard_template()
    if tmpl:
        return render_template(tmpl)
    # fallback טקסטואלי אם אין תבנית
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
    """JSON לטריידים (ללא סינון), עקומת הון וסטטוס."""
    trades = _read_csv(TRADES_CSV)   # סינון בצד לקוח אם נדרש
    equity = _read_csv(EQUITY_CSV)   # לציור גרף
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
    """
    הורדה ישירה של trades.csv.
    אם אין קובץ—נחזיר קובץ ריק עם כותרות סטנדרטיות.
    """
    headers = ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"]
    if not os.path.exists(TRADES_CSV):
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
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
    """הורדה ישירה של equity_curve.csv או קובץ ריק עם כותרות time,equity."""
    headers = ["time", "equity"]
    if not os.path.exists(EQUITY_CSV):
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
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
    """אליאס תואם-עבר לעדכון ישנים — מוריד trades.csv אם קיים."""
    if os.path.exists(TRADES_CSV):
        return send_file(TRADES_CSV, as_attachment=True, download_name="trades.csv")
    abort(404, description="trades.csv not found")


@app.route("/health")
def health():
    """בדיקת בריאות פשוטה לפריסה/מוניטורינג."""
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


if __name__ == "__main__":
    # PORT לברירת־מחדל: 10000 (ניתן לשנות עם ENV בשם PORT)
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
