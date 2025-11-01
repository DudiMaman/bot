# dashboard/app.py
import os
import io
import csv
import json
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify, send_file, abort, request

# ===== הגדרות כלליות =====
APP_TZ = timezone.utc  # השרת עובד ב-UTC; ההמרה לישראל תיעשה בצד-לקוח בשלב 2
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# נתיב לוגים של הבוט (ניתן לשנות עם ENV בשם LOG_DIR)
DEFAULT_LOG_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "bot", "logs"))
LOG_DIR = os.getenv("LOG_DIR", DEFAULT_LOG_DIR)

TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")
STATE_JSON = os.path.join(LOG_DIR, "bot_state.json")   # לא מפיל תהליך; רק חיווי/override לדאשבורד

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
    if not os.path.exists(STATE_JSON):
        _write_state({"manual_status": None, "updated_at": None})  # None = אין override

_ensure_logs_and_headers()


# ===== אחסון סטטוס ידני (Play/Pause עדין) =====
def _read_state():
    try:
        with open(STATE_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"manual_status": None, "updated_at": None}

def _write_state(obj):
    try:
        with open(STATE_JSON, "w", encoding="utf-8") as f:
            json.dump(obj, f)
    except Exception:
        pass


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
    """ממיר טקסט לזמן. תומך ב־Z, +00:00, וגם בלי אזור."""
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
    """מחלץ timestamp מתוך שורה לפי שמות מקובלים."""
    if not isinstance(row, dict):
        return None
    for key in ("time", "timestamp", "ts", "datetime"):
        if key in row and row[key]:
            return _parse_iso(row[key])
    return None


# ===== עזרי טווחי זמן =====
def _compute_range_from_query():
    """
    מחזיר (start_utc, end_utc, label) ע"פ פרמטרי שאילתה:
      - range: last_1h | last_24h | last_7d | last_30d | last_90d
      - from, to: ערכי ISO-8601 (UTC או עם offset)
    אם לא סופק דבר -> (None, None, "all")
    """
    now = datetime.now(APP_TZ)
    rng = (request.args.get("range") or "").lower().strip()
    p_from = request.args.get("from")
    p_to = request.args.get("to")

    if p_from or p_to:
        start = _parse_iso(p_from) if p_from else None
        end = _parse_iso(p_to) if p_to else None
        return start, end, "custom"

    if rng in {"last_1h", "1h"}:
        return now - timedelta(hours=1), now, "last_1h"
    if rng in {"last_24h", "24h", "1d"}:
        return now - timedelta(days=1), now, "last_24h"
    if rng in {"last_7d", "7d"}:
        return now - timedelta(days=7), now, "last_7d"
    if rng in {"last_30d", "30d"}:
        return now - timedelta(days=30), now, "last_30d"
    if rng in {"last_90d", "90d"}:
        return now - timedelta(days=90), now, "last_90d"

    return None, None, "all"


def _within_range(ts: datetime, start: datetime | None, end: datetime | None):
    if ts is None:
        return False if (start or end) else True
    if start and ts < start:
        return False
    if end and ts > end:
        return False
    return True


def _filter_rows_by_time(rows, start, end):
    out = []
    for r in rows:
        ts = _last_timestamp(r)
        if _within_range(ts, start, end):
            out.append(r)
    return out


# ===== לוגיקת סטטוס =====
def _bot_status():
    """
    RUNNING/STOPPED לפי heartbeat ב-equity_curve.csv אלא אם יש override ידני.
    """
    state = _read_state()
    if state.get("manual_status") in {"RUNNING", "STOPPED"}:
        # override ידני
        return {
            "status": state["manual_status"],
            "last_equity_ts": None,
            "age_sec": None,
            "manual_override": True,
        }

    eq_last = _read_csv(EQUITY_CSV, limit=1)
    now = datetime.now(APP_TZ)
    if not eq_last:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None, "manual_override": False}

    last_ts = _last_timestamp(eq_last[-1])
    if not last_ts:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None, "manual_override": False}

    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=APP_TZ)

    age = (now - last_ts).total_seconds()
    status = "RUNNING" if age <= 90 else "STOPPED"
    return {"status": status, "last_equity_ts": last_ts.isoformat(), "age_sec": int(age), "manual_override": False}


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
        f"<h1>Trading Bot Dashboard</h1>"
        f"<p>Status: <b>{st['status']}</b></p>"
        f"<p>Last equity timestamp: {st['last_equity_ts']}</p>"
        f"<p>Age (sec): {st['age_sec']}</p>"
        f"<p>LOG_DIR: {LOG_DIR}</p>",
        200,
        {"Content-Type": "text/html; charset=utf-8"},
    )


@app.route("/data")
def data():
    """
    מחזיר JSON עם נתוני trades & equity *מסוננים לפי טווח*.
    פרמטרים (query):
      - range: last_1h | last_24h | last_7d | last_30d | last_90d
      - from, to: ISO-8601 (UTC/offset)
    """
    start, end, label = _compute_range_from_query()
    trades_all = _read_csv(TRADES_CSV)
    equity_all = _read_csv(EQUITY_CSV)
    trades = _filter_rows_by_time(trades_all, start, end)
    equity = _filter_rows_by_time(equity_all, start, end)
    st = _bot_status()

    # פורמט "פשוט" לרענון עבור ה-UI (לפי בקשתך בשלב 7)
    now_iso_simple = datetime.now(APP_TZ).strftime("%Y-%m-%d %H:%M:%S")

    return jsonify(
        {
            "status": st["status"],
            "manual_override": st.get("manual_override", False),
            "last_equity_ts": st["last_equity_ts"],
            "age_sec": st["age_sec"],
            "now_utc": datetime.now(APP_TZ).isoformat(),
            "now_utc_simple": now_iso_simple,
            "range": {"label": label, "from": start.isoformat() if start else None, "to": end.isoformat() if end else None},
            "trades": trades,
            "equity": equity,
        }
    )


@app.route("/export/trades.csv")
def export_trades():
    """
    הורדה של trades.csv מסונן לפי פרמטרים כמו ב-/data
    """
    start, end, _ = _compute_range_from_query()
    rows = _filter_rows_by_time(_read_csv(TRADES_CSV), start, end)

    # כותבים CSV זמני לזיכרון מתוך הרשומות המסוננות
    if not rows:
        # נחזיר רק כותרות סטנדרטיות
        output = io.StringIO()
        csv.writer(output).writerow(["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"])
        output.seek(0)
        return send_file(io.BytesIO(output.getvalue().encode("utf-8")), mimetype="text/csv",
                         as_attachment=True, download_name="trades.csv")

    headers = list(rows[0].keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)
    return send_file(io.BytesIO(output.getvalue().encode("utf-8")), mimetype="text/csv",
                     as_attachment=True, download_name="trades.csv")


@app.route("/export/equity_curve.csv")
def export_equity():
    """
    הורדה של equity_curve.csv מסונן לפי פרמטרים כמו ב-/data
    """
    start, end, _ = _compute_range_from_query()
    rows = _filter_rows_by_time(_read_csv(EQUITY_CSV), start, end)

    if not rows:
        output = io.StringIO()
        csv.writer(output).writerow(["time", "equity"])
        output.seek(0)
        return send_file(io.BytesIO(output.getvalue().encode("utf-8")), mimetype="text/csv",
                         as_attachment=True, download_name="equity_curve.csv")

    headers = list(rows[0].keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)
    return send_file(io.BytesIO(output.getvalue().encode("utf-8")), mimetype="text/csv",
                     as_attachment=True, download_name="equity_curve.csv")


@app.route("/download")
def download_csv_alias():
    """אליאס תואם-עבר לעדכון ישנים — מוריד trades.csv אם קיים (ללא סינון)."""
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
            "manual_override": st.get("manual_override", False),
            "last_equity_ts": st["last_equity_ts"],
            "age_sec": st["age_sec"],
            "log_dir": LOG_DIR,
        }
    ), 200


# ===== APIs ל-Play/Pause עדין (לא מפיל תהליכים) =====
@app.route("/api/bot/state", methods=["GET"])
def bot_state_get():
    state = _read_state()
    st = _bot_status()
    return jsonify({"manual_status": state.get("manual_status"), "effective_status": st["status"],
                    "updated_at": state.get("updated_at")})

@app.route("/api/bot/start", methods=["POST"])
def bot_state_start():
    now = datetime.now(APP_TZ).isoformat()
    _write_state({"manual_status": "RUNNING", "updated_at": now})
    return jsonify({"ok": True, "manual_status": "RUNNING", "updated_at": now})

@app.route("/api/bot/pause", methods=["POST"])
def bot_state_pause():
    now = datetime.now(APP_TZ).isoformat()
    _write_state({"manual_status": "STOPPED", "updated_at": now})
    return jsonify({"ok": True, "manual_status": "STOPPED", "updated_at": now})


if __name__ == "__main__":
    # PORT לברירת־מחדל: 10000 (ניתן לשנות עם ENV בשם PORT)
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
