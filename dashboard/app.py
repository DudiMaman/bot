import os
from pathlib import Path
from datetime import datetime, timezone
from flask import Flask, jsonify, request, render_template_string, abort
import pandas as pd

app = Flask(__name__)

# --- נתיבים לקבצי הלוג של הבוט ---
BASE_DIR = Path(__file__).resolve().parents[1]   # שורש הריפו
LOG_DIR = BASE_DIR / "bot" / "logs"
TRADES_CSV = LOG_DIR / "trades.csv"
EQUITY_CSV = LOG_DIR / "equity_curve.csv"

# דגל Pause/Resume משותף (הדשבורד יוצר/מוחק; הבוט צריך לבדוק אותו בלולאה)
CONTROLS_DIR = BASE_DIR / "bot" / "controls"
PAUSE_FLAG = CONTROLS_DIR / "pause.flag"
CONTROLS_DIR.mkdir(parents=True, exist_ok=True)

# כמה דקות נחשב "חי" לעדכון Equity לפני שנאמר שהבוט לא פעיל
HEARTBEAT_MINUTES = 5


def read_trades(limit=200):
    if not TRADES_CSV.exists():
        return []
    try:
        df = pd.read_csv(TRADES_CSV)
    except Exception:
        return []
    if df.empty:
        return []
    # נמסך רק העמודות החשובות, וסידור מהאחרון לראשון
    keep = [c for c in ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"] if c in df.columns]
    df = df[keep].tail(limit).iloc[::-1].reset_index(drop=True)
    # המרות קלות לייצוג יפה
    for col in ["price", "qty", "pnl", "equity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "price" in df: df["price"] = df["price"].map(lambda v: None if pd.isna(v) else float(v))
    if "qty" in df: df["qty"] = df["qty"].map(lambda v: None if pd.isna(v) else float(v))
    if "pnl" in df: df["pnl"] = df["pnl"].map(lambda v: None if pd.isna(v) else float(v))
    if "equity" in df: df["equity"] = df["equity"].map(lambda v: None if pd.isna(v) else float(v))
    return df.to_dict(orient="records")


def read_equity(limit=500):
    if not EQUITY_CSV.exists():
        return []
    try:
        df = pd.read_csv(EQUITY_CSV)
    except Exception:
        return []
    if df.empty:
        return []
    keep = [c for c in ["time", "equity"] if c in df.columns]
    df = df[keep].tail(limit).reset_index(drop=True)
    # המרות
    df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
    # מחזיר רשימת נקודות
    points = []
    for _, r in df.iterrows():
        t = str(r["time"])
        e = None if pd.isna(r["equity"]) else float(r["equity"])
        points.append({"time": t, "equity": e})
    return points


def is_paused():
    return PAUSE_FLAG.exists()


def heartbeat_status():
    """
    סטטוס חיות: נחשב 'רץ' אם קובץ equity_curve.csv עודכן ב־HEARTBEAT_MINUTES האחרונות.
    """
    if not EQUITY_CSV.exists():
        return {"running": False, "last_update": None, "paused": is_paused()}
    mtime = datetime.fromtimestamp(EQUITY_CSV.stat().st_mtime, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = (now - mtime).total_seconds() / 60.0
    return {
        "running": delta <= HEARTBEAT_MINUTES,
        "last_update": mtime.isoformat(),
        "paused": is_paused()
    }


# -------------------- ROUTES (API) --------------------

@app.get("/data")
def data_api():
    return jsonify({
        "status": heartbeat_status(),
        "trades": read_trades(limit=200),
        "equity": read_equity(limit=500),
    })


@app.post("/pause")
def pause_api():
    try:
        PAUSE_FLAG.write_text("paused\n", encoding="utf-8")
        return jsonify({"ok": True, "paused": True})
    except Exception as e:
        return abort(500, str(e))


@app.post("/resume")
def resume_api():
    try:
        if PAUSE_FLAG.exists():
            PAUSE_FLAG.unlink()
        return jsonify({"ok": True, "paused": False})
    except Exception as e:
        return abort(500, str(e))


# -------------------- UI --------------------

INDEX_HTML = """
<!doctype html>
<html lang="he" dir="rtl">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Trading Bot Dashboard</title>
  <style>
    body{font-family:system-ui,Segoe UI,Arial,sans-serif; max-width:1100px; margin:24px auto; padding:0 12px;}
    header{display:flex; gap:12px; align-items:center; justify-content:space-between; margin-bottom:14px}
    .pill{display:inline-flex; align-items:center; gap:8px; padding:6px 12px; border-radius:20px; font-weight:600;}
    .ok{background:#eaf7ee; color:#137333;}
    .bad{background:#fdecec; color:#b00020;}
    .warn{background:#fff6e6; color:#8a6d00;}
    .row{display:grid; grid-template-columns:1.2fr 1fr; gap:16px; align-items:start;}
    .card{border:1px solid #eee; border-radius:12px; padding:14px}
    h2{margin:0 0 8px 0; font-size:18px}
    table{width:100%; border-collapse:collapse; font-size:14px}
    th,td{padding:8px 6px; border-bottom:1px solid #eee; text-align:right}
    th{background:#fafafa; position:sticky; top:0}
    .btns{display:flex; gap:8px}
    button{padding:8px 12px; border:1px solid #ddd; background:#fff; border-radius:10px; cursor:pointer}
    button.primary{background:#111; color:#fff; border-color:#111}
    .muted{color:#666; font-size:12px}
    .stack{display:flex; flex-direction:column; gap:6px}
    @media(max-width:900px){ .row{grid-template-columns:1fr} }
    canvas{width:100%; height:280px}
  </style>
</head>
<body>
  <header>
    <div class="stack">
      <div style="display:flex; gap:10px; align-items:center;">
        <h1 style="margin:0; font-size:22px;">Trading Bot Dashboard</h1>
        <span id="status-pill" class="pill warn">טוען…</span>
      </div>
      <div class="muted">התעדכנות אוטומטית כל <b id="period">10</b> שניות</div>
    </div>
    <div class="btns">
      <button id="pauseBtn" class="secondary">Pause</button>
      <button id="resumeBtn" class="primary">Resume</button>
    </div>
  </header>

  <div class="row">
    <div class="card">
      <h2>Equity Curve</h2>
      <canvas id="equityChart"></canvas>
      <div class="muted" id="lastUpdate"></div>
    </div>
    <div class="card">
      <h2>סיכום</h2>
      <div id="summaryBox" class="stack"></div>
    </div>
  </div>

  <div class="card" style="margin-top:16px;">
    <h2>עסקאות אחרונות</h2>
    <div style="max-height:420px; overflow:auto;">
      <table id="tradesTable">
        <thead>
          <tr>
            <th>זמן</th><th>מחבר</th><th>סימבול</th><th>סוג</th><th>כיוון</th>
            <th>מחיר</th><th>כמות</th><th>PnL</th><th>Equity</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <!-- Chart.js CDN -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script>
    const REFRESH_EVERY_MS = 10000; // 10 שניות
    const periodEl = document.getElementById('period');
    periodEl.textContent = Math.round(REFRESH_EVERY_MS/1000);

    const pill = document.getElementById('status-pill');
    const lastUpdateEl = document.getElementById('lastUpdate');
    const summaryBox = document.getElementById('summaryBox');
    const tbody = document.querySelector('#tradesTable tbody');

    let eqChart;
    function ensureChart() {
      if (eqChart) return eqChart;
      const ctx = document.getElementById('equityChart').getContext('2d');
      eqChart = new Chart(ctx, {
        type: 'line',
        data: { labels: [], datasets: [{ label: 'Equity', data: [], tension: 0.2 }]},
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: { y: { beginAtZero: false } },
          plugins: { legend: { display: false } }
        }
      });
      return eqChart;
    }

    async function fetchJSON(url, opts={}) {
      const r = await fetch(url, opts);
      if (!r.ok) throw new Error(await r.text());
      return await r.json();
    }

    function setPill(status){
      pill.classList.remove('ok','bad','warn');
      if (status.paused) {
        pill.classList.add('warn'); pill.textContent = 'PAUSED';
        return;
      }
      if (status.running) {
        pill.classList.add('ok'); pill.textContent = 'RUNNING';
      } else {
        pill.classList.add('bad'); pill.textContent = 'STOPPED';
      }
    }

    function fmt(n, digits=2){ return (n===null || n===undefined) ? '' : Number(n).toFixed(digits); }

    function render(data){
      setPill(data.status);
      lastUpdateEl.textContent = data.status.last_update ? ('עודכן לאחרונה: ' + data.status.last_update) : 'אין עדכון';

      // סיכום קטן
      let lastEq = (data.equity.length ? data.equity[data.equity.length-1].equity : null);
      let firstEq = (data.equity.length ? data.equity[0].equity : null);
      let pnlAbs = (lastEq!==null && firstEq!==null) ? (lastEq - firstEq) : null;
      let pnlPct = (pnlAbs!==null && firstEq ? (pnlAbs/firstEq*100) : null);

      summaryBox.innerHTML = `
        <div>Equity נוכחי: <b>${fmt(lastEq,2)}</b></div>
        <div>PnL מצטבר: <b>${fmt(pnlAbs,2)}</b> (${fmt(pnlPct,2)}%)</div>
        <div>טריידים מוצגים: <b>${data.trades.length}</b></div>
      `;

      // טבלת טריידים
      tbody.innerHTML = '';
      for (const r of data.trades){
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${r.time ?? ''}</td>
          <td>${r.connector ?? ''}</td>
          <td>${r.symbol ?? ''}</td>
          <td>${r.type ?? ''}</td>
          <td>${r.side ?? ''}</td>
          <td>${fmt(r.price, 6)}</td>
          <td>${fmt(r.qty, 6)}</td>
          <td>${fmt(r.pnl, 2)}</td>
          <td>${fmt(r.equity, 2)}</td>
        `;
        tbody.appendChild(tr);
      }

      // גרף Equity
      const c = ensureChart();
      c.data.labels = data.equity.map(p => p.time);
      c.data.datasets[0].data = data.equity.map(p => p.equity);
      c.update();
    }

    async function refresh(){
      try{
        const data = await fetchJSON('/data');
        render(data);
      } catch(e){
        pill.classList.remove('ok'); pill.classList.add('bad');
        pill.textContent = 'ERROR';
        console.error(e);
      }
    }

    // כפתורי Pause/Resume
    document.getElementById('pauseBtn').addEventListener('click', async ()=>{
      try { await fetchJSON('/pause', {method:'POST'}); await refresh(); }
      catch(e){ alert('Pause failed: '+e.message); }
    });
    document.getElementById('resumeBtn').addEventListener('click', async ()=>{
      try { await fetchJSON('/resume', {method:'POST'}); await refresh(); }
      catch(e){ alert('Resume failed: '+e.message); }
    });

    // טעינה ראשונה ורענון אוטומטי
    refresh();
    setInterval(refresh, REFRESH_EVERY_MS);
  </script>
</body>
</html>
"""

@app.get("/")
def index():
    return render_template_string(INDEX_HTML)


# -------------------- Render / Gunicorn --------------------
if __name__ == "__main__":
    # להרצה מקומית: python app.py
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
