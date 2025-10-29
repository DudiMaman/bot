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

# דגל Pause/Resume משותף
CONTROLS_DIR = BASE_DIR / "bot" / "controls"
PAUSE_FLAG = CONTROLS_DIR / "pause.flag"
CONTROLS_DIR.mkdir(parents=True, exist_ok=True)

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
    keep = [c for c in ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"] if c in df.columns]
    df = df[keep].tail(limit).iloc[::-1].reset_index(drop=True)
    for col in ["price", "qty", "pnl", "equity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
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
    df = df[["time", "equity"]].tail(limit).reset_index(drop=True)
    df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
    return [{"time": str(r["time"]), "equity": float(r["equity"])} for _, r in df.iterrows()]


def calc_daily_pnl(equity_points):
    """מקבל נקודות Equity ומחזיר שינוי יומי באחוזים"""
    if not equity_points:
        return []
    df = pd.DataFrame(equity_points)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time", "equity"])
    df["date"] = df["time"].dt.date
    daily = df.groupby("date")["equity"].last().pct_change().fillna(0) * 100
    return [{"date": str(d), "pnl_pct": round(v, 2)} for d, v in daily.items()]


def is_paused():
    return PAUSE_FLAG.exists()


def heartbeat_status():
    if not EQUITY_CSV.exists():
        return {"running": False, "last_update": None, "paused": is_paused()}
    mtime = datetime.fromtimestamp(EQUITY_CSV.stat().st_mtime, tz=timezone.utc)
    delta = (datetime.now(timezone.utc) - mtime).total_seconds() / 60
    return {"running": delta <= HEARTBEAT_MINUTES, "last_update": mtime.isoformat(), "paused": is_paused()}


@app.get("/data")
def data_api():
    equity = read_equity(limit=500)
    return jsonify({
        "status": heartbeat_status(),
        "trades": read_trades(limit=200),
        "equity": equity,
        "daily_pnl": calc_daily_pnl(equity)
    })


@app.post("/pause")
def pause_api():
    PAUSE_FLAG.write_text("paused\n", encoding="utf-8")
    return jsonify({"ok": True, "paused": True})


@app.post("/resume")
def resume_api():
    if PAUSE_FLAG.exists():
        PAUSE_FLAG.unlink()
    return jsonify({"ok": True, "paused": False})


# -------------------- UI --------------------

INDEX_HTML = """
<!doctype html>
<html lang="he" dir="rtl">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Trading Bot Dashboard</title>
  <style>
    body{font-family:system-ui,Segoe UI,Arial,sans-serif; max-width:1300px; margin:24px auto; padding:0 12px;}
    header{display:flex; gap:12px; align-items:center; justify-content:space-between; margin-bottom:16px}
    .pill{display:inline-flex; align-items:center; gap:8px; padding:6px 12px; border-radius:20px; font-weight:600;}
    .ok{background:#eaf7ee; color:#137333;}
    .bad{background:#fdecec; color:#b00020;}
    .warn{background:#fff6e6; color:#8a6d00;}
    .btns{display:flex; gap:8px}
    button{padding:8px 12px; border:1px solid #ddd; background:#fff; border-radius:10px; cursor:pointer}
    button.primary{background:#111; color:#fff; border-color:#111}
    .muted{color:#666; font-size:12px}
    h1{margin:0; font-size:22px;}
    h2{margin:0 0 8px 0; font-size:18px}
    .main-grid{display:grid; grid-template-columns:70% 30%; gap:16px;}
    .card{border:1px solid #eee; border-radius:12px; padding:14px; height:calc(100vh - 120px); overflow:auto;}
    table{width:100%; border-collapse:collapse; font-size:14px}
    th,td{padding:8px 6px; border-bottom:1px solid #eee; text-align:right}
    th{background:#fafafa; position:sticky; top:0}
    .charts{display:flex; flex-direction:column; gap:24px; height:100%;}
    .chart-wrap{flex:1; display:flex; flex-direction:column;}
    canvas{width:100%; flex:1;}
    @media(max-width:900px){
      .main-grid{grid-template-columns:1fr;}
      .card{height:auto;}
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Trading Bot Dashboard</h1>
      <div class="muted">רענון אוטומטי כל <b id="period">10</b> שניות</div>
    </div>
    <div style="display:flex; align-items:center; gap:10px;">
      <span id="status-pill" class="pill warn">טוען…</span>
      <div class="btns">
        <button id="pauseBtn">Pause</button>
        <button id="resumeBtn" class="primary">Resume</button>
      </div>
    </div>
  </header>

  <div class="main-grid">
    <div class="card">
      <h2>עסקאות אחרונות</h2>
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

    <div class="card charts">
      <div class="chart-wrap">
        <h2>Equity Curve</h2>
        <canvas id="equityChart"></canvas>
      </div>
      <div class="chart-wrap">
        <h2>Daily PnL (%)</h2>
        <canvas id="pnlChart"></canvas>
      </div>
      <div class="muted" id="lastUpdate" style="margin-top:10px;"></div>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script>
    const REFRESH_EVERY_MS = 10000;
    document.getElementById('period').textContent = REFRESH_EVERY_MS/1000;
    const pill = document.getElementById('status-pill');
    const tbody = document.querySelector('#tradesTable tbody');
    const lastUpdateEl = document.getElementById('lastUpdate');
    let eqChart, pnlChart;

    function makeLine(ctx,label,color='#111'){
      return new Chart(ctx,{
        type:'line',
        data:{labels:[],datasets:[{label:label,data:[],borderColor:color,tension:0.2}]},
        options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}
      });
    }

    async function fetchJSON(u,o={}){const r=await fetch(u,o);if(!r.ok)throw new Error(await r.text());return await r.json();}
    function fmt(v,d=2){return (v===null||v===undefined)?'':Number(v).toFixed(d);}
    function setPill(s){pill.classList.remove('ok','bad','warn');if(s.paused){pill.classList.add('warn');pill.textContent='PAUSED';return;}
      if(s.running){pill.classList.add('ok');pill.textContent='RUNNING';}else{pill.classList.add('bad');pill.textContent='STOPPED';}}

    function render(data){
      setPill(data.status);
      lastUpdateEl.textContent = data.status.last_update ? ('עודכן לאחרונה: '+data.status.last_update) : 'אין עדכון';

      tbody.innerHTML='';
      for(const r of data.trades){
        const tr=document.createElement('tr');
        tr.innerHTML=`<td>${r.time}</td><td>${r.connector}</td><td>${r.symbol}</td><td>${r.type}</td><td>${r.side}</td>
                      <td>${fmt(r.price,6)}</td><td>${fmt(r.qty,6)}</td><td>${fmt(r.pnl,2)}</td><td>${fmt(r.equity,2)}</td>`;
        tbody.appendChild(tr);
      }

      // גרף Equity
      if(!eqChart){eqChart=makeLine(document.getElementById('equityChart').getContext('2d'),'Equity','#222');}
      eqChart.data.labels=data.equity.map(p=>p.time);
      eqChart.data.datasets[0].data=data.equity.map(p=>p.equity);
      eqChart.update();

      // גרף Daily PnL
      if(!pnlChart){pnlChart=makeLine(document.getElementById('pnlChart').getContext('2d'),'Daily PnL','#0066cc');}
      pnlChart.data.labels=data.daily_pnl.map(p=>p.date);
      pnlChart.data.datasets[0].data=data.daily_pnl.map(p=>p.pnl_pct);
      pnlChart.update();
    }

    async function refresh(){try{const d=await fetchJSON('/data');render(d);}catch(e){pill.classList.remove('ok');pill.classList.add('bad');pill.textContent='ERROR';}}
    document.getElementById('pauseBtn').addEventListener('click',()=>fetchJSON('/pause',{method:'POST'}).then(refresh));
    document.getElementById('resumeBtn').addEventListener('click',()=>fetchJSON('/resume',{method:'POST'}).then(refresh));
    refresh(); setInterval(refresh,REFRESH_EVERY_MS);
  </script>
</body>
</html>
"""

@app.get("/")
def index():
    return render_template_string(INDEX_HTML)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
