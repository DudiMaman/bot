# dashboard/app.py
import os
from flask import Flask, jsonify, request, render_template_string
import pandas as pd
from datetime import datetime, timezone

# ==== DB (state only) ====
USE_DB = bool(os.getenv("DATABASE_URL"))
if USE_DB:
    try:
        import psycopg
        def db_exec(sql, params=None, fetch=False):
            with psycopg.connect(os.getenv("DATABASE_URL"), autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    return cur.fetchall() if fetch else None
        def get_state():
            db_exec("""create table if not exists bot_state(
                        id int primary key default 1,
                        state text not null default 'RUNNING',
                        updated_at timestamptz not null default now());""")
            db_exec("insert into bot_state (id) values (1) on conflict (id) do nothing;")
            row = db_exec("select state from bot_state where id=1;", fetch=True)
            return (row[0][0] if row else "RUNNING") or "RUNNING"
        def set_state(state):
            db_exec("insert into bot_state (id, state, updated_at) values (1, %s, now()) "
                    "on conflict (id) do update set state=excluded.state, updated_at=now();", (state,))
    except Exception:
        USE_DB = False

# ==== Files (CSV) ====
BASE = os.path.dirname(__file__)
LOGS = os.path.join(os.path.dirname(BASE), "bot", "logs")
TRADES_CSV = os.path.join(LOGS, "trades.csv")
EQUITY_CSV = os.path.join(LOGS, "equity_curve.csv")

app = Flask(__name__)

HTML = """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Trading Bot Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  body { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin:20px; color:#111; }
  .header { display:flex; align-items:center; gap:16px; }
  .badge { padding:8px 14px; border-radius:999px; font-weight:700; }
  .stopped { background:#fee2e2; color:#991b1b; }
  .running { background:#dcfce7; color:#166534; }
  .btn { padding:10px 16px; border-radius:12px; border:1px solid #ddd; background:#fff; cursor:pointer; font-weight:600; }
  .btn:hover { background:#f7f7f7; }
  .muted { color:#777; }
  .grid { display:grid; grid-template-columns: 2fr 1fr; gap:16px; margin-top:18px; }
  .card { border:1px solid #eee; border-radius:16px; padding:16px; }
  table { width:100%; border-collapse:separate; border-spacing:0; }
  th, td { text-align:left; padding:10px 12px; border-bottom:1px solid #f0f0f0; }
  th { background:#fafafa; font-weight:700; }
  #chart { height:420px; }
</style>
</head>
<body>
  <div class="header">
    <h1 style="margin:0;">Trading Bot Dashboard</h1>
    <div class="muted">Last refresh: <span id="last-refresh">—</span></div>
    <div id="status" class="badge stopped">STOPPED</div>
    <button class="btn" onclick="post('/resume')">Resume</button>
    <button class="btn" onclick="post('/pause')">Pause</button>
    <div id="note" class="muted">No updates yet</div>
  </div>

  <div class="grid">
    <div class="card">
      <h2 style="margin-top:0;">Recent Trades</h2>
      <table id="trades">
        <thead>
          <tr>
            <th>Time</th><th>Connector</th><th>Symbol</th><th>Type</th>
            <th>Side</th><th>Price</th><th>Qty</th><th>PnL</th><th>Equity</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>

    <div class="card">
      <h2 style="margin-top:0;">Equity Curve</h2>
      <canvas id="chart"></canvas>
    </div>
  </div>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
  let chart;

  function post(path){
    fetch(path, {method:'POST'}).then(()=>refresh());
  }

  function setStatus(s){
    const el = document.getElementById('status');
    el.textContent = s;
    el.className = 'badge ' + (s === 'RUNNING' ? 'running' : 'stopped');
  }

  function refresh(){
    fetch('/data').then(r=>r.json()).then(d=>{
      document.getElementById('last-refresh').textContent = d.now;
      setStatus(d.state);

      const tbody = document.querySelector('#trades tbody');
      tbody.innerHTML = '';
      d.trades.forEach(row=>{
        const tr = document.createElement('tr');
        row.forEach(cell=>{
          const td = document.createElement('td');
          td.textContent = cell;
          tbody.appendChild(tr);
          tr.appendChild(td);
        });
      });

      const labels = d.equity.map(x=>x[0]);
      const values = d.equity.map(x=>x[1]);
      if(!chart){
        const ctx = document.getElementById('chart').getContext('2d');
        chart = new Chart(ctx, {
          type:'line',
          data:{ labels:labels, datasets:[{ label:'Equity', data:values, tension:0.2, borderWidth:2 }] },
          options:{ responsive:true, maintainAspectRatio:false, scales:{ x:{display:false} } }
        });
      } else {
        chart.data.labels = labels;
        chart.data.datasets[0].data = values;
        chart.update();
      }
      document.getElementById('note').textContent = d.note || '';
    });
  }
  setInterval(refresh, 10000);
  refresh();
</script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/data")
def data():
    # state
    state = "RUNNING"
    if USE_DB:
        try:
            state = get_state()
        except Exception as e:
            state = "RUNNING"

    # trades
    trades = []
    if os.path.exists(TRADES_CSV):
        try:
            df = pd.read_csv(TRADES_CSV)
            if not df.empty:
                last = df.tail(50)
                trades = last.values.tolist()
        except Exception:
            pass

    # equity
    equity = []
    if os.path.exists(EQUITY_CSV):
        try:
            df2 = pd.read_csv(EQUITY_CSV)
            if not df2.empty:
                eq = df2.tail(300)
                equity = [[str(t), float(v)] for t, v in zip(eq["time"], eq["equity"])]
        except Exception:
            pass

    return jsonify({
        "now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "state": state,
        "trades": trades,
        "equity": equity,
        "note": "" if (trades or equity) else "No CSV data yet"
    })

@app.route("/resume", methods=["POST"])
def resume():
    if USE_DB:
        try: set_state("RUNNING")
        except Exception: pass
    return ("OK", 200)

@app.route("/pause", methods=["POST"])
def pause():
    if USE_DB:
        try: set_state("PAUSED")
        except Exception: pass
    return ("OK", 200)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
