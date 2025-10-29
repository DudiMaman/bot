# dashboard/app.py
import os
import json
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template_string
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is missing")

app = Flask(__name__)

HTML = """
<!doctype html>
<html lang="en" dir="ltr">
<head>
  <meta charset="utf-8" />
  <title>Trading Bot Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    body{ background: #0b1220; color:#e6e8ec; }
    .card{ background:#111a2f; border:1px solid #1f2a44; border-radius:14px;}
    .status-pill{ padding:.25rem .6rem; border-radius:999px; font-weight:600;}
    .status-running{ background:#1f6feb33; color:#58a6ff; border:1px solid #1f6feb;}
    .status-stopped{ background:#8b000033; color:#ff6b6b; border:1px solid #8b0000;}
    .btn-outline-light{border-color:#2e3a5f;}
    table thead th{ color:#cbd5e1; }
    .muted{ color:#9aa4b2; font-size:.9rem; }
  </style>
</head>
<body>
<div class="container-fluid py-3">

  <div class="d-flex align-items-center justify-content-between mb-3">
    <div class="d-flex align-items-center gap-3">
      <h3 class="m-0">Trading Bot Dashboard</h3>
      <span id="statusPill" class="status-pill status-stopped">STOPPED</span>
    </div>
    <div class="text-end">
      <div class="muted">
        <span id="lastRefresh">–</span>
      </div>
    </div>
  </div>

  <div class="row g-3">
    <!-- left: Trades table (70%) -->
    <div class="col-lg-8">
      <div class="card p-3">
        <div class="d-flex align-items-center justify-content-between mb-2">
          <h5 class="m-0">Recent Trades</h5>
        </div>
        <div class="table-responsive" style="max-height: 70vh;">
          <table class="table table-dark table-hover align-middle">
            <thead>
              <tr>
                <th>Time</th>
                <th>Conn</th>
                <th>Symbol</th>
                <th>Type</th>
                <th>Side</th>
                <th class="text-end">Price</th>
                <th class="text-end">Qty</th>
                <th class="text-end">PnL</th>
                <th class="text-end">Equity</th>
              </tr>
            </thead>
            <tbody id="tradesBody"></tbody>
          </table>
        </div>
      </div>
    </div>
    <!-- right: Equity chart (30%) -->
    <div class="col-lg-4">
      <div class="card p-3">
        <h5>Equity Curve</h5>
        <canvas id="equityChart" height="300"></canvas>
      </div>
    </div>
  </div>
</div>

<script>
let chart;

function fmt(n, d=2){
  if(n===null || n===undefined || isNaN(n)) return "";
  return Number(n).toLocaleString(undefined, {maximumFractionDigits:d});
}

async function loadData(){
  const r = await fetch('/data');
  const data = await r.json();

  // last refresh stamp
  document.getElementById('lastRefresh').textContent = 'Last refresh: ' + data.now_iso;

  // status pill
  const pill = document.getElementById('statusPill');
  pill.textContent = data.status;
  pill.className = 'status-pill ' + (data.status === 'RUNNING' ? 'status-running' : 'status-stopped');

  // trades
  const tbody = document.getElementById('tradesBody');
  tbody.innerHTML = '';
  data.trades.forEach(t => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${t.time}</td>
      <td>${t.connector}</td>
      <td>${t.symbol}</td>
      <td>${t.type}</td>
      <td>${t.side}</td>
      <td class="text-end">${fmt(t.price, 8)}</td>
      <td class="text-end">${fmt(t.qty, 8)}</td>
      <td class="text-end">${fmt(t.pnl, 2)}</td>
      <td class="text-end">${fmt(t.equity, 2)}</td>
    `;
    tbody.appendChild(tr);
  });

  // equity
  const labels = data.equity.map(p => p.time);
  const values = data.equity.map(p => p.equity);
  const ctx = document.getElementById('equityChart').getContext('2d');
  if(chart) chart.destroy();
  chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Equity',
        data: values,
        fill: false,
        tension: 0.25
      }]
    },
    options: {
      plugins: { legend: { display: false } },
      scales: { x: { display: false } }
    }
  });
}

loadData();
setInterval(loadData, 10000);
</script>
</body>
</html>
"""

def _conn():
    return psycopg2.connect(DATABASE_URL)

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/data")
def data():
    # read last 200 equity points & last 100 trades
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT time, equity
                FROM equity_curve
                ORDER BY time DESC
                LIMIT 200
            """)
            eq_rows = cur.fetchall()
            eq_rows.reverse()  # chronological for the chart

            cur.execute("""
                SELECT time, connector, symbol, type, side, price, qty, pnl, equity
                FROM trades
                ORDER BY time DESC
                LIMIT 100
            """)
            tr_rows = cur.fetchall()

    # status: running if last equity < 120s ago
    now = datetime.now(timezone.utc)
    status = "STOPPED"
    if eq_rows:
        last_t = eq_rows[-1]["time"]
        delta = (now - last_t).total_seconds()
        status = "RUNNING" if delta < 120 else "STOPPED"

    return jsonify({
        "now_iso": now.isoformat(),
        "status": status,
        "equity": [{"time": r["time"].isoformat(), "equity": float(r["equity"])} for r in eq_rows],
        "trades": [{
            "time": r["time"].isoformat(),
            "connector": r["connector"],
            "symbol": r["symbol"],
            "type": r["type"],
            "side": r["side"],
            "price": None if r["price"] is None else float(r["price"]),
            "qty":   None if r["qty"]   is None else float(r["qty"]),
            "pnl":   None if r["pnl"]   is None else float(r["pnl"]),
            "equity":None if r["equity"]is None else float(r["equity"]),
        } for r in tr_rows]
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
