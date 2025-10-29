import os
import pandas as pd
import time
from flask import Flask, render_template_string, jsonify

app = Flask(__name__)

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "bot", "logs")
TRADES_PATH = os.path.join(LOG_DIR, "trades.csv")
EQUITY_PATH = os.path.join(LOG_DIR, "equity_curve.csv")

HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Trading Bot Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 30px; background: #111; color: #eee; }
        h1 { color: #4CAF50; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #333; padding: 8px; text-align: center; }
        th { background: #222; }
        tr:nth-child(even) { background: #1a1a1a; }
        button { background: #4CAF50; color: white; border: none; padding: 10px 15px; margin: 10px 0; cursor: pointer; border-radius: 5px; }
        button:hover { background: #45a049; }
        canvas { background: #1b1b1b; border-radius: 10px; }
    </style>
</head>
<body>
    <h1>📈 Trading Bot Dashboard</h1>
    <button onclick="toggleBot()">{{ '⏸️ Pause' if running else '▶️ Start' }}</button>
    <p>Status: <b style="color: {{ 'lime' if running else 'red' }}">{{ 'RUNNING' if running else 'STOPPED' }}</b></p>
    <p>Last update: {{ last_update }}</p>

    <h2>Equity Curve</h2>
    <canvas id="equityChart" height="100"></canvas>

    <h2>Recent Trades</h2>
    <table id="trades">
        <thead>
            <tr><th>Time</th><th>Symbol</th><th>Type</th><th>Side</th><th>Price</th><th>Qty</th><th>PNL</th><th>Equity</th></tr>
        </thead>
        <tbody>
        {% for row in trades %}
            <tr>
                <td>{{ row['time'] }}</td>
                <td>{{ row['symbol'] }}</td>
                <td>{{ row['type'] }}</td>
                <td>{{ row['side'] }}</td>
                <td>{{ row['price'] }}</td>
                <td>{{ row['qty'] }}</td>
                <td>{{ row['pnl'] }}</td>
                <td>{{ row['equity'] }}</td>
            </tr>
        {% endfor %}
        </tbody>
    </table>

<script>
async function fetchEquity() {
    const resp = await fetch('/data');
    const data = await resp.json();
    const ctx = document.getElementById('equityChart').getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.labels,
            datasets: [{ label: 'Equity', data: data.values, borderColor: '#4CAF50', fill: false }]
        },
        options: { scales: { x: { display: false } } }
    });
}

async function toggleBot() {
    await fetch('/toggle', { method: 'POST' });
    location.reload();
}

fetchEquity();
</script>
</body>
</html>
"""

# --- מצב בוט (נשמר בזיכרון בלבד) ---
BOT_RUNNING = True

@app.route('/')
def dashboard():
    trades = []
    if os.path.exists(TRADES_PATH):
        df = pd.read_csv(TRADES_PATH)
        trades = df.tail(20).to_dict('records')
    last_update = time.ctime(os.path.getmtime(EQUITY_PATH)) if os.path.exists(EQUITY_PATH) else "N/A"
    return render_template_string(HTML_TEMPLATE, trades=trades, last_update=last_update, running=BOT_RUNNING)

@app.route('/data')
def data():
    if not os.path.exists(EQUITY_PATH):
        return jsonify({'labels': [], 'values': []})
    df = pd.read_csv(EQUITY_PATH)
    return jsonify({'labels': df['time'].tolist(), 'values': df['equity'].tolist()})

@app.route('/toggle', methods=['POST'])
def toggle():
    global BOT_RUNNING
    BOT_RUNNING = not BOT_RUNNING
    print("⚙️ Bot running =", BOT_RUNNING)
    return jsonify({'running': BOT_RUNNING})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
