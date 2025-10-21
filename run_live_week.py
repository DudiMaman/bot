import yaml, time, math, os, csv
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from strategies import DonchianTrendADXRSI
from risk import RiskManager, TradeManager
from utils import atr
from connectors.ccxt_connector import CCXTConnector

# <<< חדש: יבוא Alpaca אופציונלי >>>
try:
    from connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None

load_dotenv()

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def round_qty(qty, step=0.000001):
    return max(0.0, math.floor(qty/step)*step)

def resample_htf(df: pd.DataFrame, htf: str) -> pd.DataFrame:
    agg = {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}
    return df.resample(htf).agg(agg).dropna()

def prepare_features(ltf_df: pd.DataFrame, htf_df: pd.DataFrame, strat: DonchianTrendADXRSI) -> pd.DataFrame:
    f = strat.prepare(ltf_df, htf_df); f['atr'] = atr(ltf_df, 14)
    return f

def write_csv(path, header, rows):
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new: w.writerow(header)
        for r in rows: w.writerow(r)

def main():
    with open('config.yml','r') as f:
        cfg = yaml.safe_load(f)

    strat = DonchianTrendADXRSI(**cfg['strategy'])
    tm = TradeManager(**cfg['trade_manager'])

    equity = float(cfg['portfolio']['equity0'])
    rm = RiskManager(equity, cfg['portfolio']['risk_per_trade'], cfg['portfolio']['max_position_pct'])

conns = []
for c in cfg['live_connectors']:
    if c['type'] == 'ccxt':
        conn = CCXTConnector(c['exchange_id'], paper=c.get('paper', True), default_type=c.get('default_type','spot'))
    elif c['type'] == 'alpaca':
        if AlpacaConnector is None:
            print("Alpaca connector not available; skipping Alpaca.")
            continue
        conn = AlpacaConnector(paper=c.get('paper', True))
    else:
        continue
    conn.init()
    conns.append((c, conn))

    open_positions = {}
    cooldowns = {}
    last_bar_ts = {}

    trades_path = os.path.join(LOG_DIR, "trades.csv")
    equity_path = os.path.join(LOG_DIR, "equity_curve.csv")
    write_csv(trades_path, ["time","connector","symbol","type","side","price","qty","pnl","equity"], [])
    write_csv(equity_path, ["time","equity"], [[datetime.utcnow().isoformat(), equity]])

    start_time = time.time()
    SECONDS_IN_WEEK = 7*24*60*60

    while True:
        now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        rows_equity = []
        rows_trades = []

        snapshots = {}
        for c_cfg, conn in conns:
            tf = c_cfg['timeframe']; htf = c_cfg['htf_timeframe']
            for sym in c_cfg['symbols']:
                ltf_df = conn.fetch_ohlcv(sym, tf, limit=600)
                htf_df = conn.fetch_ohlcv(sym, htf, limit=600)
                feats = prepare_features(ltf_df, htf_df, strat)
                last = feats.iloc[-1]
                key = (c_cfg['name'], sym)
                snapshots[key] = last

        progressed_any = False
        for key, row in snapshots.items():
            ts = row.name
            if last_bar_ts.get(key) != ts:
                last_bar_ts[key] = ts
                progressed_any = True

        if not progressed_any:
            time.sleep(15)
            if time.time() - start_time >= SECONDS_IN_WEEK: break
            continue

        to_close = []
        for key, pos in list(open_positions.items()):
            row = snapshots.get(key); 
            if row is None: continue
            price = float(row['close'])
            atr_now = float(row['atr']) if pd.notna(row['atr']) else None
            side = pos['side']; entry = pos['entry']; qty = pos['qty']; R = pos['R']

            if atr_now:
                trail = tm.trail_level(side, price, atr_now, after_tp1=pos['tp1_done'])
                if side=='long': pos['sl'] = max(pos['sl'], trail)
                else:            pos['sl'] = min(pos['sl'], trail)

            if not pos['moved_to_be'] and atr_now:
                if side=='long' and price >= entry + tm.be_after_R*R:
                    pos['sl'] = max(pos['sl'], entry); pos['moved_to_be']=True
                if side=='short' and price <= entry - tm.be_after_R*R:
                    pos['sl'] = min(pos['sl'], entry); pos['moved_to_be']=True

            if (not pos['tp1_done']) and ((side=='long' and price>=pos['tp1']) or (side=='short' and price<=pos['tp1'])):
                close_qty = qty*tm.p1_pct
                pnl = (price - entry)*close_qty if side=='long' else (entry - price)*close_qty
                equity += pnl; pos['qty'] = qty - close_qty; pos['tp1_done']=True
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "TP1", side, f"{price:.8f}", f"{close_qty:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])

            if (not pos['tp2_done']) and ((side=='long' and price>=pos['tp2']) or (side=='short' and price<=pos['tp2'])):
                close_qty = pos['qty']*tm.p2_pct
                pnl = (price - entry)*close_qty if side=='long' else (entry - price)*close_qty
                equity += pnl; pos['qty'] = pos['qty'] - close_qty; pos['tp2_done']=True
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "TP2", side, f"{price:.8f}", f"{close_qty:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])

            if (side=='long' and price<=pos['sl']) or (side=='short' and price>=pos['sl']):
                price_exit = pos['sl']
                pnl = (price_exit - entry)*pos['qty'] if side=='long' else (entry - price_exit)*pos['qty']
                equity += pnl
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "SL", side, f"{price_exit:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])
                to_close.append(key)

            pos['bars'] += 1
            if pos['bars'] >= tm.max_bars_in_trade and not pos['tp2_done']:
                pnl = (price - entry)*pos['qty'] if side=='long' else (entry - price)*pos['qty']
                equity += pnl
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "TIME", side, f"{price:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])
                to_close.append(key)

        for key in to_close:
            open_positions.pop(key, None)

        for c_cfg, _ in conns:
            for sym in c_cfg['symbols']:
                key = (c_cfg['name'], sym)
                if key in open_positions or cooldowns.get(key,0) > 0: 
                    cooldowns[key] = max(0, cooldowns.get(key,0)-1)
                    continue
                row = snapshots.get(key); 
                if row is None or pd.isna(row['atr']) or row['atr']<=0: continue
                sig = 1 if row['long_setup'] else (-1 if row['short_setup'] else 0)
                if sig == 0: continue
                price = float(row['close']); atr_now = float(row['atr'])
                side = 'long' if sig==1 else 'short'
                sl = price - tm.atr_k_sl*atr_now if side=='long' else price + tm.atr_k_sl*atr_now
                R = (price - sl) if side=='long' else (sl - price)
                if R <= 0: continue
                qty = min((equity*cfg['portfolio']['risk_per_trade'])/R, (equity*cfg['portfolio']['max_position_pct'])/price)
                qty = round_qty(qty)
                if qty <= 0: continue
                tp1 = price + tm.r1_R*R if side=='long' else price - tm.r1_R*R
                tp2 = price + tm.r2_R*R if side=='long' else price - tm.r2_R*R
                open_positions[key] = {
                    'side': side, 'entry': price, 'sl': sl, 'tp1': tp1, 'tp2': tp2,
                    'qty': qty, 'R': R, 'bars': 0,
                    'tp1_done': False, 'tp2_done': False, 'moved_to_be': False
                }
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "ENTER", side, f"{price:.8f}", f"{qty:.8f}", "", f"{equity:.2f}"])

        if rows_trades: write_csv(trades_path, ["time","connector","symbol","type","side","price","qty","pnl","equity"], rows_trades)
        write_csv(equity_path, ["time","equity"], [[now_utc.isoformat(), f"{equity:.2f}"]])

        time.sleep(30)
        if time.time() - start_time >= SECONDS_IN_WEEK:
            break

if __name__ == "__main__":
    main()

