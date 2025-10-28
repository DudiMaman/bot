import os, sys, yaml, time, math, csv
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd

# ✅ ייבוא מוחלטים לפי המבנה החדש
from bot.strategies import DonchianTrendADXRSI
from bot.risk import RiskManager, TradeManager
from bot.utils import atr
from bot.connectors.ccxt_connector import CCXTConnector

try:
    from bot.connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None


LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)


def round_qty(qty, step=0.000001):
    return max(0.0, math.floor(qty / step) * step)


def resample_htf(df: pd.DataFrame, htf: str) -> pd.DataFrame:
    agg = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
    return df.resample(htf).agg(agg).dropna()


def prepare_features(ltf_df: pd.DataFrame, htf_df: pd.DataFrame, strat: DonchianTrendADXRSI) -> pd.DataFrame:
    from bot.utils import atr as _atr
    f = strat.prepare(ltf_df, htf_df)
    f['atr'] = _atr(ltf_df, 14)
    return f


def write_csv(path, header, rows):
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as fh:
        import csv as _csv
        w = _csv.writer(fh)
        if new:
            w.writerow(header)
        for r in rows:
            w.writerow(r)


def main():
    load_dotenv()

    with open('bot/config.yml', 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    # ✅ אסטרטגיה וניהול סיכונים
    strat = DonchianTrendADXRSI(**cfg['strategy'])
    tm = TradeManager(**cfg['trade_manager'])

    equity = float(cfg['portfolio']['equity0'])
    rm = RiskManager(equity, cfg['portfolio']['risk_per_trade'], cfg['portfolio']['max_position_pct'])

    # ✅ יצירת חיבורים
    conns = []
    for c in cfg['live_connectors']:
        if c['type'] == 'ccxt':
            conn = CCXTConnector(c['exchange_id'], paper=c.get('paper', True), default_type=c.get('default_type', 'spot'))
        elif c['type'] == 'alpaca':
            if AlpacaConnector is None:
                print("Alpaca connector not available; skipping Alpaca.")
                continue
            conn = AlpacaConnector(paper=c.get('paper', True))
        else:
            continue
        conn.init()
        conns.append((c, conn))

    # ✅ לוגים
    trades_path = os.path.join(LOG_DIR, "trades.csv")
    equity_path = os.path.join(LOG_DIR, "equity_curve.csv")
    write_csv(trades_path, ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"], [])
    write_csv(equity_path, ["time", "equity"], [[datetime.now(timezone.utc).isoformat(), equity]])

    # ✅ לולאת הריצה
    open_positions = {}
    cooldowns = {}
    last_bar_ts = {}
    start_time = time.time()
    SECONDS_IN_WEEK = 7 * 24 * 60 * 60

    while True:
        now_utc = datetime.now(timezone.utc)
        rows_equity = []
        rows_trades = []
        snapshots = {}

        for c_cfg, conn in conns:
            tf = c_cfg['timeframe']
            htf = c_cfg['htf_timeframe']
            for sym in c_cfg.get('symbols', []):
                try:
                    ltf_df = conn.fetch_ohlcv(sym, tf, limit=600)
                    htf_df = conn.fetch_ohlcv(sym, htf, limit=600)
                    feats = prepare_features(ltf_df, htf_df, strat)
                    last = feats.iloc[-1]
                    key = (c_cfg['name'], sym)
                    snapshots[key] = last
                except Exception as e:
                    print(f"⚠️ Error fetching {sym}: {e}")
                    continue

        progressed_any = False
        for key, row in snapshots.items():
            ts = row.name
            if last_bar_ts.get(key) != ts:
                last_bar_ts[key] = ts
                progressed_any = True

        if not progressed_any:
            time.sleep(15)
            if time.time() - start_time >= SECONDS_IN_WEEK:
                break
            continue

        # ✅ ניהול עסקאות
        # (שאר הקוד שלך כפי שהיה — לא צריך לשנות)

        time.sleep(30)
        if time.time() - start_time >= SECONDS_IN_WEEK:
            break


if __name__ == "__main__":
    main()
