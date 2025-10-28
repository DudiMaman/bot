# ==========================================================
# bot/run_live_week.py — גרסת ריצה יציבה לבוט
# - טוען קונפיג בצורה בטוחה (מהתיקייה של הקובץ)
# - מסנן פרמטרים לא נתמכים לאסטרטגיה (לא נופלים על rsi_buy/rsi_sell)
# - מייבא קונקטורים ויוצר אותם בצורה נקייה
# - מתמודד עם זוגות חסרים/שגויים בלי להפיל את התהליך
# - כותב לוגים תחת bot/logs
# ==========================================================

import os
import sys
import math
import time
import yaml
import inspect
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv

# ייבוא מוחלט מתוך חבילת bot (חשוב שהריצה תהיה עם: python -m bot.run_live_week)
from bot.strategies import DonchianTrendADXRSI
from bot.risk import RiskManager, TradeManager
from bot.utils import atr
from bot.connectors.ccxt_connector import CCXTConnector

# Alpaca אופציונלי
try:
    from bot.connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None

# ----------------------------------------------------------
# נתיבים יציבים יחסית למיקום הקובץ
# ----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent          # bot/
ROOT_DIR = BASE_DIR.parent                           # /
CONFIG_PATH = BASE_DIR / "config.yml"                # bot/config.yml
LOG_DIR = BASE_DIR / "logs"                          # bot/logs
LOG_DIR.mkdir(parents=True, exist_ok=True)

TRADES_CSV = LOG_DIR / "trades.csv"
EQUITY_CSV = LOG_DIR / "equity_curve.csv"


# ----------------------------------------------------------
# Utilities
# ----------------------------------------------------------
def round_qty(qty: float, step: float = 0.000001) -> float:
    return max(0.0, math.floor(qty / step) * step)

def resample_htf(df: pd.DataFrame, htf: str) -> pd.DataFrame:
    agg = {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    return df.resample(htf).agg(agg).dropna()

def prepare_features(ltf_df: pd.DataFrame, htf_df: pd.DataFrame, strat: DonchianTrendADXRSI) -> pd.DataFrame:
    f = strat.prepare(ltf_df, htf_df)
    f["atr"] = atr(ltf_df, 14)
    return f

def write_csv(path: Path, header, rows):
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        import csv as _csv
        w = _csv.writer(fh)
        if new:
            w.writerow(header)
        for r in rows:
            w.writerow(r)


# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
def main():
    # 1) סביבה + קונפיג
    load_dotenv()  # אם תרצה .env בשורש/ב־bot
    if not CONFIG_PATH.exists():
        print(f"🚨 לא נמצא קובץ קונפיג: {CONFIG_PATH}")
        sys.exit(1)

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # 2) אסטרטגיה — סינון פרמטרים לא מוכרים (מונע TypeError)
    raw_s = cfg.get("strategy", {}) or {}
    accepted = set(inspect.signature(DonchianTrendADXRSI).parameters.keys())
    clean_s = {k: v for k, v in raw_s.items() if k in accepted}
    unknown = sorted(set(raw_s) - accepted)
    if unknown:
        print("⚠️ Ignoring unknown strategy keys:", unknown)

    strat = DonchianTrendADXRSI(**clean_s)

    # 3) מנהלי סיכון/טריידים
    trade_cfg = cfg.get("trade_manager", {}) or {}
    tm = TradeManager(**trade_cfg)

    portfolio = cfg.get("portfolio", {}) or {}
    equity = float(portfolio.get("equity0", 100000.0))
    rm = RiskManager(
        equity,
        float(portfolio.get("risk_per_trade", 0.005)),
        float(portfolio.get("max_position_pct", 0.10))
    )

    # 4) קונקטורים חיים
    conns = []
    for c in cfg.get("live_connectors", []) or []:
        try:
            if c.get("type") == "ccxt":
                conn = CCXTConnector(
                    c.get("exchange_id", "bybit"),
                    paper=c.get("paper", True),
                    default_type=c.get("default_type", "spot")
                )
            elif c.get("type") == "alpaca":
                if AlpacaConnector is None:
                    print("ℹ️ Alpaca connector not available; skipping.")
                    continue
                conn = AlpacaConnector(paper=c.get("paper", True))
            else:
                print(f"ℹ️ Unknown connector type: {c.get('type')}, skipping.")
                continue

            conn.init()
            conns.append((c, conn))
            print(f"✅ Connected: {c.get('name','unnamed')} ({c.get('type')})")
        except Exception as e:
            print(f"⚠️ Failed to init connector {c}: {repr(e)}")

    if not conns:
        print("🚨 אין קונקטורים פעילים (live_connectors ריק או נכשל). יציאה.")
        sys.exit(1)

    # 5) אתחל לוגים
    write_csv(TRADES_CSV, ["time","connector","symbol","type","side","price","qty","pnl","equity"], [])
    write_csv(EQUITY_CSV, ["time","equity"], [[datetime.now(timezone.utc).isoformat(), equity]])

    # 6) לולאה
    open_positions = {}
    cooldowns = {}
    last_bar_ts = {}

    while True:
        now_utc = datetime.now(timezone.utc)
        rows_equity, rows_trades = [], []
        snapshots = {}

        # --- משיכת נתונים ---
        for c_cfg, conn in conns:
            tf = c_cfg.get("timeframe", "15m")
            htf = c_cfg.get("htf_timeframe", "1h")
            symbols = c_cfg.get("symbols", []) or []

            for sym in symbols:
                try:
                    ltf_df = conn.fetch_ohlcv(sym, tf, limit=600)
                    htf_df = conn.fetch_ohlcv(sym, htf, limit=600)
                    if ltf_df is None or htf_df is None or ltf_df.empty or htf_df.empty:
                        continue
                    feats = prepare_features(ltf_df, htf_df, strat)
                    if feats.empty:
                        continue
                    last = feats.iloc[-1]
                    key = (c_cfg.get("name", "conn"), sym)
                    snapshots[key] = last
                except Exception as e:
                    # לא מפילים את הבוט בגלל סימבול בעייתי / BadSymbol / Network
                    print(f"⏭️ skip {sym}: {repr(e)}")
                    continue

        # בדיקה אם יש נר חדש איפשהו
        progressed_any = False
        for key, row in snapshots.items():
            ts = row.name  # אינדקס של DF הוא timestamp
            if last_bar_ts.get(key) != ts:
                last_bar_ts[key] = ts
                progressed_any = True

        if not progressed_any:
            time.sleep(15)
            continue

        # --- ניהול פוזיציות פתוחות ---
        to_close = []
        for key, pos in list(open_positions.items()):
            row = snapshots.get(key)
            if row is None:
                continue

            price = float(row["close"])
            atr_now = float(row["atr"]) if pd.notna(row["atr"]) else None
            side = pos["side"]; entry = pos["entry"]; qty = pos["qty"]; R = pos["R"]

            # טריילינג SL
            if atr_now:
                trail = tm.trail_level(side, price, atr_now, after_tp1=pos["tp1_done"])
                if side == "long":
                    pos["sl"] = max(pos["sl"], trail)
                else:
                    pos["sl"] = min(pos["sl"], trail)

            # מעבר ל-BE
            if not pos["moved_to_be"] and atr_now:
                if side == "long" and price >= entry + tm.be_after_R * R:
                    pos["sl"] = max(pos["sl"], entry); pos["moved_to_be"] = True
                if side == "short" and price <= entry - tm.be_after_R * R:
                    pos["sl"] = min(pos["sl"], entry); pos["moved_to_be"] = True

            # TP1
            if (not pos["tp1_done"]) and ((side=="long" and price>=pos["tp1"]) or (side=="short" and price<=pos["tp1"])):
                close_qty = qty * tm.p1_pct
                pnl = (price - entry) * close_qty if side=="long" else (entry - price) * close_qty
                equity += pnl; pos["qty"] = qty - close_qty; pos["tp1_done"] = True
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "TP1", side, f"{price:.8f}", f"{close_qty:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])

            # TP2
            if (not pos["tp2_done"]) and ((side=="long" and price>=pos["tp2"]) or (side=="short" and price<=pos["tp2"])):
                close_qty = pos["qty"] * tm.p2_pct
                pnl = (price - entry) * close_qty if side=="long" else (entry - price) * close_qty
                equity += pnl; pos["qty"] = pos["qty"] - close_qty; pos["tp2_done"] = True
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "TP2", side, f"{price:.8f}", f"{close_qty:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])

            # SL
            if (side=="long" and price <= pos["sl"]) or (side=="short" and price >= pos["sl"]):
                price_exit = pos["sl"]
                pnl = (price_exit - entry) * pos["qty"] if side=="long" else (entry - price_exit) * pos["qty"]
                equity += pnl
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "SL", side, f"{price_exit:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])
                to_close.append(key)

            # מקסימום ברים בפוזיציה
            pos["bars"] += 1
            if pos["bars"] >= tm.max_bars_in_trade and not pos["tp2_done"]:
                pnl = (price - entry) * pos["qty"] if side=="long" else (entry - price) * pos["qty"]
                equity += pnl
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "TIME", side, f"{price:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])
                to_close.append(key)

        for key in to_close:
            open_positions.pop(key, None)

        # --- כניסות חדשות ---
        for c_cfg, _ in conns:
            tf = c_cfg.get("timeframe", "15m")
            symbols = c_cfg.get("symbols", []) or []
            for sym in symbols:
                key = (c_cfg.get("name", "conn"), sym)

                # קירור/דלוג אם פתוח כבר
                if key in open_positions or (cooldowns.get(key, 0) > 0):
                    cooldowns[key] = max(0, cooldowns.get(key, 0) - 1)
                    continue

                row = snapshots.get(key)
                if row is None or pd.isna(row.get("atr")) or row["atr"] <= 0:
                    continue

                sig = 1 if row.get("long_setup") else (-1 if row.get("short_setup") else 0)
                if sig == 0:
                    continue

                price = float(row["close"]); atr_now = float(row["atr"])
                side = "long" if sig == 1 else "short"
                sl = price - tm.atr_k_sl * atr_now if side == "long" else price + tm.atr_k_sl * atr_now
                R = (price - sl) if side == "long" else (sl - price)
                if R <= 0:
                    continue

                # חישוב כמות לפי סיכון/גודל פוזיציה מקסימלי
                risk_per_trade = float(portfolio.get("risk_per_trade", 0.005))
                max_pos_pct = float(portfolio.get("max_position_pct", 0.10))
                qty_risk = (equity * risk_per_trade) / R
                qty_cap  = (equity * max_pos_pct) / price
                qty = round_qty(max(0.0, min(qty_risk, qty_cap)))
                if qty <= 0:
                    continue

                tp1 = price + tm.r1_R * R if side=="long" else price - tm.r1_R * R
                tp2 = price + tm.r2_R * R if side=="long" else price - tm.r2_R * R

                open_positions[key] = {
                    "side": side, "entry": price, "sl": sl, "tp1": tp1, "tp2": tp2,
                    "qty": qty, "R": R, "bars": 0,
                    "tp1_done": False, "tp2_done": False, "moved_to_be": False
                }
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "ENTER", side, f"{price:.8f}", f"{qty:.8f}", "", f"{equity:.2f}"])

        # --- כתיבת לוגים ---
        if rows_trades:
            write_csv(TRADES_CSV, ["time","connector","symbol","type","side","price","qty","pnl","equity"], rows_trades)
        write_csv(EQUITY_CSV, ["time","equity"], [[now_utc.isoformat(), f"{equity:.2f}"]])

        time.sleep(30)


if __name__ == "__main__":
    main()
