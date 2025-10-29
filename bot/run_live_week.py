# bot/run_live_week.py
# ------------------------------------------------------------
# בוט בסיסי לריצה רציפה כשבוע, עם:
# - סינון מפתחות חוקיים ל-DonchianTrendADXRSI ול-TradeManager
# - סינון סימבולים שאינם קיימים בפועל ב-Bybit Testnet
# - עיטוף משיכת נתונים ב-try/except כדי למנוע קריסות
# - לוגים מסודרים ל-logs/trades.csv ו-logs/equity_curve.csv
# ------------------------------------------------------------

from bot.db_writer import DB
import os
import sys
import math
import time
import yaml
import csv as _csv
import inspect
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

# הבטחת נתיב ייבוא תקין גם בהרצה כקובץ וגם כמודול
THIS_DIR = os.path.dirname(__file__)
if THIS_DIR not in sys.path:
    sys.path.append(THIS_DIR)

# ייבוא רכיבי הבוט מתוך החבילה bot
from bot.strategies import DonchianTrendADXRSI
from bot.risk import RiskManager, TradeManager
from bot.utils import atr as calc_atr
from bot.connectors.ccxt_connector import CCXTConnector

# Alpaca הוא אופציונלי — אין בעיה אם לא קיים
try:
    from bot.connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None

LOG_DIR = os.path.join(THIS_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")


# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------
def write_csv(path: str, header: list[str], rows: list[list]):
    new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = _csv.writer(fh)
        if new_file:
            w.writerow(header)
        for r in rows:
            w.writerow(r)


def round_qty(qty: float, step: float = 0.000001) -> float:
    return max(0.0, math.floor(qty / step) * step)


def resample_htf(df: pd.DataFrame, htf: str) -> pd.DataFrame:
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    return df.resample(htf).agg(agg).dropna()


def prepare_features(ltf_df: pd.DataFrame, htf_df: pd.DataFrame, strat: DonchianTrendADXRSI) -> pd.DataFrame:
    """
    קורא ל-prepare של האסטרטגיה, ומוסיף ATR מהטיים-פריים הנמוך (ltf).
    נדרש שהאסטרטגיה תחזיר DataFrame עם אינדקס זמן ועמודות close/long_setup/short_setup לפחות.
    """
    f = strat.prepare(ltf_df, htf_df)
    f["atr"] = calc_atr(ltf_df, 14)
    return f


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    # 1) טען ENV וקובץ קונפיג
    load_dotenv()
    with open(os.path.join(THIS_DIR, "config.yml"), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
        from bot.db_writer import DB  # אם כבר קיים למעלה – אל תוסיף שוב

# אחרי טעינת הקונפיג/ENV:
db = DB(os.getenv("DATABASE_URL"))


    # 2) בנה אסטרטגיה ו-TradeManager עם סינון מפתחות חוקיים בלבד
    import inspect

    raw_s = cfg.get("strategy", {}) or {}
    accepted_s = set(inspect.signature(DonchianTrendADXRSI).parameters.keys())
    clean_s = {k: v for k, v in raw_s.items() if k in accepted_s}
    unknown_s = sorted(set(raw_s.keys()) - accepted_s)
    if unknown_s:
        print(f"⚠️ Ignoring unknown strategy keys: {unknown_s}")

    strat = DonchianTrendADXRSI(**clean_s)

    raw_t = cfg.get("trade_manager", {}) or {}
    accepted_t = set(inspect.signature(TradeManager).parameters.keys())
    clean_t = {k: v for k, v in raw_t.items() if k in accepted_t}
    unknown_t = sorted(set(raw_t.keys()) - accepted_t)
    if unknown_t:
        print(f"⚠️ Ignoring unknown trade_manager keys: {unknown_t}")

    tm = TradeManager(**clean_t)

    # 3) הגדר פורטפוליו
    portfolio = cfg.get("portfolio", {}) or {}
    equity = float(portfolio.get("equity0", 100_000.0))
    rm = RiskManager(
        equity=equity,
        risk_per_trade=float(portfolio.get("risk_per_trade", 0.005)),
        max_position_pct=float(portfolio.get("max_position_pct", 0.10)),
    )

    # 4) יצירת חיבורים בהתאם להגדרות בקונפיג + סינון סימבולים קיימים בפועל
    conns: list[tuple[dict, object]] = []
    live_connectors = cfg.get("live_connectors", []) or []

    for c in live_connectors:
        ctype = c.get("type")
        if ctype == "ccxt":
            conn = CCXTConnector(
                c.get("exchange_id", "bybit"),
                paper=c.get("paper", True),
                default_type=c.get("default_type", "spot"),
            )
        elif ctype == "alpaca":
            if AlpacaConnector is None:
                print("ℹ️ Alpaca connector not available — skipping.")
                continue
            conn = AlpacaConnector(paper=c.get("paper", True))
        else:
            print(f"ℹ️ Unknown connector type '{ctype}' — skipping.")
            continue

        # init בורסה
        try:
            conn.init()
        except Exception as e:
            print(f"❌ init() failed for connector {c.get('name','?')}: {repr(e)}")
            continue

        # סינון סימבולים לפי מה שבאמת קיים לשוק הזה
        available = set(getattr(conn.exchange, "symbols", []) or [])
        cfg_syms = list(c.get("symbols", []) or [])
        valid_syms = [s for s in cfg_syms if s in available]

        if not valid_syms:
            print(
                f"⚠️ No valid symbols for connector '{c.get('name','ccxt')}'. "
                f"Requested={len(cfg_syms)}, Available={len(available)}"
            )
        else:
            print(
                f"✅ Connector '{c.get('name','ccxt')}' loaded {len(valid_syms)} valid symbols "
                f"(of {len(cfg_syms)} requested)."
            )

        # נעדכן את עותק הקונפיג של הקונקטור כדי שהלולאה תשתמש רק ב-valid_syms
        c_local = dict(c)
        c_local["symbols"] = valid_syms
        conns.append((c_local, conn))

    # 5) אתחל לוגים
    write_csv(TRADES_CSV, ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"], [])
    write_csv(EQUITY_CSV, ["time", "equity"], [[datetime.now(timezone.utc).isoformat(), f"{equity:.2f}"]])

    # משתני ריצה
    open_positions: dict = {}   # key=(connector_name, symbol)
    cooldowns: dict = {}
    last_bar_ts: dict = {}
    start_time = time.time()
    SECONDS_IN_WEEK = 7 * 24 * 60 * 60

    # 6) לולאת ריצה
    while True:
        now_utc = datetime.now(timezone.utc)
        rows_trades = []

        # --- שלב משיכת נתונים והפקת פיצ'רים ---
        snapshots: dict = {}  # key -> last row of features
        for c_cfg, conn in conns:
            tf = c_cfg.get("timeframe", "15m")
            htf = c_cfg.get("htf_timeframe", "1h")
            for sym in c_cfg.get("symbols", []):
                try:
                    ltf_df = conn.fetch_ohlcv(sym, tf, limit=600)
                    htf_df = conn.fetch_ohlcv(sym, htf, limit=600)
                    feats = prepare_features(ltf_df, htf_df, strat)
                    last = feats.iloc[-1]
                    key = (c_cfg.get("name", "ccxt"), sym)
                    snapshots[key] = last
                except Exception as e:
                    print(f"⏭️ skip {sym}: {repr(e)}")
                    continue

        # בדיקה שהתחדש נר כלשהו (כדי לא לעשות חישובים חוזרים)
        progressed_any = False
        for key, row in snapshots.items():
            ts = row.name
            if last_bar_ts.get(key) != ts:
                last_bar_ts[key] = ts
                progressed_any = True
        if not progressed_any:
            # אין חדש — נחכה קצת ונמשיך
            time.sleep(15)
            if time.time() - start_time >= SECONDS_IN_WEEK:
                break
            continue

        # --- ניהול פוזיציות פתוחות ---
        to_close = []
        for key, pos in list(open_positions.items()):
            row = snapshots.get(key)
            if row is None:
                continue

            price = float(row["close"])
            atr_now = float(row["atr"]) if pd.notna(row["atr"]) else None
            side = pos["side"]
            entry = pos["entry"]
            qty = pos["qty"]
            R = pos["R"]

            # טריילינג לפי ATR
            if atr_now:
                trail = tm.trail_level(side, price, atr_now, after_tp1=pos["tp1_done"])
                if side == "long":
                    pos["sl"] = max(pos["sl"], trail)
                else:
                    pos["sl"] = min(pos["sl"], trail)

            # העברת SL ל-B/E אחרי be_after_R * R
            if not pos["moved_to_be"] and atr_now:
                if side == "long" and price >= entry + tm.be_after_R * R:
                    pos["sl"] = max(pos["sl"], entry)
                    pos["moved_to_be"] = True
                if side == "short" and price <= entry - tm.be_after_R * R:
                    pos["sl"] = min(pos["sl"], entry)
                    pos["moved_to_be"] = True

            # TP1
            if (not pos["tp1_done"]) and (
                (side == "long" and price >= pos["tp1"]) or (side == "short" and price <= pos["tp1"])
            ):
                close_qty = qty * tm.p1_pct
                pnl = (price - entry) * close_qty if side == "long" else (entry - price) * close_qty
                equity += pnl
                pos["qty"] = qty - close_qty
                pos["tp1_done"] = True
                rows_trades.append(
                    [now_utc.isoformat(), key[0], key[1], "TP1", side, f"{price:.8f}", f"{close_qty:.8f}", f"{pnl:.2f}", f"{equity:.2f}"]
                )

            # TP2
            if (not pos["tp2_done"]) and (
                (side == "long" and price >= pos["tp2"]) or (side == "short" and price <= pos["tp2"])
            ):
                close_qty = pos["qty"] * tm.p2_pct
                pnl = (price - entry) * close_qty if side == "long" else (entry - price) * close_qty
                equity += pnl
                pos["qty"] = pos["qty"] - close_qty
                pos["tp2_done"] = True
                rows_trades.append(
                    [now_utc.isoformat(), key[0], key[1], "TP2", side, f"{price:.8f}", f"{close_qty:.8f}", f"{pnl:.2f}", f"{equity:.2f}"]
                )

            # פגיעה ב-SL
            if (side == "long" and price <= pos["sl"]) or (side == "short" and price >= pos["sl"]):
                price_exit = pos["sl"]
                pnl = (price_exit - entry) * pos["qty"] if side == "long" else (entry - price_exit) * pos["qty"]
                equity += pnl
                rows_trades.append(
                    [now_utc.isoformat(), key[0], key[1], "SL", side, f"{price_exit:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"]
                )
                to_close.append(key)

            # יציאה בכוח אחרי מספר נרות מקסימלי
            pos["bars"] += 1
            if pos["bars"] >= tm.max_bars_in_trade and not pos["tp2_done"]:
                pnl = (price - entry) * pos["qty"] if side == "long" else (entry - price) * pos["qty"]
                equity += pnl
                rows_trades.append(
                    [now_utc.isoformat(), key[0], key[1], "TIME", side, f"{price:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"]
                )
                to_close.append(key)

        for key in to_close:
            open_positions.pop(key, None)

        # --- כניסות חדשות ---
        for c_cfg, _ in conns:
            tf = c_cfg.get("timeframe", "15m")
            for sym in c_cfg.get("symbols", []):
                key = (c_cfg.get("name", "ccxt"), sym)
                if key in open_positions:
                    continue
                if cooldowns.get(key, 0) > 0:
                    cooldowns[key] = max(0, cooldowns.get(key, 0) - 1)
                    continue

                row = snapshots.get(key)
                if row is None or pd.isna(row.get("atr")) or row["atr"] <= 0:
                    continue

                # סיגנל אסטרטגיה
                sig = 1 if row.get("long_setup") else (-1 if row.get("short_setup") else 0)
                if sig == 0:
                    continue

                price = float(row["close"])
                atr_now = float(row["atr"])
                side = "long" if sig == 1 else "short"

                # הגדרת SL/TP ע"פ R ו-ATR
                sl = price - tm.atr_k_sl * atr_now if side == "long" else price + tm.atr_k_sl * atr_now
                R = (price - sl) if side == "long" else (sl - price)
                if R <= 0:
                    continue

                # גודל פוזיציה
                qty_risk = (equity * rm.risk_per_trade) / R
                qty_cap = (equity * rm.max_position_pct) / max(price, 1e-9)
                qty = round_qty(max(0.0, min(qty_risk, qty_cap)))
                if qty <= 0:
                    continue

                tp1 = price + tm.r1_R * R if side == "long" else price - tm.r1_R * R
                tp2 = price + tm.r2_R * R if side == "long" else price - tm.r2_R * R

                open_positions[key] = {
                    "side": side,
                    "entry": price,
                    "sl": sl,
                    "tp1": tp1,
                    "tp2": tp2,
                    "qty": qty,
                    "R": R,
                    "bars": 0,
                    "tp1_done": False,
                    "tp2_done": False,
                    "moved_to_be": False,
                }
                rows_trades.append(
                    [now_utc.isoformat(), key[0], key[1], "ENTER", side, f"{price:.8f}", f"{qty:.8f}", "", f"{equity:.2f}"]
                )

        # כתיבת לוגים
        if rows_trades:
            db.write_trades(rows_trades)
        db.write_equity({
    "time": now_utc.isoformat(),
    "equity": float(f"{equity:.2f}")
})


        # סיום שבוע
        if time.time() - start_time >= SECONDS_IN_WEEK:
            break

        time.sleep(30)


if __name__ == "__main__":
    main()
