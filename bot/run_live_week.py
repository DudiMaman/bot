import os, math, time, traceback
import yaml
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

# --- מודולים פנימיים של הבוט ---
from strategies import DonchianTrendADXRSI
from risk import RiskManager, TradeManager
from utils import atr
from connectors.ccxt_connector import CCXTConnector

# אופציונלי: אלפקה (אם אין – פשוט מדלגים)
try:
    from connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None

# --- הגדרות כלליות ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# --- אינטגרציה עם monitor_fixer ---
import requests
MONITOR_URL = os.getenv("MONITOR_URL")            # למשל: https://your-monitor.onrender.com
MONITOR_API_KEY = os.getenv("MONITOR_API_KEY")    # אותו מפתח שהוגדר ב-monitor_fixer

def send_monitor(event_type: str, payload: dict, level: str = "INFO", source: str = "bot") -> None:
    """שולח אירוע/לוג לשירות monitor_fixer (/ingest). לא מפיל את הבוט במקרה של כשל."""
    if not MONITOR_URL:
        return
    try:
        headers = {}
        if MONITOR_API_KEY:
            headers["x-monitor-key"] = MONITOR_API_KEY
        requests.post(
            f"{MONITOR_URL.rstrip('/')}/ingest",
            json={
                "level": level,
                "source": source,
                "event_type": event_type,
                "payload": payload
            },
            headers=headers,
            timeout=5
        )
    except Exception:
        # לא עוצרים את הריצה אם הדיווח נכשל
        pass

# --- עזרים ---
def write_csv(path, header, rows):
    new = not os.path.exists(path)
    import csv as _csv
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = _csv.writer(fh)
        if new:
            w.writerow(header)
        for r in rows:
            w.writerow(r)

def round_qty(qty, step=0.000001):
    return max(0.0, math.floor(qty / step) * step)

def prepare_features(ltf_df: pd.DataFrame, htf_df: pd.DataFrame, strat: DonchianTrendADXRSI) -> pd.DataFrame:
    """מכין פיצ’רים לאסטרטגיה ומוסיף ATR מה־ltf."""
    f = strat.prepare(ltf_df, htf_df)
    f["atr"] = atr(ltf_df, 14)
    return f

# --- MAIN ---
def main():
    load_dotenv()

    # 1) טען קונפיג בבטחה (מתוך bot/config.yml ליד הקובץ)
    CONFIG_LOCAL_PATH = os.getenv(
        "CONFIG_LOCAL_PATH",
        os.path.join(os.path.dirname(__file__), "config.yml")
    )
    with open(CONFIG_LOCAL_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # 2) נבנה את האסטרטגיה כך שתישאר תואמת לשם פרמטרים בפועל
    import inspect
    raw_s = cfg.get("strategy", {}) or {}
    accepted = set(inspect.signature(DonchianTrendADXRSI).parameters.keys())
    clean_s = {k: v for k, v in raw_s.items() if k in accepted}
    strat = DonchianTrendADXRSI(**clean_s)

    tm = TradeManager(**cfg["trade_manager"])

    # 3) פורטפוליו (נדרש בקונפיג: portfolio: equity0, risk_per_trade, max_position_pct)
    equity = float(cfg["portfolio"]["equity0"])
    rm = RiskManager(
        equity,
        cfg["portfolio"]["risk_per_trade"],
        cfg["portfolio"]["max_position_pct"]
    )

    # 4) יצירת חיבורים לפי live_connectors
    conns = []
    for c in cfg["live_connectors"]:
        if c["type"] == "ccxt":
            conn = CCXTConnector(
                c["exchange_id"],
                paper=c.get("paper", True),
                default_type=c.get("default_type", "spot")
            )
        elif c["type"] == "alpaca":
            if AlpacaConnector is None:
                print("Alpaca connector not available; skipping Alpaca.")
                continue
            conn = AlpacaConnector(paper=c.get("paper", True))
        else:
            continue
        conn.init()
        conns.append((c, conn))

    # 5) אתחל לוגים
    trades_path = os.path.join(LOG_DIR, "trades.csv")
    equity_path = os.path.join(LOG_DIR, "equity_curve.csv")
    write_csv(trades_path, ["time","connector","symbol","type","side","price","qty","pnl","equity"], [])
    write_csv(equity_path, ["time","equity"], [[datetime.now(timezone.utc).isoformat(), f"{equity:.2f}"]])

    # Heartbeat התחלתי
    try:
        symbols_count = sum(len(c.get("symbols", [])) for c, _ in conns)
    except Exception:
        symbols_count = 0
    send_monitor("HEARTBEAT", {"msg": "bot started", "symbols_count": symbols_count}, "INFO")

    # 6) לולאת ריצה
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

        # שלב משיכה וחישוב פיצ'רים
        for c_cfg, conn in conns:
            tf = c_cfg["timeframe"]
            htf = c_cfg["htf_timeframe"]
            for sym in c_cfg.get("symbols", []):
                try:
                    ltf_df = conn.fetch_ohlcv(sym, tf, limit=600)
                    htf_df = conn.fetch_ohlcv(sym, htf, limit=600)
                    if ltf_df is None or len(ltf_df) < 50 or htf_df is None or len(htf_df) < 50:
                        continue
                    feats = prepare_features(ltf_df, htf_df, strat)
                    last = feats.iloc[-1]
                    key = (c_cfg["name"], sym)
                    snapshots[key] = last
                except Exception as e:
                    # שולחים שגיאה למוניטור כולל traceback
                    send_monitor(
                        event_type="ERROR",
                        payload={"msg": f"fetch/prepare failed for {sym}", "trace": traceback.format_exc()},
                        level="ERROR"
                    )
                    # ממשיכים הלאה – אין הפסקת בוט על כישלון סימבול יחיד
                    continue

        # נוודא שהתקדמנו בר ברים (כדי לא לסחור באותו נר)
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
            # Heartbeat תקופתי
            if int(time.time()) % 300 < 2:
                send_monitor("HEARTBEAT", {"msg": "alive (no new bar)", "open_positions": len(open_positions), "equity": equity}, "INFO")
            continue

        # --- ניהול פוזיציות פתוחות + כניסות חדשות ---
        to_close = []
        for key, pos in list(open_positions.items()):
            row = snapshots.get(key)
            if row is None:
                continue
            price = float(row["close"])
            atr_now = float(row["atr"]) if pd.notna(row["atr"]) else None
            side = pos["side"]; entry = pos["entry"]; qty = pos["qty"]; R = pos["R"]

            # עדכון טריילינג SL
            if atr_now:
                trail = tm.trail_level(side, price, atr_now, after_tp1=pos["tp1_done"])
                if side == "long":
                    pos["sl"] = max(pos["sl"], trail)
                else:
                    pos["sl"] = min(pos["sl"], trail)

            # מעבר ל-B/E אחרי R מוגדר
            if not pos["moved_to_be"] and atr_now:
                if side == "long" and price >= entry + tm.be_after_R * R:
                    pos["sl"] = max(pos["sl"], entry); pos["moved_to_be"] = True
                if side == "short" and price <= entry - tm.be_after_R * R:
                    pos["sl"] = min(pos["sl"], entry); pos["moved_to_be"] = True

            # TP1
            if (not pos["tp1_done"]) and ((side == "long" and price >= pos["tp1"]) or (side == "short" and price <= pos["tp1"])):
                close_qty = qty * tm.p1_pct
                pnl = (price - entry) * close_qty if side == "long" else (entry - price) * close_qty
                equity += pnl; pos["qty"] = qty - close_qty; pos["tp1_done"] = True
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "TP1", side, f"{price:.8f}", f"{close_qty:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])
                send_monitor("TRADE", {"action":"TP1","connector":key[0],"symbol":key[1],"side":side,"price":price,"qty":close_qty,"pnl":pnl,"equity":equity}, "INFO")

            # TP2
            if (not pos["tp2_done"]) and ((side == "long" and price >= pos["tp2"]) or (side == "short" and price <= pos["tp2"])):
                close_qty = pos["qty"] * tm.p2_pct
                pnl = (price - entry) * close_qty if side == "long" else (entry - price) * close_qty
                equity += pnl; pos["qty"] = pos["qty"] - close_qty; pos["tp2_done"] = True
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "TP2", side, f"{price:.8f}", f"{close_qty:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])
                send_monitor("TRADE", {"action":"TP2","connector":key[0],"symbol":key[1],"side":side,"price":price,"qty":close_qty,"pnl":pnl,"equity":equity}, "INFO")

            # SL
            if (side == "long" and price <= pos["sl"]) or (side == "short" and price >= pos["sl"]):
                price_exit = pos["sl"]
                pnl = (price_exit - entry) * pos["qty"] if side == "long" else (entry - price_exit) * pos["qty"]
                equity += pnl
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "SL", side, f"{price_exit:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])
                send_monitor("TRADE", {"action":"SL","connector":key[0],"symbol":key[1],"side":side,"price":price_exit,"qty":pos["qty"],"pnl":pnl,"equity":equity}, "INFO")
                to_close.append(key)

            # יציאה בזמן (safety guard)
            pos["bars"] += 1
            if pos["bars"] >= tm.max_bars_in_trade and not pos["tp2_done"]:
                pnl = (price - entry) * pos["qty"] if side == "long" else (entry - price) * pos["qty"]
                equity += pnl
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "TIME", side, f"{price:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"])
                send_monitor("TRADE", {"action":"TIME","connector":key[0],"symbol":key[1],"side":side,"price":price,"qty":pos["qty"],"pnl":pnl,"equity":equity}, "INFO")
                to_close.append(key)

        for key in to_close:
            open_positions.pop(key, None)

        # כניסות חדשות
        for c_cfg, _ in conns:
            for sym in c_cfg.get("symbols", []):
                key = (c_cfg["name"], sym)
                if key in open_positions or cooldowns.get(key, 0) > 0:
                    cooldowns[key] = max(0, cooldowns.get(key, 0) - 1)
                    continue
                row = snapshots.get(key)
                if row is None or pd.isna(row.get("atr")) or row["atr"] <= 0:
                    continue

                sig = 1 if row.get("long_setup") else (-1 if row.get("short_setup") else 0)
                if sig == 0:
                    continue

                price = float(row["close"])
                atr_now = float(row["atr"])
                side = "long" if sig == 1 else "short"
                sl = price - tm.atr_k_sl * atr_now if side == "long" else price + tm.atr_k_sl * atr_now
                R = (price - sl) if side == "long" else (sl - price)
                if R <= 0:
                    continue

                # חישוב כמות עפ"י סיכון וחשיפה מקסימלית
                risk_cap = equity * cfg["portfolio"]["risk_per_trade"] / R
                exposure_cap = (equity * cfg["portfolio"]["max_position_pct"]) / price
                qty = round_qty(min(risk_cap, exposure_cap))
                if qty <= 0:
                    continue

                tp1 = price + tm.r1_R * R if side == "long" else price - tm.r1_R * R
                tp2 = price + tm.r2_R * R if side == "long" else price - tm.r2_R * R

                open_positions[key] = {
                    "side": side, "entry": price, "sl": sl, "tp1": tp1, "tp2": tp2,
                    "qty": qty, "R": R, "bars": 0,
                    "tp1_done": False, "tp2_done": False, "moved_to_be": False
                }
                rows_trades.append([now_utc.isoformat(), key[0], key[1], "ENTER", side, f"{price:.8f}", f"{qty:.8f}", "", f"{equity:.2f}"])
                send_monitor("TRADE", {"action":"ENTER","connector":key[0],"symbol":key[1],"side":side,"price":price,"qty":qty,"equity":equity}, "INFO")

        # כתיבת לוגים לקבצים
        if rows_trades:
            write_csv(trades_path, ["time","connector","symbol","type","side","price","qty","pnl","equity"], rows_trades)
        write_csv(equity_path, ["time","equity"], [[now_utc.isoformat(), f"{equity:.2f}"]])

        # Heartbeat תקופתי
        if int(time.time()) % 300 < 2:
            send_monitor("HEARTBEAT", {"msg":"alive","open_positions":len(open_positions),"equity":equity}, "INFO")

        # עצירה אחרי שבוע ריצה
        if time.time() - start_time >= SECONDS_IN_WEEK:
            send_monitor("SYSTEM", {"msg": "bot finished 1 week window", "equity": equity}, "INFO")
            break

        time.sleep(30)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # דיווח שגיאה אחרון לפני יציאה
        send_monitor(
            event_type="ERROR",
            payload={"msg": str(e), "trace": traceback.format_exc()},
            level="ERROR"
        )
        raise
