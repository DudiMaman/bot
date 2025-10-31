import os, csv, math, time
from datetime import datetime, timezone

LOG_DIR = os.getenv("LOG_DIR", os.path.join(os.path.dirname(__file__), "logs"))
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")

def _as_float(v, default=None):
    try: return float(v)
    except Exception: return default

def _as_int(v, default=None):
    try: return int(v)
    except Exception: return default

def _now_ts(): return int(time.time())

def _parse_ts(s):
    try:
        if s.endswith("Z"):
            return int(datetime.fromisoformat(s.replace("Z","+00:00")).timestamp())
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None

def read_equity(default_equity=100000.0):
    try:
        if not os.path.exists(EQUITY_CSV):
            return default_equity
        last = None
        with open(EQUITY_CSV, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r: last = row
        if last and "equity" in last:
            return _as_float(last["equity"], default_equity) or default_equity
    except Exception:
        pass
    return default_equity

def read_open_positions_snapshot(window_sec=6*3600):
    """הערכה שמרנית של מספר 'פתוחות' לפי ENTER בחלון זמן."""
    now = _now_ts()
    per_symbol_notional, count_all = {}, 0
    if not os.path.exists(TRADES_CSV): return 0, per_symbol_notional
    try:
        with open(TRADES_CSV, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                ts = _parse_ts(row.get("time","")) or 0
                if window_sec and ts and (now - ts) > window_sec: continue
                ttype = (row.get("type","") or "").lower()
                side  = (row.get("side","") or "").lower()
                if ("enter" in ttype) or (side in {"long","short"}):
                    sym = row.get("symbol") or row.get("pair") or ""
                    px  = _as_float(row.get("price"))
                    qty = _as_float(row.get("qty"))
                    if sym and px and qty:
                        notional = abs(px * qty)
                        per_symbol_notional[sym] = per_symbol_notional.get(sym, 0.0) + notional
                        count_all += 1
    except Exception:
        pass
    return count_all, per_symbol_notional

def guard_open(symbol: str, side: str, price: float, qty: float):
    """
    מחזיר (ok: bool, reason: str). נשען על ENV:
      - MAX_CONCURRENT_POSITIONS (default 10)
      - RISK_MAX_POSITION_USD (default 2500)
      - MAX_POS_PER_SYMBOL (default 3)
      - RISK_MAX_SYMBOL_EXPOSURE_PCT (default 0.15)
    """
    symbol = (symbol or "").upper()
    side = (side or "").lower()

    max_conc   = _as_int(os.getenv("MAX_CONCURRENT_POSITIONS"), 10)
    max_usd    = _as_float(os.getenv("RISK_MAX_POSITION_USD"), 2500.0)
    max_per_sy = _as_int(os.getenv("MAX_POS_PER_SYMBOL"), 3)
    max_sympct = _as_float(os.getenv("RISK_MAX_SYMBOL_EXPOSURE_PCT"), 0.15)

    equity = read_equity()
    order_notional = abs((price or 0.0) * (qty or 0.0))

    if max_usd and order_notional > max_usd:
        return (False, f"notional {order_notional:.2f} > limit {max_usd:.2f}")

    count_all, per_symbol_notional = read_open_positions_snapshot()

    if max_conc and count_all >= max_conc:
        return (False, f"open positions {count_all} >= max {max_conc}")

    sym_count = 0
    if os.path.exists(TRADES_CSV):
        try:
            with open(TRADES_CSV, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    if (row.get("symbol") or "").upper() == symbol:
                        ttype = (row.get("type","") or "").lower()
                        rside = (row.get("side","") or "").lower()
                        if ("enter" in ttype) or (rside in {"long","short"}):
                            sym_count += 1
        except Exception:
            pass
    if max_per_sy and sym_count >= max_per_sy:
        return (False, f"{symbol} entries {sym_count} >= max {max_per_sy}")

    sym_expo = per_symbol_notional.get(symbol, 0.0) + order_notional
    if equity and max_sympct and sym_expo > (equity * max_sympct):
        return (False, f"{symbol} exposure {sym_expo:.2f} > {max_sympct*100:.0f}% of equity {equity:.2f}")

    return (True, "ok")
