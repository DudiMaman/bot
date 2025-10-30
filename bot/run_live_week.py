# bot/run_live_week.py
# ------------------------------------------------------------
# Trading bot (weekly loop) with:
# - Safe filtering of config keys for DonchianTrendADXRSI / TradeManager
# - Valid symbol filtering from exchange (Bybit testnet via CCXT)
# - Robust try/except around data fetches (no crashes on single symbol failure)
# - Dual logging: CSV files + optional Postgres via DB if DATABASE_URL is set
# - Quantity normalization against exchange precision & min limits (ccxt)
# ------------------------------------------------------------

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

# Ensure local package imports work both "python -m bot.run_live_week" and direct
THIS_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(THIS_DIR)
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)
if THIS_DIR not in sys.path:
    sys.path.append(THIS_DIR)

# Bot package imports
from bot.strategies import DonchianTrendADXRSI
from bot.risk import RiskManager, TradeManager
from bot.utils import atr as calc_atr
from bot.connectors.ccxt_connector import CCXTConnector
from bot.db_writer import DB  # provides write_trade(dict), write_equity(dict)

# Optional Alpaca connector
try:
    from bot.connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None

# Paths for CSV logging
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


def resample_htf(df: pd.DataFrame, htf: str) -> pd.DataFrame:
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    return df.resample(htf).agg(agg).dropna()


def prepare_features(ltf_df: pd.DataFrame, htf_df: pd.DataFrame, strat: DonchianTrendADXRSI) -> pd.DataFrame:
    """Call strategy.prepare() and attach ATR(14) from LTF."""
    f = strat.prepare(ltf_df, htf_df)
    f["atr"] = calc_atr(ltf_df, 14)
    return f


def normalize_qty(exchange, symbol: str, qty: float, price: float) -> float:
    """
    Normalize quantity against exchange precision and raise to min amount/cost if needed.
    Returns >0 float if feasible, otherwise 0.0
    """
    if qty is None or qty <= 0:
        return 0.0

    # Try to get market metadata
    market = None
    try:
        market = exchange.markets.get(symbol) or exchange.market(symbol)
    except Exception:
        pass

    # Precision -> amount_to_precision
    try:
        qty = float(exchange.amount_to_precision(symbol, qty))
    except Exception:
        qty = float(f"{qty:.8f}")

    # Limits
    min_amt = None
    min_cost = None
    if market:
        limits = market.get("limits") or {}
        amt_limits = limits.get("amount") or {}
        cost_limits = limits.get("cost") or {}
        min_amt = amt_limits.get("min")
        min_cost = cost_limits.get("min")

    if min_amt is not None and qty < min_amt:
        qty = min_amt

    if min_cost is not None and price and price > 0:
        if qty * price < min_cost:
            qty = (min_cost / price)

    # Round again after adjustments
    try:
        qty = float(exchange.amount_to_precision(symbol, qty))
    except Exception:
        qty = float(f"{qty:.8f}")

    return qty if qty > 0 else 0.0


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    # 1) Load env + config
    load_dotenv()
    with open(os.path.join(THIS_DIR, "config.yml"), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # 2) Init DB if available
    db = None
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        try:
            db = DB(database_url)
        except Exception as e:
            print(f"[WARN] DB init failed: {e}")
            db = None

    # 3) Build Strategy / TradeManager with safe filtering of keys
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

    # 4) Portfolio
    portfolio = cfg.get("portfolio", {}) or {}
    equity = float(portfolio.get("equity0", 100_000.0))
    rm = RiskManager(
        equity=equity,
        risk_per_trade=float(portfolio.get("risk_per_trade", 0.005)),
        max_position_pct=float(portfolio.get("max_position_pct", 0.10)),
    )

    # 5) Initial equity log (CSV + DB if available)
    now_utc = datetime.now(timezone.utc)
    write_csv(EQUITY_CSV, ["time", "equity"], [[now_utc.isoformat(), f"{equity:.2f}"]])
    if db:
        try:
            db.write_equity({"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")})
        except Exception as e:
            print(f"[WARN] DB write_equity init failed: {e}")

    # 6) Build connectors + filter valid symbols from exchange
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

        try:
            conn.init()
        except Exception as e:
            print(f"❌ init() failed for connector {c.get('name','?')}: {repr(e)}")
            continue

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

        c_local = dict(c)
        c_local["symbols"] = valid_syms
        conns.append((c_local, conn))

    # 7) Init CSV trades header
    write_csv(TRADES_CSV, ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"], [])

    # Runtime state
    open_positions: dict = {}   # key=(connector_name, symbol) -> position dict
    cooldowns: dict = {}
    last_bar_ts: dict = {}
    start_time = time.time()
    SECONDS_IN_WEEK = 7 * 24 * 60 * 60

    # 8) Main loop
    while True:
        now_utc = datetime.now(timezone.utc)
        rows_trades = []
        snapshots: dict = {}  # key -> last row of features

        # Fetch & features
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

        # Progress check (new bars?)
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
            # still log equity periodically
            write_csv(EQUITY_CSV, ["time", "equity"], [[now_utc.isoformat(), f"{equity:.2f}"]])
            if db:
                try:
                    db.write_equity({"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")})
                except Exception as e:
                    print(f"[WARN] DB write_equity loop failed: {e}")
            continue

        # Manage open positions
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

            # Trailing by ATR
            if atr_now:
                trail = tm.trail_level(side, price, atr_now, after_tp1=pos["tp1_done"])
                if side == "long":
                    pos["sl"] = max(pos["sl"], trail)
                else:
                    pos["sl"] = min(pos["sl"], trail)

            # Move to BE
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

            # Stop-loss
            if (side == "long" and price <= pos["sl"]) or (side == "short" and price >= pos["sl"]):
                price_exit = pos["sl"]
                pnl = (price_exit - entry) * pos["qty"] if side == "long" else (entry - price_exit) * pos["qty"]
                equity += pnl
                rows_trades.append(
                    [now_utc.isoformat(), key[0], key[1], "SL", side, f"{price_exit:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"]
                )
                to_close.append(key)

            # Time-based exit
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

        # Entries (use normalize_qty and conn.exchange)
        for c_cfg, conn in conns:
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

                # Position sizing (risk & cap)
                qty_risk = (equity * rm.risk_per_trade) / R
                qty_cap = (equity * rm.max_position_pct) / max(price, 1e-9)
                raw_qty = max(0.0, min(qty_risk, qty_cap))

                # Respect exchange precision & minimums
                qty = normalize_qty(conn.exchange, sym, raw_qty, price)
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

        # Persist logs (CSV + optional DB)
        if rows_trades:
            write_csv(TRADES_CSV, ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"], rows_trades)
            if db and hasattr(db, "write_trade"):
                for r in rows_trades:
                    try:
                        db.write_trade({
                            "time": r[0],
                            "connector": r[1],
                            "symbol": r[2],
                            "type": r[3],
                            "side": r[4],
                            "price": float(r[5]) if r[5] else None,
                            "qty": float(r[6]) if r[6] else None,
                            "pnl": float(r[7]) if (r[7] not in ("", None)) else None,
                            "equity": float(r[8]) if r[8] else None,
                        })
                    except Exception as e:
                        print(f"[WARN] DB write_trade failed: {e}")

        write_csv(EQUITY_CSV, ["time", "equity"], [[now_utc.isoformat(), f"{equity:.2f}"]])
        if db:
            try:
                db.write_equity({"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")})
            except Exception as e:
                print(f"[WARN] DB write_equity loop failed: {e}")

        # End-of-week stop
        if time.time() - start_time >= SECONDS_IN_WEEK:
            break

        time.sleep(30)


if __name__ == "__main__":
    main()
