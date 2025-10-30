# bot/run_live_week.py
# ------------------------------------------------------------
# Trading bot (weekly loop) with:
# - Safe filtering of config keys for DonchianTrendADXRSI / TradeManager
# - AUTO symbol discovery from Bybit (testnet) via CCXT
# - Valid symbol filtering + min-qty / min-notional / precision alignment
# - Robust try/except around data fetches
# - Dual logging: CSV files + optional Postgres via DB if DATABASE_URL is set
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

# Ensure imports
THIS_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(THIS_DIR)
for p in (ROOT_DIR, THIS_DIR):
    if p not in sys.path:
        sys.path.append(p)

# Bot package imports
from bot.strategies import DonchianTrendADXRSI
from bot.risk import RiskManager, TradeManager
from bot.utils import atr as calc_atr
from bot.connectors.ccxt_connector import CCXTConnector
from bot.db_writer import DB  # optional Postgres

# Optional Alpaca connector
try:
    from bot.connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None

# CSV logging
LOG_DIR = os.path.join(THIS_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")

# ------------------------
# Utilities
# ------------------------
def write_csv(path: str, header: list[str], rows: list[list]):
    new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = _csv.writer(fh)
        if new_file:
            w.writerow(header)
        for r in rows:
            w.writerow(r)

def round_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step

def determine_amount_step(market: dict) -> float:
    """
    Try to derive a quantity step from market precision / limits.
    """
    step = 1e-6
    prec = (market or {}).get('precision') or {}
    # If precision.amount is number of decimals:
    if 'amount' in prec and isinstance(prec['amount'], int):
        # amount=decimals, so step=10^{-decimals}
        step = 10 ** (-prec['amount'])
    else:
        # Sometimes step is given under limits.amount.step
        lim_amt = (market or {}).get('limits', {}).get('amount', {}) or {}
        step = float(lim_amt.get('step') or step)
    return max(step, 1e-12)

def prepare_features(ltf_df: pd.DataFrame, htf_df: pd.DataFrame, strat: DonchianTrendADXRSI) -> pd.DataFrame:
    f = strat.prepare(ltf_df, htf_df)
    f["atr"] = calc_atr(ltf_df, 14)
    return f

# ------------------------
# Main
# ------------------------
def main():
    # 1) Load env + config
    load_dotenv()
    with open(os.path.join(THIS_DIR, "config.yml"), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # 2) Optional DB
    db = None
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        try:
            db = DB(database_url)
        except Exception as e:
            print(f"[WARN] DB init failed: {e}")
            db = None

    # 3) Strategy / TradeManager (safe filtering)
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

    # 5) Initial equity log
    now_utc = datetime.now(timezone.utc)
    write_csv(EQUITY_CSV, ["time", "equity"], [[now_utc.isoformat(), f"{equity:.2f}"]])
    if db:
        try:
            db.write_equity({"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")})
        except Exception as e:
            print(f"[WARN] DB write_equity init failed: {e}")

    # 6) Connectors + AUTO symbol discovery + valid symbol filter
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

        # load markets so we can AUTO and validate
        try:
            markets = conn.exchange.load_markets()
        except Exception as e:
            print(f"❌ load_markets() failed: {e}")
            markets = {}

        requested_syms = list(c.get("symbols", []) or [])
        if "AUTO" in requested_syms:
            # pick up to ~25 USDT spot pairs that are active
            auto_syms = [
                m for m,info in markets.items()
                if info.get('type','spot') == 'spot'
                and info.get('quote') == 'USDT'
                and info.get('active', True)
            ]
            auto_syms = auto_syms[:25]
            cfg_syms = auto_syms
        else:
            cfg_syms = requested_syms

        available = set(getattr(conn.exchange, "symbols", []) or [])
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
    open_positions: dict = {}   # key=(connector_name, symbol)
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
            tf = c_cfg.get("timeframe", "1m")
            htf = c_cfg.get("htf_timeframe", "15m")
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

        # Progress check
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
            # log equity periodically anyway
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

        # Entries
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

                # ----- NEW: align with market min qty / min cost / precision -----
                market = None
                try:
                    market = conn.exchange.market(sym)
                except Exception:
                    market = {}

                step = determine_amount_step(market)
                lot_limits = (market or {}).get('limits', {}).get('amount', {}) or {}
                cost_limits = (market or {}).get('limits', {}).get('cost', {}) or {}
                min_qty  = lot_limits.get('min')
                min_cost = cost_limits.get('min')

                qty_risk = (equity * rm.risk_per_trade) / max(R, 1e-12)
                qty_cap  = (equity * rm.max_position_pct) / max(price, 1e-9)
                qty      = max(0.0, min(qty_risk, qty_cap))

                # round to step
                qty = round_step(qty, step)

                # enforce min qty
                if (min_qty is not None) and (qty < float(min_qty)):
                    qty = float(min_qty)

                # enforce min notional
                notional = qty * price
                if (min_cost is not None) and (notional < float(min_cost)):
                    needed_qty = float(min_cost) / max(price, 1e-9)
                    qty = max(qty, round_step(needed_qty, step))

                # final round and validate
                qty = round_step(qty, step)
                if qty <= 0:
                    continue
                # ----- END market alignment -----

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

        # Persist logs
        if rows_trades:
            write_csv(TRADES_CSV, ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"], rows_trades)
            if db:
                try:
                    db.write_trades(rows_trades)
                except Exception as e:
                    print(f"[WARN] DB write_trades failed: {e}")

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
