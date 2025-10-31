# bot/run_live_week.py
# ------------------------------------------------------------
# Trading bot (weekly loop) with:
# - Safe key filtering for DonchianTrendADXRSI / TradeManager
# - AUTO symbol discovery (Bybit via CCXT): "AUTO", "AUTO:USDT", "AUTO:USDT:50"
# - Valid symbol filtering + min-qty/min-notional/precision
# - Fallback signal logic (Donchian breakout) if strategy yields no signals
# - Robust try/except and dual logging (CSV + optional Postgres)
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

THIS_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(THIS_DIR)
for p in (ROOT_DIR, THIS_DIR):
    if p not in sys.path:
        sys.path.append(p)

from bot.strategies import DonchianTrendADXRSI
from bot.risk import RiskManager, TradeManager
from bot.utils import atr as calc_atr
from bot.connectors.ccxt_connector import CCXTConnector
from bot.db_writer import DB

try:
    from bot.connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None

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
    step = 1e-6
    prec = (market or {}).get('precision') or {}
    if 'amount' in prec and isinstance(prec['amount'], int):
        step = 10 ** (-prec['amount'])
    else:
        lim_amt = (market or {}).get('limits', {}).get('amount', {}) or {}
        step = float(lim_amt.get('step') or step)
    return max(step, 1e-12)

def attach_atr(ltf_df: pd.DataFrame) -> pd.Series:
    return calc_atr(ltf_df, 14)

def ensure_signal_columns(feats: pd.DataFrame, ltf_df: pd.DataFrame, donchian_len: int) -> pd.DataFrame:
    """
    Fallback: אם אין עמודות סיגנל, או שהכול False, נחשב סיגנל פריצה דונצ'יאן בסיסי.
    long_setup: close פורץ את max(high, N)
    short_setup: close יורד מתחת ל-min(low, N)
    """
    feats = feats.copy()
    need_fallback = False
    if 'long_setup' not in feats.columns or 'short_setup' not in feats.columns:
        need_fallback = True
    else:
        if (feats['long_setup'].sum() + feats['short_setup'].sum()) == 0:
            need_fallback = True

    if need_fallback:
        N = max(2, int(donchian_len or 4))
        highs = ltf_df['high'].rolling(N).max()
        lows  = ltf_df['low'].rolling(N).min()
        close = ltf_df['close']
        long_setup  = close > highs.shift(1)
        short_setup = close < lows.shift(1)

        # התאמת האינדקס: נאחד על פי ה־index של feats
        tmp = pd.DataFrame(index=feats.index)
        tmp['long_setup']  = long_setup.reindex(feats.index).fillna(False)
        tmp['short_setup'] = short_setup.reindex(feats.index).fillna(False)
        feats['long_setup']  = tmp['long_setup']
        feats['short_setup'] = tmp['short_setup']

    return feats

def prepare_features(ltf_df: pd.DataFrame, htf_df: pd.DataFrame, strat: DonchianTrendADXRSI, donchian_len:int) -> pd.DataFrame:
    f = strat.prepare(ltf_df, htf_df)
    f["atr"] = attach_atr(ltf_df)
    f = ensure_signal_columns(f, ltf_df, donchian_len)
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
    donchian_len_cfg = int(raw_s.get('donchian_len', 4))
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

    # 6) Connectors + AUTO
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

        # init exchange
        try:
            conn.init()
        except Exception as e:
            print(f"❌ init() failed for connector {c.get('name','?')}: {repr(e)}")
            continue

        # load markets (populates conn.exchange.symbols + markets dict)
        try:
            markets = conn.exchange.load_markets()
        except Exception as e:
            print(f"❌ load_markets() failed: {e}")
            markets = {}

        all_symbols = set(getattr(conn.exchange, "symbols", []) or [])
        requested_syms = list(c.get("symbols", []) or [])

        # --- AUTO syntax: "AUTO", "AUTO:USDT", "AUTO:USDT:50"
        auto_mode = any(isinstance(s, str) and s.upper().startswith("AUTO") for s in requested_syms)
        if auto_mode:
            raw_auto = next(s for s in requested_syms if isinstance(s, str) and s.upper().startswith("AUTO"))
            parts = raw_auto.split(":")
            quote = (parts[1].upper() if len(parts) >= 2 and parts[1] else "USDT")
            # parse limit
            limit_n = 30
            if len(parts) >= 3:
                try:
                    limit_n = int(parts[2])
                except Exception:
                    limit_n = 30

            # collect spot + quote symbols; prefer active or status Trading
            auto_syms = []
            for m, info in (markets or {}).items():
                try:
                    q = (info.get("quote") or info.get("quoteId") or "").upper()
                    status = (info.get("info", {}) or {}).get("status", "")
                    is_active = info.get("active", True)
                    typ = info.get("type")
                    is_spot = (typ == "spot") or (info.get("spot") is True)
                    if q == quote and is_spot and (is_active or status in ("Trading", "trading")):
                        auto_syms.append(m)
                except Exception:
                    continue

            # sort by 24h volume if available (fallback 0.0), then cut to limit_n
            def _vol_key(sym):
                inf = (markets.get(sym, {}) or {}).get("info", {}) if markets else {}
                for k in ("volume24h", "quoteVolume", "turnover24h", "24hTurnover"):
                    v = inf.get(k)
                    if v is not None:
                        try:
                            return float(v)
                        except Exception:
                            continue
                return 0.0

            auto_syms = sorted(auto_syms, key=_vol_key, reverse=True)[:max(1, int(limit_n))]
            cfg_syms_expanded = auto_syms
            requested_count = f"AUTO({quote},{limit_n})"
        else:
            # regular static list (ignore any stray "AUTO" strings)
            cfg_syms_expanded = [s for s in requested_syms if not (isinstance(s, str) and s.upper().startswith("AUTO"))]
            requested_count = f"{len(requested_syms)}"

        # final validity check vs exchange.symbols
        valid_syms = [s for s in cfg_syms_expanded if s in all_symbols]

        if not valid_syms:
            print(
                f"⚠️ No valid symbols for connector '{c.get('name','ccxt')}'. "
                f"Requested={requested_count}, Available={len(all_symbols)}"
            )
        else:
            print(
                f"✅ Connector '{c.get('name','ccxt')}' loaded {len(valid_syms)} valid symbols "
                f"(of {requested_count} requested)."
            )

        c_local = dict(c)
        c_local["symbols"] = valid_syms
        conns.append((c_local, conn))

    # 7) Init CSV trades header
    write_csv(TRADES_CSV, ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"], [])

    open_positions: dict = {}
    cooldowns: dict = {}
    last_bar_ts: dict = {}
    start_time = time.time()
    SECONDS_IN_WEEK = 7 * 24 * 60 * 60

    # 8) Main loop
    while True:
        now_utc = datetime.now(timezone.utc)
        rows_trades = []
        snapshots: dict = {}

        # Fetch & features
        for c_cfg, conn in conns:
            tf = c_cfg.get("timeframe", "1m")
            htf = c_cfg.get("htf_timeframe", "5m")
            for sym in c_cfg.get("symbols", []):
                try:
                    ltf_df = conn.fetch_ohlcv(sym, tf, limit=600)
                    htf_df = conn.fetch_ohlcv(sym, htf, limit=600)
                    feats = prepare_features(ltf_df, htf_df, strat, donchian_len_cfg)
                    last = feats.iloc[-1]
                    key = (c_cfg.get("name", "ccxt"), sym)
                    snapshots[key] = last
                except Exception as e:
                    print(f"⏭️ skip {sym}: {repr(e)}")
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
            write_csv(EQUITY_CSV, ["time", "equity"], [[now_utc.isoformat(), f"{equity:.2f}"]])
            if db:
                try:
                    db.write_equity({"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")})
                except Exception as e:
                    print(f"[WARN] DB write_equity loop failed: {e}")
            continue

        # Manage positions
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

            if atr_now:
                trail = tm.trail_level(side, price, atr_now, after_tp1=pos["tp1_done"])
                if side == "long":
                    pos["sl"] = max(pos["sl"], trail)
                else:
                    pos["sl"] = min(pos["sl"], trail)

            if not pos["moved_to_be"] and atr_now:
                if side == "long" and price >= entry + tm.be_after_R * R:
                    pos["sl"] = max(pos["sl"], entry)
                    pos["moved_to_be"] = True
                if side == "short" and price <= entry - tm.be_after_R * R:
                    pos["sl"] = min(pos["sl"], entry)
                    pos["moved_to_be"] = True

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

            if (side == "long" and price <= pos["sl"]) or (side == "short" and price >= pos["sl"]):
                price_exit = pos["sl"]
                pnl = (price_exit - entry) * pos["qty"] if side == "long" else (entry - price_exit) * pos["qty"]
                equity += pnl
                rows_trades.append(
                    [now_utc.isoformat(), key[0], key[1], "SL", side, f"{price_exit:.8f}", f"{pos['qty']:.8f}", f"{pnl:.2f}", f"{equity:.2f}"]
                )
                to_close.append(key)

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

                market = {}
                try:
                    market = conn.exchange.market(sym)
                except Exception:
                    pass

                step = determine_amount_step(market)
                lims  = (market or {}).get('limits', {}) or {}
                min_qty  = (lims.get('amount') or {}).get('min')
                min_cost = (lims.get('cost')   or {}).get('min')

                qty_risk = (equity * rm.risk_per_trade) / max(R, 1e-12)
                qty_cap  = (equity * rm.max_position_pct) / max(price, 1e-9)
                qty      = max(0.0, min(qty_risk, qty_cap))
                qty      = round_step(qty, step)

                if (min_qty is not None) and (qty < float(min_qty)):
                    qty = round_step(float(min_qty), step)

                notional = qty * price
                if (min_cost is not None) and (notional < float(min_cost)):
                    needed_qty = float(min_cost) / max(price, 1e-9)
                    qty = round_step(max(qty, needed_qty), step)

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

        if time.time() - start_time >= SECONDS_IN_WEEK:
            break

        time.sleep(30)

if __name__ == "__main__":
    main()
