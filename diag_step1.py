# diag_step1.py
# ------------------------------------------------------------
# Diagnostic snapshot:
# - Reads bot/config.yml and prints key fields (default_type, symbols)
# - Inits CCXTConnector and loads markets
# - Reports counts for spot vs swap markets (USDT quote), samples a few
# - Checks whether the symbols from config are actually available
# ------------------------------------------------------------

import os, sys, yaml
from pprint import pprint

THIS_DIR = os.path.dirname(__file__)
sys.path.append(THIS_DIR)

# Ensure bot package import path
BOT_DIR = os.path.join(THIS_DIR, "bot")
if BOT_DIR not in sys.path:
    sys.path.append(BOT_DIR)
ROOT_DIR = THIS_DIR
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from bot.connectors.ccxt_connector import CCXTConnector

CFG_PATH = os.path.join(BOT_DIR, "config.yml")

def load_cfg(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def summarize_markets(conn):
    try:
        markets = conn.exchange.load_markets()
    except Exception as e:
        print(f"[X] load_markets failed: {e!r}")
        return {}, [], []
    all_symbols = list(conn.exchange.symbols or [])
    spot_usdt = []
    swap_usdt = []
    # CCXT marks: type/spot/swap/contract + quote
    for m, info in markets.items():
        try:
            quote = (info.get("quote") or info.get("quoteId") or "").upper()
            typ = info.get("type")
            is_spot = (typ == "spot") or (info.get("spot") is True)
            is_swap = (typ == "swap") or (info.get("swap") is True) or bool(info.get("contract"))
            if quote == "USDT":
                if is_spot:
                    spot_usdt.append(m)
                if is_swap:
                    swap_usdt.append(m)
        except Exception:
            pass
    return markets, spot_usdt, swap_usdt

def main():
    print("=== STEP1: CONFIG SNAPSHOT ===")
    cfg = load_cfg(CFG_PATH)
    lc = (cfg.get("live_connectors") or [])
    ccxts = [c for c in lc if c.get("type") == "ccxt"]
    if not ccxts:
        print("[X] No CCXT live_connectors found in config.yml")
        return
    c = ccxts[0]
    ex_id = c.get("exchange_id", "bybit")
    default_type = c.get("default_type", "spot")
    symbols = c.get("symbols") or []
    timeframe = c.get("timeframe", "1m")
    htf_timeframe = c.get("htf_timeframe", "5m")

    print("exchange_id:", ex_id)
    print("default_type:", default_type)
    print("symbols (raw):", symbols)
    print("timeframes:", {"ltf": timeframe, "htf": htf_timeframe})

    # Init connector with these exact params
    print("\n=== STEP1: INIT CONNECTOR ===")
    paper = c.get("paper", True)
    conn = CCXTConnector(ex_id, paper=paper, default_type=default_type)
    try:
        conn.init()
        print("[OK] CCXTConnector.init()")
    except Exception as e:
        print(f"[X] CCXTConnector.init() failed: {e!r}")
        return

    print("\n=== STEP1: MARKETS SUMMARY ===")
    markets, spot_usdt, swap_usdt = summarize_markets(conn)
    print("total markets:", len(markets))
    print("spot USDT symbols:", len(spot_usdt))
    print("swap USDT symbols:", len(swap_usdt))
    print("sample spot:", spot_usdt[:10])
    print("sample swap:", swap_usdt[:10])

    # Flatten AUTO if present: formats we support:
    # - "AUTO" (all USDT of the selected default_type, capped internally by bot)
    # - "AUTO:USDT:30" (explicit: USDT quote, limit 30)
    print("\n=== STEP1: SYMBOLS AVAILABILITY CHECK ===")
    auto_specs = []
    explicit = []
    for s in symbols:
        if isinstance(s, str) and s.startswith("AUTO"):
            auto_specs.append(s)
        else:
            explicit.append(s)

    if auto_specs:
        print("AUTO entries found:", auto_specs)
        # We don't expand them here—bot does it—but we at least show counts above
    else:
        print("No AUTO entries in symbols.")

    # Check explicit symbols availability:
    if explicit:
        ex_syms = set(conn.exchange.symbols or [])
        missing = [s for s in explicit if s not in ex_syms]
        present = [s for s in explicit if s in ex_syms]
        print("explicit present:", present)
        print("explicit missing:", missing)
    else:
        print("No explicit symbols to check.")

    print("\n=== STEP1: DONE ===")

if __name__ == "__main__":
    main()
