from pathlib import Path
from typing import Dict, List
import pandas as pd
import numpy as np

from strategies import DonchianTrendADXRSI
from utils import ohlcv_csv_to_df, atr
from risk import RiskManager, TradeManager

def _resample_htf(df: pd.DataFrame, htf: str) -> pd.DataFrame:
    agg = {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}
    return df.resample(htf).agg(agg).dropna()

def _prepare(df_ltf: pd.DataFrame, htf: str, strat: DonchianTrendADXRSI) -> pd.DataFrame:
    htf_df = _resample_htf(df_ltf, htf)
    f = strat.prepare(df_ltf, htf_df); f['atr'] = atr(df_ltf, 14)
    return f

def run_portfolio_backtest(
    data_dir: str,
    symbols: List[str],
    ltf: str = '1H',
    htf: str = '4H',
    equity0: float = 100_000.0,
    risk_per_trade: float = 0.008,
    max_position_pct: float = 0.25,
    max_concurrent_positions: int = 6,
    daily_loss_cap_R: float = -4.0,
    cooldown_bars_after_loss: int = 2,
    strat_params: Dict = None,
    tm_params: Dict = None,
):
    strat_params = strat_params or {}
    tm_params = tm_params or {}
    feats = {}
    for sym in symbols:
        p = Path(data_dir) / f"{sym}.csv"
        df = ohlcv_csv_to_df(str(p)).asfreq(ltf).ffill().dropna()
        s = DonchianTrendADXRSI(**strat_params)
        feats[sym] = _prepare(df, htf, s)

    common = None
    for f in feats.values():
        common = f.index if common is None else common.intersection(f.index)
    for sym in symbols:
        feats[sym] = feats[sym].reindex(common).dropna()

    equity = equity0
    rm = RiskManager(equity, risk_per_trade, max_position_pct)
    tm = TradeManager(**tm_params)

    open_pos = {}
    daily_R = 0.0
    last_day = None
    cooldowns = {sym: 0 for sym in symbols}
    records = []

    for ts in common:
        if last_day is None or ts.date() != last_day:
            daily_R = 0.0; last_day = ts.date()

        to_close = []
        for sym, pos in open_pos.items():
            row = feats[sym].loc[ts]
            price = float(row['close'])
            atr_now = float(row['atr']) if pd.notna(row['atr']) else None
            side = pos['side']; entry = pos['entry']; qty = pos['qty']; R = pos['R']

            if atr_now:
                trail = tm.trail_level(side, price, atr_now, after_tp1=pos['tp1_done'])
                if side == 'long':  pos['sl'] = max(pos['sl'], trail)
                else:               pos['sl'] = min(pos['sl'], trail)

            if (not pos['moved_to_be']) and atr_now:
                if side=='long' and price >= entry + tm.be_after_R*R:
                    pos['sl'] = max(pos['sl'], entry); pos['moved_to_be'] = True
                if side=='short' and price <= entry - tm.be_after_R*R:
                    pos['sl'] = min(pos['sl'], entry); pos['moved_to_be'] = True

            if (not pos['tp1_done']) and ((side=='long' and price>=pos['tp1']) or (side=='short' and price<=pos['tp1'])):
                close_qty = qty*tm.p1_pct
                pnl = (price - entry)*close_qty if side=='long' else (entry - price)*close_qty
                equity += pnl; pos['qty'] = qty - close_qty; pos['tp1_done']=True; daily_R += tm.r1_R
                records.append({'time': ts, 'symbol': sym, 'type':'TP1', 'side':side, 'price':price, 'qty':close_qty, 'pnl':pnl, 'equity': equity})
                qty = pos['qty']

            if (not pos['tp2_done']) and ((side=='long' and price>=pos['tp2']) or (side=='short' and price<=pos['tp2'])):
                close_qty = qty*tm.p2_pct
                pnl = (price - entry)*close_qty if side=='long' else (entry - price)*close_qty
                equity += pnl; pos['qty'] = qty - close_qty; pos['tp2_done']=True; daily_R += tm.r2_R
                records.append({'time': ts, 'symbol': sym, 'type':'TP2', 'side':side, 'price':price, 'qty':close_qty, 'pnl':pnl, 'equity': equity})
                qty = pos['qty']

            pos['bars'] += 1
            if pos['bars'] >= tm.max_bars_in_trade and not pos['tp2_done']:
                price_exit = price
                pnl = (price_exit - entry)*pos['qty'] if side=='long' else (entry - price_exit)*pos['qty']
                equity += pnl
                records.append({'time': ts, 'symbol': sym, 'type':'TIME', 'side':side, 'price':price_exit, 'qty':pos['qty'], 'pnl':pnl, 'equity': equity})
                to_close.append(sym); continue

            if (side=='long' and price<=pos['sl']) or (side=='short' and price>=pos['sl']):
                price_exit = pos['sl']
                pnl = (price_exit - entry)*pos['qty'] if side=='long' else (entry - price_exit)*pos['qty']
                equity += pnl; daily_R -= 1.0
                records.append({'time': ts, 'symbol': sym, 'type':'SL', 'side':side, 'price':price_exit, 'qty':pos['qty'], 'pnl':pnl, 'equity': equity})
                to_close.append(sym)

        for sym in to_close: open_pos.pop(sym, None)

        if daily_R > daily_loss_cap_R:
            for sym in symbols:
                if sym in open_pos or cooldowns[sym] > 0:
                    cooldowns[sym] = max(0, cooldowns[sym]-1); continue
                if len(open_pos) >= max_concurrent_positions: break
                row = feats[sym].loc[ts]
                if pd.isna(row['atr']) or row['atr'] <= 0: continue
                sig = 1 if row['long_setup'] else (-1 if row['short_setup'] else 0)
                if sig == 0: continue
                price = float(row['close']); atr_now = float(row['atr'])
                side = 'long' if sig==1 else 'short'
                sl, tp1, tp2, R = tm.initial_levels(side, price, atr_now)
                if R <= 0: continue
                qty = min((equity*risk_per_trade)/R, (equity*max_position_pct)/price)
                qty = float(int(qty*1e6)/1e6)
                if qty <= 0: continue
                open_pos[sym] = {'side': side, 'entry': price, 'sl': sl, 'tp1': tp1, 'tp2': tp2,
                                 'qty': qty, 'R': R, 'tp1_done': False, 'tp2_done': False,
                                 'moved_to_be': False, 'bars': 0}
                records.append({'time': ts, 'symbol': sym, 'type':'ENTER', 'side':side, 'price':price, 'qty':qty, 'equity': equity})

    tr = pd.DataFrame(records)
    eq = tr.set_index('time')['equity'].dropna()
    if eq.empty: eq = pd.Series([equity0], index=[common[0]])
    eq = eq.reindex(common, method='ffill').fillna(equity0)
    total_return = float(eq.iloc[-1]/equity0 - 1)
    max_dd = float(((eq.cummax() - eq)/eq.cummax()).max())
    rets = eq.pct_change().dropna()
    sharpe = float((rets.mean()/(rets.std()+1e-12))*np.sqrt(252*24)) if not rets.empty else 0.0

    return {'equity_final': float(eq.iloc[-1]),
            'total_return_pct': total_return*100,
            'max_drawdown_pct': max_dd*100,
            'sharpe': sharpe,
            'trades': tr,
            'equity_curve': eq.reset_index().rename(columns={'index':'time','equity':'equity'})}
