import pandas as pd

eq = pd.read_csv("logs/equity_curve.csv")
eq['time'] = pd.to_datetime(eq['time'])
eq = eq.sort_values('time')
eq['equity'] = eq['equity'].astype(float)

init_equity = float(eq['equity'].iloc[0])

daily   = eq.set_index('time')['equity'].resample('1D').last().ffill().pct_change().fillna(0.0)
weekly  = eq.set_index('time')['equity'].resample('W-MON').last().ffill().pct_change().fillna(0.0)
monthly = eq.set_index('time')['equity'].resample('M').last().ffill().pct_change().fillna(0.0)

pd.DataFrame({'PnL_$': (daily*init_equity)}).to_csv("logs/pnl_daily.csv")
pd.DataFrame({'PnL_$': (weekly*init_equity)}).to_csv("logs/pnl_weekly.csv")
pd.DataFrame({'PnL_$': (monthly*init_equity)}).to_csv("logs/pnl_monthly.csv")

print("Saved: logs/pnl_daily.csv, logs/pnl_weekly.csv, logs/pnl_monthly.csv")
