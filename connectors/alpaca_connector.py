import os
import pandas as pd
from alpaca_trade_api import REST
from typing import Dict, Any
from .base import BaseConnector

class AlpacaConnector(BaseConnector):
    def __init__(self, paper: bool = True):
        base_url = os.getenv('APCA_API_BASE_URL', 'https://paper-api.alpaca.markets')
        self.api = REST(os.getenv('APCA_API_KEY_ID'), os.getenv('APCA_API_SECRET_KEY'), base_url=base_url)

    def init(self): pass

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 600) -> pd.DataFrame:
        bars = self.api.get_bars(symbol, timeframe, limit=limit).df
        df = bars.reset_index().rename(columns={'timestamp':'ts'})
        df = df[['ts','open','high','low','close','volume']]
        df['ts'] = pd.to_datetime(df['ts'])
        df.set_index('ts', inplace=True)
        return df

    def create_market_order(self, symbol: str, side: str, qty: float) -> Dict[str, Any]:
        order = self.api.submit_order(symbol=symbol, qty=qty, side=side, type='market', time_in_force='day')
        return getattr(order, '_raw', {'id': str(order.id)})

    def get_precision(self, symbol: str) -> Dict[str, Any]:
        return {'amount_min': 1.0, 'price_tick': 0.01, 'amount_step': 1.0}

    def account_equity(self) -> float:
        return float(self.api.get_account().equity)
