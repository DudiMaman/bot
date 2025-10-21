import os, time
import ccxt
import pandas as pd
from typing import Dict, Any
from .base import BaseConnector

class CCXTConnector(BaseConnector):
    def __init__(self, exchange_id: str, paper: bool = True, default_type: str = "spot"):
        ex_cls = getattr(ccxt, exchange_id)
        self.exchange = ex_cls({
            'apiKey': os.getenv('API_KEY_CTXT'),
            'secret': os.getenv('API_SECRET_CTXT'),
            'enableRateLimit': True,
            'options': {'defaultType': default_type}
        })
        self.paper = paper

    def init(self):
        if self.paper and hasattr(self.exchange, 'set_sandbox_mode'):
            self.exchange.set_sandbox_mode(True)
        self.exchange.load_markets()

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 600) -> pd.DataFrame:
        raw = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        df.set_index('ts', inplace=True)
        return df

    def create_market_order(self, symbol: str, side: str, qty: float) -> Dict[str, Any]:
        for _ in range(3):
            try:
                return self.exchange.create_order(symbol, 'market', side, qty)
            except ccxt.RateLimitExceeded:
                time.sleep(self.exchange.rateLimit/1000 + 0.2)
            except ccxt.NetworkError:
                time.sleep(1)
        raise

    def get_precision(self, symbol: str) -> Dict[str, Any]:
        m = self.exchange.market(symbol)
        return {
            'amount_min': m.get('limits', {}).get('amount', {}).get('min'),
            'price_tick': m.get('precision', {}).get('price'),
            'amount_step': m.get('precision', {}).get('amount')
        }

    def account_equity(self) -> float:
        return 0.0
