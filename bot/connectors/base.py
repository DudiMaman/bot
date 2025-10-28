class BaseConnector:
    def init(self):
        raise NotImplementedError

    def fetch_ohlcv(self, symbol, timeframe='1h', limit=100):
        raise NotImplementedError
