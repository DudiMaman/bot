import pandas as pd
import numpy as np

def rsi(series: pd.Series, length: int = 14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/length, adjust=False).mean()
    ma_down = down.ewm(alpha=1/length, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-12)
    return 100 - (100 / (1 + rs))

def adx(df: pd.DataFrame, length: int = 14):
    high, low, close = df['high'], df['low'], df['close']
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = ((up_move > down_move) & (up_move > 0)) * up_move
    minus_dm = ((down_move > up_move) & (down_move > 0)) * down_move
    tr1 = (high - low).abs()
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = tr1.combine(tr2, max).combine(tr3, max).rolling(length).sum()
    plus_di = 100 * (plus_dm.rolling(length).sum() / (tr + 1e-12))
    minus_di = 100 * (minus_dm.rolling(length).sum() / (tr + 1e-12))
    dx = ((plus_di - minus_di).abs() / ((plus_di + minus_di) + 1e-12)) * 100
    return dx.rolling(length).mean()

class DonchianTrendADXRSI:
    """
    כניסה:
      לונג:  close > DonchianHi  &&  LTF מעל EMA200-HTF  &&  ADX>=th  && RSI<=rsi_max
      שורט:  close < DonchianLo  &&  LTF מתחת EMA200-HTF &&  ADX>=th  && RSI>=100-rsi_min
    """
    def __init__(self, donchian_len=20, rsi_len=14, rsi_long_max=70, rsi_short_min=30, adx_len=14, adx_min=18):
        self.dlen = donchian_len
        self.rsi_len = rsi_len
        self.rsi_long_max = rsi_long_max
        self.rsi_short_min = rsi_short_min
        self.adx_len = adx_len
        self.adx_min = adx_min

    def prepare(self, df_ltf: pd.DataFrame, df_htf: pd.DataFrame):
        df = df_ltf.copy()
        # Donchian
        df['donch_hi'] = df['high'].rolling(self.dlen).max()
        df['donch_lo'] = df['low'].rolling(self.dlen).min()
        # RSI/ADX
        df['rsi'] = rsi(df['close'], self.rsi_len)
        df['adx'] = adx(df, self.adx_len)
        # HTF trend proxy (EMA200)
        htf_ema200 = df_htf['close'].ewm(span=200, adjust=False).mean()
        htf_ema200 = htf_ema200.reindex(df.index, method='ffill')
        df['trend_up'] = (df['close'] > htf_ema200).astype(int)
        df['trend_down'] = (df['close'] < htf_ema200).astype(int)
        # Raw setups
        df['long_setup']  = (df['close'] > df['donch_hi']) & (df['trend_up'] == 1) & (df['adx'] >= self.adx_min) & (df['rsi'] <= self.rsi_long_max)
        df['short_setup'] = (df['close'] < df['donch_lo']) & (df['trend_down'] == 1) & (df['adx'] >= self.adx_min) & (df['rsi'] >= (100 - self.rsi_short_min))
        return df

    def signal(self, row: pd.Series):
        if bool(row.get('long_setup', False)):  return 1
        if bool(row.get('short_setup', False)): return -1
        return 0
