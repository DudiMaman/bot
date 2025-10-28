import pandas as pd

def ohlcv_csv_to_df(path: str):
    df = pd.read_csv(path)
    if 'ts' not in df.columns:
        raise ValueError("CSV must have a 'ts' column")
    try:
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    except Exception:
        df['ts'] = pd.to_datetime(df['ts'])
    df = df[['ts','open','high','low','close','volume']]
    df.set_index('ts', inplace=True)
    df = df.sort_index()
    return df

def atr(df: pd.DataFrame, length=14):
    high, low, close = df['high'], df['low'], df['close']
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = tr1.combine(tr2, max).combine(tr3, max)
    return tr.rolling(length).mean()
