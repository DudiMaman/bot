import math

class RiskManager:
    def __init__(self, equity, risk_per_trade=0.01, max_position_pct=0.25):
        self.equity = float(equity)
        self.risk_per_trade = float(risk_per_trade)
        self.max_position_pct = float(max_position_pct)

    def position_size(self, entry_price, sl_price):
        risk_amt = self.equity * self.risk_per_trade
        risk_per_unit = abs(entry_price - sl_price)
        if risk_per_unit <= 0: return 0.0
        qty_by_risk = risk_amt / risk_per_unit
        max_notional = self.equity * self.max_position_pct
        qty_by_cap = max_notional / entry_price
        qty = min(qty_by_risk, qty_by_cap)
        return math.floor(qty * 1e6) / 1e6

class TradeManager:
    """
    ניהול פוזיציה: SL=ATR*k, Partial TP (1.5R, 3R), Break-Even אחרי 1R,
    Trailing שמתהדק אחרי TP1, Time-Stop, (אופציונלי) Pyramiding.
    """
    def __init__(self,
                 atr_k_sl=1.8,
                 r1_R=1.5, p1_pct=0.30,
                 r2_R=3.0, p2_pct=0.30,
                 be_after_R=1.0,
                 trail_k_before=1.8,
                 trail_k_after=1.2,
                 max_bars_in_trade=120,
                 pyramiding=False, add_unit_R=1.25, max_units=3):
        self.atr_k_sl = atr_k_sl
        self.r1_R = r1_R; self.p1_pct = p1_pct
        self.r2_R = r2_R; self.p2_pct = p2_pct
        self.be_after_R = be_after_R
        self.trail_k_before = trail_k_before
        self.trail_k_after  = trail_k_after
        self.max_bars_in_trade = max_bars_in_trade
        self.pyramiding = pyramiding
        self.add_unit_R = add_unit_R
        self.max_units = max_units

    def initial_levels(self, side, entry, atr):
        if side == 'long':
            sl = entry - self.atr_k_sl * atr
            R  = entry - sl
            tp1 = entry + self.r1_R * R
            tp2 = entry + self.r2_R * R
        else:
            sl = entry + self.atr_k_sl * atr
            R  = sl - entry
            tp1 = entry - self.r1_R * R
            tp2 = entry - self.r2_R * R
        return sl, tp1, tp2, R

    def trail_level(self, side, price, atr, after_tp1=False):
        k = self.trail_k_after if after_tp1 else self.trail_k_before
        if side == 'long':  return price - k * atr
        else:               return price + k * atr
