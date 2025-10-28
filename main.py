# ==========================================================
# main.py — מפעיל הבוט (bot/run_live_week.py)
# ==========================================================

import sys
from pathlib import Path

# ----------------------------------------------------------
# 1. ודא שהשורש של הפרויקט נוסף לנתיב ה-Python
# ----------------------------------------------------------
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ----------------------------------------------------------
# 2. נסה לייבא ולהריץ את הבוט
# ----------------------------------------------------------
try:
    from bot.run_live_week import main as run_bot_main
except Exception as e:
    print("🚨 שגיאה בייבוא bot.run_live_week:")
    print(repr(e))
    raise

if __name__ == "__main__":
    print("🚀 הפעלת הבוט מתבצעת כעת (bot/run_live_week.py)...")
    try:
        run_bot_main()
    except Exception as e:
        print("⚠️ שגיאה בזמן ריצה:")
        print(repr(e))
        raise
