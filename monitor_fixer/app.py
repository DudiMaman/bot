import os, re, json
from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse
from models import SessionLocal, Log, Fix, Status, Base, engine
from gitops import get_file_text, put_file_text, render_deploy
from rules import fix_bad_symbol, fix_config_key_alias

AUTH = os.getenv("MONITOR_API_KEY")    # סיסמה בין הבוט למוניטור
CONFIG_PATH = os.getenv("CONFIG_PATH", "bot/config.yml")  # מיקום הקונפיג בריפו
AUTO_COMMIT_TO_MAIN = os.getenv("AUTO_COMMIT_TO_MAIN", "true").lower() == "true"

app = FastAPI()

# יצירת טבלאות אוטומטית בהפעלה
Base.metadata.create_all(engine)

@app.get("/status")
def status():
    with SessionLocal() as s:
        st = s.query(Status).order_by(Status.id.desc()).first()
        return {"status": (st.status if st else "running")}

@app.post("/control")
def control(body: dict):
    new_status = body.get("status")
    if new_status not in ["running","paused","stopped"]:
        return JSONResponse({"error":"bad status"}, status_code=400)
    with SessionLocal() as s:
        s.query(Status).delete()
        s.add(Status(status=new_status))
        s.commit()
    return {"ok": True, "status": new_status}

@app.post("/ingest")
async def ingest(req: Request, x_monitor_key: str | None = Header(None)):
    if AUTH and x_monitor_key != AUTH:
        return JSONResponse({"error": "unauthorized"}, status_code=403)

    data = await req.json()
    level = data.get("level") or "INFO"
    source = data.get("source") or "bot"
    event_type = data.get("event_type") or "SYSTEM"
    payload = data.get("payload") or {}

    # שמירה ל-DB
    with SessionLocal() as s:
        s.add(Log(level=level, source=source, event_type=event_type, payload=payload))
        s.commit()

    # נסה לתקן אוטומטית
    try:
        auto_fix(event_type, payload)
    except Exception as e:
        # לא מפיל את השירות
        with SessionLocal() as s:
            s.add(Log(level="ERROR", source="monitor", event_type="ERROR",
                      payload={"msg":"auto_fix_failed","detail": str(e)}))
            s.commit()

    return {"ok": True}

# ——— לוגיקת תיקון ———

BAD_SYMBOL_RE = re.compile(r"does not have market symbol ([A-Z0-9\-\/]+)")

def auto_fix(event_type: str, payload: dict):
    # 1) BadSymbol: הסרת סימבול לא נתמך מה-config.yml
    if event_type == "ERROR" and payload.get("trace"):
        m = BAD_SYMBOL_RE.search(payload["trace"])
        if m:
            sym = m.group(1)
            apply_bad_symbol_fix([sym])
            return

    # 2) ניתן להרחיב כאן: IndentationError, KeyError בקונפיג, ועוד.

def apply_bad_symbol_fix(symbols: list[str]):
    txt, sha = get_file_text(CONFIG_PATH, ref="main")
    if txt is None:
        return

    new_txt = fix_bad_symbol(txt, symbols)
    new_txt = fix_config_key_alias(new_txt)  # ננקה גם אליאסים בקונפיג
    if new_txt == txt:
        return  # אין שינוי

    # דוחף ישירות ל-main (כי ביקשת FULL AUTO)
    r = put_file_text(CONFIG_PATH, new_txt, f"agent: remove unsupported symbols {symbols}", branch="main", sha=sha)
    # לוג לתיקונים
    with SessionLocal() as s:
        s.add(Fix(issue_type="BadSymbol", summary=f"Removed {symbols}", repo_ref="committed to main", diff="config.yml updated"))
        s.commit()

    # Redeploy לבוט
    render_deploy()
