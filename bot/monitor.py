mkdir -p bot
cat > bot/monitor.py <<'PY'
import os, json, time, threading, urllib.request

def _post(url: str, api_key: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if api_key:
        req.add_header("X-API-Key", api_key)
    with urllib.request.urlopen(req, timeout=5) as resp:  # nosec - trusted URL from env
        resp.read()

def _loop(url: str, api_key: str, interval_sec: int):
    while True:
        try:
            _post(
                url,
                api_key,
                {"event":"heartbeat","ts":int(time.time()),"service":"trading-bot-worker"}
            )
        except Exception:
            # לא מפילים את ה-worker אם המוניטור נפל/לא זמין
            pass
        time.sleep(interval_sec)

def start_heartbeat(interval_sec: int = 60):
    url = os.getenv("MONITOR_URL", "").strip()
    if not url:
        return None  # לא הופעל
    api_key = os.getenv("MONITOR_API_KEY", "").strip()
    th = threading.Thread(target=_loop, args=(url, api_key, interval_sec), daemon=True)
    th.start()
    return th
PY
