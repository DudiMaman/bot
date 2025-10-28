import os, base64, requests

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = os.getenv("GITHUB_REPO")  # דוגמה: "DudiMaman/trading-bot-week"
RENDER_SERVICE_ID = os.getenv("RENDER_SERVICE_ID")  # מזהה שירות הבוט ב-Render
RENDER_API_KEY = os.getenv("RENDER_API_KEY")        # Render API Key

def gh_req(path, method="GET", **kw):
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    return requests.request(method, f"https://api.github.com{path}", headers=headers, **kw)

def render_deploy():
    # מפעיל Redeploy לבוט
    requests.post(
        f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/deploys",
        headers={"Authorization": f"Bearer {RENDER_API_KEY}", "Content-Type": "application/json"},
        json={"clearCache": True}, timeout=15
    )

def get_file_text(path, ref="main"):
    r = gh_req(f"/repos/{REPO}/contents/{path}?ref={ref}")
    if r.status_code != 200:
        return None, None
    data = r.json()
    content_b64 = data.get("content", "")
    sha = data.get("sha")
    txt = base64.b64decode(content_b64).decode("utf-8")
    return txt, sha

def put_file_text(path, new_text, message, branch="main", sha=None):
    data = {
        "message": message,
        "content": base64.b64encode(new_text.encode("utf-8")).decode("utf-8"),
        "branch": branch
    }
    if sha:
        data["sha"] = sha
    return gh_req(f"/repos/{REPO}/contents/{path}", "PUT", json=data)
