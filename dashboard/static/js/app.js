// dashboard/static/js/app.js

const IL_TZ = 'Asia/Jerusalem';

// ממיר ISO לכלי תצוגה בשעון ישראל
function toILString(iso) {
  if (!iso) return '—';
  // חלק מהשדות מגיעים כבר עם +02:00 / Z. Date יפרש UTC תקין.
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso; // אם הגיע טקסט לא סטנדרטי, מציגים אותו כמו שהוא
  const fmt = new Intl.DateTimeFormat('he-IL', {
    timeZone: IL_TZ,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  });
  // ממיר לפורמט קומפקטי YYYY-MM-DD HH:mm:ss
  const parts = fmt.formatToParts(d).reduce((acc, p) => (acc[p.type] = p.value, acc), {});
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
}

// עיצוב תגית סטטוס
function paintStatus(el, status) {
  el.textContent = status || '—';
  el.classList.remove('badge-ok','badge-stop');
  if (status === 'RUNNING') el.classList.add('badge-ok');
  else el.classList.add('badge-stop');
}

// מילוי טבלת הטריידים
function renderTrades(rows) {
  const tbody = document.querySelector('#trades-table tbody');
  const empty = document.getElementById('empty-trades');
  tbody.innerHTML = '';

  if (!rows || rows.length === 0) {
    empty.style.display = 'block';
    return;
  }
  empty.style.display = 'none';

  rows.forEach(r => {
    const tr = document.createElement('tr');

    const tds = [
      toILString(r.time || r.timestamp || r.ts || r.datetime),   // Time (IL)
      r.symbol || '—',
      r.side || '—',
      r.type || '—',
      r.price ?? '—',
      r.qty ?? '—',
      r.pnl ?? '—',
      r.equity ?? '—'
    ];

    // צביעה מינימלית ל-Side/PnL לשיפור קריאות
    tds.forEach((val, idx) => {
      const td = document.createElement('td');
      td.textContent = val;

      if (idx === 2) { // Side
        td.classList.add(val === 'BUY' ? 'pos' : val === 'SELL' ? 'neg' : '');
      }
      if (idx === 6) { // PnL
        const num = Number(val);
        if (!Number.isNaN(num)) td.classList.add(num >= 0 ? 'pos' : 'neg');
      }

      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });
}

// משיכת נתונים והצגה עם המרה ל-IL
async function loadData() {
  const statusEl = document.getElementById('status-badge');
  const overrideEl = document.getElementById('override-badge');
  const lrIL = document.getElementById('last-refresh-il');
  const lrUTC = document.getElementById('last-refresh-utc');
  const logDir = document.getElementById('log-dir');
  const dl = document.getElementById('download-csv');

  // שימור פרמטרי טווח אם יהיו בהמשך (כרגע פשוט)
  const qs = location.search || '';
  const res = await fetch('/data' + qs);
  const data = await res.json();

  // סטטוס
  paintStatus(statusEl, data.status);
  overrideEl.style.display = data.manual_override ? 'inline-block' : 'none';

  // רענון: מציגים גם IL וגם UTC
  lrIL.textContent  = toILString(data.now_utc);
  lrUTC.textContent = (new Date(data.now_utc)).toISOString().replace('T',' ').replace('Z','');

  // מיקום לוגים (מידע עזר מתוך /health אם תרצה – כאן מציגים מהידוע)
  // כדי להביא log_dir, נבצע שאילתת בריאות קצרה (לא חובה כל רענון)
  fetch('/health').then(r => r.json()).then(h => {
    logDir.textContent = h.log_dir || '—';
  }).catch(()=>{});

  // כפתור הורדה ישתמש באותו query string של התצוגה
  dl.href = '/export/trades.csv' + qs;

  // טבלה – מציגים זמן ב-IL
  renderTrades(data.trades || []);
}

// רענון אוטומטי עדין
function startAutoRefresh() {
  loadData().catch(console.error);
  setInterval(() => loadData().catch(console.error), 15000);
}

document.addEventListener('DOMContentLoaded', startAutoRefresh);
