// מציגים זמן ישראל (Asia/Jerusalem) בתצוגה בלבד, בלי לגעת בבקאנד/CSV
const IL_TZ = 'Asia/Jerusalem';

function toILString(isoLike) {
  if (!isoLike) return '—';
  const d = new Date(isoLike);
  if (isNaN(d.getTime())) return String(isoLike);
  const fmt = new Intl.DateTimeFormat('he-IL', {
    timeZone: IL_TZ,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  });
  const p = fmt.formatToParts(d).reduce((a, x) => (a[x.type] = x.value, a), {});
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

function paintStatus(el, status) {
  el.textContent = status || '—';
  el.classList.remove('badge-ok', 'badge-stop');
  el.classList.add(status === 'RUNNING' ? 'badge-ok' : 'badge-stop');
}

function getRowTime(row) {
  return row.time || row.timestamp || row.ts || row.datetime || row.date || null;
}

function renderTrades(rows) {
  const tbody = document.querySelector('#trades-table tbody');
  const empty = document.getElementById('empty-trades');
  tbody.innerHTML = '';
  if (!Array.isArray(rows) || rows.length === 0) { empty.style.display = 'block'; return; }
  empty.style.display = 'none';

  rows.forEach(r => {
    const tr = document.createElement('tr');
    const cells = [
      toILString(getRowTime(r)),
      r.symbol ?? '—',
      r.side ?? '—',
      r.type ?? '—',
      r.price ?? '—',
      r.qty ?? '—',
      r.pnl ?? '—',
      r.equity ?? '—'
    ];
    cells.forEach((val, idx) => {
      const td = document.createElement('td');
      td.textContent = val;
      if (idx === 2) {
        const s = String(val).toUpperCase();
        if (s === 'BUY' || s === 'LONG') td.classList.add('pos');
        if (s === 'SELL' || s === 'SHORT') td.classList.add('neg');
      }
      if (idx === 6) {
        const n = Number(val);
        if (!Number.isNaN(n)) td.classList.add(n >= 0 ? 'pos' : 'neg');
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

async function loadData() {
  const statusEl   = document.getElementById('status-badge');
  const overrideEl = document.getElementById('override-badge');
  const lrIL       = document.getElementById('last-refresh-il');
  const lrUTC      = document.getElementById('last-refresh-utc');
  const logDirEl   = document.getElementById('log-dir');
  const dl         = document.getElementById('download-csv');

  const qs = location.search || '';
  const res = await fetch('/data' + qs, { cache: 'no-store' });
  const data = await res.json();

  paintStatus(statusEl, data.status);
  overrideEl.style.display = data.manual_override ? 'inline-block' : 'none';

  const serverNow = data.now_utc || data.now || data.server_time || new Date().toISOString();
  lrIL.textContent  = toILString(serverNow);
  try {
    const d = new Date(serverNow);
    lrUTC.textContent = isNaN(d.getTime()) ? String(serverNow) : d.toISOString().replace('T', ' ').replace('Z', '');
  } catch { lrUTC.textContent = String(serverNow); }

  fetch('/health', { cache: 'no-store' })
    .then(r => r.json()).then(h => { logDirEl.textContent = h.log_dir || '—'; })
    .catch(() => {});

  dl.href = '/export/trades.csv' + qs;
  renderTrades(data.trades || []);
}

function startAutoRefresh() {
  loadData().catch(console.error);
  setInterval(() => loadData().catch(console.error), 15000);
}
document.addEventListener('DOMContentLoaded', startAutoRefresh);
