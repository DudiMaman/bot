let chart;
const statusChip = document.getElementById("statusChip");
const lastUpdated = document.getElementById("lastUpdated");
const tradesBody = document.getElementById("tradesBody");
const btnResume = document.getElementById("btnResume");
const btnPause  = document.getElementById("btnPause");
const btnTheme  = document.getElementById("btnTheme");

function setStatusChip(status) {
  if (!status) { statusChip.textContent = "—"; return; }
  const s = String(status).toUpperCase();
  statusChip.textContent = s;
  statusChip.classList.remove("chip-running","chip-stopped");
  if (s === "RUNNING") statusChip.classList.add("chip-running");
  else statusChip.classList.add("chip-stopped");
}

function fmtNum(x, digits=2) {
  if (x === undefined || x === null || x === "") return "";
  const f = Number(x);
  if (Number.isNaN(f)) return String(x);
  return f.toLocaleString(undefined,{maximumFractionDigits:digits});
}

function tzNowIso() {
  return new Date().toLocaleString([], {
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit"
  });
}

async function fetchData() {
  const res = await fetch("/data", { cache: "no-cache" });
  return await res.json();
}

function buildChart(points) {
  const ctx = document.getElementById("equityChart");
  const labels = points.map(p => p.t);
  const data = points.map(p => p.y);
  if (chart) chart.destroy();
  chart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "Equity (USD)",
        data,
        pointRadius: 0,
        borderWidth: 2,
        tension: 0.25
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { display: false },
        y: { ticks: { callback: (v) => fmtNum(v,0) } }
      }
    }
  });
}

function fillTrades(rows) {
  tradesBody.innerHTML = "";
  if (!rows || !rows.length) {
    tradesBody.innerHTML = `<tr><td colspan="9" class="text-center text-muted">No trades yet.</td></tr>`;
    return;
  }
  const frag = document.createDocumentFragment();
  rows.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.time || ""}</td>
      <td>${r.connector || ""}</td>
      <td>${r.symbol || ""}</td>
      <td>${r.type || ""}</td>
      <td>${r.side || ""}</td>
      <td class="text-end">${fmtNum(r.price, 6)}</td>
      <td class="text-end">${fmtNum(r.qty, 6)}</td>
      <td class="text-end">${fmtNum(r.pnl, 2)}</td>
      <td class="text-end">${fmtNum(r.equity, 2)}</td>
    `;
    frag.appendChild(tr);
  });
  tradesBody.appendChild(frag);
}

async function refresh() {
  try {
    const d = await fetchData();
    setStatusChip(d.status);
    lastUpdated.textContent = `Last updated: ${tzNowIso()}`;
    buildChart(d.equity || []);
    fillTrades(d.trades || []);
  } catch (e) {
    console.error(e);
    lastUpdated.textContent = "Last updated: error";
  }
}

btnResume?.addEventListener("click", async () => {
  await fetch("/resume", { method: "POST" });
  await refresh();
});

btnPause?.addEventListener("click", async () => {
  await fetch("/pause", { method: "POST" });
  await refresh();
});

btnTheme?.addEventListener("click", () => {
  const html = document.documentElement;
  const cur = html.getAttribute("data-bs-theme") || "light";
  html.setAttribute("data-bs-theme", cur === "light" ? "dark" : "light");
});

refresh();
setInterval(refresh, 10000);
