/* ============================================================
   RAPID Dashboard — batch.js
   Polls the Batch/HBase API endpoints separately from streaming.
   Base URL: http://100.73.216.115:5000
   Batch endpoints prefix: /batch/
   ============================================================ */

const API_BASE = 'http://100.73.216.115:5000';
const POLL_MS  = 30000; // Batch data changes slower, poll every 30s

const CHART_COLOR = {
  accent:  '#7b61ff',  // purple – batch primary
  accent2: '#ff2d6b',
  accent3: '#00e5ff',
  green:   '#00ff9d',
  yellow:  '#ffd600',
  orange:  '#ff7b2d',
};

// ── State ────────────────────────────────────────────────────
let isPolling      = true;
let pollInterval   = null;
let currentData    = {};
let activeTab      = 'overview';

// ── Chart instances ──────────────────────────────────────────
let chartTimeline   = null;
let chartVolume     = null;
let chartPatterns   = null;
let chartReputation = null;
let chartPortTop    = null;

// ── Chart.js defaults ────────────────────────────────────────
Chart.defaults.color           = '#4a5a80';
Chart.defaults.borderColor     = '#1a2240';
Chart.defaults.font.family     = "'Share Tech Mono', monospace";
Chart.defaults.animation.duration = 600;

// ═══════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════

function startClock() {
  const el = document.getElementById('clock');
  setInterval(() => { el.textContent = new Date().toLocaleTimeString('en-GB'); }, 1000);
}

function setStatus(online) {
  const dot   = document.getElementById('dot-api');
  const label = document.getElementById('status-api');
  dot.className     = 'dot ' + (online ? 'online' : 'offline');
  label.textContent = online ? 'HBase Online' : 'API Unreachable';
}

function showError(msg) {
  const banner = document.getElementById('error-banner');
  document.getElementById('error-msg').textContent = msg;
  banner.hidden = false;
}
function hideError() { document.getElementById('error-banner').hidden = true; }

function fmtTime(ts) {
  if (!ts) return '—';
  try {
    return new Date(ts).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  } catch { return ts; }
}

function fmtDate(ts) {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    return d.toLocaleDateString('en-GB', { month: 'short', day: '2-digit' }) + ' ' +
           d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  } catch { return ts; }
}

async function fetchBatch(path, timeoutMs = 7000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(`${API_BASE}${path}`, { signal: controller.signal });
    clearTimeout(timer);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}

function severity(score) {
  if (score >= 80) return { label: 'CRITICAL', cls: 'badge-critical' };
  if (score >= 55) return { label: 'HIGH',     cls: 'badge-high' };
  if (score >= 30) return { label: 'MEDIUM',   cls: 'badge-medium' };
  return               { label: 'LOW',      cls: 'badge-low' };
}

function riskBadge(score) {
  if (score >= 80) return '<span class="badge badge-critical">CRITICAL</span>';
  if (score >= 55) return '<span class="badge badge-high">HIGH</span>';
  if (score >= 30) return '<span class="badge badge-medium">MEDIUM</span>';
  return                  '<span class="badge badge-low">LOW</span>';
}

function flashKpi(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.add('updated');
  setTimeout(() => el.classList.remove('updated'), 1200);
}

// ═══════════════════════════════════════════════════════════════
// TAB NAVIGATION
// ═══════════════════════════════════════════════════════════════

function initTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', function() {
      const tab = this.dataset.tab;
      activateTab(tab);
    });
  });
}

function activateTab(tab) {
  activeTab = tab;

  document.querySelectorAll('.tab-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.tab === tab);
  });

  ['overview', 'patterns', 'reputation', 'portscans', 'multistep'].forEach(t => {
    const el = document.getElementById(`tab-${t}`);
    if (el) el.classList.toggle('hidden', t !== tab);
  });

  // Redraw charts after tab switch (visibility issue)
  requestAnimationFrame(() => {
    if (tab === 'overview') {
      chartTimeline?.update();
      chartVolume?.update();
      chartPatterns?.update();
    } else if (tab === 'reputation') {
      chartReputation?.update();
    } else if (tab === 'portscans') {
      chartPortTop?.update();
    }
  });
}

// ═══════════════════════════════════════════════════════════════
// CONTROLS
// ═══════════════════════════════════════════════════════════════

function togglePolling() {
  isPolling = !isPolling;
  const icon = document.getElementById('pause-icon');
  const btn  = document.getElementById('pause-btn');
  if (isPolling) {
    icon.textContent = '⏸';
    btn.title = 'Pause';
    poll();
  } else {
    icon.textContent = '▶';
    btn.title = 'Resume';
  }
}

function refreshNow() { poll(); }

function clearSearch() { document.getElementById('ip-search').value = ''; }

function exportBatchData() {
  const blob = new Blob([JSON.stringify({ ...currentData, exported_at: new Date().toISOString() }, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `rapid-batch-hbase-${new Date().toISOString().slice(0,19)}.json`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ═══════════════════════════════════════════════════════════════
// IP SEARCH — uses batch endpoints
// ═══════════════════════════════════════════════════════════════

async function searchBatchIP() {
  const ip = document.getElementById('ip-search').value.trim();
  if (!ip) return;

  try {
    const [repData, msData, psData] = await Promise.all([
      fetchBatch(`/batch/ip-reputation/${ip}`).catch(() => null),
      fetchBatch(`/batch/multistep-attacks/ip/${ip}`).catch(() => null),
      fetchBatch(`/batch/port-scans/ip/${ip}?limit=1000`).catch(() => null),
    ]);

    showBatchIPModal(ip, repData, msData, psData);
  } catch (err) {
    showError(`Failed to query HBase for IP ${ip}: ${err.message}`);
  }
}

function showBatchIPModal(ip, repData, msData, psData) {
  document.getElementById('modal-ip').textContent = `HBase: ${ip}`;

  const rep    = repData?.reputation || repData || {};
  const score  = rep.reputation_score ?? rep.score ?? 0;
  const attacks = rep.attack_count ?? rep.total_attacks ?? '—';
  const risk   = rep.risk_level || '';

  document.getElementById('modal-rep-score').textContent   = typeof score === 'number' ? score.toFixed(1) : score;
  document.getElementById('modal-total-attacks').textContent = attacks;
  document.getElementById('modal-risk-level').textContent  = risk || '—';
  document.getElementById('modal-risk-level').className    = 'detail-value ' + getRiskClass(risk);

  const psItems = psData?.port_scans || psData?.scans || [];
  document.getElementById('modal-port-scans').textContent = psItems.length;

  // Multi-step
  const msItems = msData?.attacks || msData?.multistep || [];
  const msBody  = document.getElementById('modal-multistep-body');
  if (msItems.length > 0) {
    msBody.innerHTML = msItems.slice(0, 5).map(m => `
      <div class="alert-item">
        <div class="alert-header">
          <span class="alert-ip">${m.pattern || m.attack_type || 'Unknown'}</span>
          <span class="alert-time">${fmtDate(m.timestamp || m.first_seen)}</span>
        </div>
        <div class="alert-reason">Steps: ${m.step_count ?? m.steps?.length ?? '—'} · Score: ${m.score ?? '—'}</div>
      </div>
    `).join('');
  } else {
    msBody.innerHTML = '<div class="alert-empty">No multi-step attacks found</div>';
  }

  // Port scans
  const portsBody = document.getElementById('modal-ports-body');
  if (psItems.length > 0) {
    portsBody.innerHTML = psItems.slice(0, 5).map(p => `
      <div class="alert-item">
        <div class="alert-header">
          <span class="alert-ip">${p.target_ports ? p.target_ports.slice(0,5).join(', ') : p.port || '?'}</span>
          <span class="alert-time">${fmtDate(p.timestamp)}</span>
        </div>
        <div class="alert-reason">Type: ${p.scan_type || '—'} · Count: ${p.count ?? p.scan_count ?? '—'}</div>
      </div>
    `).join('');
  } else {
    portsBody.innerHTML = '<div class="alert-empty">No port scan records found</div>';
  }

  document.getElementById('ip-modal').hidden = false;
}

function closeModal() { document.getElementById('ip-modal').hidden = true; }

function getRiskClass(level) {
  level = (level || '').toUpperCase();
  if (level === 'CRITICAL') return 'text-critical';
  if (level === 'HIGH')     return 'text-critical';
  if (level === 'MEDIUM')   return 'text-warning';
  return 'text-success';
}

document.addEventListener('click', e => {
  const modal = document.getElementById('ip-modal');
  if (!modal.hidden && e.target === modal) closeModal();
});

// ═══════════════════════════════════════════════════════════════
// KPIs
// ═══════════════════════════════════════════════════════════════

function updateKPIs(patterns, reputation, portscans, multistep, volume, tables) {
  document.getElementById('kpi-patterns-val').textContent  = patterns?.length ?? '—';
  document.getElementById('kpi-reputation-val').textContent = reputation?.length ?? '—';
  document.getElementById('kpi-portscans-val').textContent = portscans?.length ?? '—';
  document.getElementById('kpi-multistep-val').textContent = multistep?.length ?? '—';
  document.getElementById('kpi-volume-val').textContent    = volume?.length ?? '—';
  document.getElementById('kpi-tables-val').textContent    = tables?.tables?.length ?? tables?.length ?? '—';

  ['kpi-patterns','kpi-reputation','kpi-portscans','kpi-multistep','kpi-volume','kpi-tables'].forEach(flashKpi);
}

// ═══════════════════════════════════════════════════════════════
// HBASE TABLES
// ═══════════════════════════════════════════════════════════════

function renderHBaseTables(data) {
  const list = document.getElementById('tables-list');
  const tables = data?.tables || (Array.isArray(data) ? data : []);

  if (tables.length === 0) {
    list.innerHTML = '<span class="table-tag loading">No tables found</span>';
    return;
  }

  list.innerHTML = tables.map(t => {
    const name = typeof t === 'string' ? t : (t.name || t.table || JSON.stringify(t));
    return `<span class="table-tag">${name}</span>`;
  }).join('');
}

// ═══════════════════════════════════════════════════════════════
// CHART: Timeline
// ═══════════════════════════════════════════════════════════════

function initTimelineChart() {
  const ctx = document.getElementById('chartTimeline').getContext('2d');
  const grad = ctx.createLinearGradient(0, 0, 0, 280);
  grad.addColorStop(0, 'rgba(123,97,255,0.3)');
  grad.addColorStop(1, 'rgba(123,97,255,0)');

  chartTimeline = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [{
        label: 'Threats',
        data: [],
        borderColor: CHART_COLOR.accent,
        backgroundColor: grad,
        borderWidth: 2,
        pointRadius: 3,
        pointBackgroundColor: CHART_COLOR.accent,
        pointBorderColor: '#050810',
        pointHoverRadius: 6,
        tension: 0.4,
        fill: true,
      }]
    },
    options: chartOpts('Threats', CHART_COLOR.accent),
  });
}

function updateTimelineChart(data) {
  if (!data || data.length === 0) return;
  const arr = data.threat_timeline || data.timeline || data;
  const labels = arr.map(d => fmtTime(d.timestamp || d.time || d.date || d.hour));
  const values = arr.map(d => d.count || d.attack_count || d.total || d.threat_count || 0);

  chartTimeline.data.labels           = labels;
  chartTimeline.data.datasets[0].data = values;
  chartTimeline.update('active');
  document.getElementById('timeline-meta').textContent =
    `${arr.length} intervals · updated ${new Date().toLocaleTimeString('en-GB')}`;
}

// ═══════════════════════════════════════════════════════════════
// CHART: Volume
// ═══════════════════════════════════════════════════════════════

function initVolumeChart() {
  const ctx = document.getElementById('chartVolume').getContext('2d');
  const grad = ctx.createLinearGradient(0, 0, 0, 220);
  grad.addColorStop(0, 'rgba(0,229,255,0.3)');
  grad.addColorStop(1, 'rgba(0,229,255,0)');

  chartVolume = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{
        label: 'Volume',
        data: [],
        backgroundColor: 'rgba(0,229,255,0.5)',
        borderColor: CHART_COLOR.accent3,
        borderWidth: 1,
        borderRadius: 2,
      }]
    },
    options: chartOpts('Volume', CHART_COLOR.accent3),
  });
}

function updateVolumeChart(data) {
  if (!data || data.length === 0) return;
  const arr = data.threat_volume || data.volume || data;
  const labels = arr.map(d => fmtTime(d.timestamp || d.time || d.date || d.window_start));
  const values = arr.map(d => d.count || d.total || d.volume || d.threat_count || 0);

  chartVolume.data.labels           = labels;
  chartVolume.data.datasets[0].data = values;
  chartVolume.update('active');
  document.getElementById('volume-meta').textContent =
    `${arr.length} records · updated ${new Date().toLocaleTimeString('en-GB')}`;
}

// ═══════════════════════════════════════════════════════════════
// CHART: Attack Patterns (doughnut)
// ═══════════════════════════════════════════════════════════════

function initPatternsChart() {
  const ctx = document.getElementById('chartPatterns').getContext('2d');
  chartPatterns = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: [],
      datasets: [{
        data: [],
        backgroundColor: [
          CHART_COLOR.accent, CHART_COLOR.accent2, CHART_COLOR.accent3,
          CHART_COLOR.yellow, CHART_COLOR.green, CHART_COLOR.orange,
        ],
        borderColor: '#0b0f1a',
        borderWidth: 2,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '58%',
      plugins: {
        legend: { position: 'right', labels: { color: '#4a5a80', font: { size: 10 }, boxWidth: 12 } },
        tooltip: {
          backgroundColor: '#0b0f1a', borderColor: CHART_COLOR.accent, borderWidth: 1,
          callbacks: { label: ctx => ` ${ctx.label}: ${ctx.parsed}` }
        }
      }
    }
  });
}

function updatePatternsChart(data) {
  if (!data || data.length === 0) return;
  // Group by pattern type
  const counts = {};
  data.forEach(d => {
    const t = d.pattern_type || d.attack_type || d.pattern || 'Unknown';
    counts[t] = (counts[t] || 0) + (d.count || d.total || 1);
  });
  const labels = Object.keys(counts);
  const values = Object.values(counts);

  chartPatterns.data.labels           = labels;
  chartPatterns.data.datasets[0].data = values;
  chartPatterns.update('active');
  document.getElementById('patterns-chart-meta').textContent =
    `${labels.length} types · updated ${new Date().toLocaleTimeString('en-GB')}`;
}

// ═══════════════════════════════════════════════════════════════
// CHART: IP Reputation bar (reputation tab)
// ═══════════════════════════════════════════════════════════════

function initReputationChart() {
  const ctx = document.getElementById('chartReputation').getContext('2d');
  chartReputation = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{
        label: 'Reputation Score',
        data: [],
        backgroundColor: [],
        borderColor: [],
        borderWidth: 1,
        borderRadius: 2,
        barPercentage: 0.7,
      }]
    },
    options: {
      ...chartOpts('Score', CHART_COLOR.accent),
      indexAxis: 'y',
    },
  });
}

function updateReputationChart(data) {
  if (!data || data.length === 0) return;
  const sorted = [...data].sort((a, b) =>
    (b.reputation_score ?? b.score ?? 0) - (a.reputation_score ?? a.score ?? 0)
  ).slice(0, 15);

  const labels = sorted.map(d => d.ip || d.source_ip || d.ip_address || '?');
  const values = sorted.map(d => d.reputation_score ?? d.score ?? 0);
  const colors = values.map(v => {
    if (v >= 80) return 'rgba(255,45,107,0.75)';
    if (v >= 55) return 'rgba(255,123,45,0.7)';
    if (v >= 30) return 'rgba(255,214,0,0.65)';
    return 'rgba(123,97,255,0.6)';
  });

  chartReputation.data.labels                      = labels;
  chartReputation.data.datasets[0].data            = values;
  chartReputation.data.datasets[0].backgroundColor = colors;
  chartReputation.data.datasets[0].borderColor     = colors.map(c => c.replace(/[\d.]+\)$/, '1)'));
  chartReputation.update('active');
  document.getElementById('rep-chart-meta').textContent =
    `Top ${sorted.length} IPs · updated ${new Date().toLocaleTimeString('en-GB')}`;
}

// ═══════════════════════════════════════════════════════════════
// CHART: Port scan top (horizontal bar)
// ═══════════════════════════════════════════════════════════════

function initPortTopChart() {
  const ctx = document.getElementById('chartPortTop').getContext('2d');
  chartPortTop = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{
        label: 'Scan Count',
        data: [],
        backgroundColor: 'rgba(255,214,0,0.6)',
        borderColor: CHART_COLOR.yellow,
        borderWidth: 1,
        borderRadius: 2,
      }]
    },
    options: {
      ...chartOpts('Scans', CHART_COLOR.yellow),
      indexAxis: 'y',
    },
  });
}

function updatePortTopChart(data) {
  if (!data || data.length === 0) return;
  const arr = data.top_scanners || data.port_scans || data;
  const labels = arr.slice(0, 10).map(d => d.ip || d.source_ip || '?');
  const values = arr.slice(0, 10).map(d => d.scan_count || d.count || d.total || 0);

  chartPortTop.data.labels           = labels;
  chartPortTop.data.datasets[0].data = values;
  chartPortTop.update('active');
  document.getElementById('top-portscan-meta').textContent =
    `Top ${labels.length} · updated ${new Date().toLocaleTimeString('en-GB')}`;
}

// ═══════════════════════════════════════════════════════════════
// SHARED CHART OPTIONS
// ═══════════════════════════════════════════════════════════════

function chartOpts(label, color) {
  return {
    responsive: true, maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: '#0b0f1a', borderColor: color, borderWidth: 1,
        titleColor: color, bodyColor: '#c8d6f0',
        callbacks: { label: ctx => ` ${label}: ${ctx.parsed.x ?? ctx.parsed.y}` }
      }
    },
    scales: {
      x: { grid: { color: 'rgba(26,34,64,0.6)' }, ticks: { color: '#4a5a80', font: { size: 10 } }, beginAtZero: true },
      y: { grid: { color: 'rgba(26,34,64,0.6)' }, ticks: { color: '#4a5a80', font: { size: 10 } }, beginAtZero: true },
    }
  };
}

// ═══════════════════════════════════════════════════════════════
// TABLE RENDERERS
// ═══════════════════════════════════════════════════════════════

function renderTopPortsTable(data) {
  const tbody = document.getElementById('top-ports-body');
  const arr = data?.top_scanners || data?.port_scans || data || [];
  if (!arr.length) { tbody.innerHTML = '<tr><td colspan="4" class="table-empty">No data</td></tr>'; return; }

  tbody.innerHTML = arr.slice(0, 10).map((d, i) => {
    const ip    = d.ip || d.source_ip || '?';
    const ports = d.scan_count || d.count || 0;
    const score = d.score ?? d.threat_score ?? 0;
    const sev   = severity(score);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td style="color:var(--accent)">${ip}</td>
      <td>${ports.toLocaleString()}</td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
    </tr>`;
  }).join('');
  document.getElementById('top-ports-meta').textContent = `${arr.length} records · updated ${new Date().toLocaleTimeString('en-GB')}`;
}

function renderReputationOverviewTable(data) {
  const tbody = document.getElementById('reputation-body');
  if (!data || !data.length) { tbody.innerHTML = '<tr><td colspan="4" class="table-empty">No data</td></tr>'; return; }

  const sorted = [...data].sort((a, b) => (b.reputation_score ?? b.score ?? 0) - (a.reputation_score ?? a.score ?? 0));
  tbody.innerHTML = sorted.slice(0, 10).map((d, i) => {
    const ip    = d.ip || d.source_ip || d.ip_address || '?';
    const score = d.reputation_score ?? d.score ?? 0;
    const risk  = d.risk_level || severity(score).label;
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td style="color:var(--accent)">${ip}</td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
      <td>${riskBadge(score)}</td>
    </tr>`;
  }).join('');
  document.getElementById('rep-meta').textContent = `${data.length} IPs · ${new Date().toLocaleTimeString('en-GB')}`;
}

function renderReputationFullTable(data) {
  const tbody = document.getElementById('rep-table-body');
  if (!data || !data.length) { tbody.innerHTML = '<tr><td colspan="7" class="table-empty">No reputation data</td></tr>'; return; }

  const sorted = [...data].sort((a, b) => (b.reputation_score ?? b.score ?? 0) - (a.reputation_score ?? a.score ?? 0));
  tbody.innerHTML = sorted.map((d, i) => {
    const ip      = d.ip || d.source_ip || d.ip_address || '?';
    const score   = d.reputation_score ?? d.score ?? 0;
    const attacks = d.attack_count ?? d.total_attacks ?? '—';
    const last    = fmtDate(d.last_seen || d.last_activity || d.timestamp);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td style="color:var(--accent);font-family:var(--font-mono)">${ip}</td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
      <td>${attacks}</td>
      <td>${riskBadge(score)}</td>
      <td style="color:var(--text-dim)">${last}</td>
      <td><button class="btn btn-small" onclick="queryIPFromTable('${ip}')">Details</button></td>
    </tr>`;
  }).join('');
  document.getElementById('rep-table-meta').textContent = `${data.length} IPs · ${new Date().toLocaleTimeString('en-GB')}`;
}

function renderPatternsTable(data) {
  const tbody = document.getElementById('patterns-table-body');
  if (!data || !data.length) { tbody.innerHTML = '<tr><td colspan="8" class="table-empty">No attack patterns</td></tr>'; return; }

  tbody.innerHTML = data.map((d, i) => {
    const ip    = d.ip || d.source_ip || d.ip_address || '?';
    const type  = d.pattern_type || d.attack_type || d.pattern || 'Unknown';
    const count = d.count || d.total || d.occurrences || '—';
    const score = d.score ?? d.threat_score ?? d.severity_score ?? 0;
    const last  = fmtDate(d.last_seen || d.timestamp);
    const sev   = severity(score);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td style="color:var(--accent);font-family:var(--font-mono)">${ip}</td>
      <td>${type}</td>
      <td>${count}</td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
      <td><span class="badge ${sev.cls}">${sev.label}</span></td>
      <td style="color:var(--text-dim)">${last}</td>
      <td><button class="btn btn-small" onclick="queryIPFromTable('${ip}')">Details</button></td>
    </tr>`;
  }).join('');
  document.getElementById('patterns-full-meta').textContent = `${data.length} patterns · ${new Date().toLocaleTimeString('en-GB')}`;
}

function renderPortScansTable(data) {
  const tbody = document.getElementById('portscans-table-body');
  const arr = data?.port_scans || data || [];
  if (!arr.length) { tbody.innerHTML = '<tr><td colspan="7" class="table-empty">No port scan records</td></tr>'; return; }

  tbody.innerHTML = arr.map((d, i) => {
    const ip    = d.ip || d.source_ip || '?';
    const ports = Array.isArray(d.target_ports) ? d.target_ports.slice(0,6).join(', ') + (d.target_ports.length > 6 ? '…' : '') : (d.port || '?');
    const type  = d.scan_type || d.type || '—';
    const count = d.scan_count || d.count || '—';
    const ts    = fmtDate(d.timestamp || d.first_seen);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td style="color:var(--accent);font-family:var(--font-mono)">${ip}</td>
      <td style="font-size:11px;color:var(--text-dim)">${ports}</td>
      <td>${type}</td>
      <td>${count}</td>
      <td style="color:var(--text-dim)">${ts}</td>
      <td><button class="btn btn-small" onclick="queryIPFromTable('${ip}')">Details</button></td>
    </tr>`;
  }).join('');
  document.getElementById('portscans-table-meta').textContent = `${arr.length} records · ${new Date().toLocaleTimeString('en-GB')}`;
}

function renderPortScansFeed(data) {
  const feed = document.getElementById('portscans-feed');
  const arr  = data?.port_scans || data || [];
  if (!arr.length) { feed.innerHTML = '<div class="alert-empty">No port scan activity</div>'; return; }

  feed.innerHTML = arr.slice(0, 15).map(d => {
    const ip    = d.ip || d.source_ip || '?';
    const ports = Array.isArray(d.target_ports) ? d.target_ports.slice(0,4).join(', ') : (d.port || '?');
    const ts    = fmtDate(d.timestamp || d.first_seen);
    return `<div class="alert-item">
      <div class="alert-header">
        <span class="alert-ip">${ip}</span>
        <span class="alert-time">${ts}</span>
      </div>
      <div class="alert-reason">Ports: ${ports}</div>
      <div class="alert-path">Type: ${d.scan_type || '—'} · Count: ${d.scan_count || d.count || '—'}</div>
    </div>`;
  }).join('');
  document.getElementById('portscans-list-meta').textContent = `${arr.length} events · ${new Date().toLocaleTimeString('en-GB')}`;
}

function renderMultiStepChains(data) {
  const container = document.getElementById('multistep-chains');
  if (!data || !data.length) { container.innerHTML = '<div class="alert-empty">No multi-step attack chains found</div>'; return; }

  container.innerHTML = data.slice(0, 10).map(d => {
    const ip    = d.ip || d.source_ip || '?';
    const score = d.score ?? d.total_score ?? 0;
    const steps = d.steps || d.attack_steps || [];
    const pattern = d.pattern || d.attack_chain || '';
    const sev   = severity(score);
    const severityClass = score >= 80 ? 'chain-critical' : score >= 55 ? 'chain-high' : score >= 30 ? 'chain-medium' : '';

    const stepsHtml = steps.length > 0
      ? steps.map((s, idx) => `
          <span class="chain-step">${typeof s === 'string' ? s : (s.type || s.name || `Step ${idx+1}`)}</span>
          ${idx < steps.length - 1 ? '<span class="chain-arrow">→</span>' : ''}
        `).join('')
      : pattern
        ? `<span class="chain-step">${pattern}</span>`
        : '<span style="color:var(--text-dim);font-size:11px">No step details</span>';

    return `<div class="chain-card ${severityClass}">
      <div class="chain-header">
        <span class="chain-ip">${ip}</span>
        <span class="badge ${sev.cls}">${sev.label}</span>
        <span style="color:var(--text-dim);font-size:11px">${fmtDate(d.timestamp || d.first_seen)}</span>
      </div>
      <div class="chain-steps-flow">${stepsHtml}</div>
      <div class="chain-meta">
        <span>Steps: ${d.step_count ?? steps.length ?? '—'}</span>
        <span>Score: ${typeof score==='number'?score.toFixed(1):score}</span>
        <span>Duration: ${d.duration || '—'}</span>
      </div>
    </div>`;
  }).join('');
  document.getElementById('multistep-meta').textContent = `${data.length} chains · ${new Date().toLocaleTimeString('en-GB')}`;
}

function renderMultiStepTable(data) {
  const tbody = document.getElementById('multistep-table-body');
  if (!data || !data.length) { tbody.innerHTML = '<tr><td colspan="7" class="table-empty">No multi-step attacks</td></tr>'; return; }

  tbody.innerHTML = data.map((d, i) => {
    const ip      = d.ip || d.source_ip || '?';
    const steps   = d.step_count ?? (d.steps?.length) ?? '—';
    const pattern = d.pattern || d.attack_chain || d.attack_type || '—';
    const score   = d.score ?? d.total_score ?? 0;
    const dur     = d.duration || '—';
    const sev     = severity(score);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td style="color:var(--accent);font-family:var(--font-mono)">${ip}</td>
      <td>${steps}</td>
      <td style="font-size:11px">${pattern}</td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
      <td style="color:var(--text-dim)">${dur}</td>
      <td><button class="btn btn-small" onclick="queryIPFromTable('${ip}')">Details</button></td>
    </tr>`;
  }).join('');
}

function scoreColor(v) {
  if (v >= 80) return '#ff2d6b';
  if (v >= 55) return '#ff7b2d';
  if (v >= 30) return '#ffd600';
  return '#7b61ff';
}

async function queryIPFromTable(ip) {
  document.getElementById('ip-search').value = ip;
  await searchBatchIP();
}

// ═══════════════════════════════════════════════════════════════
// MAIN POLL LOOP
// ═══════════════════════════════════════════════════════════════

async function poll() {
  if (!isPolling) return;

  try {
    // Fetch all batch endpoints in parallel
    const [
      tablesData,
      patternsData,
      reputationData,
      portScansData,
      portTopData,
      multistepData,
      timelineData,
      volumeData,
    ] = await Promise.all([
      fetchBatch('/batch/hbase/tables').catch(e => ({ error: e.message })),
      fetchBatch('/batch/attack-patterns?limit=50').catch(e => []),
      fetchBatch('/batch/ip-reputation?limit=50').catch(e => []),
      fetchBatch('/batch/port-scans?limit=50').catch(e => []),
      fetchBatch('/batch/port-scans/top?limit=10').catch(e => []),
      fetchBatch('/batch/multistep-attacks?limit=50').catch(e => []),
      fetchBatch('/batch/threat-timeline?limit=50').catch(e => []),
      fetchBatch('/batch/threat-volume?limit=50').catch(e => []),
    ]);

    // Normalize arrays
    const patterns    = patternsData?.attack_patterns  || patternsData?.patterns  || (Array.isArray(patternsData) ? patternsData : []);
    const reputation  = reputationData?.ip_reputation  || reputationData?.ips      || (Array.isArray(reputationData) ? reputationData : []);
    const portscans   = portScansData?.port_scans      || (Array.isArray(portScansData) ? portScansData : []);
    const multistep   = multistepData?.multistep_attacks || multistepData?.attacks || (Array.isArray(multistepData) ? multistepData : []);
    const timeline    = timelineData?.threat_timeline  || timelineData?.timeline  || (Array.isArray(timelineData) ? timelineData : []);
    const volume      = volumeData?.threat_volume      || volumeData?.volume      || (Array.isArray(volumeData) ? volumeData : []);

    // Store for export
    currentData = { tablesData, patterns, reputation, portscans, portTop: portTopData, multistep, timeline, volume };

    setStatus(true);
    hideError();

    // Update HBase tables display
    renderHBaseTables(tablesData);

    // Update KPIs
    updateKPIs(patterns, reputation, portscans, multistep, volume, tablesData);

    // Update charts
    updateTimelineChart(timeline);
    updateVolumeChart(volume);
    updatePatternsChart(patterns);
    updateReputationChart(reputation);
    updatePortTopChart(portTopData);

    // Update tables
    renderReputationOverviewTable(reputation);
    renderReputationFullTable(reputation);
    renderTopPortsTable(portTopData);
    renderPatternsTable(patterns);
    renderPortScansTable(portscans);
    renderPortScansFeed(portscans);
    renderMultiStepChains(multistep);
    renderMultiStepTable(multistep);

    document.getElementById('footer-updated').textContent =
      `Last update: ${new Date().toLocaleTimeString('en-GB')}`;

  } catch (err) {
    setStatus(false);
    console.error('[RAPID Batch] Error:', err);
    showError(`HBase API unreachable: ${err.message}`);
  }
}

// ═══════════════════════════════════════════════════════════════
// BOOT
// ═══════════════════════════════════════════════════════════════

(function init() {
  startClock();
  initTabs();
  initTimelineChart();
  initVolumeChart();
  initPatternsChart();
  initReputationChart();
  initPortTopChart();

  // Search on Enter
  document.getElementById('ip-search').addEventListener('keypress', e => {
    if (e.key === 'Enter') searchBatchIP();
  });

  // Outer click closes modal
  document.addEventListener('click', e => {
    const modal = document.getElementById('ip-modal');
    if (!modal.hidden && e.target === modal) closeModal();
  });

  // First fetch, then every 30s
  poll();
  pollInterval = setInterval(poll, POLL_MS);
})();
