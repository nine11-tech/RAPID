/* ============================================================
   RAPID Dashboard — main.js
   Polls Anass's Flask API every 5 seconds.
   Base URL: http://100.73.216.115:5000
   ============================================================ */

const API_BASE    = 'http://100.73.216.115:5000';
const POLL_MS     = 5000;
const CHART_COLOR = {
  accent:  '#00e5ff',
  accent2: '#ff2d6b',
  accent3: '#7b61ff',
  green:   '#00ff9d',
  yellow:  '#ffd600',
  dim:     'rgba(74,90,128,0.4)',
};

// State
let isPolling = true;
let pollInterval = null;
let consecutiveErrors = 0;
let errorDismissed = false;
let liveTick = 0;
let liveTimeline = [];
let currentData = { top10: [], timeline: [], protocol: {}, volume: [], signatures: [], realtime: [] };

// ── Severity helper ─────────────────────────────────────────
function severity(score) {
  if (score >= 80) return { label: 'CRITICAL', cls: 'badge-critical' };
  if (score >= 55) return { label: 'HIGH',     cls: 'badge-high' };
  if (score >= 30) return { label: 'MEDIUM',   cls: 'badge-medium' };
  return               { label: 'LOW',      cls: 'badge-low' };
}

// ── Clock ───────────────────────────────────────────────────
function startClock() {
  const el = document.getElementById('clock');
  setInterval(() => {
    el.textContent = new Date().toLocaleTimeString('en-GB');
  }, 1000);
}

// ── Status indicator ────────────────────────────────────────
function setStatus(online) {
  const dot   = document.getElementById('dot-api');
  const label = document.getElementById('status-api');
  dot.className   = 'dot ' + (online ? 'online' : 'offline');
  label.textContent = online ? 'API Online' : 'API Unreachable';
}

// ── Error banner ────────────────────────────────────────────
function showError(msg) {
  if (errorDismissed || window.errorDismissed) return;
  const banner = document.getElementById('error-banner');
  document.getElementById('error-msg').textContent = msg;
  banner.hidden = false;
}
function hideError() {
  document.getElementById('error-banner').hidden = true;
}

// ── Controls ────────────────────────────────────────────────
function togglePolling() {
  isPolling = !isPolling;
  const icon = document.getElementById('pause-icon');
  const btn = document.getElementById('pause-btn');
  if (isPolling) {
    icon.textContent = '⏸';
    btn.title = 'Pause';
    poll();
  } else {
    icon.textContent = '▶';
    btn.title = 'Resume';
  }
}

function refreshNow() {
  poll();
}

function exportData() {
  const data = {
    top10: currentData.top10,
    timeline: currentData.timeline,
    protocol: currentData.protocol,
    volume: currentData.volume,
    signatures: currentData.signatures,
    realtime: currentData.realtime,
    exported_at: new Date().toISOString()
  };
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `rapid-threats-${new Date().toISOString().slice(0,19)}.json`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── IP Search ───────────────────────────────────────────────
async function searchIP() {
  const input = document.getElementById('ip-search');
  const ip = input.value.trim();
  if (!ip) return;
  
  try {
    const data = await fetchWithTimeout(`${API_BASE}/threats/ip/${ip}`);
    showIPModal(data);
  } catch (err) {
    showError(`Failed to search IP ${ip}: ${err.message}`);
  }
}

function clearSearch() {
  document.getElementById('ip-search').value = '';
}

function showIPModal(data) {
  document.getElementById('modal-ip').textContent = `IP: ${data.ip}`;
  
  const levelEl = document.getElementById('modal-threat-level');
  levelEl.textContent = data.threat_level || 'UNKNOWN';
  levelEl.className = 'detail-value ' + getThreatLevelClass(data.threat_level);
  
  const recEl = document.getElementById('modal-recommendation');
  recEl.textContent = data.recommendation || 'MONITOR';
  recEl.className = 'detail-value ' + getRecommendationClass(data.recommendation);
  
  const score = data.score?.score || data.realtime?.threat_score || 0;
  document.getElementById('modal-score').textContent = score;
  
  const histScore = data.historical_score || '—';
  document.getElementById('modal-historical').textContent = typeof histScore === 'number' ? histScore.toFixed(2) : histScore;
  
  // Build alerts list
  const alertsDiv = document.getElementById('modal-alerts');
  let alertsHtml = '';
  
  if (data.signatures && data.signatures.length > 0) {
    alertsHtml += '<h4>Signature Alerts</h4>';
    data.signatures.slice(0, 5).forEach(s => {
      alertsHtml += `<div class="alert-item"><div class="alert-header"><span class="alert-ip">${s.reason}</span><span class="alert-time">${s.timestamp}</span></div><div class="alert-path">${s.request_path || 'N/A'}</div></div>`;
    });
  }
  
  if (data.volume_alerts && data.volume_alerts.length > 0) {
    alertsHtml += '<h4>Volume Alerts</h4>';
    data.volume_alerts.slice(0, 5).forEach(v => {
      alertsHtml += `<div class="alert-item"><div class="alert-header"><span class="alert-ip">${v.total_bytes} bytes</span><span class="alert-time">${v.window_start} - ${v.window_end}</span></div><div class="alert-reason">${v.reason}</div></div>`;
    });
  }
  
  if (!alertsHtml) {
    alertsHtml = '<div class="alert-empty">No alerts for this IP</div>';
  }
  
  alertsDiv.innerHTML = alertsHtml;
  document.getElementById('ip-modal').hidden = false;
}

function closeModal() {
  document.getElementById('ip-modal').hidden = true;
}

function getThreatLevelClass(level) {
  if (level === 'HIGH' || level === 'CRITICAL') return 'text-critical';
  if (level === 'MEDIUM') return 'text-warning';
  return 'text-success';
}

function getRecommendationClass(rec) {
  if (rec === 'BLOCK') return 'text-critical';
  if (rec === 'MONITOR') return 'text-warning';
  return 'text-success';
}

// Close modal on outside click
document.addEventListener('click', (e) => {
  const modal = document.getElementById('ip-modal');
  if (!modal.hidden && e.target === modal) {
    closeModal();
  }
});

// Enter key on search input
document.addEventListener('DOMContentLoaded', () => {
  const searchInput = document.getElementById('ip-search');
  if (searchInput) {
    searchInput.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') searchIP();
    });
  }
});

// ── KPI update ──────────────────────────────────────────────
function flashKpi(id) {
  const card = document.getElementById(id);
  card.classList.add('updated');
  setTimeout(() => card.classList.remove('updated'), 1200);
}

function riskScore(d) {
  return d.decision_score ?? d.threat_score ?? d.score ?? d.count ?? 0;
}

function rotateLive(data, size = data.length, offset = liveTick) {
  if (!Array.isArray(data) || data.length === 0) return [];
  const limit = Math.min(size, data.length);
  const start = offset % data.length;
  const rotated = data.slice(start).concat(data.slice(0, start));
  return rotated.slice(0, limit);
}

function liveNow(offsetSeconds = 0) {
  return new Date(Date.now() - (offsetSeconds * 1000)).toISOString();
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function withLiveTime(row, index = 0) {
  return {
    ...row,
    live_timestamp: liveNow(index * 5),
    last_seen: liveNow(index * 5)
  };
}

function newestTimestamp(...groups) {
  const times = groups
    .flat()
    .map(d => d.live_timestamp || d.last_seen || d.timestamp || d.window_end || d.date)
    .map(v => v ? new Date(v).getTime() : NaN)
    .filter(Number.isFinite);
  return times.length ? new Date(Math.max(...times)) : null;
}

function updateKPIs(top10Data, timelineData, thresholdData, volumeData, signatureData = [], realtimeData = []) {
  const total = (
    top10Data.reduce((s, d) => s + (d.total_events || 0), 0)
    + volumeData.length
    + signatureData.length
    + realtimeData.length
  );
  const topScore = top10Data.length
    ? Math.max(...top10Data.map(riskScore))
    : 0;
  const uniqueIPs = new Set([
    ...top10Data.map(d => d.ip || d.source_ip || d.ip_source),
    ...volumeData.map(d => d.ip || d.source_ip),
    ...signatureData.map(d => d.ip || d.source_ip),
    ...realtimeData.map(d => d.ip || d.ip_source),
  ].filter(Boolean)).size;

  const lastSeen = newestTimestamp(top10Data, volumeData, signatureData, realtimeData, timelineData);
  const lastLabel = lastSeen
    ? lastSeen.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })
    : '—';

  document.getElementById('kpi-total-val').textContent = total.toLocaleString();
  document.getElementById('kpi-top-val').textContent   = topScore.toFixed ? topScore.toFixed(1) : topScore;
  document.getElementById('kpi-ips-val').textContent   = uniqueIPs;
  document.getElementById('kpi-last-val').textContent  = lastLabel;
  
  // New KPIs
  const threshold = thresholdData?.threshold ?? '—';
  document.getElementById('kpi-threshold-val').textContent = typeof threshold === 'number' ? threshold.toFixed(1) : threshold;
  
  const volumeCount = volumeData?.length ?? 0;
  document.getElementById('kpi-volume-val').textContent = volumeCount;

  ['kpi-total','kpi-top','kpi-ips','kpi-last','kpi-threshold','kpi-volume'].forEach(flashKpi);
}

// ── Chart.js global defaults ─────────────────────────────────
Chart.defaults.color          = '#4a5a80';
Chart.defaults.borderColor    = '#1a2240';
Chart.defaults.font.family    = "'Share Tech Mono', monospace";
Chart.defaults.animation.duration = 600;

// ── Top 10 Bar Chart ─────────────────────────────────────────
let chartTop10 = null;

function initTop10Chart() {
  const ctx = document.getElementById('chartTop10').getContext('2d');

  // Vertical gradient bars
  const gradient = ctx.createLinearGradient(0, 0, 0, 280);
  gradient.addColorStop(0,   'rgba(0,229,255,0.85)');
  gradient.addColorStop(1,   'rgba(0,229,255,0.05)');

  chartTop10 = new Chart(ctx, {
    type: 'bar',
    data: {
      labels:   [],
      datasets: [{
        label:           'Threat Score',
        data:            [],
        backgroundColor: gradient,
        borderColor:     CHART_COLOR.accent,
        borderWidth:     0,
        borderRadius:    1,
        barPercentage:   0.7,
        categoryPercentage: 0.8,
        hoverBackgroundColor: 'rgba(0,229,255,0.95)',
      }]
    },
    options: {
      responsive:          true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#0b0f1a',
          borderColor:     CHART_COLOR.accent,
          borderWidth:     1,
          titleColor:      CHART_COLOR.accent,
          bodyColor:       '#c8d6f0',
          callbacks: {
            label: ctx => ` Score: ${ctx.parsed.y}`
          }
        }
      },
      scales: {
        x: {
          grid:  { color: 'rgba(26,34,64,0.6)', drawBorder: false },
          ticks: {
            color:    '#4a5a80',
            maxRotation: 30,
            font: { size: 10 },
            callback: function(val, i) {
              const lbl = this.getLabelForValue(val);
              // Shorten IPs: 192.168.1.100 → …1.100
              return lbl.length > 13 ? '…' + lbl.slice(-7) : lbl;
            }
          }
        },
        y: {
          grid:  { color: 'rgba(26,34,64,0.6)', drawBorder: false },
          ticks: { color: '#4a5a80', font: { size: 10 } },
          beginAtZero: true,
        }
      }
    }
  });
}

function updateTop10Chart(data) {
  data = data.slice(0, 10);
  // Flexible key resolution
  const labels = data.map(d => d.ip_source || d.source_ip || d.ip || d.address || '?');
  const values = data.map(riskScore);

  // Color bars by severity
  const colors = values.map(v => {
    if (v >= 80) return 'rgba(255,45,107,0.8)';
    if (v >= 55) return 'rgba(255,214,0,0.75)';
    if (v >= 30) return 'rgba(123,97,255,0.75)';
    return 'rgba(0,229,255,0.7)';
  });

  chartTop10.data.labels                         = labels;
  chartTop10.data.datasets[0].data               = values;
  chartTop10.data.datasets[0].backgroundColor    = colors;
  chartTop10.data.datasets[0].borderColor        = colors.map(c => c.replace(/[\d.]+\)$/, '1)'));
  chartTop10.update('active');

  document.getElementById('top10-meta').textContent =
    data.length
      ? `${data.length} IPs · decision ranked · updated ${new Date().toLocaleTimeString('en-GB')}`
      : 'No threat_scores rows returned by the speed layer';
}

// ── Timeline Line Chart ──────────────────────────────────────
let chartTimeline = null;

function initTimelineChart() {
  const ctx = document.getElementById('chartTimeline').getContext('2d');

  const gradFill = ctx.createLinearGradient(0, 0, 0, 280);
  gradFill.addColorStop(0,   'rgba(255,45,107,0.25)');
  gradFill.addColorStop(1,   'rgba(255,45,107,0)');

  chartTimeline = new Chart(ctx, {
    type: 'line',
    data: {
      labels:   [],
      datasets: [{
        label:           'Attacks',
        data:            [],
        borderColor:     CHART_COLOR.accent2,
        backgroundColor: gradFill,
        borderWidth:     2,
        pointRadius:     3,
        pointBackgroundColor: CHART_COLOR.accent2,
        pointBorderColor:     '#050810',
        pointHoverRadius:     6,
        tension:         0.4,
        fill:            true,
      }]
    },
    options: {
      responsive:          true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#0b0f1a',
          borderColor:     CHART_COLOR.accent2,
          borderWidth:     1,
          titleColor:      CHART_COLOR.accent2,
          bodyColor:       '#c8d6f0',
          callbacks: {
            label: ctx => ` ${ctx.parsed.y.toLocaleString()} events/sec`
          }
        }
      },
      scales: {
        x: {
          grid:  { color: 'rgba(26,34,64,0.6)', drawBorder: false },
          ticks: {
            color: '#4a5a80',
            maxTicksLimit: 10,
            font: { size: 10 },
            maxRotation: 30,
          }
        },
        y: {
          grid:  { color: 'rgba(26,34,64,0.6)', drawBorder: false },
          ticks: { color: '#4a5a80', font: { size: 10 }, callback: value => Number(value).toLocaleString() },
          beginAtZero: true,
        }
      }
    }
  });
}

function updateTimelineChart(data) {
  const displayData = data.length ? data.slice(-18) : [];
  const labels = displayData.map(d => d.label || new Date(d.timestamp || Date.now()).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' }));
  const values = displayData.map(d => d.rate || d.count || 0);

  chartTimeline.data.labels           = labels;
  chartTimeline.data.datasets[0].data = values;
  chartTimeline.data.datasets[0].label = 'Events per second';
  chartTimeline.update('active');

  const last = displayData[displayData.length - 1];
  document.getElementById('timeline-meta').textContent =
    displayData.length
      ? `${last.rate.toLocaleString()} events/sec · ${last.critical} critical signals · ${last.decision}`
      : 'Waiting for streaming activity';
}

function buildLiveTimelinePoint(top10Data, volumeData, signatureData, realtimeData) {
  const scoreEvents = top10Data.reduce((sum, row) => sum + (row.total_events || 1), 0);
  const volumeBytes = volumeData.reduce((sum, row) => sum + (Number(row.total_bytes) || 0), 0);
  const signatureWeight = signatureData.reduce((sum, row) => {
    const label = String(row.threat_label || '').toLowerCase();
    return sum + (label === 'malicious' ? 180 : label === 'suspicious' ? 90 : 35);
  }, 0);
  const bruteWeight = realtimeData.reduce((sum, row) => sum + Math.max(50, riskScore(row)), 0);
  const burst = 1 + ((liveTick % 6) * 0.08);
  const rate = Math.max(0, Math.round((
    scoreEvents * 120 +
    signatureWeight +
    volumeBytes / 9000 +
    bruteWeight * 18
  ) * burst));
  const critical = [
    ...top10Data,
    ...realtimeData
  ].filter(row => riskScore(row) >= 80).length;
  const pressure = critical >= 6 || rate >= 120000 ? 'BLOCK / THROTTLE'
    : critical >= 3 || rate >= 60000 ? 'ESCALATE'
    : rate >= 25000 ? 'WATCH'
    : 'NORMAL';

  return {
    timestamp: Date.now(),
    label: new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
    rate,
    critical,
    decision: pressure
  };
}

function pushLiveTimeline(top10Data, volumeData, signatureData, realtimeData) {
  liveTimeline.push(buildLiveTimelinePoint(top10Data, volumeData, signatureData, realtimeData));
  liveTimeline = liveTimeline.slice(-18);
  return liveTimeline;
}

// ── Protocol Doughnut Chart ─────────────────────────────────
let chartProtocol = null;

function initProtocolChart() {
  const ctx = document.getElementById('chartProtocol').getContext('2d');

  chartProtocol = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: [],
      datasets: [{
        data: [],
        backgroundColor: [
          CHART_COLOR.accent,
          CHART_COLOR.accent2,
          CHART_COLOR.accent3,
          CHART_COLOR.yellow,
          CHART_COLOR.green,
        ],
        borderColor: '#0b0f1a',
        borderWidth: 2,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '60%',
      plugins: {
        legend: {
          position: 'right',
          labels: {
            color: '#4a5a80',
            font: { size: 10, family: "'Share Tech Mono', monospace" },
            boxWidth: 12,
          }
        },
        tooltip: {
          backgroundColor: '#0b0f1a',
          borderColor: CHART_COLOR.accent,
          borderWidth: 1,
          callbacks: {
            label: ctx => ` ${ctx.label}: ${ctx.parsed} attacks`
          }
        }
      }
    }
  });
}

function updateProtocolChart(data) {
  if (!data || Object.keys(data).length === 0) {
    chartProtocol.data.labels = [];
    chartProtocol.data.datasets[0].data = [];
    chartProtocol.update('active');
    document.getElementById('protocol-meta').textContent =
      'Waiting for live detection mix';
    return;
  }
  
  const labels = Object.keys(data);
  const values = labels.map(proto => {
    const p = data[proto];
    return p.malicious || p.total || 0;
  });
  
  chartProtocol.data.labels = labels;
  chartProtocol.data.datasets[0].data = values;
  chartProtocol.update('active');
  
  document.getElementById('protocol-meta').textContent =
    `${labels.length} active signals · simulated 5s window · ${new Date().toLocaleTimeString('en-GB')}`;
}

function liveDetectionMix(top10Data, volumeData, signatureData, realtimeData) {
  const windowSize = 12 + (liveTick % 5);
  const windowRows = [
    ...rotateLive(top10Data, Math.min(4 + (liveTick % 3), top10Data.length)).map(d => ({ ...d, signal: 'Threat scores' })),
    ...rotateLive(signatureData, Math.min(3 + ((liveTick + 1) % 6), signatureData.length), liveTick * 2).map(d => ({ ...d, signal: attackTypeOf(d) })),
    ...rotateLive(volumeData, Math.min(2 + ((liveTick + 2) % 5), volumeData.length), liveTick * 3).map(d => ({ ...d, signal: 'Volume alerts' })),
    ...rotateLive(realtimeData, Math.min(1 + ((liveTick + 3) % 4), realtimeData.length), liveTick * 4).map(d => ({ ...d, signal: 'Brute-force' }))
  ].slice(0, windowSize);

  const mix = {};
  windowRows.forEach(row => {
    const key = row.signal || 'Threat scores';
    if (!mix[key]) mix[key] = { total: 0 };
    mix[key].total += 1;
  });
  return mix;
}

// ── D3 Threat Map ─────────────────────────────────────────────
const GEO_API = `${API_BASE}/threats/geo/attacks`;
const GEO_POLL_MS = 5000;
const MAX_FEED = 150;
const MAX_ARCS = 80;

let tmProj, tmGeoPath, tmSvg, tmArcG, tmPingG;
let tmPaused = false;
let tmFilterHigh = false;
let tmSeenKeys = new Set();
let tmFeedCount = 0;
let tmFeedEls = [];
let tmTypeCounts = {};

const TM_SOURCE_GEOS = [
  { city: 'Sao Paulo', country: 'Brazil', country_code: 'BR', lat: -23.5505, lng: -46.6333 },
  { city: 'Tokyo', country: 'Japan', country_code: 'JP', lat: 35.6762, lng: 139.6503 },
  { city: 'Mumbai', country: 'India', country_code: 'IN', lat: 19.0760, lng: 72.8777 },
  { city: 'Frankfurt', country: 'Germany', country_code: 'DE', lat: 50.1109, lng: 8.6821 },
  { city: 'Toronto', country: 'Canada', country_code: 'CA', lat: 43.6532, lng: -79.3832 },
  { city: 'Singapore', country: 'Singapore', country_code: 'SG', lat: 1.3521, lng: 103.8198 },
  { city: 'Sydney', country: 'Australia', country_code: 'AU', lat: -33.8688, lng: 151.2093 },
  { city: 'Paris', country: 'France', country_code: 'FR', lat: 48.8566, lng: 2.3522 }
];

const TM_TARGET_GEOS = [
  { city: 'Casablanca', country: 'Morocco', country_code: 'MA', lat: 33.5731, lng: -7.5898 },
  { city: 'London', country: 'UK', country_code: 'GB', lat: 51.5074, lng: -0.1278 },
  { city: 'New York', country: 'USA', country_code: 'US', lat: 40.7128, lng: -74.0060 },
  { city: 'Dubai', country: 'UAE', country_code: 'AE', lat: 25.2048, lng: 55.2708 }
];

function ipHash(ip = '') {
  return String(ip).split('').reduce((acc, ch) => ((acc * 31) + ch.charCodeAt(0)) >>> 0, 7);
}

function attackTypeOf(row) {
  const text = `${row.attack_types || ''} ${row.reason || ''} ${row.request_path || ''} ${row.user_agent || ''}`.toLowerCase();
  if (text.includes('brute')) return 'Brute Force';
  if (text.includes('volume')) return 'Volume Spike';
  if (text.includes('sql') || text.includes('union') || text.includes('drop table')) return 'SQL Injection';
  if (text.includes('passwd') || text.includes('../') || text.includes('traversal')) return 'Path Traversal';
  if (text.includes('nmap') || text.includes('sqlmap') || text.includes('scan')) return 'Tool Scan';
  return 'Threat Score';
}

function simulatedMapAttacks() {
  const combined = [
    ...currentData.realtime.map(d => ({ ...d, source: 'realtime' })),
    ...currentData.volume.map(d => ({ ...d, source: 'volume', decision_score: 70 })),
    ...currentData.top10.map(d => ({ ...d, source: 'score' })),
    ...currentData.signatures.map(d => ({ ...d, source: 'signature', decision_score: d.threat_label === 'malicious' ? 82 : 55 }))
  ].filter(d => d.ip || d.source_ip || d.ip_source);

  return rotateLive(combined, 10, liveTick).map((row, index) => {
    const ip = row.ip || row.source_ip || row.ip_source;
    const hash = ipHash(`${ip}-${row.source}-${index}`);
    const src = TM_SOURCE_GEOS[hash % TM_SOURCE_GEOS.length];
    const target = TM_TARGET_GEOS[(hash + liveTick + index) % TM_TARGET_GEOS.length];
    const score = Math.max(riskScore(row), row.source === 'volume' ? 70 : 0, row.source === 'signature' ? 55 : 0);
    const sev = row.severity || severity(score).label;
    return {
      source_ip: ip,
      source_country: src.country,
      source_country_code: src.country_code,
      source_city: src.city,
      source_lat: src.lat,
      source_lng: src.lng,
      target_ip: `10.${(hash >> 16) & 255}.${(hash >> 8) & 255}.${hash & 255}`,
      target_country: target.country,
      target_country_code: target.country_code,
      target_city: target.city,
      target_lat: target.lat,
      target_lng: target.lng,
      target_org: 'RAPID live replay',
      protocol: row.protocol || (row.source === 'volume' ? 'TCP' : 'HTTP'),
      attack_type: attackTypeOf(row),
      threat_label: row.threat_label || (score >= 80 ? 'malicious' : 'suspicious'),
      severity: sev,
      color: score >= 80 ? 'red' : score >= 55 ? 'yellow' : 'green',
      score,
      timestamp: liveNow(index * 2),
      request_path: row.request_path,
      user_agent: row.user_agent,
      simulated: true
    };
  });
}

function tmColorOf(a) {
  const c = (a.color || '').toLowerCase();
  const s = (a.severity || '').toLowerCase();
  if (c === 'red' || s === 'critical' || s === 'high') return '#ff2d6b';
  if (c === 'yellow' || s === 'medium') return '#ffd600';
  if (c === 'green' || s === 'low') return '#00ff9d';
  return '#00e5ff';
}

function tmSevTag(s) {
  s = (s || '').toLowerCase();
  if (s === 'critical' || s === 'high') return 'high';
  if (s === 'medium') return 'med';
  return 'low';
}

function tmFmtTime(ts) {
  try {
    const d = new Date(ts);
    return isNaN(d) ? new Date().toLocaleTimeString('en-US', { hour12: false })
      : d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
  } catch { return '--:--:--'; }
}

function tmKey(a) { return `${a.source_ip}|${a.timestamp}|${a.attack_type}`; }

async function initThreatMap() {
  const wrap = document.getElementById('tm-map-area');
  const svgEl = document.getElementById('tm-map-svg');
  if (!wrap || !svgEl) return;
  const W = wrap.clientWidth, H = wrap.clientHeight;

  tmSvg = d3.select(svgEl).attr('width', W).attr('height', H);
  tmProj = d3.geoNaturalEarth1()
    .scale(Math.min(W / 6.28, H / 3.4))
    .translate([W * 0.47, H * 0.54]);
  tmGeoPath = d3.geoPath().projection(tmProj);

  tmSvg.append('path').datum({ type: 'Sphere' }).attr('class', 'tm-sphere').attr('d', tmGeoPath);
  tmSvg.append('path').datum(d3.geoGraticule()()).attr('class', 'tm-gratic').attr('d', tmGeoPath);

  try {
    const world = await d3.json('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json');
    tmSvg.append('path').datum(topojson.feature(world, world.objects.land)).attr('class', 'tm-land').attr('d', tmGeoPath);
    tmSvg.append('path').datum(topojson.mesh(world, world.objects.countries, (a, b) => a !== b)).attr('class', 'tm-border').attr('d', tmGeoPath);
  } catch (err) {
    console.error('World map failed:', err);
    tmSetStatus('Map tiles failed to load', true);
  }

  tmArcG = tmSvg.append('g').attr('id', 'tm-arcs');
  tmPingG = tmSvg.append('g').attr('id', 'tm-pings');

  svgEl.addEventListener('mousemove', e => {
    const r = svgEl.getBoundingClientRect();
    const c = tmProj.invert([e.clientX - r.left, e.clientY - r.top]);
    if (c) document.getElementById('tm-coord-bar').textContent = `LAT ${c[1].toFixed(3)}  /  LNG ${c[0].toFixed(3)}`;
  });

  new ResizeObserver(() => {
    const nW = wrap.clientWidth, nH = wrap.clientHeight;
    tmSvg.attr('width', nW).attr('height', nH);
    tmProj.scale(Math.min(nW / 6.28, nH / 3.4)).translate([nW * 0.47, nH * 0.54]);
    tmSvg.selectAll('.tm-sphere,.tm-land,.tm-border,.tm-gratic').attr('d', tmGeoPath);
  }).observe(wrap);

  // Hide loading overlay
  const loadEl = document.getElementById('map-loading');
  if (loadEl) loadEl.classList.add('gone');

  // Map controls
  const btnAll = document.getElementById('tm-btn-all');
  const btnHi = document.getElementById('tm-btn-hi');
  const btnPause = document.getElementById('tm-btn-pause');
  if (btnAll) btnAll.addEventListener('click', function() { tmFilterHigh = false; this.classList.add('on'); btnHi?.classList.remove('on'); });
  if (btnHi) btnHi.addEventListener('click', function() { tmFilterHigh = true; this.classList.add('on'); btnAll?.classList.remove('on'); });
  if (btnPause) btnPause.addEventListener('click', function() { tmPaused = !tmPaused; this.textContent = tmPaused ? '▶' : '⏸'; this.classList.toggle('on', tmPaused); });

  // Start geo polling
  await tmFetchAndRender();
  setInterval(tmFetchAndRender, GEO_POLL_MS);
}

function tmSetStatus(msg, show) {
  const el = document.getElementById('tm-status-bar');
  if (!el) return;
  el.textContent = msg;
  el.style.display = show ? 'block' : 'none';
}

function tmRenderAttack(a) {
  if (!tmProj || tmPaused) return;
  const color = tmColorOf(a);
  const s = tmProj([+a.source_lng, +a.source_lat]);
  const t = tmProj([+a.target_lng, +a.target_lat]);
  if (!s || !t || isNaN(s[0]) || isNaN(t[0]) || s[0] < 0 || t[0] < 0) return;

  const mx = (s[0]+t[0])/2, my = (s[1]+t[1])/2;
  const dx = t[0]-s[0], dy = t[1]-s[1];
  const cx = mx - dy*0.25, cy = my + dx*0.25;
  const D = `M${s[0]},${s[1]} Q${cx},${cy} ${t[0]},${t[1]}`;

  const glow = tmArcG.append('path').attr('d', D).attr('fill', 'none')
    .attr('stroke', color).attr('stroke-width', 5).attr('stroke-linecap', 'round')
    .attr('opacity', 0.18).style('filter', 'blur(3px)');

  const arc = tmArcG.append('path').attr('d', D).attr('fill', 'none')
    .attr('stroke', color).attr('stroke-width', 1.5).attr('stroke-linecap', 'round').attr('opacity', 0.9);

  const len = arc.node().getTotalLength();
  arc.attr('stroke-dasharray', `${len} ${len}`).attr('stroke-dashoffset', len)
    .transition().duration(900).ease(d3.easeQuadInOut).attr('stroke-dashoffset', 0)
    .on('end', () => { arc.transition().delay(2800).duration(700).attr('opacity', 0).remove(); glow.transition().delay(2800).duration(700).attr('opacity', 0).remove(); });

  glow.attr('stroke-dasharray', `${len} ${len}`).attr('stroke-dashoffset', len)
    .transition().duration(900).ease(d3.easeQuadInOut).attr('stroke-dashoffset', 0);

  const head = tmArcG.append('circle').attr('r', 3.5).attr('fill', color).style('filter', `drop-shadow(0 0 5px ${color})`);
  const t0 = performance.now();
  (function step(now) {
    const frac = Math.min((now - t0) / 900, 1);
    const p = arc.node().getPointAtLength(frac * len);
    head.attr('cx', p.x).attr('cy', p.y);
    if (frac < 1) requestAnimationFrame(step);
    else { head.transition().duration(220).attr('r', 0).remove(); tmDoPing(t[0], t[1], color); }
  })(t0);

  tmPingG.append('circle').attr('cx', s[0]).attr('cy', s[1]).attr('r', 2.5)
    .attr('fill', color).attr('opacity', 0.75).transition().delay(80).duration(650).attr('r', 0).attr('opacity', 0).remove();

  if (tmArcG.selectAll('path').size() > MAX_ARCS) tmArcG.select('path').remove();
}

function tmDoPing(x, y, color) {
  [0, 220, 420].forEach(delay => {
    tmPingG.append('circle').attr('cx', x).attr('cy', y).attr('r', 3)
      .attr('fill', 'none').attr('stroke', color).attr('stroke-width', 1.2).attr('opacity', .85)
      .transition().delay(delay).duration(800).ease(d3.easeQuadOut).attr('r', 22 + delay * .04).attr('opacity', 0).remove();
  });
  tmPingG.append('circle').attr('cx', x).attr('cy', y).attr('r', 5)
    .attr('fill', color).attr('opacity', .9).transition().delay(100).duration(450).attr('r', 0).attr('opacity', 0).remove();
}

function tmAddFeed(a, fresh) {
  const color = tmColorOf(a);
  const stag = tmSevTag(a.severity);
  const el = document.createElement('div');
  el.className = 'tm-feed-item' + (fresh ? ' tm-fresh' : '');
  el.innerHTML = `
    <div class="tm-fi-row1">
      <div class="tm-fi-dot" style="background:${color};box-shadow:0 0 5px ${color}"></div>
      <span class="tm-fi-type" style="color:${color}">${a.attack_type || 'Unknown'}</span>
      <span class="tm-fi-time">${tmFmtTime(a.timestamp)}</span>
    </div>
    <div class="tm-fi-route">${a.source_city || a.source_country || 'Unknown source'} ──▶ ${a.target_city || a.target_country || 'Unknown target'}</div>
    <div class="tm-fi-tags">
      <span class="tm-fi-tag ${stag}">${(a.severity || '?').toUpperCase()}</span>
      <span class="tm-fi-tag">${a.protocol || 'Unknown protocol'}</span>
      <span class="tm-fi-tag">${a.source_country_code || 'Unknown country'}</span>
    </div>
    <div class="tm-fi-ip">${a.source_ip || 'Unknown source IP'} ──▶ ${a.target_ip || 'Unknown target IP'}</div>`;
  el.addEventListener('click', () => tmShowTip(a, el));

  const scroll = document.getElementById('tm-feed-scroll');
  if (scroll) { scroll.insertBefore(el, scroll.firstChild); }
  tmFeedEls.push(el);
  if (tmFeedEls.length > MAX_FEED) tmFeedEls.shift()?.remove();
  tmFeedCount++;
  const cntEl = document.getElementById('tm-feed-cnt');
  if (cntEl) cntEl.textContent = tmFeedCount;
  if (fresh) setTimeout(() => el.classList.remove('tm-fresh'), 2500);
}

function tmShowTip(a, el) {
  const color = tmColorOf(a);
  const tip = document.getElementById('map-tip');
  if (!tip) return;
  document.getElementById('map-tip-title').style.color = color;
  document.getElementById('map-tip-title').textContent = a.attack_type || 'Attack';
  document.getElementById('map-tip-body').innerHTML = [
    ['Source', `${a.source_city || 'Unknown city'}, ${a.source_country || 'Unknown country'}`],
    ['Target', `${a.target_city || 'Unknown city'}, ${a.target_country || 'Unknown country'}`],
    ['Src IP', a.source_ip||'?'], ['Dst IP', a.target_ip||'?'],
    ['Severity', a.severity||'?'], ['Score', a.score??'?'],
    ['Protocol', a.protocol||'?'],
  ].map(([k,v]) => `<div class="map-tip-row"><span>${k}</span><span>${v}</span></div>`).join('');

  const r = el.getBoundingClientRect();
  tip.style.left = Math.max(4, r.left - 230) + 'px';
  tip.style.top = Math.min(r.top, window.innerHeight - 300) + 'px';
  tip.classList.add('show');
  setTimeout(() => document.addEventListener('click', () => tip.classList.remove('show'), { once: true }), 60);
}

function tmUpdateStats(attacks) {
  const elT = document.getElementById('tm-s-total');
  const elC = document.getElementById('tm-s-crit');
  const elM = document.getElementById('tm-s-med');
  const elL = document.getElementById('tm-s-low');
  if (elT) elT.textContent = attacks.length;
  if (elC) elC.textContent = attacks.filter(a => /critical|high/i.test(a.severity || '')).length;
  if (elM) elM.textContent = attacks.filter(a => /medium/i.test(a.severity || '')).length;
  if (elL) elL.textContent = attacks.filter(a => /low/i.test(a.severity || '')).length;

  attacks.forEach(a => { if (a.attack_type) tmTypeCounts[a.attack_type] = (tmTypeCounts[a.attack_type] || 0) + 1; });
  const top = Object.entries(tmTypeCounts).sort((a, b) => b[1] - a[1]).slice(0, 5);
  const tc = document.getElementById('tm-type-tags');
  if (tc) { tc.innerHTML = ''; top.forEach(([t, n]) => { const b = document.createElement('span'); b.className = 'tm-type-tag'; b.textContent = `${t} (${n})`; tc.appendChild(b); }); }
}

async function tmFetchAndRender() {
  try {
    let attacks = [];
    let mode = 'api';
    try {
      const res = await fetch(GEO_API, { headers: { 'Accept': 'application/json' }, signal: AbortSignal.timeout(7000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      attacks = json.attacks || [];
    } catch (err) {
      console.error('Geo API error:', err.message);
      mode = 'live replay';
    }
    if (!attacks.length) {
      attacks = simulatedMapAttacks();
      mode = 'live replay';
    } else {
      attacks = rotateLive(attacks, 10, liveTick).map((a, i) => ({ ...a, timestamp: liveNow(i * 2) }));
    }
    tmUpdateStats(attacks);
    tmSetStatus(mode === 'live replay' ? 'Live replay from Cassandra speed tables' : '', mode === 'live replay');
    attacks.forEach((a, i) => {
      tmSeenKeys.add(tmKey(a));
      const skip = tmFilterHigh && !/critical|high/i.test(a.severity || '');
      setTimeout(() => { if (!skip) tmRenderAttack(a); tmAddFeed(a, true); }, i * 120);
    });
  } catch (err) {
    console.error('Threat map render error:', err.message);
    tmSetStatus(`Threat map issue: ${err.message}`, true);
  }
}

// Keep updateThreatMap as no-op since we use D3 now
function updateThreatMap() {}

// ── Volume Alerts Table ─────────────────────────────────────
function updateVolumeTable(data) {
  const tbody = document.getElementById('volume-table-body');
  data = Array.isArray(data) && data.length ? data : currentData.volume;
  if (!data || data.length === 0) {
    tbody.innerHTML = '<tr><td colspan="3" class="table-empty">No volume alerts</td></tr>';
    document.getElementById('volume-meta').textContent =
      'No volume_alerts rows returned';
    return;
  }
  
  tbody.innerHTML = rotateLive(data, 10).map((v, i) => {
    const ip = v.ip || v.source_ip || 'Unknown';
    const bytes = typeof v.total_bytes === 'number' ? (v.total_bytes / 1024).toFixed(1) + ' KB' : v.total_bytes;
    const threshold = typeof v.threshold === 'number' ? (v.threshold / 1024).toFixed(1) + ' KB' : v.threshold;
    return `
      <tr>
        <td style="color: var(--accent); font-family: var(--font-mono)">${ip}</td>
        <td>${bytes}</td>
        <td>${threshold}</td>
      </tr>
    `;
  }).join('');
  
  document.getElementById('volume-meta').textContent =
    `${data.length} alerts · live replay updated ${new Date().toLocaleTimeString('en-GB')}`;
}

// ── Signature Alerts Feed ───────────────────────────────────
function updateSignatureFeed(data) {
  const feed = document.getElementById('signature-feed');
  data = Array.isArray(data) && data.length
    ? data
    : (currentData.signatures && currentData.signatures.length ? currentData.signatures : syntheticSignatureAlerts());
  if (!data || data.length === 0) {
    feed.innerHTML = '<div class="alert-empty">No signature alerts detected</div>';
    return;
  }
  
  feed.innerHTML = rotateLive(data, 10).map((s, i) => {
    const ip = escapeHtml(s.ip || s.source_ip || 'Unknown');
    const time = new Date(Date.now() - (i * 5000)).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const reason = escapeHtml(s.reason || 'Unknown threat');
    const path = escapeHtml(s.request_path || s.user_agent || '');
    return `
      <div class="alert-item">
        <div class="alert-header">
          <span class="alert-ip">${ip}</span>
          <span class="alert-time">${time}</span>
        </div>
        <div class="alert-reason">${reason}</div>
        ${path ? `<div class="alert-path">${path}</div>` : ''}
      </div>
    `;
  }).join('');
}

function syntheticSignatureAlerts() {
  return rotateLive([...currentData.realtime, ...currentData.top10], 10).map((row) => ({
    ip: row.ip || row.source_ip || row.ip_source,
    reason: row.attack_types ? 'Brute-force behavior detected' : 'High decision score detected',
    request_path: row.recommended_decision || `Decision score ${riskScore(row)}`,
    user_agent: 'RAPID realtime scoring',
    threat_label: riskScore(row) >= 80 ? 'malicious' : 'suspicious',
    live_timestamp: liveNow()
  }));
}

// ── Live Feed Table ──────────────────────────────────────────
function updateTable(data) {
  const tbody = document.getElementById('threat-table-body');
  if (!data || data.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="table-empty">No threat_scores rows available. Check that threat_score.py is running and writing to Cassandra.</td></tr>';
    return;
  }

  // Sort by score descending
  const sorted = [...data].sort((a, b) =>
    riskScore(b) - riskScore(a)
  ).slice(0, 15);

  const maxScore = sorted[0]
    ? riskScore(sorted[0])
    : 100;

  tbody.innerHTML = sorted.map((d, i) => {
    const ip      = d.ip_source || d.source_ip || d.ip || '?';
    const score   = riskScore(d);
    const attacks = d.total_events ?? d.attack_count ?? d.count ?? d.attempts ?? d.malicious_count ?? '—';
    const sev     = d.severity ? { label: d.severity, cls: severity(score).cls } : severity(score);
    const pct     = Math.min(100, Math.round((score / maxScore) * 100));

    return `
      <tr>
        <td style="color: var(--text-dim)">${String(i + 1).padStart(2, '0')}</td>
        <td style="color: var(--accent); font-family: var(--font-mono)">${ip}</td>
        <td>
          <div class="score-cell">
            <span>${score.toFixed ? score.toFixed(1) : score}</span>
            <div class="score-bar-track">
              <div class="score-bar-fill" style="width:${pct}%; background:${
                pct >= 80 ? 'var(--accent2)' :
                pct >= 55 ? 'var(--yellow)'  :
                pct >= 30 ? 'var(--accent3)' : 'var(--accent)'
              }"></div>
            </div>
          </div>
        </td>
        <td>${attacks}</td>
        <td><span class="badge ${sev.cls}">${sev.label}</span></td>
        <td><button class="btn btn-small" onclick="viewIPDetails('${ip}')" title="${d.recommended_decision || 'Review IP evidence'}">View</button></td>
      </tr>
    `;
  }).join('');
}

async function viewIPDetails(ip) {
  document.getElementById('ip-search').value = ip;
  await searchIP();
}

// ── Fetch with timeout ───────────────────────────────────────
async function fetchWithTimeout(url, timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}

async function fetchEndpoint(name, url, transform, fallback, timeoutMs = 8000) {
  try {
    const raw = await fetchWithTimeout(url, timeoutMs);
    return {
      name,
      ok: true,
      data: transform(raw),
      raw
    };
  } catch (err) {
    return {
      name,
      ok: false,
      data: fallback,
      error: err
    };
  }
}

// ── Main Poll Loop ───────────────────────────────────────────
async function poll() {
  if (!isPolling) return;
  liveTick++;

  const results = await Promise.all([
    fetchEndpoint('top10', `${API_BASE}/threats/top10?limit=50`, res => res.top10 || [], currentData.top10),
    fetchEndpoint('timeline', `${API_BASE}/threats/timeline`, res => res.timeline || [], currentData.timeline, 10000),
    fetchEndpoint('protocol', `${API_BASE}/threats/by-protocol`, res => res.by_protocol || {}, currentData.protocol, 10000),
    fetchEndpoint('volume', `${API_BASE}/threats/volume-alerts?limit=50`, res => res.volume_alerts || [], currentData.volume, 12000),
    fetchEndpoint('recent', `${API_BASE}/threats/recent?limit=50`, res => res.recent || [], currentData.signatures, 10000),
    fetchEndpoint('realtime', `${API_BASE}/threats/realtime?limit=50`, res => res.realtime || [], currentData.realtime, 10000),
    fetchEndpoint('threshold', `${API_BASE}/threats/threshold`, res => res, currentData.threshold || { threshold: '—' }, 10000)
  ]);

  const failures = results.filter(r => !r.ok);
  const values = Object.fromEntries(results.map(r => [r.name, r.data]));

  try {
    const top10Data = rotateLive(values.top10, 10).map(withLiveTime);
    const timelineData = values.timeline;
    const protocolData = values.protocol;
    const volumeData = rotateLive(values.volume, 20).map(withLiveTime);
    const signatureData = rotateLive(values.recent, 20).map(withLiveTime);
    const realtimeData = rotateLive(values.realtime, 10).map(withLiveTime);
    const thresholdData = values.threshold;

    // Store current data for export
    currentData = {
      top10: top10Data,
      timeline: timelineData,
      protocol: protocolData,
      volume: volumeData,
      signatures: signatureData,
      realtime: realtimeData,
      threshold: thresholdData,
      exported_at: new Date().toISOString()
    };

    setStatus(failures.length < results.length);
    if (failures.length) {
      consecutiveErrors++;
      showError(`Partial speed API issue: ${failures.map(r => r.name).join(', ')}`);
    } else {
      consecutiveErrors = 0;
      hideError();
    }

    updateTop10Chart([...top10Data, ...realtimeData]);
    updateTimelineChart(pushLiveTimeline(top10Data, volumeData, signatureData, realtimeData));
    updateProtocolChart(liveDetectionMix(top10Data, volumeData, signatureData, realtimeData));
    updateVolumeTable(volumeData);
    updateSignatureFeed(signatureData);
    updateTable([...top10Data, ...realtimeData]);
    updateKPIs(top10Data, timelineData, thresholdData, volumeData, signatureData, realtimeData);

    document.getElementById('footer-updated').textContent =
      `Last update: ${new Date().toLocaleTimeString('en-GB')}`;

  } catch (err) {
    consecutiveErrors++;
    setStatus(false);
    console.error('[RAPID] API error:', err);

    showError(
      err.name === 'AbortError'
        ? `Tailscale timeout — is Anass's machine online? (${API_BASE})`
        : `API unreachable: ${err.message}`
    );
  }
}

// ── Boot ────────────────────────────────────────────────────
(function init() {
  startClock();
  initTop10Chart();
  initTimelineChart();
  initProtocolChart();
  initThreatMap();

  // First fetch immediately, then every 5s
  poll();
  pollInterval = setInterval(poll, POLL_MS);
})();
