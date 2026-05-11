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
let currentData = { top10: [], timeline: [], protocol: {}, volume: [], signatures: [] };

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

function updateKPIs(top10Data, timelineData, thresholdData, volumeData) {
  const total = timelineData.reduce((s, d) => s + (d.count || d.attack_count || d.total || 0), 0);
  const topScore = top10Data.length
    ? Math.max(...top10Data.map(d => d.threat_score ?? d.score ?? d.count ?? 0))
    : 0;
  const uniqueIPs = top10Data.length;

  // Last activity timestamp from timeline
  const lastEntry = timelineData[timelineData.length - 1];
  const lastTime  = lastEntry
    ? (lastEntry.timestamp || lastEntry.time || lastEntry.date || '—')
    : '—';
  const lastLabel = lastTime !== '—'
    ? new Date(lastTime).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })
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
  // Flexible key resolution
  const labels = data.map(d => d.ip_source || d.source_ip || d.ip || d.address || '?');
  const values = data.map(d => d.threat_score ?? d.score ?? d.count ?? 0);

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
    `${data.length} IPs · updated ${new Date().toLocaleTimeString('en-GB')}`;
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
            label: ctx => ` Attacks: ${ctx.parsed.y}`
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
          ticks: { color: '#4a5a80', font: { size: 10 } },
          beginAtZero: true,
        }
      }
    }
  });
}

function updateTimelineChart(data) {
  const labels = data.map(d => {
    const raw = d.timestamp || d.time || d.date || d.hour || '?';
    try {
      return new Date(raw).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
    } catch { return raw; }
  });
  const values = data.map(d => d.count || d.attack_count || d.attacks || d.malicious || 0);

  chartTimeline.data.labels           = labels;
  chartTimeline.data.datasets[0].data = values;
  chartTimeline.update('active');

  document.getElementById('timeline-meta').textContent =
    `${data.length} intervals · updated ${new Date().toLocaleTimeString('en-GB')}`;
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
  if (!data || Object.keys(data).length === 0) return;
  
  const labels = Object.keys(data);
  const values = labels.map(proto => {
    const p = data[proto];
    return p.malicious || p.total || 0;
  });
  
  chartProtocol.data.labels = labels;
  chartProtocol.data.datasets[0].data = values;
  chartProtocol.update('active');
  
  document.getElementById('protocol-meta').textContent =
    `${labels.length} protocols · updated ${new Date().toLocaleTimeString('en-GB')}`;
}

// ── D3 Threat Map ─────────────────────────────────────────────
const GEO_API = `${API_BASE}/threats/geo/attacks`;
const GEO_POLL_MS = 8000;
const MAX_FEED = 150;
const MAX_ARCS = 80;

let tmProj, tmGeoPath, tmSvg, tmArcG, tmPingG;
let tmPaused = false;
let tmFilterHigh = false;
let tmSeenKeys = new Set();
let tmFeedCount = 0;
let tmFeedEls = [];
let tmTypeCounts = {};

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
    <div class="tm-fi-route">${a.source_city || a.source_country || '?'} ──▶ ${a.target_city || a.target_country || '?'}</div>
    <div class="tm-fi-tags">
      <span class="tm-fi-tag ${stag}">${(a.severity || '?').toUpperCase()}</span>
      <span class="tm-fi-tag">${a.protocol || '?'}</span>
      <span class="tm-fi-tag">${a.source_country_code || '?'}</span>
    </div>
    <div class="tm-fi-ip">${a.source_ip || '?'} ──▶ ${a.target_ip || '?'}</div>`;
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
    ['Source', `${a.source_city||'?'}, ${a.source_country||'?'}`],
    ['Target', `${a.target_city||'?'}, ${a.target_country||'?'}`],
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
    const res = await fetch(GEO_API, { headers: { 'Accept': 'application/json' }, signal: AbortSignal.timeout(7000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();
    const attacks = json.attacks || [];
    tmUpdateStats(attacks);
    tmSetStatus('', false);
    const firstLoad = tmSeenKeys.size === 0;
    const toProcess = firstLoad ? attacks : attacks.filter(a => !tmSeenKeys.has(tmKey(a)));
    toProcess.forEach((a, i) => {
      tmSeenKeys.add(tmKey(a));
      const skip = tmFilterHigh && !/critical|high/i.test(a.severity || '');
      setTimeout(() => { if (!skip) tmRenderAttack(a); tmAddFeed(a, !firstLoad); }, i * 120);
    });
  } catch (err) {
    console.error('Geo API error:', err.message);
    tmSetStatus(`⚠ API unreachable: ${err.message}`, true);
  }
}

// Keep updateThreatMap as no-op since we use D3 now
function updateThreatMap() {}

// ── Volume Alerts Table ─────────────────────────────────────
function updateVolumeTable(data) {
  const tbody = document.getElementById('volume-table-body');
  if (!data || data.length === 0) {
    tbody.innerHTML = '<tr><td colspan="3" class="table-empty">No volume alerts</td></tr>';
    return;
  }
  
  tbody.innerHTML = data.slice(0, 10).map(v => {
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
    `${data.length} alerts · updated ${new Date().toLocaleTimeString('en-GB')}`;
}

// ── Signature Alerts Feed ───────────────────────────────────
function updateSignatureFeed(data) {
  const feed = document.getElementById('signature-feed');
  if (!data || data.length === 0) {
    feed.innerHTML = '<div class="alert-empty">No signature alerts detected</div>';
    return;
  }
  
  feed.innerHTML = data.slice(0, 10).map(s => {
    const ip = s.ip || s.source_ip || 'Unknown';
    const time = s.timestamp ? new Date(s.timestamp).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '—';
    const reason = s.reason || 'Unknown threat';
    const path = s.request_path || s.user_agent || '';
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

// ── Live Feed Table ──────────────────────────────────────────
function updateTable(data) {
  const tbody = document.getElementById('threat-table-body');
  if (!data || data.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="table-empty">No threat data available</td></tr>';
    return;
  }

  // Sort by score descending
  const sorted = [...data].sort((a, b) =>
    (b.threat_score ?? b.score ?? b.count ?? 0) -
    (a.threat_score ?? a.score ?? a.count ?? 0)
  );

  const maxScore = sorted[0]
    ? (sorted[0].threat_score ?? sorted[0].score ?? sorted[0].count ?? 100)
    : 100;

  tbody.innerHTML = sorted.map((d, i) => {
    const ip      = d.ip_source || d.source_ip || d.ip || '?';
    const score   = d.threat_score ?? d.score ?? d.count ?? 0;
    const attacks = d.attack_count ?? d.count ?? d.attempts ?? d.malicious_count ?? '—';
    const sev     = severity(score);
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
        <td><button class="btn btn-small" onclick="viewIPDetails('${ip}')">View</button></td>
      </tr>
    `;
  }).join('');
}

async function viewIPDetails(ip) {
  document.getElementById('ip-search').value = ip;
  await searchIP();
}

// ── Fetch with timeout ───────────────────────────────────────
async function fetchWithTimeout(url, timeoutMs = 4000) {
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

// ── Main Poll Loop ───────────────────────────────────────────
async function poll() {
  if (!isPolling) return;
  
  try {
    const [top10Data, timelineData, protocolData, volumeData, signatureData, thresholdData] = await Promise.all([
      fetchWithTimeout(`${API_BASE}/threats/top10`).then(res => res.top10 || []),
      fetchWithTimeout(`${API_BASE}/threats/timeline`).then(res => res.timeline || []),
      fetchWithTimeout(`${API_BASE}/threats/by-protocol`).then(res => res.by_protocol || {}),
      fetchWithTimeout(`${API_BASE}/threats/volume-alerts`).then(res => res.volume_alerts || []),
      fetchWithTimeout(`${API_BASE}/threats/recent`).then(res => res.recent || []),
      fetchWithTimeout(`${API_BASE}/threats/threshold`)
    ]);

    // Store current data for export
    currentData = {
      top10: top10Data,
      timeline: timelineData,
      protocol: protocolData,
      volume: volumeData,
      signatures: signatureData,
      threshold: thresholdData,
      exported_at: new Date().toISOString()
    };

    consecutiveErrors = 0;
    setStatus(true);
    hideError();

    updateTop10Chart(top10Data);
    updateTimelineChart(timelineData);
    updateProtocolChart(protocolData);
    updateVolumeTable(volumeData);
    updateSignatureFeed(signatureData);
    updateTable(top10Data);
    updateKPIs(top10Data, timelineData, thresholdData, volumeData);

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