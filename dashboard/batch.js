/* ============================================================
   RAPID Dashboard — batch.js
   Polls the Batch/HBase API endpoints separately from streaming.
   Base URL: http://100.73.216.115:5000
   Batch endpoints prefix: /batch/
   ============================================================ */

const API_BASE = 'http://100.73.216.115:5000';
const POLL_MS  = 120000; // Batch/HBase is historical and slower than streaming.
const BATCH_TIMEOUT_MS = 25000;

const CHART_COLOR = {
  accent:     '#8f7cff',
  critical:   '#ff3d71',
  high:       '#ff8a3d',
  medium:     '#ffd166',
  info:       '#35d0ff',
  success:    '#2ee59d',
  neutral:    '#6f7da8',
  grid:       'rgba(120, 139, 190, 0.16)',
  surface:    '#0b0f1a',
  accent2:    '#ff3d71',
  accent3:    '#35d0ff',
  green:      '#2ee59d',
  yellow:     '#ffd166',
  orange:     '#ff8a3d',
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
let detailContextSeq = 0;
const detailContexts = new Map();
let errorDismissed = false;
window.errorDismissed = false;

// ── Chart.js defaults ────────────────────────────────────────
Chart.defaults.color           = '#8c9cc6';
Chart.defaults.borderColor     = CHART_COLOR.grid;
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
  if (errorDismissed || window.errorDismissed) return;
  const banner = document.getElementById('error-banner');
  document.getElementById('error-msg').textContent = msg;
  banner.classList.remove('dismissed');
  banner.style.display = '';
  banner.hidden = false;
}
function hideError() {
  const banner = document.getElementById('error-banner');
  if (!banner) return;
  banner.hidden = true;
  banner.classList.add('dismissed');
  banner.style.display = 'none';
}
function dismissError() {
  errorDismissed = true;
  window.errorDismissed = true;
  hideError();
}
window.dismissError = dismissError;

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
    if (Number.isNaN(d.getTime())) return String(ts);
    const date = d.toLocaleDateString('en-GB', { month: 'short', day: '2-digit' });
    const time = d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
    return time === '00:00' ? date : `${date} ${time}`;
  } catch { return ts; }
}

function fmtShortDate(ts) {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return String(ts).split('|')[0];
    return d.toLocaleDateString('en-GB', { month: 'short', day: '2-digit' });
  } catch {
    return String(ts).split('|')[0];
  }
}

function fmtNumber(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return value ?? '—';
  return new Intl.NumberFormat('en-GB', {
    maximumFractionDigits: 0,
  }).format(n);
}

function fmtBytes(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return value ?? '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = Math.abs(n);
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  const signed = n < 0 ? -size : size;
  return `${signed.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function normalizeText(value) {
  return String(value || 'unknown').trim().toLowerCase();
}

function threatLabel(d) {
  return firstValue(d.threat_label, d.label, d.severity, d.category, d.type, 'unknown');
}

function isBenign(d) {
  return normalizeText(threatLabel(d)).includes('benign');
}

function timelineTime(d) {
  const keyTime = String(d.row_key || '').split('|')[0];
  return firstValue(d.timestamp, d.time, d.date, d.hour, d.heure, d.jour, keyTime);
}

function timelineCount(d) {
  return asNumber(firstValue(d.event_count, d.count, d.attack_count, d.total, d.threat_count), 0);
}

function threatColor(label, alpha = 1) {
  const t = normalizeText(label);
  const color = t.includes('malicious') || t.includes('critical')
    ? [255, 61, 113]
    : t.includes('suspicious') || t.includes('high')
      ? [255, 138, 61]
      : t.includes('benign') || t.includes('low')
        ? [46, 229, 157]
        : t.includes('medium')
          ? [255, 209, 102]
          : [53, 208, 255];
  return `rgba(${color[0]},${color[1]},${color[2]},${alpha})`;
}

function patternColor(label, index, alpha = 0.86) {
  const t = normalizeText(label);
  if (t.includes('sql')) return `rgba(255,61,113,${alpha})`;
  if (t.includes('xss')) return `rgba(255,138,61,${alpha})`;
  if (t.includes('traversal') || t.includes('path')) return `rgba(255,138,61,${alpha})`;
  if (t.includes('tool')) return `rgba(143,124,255,${alpha})`;
  if (t.includes('brute')) return `rgba(255,209,102,${alpha})`;
  if (t.includes('scan')) return `rgba(46,229,157,${alpha})`;
  if (t.includes('dos') || t.includes('ddos')) return `rgba(143,124,255,${alpha})`;
  const palette = [
    [143, 124, 255], [53, 208, 255], [255, 61, 113],
    [255, 209, 102], [46, 229, 157], [255, 138, 61],
  ];
  const c = palette[index % palette.length];
  return `rgba(${c[0]},${c[1]},${c[2]},${alpha})`;
}

function setSummary(id, items) {
  const el = document.getElementById(id);
  if (!el) return;
  el.innerHTML = items
    .filter(item => item.value !== undefined && item.value !== null && item.value !== '')
    .map(item => `<span class="summary-pill ${item.cls || ''}"><b>${item.label}</b>${item.value}</span>`)
    .join('');
}

function safeRender(label, fn) {
  try {
    fn();
  } catch (err) {
    console.error(`[RAPID Batch] ${label} render failed:`, err);
  }
}

function endpointError(label, err) {
  return { error: err.message || String(err), label };
}

async function fetchBatch(path, timeoutMs = BATCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(`${API_BASE}${path}`, { signal: controller.signal });
    clearTimeout(timer);
    if (!res.ok) {
      let detail = `HTTP ${res.status}`;
      try {
        const body = await res.json();
        detail = body?.error || detail;
      } catch {}
      throw new Error(detail);
    }
    return await res.json();
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}

async function fetchBatchEndpoint(label, path, timeoutMs = BATCH_TIMEOUT_MS) {
  try {
    return await fetchBatch(path, timeoutMs);
  } catch (err) {
    return endpointError(label, err);
  }
}

function severity(score) {
  if (score >= 80) return { label: 'CRITICAL', cls: 'badge-critical' };
  if (score >= 55) return { label: 'HIGH',     cls: 'badge-high' };
  if (score >= 30) return { label: 'MEDIUM',   cls: 'badge-medium' };
  return               { label: 'LOW',      cls: 'badge-low' };
}

function asNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function firstValue(...values) {
  return values.find(v => v !== undefined && v !== null && v !== '');
}

function escapeAttr(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function findIpInValue(value) {
  if (value === undefined || value === null) return null;
  const match = String(value).match(/\b(?:\d{1,3}\.){3}\d{1,3}\b/);
  return match ? match[0] : null;
}

function ipFromRow(d) {
  const direct = firstValue(d.ip, d.source_ip, d.src_ip, d.ip_address, d.source, d.client_ip);
  const directIp = findIpInValue(direct);
  if (directIp) return directIp;

  const columnIp = findIpInValue(firstValue(
    d.columns?.['cf:source_ip'],
    d.columns?.source_ip,
    d.columns?.['cf:ip'],
    d.columns?.ip
  ));
  if (columnIp) return columnIp;

  return findIpInValue(d.row_key) || null;
}

function sourceLabel(d) {
  const ip = ipFromRow(d);
  if (ip) return { label: ip, ip, sources: [{ ip, count: null }], aggregate: false };

  const topSources = firstValue(d.top_source_ips, d.top_sources, d.source_ips);
  if (Array.isArray(topSources) && topSources.length) {
    const sources = topSources.map(item => parseSourceIpItem(item)).filter(Boolean);
    return { label: topSources.slice(0, 3).join(', '), ip: sources[0]?.ip || null, sources, aggregate: true };
  }
  if (typeof topSources === 'string' && topSources.trim()) {
    const sources = parseSourceIpList(topSources);
    return { label: topSources, ip: sources[0]?.ip || findIpInValue(topSources), sources, aggregate: true };
  }

  const distinct = firstValue(d.distinct_ips, d.unique_ips);
  if (distinct !== undefined) return { label: `Aggregate across ${distinct} IPs`, ip: null, sources: [], aggregate: true };

  return { label: 'Aggregate pattern', ip: null, sources: [], aggregate: true };
}

function parseSourceIpItem(value) {
  const text = String(value || '').trim();
  const ip = findIpInValue(text);
  if (!ip) return null;
  const countMatch = text.match(/\(([\d,]+)\)/);
  return {
    ip,
    count: countMatch ? asNumber(countMatch[1].replaceAll(',', ''), null) : null,
  };
}

function parseSourceIpList(value) {
  return String(value || '')
    .split(',')
    .map(parseSourceIpItem)
    .filter(Boolean)
    .sort((a, b) => asNumber(b.count, 0) - asNumber(a.count, 0));
}

function renderSourceList(source) {
  if (source.sources?.length) {
    return `<div class="source-ip-list">
      ${source.sources.slice(0, 5).map((item, index) => `
        <div class="source-ip-row">
          <span class="source-ip">${item.ip}</span>
          ${item.count !== null ? `<span class="source-count">(${fmtNumber(item.count)} events)</span>` : ''}
        </div>
      `).join('')}
    </div>`;
  }
  return `<span class="source-fallback">${source.label}</span>`;
}

function renderPatternType(d) {
  const type = firstValue(d.pattern_type, d.attack_type, d.pattern, 'Unknown');
  const label = firstValue(d.threat_label, d.label, d.category);
  return `<div class="pattern-type-cell">
    <span class="pattern-name">${attackTypeDisplay(type)}</span>
    ${label ? `<span class="pattern-class">${label}</span>` : ''}
  </div>`;
}

function reputationScore(d) {
  return reputationDecision(d).score;
}

function reputationDecision(d) {
  const raw = asNumber(firstValue(d.reputation_score, d.score, d.threat_score), 0);
  const attacks = asNumber(firstValue(d.attack_count, d.total_attacks, d.threat_count, d.total_events), 0);
  const sqli = asNumber(d.sqli_hits, 0);
  const xss = asNumber(d.xss_hits, 0);
  const traversal = asNumber(d.traversal_hits, 0);
  const tool = asNumber(d.tool_hits, 0);
  const exploitHits = sqli + xss + traversal;
  const rawComponent = Math.min(22, raw * 0.22);
  const exploitComponent = exploitHits > 0 ? Math.min(38, 15 + Math.log10(exploitHits + 1) * 9) : 0;
  const toolComponent = tool > 0 ? Math.min(14, Math.log10(tool + 1) * 5) : 0;
  const volumeComponent = attacks > 0 ? Math.min(26, Math.log10(attacks + 1) * 6) : 0;
  const score = Math.min(100, rawComponent + exploitComponent + toolComponent + volumeComponent);
  return {
    score: Math.round(score * 10) / 10,
    raw,
    attacks,
    exploitHits,
    tool,
    reason: exploitHits > 0
      ? 'exploit activity and event volume'
      : tool > 0
        ? 'automated scanning and event volume'
        : attacks > 0
          ? 'event volume'
          : 'stored reputation only',
  };
}

function attackCount(d) {
  return firstValue(d.attack_count, d.total_attacks, d.threat_count, d.total_events, '—');
}

function riskLabel(d) {
  return firstValue(d.risk_level, d.severity, severity(reputationScore(d)).label);
}

function scanPortCount(d) {
  return asNumber(firstValue(d.distinct_ports, d.scan_count, d.count, d.total_connections), 0);
}

function scanConnectionCount(d) {
  return firstValue(d.total_connections, d.scan_count, d.count, '—');
}

function scanPortsLabel(d, maxPorts = 6) {
  if (Array.isArray(d.target_ports)) {
    return d.target_ports.slice(0, maxPorts).join(', ') + (d.target_ports.length > maxPorts ? '...' : '');
  }
  if (d.port) return d.port;
  if (d.distinct_ports !== undefined) return `${d.distinct_ports} distinct`;
  return 'Unknown';
}

function scanTypeLabel(d) {
  return firstValue(d.scan_type, d.type, d.distinct_ports !== undefined ? 'Port sweep' : '—');
}

function scanTimestamp(d) {
  return firstValue(d.timestamp, d.first_seen, d.window_start, d.window_end);
}

function volumeBytes(d) {
  return asNumber(firstValue(d.total_bytes, d.volume, d.bytes, d.count, d.total, d.threat_count), 0);
}

function volumeLabel(d) {
  return firstValue(d.threat_label, d.protocol, d.timestamp, d.time, d.date, d.window_start, d.row_key, 'Unknown volume bucket');
}

function portScanScore(d) {
  const explicit = firstValue(d.reputation_score, d.threat_score, d.score);
  if (explicit !== undefined) return asNumber(explicit, 0);
  return portScanDecision(d).score;
}

function portScanDecision(d) {
  const ports = scanPortCount(d);
  const connections = asNumber(firstValue(d.total_connections, d.count), 0);
  const rows = asNumber(d.rows, 1);
  const portComponent = Math.min(48, ports * 2.1);
  const connectionComponent = Math.min(34, Math.log10(connections + 1) * 14);
  const repeatComponent = rows > 1 ? Math.min(18, Math.log10(rows + 1) * 18) : 0;
  const score = Math.min(100, portComponent + connectionComponent + repeatComponent);
  return {
    score: Math.round(score * 10) / 10,
    ports,
    connections,
    rows,
    reason: rows > 1
      ? 'repeated scans, broad port coverage, and connection volume'
      : 'broad port coverage and connection volume',
  };
}

function attackPatternScore(d) {
  const explicit = firstValue(d.reputation_score, d.threat_score, d.score);
  if (explicit !== undefined) return asNumber(explicit, 0);
  return attackPatternDecision(d).score;
}

function attackPatternType(d) {
  return firstValue(d.pattern_type, d.attack_type, d.pattern, 'Unknown');
}

function attackPatternCount(d) {
  return asNumber(firstValue(d.occurrences, d.count, d.total), 0);
}

function threatPriority(d) {
  const label = normalizeText(threatLabel(d));
  if (label.includes('malicious') || label.includes('critical')) return 45;
  if (label.includes('suspicious') || label.includes('high')) return 28;
  if (label.includes('medium')) return 16;
  if (label.includes('benign') || label.includes('low')) return 0;
  return 10;
}

function attackTypePriority(type) {
  const t = normalizeText(type);
  if (t.includes('sqli') || t.includes('sql')) return 35;
  if (t.includes('traversal') || t.includes('path')) return 30;
  if (t.includes('xss')) return 28;
  if (t.includes('brute')) return 26;
  if (t.includes('dos') || t.includes('ddos')) return 24;
  if (t.includes('tool') || t.includes('scan')) return 16;
  return 10;
}

function attackTypeDisplay(type) {
  const t = normalizeText(type);
  if (t.includes('sqli') || t.includes('sql')) return 'SQL Injection';
  if (t.includes('traversal') || t.includes('path')) return 'Path Traversal';
  if (t.includes('xss')) return 'Cross-Site Scripting';
  if (t.includes('tool')) return 'Automated Tool Scan';
  return type || 'Unknown';
}

function attackPatternDecision(d) {
  const count = attackPatternCount(d);
  const typeScore = attackTypePriority(attackPatternType(d));
  const labelScore = threatPriority(d);
  const volumeScore = Math.min(20, Math.log10(Math.max(count, 1)) * 3.2);
  if (isBenign(d)) {
    return { score: Math.min(20, Math.round(volumeScore * 10) / 10), labelScore, typeScore, volumeScore, count };
  }
  const score = Math.min(100, Math.round((labelScore + typeScore + volumeScore) * 10) / 10);
  return { score, labelScore, typeScore, volumeScore, count };
}

function contextScore(context) {
  if (!context) return 0;
  if (context.context_type === 'port_scan') return portScanScore(context);
  if (context.context_type === 'attack_pattern') return attackPatternScore(context);
  return reputationScore(context);
}

function contextEventCount(context) {
  if (!context) return undefined;
  if (context.context_type === 'port_scan') {
    return firstValue(context.total_connections, context.scan_count, context.count);
  }
  if (context.context_type === 'attack_pattern') {
    return firstValue(context.occurrences, context.count, context.total);
  }
  return firstValue(context.total_attacks, context.attack_count, context.threat_count, context.total_events);
}

function recommendationForRisk(risk) {
  const level = String(risk || '').toUpperCase();
  if (level === 'CRITICAL') return 'Block source and investigate related traffic';
  if (level === 'HIGH') return 'Block or quarantine, then review recent activity';
  if (level === 'MEDIUM') return 'Monitor closely and verify exposed services';
  return 'Monitor; no immediate block unless activity increases';
}

function cacheDetailContext(row, type) {
  const key = `${type}-${++detailContextSeq}`;
  detailContexts.set(key, { ...row, context_type: type });
  return key;
}

function aggregatePortScanSources(data) {
  const arr = data?.top_scanners || data?.port_scans || data || [];
  const byIp = new Map();
  arr.forEach(d => {
    const ip = ipFromRow(d);
    if (!ip || ip === '?') return;
    const current = byIp.get(ip) || {
      ip,
      distinct_ports: 0,
      total_connections: 0,
      rows: 0,
      latest: null,
    };
    current.distinct_ports = Math.max(current.distinct_ports, scanPortCount(d));
    current.total_connections += asNumber(scanConnectionCount(d), scanPortCount(d));
    current.rows += 1;
    current.latest = firstValue(scanTimestamp(d), current.latest);
    byIp.set(ip, current);
  });
  return [...byIp.values()].sort((a, b) =>
    portScanScore(b) - portScanScore(a)
    || scanPortCount(b) - scanPortCount(a)
    || asNumber(b.total_connections, 0) - asNumber(a.total_connections, 0)
  );
}

function multiStepScore(d) {
  const explicit = firstValue(d.score, d.total_score, d.reputation_score);
  if (explicit !== undefined) return asNumber(explicit, 0);
  return multiStepDecision(d).score;
}

function multiStepDecision(d) {
  const risk = String(d.risk_level || '').toUpperCase();
  const riskBase = risk === 'CRITICAL' ? 45 : risk === 'HIGH' ? 34 : risk === 'MEDIUM' ? 24 : risk === 'LOW' ? 10 : 18;
  const steps = multiStepSteps(d);
  const completeChain = ['toolscan', 'sqli', 'pathtraversal']
    .every(step => steps.map(normalizeStepType).includes(step));
  const chainComponent = completeChain ? 25 : Math.min(18, steps.length * 6);
  const malicious = asNumber(d.malicious_count, 0);
  const events = asNumber(d.total_events, 0);
  const maliciousComponent = malicious > 0 ? Math.min(20, Math.log10(malicious + 1) * 7) : 0;
  const volumeComponent = events > 0 ? Math.min(10, Math.log10(events + 1) * 2.5) : 0;
  const score = Math.min(100, riskBase + chainComponent + maliciousComponent + volumeComponent);
  return {
    score: Math.round(score * 10) / 10,
    risk,
    completeChain,
    steps,
    malicious,
    events,
  };
}

function multiStepSteps(d) {
  const explicitChain = firstValue(d.attack_chain, d.pattern);
  if (typeof explicitChain === 'string' && explicitChain.includes('->')) {
    return orderMultiStepTypes(explicitChain.split('->').map(s => s.trim()));
  }

  const steps = firstValue(d.ordered_steps, d.steps, d.attack_steps, d.attack_types);
  if (Array.isArray(steps)) return orderMultiStepTypes(steps);
  if (typeof steps === 'string' && steps.trim()) {
    const split = steps
      .replace(/^\[|\]$/g, '')
      .split(/,|->/)
      .map(s => s.replace(/^['"\s]+|['"\s]+$/g, ''))
      .filter(Boolean);
    return orderMultiStepTypes(split.length ? split : [steps]);
  }
  return [];
}

function multiStepPattern(d) {
  const steps = multiStepSteps(d);
  if (steps.length) return steps.join(' -> ');
  const explicit = firstValue(d.pattern, d.attack_chain, d.attack_type);
  return explicit || '—';
}

function orderMultiStepTypes(steps) {
  const order = { toolscan: 1, sqli: 2, sqlinjection: 2, pathtraversal: 3 };
  return [...new Set(steps.map(s => String(s || '').trim()).filter(Boolean))]
    .sort((a, b) => (order[normalizeStepType(a)] || 99) - (order[normalizeStepType(b)] || 99));
}

function normalizeStepType(step) {
  return normalizeText(step).replace(/[^a-z0-9]/g, '');
}

function unwrapSingleRow(data, key) {
  const row = data?.[key];
  return row ? [row] : [];
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

async function searchBatchIP(context = null) {
  const ip = document.getElementById('ip-search').value.trim();
  if (!ip) return;

  try {
    const [repData, msData, psData] = await Promise.all([
      fetchBatch(`/batch/ip-reputation/${ip}`).catch(() => null),
      fetchBatch(`/batch/multistep-attacks/ip/${ip}`).catch(() => null),
      fetchBatch(`/batch/port-scans/ip/${ip}?limit=1000`).catch(() => null),
    ]);

    showBatchIPModal(ip, repData, msData, psData, context);
  } catch (err) {
    showError(`Failed to query HBase for IP ${ip}: ${err.message}`);
  }
}

function showBatchIPModal(ip, repData, msData, psData, context = null) {
  document.getElementById('modal-ip').textContent = `HBase: ${ip}`;

  const rep     = repData?.reputation || repData || {};
  const hasRep  = repData?.status !== 'not_found' && Object.keys(rep).length > 0;
  const repScore = hasRep ? reputationScore(rep) : 0;
  const ctxScore = contextScore(context);
  const score   = Math.max(repScore, ctxScore);
  const repAttacks = firstValue(rep.attack_count, rep.total_attacks, rep.threat_count, rep.total_events);
  const attacks = firstValue(repAttacks, contextEventCount(context), '—');
  const risk    = severity(score).label;
  const action  = recommendationForRisk(risk);

  document.getElementById('modal-rep-score').textContent   = typeof score === 'number' ? score.toFixed(1) : score;
  document.getElementById('modal-total-attacks').textContent = typeof attacks === 'number' ? fmtNumber(attacks) : attacks;
  document.getElementById('modal-risk-level').textContent  = `${risk || '—'} · ${action}`;
  document.getElementById('modal-risk-level').className    = 'detail-value ' + getRiskClass(risk);

  const fetchedPortScans = psData?.port_scans || psData?.scans || [];
  const psItems = fetchedPortScans.length ? fetchedPortScans : (context?.context_type === 'port_scan' ? [context] : []);
  document.getElementById('modal-port-scans').textContent = psItems.length;

  // Multi-step
  const msItems = msData?.attacks || msData?.multistep || unwrapSingleRow(msData, 'multistep_attack');
  const msBody  = document.getElementById('modal-multistep-body');
  if (msItems.length > 0) {
    msBody.innerHTML = msItems.slice(0, 5).map(m => `
      <div class="alert-item">
        <div class="alert-header">
          <span class="alert-ip">${multiStepPattern(m)}</span>
          <span class="alert-time">${riskLabel(m)}</span>
        </div>
        <div class="alert-reason">Steps: ${firstValue(m.step_count, multiStepSteps(m).length, '—')} · Score: ${multiStepScore(m).toFixed(1)}</div>
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
          <span class="alert-ip">${scanPortsLabel(p, 5)}</span>
          <span class="alert-time">${fmtDate(scanTimestamp(p))}</span>
        </div>
        <div class="alert-reason">Type: ${scanTypeLabel(p)} · Connections: ${scanConnectionCount(p)}</div>
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
  const kpiModal = document.getElementById('kpi-modal');
  if (kpiModal && !kpiModal.hidden && e.target === kpiModal) closeKpiModal();
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
  initKpiCards();
}

function initKpiCards() {
  const config = {
    'kpi-patterns': 'patterns',
    'kpi-reputation': 'reputation',
    'kpi-portscans': 'portscans',
    'kpi-multistep': 'multistep',
    'kpi-volume': 'volume',
    'kpi-tables': 'tables',
  };
  Object.entries(config).forEach(([id, type]) => {
    const el = document.getElementById(id);
    if (!el || el.dataset.bound === 'true') return;
    el.dataset.bound = 'true';
    el.tabIndex = 0;
    el.role = 'button';
    el.title = 'Click for decision summary';
    el.addEventListener('click', () => showKpiInsight(type));
    el.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        showKpiInsight(type);
      }
    });
  });
}

function showKpiInsight(type) {
  const insight = kpiInsight(type);
  document.getElementById('kpi-modal-title').textContent = insight.title;
  document.getElementById('kpi-modal-label').textContent = insight.label;
  document.getElementById('kpi-modal-value').textContent = insight.value;
  document.getElementById('kpi-modal-meaning').textContent = insight.meaning;
  document.getElementById('kpi-modal-decision').textContent = insight.decision;
  const badge = document.getElementById('kpi-modal-badge');
  badge.textContent = insight.badge || 'SUMMARY';
  badge.className = `badge ${insight.badgeClass || 'badge-info'}`;
  document.getElementById('kpi-modal-evidence').innerHTML = insight.evidence
    .map(item => `<div class="kpi-evidence-item"><span>${item.label}:</span><strong>${item.value}</strong></div>`)
    .join('');
  document.getElementById('kpi-modal').hidden = false;
}

function closeKpiModal() { document.getElementById('kpi-modal').hidden = true; }
window.closeKpiModal = closeKpiModal;

function kpiInsight(type) {
  const data = currentData || {};
  if (type === 'patterns') {
    const ranked = [...(data.patterns || [])].sort((a, b) => attackPatternDecision(b).score - attackPatternDecision(a).score);
    const top = ranked[0];
    const score = top ? attackPatternScore(top) : 0;
    return {
      title: 'Attack Patterns',
      label: 'Pattern groups',
      value: fmtNumber((data.patterns || []).length),
      badge: severity(score).label,
      badgeClass: severity(score).cls,
      meaning: top
        ? `${attackTypeDisplay(attackPatternType(top))} is currently the most decision-relevant pattern, ranked by threat label, attack impact, and event volume.`
        : 'No attack pattern rows are available from HBase right now.',
      decision: top ? recommendationForRisk(severity(score).label) : 'No action is required until attack-pattern data is available.',
      evidence: [
        { label: 'Top pattern', value: top ? attackTypeDisplay(attackPatternType(top)) : '—' },
        { label: 'Threat label', value: top ? threatLabel(top) : '—' },
        { label: 'Detected events', value: top ? fmtNumber(attackPatternCount(top)) : '—' },
      ],
    };
  }
  if (type === 'reputation') {
    const ranked = [...(data.reputation || [])].sort((a, b) => reputationScore(b) - reputationScore(a));
    const top = ranked[0];
    const score = top ? reputationScore(top) : 0;
    return {
      title: 'Flagged IPs',
      label: 'Tracked source IPs',
      value: fmtNumber((data.reputation || []).length),
      badge: severity(score).label,
      badgeClass: severity(score).cls,
      meaning: top
        ? `${ipFromRow(top)} has the highest decision score after combining reputation, exploit hits, and event volume.`
        : 'No IP reputation rows are available from HBase right now.',
      decision: top ? recommendationForRisk(severity(score).label) : 'No action is required until reputation data is available.',
      evidence: [
        { label: 'Highest risk IP', value: top ? ipFromRow(top) : '—' },
        { label: 'Decision score', value: top ? score.toFixed(1) : '—' },
        { label: 'Evidence events', value: top ? fmtNumber(attackCount(top)) : '—' },
      ],
    };
  }
  if (type === 'portscans') {
    const ranked = aggregatePortScanSources(data.portscans || []);
    const top = ranked[0];
    const score = top ? portScanScore(top) : 0;
    const decision = top ? portScanDecision(top) : null;
    return {
      title: 'Port Scan Events',
      label: 'Recorded scan rows',
      value: fmtNumber((data.portscans || []).length),
      badge: severity(score).label,
      badgeClass: severity(score).cls,
      meaning: top
        ? `${top.ip} is the highest-risk scanner after aggregating raw scan records by source IP. The score considers distinct ports, total connections, and repeated scan rows.`
        : 'No port scan rows are available from HBase right now.',
      decision: top ? recommendationForRisk(severity(score).label) : 'No port-scan action is required until scan data is available.',
      evidence: [
        { label: 'Top scanner', value: top ? top.ip : '—' },
        { label: 'Decision score', value: top ? score.toFixed(1) : '—' },
        { label: 'Distinct ports', value: decision ? fmtNumber(decision.ports) : '—' },
        { label: 'Total connections', value: decision ? fmtNumber(decision.connections) : '—' },
        { label: 'Raw records', value: decision ? fmtNumber(decision.rows) : '—' },
      ],
    };
  }
  if (type === 'multistep') {
    const ranked = [...(data.multistep || [])].sort((a, b) => multiStepScore(b) - multiStepScore(a));
    const top = ranked[0];
    const score = top ? multiStepScore(top) : 0;
    return {
      title: 'Multi-Step Attacks',
      label: 'Attack chains',
      value: fmtNumber((data.multistep || []).length),
      badge: severity(score).label,
      badgeClass: severity(score).cls,
      meaning: top
        ? `${ipFromRow(top) || 'Unknown source'} appears in the strongest multi-step chain: ${multiStepPattern(top)}.`
        : 'No multi-step attack chains are available from HBase right now.',
      decision: top ? recommendationForRisk(severity(score).label) : 'No chain response is required until multi-step data is available.',
      evidence: [
        { label: 'Top source', value: top ? (ipFromRow(top) || 'Unknown source') : '—' },
        { label: 'Chain pattern', value: top ? multiStepPattern(top) : '—' },
        { label: 'Decision score', value: top ? score.toFixed(1) : '—' },
      ],
    };
  }
  if (type === 'volume') {
    const rows = data.volume || [];
    const ranked = [...rows].sort((a, b) => volumeBytes(b) - volumeBytes(a));
    const top = ranked[0];
    return {
      title: 'Threat Volume',
      label: 'Volume buckets',
      value: fmtNumber(rows.length),
      badge: top && !isBenign(top) ? 'REVIEW' : 'CONTEXT',
      badgeClass: top && !isBenign(top) ? 'badge-high' : 'badge-info',
      meaning: top
        ? `${volumeLabel(top)} is the largest traffic bucket. Volume alone is not a block reason; use it to prioritize correlation with IPs and patterns.`
        : 'No threat volume rows are available from HBase right now.',
      decision: top ? 'Review the largest non-benign traffic bucket and correlate it with attack patterns, reputation, and port scans before taking action.' : 'No volume action is required until volume data is available.',
      evidence: [
        { label: 'Largest bucket', value: top ? volumeLabel(top) : '—' },
        { label: 'Traffic volume', value: top ? fmtBytes(volumeBytes(top)) : '—' },
        { label: 'Benign hidden', value: fmtBytes(rows.filter(isBenign).reduce((sum, d) => sum + volumeBytes(d), 0)) },
      ],
    };
  }
  const tables = data.tablesData?.tables || [];
  const missing = tables.filter(t => t.exists === false).map(t => t.table);
  return {
    title: 'HBase Tables',
    label: 'Configured tables online',
    value: `${tables.filter(t => t.exists !== false).length}/${tables.length || 0}`,
    badge: missing.length ? 'MISSING DATA' : 'READY',
    badgeClass: missing.length ? 'badge-high' : 'badge-info',
    meaning: missing.length
      ? `Some HBase tables are missing, so related dashboard panels may be incomplete: ${missing.join(', ')}.`
      : 'All configured HBase tables are visible. The dashboard can read the batch views it expects.',
    decision: missing.length ? 'Create the missing HBase tables or rerun the batch jobs that populate them.' : 'No table action is needed. Focus decisions on the highest-risk patterns, IPs, chains, and scans.',
    evidence: [
      { label: 'Available tables', value: tables.filter(t => t.exists !== false).map(t => t.table || t.name).join(', ') || '—' },
      { label: 'Missing tables', value: missing.length ? missing.join(', ') : 'None' },
      { label: 'Serving layer', value: 'HBase batch views' },
    ],
  };
}

// ═══════════════════════════════════════════════════════════════
// HBASE TABLES
// ═══════════════════════════════════════════════════════════════

function renderHBaseTables(data) {
  const list = document.getElementById('tables-list');
  if (data?.error) {
    list.innerHTML = '<span class="table-tag error">HBase unavailable</span>';
    return;
  }
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

  chartTimeline = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: []
    },
    options: chartOpts('Events', CHART_COLOR.high, {
      stacked: true,
      showLegend: true,
      xTitle: 'Historical day',
      yTitle: 'Threat events',
      tickFormatter: fmtNumber,
    }),
  });
}

function updateTimelineChart(data) {
  if (!data || data.length === 0) return;
  const arr = data.threat_timeline || data.timeline || data;
  const rawLabels = [...new Set(arr.map(timelineTime))].sort((a, b) => new Date(a) - new Date(b));
  const labels = rawLabels.map(fmtShortDate);
  const grouped = {};

  arr.forEach(d => {
    const kind = String(threatLabel(d));
    grouped[kind] = grouped[kind] || {};
    grouped[kind][timelineTime(d)] = (grouped[kind][timelineTime(d)] || 0) + timelineCount(d);
  });

  let series = Object.keys(grouped)
    .filter(label => !normalizeText(label).includes('benign'))
    .sort();
  if (!series.length) series = Object.keys(grouped).sort();

  chartTimeline.data.labels = labels;
  const dayStats = rawLabels.map(time => {
    const malicious = Object.entries(grouped)
      .filter(([label]) => normalizeText(label).includes('malicious'))
      .reduce((sum, [, values]) => sum + (values[time] || 0), 0);
    const suspicious = Object.entries(grouped)
      .filter(([label]) => normalizeText(label).includes('suspicious'))
      .reduce((sum, [, values]) => sum + (values[time] || 0), 0);
    const total = Object.keys(grouped).reduce((sum, label) => sum + (grouped[label]?.[time] || 0), 0);
    return { time, label: fmtShortDate(time), malicious, suspicious, total, risk: malicious * 3 + suspicious };
  });
  chartTimeline.data.datasets = series.map(label => ({
    label,
    data: rawLabels.map(time => grouped[label]?.[time] || 0),
    backgroundColor: threatColor(label, 0.72),
    borderColor: threatColor(label, 1),
    borderWidth: 1,
    borderRadius: 3,
    barPercentage: 0.78,
    categoryPercentage: 0.72,
  }));
  chartTimeline.options.plugins.tooltip.callbacks = {
    title: items => dayStats[items[0]?.dataIndex]?.label || '',
    label: ctx => `${ctx.dataset.label}: ${fmtNumber(ctx.parsed.y)} events`,
    afterBody: items => {
      const stat = dayStats[items[0]?.dataIndex];
      if (!stat) return [];
      const action = stat.malicious > 0
        ? 'Decision: investigate malicious sources from this day'
        : stat.suspicious > 0
          ? 'Decision: monitor suspicious spike'
          : 'Decision: no threat spike visible';
      return [
        `Total threat events: ${fmtNumber(stat.total)}`,
        `Malicious: ${fmtNumber(stat.malicious)} · Suspicious: ${fmtNumber(stat.suspicious)}`,
        action,
      ];
    },
  };
  chartTimeline.update('active');

  const totals = arr.reduce((acc, d) => {
    const kind = normalizeText(threatLabel(d));
    acc.total += timelineCount(d);
    if (kind.includes('malicious')) acc.malicious += timelineCount(d);
    if (kind.includes('suspicious')) acc.suspicious += timelineCount(d);
    if (kind.includes('benign')) acc.benign += timelineCount(d);
    return acc;
  }, { total: 0, malicious: 0, suspicious: 0, benign: 0 });

  const peak = [...dayStats].sort((a, b) => b.risk - a.risk)[0];
  const latest = dayStats[dayStats.length - 1];
  const previous = dayStats[dayStats.length - 2];
  const trend = latest && previous
    ? latest.risk > previous.risk ? 'rising' : latest.risk < previous.risk ? 'falling' : 'stable'
    : '—';

  setSummary('timeline-summary', [
    { label: 'peak risk day', value: peak ? `${peak.label} (${fmtNumber(peak.malicious)} malicious, ${fmtNumber(peak.suspicious)} suspicious)` : '—', cls: 'critical' },
    { label: 'latest trend', value: trend, cls: trend === 'rising' ? 'high' : 'muted' },
    { label: 'malicious', value: fmtNumber(totals.malicious), cls: 'critical' },
    { label: 'suspicious', value: fmtNumber(totals.suspicious), cls: 'high' },
  ]);
  document.getElementById('timeline-meta').textContent =
    `${rawLabels.length} days · hover for day-level decision`;
}

// ═══════════════════════════════════════════════════════════════
// CHART: Volume
// ═══════════════════════════════════════════════════════════════

function initVolumeChart() {
  const ctx = document.getElementById('chartVolume').getContext('2d');

  chartVolume = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{
        label: 'Volume',
        data: [],
        backgroundColor: [],
        borderColor: [],
        borderWidth: 1,
        borderRadius: 3,
      }]
    },
    options: chartOpts('Bytes', CHART_COLOR.info, {
      xTitle: 'Threat class / protocol',
      yTitle: 'Traffic volume',
      tickFormatter: fmtBytes,
    }),
  });
}

function updateVolumeChart(data) {
  if (!data || data.length === 0) return;
  const source = data.threat_volume || data.volume || data;
  const threatRows = source.filter(d => !isBenign(d));
  const chartRows = threatRows.length ? threatRows : source;
  const hiddenBenign = source
    .filter(isBenign)
    .reduce((sum, d) => sum + volumeBytes(d), 0);
  const arr = chartRows
    .map(d => ({
      label: volumeLabel(d),
      value: volumeBytes(d),
      threshold: asNumber(firstValue(d.threshold, d.limit), 0),
      risk: threatLabel(d),
    }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 12);
  const labels = arr.map(d => String(d.label));
  const values = arr.map(d => d.value);
  const colors = arr.map(d => d.threshold && d.value >= d.threshold ? threatColor('malicious', 0.74) : threatColor(d.label, 0.68));

  chartVolume.data.labels           = labels;
  chartVolume.data.datasets[0].data = values;
  chartVolume.data.datasets[0].backgroundColor = colors;
  chartVolume.data.datasets[0].borderColor = colors.map(c => c.replace(/,0\.\d+\)$/, ',1)'));
  chartVolume.options.plugins.tooltip.callbacks = {
    title: items => labels[items[0]?.dataIndex] || '',
    label: ctx => `Traffic volume: ${fmtBytes(ctx.parsed.y)}`,
    afterBody: items => {
      const row = arr[items[0]?.dataIndex];
      if (!row) return [];
      const lines = [`Category: ${row.risk}`];
      if (row.threshold) lines.push(`Threshold: ${fmtBytes(row.threshold)}`);
      lines.push(normalizeText(row.risk).includes('malicious') || normalizeText(row.risk).includes('suspicious')
        ? 'Decision: correlate with top IPs before blocking'
        : 'Decision: keep as context unless paired with attack patterns');
      return lines;
    },
  };
  chartVolume.update('active');
  const peak = arr[0];
  const threatTotal = arr.reduce((sum, d) => sum + d.value, 0);
  setSummary('volume-summary', [
    { label: 'largest bucket', value: peak ? `${peak.label} · ${fmtBytes(peak.value)}` : '—', cls: 'info' },
    { label: 'shown threat traffic', value: fmtBytes(threatTotal), cls: 'high' },
    { label: 'benign hidden', value: hiddenBenign ? fmtBytes(hiddenBenign) : '', cls: 'muted' },
  ]);
  document.getElementById('volume-meta').textContent =
    `Traffic volume by threat/category · hover for context`;
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
        backgroundColor: [],
        borderColor: CHART_COLOR.surface,
        borderWidth: 2,
        hoverOffset: 6,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '58%',
      plugins: {
        legend: { position: 'right', labels: { color: '#9badcf', font: { size: 10 }, boxWidth: 12, usePointStyle: true } },
        tooltip: {
          backgroundColor: CHART_COLOR.surface, borderColor: CHART_COLOR.accent, borderWidth: 1,
          callbacks: { label: ctx => ` ${ctx.label}: ${fmtNumber(ctx.parsed)}` }
        }
      }
    }
  });
}

function updatePatternsChart(data) {
  if (!data || data.length === 0) return;
  const source = data.filter(d => !isBenign(d));
  const chartRows = source.length ? source : data;
  const hiddenBenign = data.length - chartRows.length;
  const counts = {};
  chartRows.forEach(d => {
    const t = d.pattern_type || d.attack_type || d.pattern || 'Unknown';
    counts[t] = (counts[t] || 0) + asNumber(firstValue(d.occurrences, d.count, d.total), 1);
  });
  const ranked = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  const top = ranked.slice(0, 6);
  const other = ranked.slice(6).reduce((sum, [, v]) => sum + v, 0);
  if (other > 0) top.push(['Other', other]);
  const labels = top.map(([label]) => label);
  const values = top.map(([, value]) => value);

  chartPatterns.data.labels           = labels;
  chartPatterns.data.datasets[0].data = values;
  chartPatterns.data.datasets[0].backgroundColor = labels.map((label, index) => patternColor(label, index));
  chartPatterns.update('active');
  setSummary('patterns-summary', [
    { label: 'dominant', value: labels[0] || '—', cls: 'info' },
    { label: 'detected events', value: fmtNumber(values.reduce((sum, v) => sum + v, 0)), cls: 'muted' },
    { label: 'benign hidden', value: hiddenBenign || '', cls: 'muted' },
  ]);
  document.getElementById('patterns-chart-meta').textContent =
    `Detected events by attack type · top ${labels.length}`;
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
        label: 'Decision Score',
        data: [],
        backgroundColor: [],
        borderColor: [],
        borderWidth: 1,
        borderRadius: 2,
        barPercentage: 0.7,
      }]
    },
    options: {
      ...chartOpts('Score', CHART_COLOR.accent, {
        xMax: 100,
        xTitle: 'Decision score',
        yTitle: 'Source IP',
      }),
      indexAxis: 'y',
    },
  });
}

function updateReputationChart(data) {
  if (!data || data.length === 0) return;
  const sorted = [...data].sort((a, b) =>
    reputationScore(b) - reputationScore(a)
  ).slice(0, 15);

  const labels = sorted.map(ipFromRow);
  const values = sorted.map(reputationScore);
  const colors = values.map(v => {
    if (v >= 80) return 'rgba(255,61,113,0.78)';
    if (v >= 55) return 'rgba(255,138,61,0.72)';
    if (v >= 30) return 'rgba(255,209,102,0.68)';
    return 'rgba(143,124,255,0.62)';
  });

  chartReputation.data.labels                      = labels;
  chartReputation.data.datasets[0].data            = values;
  chartReputation.data.datasets[0].backgroundColor = colors;
  chartReputation.data.datasets[0].borderColor     = colors.map(c => c.replace(/[\d.]+\)$/, '1)'));
  chartReputation.options.plugins.tooltip.callbacks = {
    title: items => labels[items[0]?.dataIndex] || '',
    label: ctx => `Decision score: ${ctx.parsed.x?.toFixed ? ctx.parsed.x.toFixed(1) : ctx.parsed.x}`,
    afterBody: items => {
      const row = sorted[items[0]?.dataIndex];
      if (!row) return [];
      const score = reputationScore(row);
      const decision = reputationDecision(row);
      return [
        `Evidence events: ${fmtNumber(attackCount(row))}`,
        `Main driver: ${decision.reason}`,
        `Risk: ${severity(score).label}`,
        `Decision: ${recommendationForRisk(severity(score).label)}`,
      ];
    },
  };
  chartReputation.update('active');
  document.getElementById('rep-chart-meta').textContent =
    `Top ${sorted.length} IPs · exploit + volume decision score`;
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
        label: 'Decision Score',
        data: [],
        backgroundColor: [],
        borderColor: [],
        borderWidth: 1,
        borderRadius: 3,
      }]
    },
    options: {
      ...chartOpts('Ports', CHART_COLOR.medium, {
        xTitle: 'Port scan decision score',
        yTitle: 'Source IP',
        tickFormatter: fmtNumber,
      }),
      indexAxis: 'y',
    },
  });
}

function updatePortTopChart(data) {
  if (!data || data.length === 0) return;
  const arr = aggregatePortScanSources(data);
  const top = arr.slice(0, 10);
  const labels = top.map(d => d.ip || ipFromRow(d));
  const values = top.map(portScanScore);
  const max = Math.max(...values, 1);
  const colors = values.map(v => v >= max * 0.75 ? threatColor('malicious', 0.76) : v >= max * 0.45 ? threatColor('suspicious', 0.72) : 'rgba(255,209,102,0.66)');

  chartPortTop.data.labels           = labels;
  chartPortTop.data.datasets[0].data = values;
  chartPortTop.data.datasets[0].backgroundColor = colors;
  chartPortTop.data.datasets[0].borderColor = colors.map(c => c.replace(/,0\.\d+\)$/, ',1)'));
  chartPortTop.options.plugins.tooltip.callbacks = {
    title: items => labels[items[0]?.dataIndex] || '',
    label: ctx => `Decision score: ${ctx.parsed.x?.toFixed ? ctx.parsed.x.toFixed(1) : ctx.parsed.x}`,
    afterBody: items => {
      const row = top[items[0]?.dataIndex];
      if (!row) return [];
      const decision = portScanDecision(row);
      return [
        `Distinct ports: ${fmtNumber(decision.ports)}`,
        `Total connections: ${fmtNumber(decision.connections)}`,
        `Raw records: ${fmtNumber(decision.rows)}`,
        `Driver: ${decision.reason}`,
      ];
    },
  };
  chartPortTop.update('active');
  document.getElementById('top-portscan-meta').textContent =
    `Top ${labels.length} aggregated source IPs · decision ranked`;
}

// ═══════════════════════════════════════════════════════════════
// SHARED CHART OPTIONS
// ═══════════════════════════════════════════════════════════════

function chartOpts(label, color, cfg = {}) {
  return {
    responsive: true, maintainAspectRatio: false,
    plugins: {
      legend: {
        display: !!cfg.showLegend,
        position: 'top',
        align: 'end',
        labels: { color: '#9badcf', font: { size: 10 }, boxWidth: 10, usePointStyle: true },
      },
      tooltip: {
        backgroundColor: CHART_COLOR.surface, borderColor: color, borderWidth: 1,
        titleColor: color, bodyColor: '#c8d6f0',
        callbacks: {
          label: ctx => {
            const raw = ctx.parsed.x ?? ctx.parsed.y;
            const value = cfg.tickFormatter ? cfg.tickFormatter(raw) : fmtNumber(raw);
            return ` ${ctx.dataset.label || label}: ${value}`;
          }
        }
      }
    },
    scales: {
      x: {
        stacked: !!cfg.stacked,
        max: cfg.xMax,
        grid: { color: CHART_COLOR.grid, drawTicks: false },
        ticks: {
          color: '#8c9cc6',
          font: { size: 10 },
          maxRotation: 0,
          autoSkip: true,
          callback: function(value) {
            const labelValue = this?.getLabelForValue ? this.getLabelForValue(value) : value;
            return cfg.xTickFormatter ? cfg.xTickFormatter(labelValue) : labelValue;
          },
        },
        title: { display: !!cfg.xTitle, text: cfg.xTitle, color: '#65749d', font: { size: 10 } },
        beginAtZero: true,
      },
      y: {
        stacked: !!cfg.stacked,
        max: cfg.yMax,
        grid: { color: CHART_COLOR.grid, drawTicks: false },
        ticks: {
          color: '#8c9cc6',
          font: { size: 10 },
          callback: function(value) {
            if (this?.type === 'category' && this.getLabelForValue) return this.getLabelForValue(value);
            return cfg.tickFormatter ? cfg.tickFormatter(value) : fmtNumber(value);
          },
        },
        title: { display: !!cfg.yTitle, text: cfg.yTitle, color: '#65749d', font: { size: 10 } },
        beginAtZero: true,
      },
    }
  };
}

// ═══════════════════════════════════════════════════════════════
// TABLE RENDERERS
// ═══════════════════════════════════════════════════════════════

function renderTopPortsTable(data) {
  const tbody = document.getElementById('top-ports-body');
  const arr = aggregatePortScanSources(data);
  if (!arr.length) { tbody.innerHTML = '<tr><td colspan="4" class="table-empty">No data</td></tr>'; return; }

  tbody.innerHTML = arr.slice(0, 10).map((d, i) => {
    const ip    = d.ip || ipFromRow(d);
    const ports = scanPortCount(d);
    const score = portScanScore(d);
    const decision = portScanDecision(d);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td style="color:var(--accent)">${ip || 'Unknown source'}</td>
      <td>${ports.toLocaleString()} <span style="color:var(--text-dim)">ports · ${fmtNumber(decision.connections)} conns · ${fmtNumber(decision.rows)} records</span></td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
    </tr>`;
  }).join('');
  document.getElementById('top-ports-meta').textContent = `${arr.length} source IPs · decision ranked`;
}

function renderReputationOverviewTable(data) {
  const tbody = document.getElementById('reputation-body');
  if (!data || !data.length) { tbody.innerHTML = '<tr><td colspan="4" class="table-empty">No data</td></tr>'; return; }

  const sorted = [...data].sort((a, b) => reputationScore(b) - reputationScore(a));
  tbody.innerHTML = sorted.slice(0, 10).map((d, i) => {
    const ip    = ipFromRow(d);
    const score = reputationScore(d);
    const attacks = attackCount(d);
    const decision = reputationDecision(d);
    const sev = severity(score);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td><div class="ip-decision-cell"><span>${ip || 'Unknown source'}</span><small>${decision.reason} · ${fmtNumber(attacks)} events</small></div></td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
      <td><div class="risk-decision-cell">${riskBadge(score)}<small>${recommendationForRisk(sev.label)}</small></div></td>
    </tr>`;
  }).join('');
  document.getElementById('rep-meta').textContent = `${data.length} IPs · decision score`;
}

function renderReputationFullTable(data) {
  const tbody = document.getElementById('rep-table-body');
  if (!data || !data.length) { tbody.innerHTML = '<tr><td colspan="7" class="table-empty">No reputation data</td></tr>'; return; }

  const sorted = [...data].sort((a, b) => reputationScore(b) - reputationScore(a));
  tbody.innerHTML = sorted.map((d, i) => {
    const ip      = ipFromRow(d);
    const score   = reputationScore(d);
    const attacks = attackCount(d);
    const decision = reputationDecision(d);
    const last    = fmtDate(d.last_seen || d.last_activity || d.timestamp);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td><div class="ip-decision-cell"><span>${ip || 'Unknown source'}</span><small>${decision.reason}</small></div></td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
      <td>${fmtNumber(attacks)}</td>
      <td><div class="risk-decision-cell">${riskBadge(score)}<small>${recommendationForRisk(severity(score).label)}</small></div></td>
      <td style="color:var(--text-dim)">${last}</td>
      <td>${ip ? `<button class="btn btn-small" onclick="queryIPFromTable('${escapeAttr(ip)}')">Details</button>` : '<span style="color:var(--text-dim)">No source IP</span>'}</td>
    </tr>`;
  }).join('');
  document.getElementById('rep-table-meta').textContent = `${data.length} IPs · ${new Date().toLocaleTimeString('en-GB')}`;
}

function renderPatternsTable(data) {
  const tbody = document.getElementById('patterns-table-body');
  if (!data || !data.length) { tbody.innerHTML = '<tr><td colspan="8" class="table-empty">No attack patterns</td></tr>'; return; }

  const sorted = [...data].sort((a, b) => {
    if (isBenign(a) !== isBenign(b)) return isBenign(a) ? 1 : -1;
    const bDecision = attackPatternDecision(b);
    const aDecision = attackPatternDecision(a);
    return bDecision.score - aDecision.score
      || threatPriority(b) - threatPriority(a)
      || attackTypePriority(attackPatternType(b)) - attackTypePriority(attackPatternType(a))
      || bDecision.count - aDecision.count;
  });

  tbody.innerHTML = sorted.map((d, i) => {
    const source = sourceLabel(d);
    const contextKey = cacheDetailContext(d, 'attack_pattern');
    const decision = attackPatternDecision(d);
    const count = decision.count;
    const score = decision.score;
    const last  = fmtDate(d.last_seen || d.timestamp);
    const sev   = severity(score);
    const action = source.ip
      ? `<button class="btn btn-small" onclick="queryIPFromTable('${escapeAttr(source.ip)}','${contextKey}')">Details</button>`
      : '<span style="color:var(--text-dim)">Aggregate row</span>';
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td>${renderSourceList(source)}</td>
      <td>${renderPatternType(d)}</td>
      <td>${fmtNumber(count)}</td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
      <td><span class="badge ${sev.cls}">${sev.label}</span></td>
      <td style="color:var(--text-dim)">${last}</td>
      <td>${action}</td>
    </tr>`;
  }).join('');
  document.getElementById('patterns-full-meta').textContent = `${data.length} patterns · decision ranked`;
}

function renderPortScansTable(data) {
  const tbody = document.getElementById('portscans-table-body');
  const arr = [...(data?.port_scans || data || [])].sort((a, b) =>
    portScanScore(b) - portScanScore(a)
    || scanPortCount(b) - scanPortCount(a)
    || asNumber(scanConnectionCount(b), 0) - asNumber(scanConnectionCount(a), 0)
  );
  if (!arr.length) { tbody.innerHTML = '<tr><td colspan="7" class="table-empty">No port scan records</td></tr>'; return; }

  tbody.innerHTML = arr.map((d, i) => {
    const ip    = ipFromRow(d);
    const contextKey = cacheDetailContext(d, 'port_scan');
    const ports = scanPortsLabel(d);
    const type  = scanTypeLabel(d);
    const count = scanConnectionCount(d);
    const ts    = fmtDate(scanTimestamp(d));
    const score  = portScanScore(d);
    const risk   = severity(score);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td style="color:var(--accent);font-family:var(--font-mono)">${ip || 'Unknown source'}</td>
      <td style="font-size:11px;color:var(--text-dim)">${ports}</td>
      <td>${type}</td>
      <td><div class="risk-decision-cell"><span>${fmtNumber(count)}</span><small>score ${score.toFixed(1)}</small></div></td>
      <td style="color:var(--text-dim)">${ts}</td>
      <td>${ip
        ? `<button class="btn btn-small" title="${risk.label}: ${recommendationForRisk(risk.label)}" onclick="queryIPFromTable('${escapeAttr(ip)}','${contextKey}')">Details</button>`
        : '<span style="color:var(--text-dim)">No source IP</span>'}</td>
    </tr>`;
  }).join('');
  document.getElementById('portscans-table-meta').textContent = `${arr.length} raw records · sorted by scan risk`;
}

function renderPortScansFeed(data) {
  const feed = document.getElementById('portscans-feed');
  const arr  = [...(data?.port_scans || data || [])].sort((a, b) =>
    portScanScore(b) - portScanScore(a)
    || scanPortCount(b) - scanPortCount(a)
    || asNumber(scanConnectionCount(b), 0) - asNumber(scanConnectionCount(a), 0)
  );
  if (!arr.length) { feed.innerHTML = '<div class="alert-empty">No port scan activity</div>'; return; }

  feed.innerHTML = arr.slice(0, 15).map(d => {
    const ip    = ipFromRow(d);
    const ports = scanPortsLabel(d, 4);
    const ts    = fmtDate(scanTimestamp(d));
    const score = portScanScore(d);
    return `<div class="alert-item">
      <div class="alert-header">
        <span class="alert-ip">${ip || 'Unknown source'}</span>
        <span class="alert-time">${ts}</span>
      </div>
      <div class="alert-reason">Ports: ${ports}</div>
      <div class="alert-path">Raw record · Score: ${score.toFixed(1)} · Type: ${scanTypeLabel(d)} · Connections: ${scanConnectionCount(d)}</div>
    </div>`;
  }).join('');
  document.getElementById('portscans-list-meta').textContent = `${arr.length} raw records · sorted by scan risk`;
}

function renderMultiStepChains(data) {
  const container = document.getElementById('multistep-chains');
  if (!data || !data.length) { container.innerHTML = '<div class="alert-empty">No multi-step attack chains found</div>'; return; }

  const sorted = [...data].sort((a, b) => multiStepScore(b) - multiStepScore(a));
  container.innerHTML = sorted.slice(0, 10).map(d => {
    const ip    = ipFromRow(d);
    const score = multiStepScore(d);
    const decision = multiStepDecision(d);
    const steps = multiStepSteps(d);
    const pattern = multiStepPattern(d);
    const sev   = severity(score);
    const severityClass = score >= 80 ? 'chain-critical' : score >= 55 ? 'chain-high' : score >= 30 ? 'chain-medium' : '';

    const stepsHtml = steps.length > 0
      ? steps.map((s, idx) => `
          <span class="chain-step">${typeof s === 'string' ? s : (s.type || s.name || `Step ${idx+1}`)}</span>
          ${idx < steps.length - 1 ? '<span class="chain-arrow">→</span>' : ''}
        `).join('')
      : pattern && pattern !== '—'
        ? `<span class="chain-step">${pattern}</span>`
        : '<span style="color:var(--text-dim);font-size:11px">No step details</span>';

    return `<div class="chain-card ${severityClass}">
      <div class="chain-header">
        <span class="chain-ip">${ip || 'Unknown source'}</span>
        <span class="badge ${sev.cls}">${sev.label}</span>
        <span style="color:var(--text-dim);font-size:11px">${riskLabel(d)}</span>
      </div>
      <div class="chain-steps-flow">${stepsHtml}</div>
      <div class="chain-meta">
        <span>Steps: ${d.step_count ?? steps.length ?? '—'}</span>
        <span>Decision score: ${typeof score==='number'?score.toFixed(1):score}</span>
        <span>Events: ${firstValue(d.total_events, '—')}</span>
        <span>${decision.completeChain ? 'Complete chain' : 'Partial chain'}</span>
      </div>
    </div>`;
  }).join('');
  document.getElementById('multistep-meta').textContent = `${data.length} chains · decision ranked`;
}

function renderMultiStepTable(data) {
  const tbody = document.getElementById('multistep-table-body');
  if (!data || !data.length) { tbody.innerHTML = '<tr><td colspan="7" class="table-empty">No multi-step attacks</td></tr>'; return; }

  const sorted = [...data].sort((a, b) => multiStepScore(b) - multiStepScore(a));
  tbody.innerHTML = sorted.map((d, i) => {
    const ip      = ipFromRow(d);
    const stepList = multiStepSteps(d);
    const steps   = d.step_count ?? stepList.length ?? '—';
    const pattern = multiStepPattern(d);
    const score   = multiStepScore(d);
    const decision = multiStepDecision(d);
    const dur     = firstValue(d.duration, `${firstValue(d.total_events, '—')} events`);
    const sev     = severity(score);
    return `<tr>
      <td style="color:var(--text-dim)">${String(i+1).padStart(2,'0')}</td>
      <td style="color:var(--accent);font-family:var(--font-mono)">${ip || 'Unknown source'}</td>
      <td>${steps}</td>
      <td><div class="risk-decision-cell"><span>${pattern}</span><small>${decision.completeChain ? 'Complete lifecycle chain' : 'Partial chain'} · ${fmtNumber(decision.malicious)} malicious events</small></div></td>
      <td><div class="score-cell"><span>${typeof score==='number'?score.toFixed(1):score}</span>
        <div class="score-bar-track"><div class="score-bar-fill" style="width:${Math.min(100,score)}%;background:${scoreColor(score)}"></div></div>
      </div></td>
      <td style="color:var(--text-dim)">${fmtNumber(dur)}</td>
      <td>${ip ? `<button class="btn btn-small" onclick="queryIPFromTable('${escapeAttr(ip)}')">Details</button>` : '<span style="color:var(--text-dim)">No source IP</span>'}</td>
    </tr>`;
  }).join('');
}

function scoreColor(v) {
  if (v >= 80) return '#ff2d6b';
  if (v >= 55) return '#ff7b2d';
  if (v >= 30) return '#ffd600';
  return '#7b61ff';
}

async function queryIPFromTable(ip, contextKey = null) {
  if (!ip) return;
  document.getElementById('ip-search').value = ip;
  await searchBatchIP(contextKey ? detailContexts.get(contextKey) : null);
}

// ═══════════════════════════════════════════════════════════════
// MAIN POLL LOOP
// ═══════════════════════════════════════════════════════════════

async function poll() {
  if (!isPolling) return;

  try {
    // HBase Thrift is sensitive to parallel scans; keep dashboard reads sequential.
    const tablesData     = await fetchBatchEndpoint('HBase tables', '/batch/hbase/tables', 15000);
    const timelineData   = await fetchBatchEndpoint('Threat timeline', '/batch/threat-timeline?limit=50');
    const volumeData     = await fetchBatchEndpoint('Threat volume', '/batch/threat-volume?limit=10');
    const patternsData   = await fetchBatchEndpoint('Attack patterns', '/batch/attack-patterns?limit=30');
    const reputationData = await fetchBatchEndpoint('IP reputation', '/batch/ip-reputation?limit=30');
    const portTopData    = await fetchBatchEndpoint('Top port scans', '/batch/port-scans/top?limit=10');
    const portScansData  = await fetchBatchEndpoint('Port scans', '/batch/port-scans?limit=30');
    const multistepData  = await fetchBatchEndpoint('Multi-step attacks', '/batch/multistep-attacks?limit=30');

    const endpointErrors = [
      tablesData, patternsData, reputationData, portScansData,
      portTopData, multistepData, timelineData, volumeData,
    ].filter(d => d?.error);

    // Normalize arrays
    const patterns    = patternsData?.attack_patterns  || patternsData?.patterns  || (Array.isArray(patternsData) ? patternsData : []);
    const reputation  = reputationData?.ip_reputation  || reputationData?.ips      || (Array.isArray(reputationData) ? reputationData : []);
    const portscans   = portScansData?.port_scans      || (Array.isArray(portScansData) ? portScansData : []);
    const multistep   = multistepData?.multistep_attacks || multistepData?.attacks || (Array.isArray(multistepData) ? multistepData : []);
    const timeline    = timelineData?.threat_timeline  || timelineData?.timeline  || (Array.isArray(timelineData) ? timelineData : []);
    const volume      = volumeData?.threat_volume      || volumeData?.volume      || (Array.isArray(volumeData) ? volumeData : []);

    // Store for export
    currentData = { tablesData, patterns, reputation, portscans, portTop: portTopData, multistep, timeline, volume };

    const hasUsableData = [patterns, reputation, portscans, multistep, timeline, volume]
      .some(arr => Array.isArray(arr) && arr.length > 0);
    setStatus(endpointErrors.length === 0 || hasUsableData);
    if (endpointErrors.length && !hasUsableData) {
      errorDismissed = false;
      window.errorDismissed = false;
      showError(endpointErrors.map(e => `${e.label}: ${e.error}`).join(' · '));
    } else {
      hideError();
    }

    safeRender('HBase tables', () => renderHBaseTables(tablesData));
    safeRender('KPIs', () => updateKPIs(patterns, reputation, portscans, multistep, volume, tablesData));

    // Render tables before charts so data remains visible even if Chart.js rejects a config.
    safeRender('reputation overview table', () => renderReputationOverviewTable(reputation));
    safeRender('reputation full table', () => renderReputationFullTable(reputation));
    safeRender('top ports table', () => renderTopPortsTable(portTopData));
    safeRender('patterns table', () => renderPatternsTable(patterns));
    safeRender('port scans table', () => renderPortScansTable(portscans));
    safeRender('port scans feed', () => renderPortScansFeed(portscans));
    safeRender('multi-step chains', () => renderMultiStepChains(multistep));
    safeRender('multi-step table', () => renderMultiStepTable(multistep));

    safeRender('timeline chart', () => updateTimelineChart(timeline));
    safeRender('volume chart', () => updateVolumeChart(volume));
    safeRender('patterns chart', () => updatePatternsChart(patterns));
    safeRender('reputation chart', () => updateReputationChart(reputation));
    safeRender('port scan chart', () => updatePortTopChart(portTopData));

    document.getElementById('footer-updated').textContent =
      `Last update: ${new Date().toLocaleTimeString('en-GB')}`;

  } catch (err) {
    setStatus(false);
    console.error('[RAPID Batch] Error:', err);
    errorDismissed = false;
    window.errorDismissed = false;
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

  document.getElementById('error-dismiss')?.addEventListener('click', dismissError);

  // Search on Enter
  document.getElementById('ip-search').addEventListener('keypress', e => {
    if (e.key === 'Enter') searchBatchIP();
  });

  // Outer click closes modal
  document.addEventListener('click', e => {
    const modal = document.getElementById('ip-modal');
    if (!modal.hidden && e.target === modal) closeModal();
    const kpiModal = document.getElementById('kpi-modal');
    if (kpiModal && !kpiModal.hidden && e.target === kpiModal) closeKpiModal();
  });

  // First fetch, then every 30s
  poll();
  pollInterval = setInterval(poll, POLL_MS);
})();
