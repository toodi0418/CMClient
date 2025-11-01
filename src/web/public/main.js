(() => {
  const statusLabel = document.getElementById('status-label');
  const callmeshLabel = document.getElementById('callmesh-label');
  const aprsStatusLabel = document.getElementById('aprs-status-label');
  const counterPackets = document.getElementById('counter-packets');
  const counterAprs = document.getElementById('counter-aprs');
  const counterMapping = document.getElementById('counter-mapping');
  const appVersionLabel = document.getElementById('app-version');

  const callmeshCallsign = document.getElementById('callmesh-callsign');
  const callmeshSymbol = document.getElementById('callmesh-symbol');
  const callmeshCoords = document.getElementById('callmesh-coords');
  const callmeshPhg = document.getElementById('callmesh-phg');
  const callmeshComment = document.getElementById('callmesh-comment');
  const callmeshUpdated = document.getElementById('callmesh-updated');
  const summaryTable = document.getElementById('summary-table');
  const logList = document.getElementById('log-list');

  const summaryRows = [];
  const flowRowMap = new Map();
  const aprsHighlightedFlows = new Set();
  const mappingMeshIds = new Set();
  const flowAprsCallsigns = new Map();
  let currentSelfMeshId = null;
  let selfProvisionCoords = null;
  const MAX_SUMMARY_ROWS = 200;
  const logEntries = [];
  const MAX_LOG_ENTRIES = 200;

  const METERS_PER_FOOT = 0.3048;
  function escapeHtml(input) {
    return String(input)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function setCounter(element, value) {
    if (!element) return;
    element.textContent = Number.isFinite(value) ? value.toLocaleString() : '0';
  }

  function formatNumber(value, digits = 2) {
    if (typeof value !== 'number' || Number.isNaN(value)) return '—';
    return value.toFixed(digits);
  }

  function formatTimestamp(ts) {
    if (!ts) return '—';
    const date = new Date(ts);
    if (Number.isNaN(date.getTime())) return '—';
    return date.toLocaleTimeString();
  }

  function formatRelativeTime(isoString) {
    if (!isoString) return '—';
    const date = new Date(isoString);
    if (Number.isNaN(date.getTime())) return isoString;
    const diff = Date.now() - date.getTime();
    if (diff < 60_000) return '剛剛';
    const minutes = Math.floor(diff / 60_000);
    if (minutes < 60) return `${minutes} 分鐘前`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours} 小時前`;
    const days = Math.floor(hours / 24);
    if (days < 7) return `${days} 天前`;
    return date.toLocaleString();
  }

  function decodePhg(value) {
    if (value == null) return null;
    const str = String(value).trim().toUpperCase();
    if (!/^[0-9]{3,4}$/.test(str)) {
      return null;
    }
    const digits = str.split('').map((ch) => Number.parseInt(ch, 10));
    if (digits.length < 3 || digits.some((digit) => Number.isNaN(digit))) {
      return null;
    }
    const [powerDigit, heightDigit, gainDigit] = digits;
    const powerWatts = powerDigit * powerDigit;
    const heightFeet = 10 * Math.pow(2, heightDigit);
    const heightMeters = heightFeet * METERS_PER_FOOT;
    const gainDb = gainDigit;
    return {
      raw: str,
      powerWatts,
      heightMeters,
      gainDb
    };
  }

  function formatRelay(summary) {
    if (!summary) return '直收';
    const fromMeshId = summary.from?.meshId || summary.from?.meshIdNormalized || '';
    const relayMeshRaw = summary.relay?.meshId || summary.relay?.meshIdNormalized || '';
    const relayNormalized = normalizeMeshId(relayMeshRaw);
    const fromNormalized = normalizeMeshId(fromMeshId);
    const rawRelayCanonical = relayMeshRaw.startsWith('!') ? relayMeshRaw.slice(1) : relayMeshRaw;

    if (fromMeshId && isSelfMesh(fromMeshId, summary)) {
      return 'Self';
    }

    if (relayMeshRaw && isSelfMesh(relayMeshRaw, summary)) {
      return 'Self';
    }

    let relayMeshDisplay = relayMeshRaw;
    let relayNormWork = relayNormalized;
    if (relayMeshRaw && /^0{6}[0-9a-fA-F]{2}$/.test(rawRelayCanonical.toLowerCase())) {
      relayMeshDisplay = '';
      relayNormWork = null;
    }

    if (fromNormalized && relayNormWork && fromNormalized === relayNormWork) {
      return '直收';
    }

    const hopInfo = extractHopInfo(summary);

    if (summary.relay?.label) {
      return formatRelayLabel(summary.relay);
    }

    if (relayMeshDisplay) {
      return formatRelayLabel({ label: summary.relay?.label || relayMeshDisplay, meshId: relayMeshDisplay });
    }

    if (hopInfo.usedHops === 0 || hopInfo.hopsLabel === '0/0' || (hopInfo.hopsLabel && hopInfo.hopsLabel.startsWith('0/'))) {
      return '直收';
    }

    if (hopInfo.usedHops > 0) {
      return '未知?';
    }

    if (!hopInfo.hopsLabel) {
      return '直收';
    }

    if (hopInfo.hopsLabel.includes('?')) {
      return '未知?';
    }

    return '';
  }

  function formatRelayLabel(relay) {
    if (!relay) return '';
    const label = relay.label || '';
    const meshId = relay.meshId || '';
    if (!meshId) return label;
    const stripped = meshId.startsWith('!') ? meshId.slice(1) : meshId;
    if (/^0{6}[0-9a-fA-F]{2}$/.test(stripped)) {
      return label ? `${label}?` : `${meshId}?`;
    }
    return label || meshId;
  }

  function formatHops(hops) {
    if (!hops) return '—';
    if (typeof hops.label === 'string' && hops.label.trim()) {
      return hops.label.trim();
    }
    const info = extractHopInfo({ hops });
    if (info.usedHops != null && info.totalHops != null) {
      return `${info.usedHops}/${info.totalHops}`;
    }
    if (info.totalHops != null) {
      return `${info.totalHops}`;
    }
    return '—';
  }

  function formatChannel(channel) {
    if (channel === null || channel === undefined) return '—';
    const num = Number(channel);
    if (Number.isFinite(num)) return String(num);
    return String(channel);
  }

  function formatSource(summary) {
    if (!summary) return 'unknown';
    const node = summary.from || {};
    const mesh = node.meshId || node.meshIdNormalized || '';
    const meshDisplay = mesh ? mesh.toLowerCase() : '';

    let name = null;
    if (node.longName && node.longName !== 'unknown') {
      name = node.longName;
    } else if (node.shortName && node.shortName !== 'unknown') {
      name = node.shortName;
    } else if (node.label) {
      name = node.label;
    } else if (meshDisplay) {
      name = meshDisplay;
    } else {
      name = 'unknown';
    }

    const nameText = typeof name === 'string' ? name : String(name);

    if (meshDisplay && !nameText.toLowerCase().includes(meshDisplay)) {
      return `${nameText} (${meshDisplay})`;
    }
    return nameText;
  }

  function formatDetail(summary) {
    const detail = summary.detail || '';
    const safeDetail = detail ? escapeHtml(detail) : '';
    const distanceLabel = formatDistance(summary);
    if (distanceLabel) {
      const safeDistance = escapeHtml(distanceLabel);
      if (safeDetail) {
        return `${safeDetail}<br/><span class="detail-distance">${safeDistance}</span>`;
      }
      return `<span class="detail-distance">${safeDistance}</span>`;
    }
    return safeDetail;
  }

  function formatDistance(summary) {
    if (!selfProvisionCoords) return '';
    if (typeof summary.type === 'string' && summary.type.toLowerCase() !== 'position') {
      return '';
    }
    const position = summary.position || {};
    const lat = Number(position.latitude);
    const lon = Number(position.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return '';
    const distanceKm = haversineKm(selfProvisionCoords.lat, selfProvisionCoords.lon, lat, lon);
    if (!Number.isFinite(distanceKm)) return '';
    if (distanceKm < 1) {
      return `距離 ${Math.round(distanceKm * 1000)} m`;
    }
    return `距離 ${distanceKm.toFixed(1)} km`;
  }

  function haversineKm(lat1, lon1, lat2, lon2) {
    const toRad = (deg) => (deg * Math.PI) / 180;
    const R = 6371;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
  }

  function normalizeProvisionSsidValue(value) {
    if (value === null || value === undefined) return null;
    const str = String(value).trim();
    if (!str || /^0+$/.test(str)) {
      return null;
    }
    if (/^[0-9A-Za-z]{1,2}$/.test(str)) {
      return str.toUpperCase();
    }
    const num = Number(str);
    if (Number.isFinite(num) && num > 0) {
      return String(Math.trunc(num));
    }
    return null;
  }

  function resolveProvisionSsid(provision) {
    if (!provision) return null;
    const candidates = [
      provision.aprs_ssid,
      provision.aprsSsid,
      provision.ssid,
      provision.callsign_ssid,
      provision.callsignSsid
    ];
    for (const candidate of candidates) {
      const normalized = normalizeProvisionSsidValue(candidate);
      if (normalized) {
        return normalized;
      }
    }
    return null;
  }

  function formatProvisionCallsign(provision) {
    if (!provision) return '—';
    const baseCandidate =
      provision.callsign_base ??
      provision.callsign ??
      provision.callsignBase ??
      '';
    const base = String(baseCandidate).trim().toUpperCase();
    if (!base) {
      return '—';
    }
    const resolvedSsid = resolveProvisionSsid(provision);
    if (!resolvedSsid) {
      return base;
    }
    const withoutSuffix = base.replace(/-[0-9A-Z]{1,2}$/, '').replace(/-+$/, '');
    return `${withoutSuffix}-${resolvedSsid}`;
  }

  function formatSymbol(provision) {
    if (!provision) return '—';
    const table = provision.symbol_table ?? '';
    const code = provision.symbol_code ?? '';
    const overlay = provision.symbol_overlay ?? '';
    if (overlay && code) {
      return `${overlay}${code}`;
    }
    if (table || code) {
      return `${table}${code}`;
    }
    return '—';
  }

  function formatCoords(provision) {
    if (!provision) return '—';
    const lat = provision.latitude ?? provision.lat ?? null;
    const lon = provision.longitude ?? provision.lon ?? null;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return '—';
    }
    return `${lat.toFixed(5)}, ${lon.toFixed(5)}`;
  }

  function formatPhgDetails(provision) {
    const phgValue = provision?.phg;
    const phgInfo = decodePhg(phgValue);
    if (!phgInfo) return phgValue ? String(phgValue) : '—';
    return `${phgInfo.powerWatts} W / ${phgInfo.heightMeters.toFixed(1)} m / ${phgInfo.gainDb} dB`;
  }

  function normalizeMeshId(meshId) {
    if (meshId == null) return null;
    let value = String(meshId).trim();
    if (!value) return null;
    if (value.startsWith('!')) {
      value = value.slice(1);
    } else if (value.toLowerCase().startsWith('0x')) {
      value = value.slice(2);
    }
    return value.toLowerCase();
  }

  function createSummaryRow(summary) {
    const tr = document.createElement('tr');
    const snrClass =
      typeof summary.snr === 'number'
        ? summary.snr >= 0
          ? 'snr-positive'
          : 'snr-negative'
        : '';
    const timeLabel = escapeHtml(summary.timestampLabel || formatTimestamp(summary.timestamp));
    const relayLabel = escapeHtml(formatRelay(summary));
    const sourceLabel = escapeHtml(formatSource(summary));
    const hopsLabel = escapeHtml(formatHops(summary.hops));
    const typeLabel = escapeHtml(summary.type || '—');
    const channelLabel = escapeHtml(formatChannel(summary.channel));

    tr.innerHTML = `
      <td>${timeLabel}</td>
      <td>${sourceLabel}</td>
      <td>${relayLabel}</td>
      <td>${typeLabel}</td>
      <td>${channelLabel}</td>
      <td>${hopsLabel}</td>
      <td class="${snrClass}">${formatNumber(summary.snr, 2)}</td>
      <td>${formatNumber(summary.rssi, 0)}</td>
      <td class="info-cell">${formatDetail(summary)}</td>
    `;
    return tr;
  }

  function setAprsBadge(row, callsign) {
    if (!row || !callsign) return;
    row.dataset.aprsCallsign = callsign;
    const infoCell = row.querySelector('.info-cell');
    if (!infoCell) return;
    let badge = infoCell.querySelector('.aprs-badge');
    if (!badge) {
      badge = document.createElement('div');
      badge.className = 'aprs-badge';
      infoCell.appendChild(badge);
    }
    badge.textContent = `APRS: ${callsign}`;
  }

  function appendSummary(summary) {
    if (!summary.selfMeshId && currentSelfMeshId) {
      summary.selfMeshId = currentSelfMeshId;
    }

    const row = createSummaryRow(summary);
    row.__summaryData = summary;
    const meshId = normalizeMeshId(summary?.from?.meshId || summary?.from?.meshIdNormalized);
    if (meshId) {
      row.dataset.meshId = meshId;
      if (mappingMeshIds.has(meshId)) {
        row.classList.add('summary-row-mapped');
      }
    }

    const flowId = summary?.flowId;
    if (flowId) {
      row.dataset.flowId = flowId;
      flowRowMap.set(flowId, row);
      if (aprsHighlightedFlows.has(flowId)) {
        row.classList.add('summary-row-aprs');
        aprsHighlightedFlows.delete(flowId);
      }
      const aprsCallsign = flowAprsCallsigns.get(flowId);
      if (aprsCallsign) {
        setAprsBadge(row, aprsCallsign);
      }
    }

    summaryRows.unshift(row);
    summaryTable.insertBefore(row, summaryTable.firstChild);
    while (summaryRows.length > MAX_SUMMARY_ROWS) {
      const last = summaryRows.pop();
      if (last) {
        if (last.dataset?.flowId) {
          flowRowMap.delete(last.dataset.flowId);
          aprsHighlightedFlows.delete(last.dataset.flowId);
          flowAprsCallsigns.delete(last.dataset.flowId);
        }
        if (last.dataset?.meshId) {
          // no-op, kept for symmetry
        }
        if (last.parentNode === summaryTable) {
          summaryTable.removeChild(last);
        }
      }
    }
  }

  function appendLog(entry) {
    if (!entry || !logList) return;
    const li = document.createElement('li');
    li.className = 'log-entry';
    const timestamp = entry.timestamp || new Date().toISOString();
    const formattedTime = formatTimestamp(timestamp);
    const tag = (entry.tag || 'LOG').toUpperCase();
    const message = entry.message || '';
    li.innerHTML = `<span class="time">${formattedTime}</span><span class="tag">[${tag}]</span>${message}`;
    logEntries.unshift(li);
    logList.insertBefore(li, logList.firstChild);
    while (logEntries.length > MAX_LOG_ENTRIES) {
      const last = logEntries.pop();
      if (last && last.parentNode === logList) {
        logList.removeChild(last);
      }
    }
  }

  function markAprsUploaded(info) {
    if (!info || !info.flowId) return;
    const callsign = extractAprsCallsign(info);
    if (callsign) {
      flowAprsCallsigns.set(info.flowId, callsign);
    }
    aprsHighlightedFlows.add(info.flowId);
    const row = flowRowMap.get(info.flowId);
    if (row) {
      row.classList.add('summary-row-aprs');
      aprsHighlightedFlows.delete(info.flowId);
      if (callsign) {
        setAprsBadge(row, callsign);
      }
    }
  }

  function extractAprsCallsign(info) {
    if (!info) return null;
    const frame = info.frame || info.payload || '';
    const match = typeof frame === 'string' ? frame.match(/^([A-Za-z0-9]{1,9}(?:-[0-9A-Z]{1,2})?)[>]/) : null;
    if (match && match[1]) {
      return match[1].toUpperCase();
    }
    return null;
  }

  function isSelfMesh(meshId, summary) {
    if (!meshId) return false;
    const normalized = normalizeMeshId(meshId);
    if (!normalized) return false;
    if (summary?.selfMeshId && normalizeMeshId(summary.selfMeshId) === normalized) {
      return true;
    }
    if (currentSelfMeshId && normalizeMeshId(currentSelfMeshId) === normalized) {
      return true;
    }
    return false;
  }

  function isSameMesh(summary, meshId) {
    if (!meshId) return false;
    const relayNormalized = normalizeMeshId(meshId);
    if (!relayNormalized) return false;
    const selfMeshId = summary.selfMeshId || currentSelfMeshId;
    if (selfMeshId && normalizeMeshId(selfMeshId) === relayNormalized) {
      return true;
    }
    const fromMesh = normalizeMeshId(summary.from?.meshId || summary.from?.meshIdNormalized);
    if (fromMesh && fromMesh === relayNormalized) {
      return true;
    }
    return false;
  }

  function extractHopInfo(summary) {
    const hops = summary.hops || {};
    const hopStart = Number(hops.start);
    const hopLimit = Number(hops.limit);
    const label = typeof hops.label === 'string' ? hops.label.trim() : '';
    let used = null;
    let total = Number.isFinite(hopStart) ? hopStart : null;

    if (Number.isFinite(hopStart) && Number.isFinite(hopLimit)) {
      used = Math.max(hopStart - hopLimit, 0);
    } else {
      const match = label.match(/^(\d+)\s*\/\s*(\d+)/);
      if (match) {
        used = Number(match[1]);
        if (!Number.isFinite(total)) {
          total = Number(match[2]);
        }
      } else if (/^\d+$/.test(label)) {
        used = 0;
      }
    }

    if (!Number.isFinite(total)) {
      const match = label.match(/\/\s*(\d+)/);
      if (match) {
        total = Number(match[1]);
      }
    }

    return {
      usedHops: Number.isFinite(used) ? used : null,
      totalHops: Number.isFinite(total) ? total : null,
      hopsLabel: label
    };
  }

  function updateStatus(info) {
    if (!info || !statusLabel) return;
    const label = info.status || info.state || 'unknown';
    const message = info.message ? ` (${info.message})` : '';
    statusLabel.textContent = `${label}${message}`;
  }

  function updateMetrics(metrics) {
    if (!metrics) return;
    setCounter(counterPackets, metrics.packetLast10Min ?? 0);
    setCounter(counterAprs, metrics.aprsUploaded ?? 0);
    setCounter(counterMapping, metrics.mappingCount ?? 0);
  }

  function updateAppInfo(info) {
    if (!appVersionLabel) return;
    const version =
      typeof info?.version === 'string' && info.version.trim()
        ? info.version.trim()
        : '';
    appVersionLabel.textContent = version ? `v${version}` : 'v—';
  }

  function updateCallmesh(info) {
    if (!info) return;
    const parts = [];
    if (typeof info.statusText === 'string' && info.statusText.trim()) {
      parts.push(info.statusText.trim());
    }
    if (info.hasKey) {
      parts.push(info.degraded ? 'Key 已驗證(降級)' : 'Key 已驗證');
    } else {
      parts.push('Key 未驗證');
    }
    if (callmeshLabel) {
      callmeshLabel.textContent = parts.join(' / ') || '未取得';
    }

    updateAprsStatus(info.aprs);

    if (Array.isArray(info.mappingItems)) {
      mappingMeshIds.clear();
      for (const item of info.mappingItems) {
        const meshId = normalizeMeshId(item?.mesh_id ?? item?.meshId);
        if (meshId) {
          mappingMeshIds.add(meshId);
        }
      }
      refreshSummaryMappingHighlights();
    } else if (info.mappingItems == null) {
      mappingMeshIds.clear();
      refreshSummaryMappingHighlights();
    }

    const provision = info.provision || {};
    if (callmeshCallsign) callmeshCallsign.textContent = formatProvisionCallsign(provision);
    if (callmeshSymbol) callmeshSymbol.textContent = formatSymbol(provision);
    if (callmeshCoords) callmeshCoords.textContent = formatCoords(provision);
    if (callmeshPhg) callmeshPhg.textContent = formatPhgDetails(provision);
    if (callmeshComment) {
      const comment =
        provision.comment ??
        provision.provision_comment ??
        provision.notes ??
        provision.description ??
        '—';
      callmeshComment.textContent = comment || '—';
    }
    if (callmeshUpdated) {
      callmeshUpdated.textContent = info.lastMappingSyncedAt
        ? formatRelativeTime(info.lastMappingSyncedAt)
        : '—';
    }

    const provisionLat = Number(provision.latitude ?? provision.lat);
    const provisionLon = Number(provision.longitude ?? provision.lon);
    if (Number.isFinite(provisionLat) && Number.isFinite(provisionLon)) {
      selfProvisionCoords = {
        lat: provisionLat,
        lon: provisionLon
      };
    } else {
      selfProvisionCoords = null;
    }

    refreshSummarySelfLabels();
  }

  function updateAprsStatus(aprs) {
    if (!aprsStatusLabel) return;
    if (!aprs) {
      aprsStatusLabel.textContent = '尚未取得';
      return;
    }
    const server = aprs.actualServer || aprs.server || '未知伺服器';
    const state = aprs.connected ? '已連線' : '未連線';
    aprsStatusLabel.textContent = `${state} (${server})`;
  }

  function handleSummaryBatch(list) {
    if (!Array.isArray(list)) return;
    for (let i = list.length - 1; i >= 0; i -= 1) {
      appendSummary(list[i]);
    }
  }

  function handleLogBatch(list) {
    if (!Array.isArray(list)) return;
    for (let i = list.length - 1; i >= 0; i -= 1) {
      appendLog(list[i]);
    }
  }

  function refreshSummaryMappingHighlights() {
    for (const row of summaryRows) {
      if (!row || !row.dataset) continue;
      const meshId = row.dataset.meshId;
      if (meshId && mappingMeshIds.has(meshId)) {
        row.classList.add('summary-row-mapped');
      } else {
        row.classList.remove('summary-row-mapped');
      }
    }
    refreshSummarySelfLabels();
  }

  function refreshSummarySelfLabels() {
    for (const row of summaryRows) {
      if (!row) continue;
      const summary = row.__summaryData;
      if (!summary) continue;
      if (!summary.selfMeshId && currentSelfMeshId) {
        summary.selfMeshId = currentSelfMeshId;
      }
      const relayCell = row.cells?.[2];
      if (relayCell) {
        relayCell.textContent = formatRelay(summary);
      }
      const sourceCell = row.cells?.[1];
      if (sourceCell) {
        sourceCell.textContent = formatSource(summary);
      }
      const channelCell = row.cells?.[4];
      if (channelCell) {
        channelCell.textContent = formatChannel(summary.channel);
      }
      const hopsCell = row.cells?.[5];
      if (hopsCell) {
        hopsCell.textContent = formatHops(summary.hops);
      }
      const detailCell = row.cells?.[8];
      if (detailCell) {
        detailCell.innerHTML = formatDetail(summary);
        const flowId = summary.flowId;
        if (flowId) {
          const badgeCallsign = flowAprsCallsigns.get(flowId);
          if (badgeCallsign) {
            setAprsBadge(row, badgeCallsign);
          }
        }
      }
    }
  }

  function connectStream() {
    const source = new EventSource('/api/events');

    source.onmessage = (event) => {
      try {
        const packet = JSON.parse(event.data);
        if (!packet || !packet.type) return;
        switch (packet.type) {
          case 'status':
            updateStatus(packet.payload);
            break;
          case 'metrics':
            updateMetrics(packet.payload);
            break;
          case 'summary':
            appendSummary(packet.payload || {});
            break;
          case 'summary-batch':
            handleSummaryBatch(packet.payload);
            break;
          case 'callmesh':
            updateCallmesh(packet.payload);
            break;
          case 'app-info':
            updateAppInfo(packet.payload);
            break;
          case 'self':
            currentSelfMeshId = normalizeMeshId(packet.payload?.meshId);
            refreshSummarySelfLabels();
            break;
          case 'log':
            appendLog(packet.payload);
            break;
          case 'log-batch':
            handleLogBatch(packet.payload);
            break;
          case 'aprs':
            markAprsUploaded(packet.payload);
            break;
          default:
            break;
        }
      } catch (err) {
        console.error('無法解析事件', err);
      }
    };

    source.onerror = () => {
      if (statusLabel) {
        statusLabel.textContent = '連線中斷，重新連線...';
      }
      source.close();
      setTimeout(connectStream, 3000);
    };
  }

  connectStream();
})();
