'use strict';

const form = document.getElementById('connection-form');
const connectBtn = document.getElementById('connect-btn');
const statusIndicator = document.getElementById('status-indicator');
const platformStatus = document.getElementById('platform-status');
const tableBody = document.getElementById('packet-table');
const rowTemplate = document.getElementById('packet-row-template');
const currentNodeDisplay = document.getElementById('current-node-display');
const currentNodeText = document.getElementById('current-node-text');
const openSettingsBtn = document.getElementById('open-settings-btn');

const settingsHostInput = document.getElementById('settings-host');
const discoverBtn = document.getElementById('discover-btn');
const discoverStatus = document.getElementById('discover-status');
const discoverModal = document.getElementById('discover-modal');
const discoverModalBody = document.getElementById('discover-modal-body');
const discoverModalCancel = document.getElementById('discover-modal-cancel');

const apiKeyInput = document.getElementById('api-key');
const saveApiKeyBtn = document.getElementById('save-api-key');
const callmeshOverlay = document.getElementById('callmesh-overlay');
const overlayRetryBtn = document.getElementById('overlay-retry');
const overlayKeyInput = document.getElementById('overlay-api-key');
const overlaySaveBtn = document.getElementById('overlay-save');
const overlayStatus = document.getElementById('overlay-status');
const overlayStepApi = document.getElementById('overlay-step-api');
const overlayStepHost = document.getElementById('overlay-step-host');
const overlayHostInput = document.getElementById('overlay-host');
const overlayApplyHostBtn = document.getElementById('overlay-apply-host');
const overlayDiscoverHostBtn = document.getElementById('overlay-discover-host');
const overlayHostStatus = document.getElementById('overlay-host-status');

const counterPackets10Min = document.getElementById('counter-packages-10min');
const counterAprsUploaded = document.getElementById('counter-aprs-uploaded');
const counterMappingCount = document.getElementById('counter-mapping-count');
const logOutput = document.getElementById('log-output');
const logSearchInput = document.getElementById('log-search');
const logTagFilterSelect = document.getElementById('log-tag-filter');
const appNameHeading = document.getElementById('app-name');
const appVersionLabel = document.getElementById('app-version');
const aprsStatusLabel = document.getElementById('aprs-status');
const aprsServerLabel = document.getElementById('aprs-server-label');
const DEFAULT_APRS_SERVER = 'asia.aprs2.net';
const DEFAULT_APRS_BEACON_MINUTES = 10;
const SOCKET_HEARTBEAT_SECONDS = 30;
const SOCKET_IDLE_TIMEOUT_MS = 60 * 1000;
const SOCKET_KEEPALIVE_DELAY_MS = 15 * 1000;
const HOST_GUIDANCE_MESSAGE = '尚未設定節點 IP，請手動輸入或按「自動搜尋」。';
const METERS_PER_FOOT = 0.3048;
const LOG_DOWNLOAD_PREFIX = 'tmag-log';

const infoCallsign = document.getElementById('info-callsign');
const infoSymbol = document.getElementById('info-symbol');
const infoCoords = document.getElementById('info-coords');
const infoPhgPower = document.getElementById('info-phg-power');
const infoPhgHeight = document.getElementById('info-phg-height');
const infoPhgGain = document.getElementById('info-phg-gain');
const infoComment = document.getElementById('info-comment');
const infoUpdatedAt = document.getElementById('info-updated-at');

const navButtons = Array.from(document.querySelectorAll('.nav-btn'));
const monitorPage = document.getElementById('monitor-page');
const settingsPage = document.getElementById('settings-page');
const logPage = document.getElementById('log-page');
const infoPage = document.getElementById('info-page');
const jsonPage = document.getElementById('json-page');
const flowPage = document.getElementById('flow-page');
const flowList = document.getElementById('flow-list');
const flowEmptyState = document.getElementById('flow-empty-state');
const flowSearchInput = document.getElementById('flow-search');
const flowFilterStateSelect = document.getElementById('flow-filter-state');
const flowDownloadBtn = document.getElementById('flow-download-btn');
const jsonList = document.getElementById('json-list');
const jsonEmptyState = document.getElementById('json-empty-state');
const jsonEntryCount = document.getElementById('json-entry-count');
const jsonCopyBtn = document.getElementById('json-copy-btn');
const telemetryPage = document.getElementById('telemetry-page');
const telemetryNodeSelect = document.getElementById('telemetry-node-select');
const telemetryUpdatedAtLabel = document.getElementById('telemetry-updated-at');
const telemetryChartsContainer = document.getElementById('telemetry-charts');
const telemetryTableWrapper = document.getElementById('telemetry-table-wrapper');
const telemetryTableBody = document.getElementById('telemetry-table-body');
const telemetryEmptyState = document.getElementById('telemetry-empty-state');
const telemetryRangeSelect = document.getElementById('telemetry-range-select');
const telemetryRangeCustomWrap = document.getElementById('telemetry-range-custom');
const telemetryRangeStartInput = document.getElementById('telemetry-range-start');
const telemetryRangeEndInput = document.getElementById('telemetry-range-end');
const telemetryChartModeSelect = document.getElementById('telemetry-chart-mode');
const telemetryChartMetricSelect = document.getElementById('telemetry-chart-metric');
const aprsServerInput = document.getElementById('aprs-server');
const aprsBeaconIntervalInput = document.getElementById('aprs-beacon-interval');
const resetDataBtn = document.getElementById('reset-data-btn');
const copyLogBtn = document.getElementById('copy-log-btn');
const downloadLogBtn = document.getElementById('download-log-btn');

const MAX_ROWS = 200;
let discoveredDevices = [];
let callmeshHasServerKey = false;
let lastVerifiedKey = '';
let callmeshDegraded = false;
let isConnecting = false;
let isConnected = false;
let manualDisconnect = false;
let autoConnectAttempts = 0;
let autoConnectTimer = null;
let reconnectTimer = null;
let inactivityTimer = null;
let lastActivityAt = null;
let initialAutoConnectActive = false;
let manualConnectActive = false;
let manualConnectAbort = false;
let manualConnectRetryTimer = null;
let manualConnectRetryResolver = null;
let manualConnectAttempts = 0;
let manualConnectSession = 0;
let allowReconnectLoop = true;
let hostPreferenceRevision = 0;
let lastConnectedHost = null;
let lastConnectedHostRevision = -1;
let hostGuidanceActive = false;
let initialSetupAutoConnectPending = false;
let initialSetupAutoConnectTriggered = false;
const selfNodeState = {
  name: null,
  meshId: null,
  normalizedMeshId: null,
  raw: null
};
const LOG_MAX_ENTRIES = 2000;
const logEntries = [];
let logFilterTag = 'all';
let logSearchTerm = '';
const packetBuckets = new Map();
let packetSummaryLast10Min = 0;
let mappingMeshIds = new Set();
let mappingItems = [];
let lastProvisionSignature = null;
const AUTO_RECONNECT_FAILURE_LIMIT = 3;
const AUTO_RECONNECT_ROLLING_WINDOW_MS = 2 * 60 * 1000;
const AUTO_RECONNECT_ERROR_MESSAGE = '自動重連已停止，請確認裝置狀態後手動重試';
const AUTO_RECONNECT_FAILURE_DEDUP_MS = 5_000;

let autoReconnectSuspended = false;
const recentReconnectFailures = [];
const recentReconnectFailureTimestamps = new Map();
let lastCallmeshStatusLog = '';

const FLOW_MAX_ENTRIES = 1000;
const ALT_TOKEN_REGEX = /\s*(?:·\s*)?ALT\s*-?\d+(?:\.\d+)?\s*m\b/gi;
const APRS_HISTORY_MAX = 5000;
const flowEntries = [];
const flowEntryIndex = new Map();
let flowSearchTerm = '';
const pendingFlowSummaries = new Map();
const pendingAprsUplinks = new Map();
let flowFilterState = 'all';
const FLOW_CAPTURE_DELAY_MS = 5000;
let flowCaptureEnabledAt = 0;
let totalAprsUploaded = 0;
const aprsCompletedFlowIds = new Set();
const aprsCompletedQueue = [];
const JSON_MAX_ENTRIES = 300;
const jsonEntries = [];
const JSON_OMIT_KEYS = new Set([
  'queueStatus',
  'rxTime',
  'rx_time',
  'rxSnr',
  'rx_snr',
  'rxRssi',
  'rx_rssi'
]);
let jsonEntrySequence = 0;

const TELEMETRY_TABLE_LIMIT = 200;
const TELEMETRY_CHART_LIMIT = 200;
const TELEMETRY_MAX_LOCAL_RECORDS = 500;
const TELEMETRY_METRIC_DEFINITIONS = {
  batteryLevel: { label: '電量', unit: '%', decimals: 0, clamp: [0, 150], chart: true },
  voltage: { label: '電壓', unit: 'V', decimals: 2, chart: true },
  channelUtilization: { label: '通道使用率', unit: '%', decimals: 1, clamp: [0, 100], chart: true },
  airUtilTx: { label: '空中時間 (TX)', unit: '%', decimals: 1, clamp: [0, 100], chart: true },
  temperature: { label: '溫度', unit: '°C', decimals: 1, chart: true },
  relativeHumidity: { label: '濕度', unit: '%', decimals: 0, clamp: [0, 100], chart: true },
  barometricPressure: { label: '氣壓', unit: 'hPa', decimals: 1, chart: true },
  uptimeSeconds: {
    label: '運行時間',
    chart: false,
    formatter: (value) => formatSecondsAsDuration(value)
  }
};

const telemetryStore = new Map();
const telemetryRecordIds = new Set();
let telemetrySelectedMeshId = null;
let telemetryUpdatedAt = null;
let telemetryRangeMode = 'day';
let telemetryCustomRange = {
  startMs: null,
  endMs: null
};
let telemetryChartMode = 'all';
let telemetryChartMetric = null;
const telemetryCharts = new Map();

const AUTO_CONNECT_MAX_ATTEMPTS = 3;
const AUTO_CONNECT_DELAY_MS = 5000;
const MANUAL_CONNECT_MAX_ATTEMPTS = 3;
const MANUAL_CONNECT_DELAY_MS = 5000;
const INACTIVITY_THRESHOLD_MS = 10 * 60 * 1000;
const RECONNECT_INTERVAL_MS = 30 * 1000;
const INITIAL_RECONNECT_DELAY_MS = 5 * 1000;
const PACKET_WINDOW_MS = 10 * 60 * 1000;
const PACKET_BUCKET_MS = 60 * 1000;
const TYPE_ICONS = {
  Position: '📍',
  Telemetry: '🔋',
  EnvTelemetry: '🌡️',
  Routing: '🧭',
  RouteRequest: '🧭',
  RouteReply: '🧭',
  RouteError: '⚠️',
  Text: '💬',
  NodeInfo: '🧑‍🤝‍🧑',
  Admin: '🛠️',
  Traceroute: '🛰️',
  Waypoint: '🗺️',
  StoreForward: '🗃️',
  PaxCounter: '👥',
  RemoteHardware: '🔌',
  KeyVerification: '🔑',
  NeighborInfo: '🤝',
  Encrypted: '🔒'
};

const STATUS_LABELS = {
  connecting: (message) => message || '連線中...',
  connected: '已連線',
  disconnected: '已斷線',
  error: (message) => `錯誤：${message || '未知錯誤'}`,
  idle: '尚未連線'
};

const STATUS_ICONS = {
  connecting: '⏳',
  connected: '✅',
  disconnected: '⚠️',
  error: '❗',
  idle: '💤'
};

function isSelfMeshId(meshId) {
  const normalized = normalizeMeshId(meshId);
  if (!normalized) return false;
  const selfCandidate = selfNodeState.normalizedMeshId || selfNodeState.meshId;
  if (!selfCandidate) return false;
  return normalized === normalizeMeshId(selfCandidate);
}

function formatRelayLabel(relay) {
  if (!relay) return '';
  const label = relay.label || '';
  const meshId = relay.meshId || '';
  if (!meshId) return label;
  const normalized = meshId.startsWith('!') ? meshId.slice(1) : meshId;
  if (/^0{6}[0-9a-fA-F]{2}$/.test(normalized)) {
    return label ? label + '?' : meshId + '?';
  }
  return label;
}

function computeRelayLabel(summary) {
  const fromMeshId = summary.from?.meshId || summary.from?.meshIdNormalized || '';
  const fromNormalized = normalizeMeshId(fromMeshId);
  if (fromMeshId && isSelfMeshId(fromMeshId)) {
    return 'Self';
  }

  let relayMeshIdRaw = summary.relay?.meshId || summary.relay?.meshIdNormalized || '';
  if (relayMeshIdRaw && isSelfMeshId(relayMeshIdRaw)) {
    return 'Self';
  }
  let relayNormalized = normalizeMeshId(relayMeshIdRaw);
  if (relayNormalized && /^!0{6}[0-9a-fA-F]{2}$/.test(relayNormalized)) {
    relayMeshIdRaw = '';
    relayNormalized = null;
  }

  if (fromNormalized && relayNormalized && fromNormalized === relayNormalized) {
    return '直收';
  }

  const { usedHops, hopsLabel } = extractHopInfo(summary);

  if (summary.relay?.label) {
    return formatRelayLabel(summary.relay);
  }

  if (relayMeshIdRaw) {
    return formatRelayLabel({ label: summary.relay?.label || relayMeshIdRaw, meshId: relayMeshIdRaw });
  }

  if (usedHops === 0 || hopsLabel === '0/0' || hopsLabel.startsWith('0/')) {
    return '直收';
  }

  if (usedHops > 0) {
    return '未知?';
  }

  if (!hopsLabel) {
    return '直收';
  }

  if (hopsLabel.includes('?')) {
    return '未知?';
  }

  return '';
}

function extractHopInfo(summary) {
  const hopStart = Number(summary.hops?.start);
  const hopLimit = Number(summary.hops?.limit);
  const label = typeof summary.hops?.label === 'string' ? summary.hops.label.trim() : '';
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

function appendLog(tag, message, isoTimestamp) {
  const normalizedTag = String(tag || 'APP').toUpperCase();
  if (shouldSuppressLog(normalizedTag, message)) {
    return;
  }
  const date = isoTimestamp ? new Date(isoTimestamp) : new Date();
  const timestamp = formatLogTimestamp(date);
  const messageText = typeof message === 'string' ? message : String(message ?? '');
  const line = `[${timestamp}] [${normalizedTag}] ${messageText}`;
  logEntries.push({
    tag: normalizedTag,
    message: messageText,
    timestamp,
    iso: date.toISOString(),
    line,
    searchText: line.toLowerCase()
  });
  if (logEntries.length > LOG_MAX_ENTRIES) {
    logEntries.shift();
  }
  renderLogOutput({ scrollToEnd: true });
}

function getFilteredLogEntries() {
  const hasTagFilter = logFilterTag !== 'all';
  const term = logSearchTerm;
  return logEntries.filter((entry) => {
    if (hasTagFilter && entry.tag !== logFilterTag) {
      return false;
    }
    if (!term) {
      return true;
    }
    return entry.searchText.includes(term);
  });
}

function renderLogOutput({ scrollToEnd = false } = {}) {
  if (!logOutput) return;
  const previousScrollBottom = logOutput.scrollTop >= (logOutput.scrollHeight - logOutput.clientHeight - 4);
  const filtered = getFilteredLogEntries();
  const hasSearchTerm = Boolean(logSearchTerm);
  if (!filtered.length) {
    let message;
    if (!logEntries.length) {
      message = '尚未載入任何紀錄。';
    } else if (logFilterTag !== 'all' || logSearchTerm) {
      message = '沒有符合篩選條件的日誌。';
    } else {
      message = '尚未載入任何紀錄。';
    }
    logOutput.textContent = message;
    if (scrollToEnd || previousScrollBottom) {
      logOutput.scrollTop = logOutput.scrollHeight;
    }
    return;
  }

  if (hasSearchTerm) {
    const html = filtered.map((entry) => highlightLogLine(entry.line, logSearchTerm)).join('\n');
    logOutput.innerHTML = html;
  } else {
    logOutput.textContent = filtered.map((entry) => entry.line).join('\n');
  }
  if (scrollToEnd || previousScrollBottom) {
    logOutput.scrollTop = logOutput.scrollHeight;
  }
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function highlightLogLine(line, term) {
  if (!term) {
    return escapeHtml(line);
  }
  const lowerLine = line.toLowerCase();
  const termLen = term.length;
  if (termLen === 0) {
    return escapeHtml(line);
  }
  let result = '';
  let cursor = 0;
  let index = lowerLine.indexOf(term, cursor);
  while (index !== -1) {
    result += escapeHtml(line.slice(cursor, index));
    const match = line.slice(index, index + termLen);
    result += `<mark>${escapeHtml(match)}</mark>`;
    cursor = index + termLen;
    index = lowerLine.indexOf(term, cursor);
  }
  result += escapeHtml(line.slice(cursor));
  return result;
}

function setCounterValue(element, value, { positive = false, negative = false } = {}) {
  if (!element) return;
  element.textContent = String(value);
  element.classList.toggle('positive', Boolean(positive));
  element.classList.toggle('negative', Boolean(negative));
}

function updateDashboardCounters() {
  const packets = Number.isFinite(packetSummaryLast10Min) ? packetSummaryLast10Min : 0;
  const uploaded = Number.isFinite(totalAprsUploaded) ? totalAprsUploaded : 0;
  const mappingCount = mappingMeshIds ? mappingMeshIds.size : 0;

  setCounterValue(counterPackets10Min, packets, {
    positive: packets > 0,
    negative: packets === 0
  });

  setCounterValue(counterAprsUploaded, uploaded, {
    positive: uploaded > 0,
    negative: false
  });

  setCounterValue(counterMappingCount, mappingCount, {
    positive: mappingCount > 0,
    negative: mappingCount === 0
  });
}

function markAprsUploaded(flowId) {
  if (!flowId) return false;
  if (aprsCompletedFlowIds.has(flowId)) {
    return false;
  }
  aprsCompletedFlowIds.add(flowId);
  aprsCompletedQueue.push(flowId);
  if (aprsCompletedQueue.length > APRS_HISTORY_MAX) {
    const oldest = aprsCompletedQueue.shift();
    if (oldest) {
      aprsCompletedFlowIds.delete(oldest);
    }
  }
  totalAprsUploaded += 1;
  return true;
}

function formatLogTimestamp(date) {
  const hh = `${date.getHours()}`.padStart(2, '0');
  const mm = `${date.getMinutes()}`.padStart(2, '0');
  const ss = `${date.getSeconds()}`.padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

function shouldSuppressLog(tag, message) {
  if (tag !== 'APRS') return false;
  const upper = String(message || '').toUpperCase();
  if (upper.startsWith('RX # APRSC')) return true;
  if (upper.includes('LOGRESP') && upper.includes('VERIFIED')) return true;
  if (upper.includes('APRS-IS') && upper.includes('SERVER')) return true;
  if (upper.includes('FILTER') && upper.includes('PERL SERVER')) return true;
  return false;
}

function shouldRecordAutoFailure() {
  if (manualDisconnect) return false;
  if (manualConnectActive) return false;
  if (initialAutoConnectActive) return false;
  if (autoReconnectSuspended) return false;
  return true;
}

function recordReconnectFailure(reason) {
  if (!shouldRecordAutoFailure()) {
    return;
  }
  const now = Date.now();
  const reasonKey = reason || 'unknown';
  const lastFailureAt = recentReconnectFailureTimestamps.get(reasonKey);
  if (lastFailureAt && now - lastFailureAt < AUTO_RECONNECT_FAILURE_DEDUP_MS) {
    appendLog('CONNECT', `reconnect failure deduped reason=${reasonKey}`);
    return;
  }
  recentReconnectFailureTimestamps.set(reasonKey, now);
  while (recentReconnectFailures.length && now - recentReconnectFailures[0] > AUTO_RECONNECT_ROLLING_WINDOW_MS) {
    recentReconnectFailures.shift();
  }
  recentReconnectFailures.push(now);
  const remaining = Math.max(0, AUTO_RECONNECT_FAILURE_LIMIT - recentReconnectFailures.length);
  appendLog('CONNECT', `reconnect failure recorded reason=${reason || 'unknown'} remaining=${remaining}`);
  if (!autoReconnectSuspended && recentReconnectFailures.length >= AUTO_RECONNECT_FAILURE_LIMIT) {
    suspendAutoReconnect(reason);
  }
}

function suspendAutoReconnect(reason) {
  if (autoReconnectSuspended) {
    return;
  }
  autoReconnectSuspended = true;
  allowReconnectLoop = false;
  stopReconnectLoop();
  updateStatus('error', AUTO_RECONNECT_ERROR_MESSAGE);
  appendLog('CONNECT', `auto reconnect suspended${reason ? ` (${reason})` : ''}`);
}

function resumeAutoReconnect({ reason = '', resetFailures = true, silent = false } = {}) {
  if (resetFailures) {
    recentReconnectFailures.length = 0;
    recentReconnectFailureTimestamps.clear();
  }
  const wasSuspended = autoReconnectSuspended;
  autoReconnectSuspended = false;
  if (wasSuspended && !manualConnectActive) {
    allowReconnectLoop = true;
  }
  if (wasSuspended && !silent) {
    appendLog('CONNECT', `auto reconnect resumed${reason ? ` (${reason})` : ''}`);
  }
}

function normalizeMeshId(meshId) {
  if (!meshId) return null;
  if (meshId.startsWith('0x') || meshId.startsWith('0X')) {
    return `!${meshId.slice(2)}`.toLowerCase();
  }
  return meshId.toLowerCase();
}

function loadPreferences() {
  try {
    const saved = JSON.parse(localStorage.getItem('meshtastic:preferences') || '{}');
    const savedHost = typeof saved.host === 'string' ? saved.host.trim() : '';
    if (settingsHostInput) {
      settingsHostInput.value = savedHost;
    }
    if (overlayHostInput) {
      overlayHostInput.value = savedHost;
    }
    if (saved.apiKey) apiKeyInput.value = saved.apiKey;
    if (saved.platformStatus) platformStatus.textContent = saved.platformStatus;
    if (typeof saved.callmeshVerified === 'boolean') {
      callmeshHasServerKey = saved.callmeshVerified;
    }
    if (typeof saved.callmeshDegraded === 'boolean') {
      callmeshDegraded = saved.callmeshDegraded;
    }
    if (saved.lastVerifiedKey) {
      lastVerifiedKey = saved.lastVerifiedKey;
    } else if (callmeshHasServerKey) {
      lastVerifiedKey = apiKeyInput.value.trim();
    }
    if (aprsServerInput) {
      aprsServerInput.value = saved.aprsServer || DEFAULT_APRS_SERVER;
    }
    if (aprsBeaconIntervalInput) {
      const minutes = Number(saved.aprsBeaconMinutes);
      const normalized = Number.isFinite(minutes) && minutes >= 1 ? Math.min(Math.round(minutes), 1440) : DEFAULT_APRS_BEACON_MINUTES;
      aprsBeaconIntervalInput.value = String(normalized);
    }
  } catch (err) {
    console.warn('無法載入偏好設定:', err);
    if (settingsHostInput) settingsHostInput.value = '';
    if (overlayHostInput) overlayHostInput.value = '';
    if (aprsServerInput) aprsServerInput.value = DEFAULT_APRS_SERVER;
    if (aprsBeaconIntervalInput) aprsBeaconIntervalInput.value = String(DEFAULT_APRS_BEACON_MINUTES);
  }
}

function savePreferences({ persist = true } = {}) {
  const data = {
    host: settingsHostInput.value.trim(),
    apiKey: apiKeyInput.value.trim(),
    platformStatus: platformStatus.textContent,
    callmeshVerified: callmeshHasServerKey,
    callmeshDegraded,
    lastVerifiedKey,
    aprsServer: aprsServerInput ? aprsServerInput.value.trim() || DEFAULT_APRS_SERVER : DEFAULT_APRS_SERVER,
    aprsBeaconMinutes: getAprsBeaconMinutes()
  };
  localStorage.setItem('meshtastic:preferences', JSON.stringify(data));
  if (persist) {
    const hostPayload = data.host || null;
    window.meshtastic.updateClientPreferences?.({ host: hostPayload }).then((result) => {
      if (result && result.success === false && result.error) {
        console.warn('persist client preferences failed:', result.error);
      }
    }).catch((err) => {
      console.warn('persist client preferences error:', err);
    });
  }
  return data;
}

async function hydratePreferencesFromMain() {
  if (!window.meshtastic.getClientPreferences) {
    return;
  }
  try {
    const preferences = await window.meshtastic.getClientPreferences();
    if (!preferences || typeof preferences !== 'object') {
      return;
    }
    const host = typeof preferences.host === 'string' ? preferences.host.trim() : '';
    if (host && getHostValue() !== host) {
      settingsHostInput.value = host;
      if (overlayHostInput) {
        overlayHostInput.value = host;
      }
      savePreferences({ persist: false });
    }
  } catch (err) {
    console.warn('載入偏好設定失敗:', err);
  }
}

function getHostValue() {
  return (settingsHostInput.value || '').trim();
}

function markHostPreferenceUpdated() {
  hostPreferenceRevision += 1;
}

function getReconnectHost() {
  if (lastConnectedHost && lastConnectedHostRevision === hostPreferenceRevision) {
    return lastConnectedHost;
  }
  const currentHost = getHostValue();
  if (currentHost) {
    return currentHost;
  }
  return lastConnectedHost || '';
}

function getAprsBeaconMinutes() {
  if (!aprsBeaconIntervalInput) return DEFAULT_APRS_BEACON_MINUTES;
  const value = Number(aprsBeaconIntervalInput.value);
  if (!Number.isFinite(value) || value <= 0) return DEFAULT_APRS_BEACON_MINUTES;
  const rounded = Math.round(value);
  if (rounded < 1) return 1;
  if (rounded > 1440) return 1440;
  return rounded;
}

async function bootstrap() {
  loadPreferences();
  await hydratePreferencesFromMain();
  setDiscoverStatus('', 'info');
  updatePlatformStatus();
  updateConnectAvailability();
  ensureHostGuidance();
  refreshOverlay();
  activatePage('monitor-page');
  scheduleInitialAutoConnect();
  clearSelfNodeDisplay();
  appendLog('APP', 'TMAG monitor initialized.');
  updateDashboardCounters();
  updateJsonCounter();
  updateJsonEmptyState();
  await initializeTelemetry();
  setTelemetryRangeMode(telemetryRangeMode, { skipRender: true });
  setTelemetryChartMode(telemetryChartMode, { skipRender: true });
  renderTelemetryView();

  const initialAprsServer = aprsServerInput?.value?.trim() || DEFAULT_APRS_SERVER;
  window.meshtastic.setAprsServer?.(initialAprsServer);
  const initialBeaconMinutes = getAprsBeaconMinutes();
  window.meshtastic.setAprsBeaconInterval?.(initialBeaconMinutes);

  try {
    const info = await window.meshtastic.getAppInfo?.();
    if (info?.version) {
      if (appVersionLabel) {
        appVersionLabel.textContent = `v${info.version}`;
      }
      if (appNameHeading) {
        document.title = `TMMARC Meshtastic APRS Gateway (TMAG) v${info.version}`;
      }
      appendLog('APP', `version ${info.version} loaded`);
    }
  } catch (err) {
    appendLog('APP', `version lookup failed: ${err.message}`);
  }

  try {
    await maybeAutoValidateInitialKey();
  } catch (err) {
    console.warn('initial auto-validate error:', err);
  }
}

bootstrap().catch((err) => {
  console.error('renderer bootstrap failed:', err);
  appendLog('APP', `初始化失敗: ${err.message || err}`);
});

async function maybeAutoValidateInitialKey() {
  const initialKey = apiKeyInput.value.trim();
  if (!initialKey) {
    return;
  }
  try {
    const shouldAuto = await window.meshtastic.shouldAutoValidateKey?.();
    if (shouldAuto === false) {
      appendLog('CALLMESH', 'auto validation skipped (suppress flag active)');
      return;
    }
  } catch (err) {
    console.warn('auto-validate check failed:', err);
  }
  validateApiKey(initialKey, { auto: true, source: 'main' });
}

navButtons.forEach((btn) => {
  btn.addEventListener('click', () => {
    const target = btn.dataset.target;
    if (target) {
      activatePage(target);
    }
  });
});

telemetryNodeSelect?.addEventListener('change', () => {
  const value = telemetryNodeSelect.value || null;
  telemetrySelectedMeshId = value;
  renderTelemetryView();
});

telemetryRangeSelect?.addEventListener('change', (event) => {
  const mode = event.target.value;
  setTelemetryRangeMode(mode);
});

function handleTelemetryRangeInputChange() {
  const rawStart = telemetryRangeStartInput?.value || '';
  const rawEnd = telemetryRangeEndInput?.value || '';
  if (telemetryRangeMode !== 'custom') {
    telemetryRangeMode = 'custom';
    if (telemetryRangeSelect) {
      telemetryRangeSelect.value = 'custom';
    }
    if (telemetryRangeCustomWrap) {
      telemetryRangeCustomWrap.classList.remove('hidden');
    }
  }
  let startMs = parseDatetimeLocal(rawStart);
  let endMs = parseDatetimeLocal(rawEnd);
  telemetryCustomRange = {
    startMs: startMs != null ? startMs : telemetryCustomRange.startMs,
    endMs: endMs != null ? endMs : telemetryCustomRange.endMs
  };
  ensureTelemetryCustomDefaults();
  updateTelemetryRangeInputs();
  refreshTelemetrySelectors();
  renderTelemetryView();
}

telemetryRangeStartInput?.addEventListener('change', handleTelemetryRangeInputChange);
telemetryRangeEndInput?.addEventListener('change', handleTelemetryRangeInputChange);

telemetryChartModeSelect?.addEventListener('change', (event) => {
  setTelemetryChartMode(event.target.value);
});

telemetryChartMetricSelect?.addEventListener('change', (event) => {
  telemetryChartMetric = event.target.value || null;
  renderTelemetryView();
});

flowFilterStateSelect?.addEventListener('change', () => {
  flowFilterState = (flowFilterStateSelect.value || 'all').toLowerCase();
  renderFlowEntries();
});

flowDownloadBtn?.addEventListener('click', () => {
  downloadFlowCsv();
});

logTagFilterSelect?.addEventListener('change', () => {
  const value = (logTagFilterSelect.value || 'all').trim();
  logFilterTag = value === 'all' ? 'all' : value.toUpperCase();
  renderLogOutput();
});

logSearchInput?.addEventListener('input', () => {
  const term = logSearchInput.value.trim().toLowerCase();
  logSearchTerm = term;
  renderLogOutput();
});

openSettingsBtn?.addEventListener('click', () => {
  activatePage('settings-page');
});

flowSearchInput?.addEventListener('input', () => {
  const value = flowSearchInput.value.trim().toLowerCase();
  flowSearchTerm = value;
  renderFlowEntries();
});


settingsHostInput.addEventListener('input', () => {
  markHostPreferenceUpdated();
  const trimmedHost = settingsHostInput.value.trim();
  if (overlayHostInput && overlayHostInput.value !== trimmedHost) {
    overlayHostInput.value = trimmedHost;
  }
  if (!trimmedHost) {
    lastConnectedHost = null;
    lastConnectedHostRevision = -1;
  }
  resumeAutoReconnect({ reason: 'host-updated', silent: true });
  savePreferences();
  updateConnectAvailability();
  if (trimmedHost) {
    ensureHostGuidance();
  } else {
    ensureHostGuidance({ force: true });
  }
});

aprsServerInput?.addEventListener('input', () => {
  savePreferences();
  window.meshtastic.setAprsServer?.(aprsServerInput.value.trim() || DEFAULT_APRS_SERVER);
});

aprsBeaconIntervalInput?.addEventListener('change', () => {
  const minutes = getAprsBeaconMinutes();
  aprsBeaconIntervalInput.value = String(minutes);
  window.meshtastic.setAprsBeaconInterval?.(minutes);
  savePreferences();
  appendLog('APRS', `beacon interval set to ${minutes} 分鐘`);
});

resetDataBtn?.addEventListener('click', async () => {
  if (!window.confirm('確定要清除所有本地資料與 API Key 嗎？')) {
    return;
  }
  try {
    await window.meshtastic.resetCallMeshData?.();
    await window.meshtastic.updateClientPreferences?.({ host: null });
    try {
      localStorage.clear();
    } catch {
      localStorage.removeItem('meshtastic:preferences');
    }
    callmeshHasServerKey = false;
    callmeshDegraded = false;
    lastVerifiedKey = '';
    mappingMeshIds = new Set();
    mappingItems = [];
    lastProvisionSignature = null;
    apiKeyInput.value = '';
    if (overlayKeyInput) overlayKeyInput.value = '';
    platformStatus.textContent = 'CallMesh: 未設定 Key';
    clearSelfNodeDisplay();
    updateProvisionInfo(null, null);
    clearPacketFlowData();
    telemetryUpdatedAt = null;
    clearTelemetryDataLocal({ silent: true });
    hostPreferenceRevision = 0;
    lastConnectedHost = null;
    lastConnectedHostRevision = -1;
    initialSetupAutoConnectPending = false;
    initialSetupAutoConnectTriggered = false;
    loadPreferences();
    ensureHostGuidance({ force: true });
    if (aprsServerInput) {
      aprsServerInput.value = DEFAULT_APRS_SERVER;
      window.meshtastic.setAprsServer?.(DEFAULT_APRS_SERVER);
    }
    if (aprsBeaconIntervalInput) {
      const minutes = getAprsBeaconMinutes();
      window.meshtastic.setAprsBeaconInterval?.(minutes);
    }
    savePreferences();
    updateConnectAvailability();
    refreshOverlay();
    ensureHostGuidance();
    appendLog('APP', '已重置本地 CallMesh 資料與 API Key');
  } catch (err) {
    appendLog('APP', `重置失敗: ${err.message}`);
  }
});

copyLogBtn?.addEventListener('click', async () => {
  try {
    const filtered = getFilteredLogEntries();
    const text = filtered.map((entry) => entry.line).join('\n');
    if (!text) {
      appendLog('APP', '目前尚無可複製的日誌資料');
      return;
    }
    await navigator.clipboard.writeText(text);
    appendLog('APP', '日誌已複製到剪貼簿');
  } catch (err) {
    console.error('複製日誌失敗:', err);
    appendLog('APP', `複製日誌失敗: ${err.message || err}`);
  }
});

downloadLogBtn?.addEventListener('click', () => {
  const filtered = getFilteredLogEntries();
  if (!filtered.length) {
    appendLog('APP', '目前尚無可下載的日誌資料');
    return;
  }
  const text = filtered.map((entry) => entry.line).join('\n');
  const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `${LOG_DOWNLOAD_PREFIX}-${timestamp}.txt`;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  appendLog('APP', '日誌已下載');
});

jsonCopyBtn?.addEventListener('click', async () => {
  if (!jsonEntries.length) {
    appendLog('APP', '目前尚無可複製的 JSON 紀錄');
    return;
  }
  try {
    const exportPayload = jsonEntries.map((entry) => entry.data);
    await navigator.clipboard.writeText(JSON.stringify(exportPayload, null, 2));
    appendLog('APP', `已將 ${exportPayload.length} 筆 JSON 紀錄複製到剪貼簿`);
  } catch (err) {
    console.error('複製 JSON 紀錄失敗:', err);
    appendLog('APP', `複製 JSON 紀錄失敗: ${err.message || err}`);
  }
});

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (isConnecting || manualConnectActive) {
    cancelManualConnect();
    return;
  }

  if (isConnected) {
    manualDisconnect = true;
    await performDisconnect({ preserveManual: true });
    return;
  }

  if (!hasApiKey()) {
    setDiscoverStatus('請先設定 CallMesh API Key', 'error');
    updateStatus('error', 'API Key 未設定');
    updateConnectAvailability();
    appendLog('CONNECT', 'blocked: missing API key');
    allowReconnectLoop = true;
    return;
  }

  if (!hasHost()) {
    setDiscoverStatus('請先設定 Host', 'error');
    updateStatus('error', 'Host 未設定');
    updateConnectAvailability();
    appendLog('CONNECT', 'blocked: missing host');
    allowReconnectLoop = true;
    return;
  }

  manualDisconnect = false;
  clearAutoConnectTimer();
  savePreferences();
  await manualConnectWithRetries();
});

saveApiKeyBtn.addEventListener('click', () => {
  validateApiKey(apiKeyInput.value, { auto: false, source: 'main' });
});

apiKeyInput.addEventListener('input', () => {
  const trimmed = apiKeyInput.value.trim();
  callmeshHasServerKey = trimmed.length > 0 && trimmed === lastVerifiedKey;
  updateConnectAvailability();
  refreshOverlay();
});

overlaySaveBtn?.addEventListener('click', () => {
  validateApiKey(overlayKeyInput.value, { auto: false, source: 'overlay' });
});

overlayKeyInput?.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    validateApiKey(overlayKeyInput.value, { auto: false, source: 'overlay' });
  }
});

overlayRetryBtn.addEventListener('click', () => {
  validateApiKey(overlayKeyInput.value, { auto: false, source: 'overlay' });
});

overlayApplyHostBtn?.addEventListener('click', () => {
  applyOverlayHost();
});

overlayHostInput?.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    applyOverlayHost();
  }
});

overlayDiscoverHostBtn?.addEventListener('click', () => {
  setOverlayHostStatus('正在搜尋區網內的裝置...', 'info');
  discoverBtn?.click();
});

discoverBtn.addEventListener('click', async () => {
  setDiscoveringState(true);
  setDiscoverStatus('正在搜尋區網內的裝置...', 'info');
  if (isOverlayHostStepVisible()) {
    setOverlayHostStatus('正在搜尋區網內的裝置...', 'info');
  }
  appendLog('DISCOVER', 'scanning for devices');
  try {
    const results = await window.meshtastic.discover({ timeout: 4000 });
    discoveredDevices = results;
    appendLog('DISCOVER', 'found ' + results.length + ' device(s)');
    if (!results.length) {
      setDiscoverStatus('未找到 Meshtastic 裝置，請確認裝置是否與本機同網段。', 'warn');
      if (isOverlayHostStepVisible()) {
        setOverlayHostStatus('未找到節點，請確認裝置是否與本機同網段。', 'error');
      }
      hideDiscoverModal();
    } else {
      setDiscoverStatus('找到 ' + results.length + ' 個裝置。', 'success');
      if (isOverlayHostStepVisible()) {
        setOverlayHostStatus('請在清單中選擇節點。', 'info');
      }
      showDiscoverModal(results);
    }
  } catch (err) {
    console.error('搜尋裝置失敗:', err);
    setDiscoverStatus('搜尋失敗：' + err.message, 'error');
    if (isOverlayHostStepVisible()) {
      setOverlayHostStatus('搜尋失敗：' + err.message, 'error');
    }
    hideDiscoverModal();
    appendLog('DISCOVER', 'error ' + err.message);
  } finally {
    setDiscoveringState(false);
  }
});



discoverModalCancel?.addEventListener('click', () => {
  hideDiscoverModal();
});

discoverModal?.addEventListener('click', (event) => {
  if (event.target === discoverModal) {
    hideDiscoverModal();
  }
});

const unsubscribeSummary = window.meshtastic.onSummary((summary) => {
  appendSummaryRow(summary);
});

const unsubscribeRaw = window.meshtastic.onFromRadio?.((message) => {
  handleRawMessage(message);
});

const unsubscribeStatus = window.meshtastic.onStatus((info) => {
  appendLog('STATUS', `status=${info.status}${info.message ? ` message=${info.message}` : ''}`);
  updateStatus(info.status, info.message, info.nonce);

  if (info.status === 'connected') {
    initialAutoConnectActive = false;
    setConnectedState(true);
    return;
  }

  if (info.status === 'disconnected') {
    recordReconnectFailure('disconnected');
  }

  if (info.status === 'disconnected' || info.status === 'error') {
    setConnectedState(false);
    if (!manualDisconnect && !initialAutoConnectActive && !manualConnectActive && allowReconnectLoop && !autoReconnectSuspended) {
      startReconnectLoop();
    }
  }
});

const unsubscribeCallMeshStatus = window.meshtastic.onCallMeshStatus?.((info) => {
  handleCallMeshStatus(info);
});

const unsubscribeCallMeshLog = window.meshtastic.onCallMeshLog?.((entry) => {
  if (!entry) return;
  appendLog(entry.tag || 'CALLMESH', entry.message || '', entry.timestamp);
});

const unsubscribeMyInfo = window.meshtastic.onMyInfo?.((info) => {
  handleSelfInfoEvent(info);
});

const unsubscribeAprsUplink = window.meshtastic.onAprsUplink?.((info) => {
  handleAprsUplink(info);
});

const unsubscribeTelemetry = window.meshtastic.onTelemetry?.((payload) => {
  handleTelemetryEvent(payload);
});

window.addEventListener('beforeunload', () => {
  unsubscribeSummary();
  unsubscribeRaw?.();
  unsubscribeStatus();
  unsubscribeCallMeshStatus?.();
  unsubscribeCallMeshLog?.();
  unsubscribeMyInfo?.();
  unsubscribeAprsUplink?.();
  unsubscribeTelemetry?.();
});

function handleCallMeshStatus(info, { silent = false } = {}) {
  if (!info) return;
  const previousDegraded = callmeshDegraded;
  const hasKey = Boolean(info.hasKey);
  callmeshDegraded = Boolean(info.degraded);

  const statusSummary = `status=${info.statusText || ''} hasKey=${hasKey} degraded=${callmeshDegraded}`.trim();
  if (statusSummary !== lastCallmeshStatusLog) {
    appendLog('CALLMESH', statusSummary);
    lastCallmeshStatusLog = statusSummary;
  }

  if (Array.isArray(info.mappingItems)) {
    mappingItems = info.mappingItems;
    const normalizedList = info.mappingItems
      .map((item) => normalizeMeshId(item.mesh_id))
      .filter(Boolean);
    mappingMeshIds = new Set(normalizedList);
    refreshFlowEntryLabels();
    flushPendingFlowSummaries();
  } else {
    mappingItems = [];
    mappingMeshIds = new Set();
    refreshFlowEntryLabels();
  }
  updateDashboardCounters();

  if (aprsStatusLabel) {
    const aprs = info.aprs || {};
    aprsStatusLabel.textContent = `APRS: ${aprs.connected ? '已連線' : '未連線'}`;
    const configuredServer = aprs.server || DEFAULT_APRS_SERVER;
    const actualServer = aprs.actualServer;
    const serverLabel = actualServer
      ? (actualServer === configuredServer ? actualServer : `${actualServer} (${configuredServer})`)
      : configuredServer;
    aprsServerLabel.textContent = `Server: ${serverLabel}`;
    if (aprsBeaconIntervalInput && Number.isFinite(aprs.beaconIntervalMs)) {
      const minutes = Math.round(aprs.beaconIntervalMs / 60_000);
      const normalized = Math.min(Math.max(minutes, 1), 1440);
      if (!aprsBeaconIntervalInput.matches(':focus') && String(normalized) !== aprsBeaconIntervalInput.value) {
        aprsBeaconIntervalInput.value = String(normalized);
        savePreferences();
      }
    }
  }

  updateProvisionInfo(info.provision, info.lastMappingSyncedAt);

  platformStatus.textContent = info.statusText || (hasKey ? 'CallMesh: Heartbeat 正常' : 'CallMesh: 未設定 Key');

  if (hasKey) {
    callmeshHasServerKey = true;
    if (info.verifiedKey) {
      lastVerifiedKey = info.verifiedKey;
      const current = apiKeyInput.value.trim();
      if (!apiKeyInput.matches(':focus') || current === '' || current === info.verifiedKey) {
        apiKeyInput.value = info.verifiedKey;
      }
      if (overlayKeyInput && overlayKeyInput.value.trim() !== info.verifiedKey) {
        overlayKeyInput.value = info.verifiedKey;
      }
    }
    if (!silent) {
      if (callmeshDegraded && !previousDegraded) {
        setDiscoverStatus(info.statusText || 'CallMesh: Heartbeat 失敗', 'warn');
      }
      if (!callmeshDegraded && previousDegraded) {
        setDiscoverStatus(info.statusText || 'CallMesh: Heartbeat 恢復正常', 'success');
      }
    }
  } else {
    callmeshHasServerKey = false;
    callmeshDegraded = false;
    lastVerifiedKey = '';
    if (!apiKeyInput.matches(':focus')) {
      apiKeyInput.value = '';
    }
    if (overlayKeyInput) overlayKeyInput.value = '';
  }

  savePreferences();
  updateConnectAvailability();

  if (!hasKey && !silent) {
    const message = info.statusText || 'CallMesh: Key 驗證失敗';
    setDiscoverStatus(message, 'error');
    setOverlayStatus(message, 'error');
  }
}

function updatePlatformStatus() {
  window.meshtastic.getCallMeshStatus?.().then((info) => {
    if (!info) return;
    handleCallMeshStatus(info, { silent: true });
  }).catch(() => {
    refreshOverlay();
  });
}

function appendSummaryRow(summary) {
  if (!summary) return;
  registerPacketActivity(summary);
  maybeUpdateSelfNodeFromSummary(summary);
  const nodesLabel = formatNodes(summary);
  const detailSnippet = summary.detail ? ` detail=${summary.detail}` : '';
  appendLog('SUMMARY', `${summary.timestampLabel || ''} ${nodesLabel} ${summary.type || ''}${detailSnippet}`.trim());
  const fragment = rowTemplate.content.cloneNode(true);
  const row = fragment.querySelector('tr');

  row.querySelector('.time').textContent = summary.timestampLabel ?? '';
  row.querySelector('.nodes').textContent = nodesLabel;
  const relayCell = row.querySelector('.relay');
  const relayLabel = computeRelayLabel(summary);
  relayCell.textContent = relayLabel;

  const relayMeshId = summary.relay?.meshId || summary.relay?.meshIdNormalized || '';
  if (relayMeshId) {
    const normalizedRelayId = relayMeshId.startsWith('0x') ? `!${relayMeshId.slice(2)}` : relayMeshId;
    if (relayLabel && relayLabel !== normalizedRelayId) {
      relayCell.title = `${relayLabel} (${normalizedRelayId})`;
    } else {
      relayCell.title = normalizedRelayId;
    }
  } else if (relayLabel === '直收') {
    relayCell.title = '訊息為直收，未經其他節點轉發';
  } else if (relayLabel === 'Self') {
    const selfLabel = selfNodeState.name || selfNodeState.meshId || '本站節點';
    relayCell.title = `${selfLabel} 轉發`;
  } else if (relayLabel && relayLabel.includes('?')) {
    relayCell.title = '最後轉發節點未知或標號不完整';
  } else {
    relayCell.removeAttribute('title');
  }
  row.querySelector('.channel').textContent = summary.channel ?? '';
  row.querySelector('.snr').textContent = formatNumber(summary.snr, 2);
  row.querySelector('.rssi').textContent = formatNumber(summary.rssi, 0);
  renderTypeCell(row.querySelector('.type'), summary);
  row.querySelector('.hops').textContent = summary.hops?.label ?? '';
  row.querySelector('.detail-main').textContent = summary.detail || '';

  const fromMeshNormalized = normalizeMeshId(summary.from?.meshId || summary.from?.meshIdNormalized);
  const isMappedNode = fromMeshNormalized ? mappingMeshIds.has(fromMeshNormalized) : false;
  if (summary.type === 'Position' && isMappedNode) {
    row.classList.add('position-highlight');
  }

  const extras = [];
  if (Array.isArray(summary.extraLines) && summary.extraLines.length > 0) {
    extras.push(...summary.extraLines);
  }
  row.querySelector('.detail-extra').textContent = extras.join('\n');

  tableBody.insertBefore(fragment, tableBody.firstChild);

  while (tableBody.children.length > MAX_ROWS) {
    tableBody.removeChild(tableBody.lastChild);
  }

  registerPacketFlow(summary);
}

function clearSummaryTable() {
  if (!tableBody) return;
  while (tableBody.firstChild) {
    tableBody.removeChild(tableBody.firstChild);
  }
  packetBuckets.clear();
  packetSummaryLast10Min = 0;
  clearPacketFlowData();
}

function clearPacketFlowData() {
  flowEntries.length = 0;
  flowEntryIndex.clear();
  flowSearchTerm = '';
  pendingFlowSummaries.clear();
  pendingAprsUplinks.clear();
  aprsCompletedFlowIds.clear();
  aprsCompletedQueue.length = 0;
  totalAprsUploaded = 0;
  if (flowSearchInput) flowSearchInput.value = '';
  renderFlowEntries();
  updateDashboardCounters();
  clearJsonEntries();
}

function clearJsonEntries() {
  jsonEntries.length = 0;
  if (jsonList) {
    while (jsonList.firstChild) {
      jsonList.removeChild(jsonList.firstChild);
    }
  }
  updateJsonCounter();
  updateJsonEmptyState();
}

function handleRawMessage(message) {
  if (!message) return;
  if (!isDecodedPacketMessage(message)) {
    return;
  }
  if (isMessageFromSelf(message)) {
    return;
  }
  appendJsonEntry(message);
}

function appendJsonEntry(message) {
  const sanitized = sanitizeMessageForJson(message);
  const meta = extractJsonMetadata(message);
  const entryData = {
    hash: meta.hash,
    fromMeshId: meta.fromMeshId,
    relayMeshId: meta.relayMeshId,
    relayLabel: meta.relayLabel,
    hopsUsed: meta.hopsUsed,
    hopsTotal: meta.hopsTotal,
    rssi: meta.rssi,
    snr: meta.snr,
    payload: meta.payload,
    packet: sanitized?.packet ?? null,
    message: sanitized
  };
  const entry = {
    id: `json-${jsonEntrySequence++}`,
    data: entryData
  };
  jsonEntries.unshift(entry);

  if (jsonList) {
    const fragment = document.createDocumentFragment();
    fragment.appendChild(renderJsonEntry(entry));
    jsonList.prepend(fragment);
  }

  while (jsonEntries.length > JSON_MAX_ENTRIES) {
    const removed = jsonEntries.pop();
    if (!removed) break;
    if (jsonList) {
      const node = jsonList.querySelector(`[data-entry-id="${removed.id}"]`);
      if (node) {
        jsonList.removeChild(node);
      }
    }
  }

  updateJsonCounter();
  updateJsonEmptyState();
}

function renderJsonEntry(entry) {
  const wrapper = document.createElement('article');
  wrapper.className = 'json-entry';
  wrapper.dataset.entryId = entry.id;

  const data = entry.data;
  const header = document.createElement('div');
  header.className = 'json-entry-meta';
  const metaParts = [];
  const relayLabel = data.relayLabel || '直收';
  metaParts.push(`最後一跳 ${relayLabel}`);
  if (Number.isFinite(data.hopsUsed)) {
    metaParts.push(`目前 ${data.hopsUsed} 跳`);
  } else {
    metaParts.push('目前 跳數未知');
  }
  if (Number.isFinite(data.hopsTotal)) {
    metaParts.push(`總共 ${data.hopsTotal} 跳`);
  } else {
    metaParts.push('總共 跳數未知');
  }
  if (Number.isFinite(data.rssi)) {
    metaParts.push(`RSSI ${data.rssi} dBm`);
  }
  if (Number.isFinite(data.snr)) {
    metaParts.push(`SNR ${data.snr.toFixed(2)} dB`);
  }
  header.textContent = metaParts.join(' • ');

  const body = document.createElement('pre');
  body.className = 'json-entry-body';
  body.textContent = JSON.stringify(data, null, 2);

  wrapper.append(header, body);
  return wrapper;
}

function extractMessageMeshId(message) {
  if (!message || !message.packet) return null;
  const numeric =
    message.packet.from ??
    message.packet.fromId ??
    message.packet.from_id ??
    message.packet.fromNode ??
    message.packet.from_node ??
    null;
  if (numeric == null) return null;
  const meshId = meshIdFromNumeric(numeric);
  return meshId ? normalizeMeshId(meshId) : null;
}

function meshIdFromNumeric(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  const unsigned = num >>> 0;
  const hex = unsigned.toString(16).padStart(8, '0');
  return `!${hex}`;
}

function extractJsonMetadata(message) {
  const packet = message?.packet || {};
  const fromMeshId = extractMessageMeshId(message);
  const payload =
    typeof packet?.decoded?.payload === 'string' ? packet.decoded.payload : null;
  const relayInfo = resolveRelayInfo(packet);
  const hopStats = computeHopStats(packet);
  const hash = computePacketHash(packet, {
    fromMeshId,
    payload,
    hopsUsed: hopStats.used,
    hopsTotal: hopStats.total
  });
  return {
    fromMeshId,
    relayMeshId: relayInfo.meshId,
    relayLabel: relayInfo.label,
    hopsUsed: hopStats.used,
    hopsTotal: hopStats.total,
    rssi: Number.isFinite(packet.rxRssi) ? Number(packet.rxRssi) : null,
    snr: Number.isFinite(packet.rxSnr) ? Number(packet.rxSnr) : null,
    payload,
    hash
  };
}

function resolveRelayInfo(packet) {
  const relayNode = packet?.relayNode;
  if (!Number.isFinite(relayNode) || relayNode === 0) {
    return { label: '直收', meshId: null };
  }
  const meshId = meshIdFromNumeric(relayNode);
  if (!meshId) {
    return { label: '未知節點', meshId: null };
  }
  const normalized = normalizeMeshId(meshId);
  if (isSelfMeshId(meshId)) {
    return { label: '本站節點', meshId: normalized };
  }
  return {
    label: normalized.toUpperCase(),
    meshId: normalized
  };
}

function computeHopStats(packet) {
  const hopStart = Number(packet?.hopStart);
  const hopLimit = Number(packet?.hopLimit);
  let used = null;
  if (Number.isFinite(hopStart) && Number.isFinite(hopLimit)) {
    used = Math.max(hopStart - hopLimit, 0);
  }
  const total = Number.isFinite(hopStart)
    ? hopStart
    : Number.isFinite(hopLimit)
      ? hopLimit
      : null;
  return { used, total };
}

function computePacketHash(packet, context = {}) {
  const rawFrom = context.fromMeshId || meshIdFromNumeric(packet?.from) || '';
  const fromMeshId = rawFrom ? normalizeMeshId(rawFrom) : '';
  const payload = context.payload || '';
  const idPart = Number.isFinite(packet?.id) ? String(packet.id >>> 0) : '';
  const timePart = Number.isFinite(packet?.rxTime) ? String(packet.rxTime >>> 0) : '';
  const usedHop = Number.isFinite(context.hopsUsed) ? String(context.hopsUsed) : '';
  const totalHop = Number.isFinite(context.hopsTotal) ? String(context.hopsTotal) : '';
  const raw = `${fromMeshId}|${payload}|${idPart}|${timePart}|${usedHop}|${totalHop}`;
  return fnv1aHash(raw).toUpperCase();
}

function fnv1aHash(input) {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
}

function isMessageFromSelf(message) {
  const meshId = extractMessageMeshId(message);
  if (!meshId) return false;
  return isSelfMeshId(meshId);
}

function pruneJsonEntriesForSelf() {
  const meshId = selfNodeState?.normalizedMeshId;
  if (!meshId || !jsonEntries.length) {
    return;
  }
  let removedAny = false;
  for (let i = jsonEntries.length - 1; i >= 0; i -= 1) {
    const entry = jsonEntries[i];
    const entryMesh = entry.data?.fromMeshId;
    if (entryMesh && entryMesh === meshId) {
      const [removed] = jsonEntries.splice(i, 1);
      removedAny = true;
      if (removed && jsonList) {
        const node = jsonList.querySelector(`[data-entry-id="${removed.id}"]`);
        node?.remove();
      }
    }
  }
  if (removedAny) {
    updateJsonCounter();
    updateJsonEmptyState();
  }
}

function updateJsonCounter() {
  if (!jsonEntryCount) return;
  jsonEntryCount.textContent = `共 ${jsonEntries.length} 筆`;
}

function updateJsonEmptyState() {
  if (!jsonEmptyState || !jsonList) return;
  const hasEntries = jsonEntries.length > 0;
  jsonEmptyState.classList.toggle('hidden', hasEntries);
  jsonList.classList.toggle('hidden', !hasEntries);
}

function sanitizeMessageForJson(value) {
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeMessageForJson(item));
  }
  if (!value || typeof value !== 'object') {
    return value;
  }
  const result = {};
  for (const [key, original] of Object.entries(value)) {
    if (JSON_OMIT_KEYS.has(key)) {
      continue;
    }
    result[key] = sanitizeMessageForJson(original);
  }
  return result;
}

function isDecodedPacketMessage(message) {
  if (!message || message.payloadVariant !== 'packet') {
    return false;
  }
  const packet = message.packet;
  if (!packet || packet.payloadVariant !== 'decoded') {
    return false;
  }
  const decoded = packet.decoded;
  if (!decoded || typeof decoded !== 'object') {
    return false;
  }
  const payload = decoded.payload;
  return typeof payload === 'string' && payload.length > 0;
}

function registerPacketFlow(summary, { skipPending = false } = {}) {
  if (!summary) return;
  if (summary.type !== 'Position') {
    return;
  }
  if (flowCaptureEnabledAt && Date.now() < flowCaptureEnabledAt) {
    return;
  }
  const meshId = normalizeMeshId(summary.from?.meshId || summary.from?.meshIdNormalized);
  if (!meshId) return;
  const relayLabel = computeRelayLabel(summary);

  const timestampMs = extractSummaryTimestamp(summary);
  const flowId = typeof summary.flowId === 'string' && summary.flowId.length
    ? summary.flowId
    : `${timestampMs}-${Math.random().toString(16).slice(2, 10)}`;
  summary.flowId = flowId;

  const mapping = findMappingByMeshId(meshId);
  if (!mapping) {
    if (!skipPending) {
      const bucket = pendingFlowSummaries.get(meshId) || new Map();
      if (!bucket.has(flowId)) {
        try {
          const snapshot = JSON.parse(JSON.stringify(summary));
          snapshot.flowId = flowId;
          bucket.set(flowId, snapshot);
          while (bucket.size > 25) {
            const oldestKey = bucket.keys().next().value;
            if (oldestKey) {
              bucket.delete(oldestKey);
            }
          }
          pendingFlowSummaries.set(meshId, bucket);
        } catch (err) {
          console.warn('無法快取待處理的 Mapping 封包:', err);
        }
      }
    }
    return;
  }

  if (!skipPending) {
    const bucket = pendingFlowSummaries.get(meshId);
    if (bucket) {
      bucket.delete(flowId);
      if (bucket.size === 0) {
        pendingFlowSummaries.delete(meshId);
      }
    }
  }

  const fromLabel = formatNodeDisplay(summary.from);
  const toLabel = summary.to ? formatNodeDisplay(summary.to) : '';
  const mappingLabel = formatMappingLabel(mapping);
  const mappingCallsign = formatMappingCallsign(mapping);
  const mappingComment = extractMappingComment(mapping) || '';
  const hopInfo = extractHopInfo(summary);
  const hopsLabel = hopInfo.hopsLabel || summary.hops?.label || null;
  const position = summary.position || {};
  const latitude = Number.isFinite(position.latitude) ? Number(position.latitude) : null;
  const longitude = Number.isFinite(position.longitude) ? Number(position.longitude) : null;
  const altitude = Number.isFinite(position.altitude) ? Number(position.altitude) : null;
  const speedKph = Number.isFinite(position.speedKph)
    ? Number(position.speedKph)
    : Number.isFinite(position.speedMps)
      ? Number(position.speedMps) * 3.6
      : Number.isFinite(position.speedKnots)
        ? Number(position.speedKnots) * 1.852
        : null;
  const sats = Number.isFinite(position.satsInView) ? Number(position.satsInView) : null;

  const entry = {
    flowId,
    meshId,
    timestampMs,
    timestampLabel: summary.timestampLabel || formatFlowTimestamp(timestampMs),
    type: summary.type || 'Unknown',
    icon: TYPE_ICONS[summary.type] || '📦',
    fromLabel,
    pathLabel: toLabel ? `${fromLabel} → ${toLabel}` : fromLabel,
    detail: summary.detail || '',
    channel: summary.channel ?? '',
    snr: Number.isFinite(summary.snr) ? Number(summary.snr) : null,
    rssi: Number.isFinite(summary.rssi) ? Number(summary.rssi) : null,
    extras: Array.isArray(summary.extraLines) ? summary.extraLines.slice(0, 4) : [],
    mappingLabel,
    callsign: mappingCallsign,
    comment: mappingComment,
    hopsLabel,
    hopsUsed: hopInfo.usedHops,
    hopsTotal: hopInfo.totalHops,
    latitude,
    longitude,
    altitude,
    speedKph: Number.isFinite(speedKph) ? speedKph : null,
    satsInView: sats,
    relayLabel,
    aprs: null
  };

  const pendingAprs = pendingAprsUplinks.get(flowId);
  if (pendingAprs) {
    entry.aprs = pendingAprs;
    pendingAprsUplinks.delete(flowId);
  }

  flowEntries.unshift(entry);
  flowEntryIndex.set(flowId, entry);
  if (flowEntries.length > FLOW_MAX_ENTRIES) {
    const removed = flowEntries.pop();
    if (removed) {
      flowEntryIndex.delete(removed.flowId);
      pendingAprsUplinks.delete(removed.flowId);
    }
  }

  renderFlowEntries();
  updateDashboardCounters();
}

function flushPendingFlowSummaries() {
  if (!pendingFlowSummaries.size) return;
  const meshIds = Array.from(pendingFlowSummaries.keys());
  meshIds.forEach((meshId) => {
    if (!findMappingByMeshId(meshId)) {
      return;
    }
    const bucket = pendingFlowSummaries.get(meshId);
    if (!bucket || !bucket.size) {
      pendingFlowSummaries.delete(meshId);
      return;
    }
    pendingFlowSummaries.delete(meshId);
    bucket.forEach((snapshot) => {
      registerPacketFlow(snapshot, { skipPending: true });
    });
  });
  updateDashboardCounters();
}

function getFilteredFlowEntries() {
  const term = flowSearchTerm;
  let filtered = term
    ? flowEntries.filter((entry) => flowEntryMatches(entry, term))
    : flowEntries;

  if (flowFilterState === 'aprs') {
    filtered = filtered.filter((entry) => Boolean(entry.aprs));
  } else if (flowFilterState === 'pending') {
    filtered = filtered.filter((entry) => !entry.aprs);
  }

  return filtered;
}

function sanitizeFlowLeftText(value, { stripAltitude = false } = {}) {
  if (value == null) return '';
  let text = String(value).trim();
  if (!text) return '';
  if (stripAltitude) {
    ALT_TOKEN_REGEX.lastIndex = 0;
    text = text.replace(ALT_TOKEN_REGEX, '');
  }
  text = text.replace(/\s*·\s*$/, '');
  text = text.replace(/\s{2,}/g, ' ').trim();
  return text;
}

function renderFlowEntries() {
  if (!flowList) return;
  const filtered = getFilteredFlowEntries();

  flowList.innerHTML = '';

  if (!flowEntries.length) {
    flowList.classList.add('hidden');
    if (flowEmptyState) {
      flowEmptyState.textContent = '尚未收到封包。';
      flowEmptyState.classList.remove('hidden');
    }
    return;
  }

  if (!filtered.length) {
    flowList.classList.add('hidden');
    if (flowEmptyState) {
      flowEmptyState.textContent = '沒有符合搜尋條件的封包。';
      flowEmptyState.classList.remove('hidden');
    }
    return;
  }

  flowEmptyState?.classList.add('hidden');
  flowList.classList.remove('hidden');

  filtered.forEach((entry) => {
    const item = document.createElement('div');
    item.className = 'flow-item';
    if (entry.aprs) {
      item.classList.add('flow-item-has-aprs');
    } else {
      item.classList.add('flow-item-pending');
    }

    const header = document.createElement('div');
    header.className = 'flow-item-header';
    const statusEl = document.createElement('div');
    statusEl.className = `flow-item-status ${entry.aprs ? 'flow-item-status--uploaded' : 'flow-item-status--pending'}`;
    statusEl.textContent = entry.aprs ? '已上傳 APRS' : '待上傳';
    header.appendChild(statusEl);
    item.appendChild(header);

    const mappingComment = (entry.comment || '').trim();
    const infoRow = document.createElement('div');
    infoRow.className = 'flow-item-row';

    const colLeft = document.createElement('div');
    colLeft.className = 'flow-item-col flow-item-col-left';
    const colRight = document.createElement('div');
    colRight.className = 'flow-item-col flow-item-col-right';

    const leftTexts = new Set();
    const hasAltitudeChip = Number.isFinite(entry.altitude);
    const appendLeft = (text, className) => {
      const value = sanitizeFlowLeftText(text, { stripAltitude: hasAltitudeChip });
      if (!value || leftTexts.has(value)) return;
      const el = document.createElement('div');
      el.className = className;
      el.textContent = value;
      colLeft.appendChild(el);
      leftTexts.add(value);
    };

    appendLeft(entry.pathLabel, 'flow-item-path');
    appendLeft(entry.callsign, 'flow-item-callsign');
    if (entry.mappingLabel !== entry.callsign) {
      appendLeft(entry.mappingLabel, 'flow-item-mapping');
    }
    appendLeft(mappingComment, 'flow-item-comment');
    if (entry.detail !== mappingComment && entry.detail !== entry.mappingLabel) {
      appendLeft(entry.detail, 'flow-item-detail');
    }

    if (!colLeft.childElementCount) {
      const placeholder = document.createElement('div');
      placeholder.className = 'flow-item-placeholder';
      colLeft.appendChild(placeholder);
    }

    const metaParts = [];
    if (entry.channel) metaParts.push(`<span class="chip chip-channel">Ch ${entry.channel}</span>`);
    if (entry.hopsLabel) {
      metaParts.push(`<span class="chip chip-hops">${entry.hopsLabel}</span>`);
    } else if (Number.isFinite(entry.hopsUsed) || Number.isFinite(entry.hopsTotal)) {
      const hopsUsedLabel = Number.isFinite(entry.hopsUsed) ? entry.hopsUsed : '?';
      const hopsTotalLabel = Number.isFinite(entry.hopsTotal) ? entry.hopsTotal : '?';
      metaParts.push(`<span class="chip chip-hops">Hops ${hopsUsedLabel}/${hopsTotalLabel}</span>`);
    }
    if (entry.relayLabel) {
      metaParts.push(`<span class="chip chip-relay">${entry.relayLabel}</span>`);
    }
    if (Number.isFinite(entry.snr)) metaParts.push(`<span class="chip chip-snr">SNR ${entry.snr.toFixed(1)} dB</span>`);
    if (Number.isFinite(entry.rssi)) metaParts.push(`<span class="chip chip-rssi">RSSI ${entry.rssi.toFixed(0)} dBm</span>`);
    if (hasAltitudeChip) metaParts.push(`<span class="chip chip-alt">ALT ${Math.round(entry.altitude)} m</span>`);
    if (Number.isFinite(entry.speedKph)) metaParts.push(`<span class="chip chip-speed">SPD ${entry.speedKph.toFixed(1)} km/h</span>`);
    if (Number.isFinite(entry.satsInView)) metaParts.push(`<span class="chip chip-sats">SAT ${entry.satsInView}</span>`);
    const metaWrap = document.createElement('div');
    metaWrap.className = 'flow-item-meta';
    metaWrap.innerHTML = metaParts.join('');
    colRight.appendChild(metaWrap);

    const timestampEl = document.createElement('div');
    timestampEl.className = 'flow-item-timestamp';
    timestampEl.textContent = entry.timestampLabel;
    colRight.appendChild(timestampEl);

    infoRow.append(colLeft, colRight);
    item.appendChild(infoRow);

    const extraLines = [];
    if (entry.extras.length) extraLines.push(entry.extras.join('\n'));
    if (extraLines.length) {
      const extra = document.createElement('div');
      extra.className = 'flow-item-extra';
      extra.textContent = extraLines.join('\n');
      item.appendChild(extra);
    }

    if (entry.aprs) {
      const aprsBlock = document.createElement('div');
      aprsBlock.className = 'flow-item-aprs';
      const label = document.createElement('div');
      label.className = 'flow-item-aprs-label';
      label.textContent = `APRS ${entry.aprs.timestampLabel}`;
      const frame = document.createElement('div');
      frame.className = 'flow-item-aprs-frame';
      frame.textContent = entry.aprs.frame || entry.aprs.payload;
      aprsBlock.appendChild(label);
      aprsBlock.appendChild(frame);
      item.appendChild(aprsBlock);
    }

    flowList.appendChild(item);
  });
}

function extractSummaryTimestamp(summary) {
  if (!summary) return Date.now();
  const { timestamp } = summary;
  if (typeof timestamp === 'string') {
    const parsed = Date.parse(timestamp);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  if (Number.isFinite(timestamp)) {
    return Number(timestamp);
  }
  if (Number.isFinite(summary.timestampMs)) {
    return Number(summary.timestampMs);
  }
  return Date.now();
}

function formatFlowTimestamp(ms) {
  const date = new Date(ms);
  const hh = `${date.getHours()}`.padStart(2, '0');
  const mm = `${date.getMinutes()}`.padStart(2, '0');
  const ss = `${date.getSeconds()}`.padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

function findMappingByMeshId(meshId) {
  if (!meshId || !Array.isArray(mappingItems)) return null;
  return mappingItems.find((item) => normalizeMeshId(item.mesh_id ?? item.meshId) === meshId) || null;
}

function refreshFlowEntryLabels() {
  let updated = false;
  flowEntries.forEach((entry) => {
    const mapping = findMappingByMeshId(entry.meshId);
    const newMappingLabel = formatMappingLabel(mapping);
    const newCallsign = formatMappingCallsign(mapping);
    const newComment = extractMappingComment(mapping) || entry.detail || '';
    if (entry.mappingLabel !== newMappingLabel) {
      entry.mappingLabel = newMappingLabel;
      updated = true;
    }
    if (entry.callsign !== newCallsign) {
      entry.callsign = newCallsign;
      updated = true;
    }
    if (entry.comment !== newComment) {
      entry.comment = newComment;
      updated = true;
    }
  });
  if (updated) {
    renderFlowEntries();
  }
}

function handleAprsUplink(info) {
  if (!info || !info.flowId) return;
  const flowId = info.flowId;
  const entry = flowEntryIndex.get(flowId);
  const timestampMs = Number.isFinite(Number(info.timestamp)) ? Number(info.timestamp) : Date.now();
  const aprsRecord = {
    frame: info.frame || info.payload || '',
    payload: info.payload || '',
    timestampMs,
    timestampLabel: formatFlowTimestamp(timestampMs)
  };
  if (!entry) {
    const incremented = markAprsUploaded(flowId);
    pendingAprsUplinks.set(flowId, aprsRecord);
    if (incremented) {
      updateDashboardCounters();
    }
    return;
  }
  const hadAprs = Boolean(entry.aprs);
  const incremented = markAprsUploaded(flowId);
  entry.aprs = aprsRecord;
  renderFlowEntries();
  if (incremented || !hadAprs) {
    updateDashboardCounters();
  }
}

function flowEntryMatches(entry, term) {
  if (!term) return true;
  const haystacks = [
    entry.fromLabel,
    entry.pathLabel,
    entry.mappingLabel,
    entry.callsign,
    entry.relayLabel,
    entry.detail,
    entry.comment,
    entry.meshId,
    entry.aprs?.frame,
    entry.aprs?.payload
  ];
  if (Array.isArray(entry.extras)) {
    haystacks.push(entry.extras.join(' '));
  }
  return haystacks.some((value) => {
    if (!value) return false;
    return String(value).toLowerCase().includes(term);
  });
}

function formatLatLonForCsv(value) {
  if (!Number.isFinite(value)) return '';
  const fixed = value.toFixed(6);
  const trimmed = fixed.replace(/0+$/, '').replace(/\.$/, '');
  return trimmed.length ? trimmed : '0';
}

function formatNumericForCsv(value, digits = null) {
  if (!Number.isFinite(value)) return '';
  if (digits === null) {
    return String(value);
  }
  const fixed = value.toFixed(digits);
  return fixed.replace(/0+$/, '').replace(/\.$/, '') || '0';
}

function escapeCsvValue(value) {
  if (value === null || value === undefined) return '';
  const str = String(value);
  if (str.includes('"') || str.includes(',') || str.includes('\n') || str.includes('\r')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function downloadFlowCsv() {
  const exportEntries = getFilteredFlowEntries();
  if (!exportEntries.length) {
    appendLog('FLOW', 'no mapping entries available for export');
    return;
  }

  const header = ['時間', '呼號', 'MeshID', '最後轉發', '目前跳數', '總跳數', 'SNR', 'RSSI', 'Lat', 'Lon', '高度', '速度', 'comment'];
  const rows = exportEntries.map((entry) => {
    const unixTime = Number.isFinite(entry.timestampMs) ? Math.floor(entry.timestampMs / 1000) : '';
    const snrValue = formatNumericForCsv(entry.snr, 2);
    const rssiValue = formatNumericForCsv(entry.rssi, 0);
    const latValue = formatLatLonForCsv(entry.latitude);
    const lonValue = formatLatLonForCsv(entry.longitude);
    const altitudeValue = formatNumericForCsv(entry.altitude, 1);
    const speedValue = formatNumericForCsv(entry.speedKph, 1);
    const hopsUsed = Number.isFinite(entry.hopsUsed) ? entry.hopsUsed : '';
    const hopsTotal = Number.isFinite(entry.hopsTotal) ? entry.hopsTotal : '';

    const fields = [
      unixTime,
      entry.callsign || entry.mappingLabel || '',
      entry.meshId || '',
      entry.relayLabel || '',
      hopsUsed,
      hopsTotal,
      snrValue,
      rssiValue,
      latValue,
      lonValue,
      altitudeValue,
      speedValue,
      entry.comment || ''
    ];

    return fields.map(escapeCsvValue).join(',');
  });

  const csvContent = [header.map(escapeCsvValue).join(','), ...rows].join('\n');
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `mapping-log-${timestamp}.csv`;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  appendLog('FLOW', `exported ${rows.length} mapping entries to CSV`);
}

function formatMappingLabel(mapping) {
  if (!mapping) return null;
  const commentRaw = extractMappingComment(mapping) || '';
  const callsign = formatMappingCallsign(mapping) || '';
  if (commentRaw) {
    const trimmed = callsign
      ? commentRaw.replace(new RegExp(`^${callsign.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?:[\s·-]+)?`), '')
      : commentRaw;
    return trimmed.trim() || callsign || null;
  }
  return callsign || null;
}

function formatMappingCallsign(mapping) {
  if (!mapping) return null;
  const baseRaw = mapping.callsign_base ?? mapping.callsignBase ?? mapping.callsign ?? null;
  const ssidValue = mapping.aprs_ssid ?? mapping.aprsSsid ?? mapping.ssid ?? mapping.SSID ?? null;
  if (!baseRaw) return null;
  const ssidNum = Number(ssidValue);
  if (Number.isFinite(ssidNum) && ssidNum !== 0) {
    const suffixPattern = new RegExp(`-${ssidNum}$`);
    if (suffixPattern.test(baseRaw)) {
      return baseRaw.replace(/--+/g, '-');
    }
    const trimmed = baseRaw.endsWith('-') ? baseRaw.slice(0, -1) : baseRaw;
    return `${trimmed}-${ssidNum}`.replace(/--+/g, '-');
  }
  return (baseRaw.endsWith('-') ? baseRaw.slice(0, -1) : baseRaw).replace(/--+/g, '-');
}

function extractMappingComment(mapping) {
  if (!mapping) return '';
  return (
    mapping.comment ??
    mapping.aprs_comment ??
    mapping.aprsComment ??
    mapping.notes ??
    ''
  );
}

function formatNodes(summary) {
  const fromLabel = formatNodeDisplay(summary.from);
  const toLabel = summary.to ? formatNodeDisplay(summary.to) : null;
  if (!toLabel) {
    return fromLabel;
  }
  return `${fromLabel} → ${toLabel}`;
}

function formatNodeDisplay(node) {
  if (!node) {
    return 'unknown';
  }
  const name = node.longName || (node.label && node.label !== 'unknown' ? node.label : null);
  let meshId = node.meshId;
  if (meshId && meshId.startsWith('0x')) {
    meshId = `!${meshId.slice(2)}`;
  }
  if (name && meshId) {
    return name.includes(meshId) ? name : `${name} (${meshId})`;
  }
  if (name) {
    return name;
  }
  if (meshId) {
    return meshId;
  }
  return 'unknown';
}

function formatNumber(value, digits) {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return '';
  }
  return value.toFixed(digits);
}

function setConnectingState(nextState) {
  isConnecting = nextState;
  if (isConnecting) {
    isConnected = false;
  }
  updateToggleButton();
}

function setConnectedState(nextState) {
  isConnected = nextState;
  if (nextState) {
    isConnecting = false;
    manualDisconnect = false;
    autoConnectAttempts = 0;
    lastActivityAt = Date.now();
    flowCaptureEnabledAt = Date.now() + FLOW_CAPTURE_DELAY_MS;
    startInactivityMonitor();
    stopReconnectLoop();
  } else {
    isConnecting = false;
    flowCaptureEnabledAt = 0;
    stopInactivityMonitor();
  }
  updateToggleButton();
}

function updateStatus(status, message, nonce) {
  if (status === 'handshake') {
    return;
  }
  statusIndicator.className = `status status-${status || 'idle'}`;
  const icon = STATUS_ICONS[status] || STATUS_ICONS.idle;
  const labelGenerator = STATUS_LABELS[status] || STATUS_LABELS.idle;
  const label = typeof labelGenerator === 'function' ? labelGenerator(nonce ?? message) : labelGenerator;
  statusIndicator.textContent = `${icon} ${label}`;
}

function renderTypeCell(cell, summary) {
  const type = summary.type ?? '';
  const icon = TYPE_ICONS[type] || '📦';
  cell.innerHTML = '';

  const iconEl = document.createElement('span');
  iconEl.className = 'type-icon';
  iconEl.textContent = icon;

  const textEl = document.createElement('span');
  textEl.textContent = type;

  cell.append(iconEl, textEl);
}

function showDiscoverModal(devices) {
  if (!discoverModal || !discoverModalBody) return;
  discoverModalBody.innerHTML = '';

  if (!devices.length) {
    const empty = document.createElement('div');
    empty.textContent = '尚未偵測到裝置';
    discoverModalBody.appendChild(empty);
  } else {
    devices.forEach((device) => {
      const option = document.createElement('button');
      option.type = 'button';
      option.className = 'device-option';

      const label = document.createElement('span');
      label.textContent = formatDeviceLabel(device);
      const meta = document.createElement('span');
      meta.className = 'device-meta';
      meta.textContent = formatDeviceMeta(device);

      option.append(label, meta);
      option.addEventListener('click', () => {
        applyDiscoveredDevice(device);
      });
      discoverModalBody.appendChild(option);
    });
  }

  discoverModal.classList.remove('hidden');
}

function hideDiscoverModal() {
  if (!discoverModal) return;
  discoverModal.classList.add('hidden');
}

function formatDeviceLabel(device) {
  return device.name || device.host || 'Meshtastic';
}

function formatDeviceMeta(device) {
  const address = device.addresses?.find((a) => isIPv4(a)) || device.host;
  const port = device.port ?? 4403;
  const parts = [];
  if (address) parts.push(`${address}:${port}`);
  if (device.txt) {
    const txtEntries = Object.entries(device.txt)
      .filter(([, value]) => value)
      .map(([key, value]) => `${key}=${value}`);
    if (txtEntries.length) {
      parts.push(txtEntries.join(', '));
    }
  }
  return parts.join(' · ');
}

function applyDiscoveredDevice(device) {
  const address = device.addresses?.find((a) => isIPv4(a)) || device.host;
  if (!address) {
    setDiscoverStatus('該裝置沒有可用的位址', 'warn');
    appendLog('DISCOVER', 'device missing usable address');
    return;
  }
  const overlayActive = isOverlayHostStepVisible();
  settingsHostInput.value = address;
  markHostPreferenceUpdated();
  if (overlayActive || initialSetupAutoConnectPending) {
    initialSetupAutoConnectPending = true;
  }
  savePreferences();
  if (overlayHostInput) {
    overlayHostInput.value = address;
  }
  if (overlayActive) {
    setOverlayHostStatus(`已套用 ${device.name || address}`, 'success');
  }
  updateConnectAvailability();
  setDiscoverStatus(`已套用 ${device.name || address}`, 'success');
  ensureHostGuidance();
  hideDiscoverModal();
  appendLog('DISCOVER', `selected ${device.name || address}`);
  maybeTriggerInitialSetupAutoConnect('host-discovered');
}

function setDiscoveringState(isDiscovering) {
  discoverBtn.disabled = isDiscovering;
  discoverBtn.textContent = isDiscovering ? '掃描中...' : '自動搜尋';
}

function setDiscoverStatus(message, variant = 'info') {
  if (!discoverStatus) return;
  if (!message) {
    discoverStatus.textContent = '';
    delete discoverStatus.dataset.variant;
    hostGuidanceActive = false;
    return;
  }
  discoverStatus.textContent = message;
  discoverStatus.dataset.variant = variant;
  hostGuidanceActive = message === HOST_GUIDANCE_MESSAGE && variant === 'info';
}

function ensureHostGuidance({ force = false } = {}) {
  if (!discoverStatus) return;
  if (hasHost()) {
    if (hostGuidanceActive) {
      setDiscoverStatus('', 'info');
    }
    return;
  }
  if (!force) {
    if (hostGuidanceActive) {
      return;
    }
    const currentVariant = discoverStatus.dataset?.variant || '';
    const currentMessage = (discoverStatus.textContent || '').trim();
    if (currentMessage && currentVariant && currentVariant !== 'info' && currentVariant !== 'warn') {
      return;
    }
  }
  setDiscoverStatus(HOST_GUIDANCE_MESSAGE, 'info');
}

function isOverlayHostStepVisible() {
  return Boolean(overlayStepHost) && !overlayStepHost.classList.contains('hidden');
}

function applyOverlayHost() {
  if (!overlayHostInput) return;
  const value = overlayHostInput.value.trim();
  if (!value) {
    setOverlayHostStatus('請輸入節點 IP', 'error');
    overlayHostInput.focus();
    return;
  }
  const previous = getHostValue();
  const overlayActive = isOverlayHostStepVisible();
  settingsHostInput.value = value;
  if (overlayHostInput) {
    overlayHostInput.value = value;
  }
  markHostPreferenceUpdated();
  if (overlayActive || initialSetupAutoConnectPending) {
    initialSetupAutoConnectPending = true;
  }
  savePreferences();
  resumeAutoReconnect({ reason: 'host-updated', silent: true });
  updateConnectAvailability();
  ensureHostGuidance();
  if (overlayActive) {
    setOverlayHostStatus('節點 IP 已儲存', 'success');
  }
  if (previous !== value) {
    appendLog('CONNECT', 'host updated value=' + value);
  }
  maybeTriggerInitialSetupAutoConnect('overlay-host');
}

function clonePlainObject(value) {
  if (value == null || typeof value !== 'object') {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => clonePlainObject(item));
  }
  const result = {};
  for (const [key, nested] of Object.entries(value)) {
    result[key] = clonePlainObject(nested);
  }
  return result;
}

function resolveTelemetryMeshKey(meshId) {
  if (meshId == null) {
    return '__unknown__';
  }
  const value = String(meshId).trim();
  return value ? value : '__unknown__';
}

function sanitizeTelemetryNode(node) {
  if (!node || typeof node !== 'object') {
    return null;
  }
  const meshId = typeof node.meshId === 'string' ? node.meshId : null;
  const normalized =
    node.meshIdNormalized || normalizeMeshId(meshId) || normalizeMeshId(node.mesh_id);
  return {
    label: node.label ?? null,
    meshId,
    meshIdNormalized: normalized,
    shortName: node.shortName ?? null,
    longName: node.longName ?? null,
    hwModel: node.hwModel ?? null,
    role: node.role ?? null
  };
}

function sanitizeTelemetryPayload(payload) {
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  const metricsRaw = payload.metrics && typeof payload.metrics === 'object' ? payload.metrics : {};
  const metrics = clonePlainObject(metricsRaw);
  if (!Object.keys(metrics).length) {
    return null;
  }
  const timeSeconds = Number.isFinite(payload.timeSeconds) ? Number(payload.timeSeconds) : null;
  const timeMs = Number.isFinite(payload.timeMs)
    ? Number(payload.timeMs)
    : timeSeconds != null
      ? timeSeconds * 1000
      : null;
  return {
    kind: payload.kind || 'unknown',
    timeSeconds,
    timeMs,
    metrics
  };
}

function sanitizeTelemetryRecord(record, meshId) {
  if (!record || typeof record !== 'object') {
    return null;
  }
  const telemetry = sanitizeTelemetryPayload(record.telemetry);
  if (!telemetry) {
    return null;
  }
  const effectiveMeshId = meshId || record.meshId || null;
  const timestampMs = Number.isFinite(record.timestampMs)
    ? Number(record.timestampMs)
    : Number.isFinite(record.timestamp)
      ? Number(record.timestamp)
      : Date.now();
  const sampleTimeMs = Number.isFinite(record.sampleTimeMs)
    ? Number(record.sampleTimeMs)
    : (telemetry.timeMs != null && Number.isFinite(telemetry.timeMs)
        ? Number(telemetry.timeMs)
        : timestampMs);
  const nodeInfo = sanitizeTelemetryNode(record.node);
  const id =
    typeof record.id === 'string' && record.id.trim()
      ? record.id.trim()
      : `${effectiveMeshId || 'unknown'}-${timestampMs}-${Math.random().toString(16).slice(2, 10)}`;
  return {
    id,
    meshId: effectiveMeshId,
    node: nodeInfo,
    timestampMs,
    timestamp: record.timestamp || new Date(timestampMs).toISOString(),
    sampleTimeMs,
    sampleTime: record.sampleTime || new Date(sampleTimeMs).toISOString(),
    type: record.type || '',
    detail: record.detail || '',
    channel: Number.isFinite(record.channel) ? Number(record.channel) : record.channel ?? null,
    snr: Number.isFinite(record.snr) ? Number(record.snr) : null,
    rssi: Number.isFinite(record.rssi) ? Number(record.rssi) : null,
    flowId: record.flowId || null,
    telemetry
  };
}

function clearTelemetryDataLocal({ silent = false } = {}) {
  telemetryStore.clear();
  telemetryRecordIds.clear();
  if (!silent) {
    telemetryUpdatedAt = Date.now();
  }
  telemetryChartMetric = null;
  destroyAllTelemetryCharts();
  if (telemetryChartMetricSelect) {
    telemetryChartMetricSelect.innerHTML = '';
    telemetryChartMetricSelect.classList.add('hidden');
  }
  refreshTelemetrySelectors();
  renderTelemetryView();
  updateTelemetryUpdatedAtLabel();
}

function applyTelemetrySnapshot(snapshot) {
  const previousSelection = telemetrySelectedMeshId;
  clearTelemetryDataLocal({ silent: true });
  if (!snapshot || !Array.isArray(snapshot.nodes)) {
    telemetrySelectedMeshId = null;
    telemetryUpdatedAt = snapshot?.updatedAt ?? telemetryUpdatedAt ?? null;
    refreshTelemetrySelectors();
    renderTelemetryView();
    updateTelemetryUpdatedAtLabel();
    return;
  }
  for (const node of snapshot.nodes) {
    const meshId = node?.meshId;
    if (!meshId) continue;
    const nodeInfo = sanitizeTelemetryNode(node.node);
    const records = Array.isArray(node.records) ? node.records : [];
    const sanitizedRecords = [];
    for (const rawRecord of records) {
      const sanitized = sanitizeTelemetryRecord(rawRecord, meshId);
      if (!sanitized) continue;
      if (telemetryRecordIds.has(sanitized.id)) continue;
      sanitizedRecords.push(sanitized);
    }
    if (!sanitizedRecords.length) {
      continue;
    }
    sanitizedRecords.sort((a, b) => a.sampleTimeMs - b.sampleTimeMs);
    if (sanitizedRecords.length > TELEMETRY_MAX_LOCAL_RECORDS) {
      sanitizedRecords.splice(0, sanitizedRecords.length - TELEMETRY_MAX_LOCAL_RECORDS);
    }
    sanitizedRecords.forEach((item) => telemetryRecordIds.add(item.id));
    const meshKey = resolveTelemetryMeshKey(meshId);
    telemetryStore.set(meshKey, {
      meshId: meshKey,
      rawMeshId: meshId,
      node: nodeInfo,
      records: sanitizedRecords
    });
  }
  telemetryUpdatedAt = snapshot.updatedAt ?? Date.now();
  telemetrySelectedMeshId = previousSelection;
  refreshTelemetrySelectors();
  if (!telemetrySelectedMeshId && telemetryStore.size) {
    telemetrySelectedMeshId = telemetryStore.keys().next().value || null;
    if (telemetryNodeSelect && telemetrySelectedMeshId) {
      telemetryNodeSelect.value = telemetrySelectedMeshId;
    }
  }
  renderTelemetryView();
  updateTelemetryUpdatedAtLabel();
}

function handleTelemetryEvent(payload) {
  if (!payload || typeof payload !== 'object') {
    return;
  }
  if (payload.type === 'reset') {
    telemetryUpdatedAt = Number.isFinite(payload.updatedAt) ? Number(payload.updatedAt) : Date.now();
    clearTelemetryDataLocal({ silent: true });
    updateTelemetryUpdatedAtLabel();
    return;
  }
  if (payload.type === 'append') {
    appendTelemetryRecord(payload.meshId, payload.record, payload.node, payload.updatedAt);
  }
}

function appendTelemetryRecord(meshId, rawRecord, rawNode, updatedAt) {
  const sanitizedRecord = sanitizeTelemetryRecord(rawRecord, meshId);
  if (!sanitizedRecord) {
    return;
  }
  if (telemetryRecordIds.has(sanitizedRecord.id)) {
    return;
  }
  const rawMeshId = sanitizedRecord.meshId || meshId || null;
  const targetMeshKey = resolveTelemetryMeshKey(rawMeshId);
  let bucket = telemetryStore.get(targetMeshKey);
  const nodeInfo = sanitizeTelemetryNode(rawNode) || sanitizedRecord.node;
  if (!bucket) {
    bucket = {
      meshId: targetMeshKey,
      rawMeshId,
      node: nodeInfo,
      records: []
    };
    telemetryStore.set(targetMeshKey, bucket);
  } else if (nodeInfo) {
    bucket.node = {
      ...bucket.node,
      ...nodeInfo
    };
  }
  bucket.records.push(sanitizedRecord);
  telemetryRecordIds.add(sanitizedRecord.id);
  if (bucket.records.length > TELEMETRY_MAX_LOCAL_RECORDS) {
    const removed = bucket.records.splice(0, bucket.records.length - TELEMETRY_MAX_LOCAL_RECORDS);
    removed.forEach((item) => telemetryRecordIds.delete(item.id));
  }
  telemetryUpdatedAt = Number.isFinite(updatedAt) ? Number(updatedAt) : Date.now();
  const previousSelection = telemetrySelectedMeshId;
  refreshTelemetrySelectors();
  if (!telemetrySelectedMeshId && previousSelection) {
    telemetrySelectedMeshId = previousSelection;
    if (telemetryNodeSelect) {
      telemetryNodeSelect.value = telemetrySelectedMeshId;
    }
  }
  if (!telemetrySelectedMeshId) {
    telemetrySelectedMeshId = targetMeshKey;
    if (telemetryNodeSelect) {
      telemetryNodeSelect.value = targetMeshKey;
    }
  }
  if (telemetrySelectedMeshId === targetMeshKey) {
    renderTelemetryView();
  }
  updateTelemetryUpdatedAtLabel();
}

function refreshTelemetrySelectors() {
  if (!telemetryNodeSelect) {
    return;
  }
  const previous = telemetrySelectedMeshId;
  const { startMs, endMs } = getTelemetryRangeWindow();
  const nodes = Array.from(telemetryStore.values())
    .map((bucket) => {
      if (!Array.isArray(bucket.records) || !bucket.records.length) {
        return null;
      }
      const metricKeys = new Set();
      for (const record of bucket.records) {
        const time = Number(record.sampleTimeMs);
        if (!Number.isFinite(time)) {
          continue;
        }
        if (startMs != null && time < startMs) {
          continue;
        }
        if (endMs != null && time > endMs) {
          continue;
        }
        const metrics = record.telemetry?.metrics;
        if (metrics && typeof metrics === 'object') {
          for (const key of Object.keys(metrics)) {
            metricKeys.add(key);
          }
        }
      }
      const metricsCount = metricKeys.size;
      if (metricsCount === 0) {
        return null;
      }
      const meshKey = bucket.meshId || resolveTelemetryMeshKey(bucket.rawMeshId);
      const meshIdDisplay = bucket.rawMeshId || meshKey || 'unknown';
      const nodeInfo = bucket.node || {};
      const displayNode = {
        ...nodeInfo,
        meshId: nodeInfo.meshId || meshKey || meshIdDisplay,
        longName: nodeInfo.longName || nodeInfo.label || meshIdDisplay,
        label: nodeInfo.label || nodeInfo.longName || meshIdDisplay
      };
      const label = formatNodeDisplay(displayNode);
      return {
        meshId: meshKey,
        label,
        count: metricsCount
      };
    })
    .filter(Boolean);
  if (!nodes.length) {
    telemetryNodeSelect.innerHTML = '';
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = '尚未收到遙測資料';
    placeholder.disabled = true;
    placeholder.selected = true;
    telemetryNodeSelect.appendChild(placeholder);
    telemetryNodeSelect.disabled = true;
    telemetrySelectedMeshId = null;
    return;
  }
  nodes.sort((a, b) => {
    if (b.count !== a.count) {
      return b.count - a.count;
    }
    return a.label.localeCompare(b.label, 'zh-Hant', { sensitivity: 'base' });
  });
  const fragment = document.createDocumentFragment();
  for (const item of nodes) {
    const option = document.createElement('option');
    option.value = item.meshId;
    option.textContent = item.label;
    fragment.appendChild(option);
  }
  telemetryNodeSelect.innerHTML = '';
  telemetryNodeSelect.appendChild(fragment);
  if (!nodes.length) {
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = '尚無節點';
    placeholder.disabled = true;
    placeholder.selected = true;
    telemetryNodeSelect.appendChild(placeholder);
    telemetryNodeSelect.disabled = true;
    telemetrySelectedMeshId = null;
    return;
  }
  telemetryNodeSelect.disabled = false;
  const hasPrevious = previous && nodes.some((node) => node.meshId === previous);
  if (hasPrevious) {
    telemetrySelectedMeshId = previous;
  } else {
    telemetrySelectedMeshId = nodes[0].meshId;
  }
  telemetryNodeSelect.value = telemetrySelectedMeshId;
}

function getTelemetryRecordsForSelection() {
  if (!telemetryStore.size || !telemetrySelectedMeshId) {
    return [];
  }
  const bucket = telemetryStore.get(telemetrySelectedMeshId);
  if (!bucket) {
    return [];
  }
  return bucket.records
    .slice()
    .sort((a, b) => b.sampleTimeMs - a.sampleTimeMs);
}

function renderTelemetryView() {
  if (!telemetryTableBody || !telemetryEmptyState) {
    return;
  }
  if (!telemetrySelectedMeshId && telemetryStore.size) {
    const firstKey = telemetryStore.keys().next().value;
    telemetrySelectedMeshId = firstKey || null;
    if (telemetryNodeSelect && firstKey) {
      telemetryNodeSelect.value = firstKey;
    }
  }
  const baseRecords = getTelemetryRecordsForSelection();
  const filteredRecords = applyTelemetryFilters(baseRecords);
  const hasData = filteredRecords.length > 0;
  const hasBase = baseRecords.length > 0;
  telemetryEmptyState.classList.toggle('hidden', hasData);
  telemetryChartsContainer?.classList.toggle('hidden', !hasData);
  telemetryTableWrapper?.classList.toggle('hidden', !hasData);
  if (!hasData) {
    if (!hasBase) {
      telemetryEmptyState.textContent = '尚未收到遙測資料。';
    } else {
      telemetryEmptyState.textContent = '所選區間沒有資料。';
    }
    destroyAllTelemetryCharts();
    if (telemetryChartsContainer) {
      telemetryChartsContainer.innerHTML = '';
    }
    telemetryTableBody.innerHTML = '';
    return;
  }
  renderTelemetryCharts(filteredRecords);
  renderTelemetryTable(filteredRecords);
}

function collectTelemetrySeries(records) {
  const seriesMap = new Map();
  const sorted = records
    .slice(0, TELEMETRY_CHART_LIMIT)
    .filter((record) => record?.telemetry?.metrics)
    .sort((a, b) => a.sampleTimeMs - b.sampleTimeMs);
  for (const record of sorted) {
    const metrics = record.telemetry?.metrics;
    if (!metrics) continue;
    for (const [metricName, rawValue] of Object.entries(metrics)) {
      const numeric = Number(rawValue);
      if (!Number.isFinite(numeric)) continue;
      const def = TELEMETRY_METRIC_DEFINITIONS[metricName];
      if (def?.chart === false) continue;
      let series = seriesMap.get(metricName);
      if (!series) {
        series = [];
        seriesMap.set(metricName, series);
      }
      const clamped = clampMetricValue(numeric, def);
      series.push({
        time: record.sampleTimeMs,
        value: clamped
      });
    }
  }
  return seriesMap;
}

function updateTelemetryMetricOptions(seriesMap) {
  if (!telemetryChartMetricSelect) {
    return Array.from(seriesMap.keys());
  }
  const metrics = Array.from(seriesMap.keys());
  if (telemetryChartMode !== 'single') {
    telemetryChartMetricSelect.classList.add('hidden');
    telemetryChartMetricSelect.innerHTML = '';
    telemetryChartMetricSelect.disabled = true;
    return metrics;
  }
  if (!metrics.length) {
    telemetryChartMetricSelect.classList.add('hidden');
    telemetryChartMetricSelect.innerHTML = '';
    telemetryChartMetricSelect.disabled = true;
    telemetryChartMetric = null;
    return metrics;
  }
  telemetryChartMetricSelect.classList.remove('hidden');
  telemetryChartMetricSelect.disabled = false;
  const fragment = document.createDocumentFragment();
  for (const metricName of metrics) {
    const def = TELEMETRY_METRIC_DEFINITIONS[metricName];
    const option = document.createElement('option');
    option.value = metricName;
    option.textContent = def?.label || metricName;
    fragment.appendChild(option);
  }
  telemetryChartMetricSelect.innerHTML = '';
  telemetryChartMetricSelect.appendChild(fragment);
  if (!telemetryChartMetric || !seriesMap.has(telemetryChartMetric)) {
    telemetryChartMetric = metrics[0];
  }
  telemetryChartMetricSelect.value = telemetryChartMetric;
  return metrics;
}

function renderTelemetryCharts(records) {
  if (!telemetryChartsContainer) {
    return;
  }
  if (typeof window.Chart !== 'function') {
    console.warn('Chart.js 尚未載入，遙測圖表無法顯示');
    telemetryChartsContainer.classList.add('hidden');
    telemetryChartsContainer.innerHTML = '';
    destroyAllTelemetryCharts();
    return;
  }

  const seriesMap = collectTelemetrySeries(records);
  const metricsList = updateTelemetryMetricOptions(seriesMap);
  let metricsToRender = [];
  if (telemetryChartMode === 'single') {
    if (telemetryChartMetric && !seriesMap.has(telemetryChartMetric)) {
      telemetryChartMetric = metricsList.length ? metricsList[0] : null;
    }
    if (!telemetryChartMetric && metricsList.length) {
      telemetryChartMetric = metricsList[0];
    }
    if (telemetryChartMetricSelect && telemetryChartMetric) {
      telemetryChartMetricSelect.value = telemetryChartMetric;
    }
    metricsToRender = telemetryChartMetric && seriesMap.has(telemetryChartMetric)
      ? [telemetryChartMetric]
      : [];
  } else {
    metricsToRender = metricsList;
  }
  if (!metricsToRender.length) {
    telemetryChartsContainer.classList.add('hidden');
    telemetryChartsContainer.innerHTML = '';
    destroyAllTelemetryCharts();
    return;
  }
  telemetryChartsContainer.classList.remove('hidden');
  destroyAllTelemetryCharts();
  telemetryChartsContainer.innerHTML = '';

  const renderedMetrics = [];
  for (const metricName of metricsToRender) {
    const series = seriesMap.get(metricName);
    if (!Array.isArray(series) || !series.length) {
      continue;
    }
    const def = TELEMETRY_METRIC_DEFINITIONS[metricName] || { label: metricName };
    const card = document.createElement('article');
    card.className = 'telemetry-chart-card';
    const header = document.createElement('div');
    header.className = 'telemetry-chart-header';
    const title = document.createElement('span');
    title.className = 'telemetry-chart-title';
    title.textContent = def.label || metricName;
    const latest = document.createElement('span');
    latest.className = 'telemetry-chart-latest';
    latest.textContent = formatTelemetryValue(metricName, series[series.length - 1].value);
    header.appendChild(title);
    header.appendChild(latest);
    const canvasWrap = document.createElement('div');
    canvasWrap.className = 'telemetry-chart-canvas-wrap';
    const canvas = document.createElement('canvas');
    canvasWrap.appendChild(canvas);
    card.appendChild(header);
    card.appendChild(canvasWrap);
    telemetryChartsContainer.appendChild(card);

    const ctx = canvas.getContext('2d');
    const chart = new window.Chart(ctx, buildTelemetryChartConfig(metricName, def, series));
    telemetryCharts.set(metricName, chart);
    renderedMetrics.push(metricName);
  }

  if (!renderedMetrics.length) {
    telemetryChartsContainer.classList.add('hidden');
    telemetryChartsContainer.innerHTML = '';
  }
}

function buildTelemetryChartConfig(metricName, def, series) {
  const dataset = series.map((point) => ({ x: point.time, y: point.value }));
  const labelText = def.label || metricName;
  return {
    type: 'line',
    data: {
      datasets: [
        {
          label: labelText,
          data: dataset,
          borderColor: '#60a5fa',
          backgroundColor: 'rgba(96, 165, 250, 0.18)',
          pointBackgroundColor: '#bfdbfe',
          pointBorderColor: '#60a5fa',
          pointRadius: 3,
          pointHoverRadius: 4,
          borderWidth: 2,
          tension: 0.2,
          fill: false
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      parsing: false,
      animation: false,
      interaction: {
        mode: 'nearest',
        intersect: false
      },
      scales: {
        x: {
          type: 'linear',
          ticks: {
            color: '#cbd5f5',
            callback: (value) => formatTelemetryAxisTick(value)
          },
          grid: {
            color: 'rgba(148, 163, 184, 0.12)'
          }
        },
        y: {
          ticks: {
            color: '#cbd5f5',
            callback: (value) => formatTelemetryValue(metricName, value) || value
          },
          grid: {
            color: 'rgba(148, 163, 184, 0.12)'
          }
        }
      },
      plugins: {
        legend: {
          display: false
        },
        tooltip: {
          callbacks: {
            title: (items) => {
              if (!items || !items.length) return '';
              return new Date(items[0].parsed.x).toLocaleString();
            },
            label: (ctx) => {
              const value = ctx.parsed?.y;
              const formatted = formatTelemetryValue(metricName, value) || value;
              return `${labelText}: ${formatted}`;
            }
          }
        }
      }
    }
  };
}

function destroyAllTelemetryCharts() {
  for (const chart of telemetryCharts.values()) {
    try {
      chart.destroy();
    } catch {
      /* ignore */
    }
  }
  telemetryCharts.clear();
}

function formatTelemetryAxisTick(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '';
  }
  const date = new Date(numeric);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const mi = String(date.getMinutes()).padStart(2, '0');
  return `${mm}/${dd} ${hh}:${mi}`;
}

function clampMetricValue(value, def) {
  if (!def?.clamp) {
    return value;
  }
  const [min, max] = def.clamp;
  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    return value;
  }
  return Math.min(Math.max(value, min), max);
}

function formatTelemetryValue(metricName, rawValue) {
  const def = TELEMETRY_METRIC_DEFINITIONS[metricName];
  if (def?.formatter) {
    try {
      return def.formatter(rawValue);
    } catch {
      // ignore formatter errors
    }
  }
  const numeric = Number(rawValue);
  if (Number.isFinite(numeric)) {
    const clamped = def ? clampMetricValue(numeric, def) : numeric;
    const decimals =
      def?.decimals != null
        ? def.decimals
        : Math.abs(clamped) >= 100
          ? 0
          : Math.abs(clamped) >= 10
            ? 1
            : 2;
    let formatted = clamped.toFixed(decimals);
    formatted = trimTrailingZeros(formatted);
    return def?.unit ? `${formatted}${def.unit}` : formatted;
  }
  if (rawValue == null) {
    return '';
  }
  return String(rawValue);
}

function trimTrailingZeros(value) {
  if (typeof value !== 'string') {
    return value;
  }
  if (!value.includes('.')) {
    return value;
  }
  return value.replace(/\.?0+$/, '');
}

function formatSecondsAsDuration(seconds) {
  const numeric = Number(seconds);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return '';
  }
  const h = Math.floor(numeric / 3600);
  const m = Math.floor((numeric % 3600) / 60);
  const s = Math.floor(numeric % 60);
  if (h > 0) {
    return `${h}h${m}m`;
  }
  if (m > 0) {
    return `${m}m${s}s`;
  }
  return `${s}s`;
}

function renderTelemetryTable(records) {
  if (!telemetryTableBody) {
    return;
  }
  const rows = records.slice(0, TELEMETRY_TABLE_LIMIT);
  const fragment = document.createDocumentFragment();
  for (const record of rows) {
    const tr = document.createElement('tr');
    const timeLabel = formatTelemetryTimestamp(record.sampleTimeMs);
    const nodeLabel = record.node?.label || record.meshId || '未知節點';
    const summary = formatTelemetrySummary(record);
    const extra = formatTelemetryExtra(record);
    const detailHtml = record.detail
      ? `<br/><span class="telemetry-table-extra">${escapeHtml(record.detail)}</span>`
      : '';
    tr.innerHTML = `
      <td>${escapeHtml(timeLabel)}</td>
      <td>${escapeHtml(nodeLabel)}</td>
      <td><span class="telemetry-table-metrics">${escapeHtml(summary || '—')}</span>${detailHtml}</td>
      <td>${extra}</td>
    `;
    fragment.appendChild(tr);
  }
  telemetryTableBody.innerHTML = '';
  telemetryTableBody.appendChild(fragment);
}

function formatTelemetryTimestamp(ms) {
  if (!Number.isFinite(ms)) {
    return '—';
  }
  const date = new Date(ms);
  if (Number.isNaN(date.getTime())) {
    return '—';
  }
  return formatLogTimestamp(date);
}

function formatTelemetrySummary(record) {
  const metrics = record.telemetry?.metrics || {};
  const parts = [];
  for (const [metricName, def] of Object.entries(TELEMETRY_METRIC_DEFINITIONS)) {
    const value = metrics[metricName];
    if (value == null) continue;
    const formatted = formatTelemetryValue(metricName, value);
    if (!formatted) continue;
    const label = def.label || metricName;
    parts.push(`${label} ${formatted}`);
  }
  if (parts.length) {
    return parts.join(' · ');
  }
  return '—';
}

function formatTelemetryExtra(record) {
  const extras = [];
  if (record.channel != null) {
    extras.push(`Ch ${record.channel}`);
  }
  if (Number.isFinite(record.snr)) {
    extras.push(`SNR ${trimTrailingZeros(record.snr.toFixed(2))}`);
  }
  if (Number.isFinite(record.rssi)) {
    extras.push(`RSSI ${trimTrailingZeros(record.rssi.toFixed(0))}`);
  }
  const metrics = record.telemetry?.metrics || {};
  const knownKeys = new Set(Object.keys(TELEMETRY_METRIC_DEFINITIONS));
  const flat = flattenTelemetryMetrics(metrics);
  let added = 0;
  for (const [key, value] of flat) {
    if (knownKeys.has(key)) continue;
    if (added >= 4) break;
    const formatted =
      typeof value === 'number'
        ? trimTrailingZeros(value.toFixed(2))
        : String(value);
    extras.push(`${key} ${formatted}`);
    added += 1;
  }
  if (!extras.length) {
    return '<span class="telemetry-table-extra">—</span>';
  }
  const text = extras.map((item) => escapeHtml(item)).join(' · ');
  return `<span class="telemetry-table-extra">${text}</span>`;
}

function flattenTelemetryMetrics(metrics, prefix = '', target = []) {
  if (!metrics || typeof metrics !== 'object') {
    return target;
  }
  for (const [key, value] of Object.entries(metrics)) {
    if (value == null) continue;
    const path = prefix ? `${prefix}.${key}` : key;
    if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'string') {
      target.push([path, value]);
    } else if (Array.isArray(value)) {
      if (!value.length) continue;
      target.push([
        path,
        value
          .map((item) => (typeof item === 'number' ? trimTrailingZeros(item.toFixed(2)) : String(item)))
          .join(', ')
      ]);
    } else if (typeof value === 'object') {
      flattenTelemetryMetrics(value, path, target);
    }
  }
  return target;
}

function updateTelemetryUpdatedAtLabel() {
  if (!telemetryUpdatedAtLabel) {
    return;
  }
  if (!telemetryUpdatedAt) {
    telemetryUpdatedAtLabel.textContent = '—';
    telemetryUpdatedAtLabel.removeAttribute('title');
    return;
  }
  const date = new Date(telemetryUpdatedAt);
  if (Number.isNaN(date.getTime())) {
    telemetryUpdatedAtLabel.textContent = '—';
    telemetryUpdatedAtLabel.removeAttribute('title');
    return;
  }
  telemetryUpdatedAtLabel.textContent = formatLogTimestamp(date);
  telemetryUpdatedAtLabel.title = date.toLocaleString();
}

function ensureTelemetryCustomDefaults() {
  const now = Date.now();
  if (!Number.isFinite(telemetryCustomRange.startMs)) {
    telemetryCustomRange.startMs = now - 7 * 24 * 60 * 60 * 1000;
  }
  if (!Number.isFinite(telemetryCustomRange.endMs)) {
    telemetryCustomRange.endMs = now;
  }
  if (telemetryCustomRange.startMs > telemetryCustomRange.endMs) {
    const temp = telemetryCustomRange.startMs;
    telemetryCustomRange.startMs = telemetryCustomRange.endMs;
    telemetryCustomRange.endMs = temp;
  }
}

function formatDatetimeLocal(ms) {
  if (!Number.isFinite(ms)) {
    return '';
  }
  const date = new Date(ms);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const mi = String(date.getMinutes()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}`;
}

function parseDatetimeLocal(value) {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.getTime();
}

function updateTelemetryRangeInputs() {
  if (telemetryRangeMode === 'custom') {
    ensureTelemetryCustomDefaults();
    if (telemetryRangeStartInput) {
      telemetryRangeStartInput.value = formatDatetimeLocal(telemetryCustomRange.startMs);
    }
    if (telemetryRangeEndInput) {
      telemetryRangeEndInput.value = formatDatetimeLocal(telemetryCustomRange.endMs);
    }
  } else {
    if (telemetryRangeStartInput) {
      telemetryRangeStartInput.value = '';
    }
    if (telemetryRangeEndInput) {
      telemetryRangeEndInput.value = '';
    }
  }
}

function setTelemetryRangeMode(mode, { skipRender = false } = {}) {
  const allowed = new Set(['day', 'week', 'month', 'year', 'custom']);
  if (!allowed.has(mode)) {
    mode = 'day';
  }
  telemetryRangeMode = mode;
  if (telemetryRangeSelect && telemetryRangeSelect.value !== mode) {
    telemetryRangeSelect.value = mode;
  }
  if (telemetryRangeCustomWrap) {
    telemetryRangeCustomWrap.classList.toggle('hidden', mode !== 'custom');
  }
  if (mode === 'custom') {
    ensureTelemetryCustomDefaults();
  }
  updateTelemetryRangeInputs();
  refreshTelemetrySelectors();
  if (!skipRender) {
    renderTelemetryView();
  }
}

function setTelemetryChartMode(mode, { skipRender = false } = {}) {
  if (mode !== 'single') {
    mode = 'all';
  }
  telemetryChartMode = mode;
  if (telemetryChartModeSelect && telemetryChartModeSelect.value !== mode) {
    telemetryChartModeSelect.value = mode;
  }
  if (!skipRender) {
    renderTelemetryView();
  }
}

function getTelemetryRangeWindow(now = Date.now()) {
  switch (telemetryRangeMode) {
    case 'day':
      return {
        startMs: now - 24 * 60 * 60 * 1000,
        endMs: now
      };
    case 'week':
      return {
        startMs: now - 7 * 24 * 60 * 60 * 1000,
        endMs: now
      };
    case 'month':
      return {
        startMs: now - 30 * 24 * 60 * 60 * 1000,
        endMs: now
      };
    case 'year':
      return {
        startMs: now - 365 * 24 * 60 * 60 * 1000,
        endMs: now
      };
    case 'custom': {
      ensureTelemetryCustomDefaults();
      const start = Number.isFinite(telemetryCustomRange.startMs)
        ? telemetryCustomRange.startMs
        : null;
      const end = Number.isFinite(telemetryCustomRange.endMs)
        ? telemetryCustomRange.endMs
        : null;
      if (start != null && end != null && start > end) {
        return {
          startMs: end,
          endMs: start
        };
      }
      return {
        startMs: start,
        endMs: end
      };
    }
    default:
      return { startMs: null, endMs: null };
  }
}

function applyTelemetryFilters(records) {
  if (!Array.isArray(records) || !records.length) {
    return [];
  }
  const { startMs, endMs } = getTelemetryRangeWindow();
  return records.filter((record) => {
    const time = Number(record.sampleTimeMs);
    if (!Number.isFinite(time)) {
      return false;
    }
    if (startMs != null && time < startMs) {
      return false;
    }
    if (endMs != null && time > endMs) {
      return false;
    }
    return true;
  });
}

async function initializeTelemetry() {
  if (!window.meshtastic.getTelemetrySnapshot) {
    return;
  }
  try {
    const snapshot = await window.meshtastic.getTelemetrySnapshot({
      limitPerNode: TELEMETRY_MAX_LOCAL_RECORDS
    });
    applyTelemetrySnapshot(snapshot);
  } catch (err) {
    console.warn('載入遙測資料失敗:', err);
  }
}

function activatePage(targetId) {
  const pages = [
    { id: 'monitor-page', element: monitorPage },
    { id: 'telemetry-page', element: telemetryPage },
    { id: 'flow-page', element: flowPage },
    { id: 'json-page', element: jsonPage },
    { id: 'settings-page', element: settingsPage },
    { id: 'log-page', element: logPage },
    { id: 'info-page', element: infoPage }
  ];
  pages.forEach(({ id, element }) => {
    if (!element) return;
    const active = id === targetId;
    element.classList.toggle('hidden', !active);
    element.classList.toggle('active', active);
  });
  if (targetId === 'telemetry-page') {
    renderTelemetryView();
  }
  navButtons.forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.target === targetId);
  });
}

function isIPv4(address) {
  return typeof address === 'string' && /^\d{1,3}(\.\d{1,3}){3}$/.test(address);
}

function hasApiKey() {
  return callmeshHasServerKey;
}

function hasHost() {
  return getHostValue().length > 0;
}

function updateConnectAvailability() {
  updateToggleButton();
  refreshOverlay();
}

function setOverlayStatus(message, variant = 'info') {
  if (!overlayStatus) return;
  overlayStatus.textContent = message || '';
  overlayStatus.dataset.variant = variant;
}

function setOverlayHostStatus(message, variant = 'info') {
  if (!overlayHostStatus) return;
  overlayHostStatus.textContent = message || '';
  overlayHostStatus.dataset.variant = variant;
}

function setOverlayStep(step) {
  if (!callmeshOverlay) return;
  const useHost = step === 'host';
  if (overlayStepApi) {
    overlayStepApi.classList.toggle('hidden', useHost);
  }
  if (overlayStepHost) {
    overlayStepHost.classList.toggle('hidden', !useHost);
  }
  if (useHost) {
    if (overlayHostInput) {
      overlayHostInput.value = getHostValue();
      setTimeout(() => overlayHostInput?.focus(), 50);
    }
    setOverlayStatus('', 'info');
    setOverlayHostStatus('請輸入節點 IP 或使用自動搜尋。', 'info');
  } else {
    if (overlayKeyInput) {
      overlayKeyInput.value = apiKeyInput.value.trim();
      setTimeout(() => overlayKeyInput?.focus(), 50);
    }
    setOverlayStatus('請輸入 API Key 以開始使用', 'info');
  }
}

function showOverlay(step = 'api') {
  if (!callmeshOverlay) return;
  callmeshOverlay.classList.remove('hidden');
  setOverlayStep(step);
}

function hideOverlay() {
  if (!callmeshOverlay) return;
  callmeshOverlay.classList.add('hidden');
  setOverlayStatus('', 'info');
  setOverlayHostStatus('', 'info');
}

function refreshOverlay() {
  if (!hasApiKey()) {
    showOverlay('api');
    return;
  }
  if (!hasHost()) {
    showOverlay('host');
    return;
  }
  hideOverlay();
}


async function validateApiKey(key, { auto = false, source = 'main' } = {}) {
  const trimmed = (key || '').trim();
  const statusLabel = trimmed ? '正在驗證 API Key...' : '已清除 API Key';
  if (!auto) {
    if (source === 'overlay') {
      setOverlayStatus(statusLabel, 'info');
    } else {
      setDiscoverStatus(statusLabel, 'info');
    }
  }

  if (source === 'overlay') {
    if (overlaySaveBtn) overlaySaveBtn.disabled = true;
    if (overlayRetryBtn) overlayRetryBtn.disabled = true;
  }

  try {
    const info = await window.meshtastic.saveCallmeshKey?.(trimmed);
    if (!info) return;

    if (info.success && info.hasKey) {
      const degraded = Boolean(info.degraded);
      const statusMsg = degraded
        ? 'CallMesh 暫時無回應'
        : 'API Key 驗證成功';
      setDiscoverStatus(statusMsg, degraded ? 'warn' : 'success');
      setOverlayStatus(statusMsg, degraded ? 'warn' : 'success');
      appendLog('CALLMESH', 'API key verified degraded=' + degraded);

      apiKeyInput.value = trimmed;
      if (overlayKeyInput) overlayKeyInput.value = trimmed;
      callmeshHasServerKey = true;
      callmeshDegraded = degraded;
      lastVerifiedKey = trimmed;
      if (info.statusText) {
        platformStatus.textContent = info.statusText;
      }
      savePreferences();
      refreshOverlay();
      ensureHostGuidance();
      if (source === 'overlay') {
        initialSetupAutoConnectPending = true;
        maybeTriggerInitialSetupAutoConnect('api-key-overlay');
      }
      return;
    }

    if (info.success && !info.hasKey) {
      const message = 'API Key 已移除';
      setDiscoverStatus(message, 'warn');
      setOverlayStatus(message, 'warn');
      appendLog('CALLMESH', 'API key removed');

      apiKeyInput.value = '';
      if (overlayKeyInput) overlayKeyInput.value = '';
      callmeshHasServerKey = false;
      callmeshDegraded = false;
      lastVerifiedKey = '';
      platformStatus.textContent = info.statusText || 'CallMesh: 未設定 Key';
      savePreferences();
      showOverlay('api');
      initialSetupAutoConnectPending = false;
      initialSetupAutoConnectTriggered = false;
      return;
    }

    const failureMessage = info?.error || '未知錯誤';
    setDiscoverStatus('API Key 驗證失敗：' + failureMessage, 'error');
    setOverlayStatus('API Key 驗證失敗：' + failureMessage, 'error');
    appendLog('CALLMESH', 'API key verify failed ' + failureMessage);

    callmeshHasServerKey = false;
    callmeshDegraded = false;
    platformStatus.textContent = info?.statusText || 'CallMesh: Key 驗證失敗';
    apiKeyInput.value = '';
    if (overlayKeyInput) overlayKeyInput.value = '';
    if (apiKeyInput) {
      apiKeyInput.focus();
      apiKeyInput.select();
    }
    lastVerifiedKey = '';
    savePreferences();
    showOverlay('api');
  } catch (err) {
    const message = err?.message || '未知錯誤';
    setDiscoverStatus('驗證 API Key 時發生錯誤：' + message, 'error');
    setOverlayStatus('驗證 API Key 時發生錯誤：' + message, 'error');
    appendLog('CALLMESH', 'API key verify error ' + message);
    callmeshHasServerKey = false;
    platformStatus.textContent = 'CallMesh: Key 驗證失敗';
    callmeshDegraded = false;
    apiKeyInput.value = '';
    if (overlayKeyInput) overlayKeyInput.value = '';
    lastVerifiedKey = '';
    savePreferences();
    showOverlay('api');
    initialSetupAutoConnectPending = false;
    initialSetupAutoConnectTriggered = false;
  } finally {
    updateConnectAvailability();
    if (source === 'overlay') {
      if (overlaySaveBtn) overlaySaveBtn.disabled = false;
      if (overlayRetryBtn) overlayRetryBtn.disabled = false;
    }
  }
}

function updateToggleButton() {
  if (!connectBtn) return;
  if (isConnecting || manualConnectActive) {
    connectBtn.textContent = '取消連線';
    connectBtn.disabled = false;
    connectBtn.dataset.state = 'connecting';
    return;
  }

  if (isConnected) {
    connectBtn.textContent = '中斷連線';
    connectBtn.disabled = false;
    connectBtn.dataset.state = 'connected';
    return;
  }

  connectBtn.textContent = '連線';
  connectBtn.disabled = !hasApiKey() || !hasHost();
  connectBtn.dataset.state = 'idle';
}

function maybeTriggerInitialSetupAutoConnect(reason) {
  if (initialSetupAutoConnectTriggered || !initialSetupAutoConnectPending) {
    return;
  }
  if (!hasApiKey() || !hasHost()) {
    return;
  }
  if (manualDisconnect) {
    appendLog('CONNECT', `initial setup auto-connect skipped (${reason}): manual disconnect active`);
    initialSetupAutoConnectPending = false;
    initialSetupAutoConnectTriggered = true;
    return;
  }
  if (isConnecting || isConnected) {
    initialSetupAutoConnectPending = false;
    initialSetupAutoConnectTriggered = true;
    return;
  }
  initialSetupAutoConnectPending = false;
  initialSetupAutoConnectTriggered = true;
  appendLog('CONNECT', `initial setup auto-connect triggered (${reason})`);
  connectNow({ context: 'initial' }).catch((err) => {
    console.warn('initial setup auto-connect failed:', err);
  });
}

function scheduleInitialAutoConnect() {
  clearAutoConnectTimer();
  if (manualDisconnect || isConnected || isConnecting) {
    return;
  }
  if (!hasHost()) {
    appendLog('CONNECT', 'initial auto-connect skipped (missing host)');
    ensureHostGuidance();
    return;
  }
  initialAutoConnectActive = true;
  autoConnectAttempts = 0;
  autoConnectTimer = setTimeout(attemptAutoConnect, 500);
  appendLog('CONNECT', 'scheduled initial auto-connect');
}

async function attemptAutoConnect() {
  clearAutoConnectTimer();
  if (manualDisconnect || isConnected || isConnecting) {
    initialAutoConnectActive = false;
    return;
  }
  if (!hasHost()) {
    appendLog('CONNECT', 'auto attempt skipped (missing host)');
    initialAutoConnectActive = false;
    ensureHostGuidance();
    return;
  }
  appendLog('CONNECT', `auto attempt ${autoConnectAttempts + 1}`);
  const success = await connectNow({ context: 'initial' });
  if (!success) {
    autoConnectAttempts += 1;
    if (autoConnectAttempts < AUTO_CONNECT_MAX_ATTEMPTS) {
      autoConnectTimer = setTimeout(attemptAutoConnect, AUTO_CONNECT_DELAY_MS);
      return;
    }
    initialAutoConnectActive = false;
    allowReconnectLoop = false;
    stopReconnectLoop();
  } else {
    initialAutoConnectActive = false;
    allowReconnectLoop = true;
  }
}

function clearAutoConnectTimer() {
  if (autoConnectTimer) {
    clearTimeout(autoConnectTimer);
    autoConnectTimer = null;
  }
}

function clearReconnectTimer() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

function clearManualConnectRetryTimer() {
  if (manualConnectRetryTimer) {
    clearTimeout(manualConnectRetryTimer);
    manualConnectRetryTimer = null;
  }
  if (manualConnectRetryResolver) {
    manualConnectRetryResolver();
    manualConnectRetryResolver = null;
  }
}

function waitManualRetryDelay() {
  return new Promise((resolve) => {
    manualConnectRetryResolver = () => {
      if (manualConnectRetryResolver) {
        manualConnectRetryResolver = null;
        manualConnectRetryTimer = null;
        resolve();
      }
    };
    manualConnectRetryTimer = setTimeout(() => {
      manualConnectRetryTimer = null;
      manualConnectRetryResolver = null;
      resolve();
    }, MANUAL_CONNECT_DELAY_MS);
  });
}

function cancelManualConnect({ silent = false } = {}) {
  if (!manualConnectActive && !isConnecting) return;
  manualConnectAbort = true;
  manualConnectSession += 1;
  clearManualConnectRetryTimer();
  appendLog('CONNECT', 'manual connect cancelled by user');
  performDisconnect({ silent: true, preserveManual: false }).finally(() => {
    manualConnectActive = false;
    manualConnectAttempts = 0;
    allowReconnectLoop = false;
    stopReconnectLoop();
    setConnectingState(false);
    if (!silent) updateStatus('idle');
    updateConnectAvailability();
    updateToggleButton();
  });
}

async function manualConnectWithRetries() {
  if (manualConnectActive) return false;
  manualDisconnect = false;
  stopReconnectLoop();
  allowReconnectLoop = false;
  clearManualConnectRetryTimer();
  manualConnectActive = true;
  manualConnectAbort = false;
  manualConnectAttempts = 0;
  const sessionId = ++manualConnectSession;
  updateToggleButton();

  try {
    while (!manualConnectAbort && manualConnectAttempts < MANUAL_CONNECT_MAX_ATTEMPTS) {
      manualConnectAttempts += 1;
      appendLog('CONNECT', `manual attempt ${manualConnectAttempts}`);
      const success = await connectNow({ context: 'manual' });

      if (manualConnectAbort || sessionId !== manualConnectSession) {
        appendLog('CONNECT', 'manual session aborted before completion');
        allowReconnectLoop = false;
        updateStatus('idle');
        return false;
      }

      if (success) {
        resumeAutoReconnect({ reason: 'manual-success' });
        allowReconnectLoop = true;
        return true;
      }

      if (manualConnectAttempts >= MANUAL_CONNECT_MAX_ATTEMPTS) {
        break;
      }

      appendLog(
        'CONNECT',
        `manual retry ${manualConnectAttempts + 1} scheduled in ${MANUAL_CONNECT_DELAY_MS / 1000}s`
      );
      updateStatus('error', '連線失敗，將在 5 秒後重試');
      await waitManualRetryDelay();
      clearManualConnectRetryTimer();
      if (manualConnectAbort || sessionId !== manualConnectSession) {
        appendLog('CONNECT', 'manual session aborted during wait');
        allowReconnectLoop = false;
        updateStatus('idle');
        return false;
      }
    }

    allowReconnectLoop = false;
    updateStatus('error', '手動連線失敗');
    appendLog('CONNECT', 'manual retries exhausted');
    return false;
  } finally {
    clearManualConnectRetryTimer();
    manualConnectActive = false;
    manualConnectAbort = false;
    manualConnectAttempts = 0;
    updateToggleButton();
    updateConnectAvailability();
  }
}

async function connectNow({ context = 'manual', overrideHost } = {}) {
  if (isConnecting || isConnected) {
    return false;
  }

  const hostSource = overrideHost != null ? overrideHost : getHostValue();
  const host = typeof hostSource === 'string' ? hostSource.trim() : '';
  if (!host) {
    if (context === 'manual') {
      setDiscoverStatus('請先設定 Host', 'error');
      updateStatus('error', 'Host 未設定');
      ensureHostGuidance();
    } else {
      ensureHostGuidance({ force: true });
    }
    updateConnectAvailability();
    return false;
  }

  if (!hasApiKey()) {
    if (context === 'manual') {
      setDiscoverStatus('請先設定 CallMesh API Key', 'error');
      updateStatus('error', 'API Key 未設定');
    }
    updateConnectAvailability();
    return false;
  }

  setConnectingState(true);
  const statusMessage = context === 'reconnect' ? '重新連線中...' : '連線中...';
  updateStatus('connecting', statusMessage);
  appendLog('CONNECT', `attempt context=${context} host=${host}`);

  try {
    await window.meshtastic.connect({
      host,
      port: 4403,
      handshake: true,
      heartbeat: SOCKET_HEARTBEAT_SECONDS,
      keepAlive: true,
      keepAliveDelayMs: SOCKET_KEEPALIVE_DELAY_MS,
      idleTimeoutMs: SOCKET_IDLE_TIMEOUT_MS
    });
    appendLog('CONNECT', `success context=${context}`);
    lastConnectedHost = host;
    lastConnectedHostRevision = hostPreferenceRevision;
    return true;
  } catch (err) {
    console.error('連線失敗:', err);
    if (context === 'manual') {
      updateStatus('error', err.message);
    } else if (context === 'initial') {
      updateStatus('error', '自動連線失敗，稍候再嘗試');
    } else if (context === 'reconnect') {
      updateStatus('error', '連線中斷，正在重試');
    }
    appendLog('CONNECT', `failure context=${context} error=${err.message}`);
    return false;
  } finally {
    setConnectingState(false);
    updateConnectAvailability();
  }
}

async function performDisconnect({ silent = false, preserveManual = false } = {}) {
  manualConnectAbort = true;
  stopInactivityMonitor();
  stopReconnectLoop();
  clearAutoConnectTimer();
  clearManualConnectRetryTimer();
  appendLog('DISCONNECT', `request silent=${silent} preserveManual=${preserveManual}`);
  try {
    await window.meshtastic.disconnect();
  } catch (err) {
    console.warn('中斷連線失敗:', err);
    if (!silent) {
      setDiscoverStatus(`中斷失敗：${err.message}`, 'error');
    }
    appendLog('DISCONNECT', `error ${err.message}`);
  }
  if (!preserveManual) {
    manualDisconnect = false;
  }
  setConnectedState(false);
  if (!silent) {
    updateStatus('disconnected');
  }
  clearSelfNodeDisplay();
  appendLog('DISCONNECT', 'completed');
}

function startReconnectLoop() {
  if (manualDisconnect) {
    return;
  }
  if (autoReconnectSuspended) {
    appendLog('CONNECT', 'reconnect loop suppressed (auto reconnect suspended)');
    return;
  }
  if (!allowReconnectLoop || manualConnectActive) {
    appendLog('CONNECT', 'reconnect loop suppressed');
    return;
  }
  const reconnectHost = getReconnectHost();
  if (!reconnectHost || !hasApiKey()) {
    return;
  }
  if (reconnectTimer) {
    return;
  }
  appendLog('CONNECT', 'start reconnect loop');
  scheduleReconnectAttempt(INITIAL_RECONNECT_DELAY_MS);
}

function stopReconnectLoop() {
  if (reconnectTimer) {
    clearReconnectTimer();
    appendLog('CONNECT', 'stop reconnect loop');
  }
}

function startInactivityMonitor() {
  stopInactivityMonitor();
  inactivityTimer = setInterval(checkInactivity, RECONNECT_INTERVAL_MS);
}

function stopInactivityMonitor() {
  if (inactivityTimer) {
    clearInterval(inactivityTimer);
    inactivityTimer = null;
  }
}

function checkInactivity() {
  if (!isConnected || manualDisconnect) {
    return;
  }
  if (!lastActivityAt) {
    return;
  }
  const idleDuration = Date.now() - lastActivityAt;
  if (idleDuration >= INACTIVITY_THRESHOLD_MS) {
    handleInactivityReconnect();
  }
}

async function handleInactivityReconnect() {
  updateStatus('error', '超過 10 分鐘未收到節點資料，重新連線中');
  manualDisconnect = false;
  appendLog('CONNECT', 'idle timeout reached, reconnecting');
  await performDisconnect({ silent: true, preserveManual: false });
  startReconnectLoop();
}

function scheduleReconnectAttempt(delayMs = 0) {
  clearReconnectTimer();
  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null;
    if (!allowReconnectLoop || manualDisconnect || manualConnectActive || autoReconnectSuspended) {
      appendLog('CONNECT', 'reconnect attempt cancelled (state changed)');
      return;
    }
    if (isConnecting || isConnected) {
      appendLog('CONNECT', 'reconnect attempt skipped (already connecting/connected)');
      return;
    }
    const attemptHost = getReconnectHost();
    if (!attemptHost || !hasApiKey()) {
      appendLog('CONNECT', 'reconnect attempt skipped (missing host/api key)');
      return;
    }

    appendLog('CONNECT', `attempt context=reconnect host=${attemptHost}`);
    const success = await connectNow({ context: 'reconnect', overrideHost: attemptHost });

    if (!success) {
      recordReconnectFailure('connect-failure');
    }

    if (!success && !autoReconnectSuspended && allowReconnectLoop && !manualDisconnect && !manualConnectActive) {
      appendLog('CONNECT', `reconnect retry scheduled in ${RECONNECT_INTERVAL_MS / 1000}s`);
      scheduleReconnectAttempt(RECONNECT_INTERVAL_MS);
    }
  }, delayMs);
}

function registerPacketActivity(summary) {
  lastActivityAt = Date.now();
  const fromMeshId = summary?.from?.meshId || summary?.from?.meshIdNormalized;
  if (fromMeshId && isSelfMeshId(fromMeshId)) {
    return;
  }
  const timestamp = typeof summary.timestamp === 'string'
    ? Date.parse(summary.timestamp)
    : summary.timestamp ? Number(summary.timestamp) : Date.now();
  const ts = Number.isFinite(timestamp) ? timestamp : Date.now();
  const bucketKey = Math.floor(ts / PACKET_BUCKET_MS) * PACKET_BUCKET_MS;
  const current = packetBuckets.get(bucketKey) ?? 0;
  packetBuckets.set(bucketKey, current + 1);
  prunePacketBuckets();
  packetSummaryLast10Min = Array.from(packetBuckets.values()).reduce((sum, count) => sum + count, 0);
  updateDashboardCounters();
}

function prunePacketBuckets() {
  const cutoff = Date.now() - PACKET_WINDOW_MS;
  for (const key of Array.from(packetBuckets.keys())) {
    if (key < cutoff) {
      packetBuckets.delete(key);
    }
  }
}

function handleSelfInfoEvent(info) {
  if (!info || !info.node) {
    return;
  }
  const node = info.node;
  if (info.raw != null) {
    selfNodeState.raw = info.raw;
  }
  if (node.meshId) {
    selfNodeState.meshId = node.meshId;
    selfNodeState.normalizedMeshId = normalizeMeshId(node.meshId);
  }
  const name = node.longName || (node.label && node.label !== 'unknown' ? node.label : null);
  if (name) {
    selfNodeState.name = name;
  }
  applySelfNodeDisplay();
  pruneJsonEntriesForSelf();
  appendLog('SELF', `myInfo meshId=${selfNodeState.meshId || 'unknown'} name=${selfNodeState.name || 'unknown'}`);
}

function maybeUpdateSelfNodeFromSummary(summary) {
  if (!summary || !summary.from) {
    return;
  }
  const node = summary.from;
  const meshId = node.meshId || null;
  const normalizedMeshId = meshId ? normalizeMeshId(meshId) : null;
  const raw = node.raw ?? null;

  const hasState = Boolean(selfNodeState.meshId) || selfNodeState.raw != null;
  const matchesMesh = (selfNodeState.meshId && meshId && selfNodeState.meshId === meshId)
    || (selfNodeState.normalizedMeshId && normalizedMeshId && selfNodeState.normalizedMeshId === normalizedMeshId);
  const matchesRaw = selfNodeState.raw != null && raw != null && selfNodeState.raw === raw;
  const shouldInitialize = !hasState && meshId;

  if (!matchesMesh && !matchesRaw && !shouldInitialize) {
    return;
  }

  let updated = false;
  if (meshId) {
    if (selfNodeState.meshId !== meshId) {
      selfNodeState.meshId = meshId;
      updated = true;
    }
    if (selfNodeState.normalizedMeshId !== normalizedMeshId) {
      selfNodeState.normalizedMeshId = normalizedMeshId;
      updated = true;
    }
  }
  if (raw != null) {
    if (selfNodeState.raw !== raw) {
      selfNodeState.raw = raw;
      updated = true;
    }
  }

  const name = node.longName || (node.label && node.label !== 'unknown' ? node.label : null);
  if (name) {
    if (selfNodeState.name !== name) {
      selfNodeState.name = name;
      updated = true;
    }
  }

  applySelfNodeDisplay();
  pruneJsonEntriesForSelf();
  if (updated) {
    appendLog('SELF', `updated from packet meshId=${selfNodeState.meshId || 'unknown'} name=${selfNodeState.name || 'unknown'}`);
  }
}

function applySelfNodeDisplay() {
  if (!currentNodeDisplay || !currentNodeText) {
    return;
  }
  if (!selfNodeState || (!selfNodeState.name && !selfNodeState.meshId)) {
    currentNodeDisplay.classList.add('hidden');
    currentNodeText.textContent = '尚未取得節點資訊';
    return;
  }
  const parts = [];
  if (selfNodeState.name) parts.push(selfNodeState.name);
  if (selfNodeState.meshId) parts.push(selfNodeState.meshId);
  currentNodeText.textContent = parts.join(' · ');
  currentNodeDisplay.classList.remove('hidden');
}

function clearSelfNodeDisplay() {
  selfNodeState.name = null;
  selfNodeState.meshId = null;
  selfNodeState.normalizedMeshId = null;
  selfNodeState.raw = null;
  applySelfNodeDisplay();
  appendLog('SELF', 'cleared self node state');
}

function updateProvisionInfo(provision, mappingSyncedAt) {
  if (!infoCallsign) return;
  if (!provision) {
    infoCallsign.textContent = '—';
    infoSymbol.textContent = '—';
    if (infoCoords) infoCoords.textContent = '—';
    if (infoPhgPower) infoPhgPower.textContent = '—';
    if (infoPhgHeight) infoPhgHeight.textContent = '—';
    if (infoPhgGain) infoPhgGain.textContent = '—';
    infoComment.textContent = '—';
    infoUpdatedAt.textContent = mappingSyncedAt ? formatRelativeTime(mappingSyncedAt) : '—';
    lastProvisionSignature = null;
    return;
  }

  const callsign = provision.callsign_base || '—';
  const ssidSuffix = formatAprsSsid(provision.ssid);
  const aprsCallsign = callsign === '—' ? '—' : (ssidSuffix ? `${callsign}${ssidSuffix}` : callsign);
  const symbolTable = provision.symbol_table || '';
  const symbolCode = provision.symbol_code || '';
  const overlayChar = provision.symbol_overlay || '';
  const overlaySymbol = overlayChar && symbolCode ? `${overlayChar}${symbolCode}` : '';
  const symbol = symbolTable || symbolCode ? `${symbolTable}${symbolCode}` : '';
  const displaySymbol = overlaySymbol || symbol || '—';
  const comment = provision.comment || '—';
  const phgInfo = decodePhg(provision.phg);

  infoCallsign.textContent = aprsCallsign;
  infoSymbol.textContent = displaySymbol;
  if (infoCoords) infoCoords.textContent = formatProvisionCoords(provision);
  if (infoPhgPower) infoPhgPower.textContent = phgInfo ? `${phgInfo.powerWatts} W` : '—';
  if (infoPhgHeight) infoPhgHeight.textContent = phgInfo ? `${phgInfo.heightMeters.toFixed(1)} m` : '—';
  if (infoPhgGain) infoPhgGain.textContent = phgInfo ? `${phgInfo.gainDb} dB` : '—';
  infoComment.textContent = comment;
  infoUpdatedAt.textContent = mappingSyncedAt ? formatRelativeTime(mappingSyncedAt) : new Date().toLocaleString();

  const signature = JSON.stringify({
    aprsCallsign,
    displaySymbol,
    phg: phgInfo
      ? {
          power: phgInfo.powerWatts,
          height: Number(phgInfo.heightMeters.toFixed(2)),
          gain: phgInfo.gainDb
        }
      : null,
    comment,
    coords: infoCoords ? infoCoords.textContent : ''
  });
  if (signature !== lastProvisionSignature) {
    lastProvisionSignature = signature;
    const logParts = [`callsign=${aprsCallsign}`, `symbol=${displaySymbol}`];
    if (phgInfo) {
      logParts.push(`power=${phgInfo.powerWatts}W`);
      logParts.push(`height=${phgInfo.heightMeters.toFixed(1)}m`);
      logParts.push(`gain=${phgInfo.gainDb}dB`);
    }
    appendLog('PROVISION', logParts.join(' '));
  }
}

function formatProvisionCoords(provision) {
  const lat = provision.latitude; // maybe null
  const lon = provision.longitude;
  if (lat == null || lon == null) return '—';
  const latFmt = Number(lat).toFixed(4);
  const lonFmt = Number(lon).toFixed(4);
  return `(${latFmt}, ${lonFmt})`;
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
    heightFeet,
    heightMeters,
    gainDb
  };
}

function formatRelativeTime(isoString) {
  if (!isoString) return '—';
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) {
    return isoString;
  }
  const diff = Date.now() - date.getTime();
  if (diff < 60_000) {
    return '剛剛';
  }
  const minutes = Math.floor(diff / 60_000);
  if (minutes < 60) {
    return `${minutes} 分鐘前`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours} 小時前`;
  }
  const days = Math.floor(hours / 24);
  if (days < 7) {
    return `${days} 天前`;
  }
  return date.toLocaleString();
}

function formatAprsSsid(ssid) {
  if (ssid === null || ssid === undefined) return '';
  if (ssid === 0) return '';
  if (ssid < 0) return `${ssid}`;
  return `-${ssid}`;
}
