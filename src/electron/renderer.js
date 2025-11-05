'use strict';

const CHANNEL_CONFIG = [
  { id: 0, code: 'CH0', name: 'Primary Channel', note: '日常主要通訊頻道' },
  { id: 1, code: 'CH1', name: 'Mesh TW', note: '跨節點廣播與共通交換' },
  { id: 2, code: 'CH2', name: 'Signal Test', note: '訊號測試、天線調校專用' },
  { id: 3, code: 'CH3', name: 'Emergency', note: '緊急狀況 / 救援聯絡' }
];
const CHANNEL_MESSAGE_LIMIT = 200;
const channelMessageStore = new Map();
CHANNEL_CONFIG.forEach((channel) => {
  channelMessageStore.set(channel.id, []);
});

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
const relayHintModal = document.getElementById('relay-hint-modal');
const relayHintReasonEl = document.getElementById('relay-hint-reason');
const relayHintNodeEl = document.getElementById('relay-hint-node');
const relayHintMeshEl = document.getElementById('relay-hint-mesh');
const relayHintSubtitleEl = document.getElementById('relay-hint-subtitle');
const relayHintCloseBtn = document.getElementById('relay-hint-close');

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
const NODE_ONLINE_WINDOW_MS = 60 * 60 * 1000;

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
const messagesPage = document.getElementById('messages-page');
const nodesPage = document.getElementById('nodes-page');
const nodesTableWrapper = document.getElementById('nodes-table-wrapper');
const nodesTableBody = document.getElementById('nodes-table-body');
const nodesEmptyState = document.getElementById('nodes-empty-state');
const nodesTotalCountLabel = document.getElementById('nodes-total-count');
const nodesOnlineCountLabel = document.getElementById('nodes-online-count');
const nodesOnlineTotalLabel = document.getElementById('nodes-online-total');
const nodesSearchInput = document.getElementById('nodes-search');
const nodesClearBtn = document.getElementById('nodes-clear-btn');
const nodesStatusLabel = document.getElementById('nodes-status');
const settingsPage = document.getElementById('settings-page');
const logPage = document.getElementById('log-page');
const infoPage = document.getElementById('info-page');
const flowPage = document.getElementById('flow-page');
const flowList = document.getElementById('flow-list');
const flowEmptyState = document.getElementById('flow-empty-state');
const flowSearchInput = document.getElementById('flow-search');
const flowFilterStateSelect = document.getElementById('flow-filter-state');
const flowDownloadBtn = document.getElementById('flow-download-btn');
const telemetryPage = document.getElementById('telemetry-page');
const telemetryNodeInput = document.getElementById('telemetry-node-input');
const telemetryNodeDropdown = document.getElementById('telemetry-node-dropdown');
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
const telemetryClearBtn = document.getElementById('telemetry-clear-btn');
const telemetryStatsRecords = document.getElementById('telemetry-stats-records');
const telemetryStatsNodes = document.getElementById('telemetry-stats-nodes');
const telemetryStatsDisk = document.getElementById('telemetry-stats-disk');
const aprsServerInput = document.getElementById('aprs-server');
const aprsBeaconIntervalInput = document.getElementById('aprs-beacon-interval');
const webUiEnabledCheckbox = document.getElementById('web-ui-enabled');
const resetDataBtn = document.getElementById('reset-data-btn');
const copyLogBtn = document.getElementById('copy-log-btn');
const downloadLogBtn = document.getElementById('download-log-btn');
const channelNav = document.getElementById('channel-nav');
const channelMessageList = document.getElementById('channel-message-list');
const channelTitleLabel = document.getElementById('channel-title');
const channelNoteLabel = document.getElementById('channel-note');
const channelNavButtons = new Map();
let selectedChannelId = CHANNEL_CONFIG[0]?.id ?? 0;

initializeChannelMessages();
loadPersistedMessages();

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
let nodeDistanceReference = null;
let nodesSearchTerm = '';
let telemetrySearchTerm = '';
let telemetrySearchRaw = '';
let telemetryLastExplicitMeshId = null;
let telemetryNodeInputHoldEmpty = false;
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
const telemetryNodeLookup = new Map();
const telemetryNodeDisplayByMesh = new Map();
let telemetryNodeOptions = [];
let telemetryDropdownVisible = false;
let telemetryDropdownInteracting = false;
const nodeRegistry = new Map();

function isIgnoredMeshId(meshId) {
  const normalized = normalizeMeshId(meshId);
  if (!normalized) return false;
  return normalized.toLowerCase().startsWith('!abcd');
}
let nodeSnapshotLoaded = false;

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

const RELAY_GUESS_EXPLANATION =
  '最後轉發節點由 SNR/RSSI 推測（韌體僅提供節點尾碼），結果可能不完全準確。';

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

function isRelayGuessed(summary) {
  return Boolean(summary?.relay?.guessed || summary?.relayGuess);
}

function getRelayGuessReason(summary) {
  return summary?.relayGuessReason || RELAY_GUESS_EXPLANATION;
}

function hideRelayHintModal() {
  if (!relayHintModal) return;
  relayHintModal.classList.add('hidden');
  document.body.classList.remove('modal-open');
}

function showRelayHint({ reason, relayLabel, meshId } = {}) {
  const fallbackText = reason || RELAY_GUESS_EXPLANATION;
  if (!relayHintModal || !relayHintReasonEl) {
    window.alert([fallbackText, relayLabel ? `節點：${relayLabel}` : null, meshId ? `Mesh ID：${meshId}` : null].filter(Boolean).join('\n'));
    return;
  }
  relayHintReasonEl.textContent = fallbackText;
  if (relayHintSubtitleEl) {
    relayHintSubtitleEl.textContent = '系統依歷史統計推測可能的最後轉發節點';
  }
  if (relayHintNodeEl) {
    relayHintNodeEl.textContent = relayLabel || '—';
  }
  if (relayHintMeshEl) {
    relayHintMeshEl.textContent = meshId || '—';
  }
  relayHintModal.classList.remove('hidden');
  document.body.classList.add('modal-open');
  setTimeout(() => relayHintCloseBtn?.focus(), 0);
}

function ensureRelayGuessSuffix(label, summary) {
  if (!isRelayGuessed(summary)) {
    return label;
  }
  const value = (label || '').trim();
  if (!value) {
    return '未知';
  }
  return value;
}

function formatRelayLabel(relay) {
  if (!relay) return '';
  const meshId =
    relay.meshId || relay.meshIdOriginal || relay.meshIdNormalized || relay.mesh_id || '';
  const normalized =
    typeof meshId === 'string' && meshId.startsWith('!') ? meshId.slice(1) : meshId;
  const shortDisplay = sanitizeNodeName(relay.shortName);
  let display = formatNodeDisplay(relay);
  if (!display || display === 'unknown') {
    display = sanitizeNodeName(relay.longName) || sanitizeNodeName(relay.label) || meshId || '';
  }
  if (shortDisplay) {
    const lowerShort = shortDisplay.toLowerCase();
    if (!display.toLowerCase().includes(lowerShort)) {
      display = display ? `${display} / ${shortDisplay}` : shortDisplay;
    }
  }
  if (normalized && /^0{6}[0-9a-fA-F]{2}$/.test(String(normalized).toLowerCase())) {
    if (display && display !== 'unknown') {
      return display;
    }
    return meshId || '未知';
  }
  if (display && display !== 'unknown') {
    return display;
  }
  return meshId || relay.label || '';
}

function computeRelayLabel(summary) {
  const fromMeshId = summary.from?.meshId || summary.from?.meshIdNormalized || '';
  const fromNormalized = normalizeMeshId(fromMeshId);
  if (fromMeshId && isSelfMeshId(fromMeshId)) {
    return ensureRelayGuessSuffix('Self', summary);
  }

  let relayMeshIdRaw =
    summary.relay?.meshId ||
    summary.relay?.meshIdNormalized ||
    summary.relayMeshId ||
    summary.relayMeshIdNormalized ||
    '';
  const hydratedRelay = hydrateSummaryNode(summary.relay, relayMeshIdRaw);
  if (hydratedRelay) {
    summary.relay = hydratedRelay;
    relayMeshIdRaw =
      hydratedRelay.meshId || hydratedRelay.meshIdOriginal || hydratedRelay.meshIdNormalized || relayMeshIdRaw;
  }
  if (relayMeshIdRaw && isSelfMeshId(relayMeshIdRaw)) {
    return ensureRelayGuessSuffix('Self', summary);
  }
  let relayNormalized = normalizeMeshId(relayMeshIdRaw);
  if (relayNormalized && /^!0{6}[0-9a-fA-F]{2}$/.test(relayNormalized)) {
    relayMeshIdRaw = '';
    relayNormalized = null;
  }

  if (fromNormalized && relayNormalized && fromNormalized === relayNormalized) {
    return ensureRelayGuessSuffix('直收', summary);
  }

  const { usedHops, hopsLabel } = extractHopInfo(summary);
  const normalizedHopsLabel = hopsLabel || '';
  const zeroHop = usedHops === 0 || /^0(?:\s*\/|$)/.test(normalizedHopsLabel);

  if (summary.relay?.label) {
    if (zeroHop) {
      return ensureRelayGuessSuffix('直收', summary);
    }
    return ensureRelayGuessSuffix(formatRelayLabel(summary.relay), summary);
  }

  if (relayMeshIdRaw) {
    if (zeroHop) {
      return ensureRelayGuessSuffix('直收', summary);
    }
    return ensureRelayGuessSuffix(
      formatRelayLabel({ label: summary.relay?.label || relayMeshIdRaw, meshId: relayMeshIdRaw }),
      summary
    );
  }

  if (zeroHop) {
    return ensureRelayGuessSuffix('直收', summary);
  }

  if (usedHops != null && usedHops > 0) {
    return ensureRelayGuessSuffix('未知', summary);
  }

  if (!normalizedHopsLabel) {
    return ensureRelayGuessSuffix('直收', summary);
  }

  if (normalizedHopsLabel.includes('?')) {
    return ensureRelayGuessSuffix('未知', summary);
  }

  return ensureRelayGuessSuffix('', summary);
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

function initializeChannelMessages() {
  CHANNEL_CONFIG.forEach((channel) => {
    if (!channelMessageStore.has(channel.id)) {
      channelMessageStore.set(channel.id, []);
    }
  });

  if (channelNav) {
    channelNav.innerHTML = '';
    CHANNEL_CONFIG.forEach((channel) => {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'channel-nav-btn';
      button.dataset.channelId = String(channel.id);

      const codeEl = document.createElement('span');
      codeEl.className = 'channel-nav-code';
      codeEl.textContent = channel.code;

      const textWrap = document.createElement('span');
      textWrap.className = 'channel-nav-text';

      const nameEl = document.createElement('span');
      nameEl.className = 'channel-nav-name';
      nameEl.textContent = channel.name;

      const noteEl = document.createElement('span');
      noteEl.className = 'channel-nav-note';
      noteEl.textContent = channel.note;

      const metaEl = document.createElement('span');
      metaEl.className = 'channel-nav-meta';
      metaEl.textContent = '尚無訊息';

      textWrap.append(nameEl, noteEl, metaEl);
      button.append(codeEl, textWrap);
      button.addEventListener('click', () => selectChannel(channel.id));
      channelNav.appendChild(button);
      channelNavButtons.set(channel.id, { button, meta: metaEl });
    });
  }

  if (!CHANNEL_CONFIG.some((channel) => channel.id === selectedChannelId)) {
    selectedChannelId = CHANNEL_CONFIG[0]?.id ?? null;
  }

  if (selectedChannelId != null) {
    selectChannel(selectedChannelId);
  }
}

function selectChannel(channelId) {
  if (!channelMessageStore.has(channelId)) {
    return;
  }
  selectedChannelId = channelId;
  channelNavButtons.forEach(({ button }) => {
    const isActive = Number(button.dataset.channelId) === channelId;
    button.classList.toggle('active', isActive);
    if (isActive) {
      button.classList.remove('channel-nav-btn--unread');
    }
  });

  const channel = CHANNEL_CONFIG.find((item) => item.id === channelId);
  if (channelTitleLabel) {
    channelTitleLabel.textContent = channel ? `${channel.name} (${channel.code})` : `Channel ${channelId}`;
  }
  if (channelNoteLabel) {
    channelNoteLabel.textContent = channel?.note || '';
  }

  updateChannelNavMeta(channelId, { unread: false });
  renderChannelMessages(channelId);
}

function updateChannelNavMeta(channelId, { unread = false } = {}) {
  const navItem = channelNavButtons.get(channelId);
  if (!navItem) return;
  const latest = channelMessageStore.get(channelId)?.[0];
  if (latest) {
    const timeLabel = latest.timestampLabel || '—';
    const fromLabel = latest.from || '未知節點';
    navItem.meta.textContent = `${timeLabel} · ${fromLabel}`;
  } else {
    navItem.meta.textContent = '尚無訊息';
  }
  if (unread) {
    navItem.button.classList.add('channel-nav-btn--unread');
  } else {
    navItem.button.classList.remove('channel-nav-btn--unread');
  }
}

function appendMeta(metaEl, text) {
  if (!text) return;
  if (metaEl.children.length) {
    const separator = document.createElement('span');
    separator.className = 'meta-separator';
    separator.textContent = '•';
    metaEl.appendChild(separator);
  }
  const span = document.createElement('span');
  span.textContent = text;
  metaEl.appendChild(span);
}

function renderChannelMessages(channelId) {
  if (channelId !== selectedChannelId) {
    return;
  }
  if (!channelMessageList) {
    return;
  }
  const entries = channelMessageStore.get(channelId) || [];
  channelMessageList.innerHTML = '';
  if (!entries.length) {
    const empty = document.createElement('div');
    empty.className = 'channel-message-empty';
    empty.textContent = '尚未收到訊息';
    channelMessageList.appendChild(empty);
    return;
  }

  entries.forEach((entry) => {
    const wrapper = document.createElement('div');
    wrapper.className = 'channel-message';

    const text = document.createElement('div');
    text.className = 'channel-message-text';
    text.textContent = entry.text;

    const meta = document.createElement('div');
    meta.className = 'channel-message-meta';
    appendMeta(meta, `來自：${entry.from}`);
    appendMeta(meta, entry.hops);
    appendMeta(meta, entry.relay);
    appendMeta(meta, `時間：${entry.timestampLabel}`);

    wrapper.append(text, meta);
    channelMessageList.appendChild(wrapper);
  });
}

function clearChannelMessages() {
  channelMessageStore.forEach((store, channelId) => {
    if (Array.isArray(store)) {
      store.length = 0;
    }
    updateChannelNavMeta(channelId, { unread: false });
  });
  renderChannelMessages(selectedChannelId);
}

function isTextSummary(summary) {
  if (!summary || typeof summary !== 'object') return false;
  const type = typeof summary.type === 'string' ? summary.type.trim().toLowerCase() : '';
  return type === 'text';
}

function recordChannelMessage(summary, { markUnread = true } = {}) {
  if (!isTextSummary(summary)) {
    return;
  }
  const channelId = Number(summary.channel);
  if (!Number.isFinite(channelId)) {
    return;
  }

  const rawDetail = typeof summary.detail === 'string' ? summary.detail.trim() : '';
  const extraDetail = Array.isArray(summary.extraLines)
    ? summary.extraLines
        .map((line) => (typeof line === 'string' ? line.trim() : ''))
        .filter(Boolean)
        .join('\n')
    : '';
  const text = rawDetail || extraDetail || '（無內容）';

  const fromLabel =
    sanitizeNodeName(summary.from?.longName) ||
    sanitizeNodeName(summary.from?.shortName) ||
    sanitizeNodeName(summary.from?.label) ||
    '未知節點';
  const timestampMs = Number.isFinite(Number(summary.timestampMs)) ? Number(summary.timestampMs) : Date.now();
  const timestampLabel =
    typeof summary.timestampLabel === 'string' && summary.timestampLabel.trim()
      ? summary.timestampLabel.trim()
      : formatLogTimestamp(new Date(timestampMs));
  const flowIdRaw = summary.flowId || `${channelId}-${timestampMs}-${text}`;
  const flowId = String(flowIdRaw);

  const hopStats = extractHopInfo(summary);
  let hopSummary;
  if (hopStats.usedHops === 0) {
    hopSummary = '跳數：0 (直收)';
  } else if (hopStats.usedHops != null && hopStats.totalHops != null) {
    hopSummary = `跳數：${hopStats.usedHops}/${hopStats.totalHops}`;
  } else if (hopStats.usedHops != null) {
    hopSummary = `跳數：${hopStats.usedHops}`;
  } else if (hopStats.hopsLabel) {
    hopSummary = `跳數：${hopStats.hopsLabel}`;
  } else {
    hopSummary = '跳數：未知';
  }

  let relaySummary = computeRelayLabel(summary);
  if (!relaySummary || relaySummary === '未知' || relaySummary === '?') {
    relaySummary = formatNodeDisplay(summary.relay);
  }
  if (!relaySummary || relaySummary === 'unknown') {
    relaySummary = '未知';
  }
  relaySummary = relaySummary === '直收' ? '最後一跳：直收' : `最後一跳：${relaySummary}`;

  const store = channelMessageStore.get(channelId) || [];
  const existingIndex = store.findIndex((entry) => entry.flowId === flowId);
  if (existingIndex !== -1) {
    store.splice(existingIndex, 1);
  }
  store.unshift({
    flowId,
    timestampMs,
    timestampLabel,
    text,
    from: fromLabel,
    hops: hopSummary,
    relay: relaySummary
  });
  if (store.length > CHANNEL_MESSAGE_LIMIT) {
    store.length = CHANNEL_MESSAGE_LIMIT;
  }
  channelMessageStore.set(channelId, store);

  const isSelected = channelId === selectedChannelId;
  updateChannelNavMeta(channelId, { unread: markUnread && !isSelected && store.length > 0 });
  if (isSelected) {
    renderChannelMessages(channelId);
  }
}

async function loadPersistedMessages() {
  if (!window.meshtastic.getMessageSnapshot) {
    return;
  }
  try {
    const snapshot = await window.meshtastic.getMessageSnapshot();
    if (!snapshot || typeof snapshot !== 'object') {
      return;
    }
    const channels = snapshot.channels || snapshot;
    const channelIds = Object.keys(channels)
      .map((key) => Number(key))
      .filter((value) => Number.isFinite(value));
    channelIds.sort((a, b) => a - b);
    channelIds.forEach((channelId) => {
      const entries = Array.isArray(channels[channelId]) ? channels[channelId] : [];
      for (let i = entries.length - 1; i >= 0; i -= 1) {
        const entry = entries[i];
        if (!entry) continue;
        const hydratedChannel = Number(entry.channel);
        const hydrated = {
          type: entry.type || 'Text',
          channel: Number.isFinite(hydratedChannel) ? hydratedChannel : Number(channelId),
          detail: entry.detail,
          extraLines: Array.isArray(entry.extraLines) ? entry.extraLines : [],
          from: entry.from,
          relay: entry.relay,
          relayMeshId: entry.relayMeshId,
          relayMeshIdNormalized: entry.relayMeshIdNormalized,
          hops: entry.hops,
          timestampMs: entry.timestampMs,
          timestampLabel: entry.timestampLabel,
          flowId: entry.flowId
        };
        recordChannelMessage(hydrated, { markUnread: false });
      }
    });
    if (selectedChannelId != null) {
      selectChannel(selectedChannelId);
    }
  } catch (err) {
    console.warn('載入訊息快取失敗:', err);
  }
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
    if (webUiEnabledCheckbox) {
      webUiEnabledCheckbox.checked = Boolean(saved.webDashboardEnabled);
    }
  } catch (err) {
    console.warn('無法載入偏好設定:', err);
    if (settingsHostInput) settingsHostInput.value = '';
    if (overlayHostInput) overlayHostInput.value = '';
    if (aprsServerInput) aprsServerInput.value = DEFAULT_APRS_SERVER;
    if (aprsBeaconIntervalInput) aprsBeaconIntervalInput.value = String(DEFAULT_APRS_BEACON_MINUTES);
    if (webUiEnabledCheckbox) webUiEnabledCheckbox.checked = false;
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
    aprsBeaconMinutes: getAprsBeaconMinutes(),
    webDashboardEnabled: webUiEnabledCheckbox ? Boolean(webUiEnabledCheckbox.checked) : false
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
    let shouldPersist = false;
    const host = typeof preferences.host === 'string' ? preferences.host.trim() : '';
    if (host && getHostValue() !== host) {
      settingsHostInput.value = host;
      if (overlayHostInput) {
        overlayHostInput.value = host;
      }
      shouldPersist = true;
    }
    if (webUiEnabledCheckbox && Object.prototype.hasOwnProperty.call(preferences, 'webDashboardEnabled')) {
      const desired = Boolean(preferences.webDashboardEnabled);
      if (webUiEnabledCheckbox.checked !== desired) {
        webUiEnabledCheckbox.checked = desired;
        shouldPersist = true;
      }
    }
    if (shouldPersist) {
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
  await initializeNodeRegistry();
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

nodesSearchInput?.addEventListener('input', () => {
  nodesSearchTerm = nodesSearchInput.value.trim().toLowerCase();
  renderNodeDatabase();
});

telemetryNodeInput?.addEventListener('focus', () => {
  renderTelemetryDropdown();
  if (getTelemetryNavigationCandidates().length) {
    showTelemetryDropdown();
  }
});

telemetryNodeInput?.addEventListener('input', (event) => {
  handleTelemetryNodeInputChange(event);
  renderTelemetryDropdown();
});

telemetryNodeInput?.addEventListener('change', (event) => {
  handleTelemetryNodeInputChange(event);
});

telemetryNodeInput?.addEventListener('blur', () => {
  setTimeout(() => {
    if (telemetryDropdownInteracting) {
      telemetryDropdownInteracting = false;
      return;
    }
    hideTelemetryDropdown();
    if (telemetryNodeInputHoldEmpty) {
      telemetryNodeInputHoldEmpty = false;
      updateTelemetryNodeInputDisplay();
    }
  }, 80);
});

telemetryNodeInput?.addEventListener('keydown', (event) => {
  if (!event) return;
  if (event.key === 'Escape') {
    event.preventDefault();
    telemetrySearchRaw = '';
    telemetrySearchTerm = '';
    telemetryNodeInputHoldEmpty = false;
    hideTelemetryDropdown();
    updateTelemetryNodeInputDisplay();
    renderTelemetryView();
    return;
  }
  const keys = ['ArrowDown', 'ArrowUp', 'PageDown', 'PageUp', 'Home', 'End', 'Enter'];
  if (keys.includes(event.key)) {
    handleTelemetryNodeNavigationKey(event);
  }
});

telemetryNodeInput?.addEventListener(
  'wheel',
  (event) => {
    handleTelemetryNodeWheel(event);
  },
  { passive: false }
);

telemetryNodeDropdown?.addEventListener('mousedown', (event) => {
  const option = event.target.closest('.telemetry-node-option');
  if (!option) {
    return;
  }
  event.preventDefault();
  const meshId = option.dataset.meshId || null;
  if (!meshId) {
    return;
  }
  telemetryDropdownInteracting = true;
  applyTelemetryNodeSelection(meshId, { hideDropdown: true });
  telemetryNodeInput?.focus();
  telemetryDropdownInteracting = false;
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
  refreshTelemetrySelectors(telemetrySelectedMeshId);
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

nodesClearBtn?.addEventListener('click', handleNodeDatabaseClear);

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

webUiEnabledCheckbox?.addEventListener('change', async () => {
  if (!window.meshtastic.setWebDashboardEnabled) {
    appendLog('APP', '目前無法切換 Web UI：功能未初始化');
    if (webUiEnabledCheckbox) {
      webUiEnabledCheckbox.checked = !webUiEnabledCheckbox.checked;
    }
    return;
  }
  const checkbox = webUiEnabledCheckbox;
  const enabled = Boolean(checkbox.checked);
  const previous = !enabled;
  checkbox.disabled = true;
  savePreferences({ persist: false });
  try {
    const result = await window.meshtastic.setWebDashboardEnabled(enabled);
    if (result && result.success === false) {
      throw new Error(result.error || 'unknown error');
    }
    appendLog('APP', enabled ? 'Web UI 已啟用' : 'Web UI 已停用');
  } catch (err) {
    console.error('切換 Web UI 失敗:', err);
    appendLog('APP', `切換 Web UI 失敗: ${err.message || err}`);
    checkbox.checked = previous;
    savePreferences({ persist: false });
  } finally {
    checkbox.disabled = false;
  }
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

telemetryClearBtn?.addEventListener('click', async () => {
  if (!window.meshtastic.clearTelemetry) {
    appendLog('APP', '目前無法清空遙測資料：功能未初始化');
    return;
  }
  const button = telemetryClearBtn;
  button.disabled = true;
  try {
    const result = await window.meshtastic.clearTelemetry();
    if (result && result.success === false) {
      throw new Error(result.error || 'unknown error');
    }
    appendLog('APP', '已清空遙測資料');
  } catch (err) {
    console.error('清空遙測資料失敗:', err);
    appendLog('APP', `清空遙測資料失敗: ${err.message || err}`);
  } finally {
    button.disabled = false;
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

relayHintCloseBtn?.addEventListener('click', () => {
  hideRelayHintModal();
});

relayHintModal?.addEventListener('click', (event) => {
  if (event.target === relayHintModal) {
    hideRelayHintModal();
  }
});

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && relayHintModal && !relayHintModal.classList.contains('hidden')) {
    hideRelayHintModal();
  }
});

const unsubscribeSummary = window.meshtastic.onSummary((summary) => {
  appendSummaryRow(summary);
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

const unsubscribeNodeSnapshot = window.meshtastic.onNodeSnapshot?.((list) => {
  applyNodeRegistrySnapshot(list);
});

const unsubscribeNode = window.meshtastic.onNode?.((payload) => {
  handleNodeEvent(payload);
});

window.addEventListener('beforeunload', () => {
  unsubscribeSummary();
  unsubscribeStatus();
  unsubscribeCallMeshStatus?.();
  unsubscribeCallMeshLog?.();
  unsubscribeMyInfo?.();
  unsubscribeAprsUplink?.();
  unsubscribeTelemetry?.();
  unsubscribeNodeSnapshot?.();
  unsubscribeNode?.();
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

function hydrateSummaryNode(node, fallbackMeshId = null) {
  const meshCandidate = node?.meshId ?? node?.meshIdNormalized ?? fallbackMeshId;
  const registryNode = getRegistryNode(meshCandidate);
  const upserted = node ? upsertNodeRegistry(node) : null;
  const merged = mergeNodeMetadata(node, upserted, registryNode);
  return merged || node || registryNode || null;
}

function hydrateSummaryNodes(summary) {
  if (!summary || typeof summary !== 'object') {
    return summary;
  }
  if (summary.from || summary.fromMeshId) {
    summary.from = hydrateSummaryNode(summary.from, summary.fromMeshId);
  }
  if (summary.to || summary.toMeshId) {
    summary.to = hydrateSummaryNode(summary.to, summary.toMeshId);
  }
  if (summary.relay || summary.relayMeshId) {
    summary.relay = hydrateSummaryNode(summary.relay, summary.relayMeshId);
  }
  if (summary.nextHop || summary.nextHopMeshId) {
    summary.nextHop = hydrateSummaryNode(summary.nextHop, summary.nextHopMeshId);
  }
  return summary;
}

function appendSummaryRow(summary) {
  if (!summary) return;
  hydrateSummaryNodes(summary);
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
  const hopInfo = extractHopInfo(summary);
  const relayLabel = computeRelayLabel(summary);
  let relayGuessed = isRelayGuessed(summary);
  if (relayLabel === '直收' || relayLabel === 'Self') {
    relayGuessed = false;
  }
  const relayGuessReason = relayGuessed ? summary.relayGuessReason || RELAY_GUESS_EXPLANATION : '';
  relayCell.innerHTML = '';
  const relayLabelSpan = document.createElement('span');
  const relayDisplay = relayLabel || (relayGuessed ? '未知' : '—');
  relayLabelSpan.textContent = relayDisplay;
  relayCell.appendChild(relayLabelSpan);

  const relayMeshId =
    summary.relay?.meshId ||
    summary.relay?.meshIdNormalized ||
    summary.relayMeshId ||
    summary.relayMeshIdNormalized ||
    '';
  let relayTitle = '';
  let normalizedRelayId = '';
  if (relayMeshId) {
    normalizedRelayId = relayMeshId.startsWith('0x') ? `!${relayMeshId.slice(2)}` : relayMeshId;
    if (relayLabel && relayLabel !== normalizedRelayId) {
      relayTitle = `${relayLabel} (${normalizedRelayId})`;
    } else {
      relayTitle = normalizedRelayId;
    }
  } else if (relayLabel === '直收') {
    relayTitle = '訊息為直收，未經其他節點轉發';
  } else if (relayLabel === 'Self') {
    const selfLabel = selfNodeState.name || selfNodeState.meshId || '本站節點';
    relayTitle = `${selfLabel} 轉發`;
  } else if (relayGuessed) {
    relayTitle = '最後轉發節點未知或標號不完整';
  }

  if (relayGuessed) {
    const reason = relayGuessReason || RELAY_GUESS_EXPLANATION;
    relayCell.classList.add('relay-guess');
    relayCell.dataset.relayGuess = 'true';
    const hintButton = document.createElement('button');
    hintButton.type = 'button';
    hintButton.className = 'relay-hint-btn';
    hintButton.textContent = '?';
    hintButton.title = reason;
    hintButton.setAttribute('aria-label', '顯示推測原因');
    hintButton.addEventListener('click', (event) => {
      event.stopPropagation();
      showRelayHint({
        reason,
        relayLabel: relayLabel || normalizedRelayId || '',
        meshId: normalizedRelayId || ''
      });
    });
    relayCell.appendChild(hintButton);
  } else {
    relayCell.classList.remove('relay-guess');
    relayCell.removeAttribute('data-relay-guess');
  }

  if (relayTitle) {
    relayCell.title = relayTitle;
  } else {
    relayCell.removeAttribute('title');
  }
  row.querySelector('.channel').textContent = summary.channel ?? '';
  row.querySelector('.snr').textContent = formatNumber(summary.snr, 2);
  row.querySelector('.rssi').textContent = formatNumber(summary.rssi, 0);
  renderTypeCell(row.querySelector('.type'), summary);
  row.querySelector('.hops').textContent = hopInfo.hopsLabel || summary.hops?.label || '';
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

  recordChannelMessage(summary);
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
  let relayGuessed = isRelayGuessed(summary);
  if (relayLabel === '直收' || relayLabel === 'Self') {
    relayGuessed = false;
  }
  const relayGuessReason = relayGuessed ? summary.relayGuessReason || RELAY_GUESS_EXPLANATION : '';

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

  const relayMeshIdRaw =
    summary.relay?.meshId ||
    summary.relay?.meshIdNormalized ||
    summary.relayMeshId ||
    summary.relayMeshIdNormalized ||
    '';
  const relayMeshIdNormalized = normalizeMeshId(relayMeshIdRaw);

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
    relayGuess: relayGuessed,
    relayGuessReason,
    relayMeshId: relayMeshIdRaw,
    relayMeshIdNormalized,
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
    if (entry.relayGuessReason) {
      const relayChip = metaWrap.querySelector('.chip-relay');
      if (relayChip) {
        relayChip.title = entry.relayGuessReason;
        relayChip.classList.add('chip-relay-guess');
        if (!relayChip.querySelector('.relay-hint-btn')) {
          const hintBtn = document.createElement('button');
          hintBtn.type = 'button';
          hintBtn.className = 'relay-hint-btn relay-hint-btn--chip';
          hintBtn.textContent = '?';
          hintBtn.title = entry.relayGuessReason;
          hintBtn.setAttribute('aria-label', '顯示推測原因');
          hintBtn.addEventListener('click', (event) => {
            event.stopPropagation();
            showRelayHint({
              reason: entry.relayGuessReason,
              relayLabel: entry.relayLabel || '',
              meshId: entry.relayMeshIdNormalized || entry.relayMeshId || ''
            });
          });
          relayChip.appendChild(hintBtn);
        }
      }
    }
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

function setNodeDatabaseStatus(message, variant = 'info') {
  if (!nodesStatusLabel) return;
  if (!message) {
    nodesStatusLabel.textContent = '';
    delete nodesStatusLabel.dataset.variant;
    return;
  }
  nodesStatusLabel.textContent = message;
  nodesStatusLabel.dataset.variant = variant;
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

function formatBytes(bytes) {
  const numeric = Number(bytes);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return '—';
  }
  if (numeric === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const index = Math.min(Math.floor(Math.log10(numeric) / Math.log10(1024)), units.length - 1);
  const value = numeric / 1024 ** index;
  return `${value >= 100 ? value.toFixed(0) : value >= 10 ? value.toFixed(1) : value.toFixed(2)} ${units[index]}`;
}

function normalizeEnumLabel(value) {
  if (value === undefined || value === null) {
    return null;
  }
  return String(value).replace(/_/g, ' ').trim();
}

function setNodeDistanceReferenceFromProvision(provision) {
  if (!provision) {
    nodeDistanceReference = null;
    return;
  }
  const lat = Number(provision.latitude ?? provision.lat);
  const lon = Number(provision.longitude ?? provision.lon);
  if (Number.isFinite(lat) && Number.isFinite(lon)) {
    nodeDistanceReference = { lat, lon };
  } else {
    nodeDistanceReference = null;
  }
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

function formatNodeCoordinateValue(entry) {
  if (!entry) {
    return '';
  }
  const latRaw = entry.latitude;
  const lonRaw = entry.longitude;
  if (latRaw == null || lonRaw == null) {
    return '';
  }
  const lat = typeof latRaw === 'number' ? latRaw : Number(latRaw);
  const lon = typeof lonRaw === 'number' ? lonRaw : Number(lonRaw);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return '';
  }
  if (Math.abs(lat) > 90 || Math.abs(lon) > 180) {
    return '';
  }
  const formatComponent = (value) => {
    const fixed = value.toFixed(5);
    const trimmed = fixed.replace(/\.?0+$/, '');
    return trimmed || '0';
  };
  const components = [`${formatComponent(lat)}`, `${formatComponent(lon)}`];
  const altitudeRaw = entry.altitude;
  const altitude = typeof altitudeRaw === 'number' ? altitudeRaw : Number(altitudeRaw);
  if (Number.isFinite(altitude)) {
    components.push(`${Math.round(altitude)}m`);
  }
  return components.join(', ');
}

function formatNodeDistanceValue(entry) {
  if (
    !nodeDistanceReference ||
    !Number.isFinite(nodeDistanceReference.lat) ||
    !Number.isFinite(nodeDistanceReference.lon)
  ) {
    return '';
  }
  const latRaw = entry.latitude;
  const lonRaw = entry.longitude;
  if (latRaw == null || lonRaw == null) {
    return '';
  }
  const lat = typeof latRaw === 'number' ? latRaw : Number(latRaw);
  const lon = typeof lonRaw === 'number' ? lonRaw : Number(lonRaw);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return '';
  }
  if (Math.abs(lat) > 90 || Math.abs(lon) > 180) {
    return '';
  }
  if (Math.abs(lat) < 1e-6 && Math.abs(lon) < 1e-6) {
    return '';
  }
  const distanceKm = haversineKm(nodeDistanceReference.lat, nodeDistanceReference.lon, lat, lon);
  if (!Number.isFinite(distanceKm)) {
    return '';
  }
  if (distanceKm < 1) {
    const meters = Math.round(distanceKm * 1000);
    return `${meters} m`;
  }
  if (distanceKm >= 100) {
    return `${distanceKm.toFixed(0)} km`;
  }
  return `${distanceKm.toFixed(distanceKm >= 10 ? 1 : 2)} km`;
}

function formatNodeLastSeen(value) {
  if (value == null) {
    return { display: '—', tooltip: '', timestamp: null };
  }
  let timestamp = null;
  if (Number.isFinite(value)) {
    timestamp = Number(value);
  } else if (typeof value === 'string' && value.trim()) {
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
      timestamp = numeric;
    } else {
      const parsed = Date.parse(value);
      if (!Number.isNaN(parsed)) {
        timestamp = parsed;
      }
    }
  }
  if (!Number.isFinite(timestamp)) {
    return { display: '—', tooltip: '', timestamp: null };
  }
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return { display: '—', tooltip: '', timestamp: null };
  }
  const display = date.toLocaleString();
  const relative = formatRelativeTime(date.toISOString());
  const tooltip = display;
  return { display: relative, tooltip, timestamp };
}

function mergeNodeMetadata(...sources) {
  const result = {
    label: null,
    meshId: null,
    meshIdNormalized: null,
    meshIdOriginal: null,
    shortName: null,
    longName: null,
    hwModel: null,
    hwModelLabel: null,
    role: null,
    roleLabel: null,
    latitude: null,
    longitude: null,
    altitude: null,
    lastSeenAt: null
  };
  let hasValue = false;
  for (const source of sources) {
    if (!source || typeof source !== 'object') continue;
    const items = Array.isArray(source) ? source : [source];
    for (const item of items) {
      if (!item || typeof item !== 'object') continue;
      for (const [key, value] of Object.entries(item)) {
        if (value === undefined || value === null) continue;
        if (Object.prototype.hasOwnProperty.call(result, key)) {
          result[key] = value;
          hasValue = true;
        }
        if (key === 'lastSeenAt' && Number.isFinite(value)) {
          result.lastSeenAt = Number(value);
        }
      }
      if (item.position && typeof item.position === 'object') {
        const pos = item.position;
        if (Number.isFinite(pos.latitude)) {
          result.latitude = Number(pos.latitude);
          hasValue = true;
        }
        if (Number.isFinite(pos.longitude)) {
          result.longitude = Number(pos.longitude);
          hasValue = true;
        }
        if (Number.isFinite(pos.altitude)) {
          result.altitude = Number(pos.altitude);
          hasValue = true;
        }
      }
      if (item.latitude != null) {
        const numeric = Number(item.latitude);
        if (Number.isFinite(numeric)) {
          result.latitude = numeric;
          hasValue = true;
        }
      }
      if (item.longitude != null) {
        const numeric = Number(item.longitude);
        if (Number.isFinite(numeric)) {
          result.longitude = numeric;
          hasValue = true;
        }
      }
      if (item.altitude != null) {
        const numeric = Number(item.altitude);
        if (Number.isFinite(numeric)) {
          result.altitude = numeric;
          hasValue = true;
        }
      }
    }
  }
  if (!hasValue) {
    return null;
  }
  if (!result.meshIdNormalized && result.meshId) {
    result.meshIdNormalized = normalizeMeshId(result.meshId);
  }
  if (!result.meshId && result.meshIdNormalized) {
    result.meshId = result.meshIdNormalized;
  }
  if (result.hwModel && !result.hwModelLabel) {
    result.hwModelLabel = normalizeEnumLabel(result.hwModel);
  }
  if (result.role && !result.roleLabel) {
    result.roleLabel = normalizeEnumLabel(result.role);
  }
  if (!Number.isFinite(result.latitude) || Math.abs(result.latitude) > 90) {
    result.latitude = null;
  }
  if (!Number.isFinite(result.longitude) || Math.abs(result.longitude) > 180) {
    result.longitude = null;
  }
  if (
    result.latitude !== null &&
    result.longitude !== null &&
    Math.abs(result.latitude) < 1e-6 &&
    Math.abs(result.longitude) < 1e-6
  ) {
    result.latitude = null;
    result.longitude = null;
  }
  if (!Number.isFinite(result.altitude)) {
    result.altitude = null;
  }
  if (result.latitude === null || result.longitude === null) {
    result.altitude = null;
  }
  if (!result.label) {
    const name = result.longName || result.shortName || null;
    const meshLabel = result.meshIdOriginal || result.meshId || null;
    if (name && meshLabel) {
      result.label = `${name} (${meshLabel})`;
    } else {
      result.label = name || meshLabel || null;
    }
  }
  return result;
}

function upsertNodeRegistry(entry) {
  if (!entry || typeof entry !== 'object') return null;
  const keyCandidate = entry.meshId || entry.meshIdNormalized || entry.meshIdOriginal;
  const normalized = normalizeMeshId(keyCandidate);
  if (!normalized) return null;
  if (isIgnoredMeshId(normalized) || isIgnoredMeshId(entry.meshIdOriginal)) {
    nodeRegistry.delete(normalized);
    return null;
  }
  const existing = nodeRegistry.get(normalized) || null;
  const merged = mergeNodeMetadata(existing, entry, { meshIdNormalized: normalized });
  if (merged) {
    nodeRegistry.set(normalized, merged);
    updateTelemetryNodesWithRegistry(normalized, merged);
  }
  return merged;
}

function getSortedNodeRegistryEntries() {
  const entries = Array.from(nodeRegistry.values()).filter(
    (entry) => !isIgnoredMeshId(entry.meshId) && !isIgnoredMeshId(entry.meshIdOriginal)
  );
  entries.sort((a, b) => {
    const timeA = typeof a.lastSeenAt === 'number'
      ? a.lastSeenAt
      : typeof a.lastSeenAt === 'string'
        ? (Date.parse(a.lastSeenAt) || 0)
        : 0;
    const timeB = typeof b.lastSeenAt === 'number'
      ? b.lastSeenAt
      : typeof b.lastSeenAt === 'string'
        ? (Date.parse(b.lastSeenAt) || 0)
        : 0;
    if (timeA !== timeB) {
      return timeB - timeA;
    }
    const labelA = (a.longName || a.shortName || a.meshIdOriginal || a.meshId || '').toLowerCase();
    const labelB = (b.longName || b.shortName || b.meshIdOriginal || b.meshId || '').toLowerCase();
    if (labelA < labelB) return -1;
    if (labelA > labelB) return 1;
    return 0;
  });
  return entries;
}

function renderNodeDatabase() {
  if (!nodesTableBody || !nodesTotalCountLabel) {
    return;
  }
  const entries = getSortedNodeRegistryEntries();
  const totalCount = entries.length;
  const hasFilter = Boolean(nodesSearchTerm);
  const filteredEntries = hasFilter
    ? entries.filter((entry) => matchesNodeSearch(entry, nodesSearchTerm))
    : entries;

  nodesTotalCountLabel.textContent = hasFilter
    ? `${filteredEntries.length} / ${totalCount}`
    : String(totalCount);

  const now = Date.now();
  const totalOnline = entries.reduce((acc, entry) => {
    const ts = getNodeLastSeenTimestamp(entry);
    return acc + (ts != null && now - ts <= NODE_ONLINE_WINDOW_MS ? 1 : 0);
  }, 0);

  if (!filteredEntries.length) {
    nodesTableBody.innerHTML = '';
    nodesTableWrapper?.classList.add('hidden');
    nodesEmptyState?.classList.remove('hidden');
    if (nodesEmptyState) {
      nodesEmptyState.textContent = hasFilter ? '沒有符合搜尋的節點。' : '目前沒有節點資料。';
    }
    if (nodesOnlineCountLabel) {
      nodesOnlineCountLabel.textContent = '0';
    }
    if (nodesOnlineTotalLabel) {
      nodesOnlineTotalLabel.textContent = hasFilter ? ` / ${totalOnline}` : '';
    }
    return;
  }

  nodesTableWrapper?.classList.remove('hidden');
  nodesEmptyState?.classList.add('hidden');
  let onlineCount = 0;

  const rows = filteredEntries.map((entry) => {
    const longName = sanitizeNodeName(entry.longName);
    const shortName = sanitizeNodeName(entry.shortName);
    const labelName = sanitizeNodeName(entry.label);
    const meshIdOriginal = entry.meshIdOriginal || '';
    const meshId = entry.meshId || '';

    const primaryName =
      longName ||
      shortName ||
      labelName ||
      meshIdOriginal ||
      meshId ||
      '—';

    const secondaryParts = [];
    if (shortName && shortName !== primaryName) {
      secondaryParts.push(shortName);
    }
    if (labelName && labelName !== primaryName && secondaryParts.indexOf(labelName) === -1) {
      secondaryParts.push(labelName);
    }
    const nameSegments = [`<div class="nodes-name-primary">${escapeHtml(primaryName)}</div>`];
    if (secondaryParts.length) {
      nameSegments.push(`<div class="nodes-name-secondary">${escapeHtml(secondaryParts.join(' / '))}</div>`);
    }

    const meshLabel = meshIdOriginal || meshId || '—';
    const hwModelDisplay = entry.hwModelLabel || normalizeEnumLabel(entry.hwModel) || '—';
    const roleDisplay = entry.roleLabel || normalizeEnumLabel(entry.role) || '—';
    const coordinateDisplay = formatNodeCoordinateValue(entry);
    const distanceDisplay = formatNodeDistanceValue(entry);

    const { display: lastSeenDisplay, tooltip: lastSeenTooltip, timestamp: lastSeenTimestamp } = formatNodeLastSeen(entry.lastSeenAt);
    if (lastSeenTimestamp != null && now - lastSeenTimestamp <= NODE_ONLINE_WINDOW_MS) {
      onlineCount += 1;
    }
    const lastSeenCell =
      lastSeenDisplay === '—'
        ? '—'
        : `<span title="${escapeHtml(lastSeenTooltip || '')}">${escapeHtml(lastSeenDisplay)}</span>`;

    return (
      '<tr>' +
      `<td>${nameSegments.join('')}</td>` +
      `<td>${escapeHtml(meshLabel)}</td>` +
      `<td>${escapeHtml(hwModelDisplay)}</td>` +
      `<td>${escapeHtml(roleDisplay)}</td>` +
      `<td>${escapeHtml(coordinateDisplay || '—')}</td>` +
      `<td>${escapeHtml(distanceDisplay)}</td>` +
      `<td>${lastSeenCell}</td>` +
      '</tr>'
    );
  });

  nodesTableBody.innerHTML = rows.join('');
  if (nodesOnlineCountLabel) {
    nodesOnlineCountLabel.textContent = String(onlineCount);
  }
  if (nodesOnlineTotalLabel) {
    nodesOnlineTotalLabel.textContent = hasFilter ? ` / ${totalOnline}` : '';
  }
}

function sanitizeNodeName(value) {
  if (!value || typeof value !== 'string') {
    return '';
  }
  const trimmed = value.trim();
  if (!trimmed) return '';
  const lowered = trimmed.toLowerCase();
  if (lowered === 'unknown' || lowered === 'null') {
    return '';
  }
  return trimmed;
}

function resolveTelemetryNodeSelection(raw, { allowPartial = false } = {}) {
  if (!raw) return null;
  const lowered = raw.toLowerCase();
  if (telemetryNodeLookup.has(lowered)) {
    return telemetryNodeLookup.get(lowered) || null;
  }
  const normalized = normalizeMeshId(raw);
  if (normalized && telemetryStore.has(normalized)) {
    return normalized;
  }
  if (allowPartial) {
    for (const [displayLower, meshId] of telemetryNodeLookup.entries()) {
      if (displayLower.includes(lowered)) {
        return meshId;
      }
    }
  }
  return null;
}

function updateTelemetryNodeInputDisplay() {
  if (!telemetryNodeInput) {
    return;
  }
  if (telemetryNodeInputHoldEmpty) {
    if (document.activeElement === telemetryNodeInput) {
      telemetryNodeInput.value = '';
      return;
    }
    telemetryNodeInputHoldEmpty = false;
  }
  if (telemetrySearchRaw) {
    telemetryNodeInput.value = telemetrySearchRaw;
    return;
  }
  if (telemetrySelectedMeshId && telemetryNodeDisplayByMesh.has(telemetrySelectedMeshId)) {
    telemetryNodeInput.value = telemetryNodeDisplayByMesh.get(telemetrySelectedMeshId);
    return;
  }
  if (telemetrySelectedMeshId) {
    telemetryNodeInput.value = telemetrySelectedMeshId;
    return;
  }
  telemetryNodeInput.value = '';
}

function getTelemetryNavigationCandidates() {
  if (!telemetryNodeOptions.length) {
    return [];
  }
  if (!telemetrySearchTerm) {
    return telemetryNodeOptions;
  }
  return telemetryNodeOptions.filter((entry) => {
    if (!entry || !Array.isArray(entry.searchKeys)) return false;
    return entry.searchKeys.some((key) => key && key.includes(telemetrySearchTerm));
  });
}

function findTelemetryCandidateIndex(candidates, meshId) {
  if (!meshId) return -1;
  return candidates.findIndex((entry) => entry.meshId === meshId);
}

function showTelemetryDropdown() {
  if (!telemetryNodeDropdown) return;
  if (telemetryDropdownVisible) return;
  telemetryNodeDropdown.classList.remove('hidden');
  telemetryDropdownVisible = true;
}

function hideTelemetryDropdown() {
  if (!telemetryNodeDropdown) return;
  if (!telemetryDropdownVisible && !telemetryNodeDropdown.hasChildNodes()) {
    return;
  }
  telemetryNodeDropdown.classList.add('hidden');
  telemetryNodeDropdown.innerHTML = '';
  telemetryDropdownVisible = false;
  telemetryDropdownInteracting = false;
}

function highlightTelemetryDropdown(meshId) {
  if (!telemetryNodeDropdown) return;
  const options = telemetryNodeDropdown.querySelectorAll('.telemetry-node-option');
  let activeOption = null;
  options.forEach((option) => {
    const isActive = option.dataset.meshId === meshId;
    option.classList.toggle('active', isActive);
    if (isActive) {
      activeOption = option;
    }
  });
  if (activeOption) {
    activeOption.scrollIntoView({ block: 'nearest' });
  }
}

function renderTelemetryDropdown() {
  if (!telemetryNodeDropdown) {
    return;
  }
  const candidates = getTelemetryNavigationCandidates();
  const shouldShow = Boolean(candidates.length) && document.activeElement === telemetryNodeInput;
  if (!shouldShow) {
    hideTelemetryDropdown();
    return;
  }
  const fragment = document.createDocumentFragment();
  for (const candidate of candidates) {
    const option = document.createElement('div');
    option.className = 'telemetry-node-option';
    option.dataset.meshId = candidate.meshId || '';
    const displayText = candidate.display || candidate.meshId || '未知節點';
    option.textContent = displayText;
    option.title = displayText;
    fragment.appendChild(option);
  }
  telemetryNodeDropdown.innerHTML = '';
  telemetryNodeDropdown.appendChild(fragment);
  showTelemetryDropdown();
  highlightTelemetryDropdown(telemetrySelectedMeshId);
}

function applyTelemetryNodeSelection(meshId, { preserveSearch = false, hideDropdown = false } = {}) {
  if (!meshId) {
    return;
  }
  telemetrySelectedMeshId = meshId;
  telemetryLastExplicitMeshId = meshId;
  telemetryNodeInputHoldEmpty = false;
  if (!preserveSearch) {
    telemetrySearchRaw = '';
    telemetrySearchTerm = '';
  }
  updateTelemetryNodeInputDisplay();
  renderTelemetryView();
  if (hideDropdown) {
    hideTelemetryDropdown();
  } else {
    renderTelemetryDropdown();
  }
}

function handleTelemetryNodeNavigationKey(event) {
  if (!telemetryNodeInput) {
    return;
  }
  renderTelemetryDropdown();
  const candidates = getTelemetryNavigationCandidates();
  if (!candidates.length) {
    return;
  }

  const key = event.key;
  let nextIndex = null;
  const pageJump = Math.max(1, Math.floor(candidates.length / 10)) || 1;
  const currentIndex = findTelemetryCandidateIndex(candidates, telemetrySelectedMeshId);
  const fallbackMeshId = telemetrySelectedMeshId || telemetryLastExplicitMeshId || getFirstTelemetryMeshId();
  let effectiveIndex = currentIndex;
  if (effectiveIndex === -1 && fallbackMeshId) {
    effectiveIndex = findTelemetryCandidateIndex(candidates, fallbackMeshId);
  }

  if (key === 'ArrowDown') {
    nextIndex = effectiveIndex === -1 ? 0 : Math.min(effectiveIndex + 1, candidates.length - 1);
  } else if (key === 'ArrowUp') {
    nextIndex = effectiveIndex === -1 ? candidates.length - 1 : Math.max(effectiveIndex - 1, 0);
  } else if (key === 'PageDown') {
    nextIndex = effectiveIndex === -1 ? Math.min(pageJump, candidates.length - 1) : Math.min(effectiveIndex + pageJump, candidates.length - 1);
  } else if (key === 'PageUp') {
    nextIndex = effectiveIndex === -1 ? Math.max(candidates.length - 1 - pageJump, 0) : Math.max(effectiveIndex - pageJump, 0);
  } else if (key === 'Home') {
    nextIndex = 0;
  } else if (key === 'End') {
    nextIndex = candidates.length - 1;
  } else if (key === 'Enter') {
    nextIndex = effectiveIndex === -1 ? 0 : effectiveIndex;
  } else {
    return;
  }

  if (nextIndex == null || nextIndex < 0 || nextIndex >= candidates.length) {
    return;
  }

  event.preventDefault();
  const target = candidates[nextIndex];
  if (!target || !target.meshId) {
    return;
  }
  const hideDropdown = key === 'Enter';
  applyTelemetryNodeSelection(target.meshId, { hideDropdown });
}

function handleTelemetryNodeWheel(event) {
  if (!telemetryNodeInput || document.activeElement !== telemetryNodeInput) {
    return;
  }
  renderTelemetryDropdown();
  const candidates = getTelemetryNavigationCandidates();
  if (!candidates.length) {
    return;
  }
  const direction = event.deltaY;
  if (!direction) {
    return;
  }
  event.preventDefault();
  const currentIndex = findTelemetryCandidateIndex(candidates, telemetrySelectedMeshId);
  const fallbackMeshId = telemetrySelectedMeshId || telemetryLastExplicitMeshId || getFirstTelemetryMeshId();
  let effectiveIndex = currentIndex;
  if (effectiveIndex === -1 && fallbackMeshId) {
    effectiveIndex = findTelemetryCandidateIndex(candidates, fallbackMeshId);
  }
  let nextIndex;
  if (direction > 0) {
    nextIndex = effectiveIndex === -1 ? 0 : Math.min(effectiveIndex + 1, candidates.length - 1);
  } else {
    nextIndex = effectiveIndex === -1 ? candidates.length - 1 : Math.max(effectiveIndex - 1, 0);
  }
  const target = candidates[nextIndex];
  if (!target || !target.meshId) {
    return;
  }
  applyTelemetryNodeSelection(target.meshId);
}

function handleTelemetryNodeInputChange(event) {
  if (!telemetryNodeInput) {
    return;
  }
  const rawValue = telemetryNodeInput.value;
  const raw = rawValue.trim();
  const isInputEvent = event?.type === 'input';

  if (!raw) {
    telemetryNodeInputHoldEmpty =
      isInputEvent && document.activeElement === telemetryNodeInput;
    telemetrySearchRaw = '';
    telemetrySearchTerm = '';
    if (!telemetrySelectedMeshId) {
      const fallback = telemetryLastExplicitMeshId || getFirstTelemetryMeshId();
      telemetrySelectedMeshId = fallback;
      if (fallback) {
        telemetryLastExplicitMeshId = fallback;
      }
    }
    updateTelemetryNodeInputDisplay();
    renderTelemetryView();
    return;
  }
  telemetryNodeInputHoldEmpty = false;
  const isChangeEvent = event?.type === 'change';
  const matched = resolveTelemetryNodeSelection(raw, {
    allowPartial: isChangeEvent
  });
  if (matched) {
    applyTelemetryNodeSelection(matched, { hideDropdown: isChangeEvent });
    return;
  }
  telemetrySearchRaw = raw;
  telemetrySearchTerm = raw.toLowerCase();
  updateTelemetryNodeInputDisplay();
  renderTelemetryView();
}

function matchesNodeSearch(entry, term) {
  if (!term) return true;
  const lowerTerm = term.toLowerCase();
  const fields = [
    sanitizeNodeName(entry.longName),
    sanitizeNodeName(entry.shortName),
    sanitizeNodeName(entry.label),
    entry.meshId,
    entry.meshIdOriginal,
    entry.meshIdNormalized,
    entry.hwModel,
    entry.hwModelLabel,
    entry.role,
    entry.roleLabel,
    formatNodeCoordinateValue(entry)
  ];
  return fields.some((value) => {
    if (!value) return false;
    return String(value).toLowerCase().includes(lowerTerm);
  });
}

function getNodeLastSeenTimestamp(entry) {
  const value = entry?.lastSeenAt;
  if (Number.isFinite(value)) {
    return Number(value);
  }
  if (typeof value === 'string' && value.trim()) {
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
      return numeric;
    }
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return null;
}

async function handleNodeDatabaseClear() {
  if (!window.meshtastic?.clearNodeDatabase) {
    setNodeDatabaseStatus('無法清除節點資料庫：IPC 尚未就緒', 'error');
    return;
  }

  if (nodesClearBtn) {
    nodesClearBtn.disabled = true;
  }
  setNodeDatabaseStatus('正在清除節點資料庫...', 'info');

  try {
    const result = await window.meshtastic.clearNodeDatabase();
    if (!result || result.success !== true) {
      throw new Error(result?.error || '未知錯誤');
    }
    const snapshot = Array.isArray(result.nodes) ? result.nodes : [];
    applyNodeRegistrySnapshot(snapshot);
    const cleared =
      typeof result.cleared === 'number' && Number.isFinite(result.cleared)
        ? result.cleared
        : snapshot.length;
    setNodeDatabaseStatus(`已清除 ${cleared} 個節點`, 'success');
    appendLog('NODE-DB', `cleared node database count=${cleared}`);
  } catch (err) {
    setNodeDatabaseStatus(`清除節點資料庫失敗：${err.message}`, 'error');
    appendLog('NODE-DB', `clear node database failed: ${err.message}`);
  } finally {
    if (nodesClearBtn) {
      nodesClearBtn.disabled = false;
    }
    renderNodeDatabase();
  }
}

function applyNodeRegistrySnapshot(list) {
  nodeRegistry.clear();
  if (Array.isArray(list)) {
    for (const entry of list) {
      upsertNodeRegistry(entry);
    }
  }
  nodeSnapshotLoaded = true;
  refreshTelemetrySelectors(telemetrySelectedMeshId);
  renderNodeDatabase();
  renderTelemetryView();
}

function handleNodeEvent(payload) {
  if (!payload || typeof payload !== 'object') {
    return;
  }
  const merged = upsertNodeRegistry(payload);
  if (!merged) {
    return;
  }
  renderNodeDatabase();
  if (telemetrySelectedMeshId) {
    renderTelemetryView();
  }
  refreshSummarySelfLabels();
  refreshFlowEntryLabels();
  renderFlowEntries();
}

function updateTelemetryNodesWithRegistry(normalizedMeshId, registryInfo) {
  if (!normalizedMeshId) return;
  const meshKey = resolveTelemetryMeshKey(normalizedMeshId);
  const bucket = telemetryStore.get(meshKey) || telemetryStore.get(normalizedMeshId) || null;
  if (bucket) {
    bucket.node = mergeNodeMetadata(bucket.node, registryInfo);
    if (Array.isArray(bucket.records)) {
      for (const record of bucket.records) {
        record.node = mergeNodeMetadata(record.node, registryInfo);
      }
    }
  }
}

function getRegistryNode(meshId) {
  const normalized = normalizeMeshId(meshId);
  if (!normalized) return null;
  const entry = nodeRegistry.get(normalized);
  return entry ? { ...entry } : null;
}

function updateTelemetryStats(stats) {
  if (!telemetryStatsRecords || !telemetryStatsNodes || !telemetryStatsDisk) {
    return;
  }
  if (!stats) {
    telemetryStatsRecords.textContent = '0';
    telemetryStatsNodes.textContent = '0';
    telemetryStatsDisk.textContent = '—';
    return;
  }
  const records = Number.isFinite(stats.totalRecords) ? stats.totalRecords : 0;
  const nodes = Number.isFinite(stats.totalNodes)
    ? stats.totalNodes
    : getSortedNodeRegistryEntries().length;
  telemetryStatsRecords.textContent = records.toLocaleString();
  telemetryStatsNodes.textContent = nodes.toLocaleString();
  telemetryStatsDisk.textContent = formatBytes(stats.diskBytes);
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
  const merged = mergeNodeMetadata(
    {
      label: node.label ?? null,
      meshId,
      meshIdNormalized: normalized,
      meshIdOriginal: node.meshIdOriginal ?? meshId ?? null,
      shortName: node.shortName ?? null,
      longName: node.longName ?? null,
      hwModel: node.hwModel ?? null,
      role: node.role ?? null
    },
    getRegistryNode(normalized)
  );
  return merged;
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
  telemetryNodeLookup.clear();
  telemetryNodeDisplayByMesh.clear();
  telemetryNodeOptions = [];
  hideTelemetryDropdown();
  telemetryNodeOptions = [];
  telemetryLastExplicitMeshId = null;
  if (!silent) {
    telemetryUpdatedAt = Date.now();
  }
  telemetryChartMetric = null;
  destroyAllTelemetryCharts();
  if (telemetryChartMetricSelect) {
    telemetryChartMetricSelect.innerHTML = '';
    telemetryChartMetricSelect.classList.add('hidden');
  }
  refreshTelemetrySelectors(telemetrySelectedMeshId);
  renderTelemetryView();
  updateTelemetryUpdatedAtLabel();
}

function applyTelemetrySnapshot(snapshot) {
  const previousSelection = telemetrySelectedMeshId;
  clearTelemetryDataLocal({ silent: true });
  if (!snapshot || !Array.isArray(snapshot.nodes)) {
    telemetrySelectedMeshId = null;
    telemetryUpdatedAt = snapshot?.updatedAt ?? telemetryUpdatedAt ?? null;
    refreshTelemetrySelectors(telemetrySelectedMeshId);
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
  refreshTelemetrySelectors(previousSelection);
  if (!telemetrySearchRaw && !telemetrySelectedMeshId && telemetryStore.size) {
    telemetrySelectedMeshId = telemetryStore.keys().next().value || null;
    updateTelemetryNodeInputDisplay();
  }
  renderTelemetryView();
  updateTelemetryUpdatedAtLabel();
  updateTelemetryStats(snapshot?.stats);
}

function handleTelemetryEvent(payload) {
  if (!payload || typeof payload !== 'object') {
    return;
  }
  if (payload.type === 'reset') {
    telemetryUpdatedAt = Number.isFinite(payload.updatedAt) ? Number(payload.updatedAt) : Date.now();
    clearTelemetryDataLocal({ silent: true });
    updateTelemetryUpdatedAtLabel();
    updateTelemetryStats(payload.stats);
    return;
  }
  if (payload.type === 'append') {
    appendTelemetryRecord(payload.meshId, payload.record, payload.node, payload.updatedAt);
    updateTelemetryStats(payload.stats);
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
  const preferredMesh = previousSelection || targetMeshKey;
  refreshTelemetrySelectors(preferredMesh);
  if (!telemetrySearchRaw && !telemetrySelectedMeshId && preferredMesh) {
    telemetrySelectedMeshId = preferredMesh;
    telemetryLastExplicitMeshId = preferredMesh;
    updateTelemetryNodeInputDisplay();
    renderTelemetryDropdown();
  }
  renderTelemetryView();
  updateTelemetryUpdatedAtLabel();
}

function refreshTelemetrySelectors(preferredMeshId = null) {
  if (!telemetryNodeInput) {
    return;
  }

  const previousSelection = telemetrySelectedMeshId;
  const searchActive = Boolean(telemetrySearchRaw);
  const { startMs, endMs } = getTelemetryRangeWindow();

  const nodes = Array.from(telemetryStore.values())
    .map((bucket) => {
      if (!Array.isArray(bucket.records) || !bucket.records.length) {
        return null;
      }
      const metricKeys = new Set();
      let latestTime = null;
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
        if (latestTime == null || time > latestTime) {
          latestTime = time;
        }
      }
      if (!metricKeys.size) {
        return null;
      }
      const meshKey = bucket.meshId || resolveTelemetryMeshKey(bucket.rawMeshId);
      const rawMeshId = bucket.rawMeshId || meshKey || 'unknown';
      const nodeInfo = bucket.node || {};
      const displayNode = {
        ...nodeInfo,
        meshId: nodeInfo.meshId || meshKey || rawMeshId,
        meshIdOriginal: nodeInfo.meshIdOriginal || rawMeshId,
        longName: nodeInfo.longName || nodeInfo.label || rawMeshId,
        label: nodeInfo.label || nodeInfo.longName || rawMeshId
      };
      const label = formatNodeDisplay(displayNode);
      return {
        meshId: meshKey,
        rawMeshId,
        label,
        count: metricKeys.size,
        latestMs: Number.isFinite(latestTime) ? latestTime : null
      };
    })
    .filter(Boolean);

  telemetryNodeLookup.clear();
  telemetryNodeDisplayByMesh.clear();
  telemetryNodeOptions = [];

  if (!nodes.length) {
    hideTelemetryDropdown();
    if (!searchActive) {
      telemetrySelectedMeshId = null;
      telemetryLastExplicitMeshId = null;
      updateTelemetryNodeInputDisplay();
    }
    return;
  }

  nodes.sort((a, b) => {
    const aTime = Number.isFinite(a.latestMs) ? a.latestMs : -Infinity;
    const bTime = Number.isFinite(b.latestMs) ? b.latestMs : -Infinity;
    if (bTime !== aTime) {
      return bTime - aTime;
    }
    if (b.count !== a.count) {
      return b.count - a.count;
    }
    return a.label.localeCompare(b.label, 'zh-Hant', { sensitivity: 'base' });
  });

  for (const item of nodes) {
    const meshIdNormalized = normalizeMeshId(item.meshId) || normalizeMeshId(item.rawMeshId) || item.meshId || item.rawMeshId || '';
    const display = formatTelemetryNodeDisplay(item.label, meshIdNormalized, item.rawMeshId);

    if (meshIdNormalized) {
      telemetryNodeDisplayByMesh.set(meshIdNormalized, display);
      telemetryNodeLookup.set(meshIdNormalized.toLowerCase(), meshIdNormalized);
    }
    if (item.rawMeshId) {
      telemetryNodeLookup.set(String(item.rawMeshId).toLowerCase(), meshIdNormalized);
    }
    if (item.label) {
      telemetryNodeLookup.set(item.label.toLowerCase(), meshIdNormalized);
    }
    telemetryNodeLookup.set(display.toLowerCase(), meshIdNormalized);

    const searchKeys = new Set();
    if (display) {
      searchKeys.add(display.toLowerCase());
    }
    if (meshIdNormalized) {
      searchKeys.add(meshIdNormalized.toLowerCase());
    }
    if (item.rawMeshId) {
      searchKeys.add(String(item.rawMeshId).toLowerCase());
    }
    if (item.label) {
      searchKeys.add(item.label.toLowerCase());
    }
    telemetryNodeOptions.push({
      meshId: meshIdNormalized,
      display,
      searchKeys: Array.from(searchKeys).filter(Boolean),
      latestMs: item.latestMs ?? null
    });
  }

  if (searchActive) {
    updateTelemetryNodeInputDisplay();
    renderTelemetryDropdown();
    return;
  }

  let nextSelection = previousSelection;
  if (preferredMeshId && nodes.some((node) => node.meshId === preferredMeshId)) {
    nextSelection = preferredMeshId;
  }
  if (nextSelection && !nodes.some((node) => node.meshId === nextSelection)) {
    nextSelection = null;
  }
  if (!nextSelection && nodes.length) {
    nextSelection = nodes[0].meshId;
  }
  telemetrySelectedMeshId = nextSelection;
  if (!searchActive) {
    telemetryLastExplicitMeshId = nextSelection;
  }
  telemetrySearchRaw = '';
  telemetrySearchTerm = '';
  updateTelemetryNodeInputDisplay();
  renderTelemetryDropdown();
}

function formatTelemetryNodeDisplay(label, meshId, rawMeshId) {
  const normalized = normalizeMeshId(meshId) || meshId || rawMeshId || '';
  const cleanLabel = label && label !== normalized ? label : null;
  if (cleanLabel) {
    if (cleanLabel.includes(normalized)) {
      return cleanLabel;
    }
    return `${cleanLabel} (${normalized})`;
  }
  return normalized || '未知節點';
}
function getFirstTelemetryMeshId() {
  const iterator = telemetryStore.keys();
  const first = iterator.next();
  return first && !first.done ? first.value : null;
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

function filterTelemetryBySearch(records) {
  if (!telemetrySearchTerm) {
    return records;
  }
  const term = telemetrySearchTerm.toLowerCase();
  return records.filter((record) => matchesTelemetrySearch(record, term));
}

function matchesTelemetrySearch(record, term) {
  if (!record) return false;
  const haystack = [];
  const node = record.node || {};
  haystack.push(node.label, node.longName, node.shortName, node.hwModelLabel, node.roleLabel);
  haystack.push(record.meshId, node.meshId, node.meshIdOriginal, node.meshIdNormalized);
  if (record.detail) haystack.push(record.detail);
  if (record.channel != null) haystack.push(`ch ${record.channel}`);
  if (Number.isFinite(record.snr)) haystack.push(`snr ${record.snr}`);
  if (Number.isFinite(record.rssi)) haystack.push(`rssi ${record.rssi}`);
  const summary = formatTelemetrySummary(record);
  if (summary && summary !== '—') {
    haystack.push(summary);
  }
  const metrics = record.telemetry?.metrics;
  if (metrics) {
    for (const [key, value] of flattenTelemetryMetrics(metrics)) {
      if (key) {
        haystack.push(key);
      }
      if (value != null) {
        haystack.push(String(value));
      }
    }
  }
  return haystack.some((value) => {
    if (value == null) return false;
    return String(value).toLowerCase().includes(term);
  });
}

function renderTelemetryView() {
  if (!telemetryTableBody || !telemetryEmptyState) {
    return;
  }
  if (!telemetrySelectedMeshId && telemetryStore.size && !telemetrySearchRaw) {
    const firstKey = telemetryStore.keys().next().value;
    telemetrySelectedMeshId = firstKey || null;
    telemetryLastExplicitMeshId = telemetrySelectedMeshId;
    updateTelemetryNodeInputDisplay();
    renderTelemetryDropdown();
  }
  const baseRecords = getTelemetryRecordsForSelection();
  const filteredRecords = applyTelemetryFilters(baseRecords);
  const searchFilteredRecords = filterTelemetryBySearch(filteredRecords);
  const hasData = searchFilteredRecords.length > 0;
  const hasBase = filteredRecords.length > 0;
  telemetryEmptyState.classList.toggle('hidden', hasData);
  telemetryChartsContainer?.classList.toggle('hidden', !hasData);
  telemetryTableWrapper?.classList.toggle('hidden', !hasData);
  if (!hasData) {
    if (!hasBase) {
      telemetryEmptyState.textContent = '尚未收到遙測資料。';
    } else if (telemetrySearchTerm) {
      telemetryEmptyState.textContent = '沒有符合搜尋的遙測資料。';
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
  renderTelemetryCharts(searchFilteredRecords);
  renderTelemetryTable(searchFilteredRecords);
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
    destroyAllTelemetryCharts();
    telemetryChartsContainer.classList.add('hidden');
    telemetryChartsContainer.innerHTML = '';
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
    metricsToRender =
      telemetryChartMetric && seriesMap.has(telemetryChartMetric)
        ? [telemetryChartMetric]
        : [];
  } else {
    metricsToRender = metricsList;
  }

  if (!metricsToRender.length) {
    destroyAllTelemetryCharts();
    telemetryChartsContainer.classList.add('hidden');
    telemetryChartsContainer.innerHTML = '';
    return;
  }

  const activeMetrics = new Set();

  for (const metricName of metricsToRender) {
    const series = seriesMap.get(metricName);
    if (!Array.isArray(series) || !series.length) {
      continue;
    }
    activeMetrics.add(metricName);
    const def = TELEMETRY_METRIC_DEFINITIONS[metricName] || { label: metricName };
    const latestPoint = series[series.length - 1] || null;
    const latestValue = latestPoint ? latestPoint.value : null;
    const datasetPoints = series.map((point) => ({ x: point.time, y: point.value }));

    let view = telemetryCharts.get(metricName);
    if (!view) {
      const card = document.createElement('article');
      card.className = 'telemetry-chart-card';
      const header = document.createElement('div');
      header.className = 'telemetry-chart-header';
      const title = document.createElement('span');
      title.className = 'telemetry-chart-title';
      title.textContent = def.label || metricName;
      const latest = document.createElement('span');
      latest.className = 'telemetry-chart-latest';
      latest.textContent = formatTelemetryValue(metricName, latestValue) || '—';
      header.append(title, latest);
      const canvasWrap = document.createElement('div');
      canvasWrap.className = 'telemetry-chart-canvas-wrap';
      const canvas = document.createElement('canvas');
      canvasWrap.appendChild(canvas);
      card.append(header, canvasWrap);
      const ctx = canvas.getContext('2d');
      const chart = new window.Chart(ctx, buildTelemetryChartConfig(metricName, def, series));
      view = {
        chart,
        card,
        titleEl: title,
        latestEl: latest
      };
      telemetryCharts.set(metricName, view);
    } else if (view.titleEl) {
      view.titleEl.textContent = def.label || metricName;
    }

    const chart = view.chart;
    const dataset = chart.data?.datasets?.[0];
    if (dataset) {
      dataset.label = def.label || metricName;
      dataset.data = datasetPoints;
    } else {
      const fallback = buildTelemetryChartConfig(metricName, def, series).data.datasets[0];
      chart.data.datasets = [{ ...fallback }];
    }
    chart.update('none');

    if (view.latestEl) {
      view.latestEl.textContent = formatTelemetryValue(metricName, latestValue) || '—';
    }

    telemetryChartsContainer.appendChild(view.card);
  }

  for (const [metricName, view] of Array.from(telemetryCharts.entries())) {
    if (!activeMetrics.has(metricName)) {
      try {
        view.chart?.destroy();
      } catch {
        /* ignore */
      }
      if (view.card?.parentNode) {
        view.card.parentNode.removeChild(view.card);
      }
      telemetryCharts.delete(metricName);
    }
  }

  if (!telemetryCharts.size) {
    telemetryChartsContainer.classList.add('hidden');
  } else {
    telemetryChartsContainer.classList.remove('hidden');
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
  for (const [, view] of telemetryCharts.entries()) {
    try {
      view.chart?.destroy();
    } catch {
      /* ignore */
    }
    if (view.card?.parentNode) {
      view.card.parentNode.removeChild(view.card);
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
  refreshTelemetrySelectors(telemetrySelectedMeshId);
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

async function initializeNodeRegistry() {
  if (!window.meshtastic.getNodeSnapshot) {
    return;
  }
  try {
    const nodes = await window.meshtastic.getNodeSnapshot();
    applyNodeRegistrySnapshot(nodes);
  } catch (err) {
    console.warn('載入節點資訊失敗:', err);
  }
}

function activatePage(targetId) {
  const pages = [
    { id: 'monitor-page', element: monitorPage },
    { id: 'messages-page', element: messagesPage },
    { id: 'telemetry-page', element: telemetryPage },
    { id: 'flow-page', element: flowPage },
    { id: 'nodes-page', element: nodesPage },
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
  } else if (targetId === 'nodes-page') {
    renderNodeDatabase();
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
  setNodeDistanceReferenceFromProvision(provision);
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
    renderNodeDatabase();
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

  renderNodeDatabase();
}

function formatAprsSsid(ssid) {
  if (ssid === null || ssid === undefined) return '';
  if (ssid === 0) return '';
  if (ssid < 0) return `${ssid}`;
  return `-${ssid}`;
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
  const weeks = Math.floor(days / 7);
  if (weeks < 5) {
    return `${weeks} 週前`;
  }
  const months = Math.floor(days / 30);
  if (months < 12) {
    return `${months} 個月前`;
  }
  const years = Math.floor(days / 365);
  if (years >= 1) {
    return `${years} 年前`;
  }
  return date.toLocaleString();
}
