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
  const callmeshProvisionDetails = document.getElementById('callmesh-provision-details');
  const summaryTable = document.getElementById('summary-table');
  const logList = document.getElementById('log-list');
  const navButtons = document.querySelectorAll('.nav-btn');
  const pages = document.querySelectorAll('.page');
  const messagesPage = document.getElementById('messages-page');
  const channelNav = document.getElementById('channel-nav');
  const channelTitleLabel = document.getElementById('channel-title');
  const channelNoteLabel = document.getElementById('channel-note');
  const channelMessageList = document.getElementById('channel-message-list');
  const flowPage = document.getElementById('flow-page');
  const flowList = document.getElementById('flow-list');
  const flowEmptyState = document.getElementById('flow-empty-state');
  const flowSearchInput = document.getElementById('flow-search');
  const flowFilterStateSelect = document.getElementById('flow-filter-state');
  const telemetryPage = document.getElementById('telemetry-page');
  const telemetryNodeInput = document.getElementById('telemetry-node-input');
  const telemetryNodeDropdown = document.getElementById('telemetry-node-dropdown');
  const telemetryRangeSelect = document.getElementById('telemetry-range-select');
  const telemetryRangeCustomWrap = document.getElementById('telemetry-range-custom');
  const telemetryRangeStartInput = document.getElementById('telemetry-range-start');
  const telemetryRangeEndInput = document.getElementById('telemetry-range-end');
  const telemetryChartModeSelect = document.getElementById('telemetry-chart-mode');
  const telemetryChartMetricSelect = document.getElementById('telemetry-chart-metric');
  const telemetryUpdatedAtLabel = document.getElementById('telemetry-updated-at');
  const telemetryEmptyState = document.getElementById('telemetry-empty-state');
  const telemetryChartsContainer = document.getElementById('telemetry-charts');
  const telemetryTableWrapper = document.getElementById('telemetry-table-wrapper');
  const telemetryTableBody = document.getElementById('telemetry-table-body');
  const telemetryStatsRecords = document.getElementById('telemetry-stats-records');
  const telemetryStatsNodes = document.getElementById('telemetry-stats-nodes');
  const telemetryStatsDisk = document.getElementById('telemetry-stats-disk');
  const telemetryDownloadBtn = document.getElementById('telemetry-download-btn');
  const nodesTableWrapper = document.getElementById('nodes-table-wrapper');
  const nodesTableBody = document.getElementById('nodes-table-body');
  const nodesEmptyState = document.getElementById('nodes-empty-state');
  const nodesTotalCountLabel = document.getElementById('nodes-total-count');
  const nodesOnlineCountLabel = document.getElementById('nodes-online-count');
  const nodesOnlineTotalLabel = document.getElementById('nodes-online-total');
  const nodesSearchInput = document.getElementById('nodes-search');
  const nodesStatusLabel = document.getElementById('nodes-status');
  const relayHintModal = document.getElementById('relay-hint-modal');
  const relayHintReasonEl = document.getElementById('relay-hint-reason');
  const relayHintNodeEl = document.getElementById('relay-hint-node');
  const relayHintMeshEl = document.getElementById('relay-hint-mesh');
  const relayHintSubtitleEl = document.getElementById('relay-hint-subtitle');
  const relayHintCloseBtn = document.getElementById('relay-hint-close');
  const relayHintOkBtn = document.getElementById('relay-hint-ok');

  const summaryRows = [];
  const flowRowMap = new Map();
  const aprsHighlightedFlows = new Set();
  const mappingMeshIds = new Set();
  const mappingByMeshId = new Map();
  const flowAprsCallsigns = new Map();
  const flowEntries = [];
  const flowEntryIndex = new Map();
  const pendingFlowSummaries = new Map();
  const pendingAprsUplinks = new Map();
  const MAX_PENDING_FLOW_SUMMARIES_PER_MESH = 25;
  const MAX_PENDING_FLOW_SUMMARIES_TOTAL = 400;
  const PENDING_FLOW_SUMMARY_TTL_MS = 15 * 60 * 1000;
  const pendingFlowSummaryQueue = [];
  let pendingFlowSummaryCount = 0;
  const MAX_PENDING_APRS_RECORDS = 200;
  const PENDING_APRS_TTL_MS = 15 * 60 * 1000;
  const pendingAprsQueue = [];
  let flowFilterState = 'all';
  let flowSearchTerm = '';
  const FLOW_MAX_ENTRIES = 1000;
  const RELAY_GUESS_EXPLANATION =
    '最後轉發節點由 SNR/RSSI 推測（韌體僅提供節點尾碼），結果可能不完全準確。';

  let currentSelfMeshId = null;
  let selfProvisionCoords = null;
  const MAX_SUMMARY_ROWS = 200;
  const logEntries = [];
  const MAX_LOG_ENTRIES = 200;
  const telemetryStore = new Map();
  const telemetryRecordIds = new Set();
  const telemetryRecordOrder = [];
  const telemetryCharts = new Map();
  let telemetrySelectedMeshId = null;
  const telemetryNodeLookup = new Map();
  const telemetryNodeDisplayByMesh = new Map();
  let telemetryNodeOptions = [];
  let telemetrySearchRaw = '';
  let telemetrySearchTerm = '';
  let telemetryLastExplicitMeshId = null;
  let telemetryNodeInputHoldEmpty = false;
  let telemetryDropdownVisible = false;
  let telemetryDropdownInteracting = false;
  let nodesSearchTerm = '';
  let nodesStatusResetTimer = null;
  const TELEMETRY_RANGE_OPTIONS = ['hour1', 'hour3', 'hour6', 'hour12', 'day', 'week', 'month', 'year', 'custom'];
  let telemetryRangeMode = 'day';
  let telemetryCustomRange = { startMs: null, endMs: null };
  let telemetryChartMode = 'all';
  let telemetryChartMetric = null;
  let telemetryUpdatedAt = null;
  let telemetryLoading = false;
  let telemetryLoadingMeshId = null;
  let telemetryFetchController = null;
  let telemetryLastFetchKey = null;
  const telemetryNodeInputDefaultPlaceholder =
    telemetryNodeInput?.getAttribute('placeholder') || '輸入節點 Mesh ID 或搜尋關鍵字';
  const nodeRegistry = new Map();
  const MESH_ID_PATTERN = /^![0-9a-f]{8}$/i;

  function isIgnoredMeshId(meshId) {
    const normalized = normalizeMeshId(meshId);
    if (!normalized) return false;
    return normalized.toLowerCase().startsWith('!abcd');
  }
  let nodeSnapshotLoaded = false;
  const TELEMETRY_TABLE_LIMIT = 200;
  const TELEMETRY_CHART_LIMIT = 200;
  const TELEMETRY_MAX_LOCAL_RECORDS = 500;
  let telemetryMaxTotalRecords = 20000;
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
  const CHANNEL_CONFIG = [
    { id: 0, code: 'CH0', name: 'Primary Channel', note: '日常主要通訊頻道' },
    { id: 1, code: 'CH1', name: 'Mesh TW', note: '跨節點廣播與共通交換' },
    { id: 2, code: 'CH2', name: 'Signal Test', note: '訊號測試、天線調校專用' },
    { id: 3, code: 'CH3', name: 'Emergency', note: '緊急狀況 / 救援聯絡' }
  ];
  const channelConfigs = CHANNEL_CONFIG.map((item) => ({ ...item }));
  const channelConfigMap = new Map(channelConfigs.map((item) => [item.id, item]));
  const channelMessageStore = new Map();
  const channelNavButtons = new Map();
  const CHANNEL_MESSAGE_LIMIT = 200;
  let selectedChannelId = channelConfigs[0]?.id ?? null;
  let messagesNavNeedsRender = true;
  for (const channel of channelConfigs) {
    channelMessageStore.set(channel.id, []);
  }

  const METERS_PER_FOOT = 0.3048;
  const NODE_ONLINE_WINDOW_MS = 60 * 60 * 1000;
  const STORAGE_KEYS = {
    callmeshProvisionOpen: 'tmag:web:callmeshProvision:open',
    telemetryRangeMode: 'tmag:web:telemetry:range-mode'
  };

  function isValidTelemetryRangeMode(mode) {
    return typeof mode === 'string' && TELEMETRY_RANGE_OPTIONS.includes(mode);
  }

  function safeStorageGet(key) {
    try {
      return window.localStorage?.getItem(key);
    } catch {
      return null;
    }
  }

  const storedTelemetryRangeMode = safeStorageGet(STORAGE_KEYS.telemetryRangeMode);
  if (isValidTelemetryRangeMode(storedTelemetryRangeMode)) {
    telemetryRangeMode = storedTelemetryRangeMode;
  }

  function safeStorageSet(key, value) {
    try {
      window.localStorage?.setItem(key, value);
    } catch {
      // ignore storage errors (例如隱私模式)
    }
  }

  if (callmeshProvisionDetails) {
    const stored = safeStorageGet(STORAGE_KEYS.callmeshProvisionOpen);
    if (stored === '0') {
      callmeshProvisionDetails.open = false;
    } else if (stored === '1') {
      callmeshProvisionDetails.open = true;
    }
    callmeshProvisionDetails.addEventListener('toggle', () => {
      safeStorageSet(STORAGE_KEYS.callmeshProvisionOpen, callmeshProvisionDetails.open ? '1' : '0');
    });
  }

  relayHintCloseBtn?.addEventListener('click', () => {
    closeRelayHintDialog();
  });

  relayHintOkBtn?.addEventListener('click', () => {
    closeRelayHintDialog();
  });

  relayHintModal?.addEventListener('click', (event) => {
    if (event.target === relayHintModal) {
      closeRelayHintDialog();
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && relayHintModal && !relayHintModal.classList.contains('hidden')) {
      closeRelayHintDialog();
    }
  });

  function escapeHtml(input) {
    return String(input)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function escapeCsvValue(value) {
    if (value === null || value === undefined) return '';
    const str = String(value);
    if (str.includes('"') || str.includes(',') || str.includes('\n') || str.includes('\r')) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  }

  function formatNumericForCsv(value, digits = null) {
    if (!Number.isFinite(value)) return '';
    if (digits == null) {
      return String(value);
    }
    const fixed = value.toFixed(digits);
    return fixed.replace(/0+$/, '').replace(/\.$/, '') || '0';
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

  function formatBytes(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric < 0) {
      return '—';
    }
    if (numeric === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const index = Math.min(Math.floor(Math.log10(numeric) / Math.log10(1024)), units.length - 1);
    const scaled = numeric / 1024 ** index;
    const formatted = scaled >= 100 ? scaled.toFixed(0) : scaled >= 10 ? scaled.toFixed(1) : scaled.toFixed(2);
    return `${formatted} ${units[index]}`;
  }

  function normalizeEnumLabel(value) {
    if (value === undefined || value === null) {
      return null;
    }
    return String(value).replace(/_/g, ' ').trim();
  }

  function mergeNodeMetadata(...sources) {
    const result = {
      meshId: null,
      meshIdOriginal: null,
      meshIdNormalized: null,
      shortName: null,
      longName: null,
      hwModel: null,
      hwModelLabel: null,
      role: null,
      roleLabel: null,
      label: null,
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
        if (item.meshIdNormalized) {
          result.meshIdNormalized = item.meshIdNormalized;
          hasValue = true;
        }
        if (item.meshIdOriginal) {
          result.meshIdOriginal = item.meshIdOriginal;
          hasValue = true;
        }
        if (item.meshId) {
          result.meshId = item.meshId;
          hasValue = true;
        }
        if (item.shortName != null) {
          result.shortName = item.shortName;
          hasValue = true;
        }
        if (item.longName != null) {
          result.longName = item.longName;
          hasValue = true;
        }
        if (item.hwModel != null) {
          result.hwModel = item.hwModel;
          hasValue = true;
        }
        if (item.hwModelLabel != null) {
          result.hwModelLabel = item.hwModelLabel;
          hasValue = true;
        }
        if (item.role != null) {
          result.role = item.role;
          hasValue = true;
        }
        if (item.roleLabel != null) {
          result.roleLabel = item.roleLabel;
          hasValue = true;
        }
        if (item.label) {
          result.label = item.label;
          hasValue = true;
        }
        if (item.lastSeenAt != null) {
          const numeric = Number(item.lastSeenAt);
          if (Number.isFinite(numeric)) {
            result.lastSeenAt = numeric;
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
      const meshLabel = result.meshIdOriginal || result.meshId || result.meshIdNormalized || null;
      result.label = name && meshLabel ? `${name} (${meshLabel})` : name || meshLabel || null;
    }
    return result;
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

  function formatNodeDisplayLabel(node) {
    if (!node || typeof node !== 'object') return '';
    const name =
      sanitizeNodeName(node.longName) ||
      sanitizeNodeName(node.shortName) ||
      sanitizeNodeName(node.label);
    const meshId = node.meshId || node.meshIdOriginal || node.meshIdNormalized || '';
    if (!meshId) {
      return name || '';
    }
    if (!name) {
      return meshId;
    }
    const meshDisplay = meshId.toLowerCase();
    return name.toLowerCase().includes(meshDisplay) ? name : `${name} (${meshId})`;
  }

  function ensureChannelConfig(channelId) {
    const numeric = Number(channelId);
    if (!Number.isFinite(numeric) || numeric < 0) {
      return null;
    }
    if (channelConfigMap.has(numeric)) {
      return channelConfigMap.get(numeric);
    }
    const fallback = {
      id: numeric,
      code: `CH${numeric}`,
      name: `Channel ${numeric}`,
      note: ''
    };
    channelConfigs.push(fallback);
    channelConfigs.sort((a, b) => a.id - b.id);
    channelConfigMap.set(numeric, fallback);
    messagesNavNeedsRender = true;
    return fallback;
  }

  function getChannelConfig(channelId) {
    const config = ensureChannelConfig(channelId);
    return config || null;
  }

  function ensureChannelStore(channelId) {
    const numeric = Number(channelId);
    if (!Number.isFinite(numeric) || numeric < 0) {
      return [];
    }
    if (!channelMessageStore.has(numeric)) {
      channelMessageStore.set(numeric, []);
    }
    const store = channelMessageStore.get(numeric);
    return Array.isArray(store) ? store : [];
  }

  function isMessagesPageActive() {
    return messagesPage?.classList.contains('active');
  }

  function appendMessageMeta(metaEl, text) {
    if (!metaEl || !text) return;
    const trimmed = String(text).trim();
    if (!trimmed) return;
    if (metaEl.children.length) {
      const separator = document.createElement('span');
      separator.className = 'meta-separator';
      separator.textContent = '•';
      metaEl.appendChild(separator);
    }
    const span = document.createElement('span');
    span.textContent = trimmed;
    metaEl.appendChild(span);
  }

  function updateChannelNavMeta(channelId, { unread = false } = {}) {
    const navItem = channelNavButtons.get(channelId);
    if (!navItem) return;
    const store = ensureChannelStore(channelId);
    const latest = store[0];
    if (latest) {
      const timeLabel = latest.timestampLabel || '—';
      const fromLabel = resolveStoredMessageFromLabel(latest);
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

  function resolveMessageSource(summary) {
    const fallback = { label: '未知節點', meshId: null };
    if (!summary || typeof summary !== 'object') {
      return fallback;
    }
    const node = summary.from || {};
    const directName =
      sanitizeNodeName(node.longName) ||
      sanitizeNodeName(node.shortName) ||
      sanitizeNodeName(node.label);
    const meshIdRaw =
      node.meshIdNormalized ||
      node.meshId ||
      node.meshIdOriginal ||
      summary.fromMeshIdNormalized ||
      summary.fromMeshId ||
      summary.fromMeshIdOriginal ||
      null;
    const normalized = normalizeMeshId(meshIdRaw);
    if (normalized) {
      fallback.meshId = normalized;
    }
    if (directName) {
      return {
        label: directName,
        meshId: normalized || null
      };
    }
    if (normalized) {
      const registryNode = nodeRegistry.get(normalized);
      if (registryNode) {
        const registryName =
          sanitizeNodeName(registryNode.longName) ||
          sanitizeNodeName(registryNode.shortName) ||
          sanitizeNodeName(registryNode.label);
        if (registryName) {
          return {
            label: registryName,
            meshId: normalized
          };
        }
      }
      return {
        label: normalized,
        meshId: normalized
      };
    }
    return fallback;
  }

  function resolveStoredMessageFromLabel(entry) {
    if (!entry || typeof entry !== 'object') {
      return '未知節點';
    }
    const stored = sanitizeNodeName(entry.from);
    if (stored && !MESH_ID_PATTERN.test(stored)) {
      return stored;
    }
    const normalized = entry.fromMeshId || normalizeMeshId(stored);
    if (normalized) {
      const registryNode = nodeRegistry.get(normalized);
      if (registryNode) {
        const registryName =
          sanitizeNodeName(registryNode.longName) ||
          sanitizeNodeName(registryNode.shortName) ||
          sanitizeNodeName(registryNode.label);
        if (registryName) {
          return registryName;
        }
      }
      return normalized;
    }
    return stored || '未知節點';
  }

  function formatMessageDistanceMeta(entry) {
    if (!entry || !entry.fromMeshId) {
      return '';
    }
    const normalized = normalizeMeshId(entry.fromMeshId);
    if (!normalized) {
      return '';
    }
    const node = nodeRegistry.get(normalized);
    if (!node) {
      return '';
    }
    const distanceText = formatNodeDistanceValue(node);
    const lastSeenTs = getNodeLastSeenTimestamp(node);
    let recencyText = '';
    if (lastSeenTs != null && Number.isFinite(lastSeenTs)) {
      recencyText = formatRelativeTime(new Date(lastSeenTs).toISOString());
    }
    if (distanceText && recencyText) {
      return `${distanceText} (${recencyText})`;
    }
    return distanceText || recencyText || '';
  }

  function renderChannelMessages(channelId) {
    if (channelId !== selectedChannelId) {
      return;
    }
    if (!channelMessageList) {
      return;
    }
    const entries = ensureChannelStore(channelId);
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
      appendMessageMeta(meta, `來自：${resolveStoredMessageFromLabel(entry)}`);
      appendMessageMeta(meta, entry.hops);
      appendMessageMeta(meta, entry.relay);
      appendMessageMeta(meta, formatMessageDistanceMeta(entry));
      appendMessageMeta(meta, `時間：${entry.timestampLabel}`);

      wrapper.append(text, meta);
      channelMessageList.appendChild(wrapper);
    });
  }

  function selectChannel(channelId, { fromNavRender = false } = {}) {
    const numeric = Number(channelId);
    if (!Number.isFinite(numeric) || numeric < 0) {
      return;
    }
    const config = getChannelConfig(numeric);
    ensureChannelStore(numeric);
    selectedChannelId = numeric;
    channelNavButtons.forEach(({ button }) => {
      const isActive = Number(button.dataset.channelId) === numeric;
      button.classList.toggle('active', isActive);
      if (isActive) {
        button.classList.remove('channel-nav-btn--unread');
      }
    });
    if (channelTitleLabel && config) {
      channelTitleLabel.textContent = `${config.name} (${config.code})`;
    }
    if (channelNoteLabel) {
      channelNoteLabel.textContent = config?.note || '';
    }
    updateChannelNavMeta(numeric, { unread: false });
    renderChannelMessages(numeric);
    if (!fromNavRender && isMessagesPageActive()) {
      const store = ensureChannelStore(numeric);
      if (store.length) {
        const navItem = channelNavButtons.get(numeric);
        navItem?.button.classList.remove('channel-nav-btn--unread');
      }
    }
  }

  function renderChannelNav({ force = false } = {}) {
    if (!channelNav) return;
    if (force) {
      messagesNavNeedsRender = true;
    }
    if (!messagesNavNeedsRender && channelNavButtons.size && channelNav.children.length) {
      return;
    }
    messagesNavNeedsRender = false;
    const previousSelected = selectedChannelId;
    channelNavButtons.clear();
    channelNav.innerHTML = '';

    const sorted = channelConfigs.slice().sort((a, b) => a.id - b.id);
    sorted.forEach((channel) => {
      ensureChannelStore(channel.id);
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
      updateChannelNavMeta(channel.id, { unread: false });
    });

    if (!sorted.length) {
      selectedChannelId = null;
      return;
    }
    const targetId = sorted.some((item) => item.id === previousSelected) ? previousSelected : sorted[0].id;
    selectChannel(targetId, { fromNavRender: true });
  }

  function initializeChannelMessages() {
    renderChannelNav({ force: true });
    channelNavButtons.forEach((_, channelId) => updateChannelNavMeta(channelId, { unread: false }));
  }

  function isTextSummary(summary) {
    if (!summary || typeof summary !== 'object') return false;
    const type = typeof summary.type === 'string' ? summary.type.trim().toLowerCase() : '';
    return type === 'text';
  }

  function formatMessageText(summary) {
    const detail = typeof summary.detail === 'string' ? summary.detail.trim() : '';
    const extra = Array.isArray(summary.extraLines)
      ? summary.extraLines
          .map((line) => (typeof line === 'string' ? line.trim() : ''))
          .filter(Boolean)
          .join('\n')
      : '';
    return detail || extra || '（無內容）';
  }

  function resolveMessageTimestamp(summary, timestampMs) {
    if (typeof summary.timestampLabel === 'string' && summary.timestampLabel.trim()) {
      return summary.timestampLabel.trim();
    }
    return formatFlowTimestamp(timestampMs);
  }

  function buildMessageRelayLabel(summary) {
    let relayLabel = formatRelay({ ...summary });
    if (!relayLabel || relayLabel === '未知' || relayLabel === '?') {
      relayLabel = formatNodeDisplayLabel(summary.relay);
    }
    if (!relayLabel || relayLabel === 'unknown') {
      relayLabel = '未知';
    }
    return relayLabel === '直收' ? '最後一跳：直收' : `最後一跳：${relayLabel}`;
  }

  function buildMessageHopLabel(summary, hopInfo) {
    if (hopInfo.usedHops === 0) {
      return '跳數：0 (直收)';
    }
    if (hopInfo.usedHops != null && hopInfo.totalHops != null) {
      return `跳數：${hopInfo.usedHops}/${hopInfo.totalHops}`;
    }
    if (hopInfo.usedHops != null) {
      return `跳數：${hopInfo.usedHops}`;
    }
    if (hopInfo.hopsLabel) {
      return `跳數：${hopInfo.hopsLabel}`;
    }
    return '跳數：未知';
  }

  function recordChannelMessage(summary, { markUnread = true, deferRender = false } = {}) {
    if (!isTextSummary(summary)) {
      return;
    }
    const channelId = Number(summary.channel);
    if (!Number.isFinite(channelId) || channelId < 0) {
      return;
    }
    ensureChannelConfig(channelId);
    const store = ensureChannelStore(channelId);

    const text = formatMessageText(summary);
    const { label: fromLabel, meshId: fromMeshId } = resolveMessageSource(summary);
    const timestampMs = Number.isFinite(Number(summary.timestampMs)) ? Number(summary.timestampMs) : Date.now();
    const timestampLabel = resolveMessageTimestamp(summary, timestampMs);
    const flowIdRaw = summary.flowId || `${channelId}-${timestampMs}-${text}`;
    const flowId = String(flowIdRaw);
    const hopStats = extractHopInfo(summary);
    const hopSummary = buildMessageHopLabel(summary, hopStats);
    const relaySummary = buildMessageRelayLabel(summary);

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
      fromMeshId: fromMeshId || null,
      hops: hopSummary,
      relay: relaySummary
    });
    if (store.length > CHANNEL_MESSAGE_LIMIT) {
      store.length = CHANNEL_MESSAGE_LIMIT;
    }

    const isSelected = channelId === selectedChannelId;
    updateChannelNavMeta(channelId, { unread: markUnread && !isSelected && store.length > 0 });
    if (isSelected && !deferRender) {
      renderChannelMessages(channelId);
    }
  }

  function clearChannelMessages() {
    channelMessageStore.forEach((store, channelId) => {
      if (Array.isArray(store)) {
        store.length = 0;
      }
      updateChannelNavMeta(channelId, { unread: false });
    });
    if (selectedChannelId != null) {
      renderChannelMessages(selectedChannelId);
    }
  }

  function applyMessageSnapshot(payload) {
    const channels = payload?.channels || {};
    clearChannelMessages();
    const channelIds = Object.keys(channels);
    if (channelIds.length) {
      channelIds.forEach((key) => {
        const channelId = Number(key);
        if (!Number.isFinite(channelId) || channelId < 0) {
          return;
        }
        ensureChannelConfig(channelId);
        const store = ensureChannelStore(channelId);
        store.length = 0;
        const list = Array.isArray(channels[key]) ? channels[key] : [];
        for (let i = list.length - 1; i >= 0; i -= 1) {
          const entry = list[i];
          if (!entry) continue;
          const hydrated = {
            type: entry.type || 'Text',
            channel: Number.isFinite(entry.channel) ? entry.channel : channelId,
            detail: entry.detail,
            extraLines: Array.isArray(entry.extraLines) ? entry.extraLines : [],
            from: entry.from,
            fromMeshId:
              entry.from?.meshId ||
              entry.from?.meshIdNormalized ||
              entry.from?.meshIdOriginal ||
              entry.fromMeshId ||
              entry.fromMeshIdNormalized ||
              null,
            relay: entry.relay,
            relayMeshId: entry.relayMeshId,
            relayMeshIdNormalized: entry.relayMeshIdNormalized,
            hops: entry.hops,
            timestampMs: entry.timestampMs,
            timestampLabel: entry.timestampLabel,
            flowId: entry.flowId
          };
          hydrateSummaryNodes(hydrated);
          recordChannelMessage(hydrated, { markUnread: false, deferRender: true });
        }
      });
    }
    renderChannelNav();
    channelNavButtons.forEach((_, channelId) => updateChannelNavMeta(channelId, { unread: false }));
    if (selectedChannelId != null) {
      renderChannelMessages(selectedChannelId);
    }
  }

  function handleMessageAppend(payload) {
    if (!payload) return;
    const entry = payload.entry || payload;
    const channelId = Number(payload.channelId ?? entry?.channel);
    if (!Number.isFinite(channelId) || channelId < 0) {
      return;
    }
    const hydrated = {
      type: entry.type || 'Text',
      channel: channelId,
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
    recordChannelMessage(hydrated, { markUnread: true });
    renderChannelNav();
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

  function formatNodeLastSeen(value) {
    if (value == null) {
      return { display: '—', tooltip: '', timestamp: null };
    }
    const timestamp = getNodeLastSeenTimestamp({ lastSeenAt: value });
    if (!Number.isFinite(timestamp)) {
      return { display: '—', tooltip: '', timestamp: null };
    }
    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) {
      return { display: '—', tooltip: '', timestamp: null };
    }
    const display = formatRelativeTime(date.toISOString());
    const tooltip = date.toLocaleString();
    return { display, tooltip, timestamp };
  }

  function formatNodeCoordinateValue(entry) {
    if (!entry) return '';
    const latRaw = entry.latitude;
    const lonRaw = entry.longitude;
    if (latRaw == null || lonRaw == null) {
      return '';
    }
    const lat = Number(latRaw);
    const lon = Number(lonRaw);
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
    const parts = [`${formatComponent(lat)}`, `${formatComponent(lon)}`];
    const altitude = Number(entry.altitude);
    if (Number.isFinite(altitude)) {
      parts.push(`${Math.round(altitude)}m`);
    }
    return parts.join(', ');
  }

  function formatNodeDistanceValue(entry) {
    if (
      !selfProvisionCoords ||
      !Number.isFinite(selfProvisionCoords.lat) ||
      !Number.isFinite(selfProvisionCoords.lon)
    ) {
      return '';
    }
    const lat = Number(entry?.latitude);
    const lon = Number(entry?.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return '';
    }
    if (Math.abs(lat) > 90 || Math.abs(lon) > 180) {
      return '';
    }
    if (Math.abs(lat) < 1e-6 && Math.abs(lon) < 1e-6) {
      return '';
    }
    const distanceKm = haversineKm(selfProvisionCoords.lat, selfProvisionCoords.lon, lat, lon);
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

  function getSortedNodeRegistryEntries() {
    const entries = Array.from(nodeRegistry.values()).filter(
      (entry) => !isIgnoredMeshId(entry.meshId) && !isIgnoredMeshId(entry.meshIdOriginal)
    );
    entries.sort((a, b) => {
      const tsA = getNodeLastSeenTimestamp(a) || 0;
      const tsB = getNodeLastSeenTimestamp(b) || 0;
      if (tsA !== tsB) {
        return tsB - tsA;
      }
      const labelA = (a.longName || a.shortName || a.meshIdOriginal || a.meshId || '').toLowerCase();
      const labelB = (b.longName || b.shortName || b.meshIdOriginal || b.meshId || '').toLowerCase();
      if (labelA < labelB) return -1;
      if (labelA > labelB) return 1;
      return 0;
    });
    return entries;
  }

  function setNodeDatabaseStatus(message, variant = 'info') {
    if (!nodesStatusLabel) {
      return;
    }
    if (nodesStatusResetTimer) {
      clearTimeout(nodesStatusResetTimer);
      nodesStatusResetTimer = null;
    }
    nodesStatusLabel.textContent = message || '';
    nodesStatusLabel.classList.remove('status-info', 'status-success', 'status-error');
    if (message) {
      const className =
        variant === 'error' ? 'status-error' : variant === 'success' ? 'status-success' : 'status-info';
      nodesStatusLabel.classList.add(className);
      if (variant !== 'error') {
        nodesStatusResetTimer = setTimeout(() => {
          nodesStatusLabel.textContent = '';
          nodesStatusLabel.classList.remove('status-info', 'status-success', 'status-error');
          nodesStatusResetTimer = null;
        }, 6000);
      }
    }
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

    if (nodesSearchInput) {
      if (totalCount === 0) {
        nodesSearchInput.value = '';
        nodesSearchTerm = '';
      }
      nodesSearchInput.disabled = totalCount === 0;
      nodesSearchInput.placeholder = totalCount === 0
        ? '尚未收到節點資料'
        : '搜尋節點名稱、Mesh ID 或角色';
    }

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

      const { display: lastSeenDisplay, tooltip: lastSeenTooltip, timestamp: lastSeenTimestamp } =
        formatNodeLastSeen(entry.lastSeenAt);
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

  function upsertNodeRegistry(entry) {
    if (!entry || typeof entry !== 'object') return null;
    const candidate = entry.meshId || entry.meshIdNormalized || entry.meshIdOriginal;
    const normalized = normalizeMeshId(candidate);
    if (!normalized) return null;
    if (isIgnoredMeshId(normalized) || isIgnoredMeshId(entry.meshIdOriginal)) {
      nodeRegistry.delete(normalized);
      return null;
    }
    const existing = nodeRegistry.get(normalized) || {};
    const merged = mergeNodeMetadata(existing, entry, { meshIdNormalized: normalized });
    if (merged) {
      nodeRegistry.set(normalized, merged);
      updateTelemetryNodesWithRegistry(normalized, merged);
    }
    return merged;
  }

  function applyNodeSnapshot(list) {
    nodeRegistry.clear();
    if (Array.isArray(list)) {
      for (const entry of list) {
        upsertNodeRegistry(entry);
      }
    }
    nodeSnapshotLoaded = true;
    refreshSummaryMappingHighlights();
    refreshFlowEntryLabels();
    renderFlowEntries();
    refreshTelemetrySelectors();
    refreshSummaryRows();
    renderNodeDatabase();
    renderTelemetryView();
    if (selectedChannelId != null) {
      renderChannelMessages(selectedChannelId);
    }
  }

  function handleNodeUpdate(payload) {
    if (!payload || typeof payload !== 'object') return;
    const merged = upsertNodeRegistry(payload);
    if (!merged) return;
    refreshSummaryMappingHighlights();
    refreshFlowEntryLabels();
    renderFlowEntries();
    refreshTelemetrySelectors();
    refreshSummaryRows();
    renderNodeDatabase();
    renderTelemetryView();
    if (selectedChannelId != null) {
      renderChannelMessages(selectedChannelId);
    }
  }

  function getRegistryNode(meshId) {
    const normalized = normalizeMeshId(meshId);
    if (!normalized) return null;
    const entry = nodeRegistry.get(normalized);
    return entry ? { ...entry } : null;
  }

  function updateTelemetryNodesWithRegistry(normalizedMeshId, registryInfo) {
    if (!normalizedMeshId || !registryInfo) return;
    const bucketKey = resolveTelemetryMeshKey(normalizedMeshId);
    const bucket = telemetryStore.get(bucketKey);
    if (bucket) {
      bucket.node = mergeNodeMetadata(bucket.node, registryInfo);
      if (Array.isArray(bucket.records)) {
        for (const record of bucket.records) {
          record.node = mergeNodeMetadata(record.node, registryInfo);
        }
      }
    }
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
    const totalRecords = Number.isFinite(stats.totalRecords) ? stats.totalRecords : 0;
    const totalNodes = Number.isFinite(stats.totalNodes)
      ? stats.totalNodes
      : getSortedNodeRegistryEntries().length;
    telemetryStatsRecords.textContent = totalRecords.toLocaleString();
    telemetryStatsNodes.textContent = totalNodes.toLocaleString();
    telemetryStatsDisk.textContent = formatBytes(stats.diskBytes);
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

  function activatePage(targetId) {
    if (!targetId) return;
    pages.forEach((page) => {
      if (!page || !page.id) return;
      const isActive = page.id === targetId;
      page.classList.toggle('active', isActive);
      if (isActive && targetId === 'telemetry-page') {
        renderTelemetryView();
      } else if (isActive && targetId === 'flow-page') {
        renderFlowEntries();
      } else if (isActive && targetId === 'nodes-page') {
        renderNodeDatabase();
      } else if (isActive && targetId === 'messages-page') {
        renderChannelNav();
        if (selectedChannelId != null) {
          renderChannelMessages(selectedChannelId);
        }
      }
    });
    navButtons.forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.target === targetId);
    });
  }

  navButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const target = btn.dataset.target;
      if (target) {
        activatePage(target);
      }
    });
  });

  initializeChannelMessages();

  flowSearchInput?.addEventListener('input', () => {
    const raw = flowSearchInput.value || '';
    flowSearchTerm = raw.trim().toLowerCase();
    renderFlowEntries();
  });

  flowFilterStateSelect?.addEventListener('change', (event) => {
    flowFilterState = (event.target.value || 'all').toLowerCase();
    renderFlowEntries();
  });

  nodesSearchInput?.addEventListener('input', () => {
    nodesSearchTerm = (nodesSearchInput.value || '').trim().toLowerCase();
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
    renderTelemetryDropdown({ force: true });
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
    setTelemetryRangeMode(mode, { persist: true });
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
    loadTelemetryRecordsForSelection(telemetrySelectedMeshId, { force: true });
    safeStorageSet(STORAGE_KEYS.telemetryRangeMode, 'custom');
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

  telemetryDownloadBtn?.addEventListener('click', () => {
    downloadTelemetryCsv();
  });

  function isRelayGuessed(summary) {
    return Boolean(summary?.relay?.guessed || summary?.relayGuess);
  }

function getRelayGuessReason(summary) {
  return summary?.relayGuessReason || RELAY_GUESS_EXPLANATION;
}

  function openRelayHintDialog({ reason, relayLabel, meshId } = {}) {
    const text = reason && reason.trim() ? reason.trim() : RELAY_GUESS_EXPLANATION;
    if (!relayHintModal || !relayHintReasonEl) {
      const fallback = [text, relayLabel ? `節點：${relayLabel}` : null, meshId ? `Mesh ID：${meshId}` : null]
        .filter(Boolean)
        .join('\n');
      window.alert(fallback);
      return;
    }
    relayHintReasonEl.textContent = text;
    if (relayHintNodeEl) {
      relayHintNodeEl.textContent = relayLabel && relayLabel.trim() ? relayLabel.trim() : '—';
    }
    if (relayHintMeshEl) {
      relayHintMeshEl.textContent = meshId && meshId.trim() ? meshId.trim() : '—';
    }
    if (relayHintSubtitleEl) {
      relayHintSubtitleEl.textContent = '系統依歷史統計推測可能的最後轉發節點';
    }
    relayHintModal.classList.remove('hidden');
    document.body.classList.add('modal-open');
    setTimeout(() => relayHintOkBtn?.focus(), 0);
  }

  function closeRelayHintDialog() {
    if (!relayHintModal) return;
    relayHintModal.classList.add('hidden');
    document.body.classList.remove('modal-open');
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

  function formatRelay(summary) {
    if (!summary) return '直收';
    const fromMeshId = summary.from?.meshId || summary.from?.meshIdNormalized || '';
    const fromNormalized = normalizeMeshId(fromMeshId);
    if (fromMeshId && isSelfMesh(fromMeshId, summary)) {
      return ensureRelayGuessSuffix('Self', summary);
    }

    let relayMeshIdRaw =
      summary.relay?.meshId ||
      summary.relay?.meshIdNormalized ||
      summary.relayMeshId ||
      summary.relayMeshIdNormalized ||
      '';
    const relayNode = relayMeshIdRaw ? hydrateSummaryNode(summary.relay, relayMeshIdRaw) : null;
    if (relayNode) {
      summary.relay = relayNode;
      relayMeshIdRaw =
        relayNode.meshId || relayNode.meshIdOriginal || relayNode.meshIdNormalized || relayMeshIdRaw;
    }
    if (relayMeshIdRaw && isSelfMesh(relayMeshIdRaw, summary)) {
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

    const hopInfo = extractHopInfo(summary);
    const normalizedHopsLabel = hopInfo.hopsLabel || '';
    const zeroHop = hopInfo.usedHops === 0 || /^0(?:\s*\/|$)/.test(normalizedHopsLabel);

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

    if (hopInfo.usedHops != null && hopInfo.usedHops > 0) {
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

  function updateRelayCellDisplay(cell, summary) {
    if (!cell) return;
    const label = formatRelay(summary);
    let relayGuessed = isRelayGuessed(summary);
    if (label === '直收' || label === 'Self') {
      relayGuessed = false;
    }
    const relayGuessReason = relayGuessed ? getRelayGuessReason(summary) : '';
    cell.innerHTML = '';

    const labelSpan = document.createElement('span');
    const relayDisplay = label || (relayGuessed ? '未知' : '—');
    labelSpan.textContent = relayDisplay;
    cell.appendChild(labelSpan);

    let relayMeshRaw =
      summary?.relay?.meshId ||
      summary?.relay?.meshIdNormalized ||
      summary?.relayMeshId ||
      summary?.relayMeshIdNormalized ||
      '';
    let normalizedRelayMeshId = '';
    let relayTitle = '';
    if (typeof relayMeshRaw === 'string' && relayMeshRaw) {
      if (relayMeshRaw.startsWith('0x')) {
        normalizedRelayMeshId = `!${relayMeshRaw.slice(2)}`;
      }
      if (!normalizedRelayMeshId) {
        normalizedRelayMeshId = relayMeshRaw;
      }
      const displayMeshId = normalizedRelayMeshId;
      if (label && displayMeshId && label !== displayMeshId) {
        relayTitle = `${label} (${displayMeshId})`;
      } else {
        relayTitle = displayMeshId;
      }
    } else if (label === '直收') {
      relayTitle = '訊息為直收，未經其他節點轉發';
    } else if (label === 'Self') {
      relayTitle = '本站節點轉發';
    } else if (relayGuessed) {
      relayTitle = '最後轉發節點未知或標號不完整';
    }

    const reason = relayGuessReason || RELAY_GUESS_EXPLANATION;
    cell.classList.toggle('relay-guess', relayGuessed);
    if (relayGuessed) {
      cell.dataset.relayGuess = 'true';
      const hintButton = document.createElement('button');
      hintButton.type = 'button';
      hintButton.className = 'relay-hint-btn';
      hintButton.textContent = '?';
      hintButton.title = reason;
      hintButton.setAttribute('aria-label', '顯示推測原因');
      hintButton.addEventListener('click', (event) => {
        event.stopPropagation();
        openRelayHintDialog({
          reason,
          relayLabel: label || normalizedRelayMeshId || '',
          meshId: normalizedRelayMeshId || ''
        });
      });
      cell.appendChild(hintButton);
    } else {
      cell.classList.remove('relay-guess');
      cell.removeAttribute('data-relay-guess');
    }

    if (relayTitle) {
      cell.title = relayTitle;
    } else {
      cell.removeAttribute('title');
    }
  }

  function formatRelayLabel(relay) {
    if (!relay) return '';
    const meshId = relay.meshId || relay.meshIdOriginal || relay.meshIdNormalized || '';
    const stripped = typeof meshId === 'string' && meshId.startsWith('!') ? meshId.slice(1) : meshId;
    const shortDisplay = sanitizeNodeName(relay.shortName);
    let display = formatNodeDisplayLabel(relay);
    if (!display) {
      display = sanitizeNodeName(relay.longName) || sanitizeNodeName(relay.label) || meshId || '';
    }
    if (shortDisplay) {
      const lowerShort = shortDisplay.toLowerCase();
      if (!display.toLowerCase().includes(lowerShort)) {
        display = display ? `${display} / ${shortDisplay}` : shortDisplay;
      }
    }
    if (stripped && /^0{6}[0-9a-fA-F]{2}$/.test(String(stripped).toLowerCase())) {
      const fallback = display || meshId || '';
      return fallback || '未知';
    }
    if (display) {
      return display;
    }
    return meshId || relay.label || '';
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

  function clampMetricValue(metricName, numeric) {
    const def = TELEMETRY_METRIC_DEFINITIONS[metricName];
    if (!Number.isFinite(numeric)) {
      return numeric;
    }
    if (def?.clamp) {
      return Math.min(Math.max(numeric, def.clamp[0]), def.clamp[1]);
    }
    return numeric;
  }

  function formatSecondsAsDuration(seconds) {
    const numeric = Number(seconds);
    if (!Number.isFinite(numeric) || numeric < 0) {
      return '';
    }
    let remaining = Math.floor(numeric);
    const units = [
      { label: '年', seconds: 365 * 24 * 60 * 60 },
      { label: '月', seconds: 30 * 24 * 60 * 60 },
      { label: '日', seconds: 24 * 60 * 60 },
      { label: '小時', seconds: 60 * 60 },
      { label: '分鐘', seconds: 60 },
      { label: '秒', seconds: 1 }
    ];
    const parts = [];
    for (const unit of units) {
      const value = Math.floor(remaining / unit.seconds);
      if (value > 0 || (unit.seconds === 1 && !parts.length)) {
        parts.push(`${value}${unit.label}`);
      }
      remaining -= value * unit.seconds;
    }
    return parts.join('');
  }

  function cloneTelemetry(value) {
    if (value === null || value === undefined) {
      return value;
    }
    if (typeof structuredClone === 'function') {
      try {
        return structuredClone(value);
      } catch {
        // fall through
      }
    }
    return JSON.parse(JSON.stringify(value));
  }

  function resolveTelemetryMeshKey(meshId) {
    if (meshId == null) {
      return '__unknown__';
    }
    const value = String(meshId).trim();
    if (!value) {
      return '__unknown__';
    }
    return value;
  }

  function sanitizeTelemetryNodeData(node) {
    if (!node || typeof node !== 'object') {
      return null;
    }
    const base = {
      label: node.label ?? null,
      meshId: node.meshId ?? null,
      meshIdOriginal: node.meshIdOriginal ?? node.meshId ?? null,
      meshIdNormalized: node.meshIdNormalized ?? normalizeMeshId(node.meshId),
      shortName: node.shortName ?? null,
      longName: node.longName ?? null,
      hwModel: node.hwModel ?? null,
      hwModelLabel: node.hwModelLabel ?? null,
      role: node.role ?? null,
      roleLabel: node.roleLabel ?? null,
      lastSeenAt: Number.isFinite(node.lastSeenAt) ? Number(node.lastSeenAt) : null
    };
    const registry = getRegistryNode(base.meshIdNormalized || base.meshId);
    return mergeNodeMetadata(base, registry);
  }

  function trackTelemetryRecord(meshId, recordId) {
    if (!recordId) {
      return;
    }
    telemetryRecordOrder.push({ meshId, recordId });
  }

  function removeTelemetryOrderEntry(meshId, recordId) {
    if (!recordId || telemetryRecordOrder.length === 0) {
      return;
    }
    for (let i = telemetryRecordOrder.length - 1; i >= 0; i -= 1) {
      const entry = telemetryRecordOrder[i];
      if (entry.recordId === recordId && (meshId == null || entry.meshId === meshId)) {
        telemetryRecordOrder.splice(i, 1);
        break;
      }
    }
  }

  function updateTelemetryMaxTotalRecords(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric <= 0) {
      return;
    }
    telemetryMaxTotalRecords = Math.floor(numeric);
  }

  function enforceTelemetryGlobalLimit() {
    if (!Number.isFinite(telemetryMaxTotalRecords) || telemetryMaxTotalRecords <= 0) {
      return;
    }
    while (telemetryRecordOrder.length > telemetryMaxTotalRecords) {
      const oldest = telemetryRecordOrder.shift();
      if (!oldest) {
        break;
      }
      const bucket = telemetryStore.get(oldest.meshId);
      if (!bucket || !Array.isArray(bucket.records) || !bucket.records.length) {
        telemetryRecordIds.delete(oldest.recordId);
        continue;
      }
      const index = bucket.records.findIndex((item) => item?.id === oldest.recordId);
      if (index === -1) {
        telemetryRecordIds.delete(oldest.recordId);
        continue;
      }
      const [removed] = bucket.records.splice(index, 1);
      if (removed?.id) {
        telemetryRecordIds.delete(removed.id);
      }
      if (!bucket.records.length) {
        telemetryStore.delete(oldest.meshId);
      }
    }
  }

  function formatTelemetryNodeLabel(meshId, node) {
    const normalized =
      (node?.meshIdNormalized && String(node.meshIdNormalized).trim()) ||
      (() => {
        const value = normalizeMeshId(meshId);
        if (!value) {
          return null;
        }
        return value.startsWith('!') ? value : `!${value}`;
      })();
    const displayMesh =
      (typeof node?.meshId === 'string' && node.meshId.trim() && node.meshId !== 'unknown'
        ? node.meshId.trim()
        : null) ||
      normalized ||
      (typeof meshId === 'string' && meshId.trim() && meshId !== '__unknown__'
        ? meshId.trim()
        : null);
    let meshLabel = displayMesh;
    if (meshLabel && meshLabel.toLowerCase().includes('unknown')) {
      meshLabel = null;
    }

    const nameCandidate =
      node?.longName && node.longName !== 'unknown'
        ? node.longName
        : node?.label && node.label !== 'unknown'
          ? node.label
          : null;

    if (nameCandidate && meshLabel) {
      return nameCandidate.includes(meshLabel)
        ? nameCandidate
        : `${nameCandidate} (${meshLabel})`;
    }
    if (nameCandidate) {
      return nameCandidate;
    }
    if (meshLabel) {
      return meshLabel;
    }
    return '未知節點';
  }

  function addTelemetryRecord(meshId, node, rawRecord) {
    if (!meshId || !rawRecord) {
      return null;
    }
    const record = cloneTelemetry(rawRecord);
    if (!record || !record.telemetry || !record.telemetry.metrics) {
      return null;
    }
    const meshKey = resolveTelemetryMeshKey(meshId);
    const rawMeshId =
      typeof meshId === 'string' && meshId.trim() ? meshId.trim() : record.meshId || null;
    const sampleMs = Number(record.sampleTimeMs ?? record.timestampMs ?? Date.now());
    record.sampleTimeMs = Number.isFinite(sampleMs) ? sampleMs : Date.now();
    if (!record.id) {
      record.id = `${meshKey}-${record.sampleTimeMs}-${Math.random().toString(16).slice(2, 10)}`;
    }
    const key = meshKey;
    let bucket = telemetryStore.get(key);
    if (!bucket) {
      bucket = {
        meshId: key,
        rawMeshId: rawMeshId || key,
        node: null,
        records: [],
        recordIdSet: new Set(),
        loadedRange: null,
        totalRecords: 0,
        metrics: new Set(),
        latestSampleMs: null,
        earliestSampleMs: null
      };
      telemetryStore.set(key, bucket);
    } else if (rawMeshId && !bucket.rawMeshId) {
      bucket.rawMeshId = rawMeshId;
    }
    const nodeInfo = sanitizeTelemetryNodeData(node) || sanitizeTelemetryNodeData(record.node);
    const registryNode = getRegistryNode(rawMeshId || meshKey);
    const mergedNode = mergeNodeMetadata(bucket.node, nodeInfo, registryNode, {
      meshId: rawMeshId || meshKey,
      meshIdNormalized: normalizeMeshId(rawMeshId || meshKey)
    });
    if (mergedNode) {
      bucket.node = mergedNode;
      record.node = mergeNodeMetadata(record.node ? sanitizeTelemetryNodeData(record.node) : null, mergedNode);
    } else if (record.node) {
      record.node = sanitizeTelemetryNodeData(record.node);
    }
    if (mergedNode && mergedNode.meshIdNormalized) {
      upsertNodeRegistry(mergedNode);
    }
    record.meshId = record.meshId ?? key;
    record.rawMeshId = rawMeshId || record.rawMeshId || null;

    const metrics = record.telemetry?.metrics;
    if (metrics && typeof metrics === 'object') {
      if (!bucket.metrics) {
        bucket.metrics = new Set();
      }
      for (const metricKey of Object.keys(metrics)) {
        bucket.metrics.add(metricKey);
      }
    }
    const sampleTime = Number(record.sampleTimeMs);
    if (Number.isFinite(sampleTime)) {
      if (bucket.latestSampleMs == null || sampleTime > bucket.latestSampleMs) {
        bucket.latestSampleMs = sampleTime;
      }
      if (bucket.earliestSampleMs == null || sampleTime < bucket.earliestSampleMs) {
        bucket.earliestSampleMs = sampleTime;
      }
    }
    bucket.totalRecords = Number.isFinite(bucket.totalRecords)
      ? bucket.totalRecords + 1
      : (Array.isArray(bucket.records) ? bucket.records.length : 0) + 1;

    const currentWindow = getTelemetryRangeWindow();
    const currentStart = Number.isFinite(currentWindow.startMs) ? Number(currentWindow.startMs) : null;
    const currentEnd = Number.isFinite(currentWindow.endMs) ? Number(currentWindow.endMs) : null;
    const relativeRangeActive = isRelativeTelemetryRange();

    if (!bucket.loadedRange && relativeRangeActive) {
      bucket.loadedRange = {
        startMs: currentStart,
        endMs: currentEnd
      };
    }

    const hasLoadedRange =
      bucket.loadedRange && typeof bucket.loadedRange === 'object' && Array.isArray(bucket.records);
    if (!hasLoadedRange) {
      return record;
    }

    let startLimit = Number.isFinite(bucket.loadedRange.startMs) ? Number(bucket.loadedRange.startMs) : null;
    let endLimit = Number.isFinite(bucket.loadedRange.endMs) ? Number(bucket.loadedRange.endMs) : null;
    if (relativeRangeActive) {
      startLimit = currentStart ?? startLimit;
      endLimit = currentEnd ?? endLimit;
      bucket.loadedRange = {
        startMs: Number.isFinite(startLimit) ? startLimit : null,
        endMs: Number.isFinite(endLimit) ? endLimit : null
      };
    }

    if (Number.isFinite(sampleTime) && startLimit != null && sampleTime < startLimit) {
      return record;
    }
    if (Number.isFinite(sampleTime) && endLimit != null && sampleTime > endLimit) {
      return record;
    }
    if (record.id && bucket.recordIdSet && bucket.recordIdSet.has(record.id)) {
      return null;
    }
    bucket.records.push(record);
    bucket.records.sort((a, b) => a.sampleTimeMs - b.sampleTimeMs);
    if (record.id) {
      if (!bucket.recordIdSet) {
        bucket.recordIdSet = new Set();
      }
      bucket.recordIdSet.add(record.id);
      telemetryRecordIds.add(record.id);
    }
    while (bucket.records.length > TELEMETRY_MAX_LOCAL_RECORDS) {
      const removed = bucket.records.shift();
      if (removed?.id) {
        bucket.recordIdSet?.delete(removed.id);
        telemetryRecordIds.delete(removed.id);
        removeTelemetryOrderEntry(key, removed.id);
      }
    }
    bucket.loadedCount = bucket.records.length;
    if (Number.isFinite(bucket.totalRecords)) {
      bucket.partial = bucket.totalRecords > bucket.loadedCount;
    } else {
      bucket.partial = false;
    }
    if (record.id) {
      trackTelemetryRecord(key, record.id);
    }
    enforceTelemetryGlobalLimit();
    return record;
  }

  function clearTelemetryDataLocal({ silent = false } = {}) {
    if (telemetryFetchController) {
      telemetryFetchController.abort();
      telemetryFetchController = null;
    }
    telemetryLoading = false;
    telemetryLoadingMeshId = null;
    telemetryLastFetchKey = null;
    telemetryStore.clear();
    telemetryRecordIds.clear();
    telemetryRecordOrder.length = 0;
    telemetrySelectedMeshId = null;
    telemetryNodeLookup.clear();
    telemetryNodeDisplayByMesh.clear();
    telemetryNodeOptions = [];
    telemetrySearchRaw = '';
    telemetrySearchTerm = '';
    telemetryLastExplicitMeshId = null;
    telemetryNodeInputHoldEmpty = false;
    telemetryDropdownVisible = false;
    telemetryDropdownInteracting = false;
    if (!silent) {
      telemetryUpdatedAt = Date.now();
    }
    destroyAllTelemetryCharts();
    if (telemetryChartMetricSelect) {
      telemetryChartMetricSelect.innerHTML = '';
      telemetryChartMetricSelect.classList.add('hidden');
      telemetryChartMetricSelect.disabled = true;
    }
    if (telemetryTableBody) {
      telemetryTableBody.innerHTML = '';
    }
    if (telemetryEmptyState) {
      telemetryEmptyState.classList.remove('hidden');
      telemetryEmptyState.textContent = '尚未收到遙測資料。';
    }
    if (telemetryChartsContainer) {
      telemetryChartsContainer.classList.add('hidden');
      telemetryChartsContainer.innerHTML = '';
    }
    if (telemetryTableWrapper) {
      telemetryTableWrapper.classList.add('hidden');
    }
    hideTelemetryDropdown();
    if (telemetryNodeInput) {
      telemetryNodeInput.value = '';
      telemetryNodeInput.disabled = true;
      telemetryNodeInput.placeholder = '尚未收到遙測資料';
    }
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
    const label = `${date.getFullYear()}/${String(date.getMonth() + 1).padStart(2, '0')}/${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
    telemetryUpdatedAtLabel.textContent = label;
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

  function setTelemetryRangeMode(mode, { skipRender = false, persist = false } = {}) {
    if (!isValidTelemetryRangeMode(mode)) {
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
      loadTelemetryRecordsForSelection(telemetrySelectedMeshId, { force: true });
    }
    if (persist) {
      safeStorageSet(STORAGE_KEYS.telemetryRangeMode, telemetryRangeMode);
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
      case 'hour1':
        return {
          startMs: now - 1 * 60 * 60 * 1000,
          endMs: now
        };
      case 'hour3':
        return {
          startMs: now - 3 * 60 * 60 * 1000,
          endMs: now
        };
      case 'hour6':
        return {
          startMs: now - 6 * 60 * 60 * 1000,
          endMs: now
        };
      case 'hour12':
        return {
          startMs: now - 12 * 60 * 60 * 1000,
          endMs: now
        };
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
          return { startMs: end, endMs: start };
        }
        return { startMs: start, endMs: end };
      }
      default:
        return { startMs: null, endMs: null };
    }
  }

  const TELEMETRY_RANGE_DURATIONS = {
    hour1: 1 * 60 * 60 * 1000,
    hour3: 3 * 60 * 60 * 1000,
    hour6: 6 * 60 * 60 * 1000,
    hour12: 12 * 60 * 60 * 1000,
    day: 24 * 60 * 60 * 1000,
    week: 7 * 24 * 60 * 60 * 1000,
    month: 30 * 24 * 60 * 60 * 1000,
    year: 365 * 24 * 60 * 60 * 1000
  };

  function getTelemetryRangeDuration(mode = telemetryRangeMode) {
    return TELEMETRY_RANGE_DURATIONS[mode] ?? null;
  }

  function isRelativeTelemetryRange(mode = telemetryRangeMode) {
    return mode !== 'custom' && getTelemetryRangeDuration(mode) != null;
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
      for (const [lookup, meshId] of telemetryNodeLookup.entries()) {
        if (lookup.includes(lowered)) {
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
    if (!telemetryNodeDropdown || telemetryDropdownVisible) {
      return;
    }
    telemetryNodeDropdown.classList.remove('hidden');
    telemetryDropdownVisible = true;
  }

  function hideTelemetryDropdown() {
    if (!telemetryNodeDropdown) {
      return;
    }
    telemetryNodeDropdown.classList.add('hidden');
    telemetryNodeDropdown.innerHTML = '';
    telemetryDropdownVisible = false;
    telemetryDropdownInteracting = false;
  }

  function renderTelemetryDropdown({ force = false } = {}) {
    if (!telemetryNodeDropdown || !telemetryNodeInput || telemetryNodeInput.disabled) {
      hideTelemetryDropdown();
      return;
    }
    const candidates = getTelemetryNavigationCandidates();
    const shouldShow = Boolean(candidates.length) && document.activeElement === telemetryNodeInput;
    if (!shouldShow) {
      hideTelemetryDropdown();
      return;
    }
    if (telemetryDropdownVisible && !force) {
      return;
    }
    const existingOptions = telemetryNodeDropdown.children;
    let needsRebuild = existingOptions.length !== candidates.length;
    if (!needsRebuild) {
      for (let i = 0; i < candidates.length; i += 1) {
        const candidate = candidates[i];
        const option = existingOptions[i];
        if (!option) {
          needsRebuild = true;
          break;
        }
        const displayText = candidate.display || candidate.meshId || '未知節點';
        if (option.dataset.meshId !== (candidate.meshId || '') || option.textContent !== displayText) {
          needsRebuild = true;
          break;
        }
      }
    }
    let activeOption = null;
    if (needsRebuild) {
      const fragment = document.createDocumentFragment();
      for (const candidate of candidates) {
        const option = document.createElement('div');
        option.className = 'telemetry-node-option';
        option.dataset.meshId = candidate.meshId || '';
        const displayText = candidate.display || candidate.meshId || '未知節點';
        option.textContent = displayText;
        option.title = displayText;
        if (candidate.meshId === telemetrySelectedMeshId) {
          option.classList.add('active');
          activeOption = option;
        }
        fragment.appendChild(option);
      }
      telemetryNodeDropdown.innerHTML = '';
      telemetryNodeDropdown.appendChild(fragment);
    } else {
      for (let i = 0; i < candidates.length; i += 1) {
        const candidate = candidates[i];
        const option = existingOptions[i];
        const displayText = candidate.display || candidate.meshId || '未知節點';
        option.textContent = displayText;
        option.title = displayText;
        option.dataset.meshId = candidate.meshId || '';
        if (candidate.meshId === telemetrySelectedMeshId) {
          option.classList.add('active');
          activeOption = option;
        } else {
          option.classList.remove('active');
        }
      }
    }
    showTelemetryDropdown();
    if (activeOption) {
      activeOption.scrollIntoView({ block: 'nearest' });
    }
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
    if (hideDropdown) {
      hideTelemetryDropdown();
    } else {
      renderTelemetryDropdown({ force: true });
    }
    loadTelemetryRecordsForSelection(meshId);
  }

  async function requestTelemetryRange({ meshId, startMs = null, endMs = null, limit = null, signal } = {}) {
    if (!meshId) {
      throw new Error('meshId is required');
    }
    const params = new URLSearchParams();
    params.set('meshId', meshId);
    if (Number.isFinite(Number(startMs))) {
      params.set('startMs', String(Math.floor(Number(startMs))));
    }
    if (Number.isFinite(Number(endMs))) {
      params.set('endMs', String(Math.floor(Number(endMs))));
    }
    if (limit != null && Number.isFinite(Number(limit)) && Number(limit) > 0) {
      params.set('limit', String(Math.floor(Number(limit))));
    }
    const response = await fetch(`/api/telemetry?${params.toString()}`, {
      signal,
      headers: {
        Accept: 'application/json'
      }
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    if (!payload || typeof payload !== 'object') {
      throw new Error('Invalid telemetry response');
    }
    return payload;
  }

  async function loadTelemetryRecordsForSelection(
    meshId = telemetrySelectedMeshId,
    { force = false } = {}
  ) {
    if (!meshId) {
      renderTelemetryView();
      return;
    }
    const { startMs, endMs } = getTelemetryRangeWindow();
    const fetchKey = `${meshId}:${startMs ?? ''}:${endMs ?? ''}`;
    const existingBucket = telemetryStore.get(meshId);
    if (
      !force &&
      existingBucket &&
      existingBucket.loadedRange &&
      existingBucket.loadedRange.startMs === (startMs ?? null) &&
      existingBucket.loadedRange.endMs === (endMs ?? null) &&
      Array.isArray(existingBucket.records) &&
      existingBucket.records.length
    ) {
      telemetrySelectedMeshId = meshId;
      telemetryLoading = false;
      telemetryLoadingMeshId = null;
      telemetryLastFetchKey = fetchKey;
      renderTelemetryView();
      return;
    }

    if (telemetryFetchController) {
      telemetryFetchController.abort();
    }
    telemetryFetchController = new AbortController();
    telemetryLoading = true;
    telemetryLoadingMeshId = meshId;
    telemetryLastFetchKey = fetchKey;
    renderTelemetryView();

    try {
      const viewLimit = TELEMETRY_MAX_LOCAL_RECORDS;
      const payload = await requestTelemetryRange({
        meshId,
        startMs,
        endMs,
        limit: viewLimit,
        signal: telemetryFetchController.signal
      });
      if (telemetryFetchController.signal.aborted) {
        return;
      }
      const bucketKey = resolveTelemetryMeshKey(payload.meshId || meshId);
      if (!bucketKey) {
        throw new Error('Invalid telemetry response');
      }
      const sanitizedNode = sanitizeTelemetryNodeData(payload.node);
      let bucket = telemetryStore.get(bucketKey);
      if (!bucket) {
        bucket = {
          meshId: bucketKey,
          rawMeshId: payload.rawMeshId || payload.meshId || bucketKey,
          node: sanitizedNode,
          records: [],
          recordIdSet: new Set(),
          loadedRange: null,
          loadedCount: 0,
          totalRecords: Number.isFinite(payload.totalRecords) ? Number(payload.totalRecords) : 0,
          metrics: new Set(Array.isArray(payload.availableMetrics) ? payload.availableMetrics : []),
          latestSampleMs: Number.isFinite(payload.latestSampleMs)
            ? Number(payload.latestSampleMs)
            : null,
          earliestSampleMs: Number.isFinite(payload.earliestSampleMs)
            ? Number(payload.earliestSampleMs)
            : null,
          partial: false
        };
        telemetryStore.set(bucketKey, bucket);
      } else {
        if (payload.rawMeshId && !bucket.rawMeshId) {
          bucket.rawMeshId = payload.rawMeshId;
        }
        if (sanitizedNode) {
          bucket.node = mergeNodeMetadata(bucket.node, sanitizedNode);
        }
        if (Number.isFinite(payload.totalRecords)) {
          bucket.totalRecords = Number(payload.totalRecords);
        }
        if (Number.isFinite(payload.latestSampleMs)) {
          bucket.latestSampleMs = Number(payload.latestSampleMs);
        }
        if (Number.isFinite(payload.earliestSampleMs)) {
          bucket.earliestSampleMs = Number(payload.earliestSampleMs);
        }
        if (!bucket.metrics) {
          bucket.metrics = new Set();
        }
        if (Array.isArray(payload.availableMetrics)) {
          for (const metricKey of payload.availableMetrics) {
            bucket.metrics.add(metricKey);
          }
        }
      }
      if (sanitizedNode && sanitizedNode.meshIdNormalized) {
        upsertNodeRegistry(sanitizedNode);
      }

      if (Array.isArray(bucket.records)) {
        for (const existing of bucket.records) {
          if (existing?.id) {
            telemetryRecordIds.delete(existing.id);
            bucket.recordIdSet?.delete(existing.id);
          }
        }
      }

      const fetchedRecords = Array.isArray(payload.records) ? payload.records : [];
      const limitedRecords =
        viewLimit && fetchedRecords.length > viewLimit
          ? fetchedRecords.slice(fetchedRecords.length - viewLimit)
          : fetchedRecords;
      bucket.records = limitedRecords.map((item) => cloneTelemetry(item));
      bucket.recordIdSet = new Set();
      for (const rec of bucket.records) {
        if (rec?.id) {
          bucket.recordIdSet.add(rec.id);
          telemetryRecordIds.add(rec.id);
        }
      }
      bucket.loadedRange = {
        startMs: startMs != null ? startMs : null,
        endMs: endMs != null ? endMs : null,
        limit: viewLimit
      };
      const totalRecords = Number.isFinite(payload.totalRecords)
        ? Number(payload.totalRecords)
        : fetchedRecords.length;
      bucket.totalRecords = totalRecords;
      const loadedCount = Math.min(
        Number.isFinite(payload.filteredCount) ? Number(payload.filteredCount) : bucket.records.length,
        bucket.records.length
      );
      bucket.loadedCount = loadedCount;
      bucket.partial =
        Number.isFinite(bucket.totalRecords) &&
        Number.isFinite(loadedCount) &&
        loadedCount < bucket.totalRecords;
      if (!Number.isFinite(bucket.latestSampleMs) || bucket.latestSampleMs == null) {
        const latestFromRecords = bucket.records.length
          ? bucket.records[bucket.records.length - 1].sampleTimeMs
          : null;
        bucket.latestSampleMs = Number.isFinite(latestFromRecords) ? Number(latestFromRecords) : bucket.latestSampleMs;
      }
      if (!Number.isFinite(bucket.earliestSampleMs) || bucket.earliestSampleMs == null) {
        const earliestFromRecords = bucket.records.length ? bucket.records[0].sampleTimeMs : null;
        if (Number.isFinite(earliestFromRecords)) {
          bucket.earliestSampleMs = Number(earliestFromRecords);
        }
      }

      telemetrySelectedMeshId = bucketKey;
      telemetryLastExplicitMeshId = bucketKey;
      telemetryLoading = false;
      telemetryLoadingMeshId = null;
      telemetryFetchController = null;
      refreshTelemetrySelectors(bucketKey);
      updateTelemetryNodeInputDisplay();
      renderTelemetryView();
    } catch (err) {
      if (err.name === 'AbortError') {
        return;
      }
      telemetryLoading = false;
      telemetryLoadingMeshId = null;
      telemetryFetchController = null;
      console.error('載入遙測資料失敗：', err);
      if (telemetryEmptyState) {
        telemetryEmptyState.classList.remove('hidden');
        telemetryEmptyState.textContent = `載入遙測資料失敗：${err.message}`;
      }
      renderTelemetryView();
    }
  }

  function handleTelemetryNodeNavigationKey(event) {
    if (!telemetryNodeInput || telemetryNodeInput.disabled) {
      return;
    }
    renderTelemetryDropdown({ force: true });
    const candidates = getTelemetryNavigationCandidates();
    if (!candidates.length) {
      return;
    }
    const key = event.key;
    const pageJump = Math.max(1, Math.floor(candidates.length / 10)) || 1;
    const currentIndex = findTelemetryCandidateIndex(candidates, telemetrySelectedMeshId);
    const fallbackMeshId = telemetrySelectedMeshId || telemetryLastExplicitMeshId || getFirstTelemetryMeshId();
    let effectiveIndex = currentIndex;
    if (effectiveIndex === -1 && fallbackMeshId) {
      effectiveIndex = findTelemetryCandidateIndex(candidates, fallbackMeshId);
    }
    let nextIndex = null;
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
    if (!telemetryNodeInput || telemetryNodeInput.disabled || document.activeElement !== telemetryNodeInput) {
      return;
    }
    renderTelemetryDropdown({ force: true });
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
    if (!telemetryNodeInput || telemetryNodeInput.disabled) {
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
    const matched = resolveTelemetryNodeSelection(raw, { allowPartial: isChangeEvent });
    if (matched) {
      applyTelemetryNodeSelection(matched, { hideDropdown: isChangeEvent });
      return;
    }
    telemetrySearchRaw = raw;
    telemetrySearchTerm = raw.toLowerCase();
    updateTelemetryNodeInputDisplay();
    renderTelemetryView();
  }

  function getFirstTelemetryMeshId() {
    const iterator = telemetryStore.keys();
    const first = iterator.next();
    return first && !first.done ? first.value : null;
  }

  function refreshTelemetrySelectors(preferredMeshId = null) {
    if (!telemetryNodeInput) {
      return;
    }
    const previousSelection = telemetrySelectedMeshId;
    const searchActive = Boolean(telemetrySearchRaw);
    const { startMs, endMs } = getTelemetryRangeWindow();
    const nodes = [];
    for (const bucket of telemetryStore.values()) {
      if (!bucket) continue;
      const meshKey = bucket.meshId || resolveTelemetryMeshKey(bucket.rawMeshId);
      if (!meshKey) continue;
      const labelBase = formatTelemetryNodeLabel(meshKey, bucket.node);
      const metricsCount = bucket.metrics instanceof Set ? bucket.metrics.size : 0;
      const latestMs = Number.isFinite(bucket.latestSampleMs) ? bucket.latestSampleMs : null;
      const totalRecords = Number.isFinite(bucket.totalRecords) ? bucket.totalRecords : 0;
      let label = labelBase;
      const hasLoadedRange =
        bucket.loadedRange &&
        bucket.loadedRange.startMs === (startMs ?? null) &&
        bucket.loadedRange.endMs === (endMs ?? null);
      if (hasLoadedRange && (!Array.isArray(bucket.records) || !bucket.records.length)) {
        label = `${labelBase}（區間無資料）`;
      } else if (totalRecords === 0) {
        label = `${labelBase}（尚無資料）`;
      }
      nodes.push({
        meshId: meshKey,
        rawMeshId: bucket.rawMeshId || meshKey,
        label,
        baseLabel: labelBase,
        metricsCount,
        totalRecords,
        latestMs
      });
    }

    telemetryNodeLookup.clear();
    telemetryNodeDisplayByMesh.clear();
    telemetryNodeOptions = [];

    if (!nodes.length) {
      hideTelemetryDropdown();
      telemetrySelectedMeshId = null;
      telemetryLastExplicitMeshId = null;
      telemetrySearchRaw = '';
      telemetrySearchTerm = '';
      if (telemetryNodeInput) {
        telemetryNodeInput.value = '';
        telemetryNodeInput.disabled = true;
        telemetryNodeInput.placeholder = '尚未收到遙測資料';
      }
      return;
    }

    telemetryNodeInput.disabled = false;
    telemetryNodeInput.placeholder = telemetryNodeInputDefaultPlaceholder;

    nodes.sort((a, b) => {
      const aTime = Number.isFinite(a.latestMs) ? a.latestMs : -Infinity;
      const bTime = Number.isFinite(b.latestMs) ? b.latestMs : -Infinity;
      if (bTime !== aTime) {
        return bTime - aTime;
      }
      if (b.totalRecords !== a.totalRecords) {
        return b.totalRecords - a.totalRecords;
      }
      return a.baseLabel.localeCompare(b.baseLabel, 'zh-Hant', { sensitivity: 'base' });
    });

    for (const item of nodes) {
      const meshIdNormalized =
        normalizeMeshId(item.meshId) || normalizeMeshId(item.rawMeshId) || item.rawMeshId || item.meshId;
      const meshIdRaw = item.meshId || item.rawMeshId || meshIdNormalized || '__unknown__';
      const display = item.label;

      if (meshIdRaw) {
        telemetryNodeDisplayByMesh.set(meshIdRaw, display);
        telemetryNodeLookup.set(meshIdRaw.toLowerCase(), meshIdRaw);
      }
      if (meshIdNormalized) {
        telemetryNodeDisplayByMesh.set(meshIdNormalized, display);
        telemetryNodeLookup.set(meshIdNormalized.toLowerCase(), meshIdRaw);
      }
      if (item.rawMeshId) {
        telemetryNodeLookup.set(String(item.rawMeshId).toLowerCase(), meshIdRaw);
      }
      if (item.baseLabel) {
        telemetryNodeLookup.set(item.baseLabel.toLowerCase(), meshIdRaw);
      }
      telemetryNodeLookup.set(display.toLowerCase(), meshIdRaw);

      const searchKeys = new Set();
      if (display) {
        searchKeys.add(display.toLowerCase());
      }
      if (meshIdRaw) {
        searchKeys.add(meshIdRaw.toLowerCase());
      }
      if (meshIdNormalized) {
        searchKeys.add(meshIdNormalized.toLowerCase());
      }
      if (item.rawMeshId) {
        searchKeys.add(String(item.rawMeshId).toLowerCase());
      }
      if (item.baseLabel) {
        searchKeys.add(item.baseLabel.toLowerCase());
      }
      telemetryNodeOptions.push({
        meshId: meshIdRaw,
        display,
        latestMs: item.latestMs ?? null,
        searchKeys: Array.from(searchKeys).filter(Boolean)
      });
    }

    if (searchActive) {
      updateTelemetryNodeInputDisplay();
      renderTelemetryDropdown({ force: true });
      return;
    }

    const resolveMeshId = (value) => {
      if (value == null) return null;
      const normalized = normalizeMeshId(value) || value;
      const byNormalized = telemetryNodeLookup.get(normalized.toLowerCase());
      if (byNormalized) {
        return byNormalized;
      }
      return telemetryNodeLookup.get(String(value).toLowerCase()) || null;
    };

    const candidateMeshIds = telemetryNodeOptions.map((option) => option.meshId).filter(Boolean);
    const preferredRaw = resolveMeshId(preferredMeshId);
    const previousRaw = resolveMeshId(previousSelection);
    const explicitRaw = resolveMeshId(telemetryLastExplicitMeshId);

    let nextSelection = previousRaw;
    if (preferredRaw && candidateMeshIds.includes(preferredRaw)) {
      nextSelection = preferredRaw;
    } else if (previousRaw && candidateMeshIds.includes(previousRaw)) {
      nextSelection = previousRaw;
    } else if (explicitRaw && candidateMeshIds.includes(explicitRaw)) {
      nextSelection = explicitRaw;
    } else if (candidateMeshIds.includes(telemetrySelectedMeshId)) {
      nextSelection = telemetrySelectedMeshId;
    } else {
      nextSelection = null;
    }

    telemetrySelectedMeshId = nextSelection;
    if (nextSelection) {
      telemetryNodeInput.placeholder = telemetryNodeInputDefaultPlaceholder;
    } else {
      telemetryNodeInput.placeholder = '選擇節點以載入資料';
    }
    if (!telemetryDropdownVisible) {
      renderTelemetryDropdown();
    }
    updateTelemetryNodeInputDisplay();
  }

  function getTelemetryRecordsForSelection() {
    if (!telemetrySelectedMeshId) {
      return [];
    }
    const bucket = telemetryStore.get(telemetrySelectedMeshId);
    if (!bucket || !Array.isArray(bucket.records)) {
      return [];
    }
    return bucket.records.slice().sort((a, b) => b.sampleTimeMs - a.sampleTimeMs);
  }

  function filterTelemetryBySearch(records) {
    if (!Array.isArray(records) || !records.length || !telemetrySearchTerm) {
      return records;
    }
    const term = telemetrySearchTerm.toLowerCase();
    return records.filter((record) => matchesTelemetrySearch(record, term));
  }

  function matchesTelemetrySearch(record, term) {
    if (!record || !term) return false;
    const haystack = [];
    const node = record.node || {};
    haystack.push(node.label, node.longName, node.shortName, node.hwModelLabel, node.roleLabel);
    haystack.push(record.meshId, node.meshId, node.meshIdOriginal, node.meshIdNormalized);
    const relay = record.relay || {};
    haystack.push(
      record.relayLabel,
      record.relayMeshId,
      record.relayMeshIdNormalized,
      relay.label,
      relay.longName,
      relay.shortName
    );
    if (record.relayGuessReason) {
      haystack.push(record.relayGuessReason);
    }
    if (record.detail) haystack.push(record.detail);
    if (record.channel != null) haystack.push(`ch ${record.channel}`);
    if (Number.isFinite(record.snr)) haystack.push(`snr ${record.snr}`);
    if (Number.isFinite(record.rssi)) haystack.push(`rssi ${record.rssi}`);
    if (record.hopsLabel) {
      haystack.push(record.hopsLabel);
    }
    if (Number.isFinite(record.hopsUsed)) {
      haystack.push(`hops ${record.hopsUsed}`);
    }
    if (Number.isFinite(record.hopsTotal)) {
      haystack.push(`hops ${record.hopsUsed}/${record.hopsTotal}`);
      haystack.push(String(record.hopsTotal));
    }
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
        const clamped = def?.clamp
          ? Math.min(Math.max(numeric, def.clamp[0]), def.clamp[1])
          : numeric;
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
      const decimalsForSeries = computeSeriesDecimals(metricName, series);
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
        latest.textContent = formatTelemetryFixed(metricName, latestValue, decimalsForSeries) || '—';
        header.append(title, latest);
        const canvasWrap = document.createElement('div');
        canvasWrap.className = 'telemetry-chart-canvas-wrap';
        const canvas = document.createElement('canvas');
        canvasWrap.appendChild(canvas);
        card.append(header, canvasWrap);
        const ctx = canvas.getContext('2d');
        const chart = new window.Chart(
          ctx,
          buildTelemetryChartConfig(metricName, def, series, decimalsForSeries)
        );
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
        dataset.telemetryDecimals = decimalsForSeries;
      } else {
        const fallback = buildTelemetryChartConfig(
          metricName,
          def,
          series,
          decimalsForSeries
        ).data.datasets[0];
        chart.data.datasets = [{ ...fallback }];
      }
      chart.update('none');

      if (view.latestEl) {
        view.latestEl.textContent =
          formatTelemetryFixed(metricName, latestValue, decimalsForSeries) || '—';
      }

      telemetryChartsContainer.appendChild(view.card);
    }

    for (const [metricName, view] of Array.from(telemetryCharts.entries())) {
      if (!activeMetrics.has(metricName)) {
        try {
          view.chart?.destroy();
        } catch {
          // ignore
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

  function buildTelemetryChartConfig(metricName, def, series, seriesDecimals) {
    const dataset = series.map((point) => ({ x: point.time, y: point.value }));
    const labelText = def.label || metricName;
    return {
      type: 'line',
      data: {
        datasets: [
          {
            label: labelText,
            data: dataset,
            telemetryDecimals: seriesDecimals,
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
              callback: (value, index, ticks) =>
                formatTelemetryAxisValue(metricName, value, ticks) || value
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
                const decimals = ctx.dataset?.telemetryDecimals;
                const formatted =
                  formatTelemetryFixed(metricName, value, decimals) ||
                  value;
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
        // ignore
      }
      if (view.card?.parentNode) {
        view.card.parentNode.removeChild(view.card);
      }
    }
    telemetryCharts.clear();
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
      const nodeLabel = record.node
        ? formatTelemetryNodeLabel(record.meshId, record.node)
        : record.meshId || '未知節點';
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
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const mi = String(date.getMinutes()).padStart(2, '0');
    return `${yyyy}/${mm}/${dd} ${hh}:${mi}`;
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

  function buildTelemetryRelayDescriptor(record) {
    if (!record) return null;
    const relay = record.relay || {};
    const label =
      (typeof record.relayLabel === 'string' && record.relayLabel.trim()) ||
      (typeof relay.label === 'string' && relay.label.trim()) ||
      (typeof relay.longName === 'string' && relay.longName.trim()) ||
      (typeof relay.shortName === 'string' && relay.shortName.trim()) ||
      (typeof record.relayMeshId === 'string' && record.relayMeshId.trim()) ||
      (typeof record.relayMeshIdNormalized === 'string' && record.relayMeshIdNormalized.trim()) ||
      '';
    if (!label) {
      return null;
    }
    const guessed = Boolean(record.relayGuessed || relay.guessed);
    return guessed ? `${label} (?)` : label;
  }

  function buildTelemetryHopsDescriptor(record) {
    if (!record) return null;
    if (Number.isFinite(record.hopsUsed) && Number.isFinite(record.hopsTotal)) {
      return `${record.hopsUsed}/${record.hopsTotal}`;
    }
    if (Number.isFinite(record.hopsUsed)) {
      return String(record.hopsUsed);
    }
    if (typeof record.hopsLabel === 'string' && record.hopsLabel.trim()) {
      return record.hopsLabel.trim();
    }
    const hops = record.hops || {};
    if (Number.isFinite(hops.start) && Number.isFinite(hops.limit)) {
      const used = Math.max(hops.start - hops.limit, 0);
      return `${used}/${hops.start}`;
    }
    return null;
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
    const relayDescriptor = buildTelemetryRelayDescriptor(record);
    if (relayDescriptor) {
      extras.push(`Relay ${relayDescriptor}`);
    }
    const hopsDescriptor = buildTelemetryHopsDescriptor(record);
    if (hopsDescriptor) {
      extras.push(`Hops ${hopsDescriptor}`);
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
      const clamped = clampMetricValue(metricName, numeric);
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

  function resolveMetricBaseDecimals(metricName) {
    const def = TELEMETRY_METRIC_DEFINITIONS[metricName];
    if (def?.decimals != null) {
      return def.decimals;
    }
    return 2;
  }

  function resolveAxisDecimals(metricName, ticks) {
    let decimals = resolveMetricBaseDecimals(metricName);
    if (!Array.isArray(ticks) || ticks.length <= 1) {
      return decimals;
    }
    const numericTicks = ticks
      .map((tick) => Number(tick?.value ?? tick))
      .filter((value) => Number.isFinite(value))
      .sort((a, b) => a - b);
    if (numericTicks.length <= 1) {
      return decimals;
    }
    let minDiff = Infinity;
    for (let i = 1; i < numericTicks.length; i += 1) {
      const diff = Math.abs(numericTicks[i] - numericTicks[i - 1]);
      if (diff > 0 && diff < minDiff) {
        minDiff = diff;
      }
    }
    if (!Number.isFinite(minDiff) || minDiff <= 0) {
      return Math.min(Math.max(decimals, 2), 6);
    }
    const required = Math.ceil(Math.max(0, -Math.log10(minDiff)));
    decimals = Math.max(decimals, required);
    return Math.min(Math.max(decimals, 0), 6);
  }

  function formatTelemetryAxisValue(metricName, rawValue, ticks) {
    const numeric = Number(rawValue);
    if (!Number.isFinite(numeric)) {
      return '';
    }
    const decimals = resolveAxisDecimals(metricName, ticks);
    const formatted = numeric.toFixed(decimals);
    const def = TELEMETRY_METRIC_DEFINITIONS[metricName];
    return def?.unit ? `${formatted}${def.unit}` : formatted;
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

  function computeSeriesDecimals(metricName, series) {
    if (!Array.isArray(series) || series.length < 2) {
      return resolveMetricBaseDecimals(metricName);
    }
    let minDiff = Infinity;
    for (let i = 1; i < series.length; i += 1) {
      const diff = Math.abs(series[i].value - series[i - 1].value);
      if (diff > 0 && diff < minDiff) {
        minDiff = diff;
      }
    }
    if (!Number.isFinite(minDiff) || minDiff <= 0) {
      return resolveMetricBaseDecimals(metricName);
    }
    const required = Math.ceil(Math.max(0, -Math.log10(minDiff)));
    return Math.min(Math.max(Math.max(resolveMetricBaseDecimals(metricName), required), 0), 6);
  }

  function formatTelemetryFixed(metricName, rawValue, decimalsOverride) {
    const numeric = Number(rawValue);
    if (!Number.isFinite(numeric)) {
      return '';
    }
    const clamped = clampMetricValue(metricName, numeric);
    const baseDecimals = resolveMetricBaseDecimals(metricName);
    const decimals = Math.min(Math.max(decimalsOverride ?? baseDecimals, 0), 6);
    let formatted = clamped.toFixed(decimals);
    formatted = trimTrailingZeros(formatted);
    const def = TELEMETRY_METRIC_DEFINITIONS[metricName];
    return def?.unit ? `${formatted}${def.unit}` : formatted;
  }

  function renderTelemetryView() {
    if (!telemetryTableBody || !telemetryEmptyState) {
      return;
    }
    if (!telemetrySelectedMeshId) {
      if (telemetryDownloadBtn) {
        telemetryDownloadBtn.disabled = true;
        telemetryDownloadBtn.title = '請先選擇節點';
      }
      destroyAllTelemetryCharts();
      if (telemetryChartsContainer) {
        telemetryChartsContainer.classList.add('hidden');
        telemetryChartsContainer.innerHTML = '';
      }
      if (telemetryTableWrapper) {
        telemetryTableWrapper.classList.add('hidden');
      }
      telemetryTableBody.innerHTML = '';
      telemetryEmptyState.classList.remove('hidden');
      telemetryEmptyState.textContent = '請選擇節點以載入遙測資料。';
      return;
    }
    if (telemetryLoading && telemetryLoadingMeshId === telemetrySelectedMeshId) {
      if (telemetryDownloadBtn) {
        telemetryDownloadBtn.disabled = true;
        telemetryDownloadBtn.title = '資料載入中';
      }
      destroyAllTelemetryCharts();
      if (telemetryChartsContainer) {
        telemetryChartsContainer.classList.add('hidden');
        telemetryChartsContainer.innerHTML = '';
      }
      if (telemetryTableWrapper) {
        telemetryTableWrapper.classList.add('hidden');
      }
      telemetryTableBody.innerHTML = '';
      telemetryEmptyState.classList.remove('hidden');
      telemetryEmptyState.textContent = '載入遙測資料中...';
      return;
    }
    const baseRecords = getTelemetryRecordsForSelection();
    const filteredRecords = applyTelemetryFilters(baseRecords);
    const searchFilteredRecords = filterTelemetryBySearch(filteredRecords);
    const hasData = searchFilteredRecords.length > 0;
    const hasBase = baseRecords.length > 0;
    if (telemetryDownloadBtn) {
      telemetryDownloadBtn.disabled = !hasData;
      if (!hasData) {
        telemetryDownloadBtn.title = '目前沒有可匯出的遙測資料';
      } else {
        const bucket = telemetrySelectedMeshId ? telemetryStore.get(telemetrySelectedMeshId) : null;
        if (bucket && bucket.partial && Number.isFinite(bucket.totalRecords)) {
          const loadedCount = Number.isFinite(bucket.loadedCount)
            ? bucket.loadedCount
            : bucket.records.length;
          telemetryDownloadBtn.title = `目前僅載入 ${loadedCount} / ${bucket.totalRecords} 筆，匯出會下載完整區間資料。`;
        } else {
          telemetryDownloadBtn.title = '';
        }
      }
    }
    telemetryEmptyState.classList.toggle('hidden', hasData);
    if (telemetryTableWrapper) {
      telemetryTableWrapper.classList.toggle('hidden', !hasData);
    }
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
        telemetryChartsContainer.classList.add('hidden');
      }
      telemetryTableBody.innerHTML = '';
      return;
    }
    renderTelemetryCharts(searchFilteredRecords);
    renderTelemetryTable(searchFilteredRecords);
  }

  function downloadTelemetryCsv() {
    const meshId = telemetrySelectedMeshId || telemetryLastExplicitMeshId;
    if (!meshId) {
      appendLog({ tag: 'telemetry', message: '尚未選擇節點，無法匯出遙測資料' });
      return;
    }
    if (telemetryLoading && telemetryLoadingMeshId === meshId) {
      appendLog({ tag: 'telemetry', message: '資料載入中，請稍候再試匯出' });
      return;
    }

    const bucket = telemetryStore.get(meshId);
    const hasKnownRecords =
      (bucket && Number.isFinite(bucket.totalRecords) && bucket.totalRecords > 0) ||
      (bucket && Array.isArray(bucket.records) && bucket.records.length > 0);
    if (!hasKnownRecords) {
      appendLog({ tag: 'telemetry', message: '目前沒有可匯出的遙測資料' });
      return;
    }

    const button = telemetryDownloadBtn || null;
    if (button) {
      button.disabled = true;
      button.textContent = '準備中…';
      button.title = '資料匯出中';
    }

    const { startMs, endMs } = getTelemetryRangeWindow();
    const params = new URLSearchParams();
    params.set('meshId', meshId);
    if (startMs != null) {
      params.set('startMs', String(Math.floor(startMs)));
    }
    if (endMs != null) {
      params.set('endMs', String(Math.floor(endMs)));
    }
    if (telemetrySearchTerm) {
      params.set('search', telemetrySearchRaw || telemetrySearchTerm);
    }

    const url = `/api/telemetry/export.csv?${params.toString()}`;
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = '';
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);

    const logLabel =
      telemetryNodeDisplayByMesh.get(meshId || '') ||
      normalizeMeshId(meshId) ||
      meshId ||
      '未選擇節點';
    appendLog({
      tag: 'telemetry',
      message: `已開始匯出遙測資料 (${logLabel})`
    });

    if (button) {
      setTimeout(() => {
        button.disabled = false;
        button.textContent = '下載 CSV';
        button.title = bucket && bucket.partial && Number.isFinite(bucket.totalRecords)
          ? `目前僅載入 ${bucket.loadedCount ?? bucket.records.length} / ${bucket.totalRecords} 筆，匯出會下載完整區間資料。`
          : '';
      }, 1500);
    }
  }

  function applyTelemetrySnapshot(snapshot) {
    const previousSelection = telemetrySelectedMeshId;
    const previousExplicit = telemetryLastExplicitMeshId;
    clearTelemetryDataLocal({ silent: true });
    if (Number.isFinite(snapshot?.maxTotalRecords) && snapshot.maxTotalRecords > 0) {
      updateTelemetryMaxTotalRecords(snapshot.maxTotalRecords);
    }
    if (!snapshot || !Array.isArray(snapshot.nodes) || !snapshot.nodes.length) {
      telemetrySelectedMeshId = null;
      telemetryUpdatedAt = snapshot?.updatedAt ?? telemetryUpdatedAt ?? null;
      refreshTelemetrySelectors();
      renderTelemetryView();
      updateTelemetryUpdatedAtLabel();
      updateTelemetryStats(snapshot?.stats);
      return;
    }

    for (const node of snapshot.nodes) {
      const meshIdRaw = node?.meshId ?? node?.rawMeshId ?? null;
      const meshKey = resolveTelemetryMeshKey(meshIdRaw);
      if (!meshKey) continue;
      const sanitizedNode = sanitizeTelemetryNodeData(node.node);
      const bucket = {
        meshId: meshKey,
        rawMeshId: node?.rawMeshId || meshIdRaw || meshKey,
        node: sanitizedNode,
        records: [],
        recordIdSet: new Set(),
        loadedRange: null,
        loadedCount: 0,
        totalRecords: Number.isFinite(node?.totalRecords) ? Number(node.totalRecords) : 0,
        metrics: new Set(Array.isArray(node?.metrics) ? node.metrics : []),
        latestSampleMs: Number.isFinite(node?.latestSampleMs) ? Number(node.latestSampleMs) : null,
        earliestSampleMs: Number.isFinite(node?.earliestSampleMs)
          ? Number(node.earliestSampleMs)
          : null,
        partial:
          Number.isFinite(node?.totalRecords) && Number(node.totalRecords) > TELEMETRY_MAX_LOCAL_RECORDS
      };
      telemetryStore.set(meshKey, bucket);
      if (sanitizedNode && sanitizedNode.meshIdNormalized) {
        upsertNodeRegistry(sanitizedNode);
      }
    }

    telemetryUpdatedAt = snapshot.updatedAt ?? Date.now();

    let nextSelection = null;
    if (previousExplicit && telemetryStore.has(previousExplicit)) {
      nextSelection = previousExplicit;
    } else if (previousSelection && telemetryStore.has(previousSelection)) {
      nextSelection = previousSelection;
    }
    telemetrySelectedMeshId = nextSelection;
    telemetryLastExplicitMeshId = nextSelection || null;
    refreshTelemetrySelectors(nextSelection);
    updateTelemetryNodeInputDisplay();
    updateTelemetryUpdatedAtLabel();
    updateTelemetryStats(snapshot.stats);

    if (telemetrySelectedMeshId) {
      loadTelemetryRecordsForSelection(telemetrySelectedMeshId, { force: true });
    } else {
      renderTelemetryView();
    }
  }

  function handleTelemetryAppend(payload) {
    if (!payload) {
      return;
    }
    if (payload.type && payload.type !== 'append') {
      return;
    }
    if (Number.isFinite(payload.maxTotalRecords) && payload.maxTotalRecords > 0) {
      updateTelemetryMaxTotalRecords(payload.maxTotalRecords);
    }
    const meshId = payload.meshId || payload.record?.meshId;
    const record = addTelemetryRecord(meshId, payload.node, payload.record);
    if (!record) {
      return;
    }
    telemetryUpdatedAt =
      Number.isFinite(payload.updatedAt) && payload.updatedAt > 0
        ? Number(payload.updatedAt)
        : Date.now();
    const previousSelection = telemetrySelectedMeshId;
    refreshTelemetrySelectors(previousSelection);
    if (previousSelection && telemetryStore.has(previousSelection)) {
      telemetrySelectedMeshId = previousSelection;
    } else if (telemetrySelectedMeshId && !telemetryStore.has(telemetrySelectedMeshId)) {
      telemetrySelectedMeshId = null;
    }
    updateTelemetryNodeInputDisplay();
    if (record && telemetrySelectedMeshId && normalizeMeshId(telemetrySelectedMeshId) === normalizeMeshId(meshId)) {
      renderTelemetryView();
    }
    updateTelemetryUpdatedAtLabel();
    updateTelemetryStats(payload.stats);
  }

  function handleTelemetryReset(payload) {
    if (!payload) {
      return;
    }
    if (payload.type && payload.type !== 'reset') {
      return;
    }
    if (Number.isFinite(payload.maxTotalRecords) && payload.maxTotalRecords > 0) {
      updateTelemetryMaxTotalRecords(payload.maxTotalRecords);
    }
    telemetryUpdatedAt =
      Number.isFinite(payload.updatedAt) && payload.updatedAt > 0
        ? Number(payload.updatedAt)
        : Date.now();
    clearTelemetryDataLocal({ silent: true });
    refreshTelemetrySelectors();
    renderTelemetryView();
    updateTelemetryUpdatedAtLabel();
    updateTelemetryStats(payload.stats);
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
    const display = formatNodeDisplayLabel(summary.from);
    if (display) {
      return display;
    }
    const mesh = summary.from?.meshId || summary.from?.meshIdNormalized || summary.from?.meshIdOriginal || '';
    return mesh || 'unknown';
  }

  function getDetailExtraSegments(summary) {
    if (!summary || !Array.isArray(summary.extraLines) || !summary.extraLines.length) {
      return [];
    }
    return summary.extraLines
      .map((line) => {
        if (line === null || line === undefined) return '';
        const text = String(line).trim();
        return text ? `<span class="detail-extra">${escapeHtml(text)}</span>` : '';
      })
      .filter(Boolean);
  }

  function formatDetail(summary) {
    const detail = summary.detail || '';
    const segments = [];
    if (detail) {
      segments.push(escapeHtml(detail));
    }
    const extraSegments = getDetailExtraSegments(summary);
    if (extraSegments.length) {
      segments.push(...extraSegments);
    }
    const distanceLabel = formatDistance(summary);
    if (distanceLabel) {
      segments.push(`<span class="detail-distance">${escapeHtml(distanceLabel)}</span>`);
    }
    if (!segments.length) {
      return '';
    }
    return segments.join('<br/>');
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

  function normalizeProvisionCallsignString(value) {
    if (value === null || value === undefined) return null;
    const str = String(value).trim();
    if (!str) {
      return null;
    }
    return str.toUpperCase();
  }

  function tryFormatAprsCallsign(aprs) {
    if (!aprs) return null;
    const callsignCandidates = [
      aprs.callsign_with_ssid,
      aprs.callsignWithSsid,
      aprs.callsign_full,
      aprs.callsignFull,
      aprs.aprs_callsign,
      aprs.aprsCallsign,
      aprs.callsign
    ];
    for (const candidate of callsignCandidates) {
      const normalized = normalizeProvisionCallsignString(candidate);
      if (normalized) {
        return normalized;
      }
    }
    const base =
      normalizeProvisionCallsignString(aprs.callsign_base) ||
      normalizeProvisionCallsignString(aprs.callsignBase);
    if (!base) {
      return null;
    }
    const ssid = normalizeProvisionSsidValue(
      aprs.aprs_ssid ??
        aprs.aprsSsid ??
        aprs.callsign_ssid ??
        aprs.callsignSsid ??
        aprs.ssid
    );
    if (!ssid) {
      return base;
    }
    const baseWithout = base.replace(/-[0-9A-Z]{1,2}$/, '').replace(/-+$/, '');
    return `${baseWithout}-${ssid}`;
  }

  function formatProvisionCallsign(provision, aprsState) {
    const aprsOverride = tryFormatAprsCallsign(aprsState);
    if (aprsOverride) {
      return aprsOverride;
    }
    if (!provision) return '—';
    const callsignWithSsidCandidates = [
      provision.aprs_callsign,
      provision.aprsCallsign,
      provision.callsign_with_ssid,
      provision.callsignWithSsid,
      provision.callsign_full,
      provision.callsignFull
    ];
    for (const candidate of callsignWithSsidCandidates) {
      const normalized = normalizeProvisionCallsignString(candidate);
      if (normalized) {
        return normalized;
      }
    }
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
    updateRelayCellDisplay(tr.cells[2], summary);
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

    hydrateSummaryNodes(summary);

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

    registerFlow(summary);
  }

  function refreshSummaryRows() {
    if (!Array.isArray(summaryRows) || !summaryRows.length) {
      return;
    }
    for (const row of summaryRows) {
      if (!row || !row.__summaryData) continue;
      const summary = row.__summaryData;
      hydrateSummaryNodes(summary);
      const cells = row.children;
      if (!cells || cells.length < 3) {
        continue;
      }
      cells[1].textContent = formatSource(summary);
      updateRelayCellDisplay(cells[2], summary);
    }
  }

  function unwrapPendingFlowSummary(entry) {
    if (!entry || typeof entry !== 'object') {
      return entry || null;
    }
    if (entry.summary && typeof entry.summary === 'object') {
      return entry.summary;
    }
    return entry;
  }

  function deletePendingFlowSummary(meshId, flowId) {
    if (!meshId || !flowId) return false;
    const bucket = pendingFlowSummaries.get(meshId);
    if (!bucket) return false;
    const removed = bucket.delete(flowId);
    if (!removed) return false;
    pendingFlowSummaryCount = Math.max(0, pendingFlowSummaryCount - 1);
    if (!bucket.size) {
      pendingFlowSummaries.delete(meshId);
    }
    return true;
  }

  function trimPendingFlowSummaries() {
    if (!pendingFlowSummaryQueue.length) {
      return;
    }
    const now = Date.now();
    while (pendingFlowSummaryQueue.length) {
      const head = pendingFlowSummaryQueue[0];
      if (!head) {
        pendingFlowSummaryQueue.shift();
        continue;
      }
      const expired = head.insertedAt + PENDING_FLOW_SUMMARY_TTL_MS < now;
      const overLimit = pendingFlowSummaryCount > MAX_PENDING_FLOW_SUMMARIES_TOTAL;
      if (!expired && !overLimit) {
        break;
      }
      pendingFlowSummaryQueue.shift();
      if (!head.meshId || !head.flowId) {
        continue;
      }
      if (!expired && !overLimit) {
        continue;
      }
      deletePendingFlowSummary(head.meshId, head.flowId);
    }
  }

  function addPendingFlowSummary(meshId, flowId, summary) {
    if (!meshId || !flowId || !summary) return;
    let bucket = pendingFlowSummaries.get(meshId);
    const now = Date.now();
    if (!bucket) {
      bucket = new Map();
      pendingFlowSummaries.set(meshId, bucket);
    }
    if (bucket.has(flowId)) {
      bucket.set(flowId, { summary, insertedAt: now });
      return;
    }
    bucket.set(flowId, { summary, insertedAt: now });
    pendingFlowSummaryQueue.push({ meshId, flowId, insertedAt: now });
    pendingFlowSummaryCount += 1;

    while (bucket.size > MAX_PENDING_FLOW_SUMMARIES_PER_MESH) {
      const oldestKey = bucket.keys().next().value;
      if (!oldestKey) {
        break;
      }
      const removed = deletePendingFlowSummary(meshId, oldestKey);
      if (!removed) {
        bucket.delete(oldestKey);
      }
      bucket = pendingFlowSummaries.get(meshId);
      if (!bucket) {
        break;
      }
    }

    trimPendingFlowSummaries();
  }

  function normalizeAprsRecord(record) {
    if (!record || typeof record !== 'object') {
      return null;
    }
    const {
      frame = '',
      payload = '',
      timestampMs = null,
      timestampLabel = ''
    } = record;
    return { frame, payload, timestampMs, timestampLabel };
  }

  function discardPendingAprsRecord(flowId) {
    if (!flowId) return;
    pendingAprsUplinks.delete(flowId);
  }

  function trimPendingAprsRecords() {
    if (!pendingAprsQueue.length) {
      return;
    }
    const now = Date.now();
    while (pendingAprsQueue.length) {
      const head = pendingAprsQueue[0];
      if (!head) {
        pendingAprsQueue.shift();
        continue;
      }
      const entry = pendingAprsUplinks.get(head.flowId);
      if (!entry) {
        pendingAprsQueue.shift();
        continue;
      }
      const isCurrent = entry.cachedAt === head.cachedAt;
      if (!isCurrent) {
        pendingAprsQueue.shift();
        continue;
      }
      const expired = entry.cachedAt + PENDING_APRS_TTL_MS < now;
      const overLimit = pendingAprsUplinks.size > MAX_PENDING_APRS_RECORDS;
      if (!expired && !overLimit) {
        break;
      }
      pendingAprsQueue.shift();
      pendingAprsUplinks.delete(head.flowId);
    }
  }

  function rememberPendingAprsRecord(flowId, record) {
    if (!flowId || !record) return;
    const cachedAt = Date.now();
    const stored = { ...record, cachedAt };
    pendingAprsUplinks.set(flowId, stored);
    pendingAprsQueue.push({ flowId, cachedAt });
    trimPendingAprsRecords();
  }

  function registerFlow(summary, { skipPending = false } = {}) {
    if (!summary) return;
    const type = String(summary.type || '').toLowerCase();
    if (type !== 'position') {
      return;
    }
    const meshId = normalizeMeshId(summary.from?.meshId || summary.from?.meshIdNormalized);
    if (!meshId) return;

    const timestampMs = extractSummaryTimestampMs(summary);
    const flowId =
      typeof summary.flowId === 'string' && summary.flowId.trim()
        ? summary.flowId
        : `${timestampMs}-${Math.random().toString(16).slice(2, 10)}`;
    summary.flowId = flowId;

    const existing = flowEntryIndex.get(flowId);
    if (existing) {
      updateFlowEntryFromSummary(existing, summary);
      repositionFlowEntry(existing);
      renderFlowEntries();
      return;
    }

    const mapping = findMappingByMeshId(meshId);
    if (!mapping) {
      if (!skipPending) {
        const clone = cloneSummaryForPending(summary);
        if (clone) {
          addPendingFlowSummary(meshId, flowId, clone);
        }
      }
      return;
    }

    if (!skipPending) {
      deletePendingFlowSummary(meshId, flowId);
    }

    const entry = buildFlowEntry(summary, {
      meshId,
      mapping,
      timestampMs,
      flowId
    });
    flowEntryIndex.set(flowId, entry);
    insertFlowEntry(entry);

    const aprsRecord = pendingAprsUplinks.get(flowId);
    if (aprsRecord) {
      entry.aprs = normalizeAprsRecord(aprsRecord);
      entry.status = 'aprs';
      discardPendingAprsRecord(flowId);
      trimPendingAprsRecords();
    }

    renderFlowEntries();
  }

  function buildFlowEntry(summary, context) {
    const { meshId, mapping, timestampMs, flowId } = context;
    const mappingLabel = formatMappingLabel(mapping);
    const callsign = formatMappingCallsign(mapping);
    const mappingComment = extractMappingComment(mapping);
    const extras = Array.isArray(summary.extraLines)
      ? summary.extraLines.filter((line) => typeof line === 'string' && line.trim())
      : [];
    const relayLabel = formatRelay(summary);
    let relayGuessed = isRelayGuessed(summary);
    if (relayLabel === '直收' || relayLabel === 'Self') {
      relayGuessed = false;
    }
    const relayGuessReason = relayGuessed ? getRelayGuessReason(summary) : '';
    const hopInfo = extractHopInfo(summary);
    const position = summary.position || {};
    const altitude = resolveAltitudeMeters(position);
    const speedKph = computeSpeedKph(position);
    const satsInView = Number.isFinite(position.satsInView) ? Number(position.satsInView) : null;
    const timestampLabel = summary.timestampLabel || formatFlowTimestamp(timestampMs);

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
      timestampLabel,
      type: summary.type || 'Position',
      icon: resolveFlowIcon(summary.type),
      fromLabel: formatFlowNode(summary.from),
      toLabel: summary.to ? formatFlowNode(summary.to) : null,
      pathLabel: formatFlowPath(summary),
      channel: summary.channel ?? '',
      relayLabel,
      relayGuess: relayGuessed,
      relayGuessReason,
      hopsLabel: hopInfo.hopsLabel || formatHops(summary.hops),
      hopsUsed: hopInfo.usedHops,
      hopsTotal: hopInfo.totalHops,
      snr: Number.isFinite(summary.snr) ? Number(summary.snr) : null,
      rssi: Number.isFinite(summary.rssi) ? Number(summary.rssi) : null,
      mappingLabel,
      callsign,
      comment: mappingComment || summary.detail || extras.join('\n'),
      detail: summary.detail || '',
      extras,
      altitude,
      speedKph,
      satsInView,
      status: 'pending',
      aprs: null,
      relayMeshId: relayMeshIdRaw,
      relayMeshIdNormalized
    };

    if (!entry.comment && extras.length) {
      entry.comment = extras.join('\n');
    }

    return entry;
  }

  function updateFlowEntryFromSummary(entry, summary) {
    const timestampMs = extractSummaryTimestampMs(summary);
    entry.timestampMs = timestampMs;
    entry.timestampLabel = summary.timestampLabel || formatFlowTimestamp(timestampMs);
    entry.type = summary.type || entry.type;
    entry.icon = resolveFlowIcon(entry.type);
    entry.fromLabel = formatFlowNode(summary.from);
    entry.toLabel = summary.to ? formatFlowNode(summary.to) : null;
    entry.pathLabel = formatFlowPath(summary);
    entry.channel = summary.channel ?? entry.channel;
    const relayLabel = formatRelay(summary);
    let relayGuessed = isRelayGuessed(summary);
    if (relayLabel === '直收' || relayLabel === 'Self') {
      relayGuessed = false;
    }
    entry.relayLabel = relayLabel;
    entry.relayGuess = relayGuessed;
    entry.relayGuessReason = relayGuessed ? getRelayGuessReason(summary) : '';
    entry.relayMeshId =
      summary.relay?.meshId ||
      summary.relay?.meshIdNormalized ||
      summary.relayMeshId ||
      summary.relayMeshIdNormalized ||
      entry.relayMeshId ||
      '';
    entry.relayMeshIdNormalized = normalizeMeshId(entry.relayMeshId) || entry.relayMeshIdNormalized || '';
    const hopInfo = extractHopInfo(summary);
    entry.hopsLabel = hopInfo.hopsLabel || formatHops(summary.hops);
    entry.hopsUsed = hopInfo.usedHops;
    entry.hopsTotal = hopInfo.totalHops;
    entry.snr = Number.isFinite(summary.snr) ? Number(summary.snr) : entry.snr;
    entry.rssi = Number.isFinite(summary.rssi) ? Number(summary.rssi) : entry.rssi;
    entry.detail = summary.detail || entry.detail;
    entry.extras = Array.isArray(summary.extraLines)
      ? summary.extraLines.filter((line) => typeof line === 'string' && line.trim())
      : [];
    const position = summary.position || {};
    entry.altitude = resolveAltitudeMeters(position);
    entry.speedKph = computeSpeedKph(position);
    entry.satsInView = Number.isFinite(position.satsInView) ? Number(position.satsInView) : entry.satsInView;

    const mapping = findMappingByMeshId(entry.meshId);
    if (mapping) {
      const nextLabel = formatMappingLabel(mapping);
      const nextCallsign = formatMappingCallsign(mapping);
      const nextComment = extractMappingComment(mapping);
      if (entry.mappingLabel !== nextLabel) {
        entry.mappingLabel = nextLabel;
      }
      if (nextCallsign && entry.callsign !== nextCallsign) {
        entry.callsign = nextCallsign;
      }
      if (nextComment) {
        entry.comment = nextComment;
      }
    }
    if (!entry.comment) {
      entry.comment = summary.detail || entry.extras.join('\n');
    }
  }

  function insertFlowEntry(entry) {
    let inserted = false;
    for (let i = 0; i < flowEntries.length; i += 1) {
      if (Number(entry.timestampMs) >= Number(flowEntries[i].timestampMs)) {
        flowEntries.splice(i, 0, entry);
        inserted = true;
        break;
      }
    }
    if (!inserted) {
      flowEntries.push(entry);
    }
    while (flowEntries.length > FLOW_MAX_ENTRIES) {
      const removed = flowEntries.pop();
      if (removed) {
        flowEntryIndex.delete(removed.flowId);
      }
    }
  }

  function repositionFlowEntry(entry) {
    const index = flowEntries.indexOf(entry);
    if (index >= 0) {
      flowEntries.splice(index, 1);
    }
    insertFlowEntry(entry);
  }

  function renderFlowEntries() {
    if (!flowList || !flowEmptyState) return;
    const term = flowSearchTerm;
    const filtered = flowEntries.filter((entry) => {
      if (flowFilterState === 'aprs' && !entry.aprs) return false;
      if (flowFilterState === 'pending' && entry.aprs) return false;
      if (term && !flowEntryMatches(entry, term)) return false;
      return true;
    });

    flowList.innerHTML = '';
    if (!filtered.length) {
      flowEmptyState.classList.remove('hidden');
      flowList.classList.add('hidden');
      return;
    }

    flowEmptyState.classList.add('hidden');
    flowList.classList.remove('hidden');

    const fragment = document.createDocumentFragment();
    for (const entry of filtered) {
      fragment.appendChild(renderFlowEntry(entry));
    }
    flowList.appendChild(fragment);
  }

  function renderFlowEntry(entry) {
    const primaryLabel = entry.mappingLabel || entry.callsign || entry.pathLabel || entry.fromLabel;
    const item = document.createElement('div');
    item.className = `flow-item ${entry.aprs ? 'flow-item--aprs' : 'flow-item--pending'}`;
    item.dataset.flowId = entry.flowId;

    const header = document.createElement('div');
    header.className = 'flow-item-header';

    const title = document.createElement('div');
    title.className = 'flow-item-title';
    const icon = document.createElement('span');
    icon.className = 'flow-item-icon';
    icon.textContent = entry.icon || '📡';
    const label = document.createElement('span');
    label.textContent = primaryLabel || '未知節點';
    title.append(icon, label);

    const status = document.createElement('span');
    status.className = `flow-item-status ${entry.aprs ? 'flow-item-status--aprs' : 'flow-item-status--pending'}`;
    status.textContent = entry.aprs ? '已上傳 APRS' : '待上傳';

    header.append(title, status);
    item.appendChild(header);

    if (entry.pathLabel) {
      const path = document.createElement('div');
      path.className = 'flow-item-path';
      path.textContent = entry.pathLabel;
      item.appendChild(path);
    }

    const chips = buildFlowChips(entry);
    if (chips.length) {
      const meta = document.createElement('div');
      meta.className = 'flow-item-meta';
      for (const chip of chips) {
        const chipEl = document.createElement('span');
        chipEl.className = 'flow-chip';
        const strong = document.createElement('strong');
        strong.textContent = chip.label;
        chipEl.append(strong, document.createTextNode(` ${chip.value}`));
        if (chip.label === 'Relay' && entry.relayGuessReason) {
          chipEl.title = entry.relayGuessReason;
          chipEl.classList.add('flow-chip-relay-guess');
          if (!chipEl.querySelector('.relay-hint-btn')) {
            const hintBtn = document.createElement('button');
            hintBtn.type = 'button';
            hintBtn.className = 'relay-hint-btn relay-hint-btn--chip';
            hintBtn.textContent = '?';
            hintBtn.title = entry.relayGuessReason;
            hintBtn.setAttribute('aria-label', '顯示推測原因');
            hintBtn.addEventListener('click', (event) => {
              event.stopPropagation();
            openRelayHintDialog({
              reason: entry.relayGuessReason,
              relayLabel: entry.relayLabel || '',
              meshId: entry.relayMeshIdNormalized || entry.relayMeshId || ''
            });
            });
            chipEl.appendChild(hintBtn);
          }
        }
        meta.appendChild(chipEl);
      }
      item.appendChild(meta);
    }

    const commentText =
      entry.comment ||
      entry.detail ||
      (entry.extras && entry.extras.length ? entry.extras.join('\n') : '');
    const comment = document.createElement('div');
    comment.className = 'flow-item-comment';
    if (!commentText) {
      comment.classList.add('empty');
      comment.textContent = '無額外註記';
    } else {
      comment.textContent = commentText;
    }
    item.appendChild(comment);

    if (entry.aprs) {
      const aprsBlock = document.createElement('div');
      aprsBlock.className = 'flow-item-aprs';
      const labelEl = document.createElement('div');
      labelEl.className = 'flow-item-aprs-label';
      labelEl.textContent = `APRS ${entry.aprs.timestampLabel}`;
      const frameEl = document.createElement('div');
      frameEl.className = 'flow-item-aprs-frame';
      frameEl.textContent = entry.aprs.frame || entry.aprs.payload || '';
      aprsBlock.append(labelEl, frameEl);
      item.appendChild(aprsBlock);
    }

    return item;
  }

  function buildFlowChips(entry) {
    const chips = [];
    if (entry.timestampLabel) {
      chips.push({ label: '時間', value: entry.timestampLabel });
    }
    if (entry.callsign && entry.callsign !== entry.mappingLabel) {
      chips.push({ label: '呼號', value: entry.callsign });
    }
    if (entry.channel !== '' && entry.channel !== null && entry.channel !== undefined) {
      chips.push({ label: 'Ch', value: entry.channel });
    }
    if (entry.relayLabel) {
      chips.push({ label: 'Relay', value: entry.relayLabel });
    }
    if (entry.hopsLabel) {
      chips.push({ label: 'Hops', value: entry.hopsLabel });
    } else if (entry.hopsUsed != null || entry.hopsTotal != null) {
      const used = entry.hopsUsed != null ? entry.hopsUsed : '?';
      const total = entry.hopsTotal != null ? entry.hopsTotal : '?';
      chips.push({ label: 'Hops', value: `${used}/${total}` });
    }
    if (Number.isFinite(entry.snr)) {
      chips.push({ label: 'SNR', value: `${entry.snr.toFixed(1)} dB` });
    }
    if (Number.isFinite(entry.rssi)) {
      chips.push({ label: 'RSSI', value: `${entry.rssi.toFixed(0)} dBm` });
    }
    if (Number.isFinite(entry.altitude)) {
      chips.push({ label: 'ALT', value: `${Math.round(entry.altitude)} m` });
    }
    if (Number.isFinite(entry.speedKph)) {
      chips.push({ label: 'SPD', value: `${entry.speedKph.toFixed(1)} km/h` });
    }
    if (Number.isFinite(entry.satsInView)) {
      chips.push({ label: 'SAT', value: `${entry.satsInView}` });
    }
    return chips;
  }

  function flowEntryMatches(entry, term) {
    if (!term) return true;
    const haystack = [
      entry.mappingLabel,
      entry.callsign,
      entry.fromLabel,
      entry.toLabel,
      entry.pathLabel,
      entry.relayLabel,
      entry.comment,
      entry.detail,
      ...(entry.extras || []),
      entry.aprs?.frame,
      entry.aprs?.payload
    ]
      .filter(Boolean)
      .map((value) => String(value).toLowerCase());
    return haystack.some((text) => text.includes(term));
  }

  function cloneSummaryForPending(summary) {
    try {
      const clone = typeof structuredClone === 'function' ? structuredClone(summary) : null;
      if (clone) return clone;
    } catch {
      // ignore structuredClone failure
    }
    try {
      return JSON.parse(JSON.stringify(summary));
    } catch {
      return null;
    }
  }

  function extractSummaryTimestampMs(summary) {
    if (!summary) return Date.now();
    if (Number.isFinite(summary.timestampMs)) {
      return Number(summary.timestampMs);
    }
    if (Number.isFinite(summary.timestamp)) {
      return Number(summary.timestamp);
    }
    if (typeof summary.timestamp === 'string') {
      const parsed = Date.parse(summary.timestamp);
      if (!Number.isNaN(parsed)) {
        return parsed;
      }
    }
    if (typeof summary.timestampLabel === 'string') {
      const parsed = Date.parse(summary.timestampLabel);
      if (!Number.isNaN(parsed)) {
        return parsed;
      }
    }
    return Date.now();
  }

  function formatFlowTimestamp(timestampMs) {
    const date = new Date(timestampMs);
    if (Number.isNaN(date.getTime())) return '—';
    const hh = `${date.getHours()}`.padStart(2, '0');
    const mm = `${date.getMinutes()}`.padStart(2, '0');
    const ss = `${date.getSeconds()}`.padStart(2, '0');
    return `${hh}:${mm}:${ss}`;
  }

  function resolveFlowIcon(type) {
    if (!type) return '📡';
    const lower = String(type).toLowerCase();
    if (lower.includes('position')) return '📍';
    if (lower.includes('telemetry')) return '📈';
    return '📡';
  }

  function formatFlowNode(node) {
    if (!node) return 'unknown';
    const mesh = node.meshId || node.meshIdNormalized || '';
    const meshLabel = mesh ? mesh.toLowerCase() : '';
    const name =
      (node.longName && node.longName !== 'unknown' && node.longName) ||
      (node.shortName && node.shortName !== 'unknown' && node.shortName) ||
      (node.label && node.label !== 'unknown' && node.label) ||
      '';
    if (!name && !meshLabel) {
      return 'unknown';
    }
    if (meshLabel && name && !name.toLowerCase().includes(meshLabel)) {
      return `${name} (${meshLabel})`;
    }
    return name || meshLabel || 'unknown';
  }

  function formatFlowPath(summary) {
    if (!summary) return '';
    const fromLabel = formatFlowNode(summary.from);
    const toLabel = summary.to ? formatFlowNode(summary.to) : '';
    if (toLabel) {
      return `${fromLabel} → ${toLabel}`;
    }
    return fromLabel;
  }

  function computeSpeedKph(position = {}) {
    if (!position) return null;
    if (Number.isFinite(position.speedKph)) {
      return Number(position.speedKph);
    }
    if (Number.isFinite(position.speedMps)) {
      return Number(position.speedMps) * 3.6;
    }
    if (Number.isFinite(position.speedKnots)) {
      return Number(position.speedKnots) * 1.852;
    }
    return null;
  }

  function resolveAltitudeMeters(position = {}) {
    if (!position) return null;
    if (Number.isFinite(position.altitudeMeters)) {
      return Number(position.altitudeMeters);
    }
    if (Number.isFinite(position.altitude)) {
      return Number(position.altitude);
    }
    return null;
  }

  function findMappingByMeshId(meshId) {
    if (!meshId) return null;
    return mappingByMeshId.get(meshId) || null;
  }

  function formatMappingCallsign(mapping) {
    if (!mapping) return null;
    const baseRaw =
      mapping.callsign_base ??
      mapping.callsignBase ??
      mapping.callsign ??
      mapping.base ??
      null;
    if (!baseRaw) return null;
    let base = String(baseRaw).trim().toUpperCase();
    if (!base) return null;
    if (base.endsWith('-')) {
      base = base.slice(0, -1);
    }
    const ssidRaw =
      mapping.aprs_ssid ??
      mapping.aprsSsid ??
      mapping.ssid ??
      mapping.SSID ??
      null;
    const ssidNum = Number(ssidRaw);
    if (Number.isFinite(ssidNum) && ssidNum > 0) {
      const suffix = `-${ssidNum}`;
      if (!base.endsWith(suffix)) {
        base = `${base}${suffix}`;
      }
    }
    return base.replace(/--+/g, '-');
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

  function formatMappingLabel(mapping) {
    if (!mapping) return null;
    const callsign = formatMappingCallsign(mapping) || '';
    const comment = extractMappingComment(mapping) || '';
    if (!comment) {
      return callsign || null;
    }
    if (!callsign) {
      return comment.trim() || null;
    }
    const escaped = callsign.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const pattern = new RegExp(`^${escaped}(?:[\\s·-]+)?`, 'i');
    const trimmed = comment.replace(pattern, '').trim();
    return trimmed || callsign || null;
  }

  function flushPendingFlowsFor(meshId) {
    if (!meshId) return;
    const bucket = pendingFlowSummaries.get(meshId);
    if (!bucket || !bucket.size) return;
    const bucketSize = bucket.size;
    pendingFlowSummaries.delete(meshId);
    pendingFlowSummaryCount = Math.max(0, pendingFlowSummaryCount - bucketSize);
    for (const entry of bucket.values()) {
      const summary = unwrapPendingFlowSummary(entry);
      if (summary) {
        registerFlow(summary, { skipPending: true });
      }
    }
    trimPendingFlowSummaries();
  }

  function refreshFlowEntryLabels() {
    let mutated = false;
    for (const entry of flowEntries) {
      const mapping = findMappingByMeshId(entry.meshId);
      const nextLabel = formatMappingLabel(mapping);
      const nextCallsign = formatMappingCallsign(mapping);
      const nextComment = extractMappingComment(mapping);
      if (entry.mappingLabel !== nextLabel) {
        entry.mappingLabel = nextLabel;
        mutated = true;
      }
      if (nextCallsign && entry.callsign !== nextCallsign) {
        entry.callsign = nextCallsign;
        mutated = true;
      }
      if (nextComment && entry.comment !== nextComment) {
        entry.comment = nextComment;
        mutated = true;
      }
    }
    if (mutated) {
      renderFlowEntries();
    }
  }

  function buildAprsRecord(info) {
    if (!info) return null;
    const rawFrame = info.frame ?? info.payload ?? '';
    const frame = String(rawFrame);
    if (!frame.trim()) return null;
    const timestampSource = info.timestamp ?? info.timestampMs ?? Date.now();
    const timestampMs = Number.isFinite(Number(timestampSource))
      ? Number(timestampSource)
      : Date.now();
    return {
      frame,
      payload: info.payload ? String(info.payload) : '',
      timestampMs,
      timestampLabel: formatFlowTimestamp(timestampMs)
    };
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
    const aprsRecord = buildAprsRecord(info);
    if (aprsRecord) {
      const entry = flowEntryIndex.get(info.flowId);
      if (entry) {
        entry.aprs = normalizeAprsRecord(aprsRecord);
        entry.status = 'aprs';
        discardPendingAprsRecord(info.flowId);
        trimPendingAprsRecords();
        renderFlowEntries();
      } else {
        rememberPendingAprsRecord(info.flowId, normalizeAprsRecord(aprsRecord));
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
    const humanStatus = info.hasKey && !info.degraded ? '正常' : '異常';
    if (callmeshLabel) {
      callmeshLabel.textContent = humanStatus;
    }

    updateAprsStatus(info.aprs);

    if (Array.isArray(info.mappingItems)) {
      mappingMeshIds.clear();
      mappingByMeshId.clear();
      for (const item of info.mappingItems) {
        const meshId = normalizeMeshId(item?.mesh_id ?? item?.meshId);
        if (!meshId) continue;
        mappingMeshIds.add(meshId);
        mappingByMeshId.set(meshId, item);
        flushPendingFlowsFor(meshId);
      }
      refreshSummaryMappingHighlights();
      refreshFlowEntryLabels();
    } else if (info.mappingItems == null) {
      mappingMeshIds.clear();
      mappingByMeshId.clear();
      refreshSummaryMappingHighlights();
      refreshFlowEntryLabels();
    }

    const provision = info.provision || {};
    if (callmeshCallsign) {
      const callsignLabel = formatProvisionCallsign(provision, info.aprs);
      callmeshCallsign.textContent = callsignLabel || '—';
    }
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
    renderNodeDatabase();
    if (selectedChannelId != null) {
      renderChannelMessages(selectedChannelId);
    }
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
        updateRelayCellDisplay(relayCell, summary);
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
          case 'telemetry-snapshot':
            applyTelemetrySnapshot(packet.payload);
            break;
          case 'telemetry-append':
            handleTelemetryAppend(packet.payload);
            break;
          case 'telemetry-reset':
            handleTelemetryReset(packet.payload);
            break;
          case 'aprs':
            markAprsUploaded(packet.payload);
            break;
          case 'node-snapshot':
            applyNodeSnapshot(packet.payload);
            break;
          case 'node':
            handleNodeUpdate(packet.payload);
            break;
          case 'message-snapshot':
            applyMessageSnapshot(packet.payload);
            break;
          case 'message-append':
            handleMessageAppend(packet.payload);
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

  setTelemetryRangeMode(telemetryRangeMode, { skipRender: true });
  renderNodeDatabase();
  connectStream();
})();
