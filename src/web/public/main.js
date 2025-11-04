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
  const nodesTableWrapper = document.getElementById('nodes-table-wrapper');
  const nodesTableBody = document.getElementById('nodes-table-body');
  const nodesEmptyState = document.getElementById('nodes-empty-state');
  const nodesTotalCountLabel = document.getElementById('nodes-total-count');
  const nodesOnlineCountLabel = document.getElementById('nodes-online-count');
  const nodesOnlineTotalLabel = document.getElementById('nodes-online-total');
  const nodesSearchInput = document.getElementById('nodes-search');
  const nodesStatusLabel = document.getElementById('nodes-status');

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
  let telemetryRangeMode = 'day';
  let telemetryCustomRange = { startMs: null, endMs: null };
  let telemetryChartMode = 'all';
  let telemetryChartMetric = null;
  let telemetryUpdatedAt = null;
  const telemetryNodeInputDefaultPlaceholder =
    telemetryNodeInput?.getAttribute('placeholder') || '輸入節點 Mesh ID 或搜尋關鍵字';
  const nodeRegistry = new Map();
  let nodeSnapshotLoaded = false;
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
    callmeshProvisionOpen: 'tmag:web:callmeshProvision:open'
  };

  function safeStorageGet(key) {
    try {
      return window.localStorage?.getItem(key);
    } catch {
      return null;
    }
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
      const fromLabel = latest.from || 'unknown';
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
      appendMessageMeta(meta, `來自：${entry.from}`);
      appendMessageMeta(meta, entry.hops);
      appendMessageMeta(meta, entry.relay);
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
    if (!relayLabel || relayLabel === '未知?' || relayLabel === '?') {
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
    if (!summary || summary.type !== 'Text') {
      return;
    }
    const channelId = Number(summary.channel);
    if (!Number.isFinite(channelId) || channelId < 0) {
      return;
    }
    ensureChannelConfig(channelId);
    const store = ensureChannelStore(channelId);

    const text = formatMessageText(summary);
    const fromLabel = formatNodeDisplayLabel(summary.from) || 'unknown';
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
            relay: entry.relay,
            relayMeshId: entry.relayMeshId,
            relayMeshIdNormalized: entry.relayMeshIdNormalized,
            hops: entry.hops,
            timestampMs: entry.timestampMs,
            timestampLabel: entry.timestampLabel,
            flowId: entry.flowId
          };
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
      entry.roleLabel
    ];
    return fields.some((value) => {
      if (!value) return false;
      return String(value).toLowerCase().includes(lowerTerm);
    });
  }

  function getSortedNodeRegistryEntries() {
    const entries = Array.from(nodeRegistry.values());
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
    const totalNodes = Number.isFinite(stats.totalNodes) ? stats.totalNodes : nodeRegistry.size;
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

  function isRelayGuessed(summary) {
    return Boolean(summary?.relay?.guessed || summary?.relayGuess);
  }

  function getRelayGuessReason(summary) {
    return summary?.relayGuessReason || RELAY_GUESS_EXPLANATION;
  }

  function ensureRelayGuessSuffix(label, summary) {
    if (!isRelayGuessed(summary)) {
      return label;
    }
    const value = label || '';
    if (!value) {
      return '?';
    }
    return value.endsWith('?') ? value : `${value}?`;
  }

  function formatRelay(summary) {
    if (!summary) return '直收';
    const fromMeshId = summary.from?.meshId || summary.from?.meshIdNormalized || '';
    const relayMeshRaw =
      summary.relay?.meshId ||
      summary.relay?.meshIdNormalized ||
      summary.relayMeshId ||
      summary.relayMeshIdNormalized ||
      '';
    const relayNormalized = normalizeMeshId(relayMeshRaw);
    const fromNormalized = normalizeMeshId(fromMeshId);
    const rawRelayCanonical = relayMeshRaw.startsWith('!') ? relayMeshRaw.slice(1) : relayMeshRaw;

    if (fromMeshId && isSelfMesh(fromMeshId, summary)) {
      return ensureRelayGuessSuffix('Self', summary);
    }

    if (relayMeshRaw && isSelfMesh(relayMeshRaw, summary)) {
      return ensureRelayGuessSuffix('Self', summary);
    }

    const relayNode = relayMeshRaw ? hydrateSummaryNode(summary.relay, relayMeshRaw) : null;
    if (relayNode) {
      summary.relay = relayNode;
    }

    let relayMeshDisplay = relayNode?.meshId || relayMeshRaw;
    let relayNormWork = relayNode?.meshIdNormalized || relayNormalized;
    if (relayMeshRaw && /^0{6}[0-9a-fA-F]{2}$/.test(rawRelayCanonical.toLowerCase())) {
      relayMeshDisplay = '';
      relayNormWork = null;
    }

    if (fromNormalized && relayNormWork && fromNormalized === relayNormWork) {
      return ensureRelayGuessSuffix('直收', summary);
    }

    const hopInfo = extractHopInfo(summary);
    const usedHops = Number.isFinite(hopInfo.usedHops) ? hopInfo.usedHops : null;
    const hopsLabel = hopInfo.hopsLabel || '';
    const zeroHop = usedHops === 0 || /^0(?:\s*\/|$)/.test(hopsLabel);

    if (summary.relay) {
      if (zeroHop) {
        return ensureRelayGuessSuffix('直收', summary);
      }
      return ensureRelayGuessSuffix(formatRelayLabel(summary.relay), summary);
    }

    if (relayMeshDisplay) {
      if (zeroHop) {
        return ensureRelayGuessSuffix('直收', summary);
      }
    return ensureRelayGuessSuffix(
      formatRelayLabel({ label: summary.relay?.label || relayMeshDisplay, meshId: relayMeshDisplay }),
      summary
    );
  }

    if (zeroHop) {
      return ensureRelayGuessSuffix('直收', summary);
    }

    if (usedHops != null && usedHops > 0) {
      return ensureRelayGuessSuffix('未知?', summary);
    }

    if (!hopsLabel) {
      return ensureRelayGuessSuffix('直收', summary);
    }

    if (hopsLabel.includes('?')) {
      return ensureRelayGuessSuffix('未知?', summary);
    }

    return ensureRelayGuessSuffix('', summary);
  }

  function updateRelayCellDisplay(cell, summary) {
    if (!cell) return;
    const label = formatRelay(summary);
    cell.textContent = label;

    let relayMeshRaw =
      summary?.relay?.meshId ||
      summary?.relay?.meshIdNormalized ||
      summary?.relayMeshId ||
      summary?.relayMeshIdNormalized ||
      '';
    let relayTitle = '';
    if (typeof relayMeshRaw === 'string' && relayMeshRaw) {
      if (relayMeshRaw.startsWith('0x')) {
        relayMeshRaw = `!${relayMeshRaw.slice(2)}`;
      }
      if (label && relayMeshRaw && label !== relayMeshRaw) {
        relayTitle = `${label} (${relayMeshRaw})`;
      } else {
        relayTitle = relayMeshRaw;
      }
    } else if (label === '直收') {
      relayTitle = '訊息為直收，未經其他節點轉發';
    } else if (label === 'Self') {
      relayTitle = '本站節點轉發';
    } else if (label && label.includes('?')) {
      relayTitle = '最後轉發節點未知或標號不完整';
    }

    if (isRelayGuessed(summary)) {
      const reason = getRelayGuessReason(summary);
      cell.classList.add('relay-guess');
      cell.dataset.relayGuess = 'true';
      relayTitle = relayTitle ? `${relayTitle}\n${reason}` : reason;
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
      return fallback ? (fallback.includes('?') ? fallback : `${fallback}?`) : '?';
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
    if (telemetryRecordIds.has(record.id)) {
      return null;
    }
    const key = meshKey;
    let bucket = telemetryStore.get(key);
    if (!bucket) {
      bucket = {
        meshId: key,
        rawMeshId,
        node: null,
        records: []
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
    telemetryRecordIds.add(record.id);
    bucket.records.push(record);
    bucket.records.sort((a, b) => a.sampleTimeMs - b.sampleTimeMs);
    while (bucket.records.length > TELEMETRY_MAX_LOCAL_RECORDS) {
      const removed = bucket.records.shift();
      if (removed?.id) {
        telemetryRecordIds.delete(removed.id);
      }
    }
    return record;
  }

  function clearTelemetryDataLocal({ silent = false } = {}) {
    telemetryStore.clear();
    telemetryRecordIds.clear();
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
          return { startMs: end, endMs: start };
        }
        return { startMs: start, endMs: end };
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

  function renderTelemetryDropdown() {
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
    const fragment = document.createDocumentFragment();
    let activeOption = null;
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
    renderTelemetryView();
    if (hideDropdown) {
      hideTelemetryDropdown();
    } else {
      renderTelemetryDropdown();
    }
  }

  function handleTelemetryNodeNavigationKey(event) {
    if (!telemetryNodeInput || telemetryNodeInput.disabled) {
      return;
    }
    renderTelemetryDropdown();
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
    const previous = telemetrySelectedMeshId;
    const searchActive = Boolean(telemetrySearchRaw);
    const { startMs, endMs } = getTelemetryRangeWindow();
    const nodes = [];
    for (const bucket of telemetryStore.values()) {
      if (!bucket || !Array.isArray(bucket.records) || !bucket.records.length) {
        continue;
      }
      const metricsAny = new Set();
      const metricsInRange = new Set();
      let latestTimeAny = null;
      let latestTimeInRange = null;
      for (const record of bucket.records) {
        const metrics = record.telemetry?.metrics;
        if (!metrics || typeof metrics !== 'object') {
          continue;
        }
        const metricKeys = Object.keys(metrics);
        if (!metricKeys.length) {
          continue;
        }
        for (const key of metricKeys) {
          metricsAny.add(key);
        }
        const time = Number(record.sampleTimeMs);
        if (!Number.isFinite(time)) {
          continue;
        }
        if (latestTimeAny == null || time > latestTimeAny) {
          latestTimeAny = time;
        }
        if (startMs != null && time < startMs) {
          continue;
        }
        if (endMs != null && time > endMs) {
          continue;
        }
        for (const key of metricKeys) {
          metricsInRange.add(key);
        }
        if (latestTimeInRange == null || time > latestTimeInRange) {
          latestTimeInRange = time;
        }
      }
      if (!metricsAny.size) {
        continue;
      }
      const meshKey = bucket.meshId || resolveTelemetryMeshKey(bucket.rawMeshId);
      const labelBase = formatTelemetryNodeLabel(meshKey, bucket.node);
      const hasInRange = metricsInRange.size > 0;
      const displayCount = hasInRange ? metricsInRange.size : metricsAny.size;
      const latestMs = Number.isFinite(latestTimeInRange)
        ? latestTimeInRange
        : Number.isFinite(latestTimeAny)
          ? latestTimeAny
          : null;
      nodes.push({
        meshId: meshKey,
        rawMeshId: bucket.rawMeshId || meshKey,
        label: hasInRange ? labelBase : `${labelBase}（區間無資料）`,
        baseLabel: labelBase,
        count: displayCount,
        hasInRange,
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
        telemetryNodeInput.placeholder = '所選區間無遙測資料';
      }
      return;
    }

    telemetryNodeInput.disabled = false;
    telemetryNodeInput.placeholder = telemetryNodeInputDefaultPlaceholder;

    nodes.sort((a, b) => {
      if (a.hasInRange !== b.hasInRange) {
        return a.hasInRange ? -1 : 1;
      }
      const aTime = Number.isFinite(a.latestMs) ? a.latestMs : -Infinity;
      const bTime = Number.isFinite(b.latestMs) ? b.latestMs : -Infinity;
      if (bTime !== aTime) {
        return bTime - aTime;
      }
      if (b.count !== a.count) {
        return b.count - a.count;
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
      renderTelemetryDropdown();
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
    const previousRaw = resolveMeshId(previous);

    let nextSelection = previousRaw;
    if (preferredRaw && candidateMeshIds.includes(preferredRaw)) {
      nextSelection = preferredRaw;
    }
    if (nextSelection && !candidateMeshIds.includes(nextSelection)) {
      nextSelection = null;
    }
    if (!nextSelection && candidateMeshIds.length) {
      nextSelection = candidateMeshIds[0];
    }
    telemetrySelectedMeshId = nextSelection;
    telemetryLastExplicitMeshId = nextSelection;
    telemetrySearchRaw = '';
    telemetrySearchTerm = '';
    updateTelemetryNodeInputDisplay();
    renderTelemetryDropdown();
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
    const hasBase = baseRecords.length > 0;
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
      const nodeInfo = sanitizeTelemetryNodeData(node.node);
      const records = Array.isArray(node.records) ? node.records : [];
      for (const rawRecord of records) {
        addTelemetryRecord(meshId, nodeInfo, rawRecord);
      }
    }
    telemetryUpdatedAt = snapshot.updatedAt ?? Date.now();
    refreshTelemetrySelectors(previousSelection);
    if (previousSelection && telemetryStore.has(previousSelection)) {
      telemetrySelectedMeshId = previousSelection;
      telemetryLastExplicitMeshId = previousSelection;
      updateTelemetryNodeInputDisplay();
      renderTelemetryDropdown();
    }
    renderTelemetryView();
    updateTelemetryUpdatedAtLabel();
    updateTelemetryStats(snapshot.stats);
  }

  function handleTelemetryAppend(payload) {
    if (!payload) {
      return;
    }
    if (payload.type && payload.type !== 'append') {
      return;
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
    refreshTelemetrySelectors(previousSelection || meshId);
    if (previousSelection && telemetryStore.has(previousSelection)) {
      telemetrySelectedMeshId = previousSelection;
      telemetryLastExplicitMeshId = previousSelection;
    } else if (!telemetrySelectedMeshId && meshId) {
      telemetrySelectedMeshId = meshId;
      telemetryLastExplicitMeshId = meshId;
    }
    updateTelemetryNodeInputDisplay();
    renderTelemetryDropdown();
    if (telemetrySelectedMeshId === meshId) {
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
    if (isRelayGuessed(summary)) {
      segments.push(`<span class="detail-extra">${escapeHtml(getRelayGuessReason(summary))}</span>`);
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
        let bucket = pendingFlowSummaries.get(meshId);
        if (!bucket) {
          bucket = new Map();
          pendingFlowSummaries.set(meshId, bucket);
        }
        if (!bucket.has(flowId)) {
          const clone = cloneSummaryForPending(summary);
          if (clone) {
            bucket.set(flowId, clone);
            while (bucket.size > 25) {
              const oldestKey = bucket.keys().next().value;
              if (oldestKey) {
                bucket.delete(oldestKey);
              } else {
                break;
              }
            }
          }
        }
      }
      return;
    }

    if (!skipPending) {
      const bucket = pendingFlowSummaries.get(meshId);
      if (bucket) {
        bucket.delete(flowId);
        if (!bucket.size) {
          pendingFlowSummaries.delete(meshId);
        }
      }
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
      entry.aprs = aprsRecord;
      entry.status = 'aprs';
      pendingAprsUplinks.delete(flowId);
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
    const relayGuessed = isRelayGuessed(summary);
    const relayGuessReason = relayGuessed ? getRelayGuessReason(summary) : '';
    const hopInfo = extractHopInfo(summary);
    const position = summary.position || {};
    const altitude = resolveAltitudeMeters(position);
    const speedKph = computeSpeedKph(position);
    const satsInView = Number.isFinite(position.satsInView) ? Number(position.satsInView) : null;
    const timestampLabel = summary.timestampLabel || formatFlowTimestamp(timestampMs);

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
      aprs: null
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
    entry.relayLabel = formatRelay(summary);
    entry.relayGuess = isRelayGuessed(summary);
    entry.relayGuessReason = entry.relayGuess ? getRelayGuessReason(summary) : '';
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
    pendingFlowSummaries.delete(meshId);
    for (const summary of bucket.values()) {
      registerFlow(summary, { skipPending: true });
    }
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
        entry.aprs = aprsRecord;
        entry.status = 'aprs';
        renderFlowEntries();
      } else {
        pendingAprsUplinks.set(info.flowId, aprsRecord);
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

  renderNodeDatabase();
  connectStream();
})();
