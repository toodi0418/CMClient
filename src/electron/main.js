'use strict';

const electronModule = require('electron');

if (
  !electronModule ||
  typeof electronModule !== 'object' ||
  typeof electronModule.app === 'undefined'
) {
  // 若以 Node 方式啟動（常見於 ELECTRON_RUN_AS_NODE=1），自動重新以 GUI 模式啟動。
  const { spawnSync } = require('child_process');
  const electronBinary =
    typeof electronModule === 'string' && electronModule.length
      ? electronModule
      : process.execPath;
  const env = { ...process.env };
  delete env.ELECTRON_RUN_AS_NODE;
  const result = spawnSync(
    electronBinary,
    [__filename, ...process.argv.slice(2)],
    { stdio: 'inherit', env }
  );
  if (result.error) {
    console.error('重新啟動 Electron 失敗:', result.error);
    process.exit(1);
  }
  process.exit(result.status ?? 0);
}

const { app, BrowserWindow, ipcMain, Menu } = electronModule;
const path = require('path');
const fs = require('fs/promises');
const fsSync = require('fs');
const { version: appVersion } = require('../../package.json');
const MeshtasticClient = require('../meshtasticClient');
const { discoverMeshtasticDevices } = require('../discovery');
const { CallMeshAprsBridge } = require('../callmesh/aprsBridge');
const { WebDashboardServer } = require('../web/server');
const { SerialPort } = require('serialport');

function parseEnvBoolean(value, defaultValue = false) {
  if (value === undefined || value === null) {
    return defaultValue;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) {
    return defaultValue;
  }
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
    return false;
  }
  return defaultValue;
}

const HEARTBEAT_INTERVAL_MS = 60_000;
const MESSAGE_LOG_FILENAME = 'message-log.jsonl';
const MESSAGE_MAX_PER_CHANNEL = 200;

const messageStore = new Map();
let messageWritePromise = Promise.resolve();
let bridgeSummaryListener = null;

function getMessageLogPath() {
  return path.join(getCallMeshDataDir(), MESSAGE_LOG_FILENAME);
}

function resolveTelemetryMaxTotalRecords() {
  const raw = process.env.TMAG_WEB_TELEMETRY_MAX_TOTAL;
  if (!raw) {
    return undefined;
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    console.warn(`TMAG_WEB_TELEMETRY_MAX_TOTAL 必須為正整數，已忽略：${raw}`);
    return undefined;
  }
  return Math.floor(value);
}

function sanitizeMessageNodeForPersist(node) {
  if (!node || typeof node !== 'object') {
    return null;
  }
  return {
    meshId: node.meshId ?? null,
    meshIdNormalized: node.meshIdNormalized ?? null,
    meshIdOriginal: node.meshIdOriginal ?? null,
    longName: node.longName ?? null,
    shortName: node.shortName ?? null,
    label: node.label ?? null
  };
}

function sanitizeHopsForPersist(hops) {
  if (!hops || typeof hops !== 'object') {
    return null;
  }
  return {
    start: Number.isFinite(hops.start) ? Number(hops.start) : null,
    limit: Number.isFinite(hops.limit) ? Number(hops.limit) : null,
    label: typeof hops.label === 'string' ? hops.label : null
  };
}

function sanitizeMessageSummary(summary) {
  if (!summary || typeof summary !== 'object') {
    return null;
  }
  const type = typeof summary.type === 'string' ? summary.type.trim().toLowerCase() : '';
  if (type !== 'text') {
    return null;
  }
  const channelId = Number(summary.channel);
  if (!Number.isFinite(channelId) || channelId < 0) {
    return null;
  }
  const timestampMs = Number.isFinite(summary.timestampMs) ? Number(summary.timestampMs) : Date.now();
  const flowIdRaw = typeof summary.flowId === 'string' && summary.flowId.trim()
    ? summary.flowId.trim()
    : `${channelId}-${timestampMs}-${Math.random().toString(16).slice(2, 10)}`;

  return {
    type: 'Text',
    channel: channelId,
    detail: typeof summary.detail === 'string' ? summary.detail : '',
    extraLines: Array.isArray(summary.extraLines)
      ? summary.extraLines.filter((line) => typeof line === 'string' && line.trim())
      : [],
    from: sanitizeMessageNodeForPersist(summary.from),
    relay: sanitizeMessageNodeForPersist(summary.relay),
    relayMeshId: summary.relay?.meshId ?? summary.relayMeshId ?? null,
    relayMeshIdNormalized: summary.relay?.meshIdNormalized ?? summary.relayMeshIdNormalized ?? null,
    hops: sanitizeHopsForPersist(summary.hops),
    timestampMs,
    timestampLabel:
      typeof summary.timestampLabel === 'string' && summary.timestampLabel.trim()
        ? summary.timestampLabel.trim()
        : new Date(timestampMs).toISOString(),
    flowId: flowIdRaw,
    meshPacketId: Number.isFinite(summary.meshPacketId) ? Number(summary.meshPacketId) : null,
    replyId: Number.isFinite(summary.replyId) ? Number(summary.replyId) : null,
    replyTo: typeof summary.replyTo === 'string' ? summary.replyTo : null,
    scope: typeof summary.scope === 'string' ? summary.scope : null,
    synthetic: Boolean(summary.synthetic)
  };
}

function cloneMessageEntry(entry) {
  if (!entry || typeof entry !== 'object') {
    return null;
  }
  return {
    ...entry,
    extraLines: Array.isArray(entry.extraLines) ? [...entry.extraLines] : [],
    from: entry.from ? { ...entry.from } : null,
    relay: entry.relay ? { ...entry.relay } : null,
    hops: entry.hops ? { ...entry.hops } : null
  };
}

function addMessageEntry(entry) {
  const channelId = entry.channel;
  if (!messageStore.has(channelId)) {
    messageStore.set(channelId, []);
  }
  const list = messageStore.get(channelId);
  const existingIndex = list.findIndex((item) => item.flowId === entry.flowId);
  if (existingIndex !== -1) {
    list.splice(existingIndex, 1);
  }
  list.unshift(entry);
  while (list.length > MESSAGE_MAX_PER_CHANNEL) {
    list.pop();
  }
}

async function flushMessageLog() {
  const channelEntries = Array.from(messageStore.entries()).sort((a, b) => a[0] - b[0]);
  const orderedEntries = [];
  for (const [, list] of channelEntries) {
    for (let i = list.length - 1; i >= 0; i -= 1) {
      orderedEntries.push(list[i]);
    }
  }
  const store = bridge?.getDataStore?.();
  if (store) {
    try {
      store.saveMessageLog(orderedEntries);
      await fs.rm(getMessageLogPath(), { force: true });
      return;
    } catch (err) {
      console.error('寫入訊息紀錄失敗 (SQLite):', err);
    }
  }
  const filePath = getMessageLogPath();
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const lines = orderedEntries.map((entry) => JSON.stringify(entry));
  await fs.writeFile(filePath, lines.join('\n'), 'utf8');
}

function persistMessageSummary(summary) {
  const entry = sanitizeMessageSummary(summary);
  if (!entry) {
    return null;
  }
  addMessageEntry(entry);
  messageWritePromise = messageWritePromise
    .then(() => flushMessageLog())
    .catch((err) => {
      console.error('寫入訊息紀錄失敗:', err);
    });
  return entry;
}

function getMessageSnapshot() {
  const snapshot = {};
  messageStore.forEach((list, channelId) => {
    snapshot[channelId] = list.map((entry) => cloneMessageEntry(entry)).filter(Boolean);
  });
  return snapshot;
}

async function loadMessageLog() {
  const filePath = getMessageLogPath();
  messageStore.clear();
  const store = bridge?.getDataStore?.();
  if (store) {
    try {
      const entries = store.loadMessageLog();
      if (Array.isArray(entries) && entries.length) {
        for (const rawEntry of entries) {
          const entry = sanitizeMessageSummary(rawEntry);
          if (entry) {
            addMessageEntry(entry);
          }
        }
        await fs.rm(filePath, { force: true });
        return;
      }
    } catch (err) {
      console.warn('從 SQLite 載入訊息紀錄失敗:', err.message);
    }
  }
  const migratedEntries = [];
  try {
    const content = await fs.readFile(filePath, 'utf8');
    if (!content) {
      return;
    }
    const lines = content.split(/\n+/).filter((line) => line.trim().length > 0);
    for (const line of lines) {
      try {
        const parsed = JSON.parse(line);
        if (parsed && typeof parsed === 'object') {
          const entry = sanitizeMessageSummary(parsed);
          if (entry) {
            addMessageEntry(entry);
            migratedEntries.push(entry);
          }
        }
      } catch (err) {
        console.warn('跳過無法解析的訊息紀錄:', err.message);
      }
    }
  } catch (err) {
    if (err?.code !== 'ENOENT') {
      console.warn('載入訊息紀錄失敗:', err.message);
    }
  }
  if (store && migratedEntries.length) {
    try {
      store.saveMessageLog(migratedEntries);
      await fs.rm(filePath, { force: true });
    } catch (err) {
      console.warn('遷移訊息紀錄至 SQLite 失敗:', err.message);
    }
  }
}

let mainWindow = null;
let client = null;
let bridge = null;
let callmeshRestoreAllowed = true;
let lastCallmeshStateSnapshot = null;
let cachedClientPreferences = null;
let webServer = null;
let lastMeshtasticStatus = { status: 'idle' };

process.on('uncaughtException', (err) => {
  console.error('未攔截的例外:', err);
});

process.on('unhandledRejection', (reason) => {
  console.error('未處理的 Promise 拒絕:', reason);
});

function getCallMeshDataDir() {
  return path.join(app.getPath('userData'), 'callmesh');
}

function getSkipEnvSentinelPath() {
  return path.join(getCallMeshDataDir(), '.skip-env-key');
}

function getVerificationFilePath() {
  return path.join(getCallMeshDataDir(), 'verification.json');
}

function getClientPreferencesPath() {
  return path.join(getCallMeshDataDir(), 'client-preferences.json');
}

function shouldIncludeEnvApiKey() {
  try {
    if (process.env.CALLMESH_API_KEY || process.env.CALLMESH_VERIFICATION_FILE) {
      return false;
    }
    return !fsSync.existsSync(getSkipEnvSentinelPath());
  } catch {
    return true;
  }
}

async function markSkipEnvKey() {
  const filePath = getSkipEnvSentinelPath();
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, '1', 'utf8');
}

async function clearSkipEnvKey() {
  await fs.rm(getSkipEnvSentinelPath(), { force: true });
}

async function loadPersistedApiKey() {
  try {
    const payload = await loadJsonSafe(getVerificationFilePath());
    const key = payload?.apiKey;
    if (typeof key === 'string' && key.trim()) {
      return key.trim();
    }
    return null;
  } catch (err) {
    console.warn('無法載入先前儲存的 CallMesh API Key:', err);
    return null;
  }
}

async function persistVerifiedApiKey(key) {
  const trimmed = typeof key === 'string' ? key.trim() : '';
  if (!trimmed) {
    await clearPersistedApiKey();
    return;
  }
  const filePath = getVerificationFilePath();
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const payload = {
    apiKey: trimmed,
    savedAt: new Date().toISOString(),
    version: 1
  };
  await fs.writeFile(filePath, JSON.stringify(payload, null, 2), {
    encoding: 'utf8',
    mode: 0o600
  });
}

async function clearPersistedApiKey() {
  await fs.rm(getVerificationFilePath(), { force: true });
}

function normalizeClientPreferences(raw) {
  const normalized = {};
  if (raw && typeof raw === 'object') {
    if (typeof raw.host === 'string') {
      const trimmed = raw.host.trim();
      if (trimmed) {
        normalized.host = trimmed;
      }
    }
    if (Object.prototype.hasOwnProperty.call(raw, 'webDashboardEnabled')) {
      normalized.webDashboardEnabled = Boolean(raw.webDashboardEnabled);
    }
    if (typeof raw.connectionMode === 'string') {
      const mode = raw.connectionMode.trim().toLowerCase();
      if (mode === 'serial' || mode === 'tcp') {
        normalized.connectionMode = mode;
      }
    }
    if (typeof raw.serialPath === 'string') {
      const trimmedPath = raw.serialPath.trim();
      if (trimmedPath) {
        normalized.serialPath = trimmedPath;
      }
    }
    if (Object.prototype.hasOwnProperty.call(raw, 'shareWithTenmanMap')) {
      if (raw.shareWithTenmanMap === null) {
        normalized.shareWithTenmanMap = null;
      } else {
        normalized.shareWithTenmanMap = Boolean(raw.shareWithTenmanMap);
      }
    }
  }
  return normalized;
}

async function getCachedClientPreferences() {
  if (cachedClientPreferences) {
    return cachedClientPreferences;
  }
  const payload = await loadJsonSafe(getClientPreferencesPath());
  cachedClientPreferences = normalizeClientPreferences(payload);
  return cachedClientPreferences;
}

async function writeClientPreferences(preferences) {
  const normalized = normalizeClientPreferences(preferences);
  if (normalized.connectionMode !== 'serial') {
    delete normalized.serialPath;
  }
  const filePath = getClientPreferencesPath();
  if (!Object.keys(normalized).length) {
    await fs.rm(filePath, { force: true });
    cachedClientPreferences = {};
    return cachedClientPreferences;
  }
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const payload = {
    ...normalized,
    version: 1,
    savedAt: new Date().toISOString()
  };
  await fs.writeFile(filePath, JSON.stringify(payload, null, 2), {
    encoding: 'utf8',
    mode: 0o600
  });
  cachedClientPreferences = { ...normalized };
  return cachedClientPreferences;
}

async function updateClientPreferences(updates = {}) {
  if (!updates || typeof updates !== 'object') {
    return getCachedClientPreferences();
  }
  const existing = { ...(await getCachedClientPreferences()) };
  if (Object.prototype.hasOwnProperty.call(updates, 'host')) {
    const value = updates.host;
    const trimmed = typeof value === 'string' ? value.trim() : '';
    if (trimmed) {
      existing.host = trimmed;
    } else {
      delete existing.host;
    }
  }
  if (Object.prototype.hasOwnProperty.call(updates, 'webDashboardEnabled')) {
    existing.webDashboardEnabled = Boolean(updates.webDashboardEnabled);
  }
  if (Object.prototype.hasOwnProperty.call(updates, 'connectionMode')) {
    const modeValue =
      typeof updates.connectionMode === 'string'
        ? updates.connectionMode.trim().toLowerCase()
        : '';
    if (modeValue === 'serial' || modeValue === 'tcp') {
      existing.connectionMode = modeValue;
    } else {
      delete existing.connectionMode;
    }
    if (modeValue !== 'serial') {
      delete existing.serialPath;
    }
  }
  if (Object.prototype.hasOwnProperty.call(updates, 'serialPath')) {
    const pathValue =
      typeof updates.serialPath === 'string' ? updates.serialPath.trim() : '';
    if (pathValue && (existing.connectionMode === 'serial')) {
      existing.serialPath = pathValue;
    } else if (!pathValue) {
      delete existing.serialPath;
    }
  }
  if (Object.prototype.hasOwnProperty.call(updates, 'shareWithTenmanMap')) {
    const rawValue = updates.shareWithTenmanMap;
    if (rawValue === null) {
      delete existing.shareWithTenmanMap;
      bridge?.setTenmanShareEnabled?.(null);
    } else {
      const desired = Boolean(rawValue);
      existing.shareWithTenmanMap = desired;
      bridge?.setTenmanShareEnabled?.(desired);
    }
  }
  return writeClientPreferences(existing);
}

async function loadJsonSafe(filePath) {
  try {
    const content = await fs.readFile(filePath, 'utf8');
    return JSON.parse(content);
  } catch (err) {
    if (err && err.code !== 'ENOENT') {
      console.warn(`讀取 ${filePath} 失敗: ${err.message}`);
    }
    return null;
  }
}

function toRendererCallmeshState(state) {
  return {
    statusText: state.lastStatus,
    agent: state.agent,
    hasKey: Boolean(state.verified && state.apiKey),
    verified: Boolean(state.verified),
    verifiedKey: state.verifiedKey || '',
    degraded: state.degraded,
    lastHeartbeatAt: state.lastHeartbeatAt,
    lastMappingHash: state.lastMappingHash,
    lastMappingSyncedAt: state.lastMappingSyncedAt,
    provision: state.provision,
    mappingItems: state.mappingItems,
    aprs: {
      server: state.aprs?.server ?? null,
      port: state.aprs?.port ?? null,
      callsign: state.aprs?.callsign ?? null,
      connected: Boolean(state.aprs?.connected),
      actualServer: state.aprs?.actualServer ?? null,
      beaconIntervalMs: state.aprs?.beaconIntervalMs ?? null
    }
  };
}

function sendCallmeshStateToRenderer(state) {
  const payload = toRendererCallmeshState(state);
  if (!payload) return;
  if (mainWindow) {
    mainWindow.webContents.send('callmesh:status', payload);
  }
  webServer?.publishCallmesh(payload);
}

function sendCallmeshLog(entry) {
  if (mainWindow) {
    mainWindow.webContents.send('callmesh:log', entry);
  }
  webServer?.publishLog(entry);
}

function sendAprsUplink(info) {
  if (!info) return;
  if (mainWindow) {
    mainWindow.webContents.send('meshtastic:aprs-uplink', info);
  }
  webServer?.publishAprs(info);
}

function sendTelemetryUpdate(payload) {
  if (!payload) return;
  if (mainWindow) {
    mainWindow.webContents.send('telemetry:update', payload);
  }
  webServer?.publishTelemetry(payload);
}

function sendNodeInfo(payload) {
  if (!payload) return;
  if (mainWindow) {
    mainWindow.webContents.send('meshtastic:node', payload);
  }
  webServer?.publishNode(payload);
}

async function syncWebTelemetrySnapshot() {
  if (!bridge || !webServer || typeof bridge.getTelemetrySummary !== 'function') {
    return;
  }
  try {
    const summary = bridge.getTelemetrySummary();
    webServer.seedTelemetrySummary(summary);
  } catch (err) {
    console.warn('同步 Web Telemetry 摘要失敗:', err);
  }
}

async function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js')
    },
    autoHideMenuBar: true
  });

  Menu.setApplicationMenu(null);
  mainWindow.setMenuBarVisibility(false);

  await mainWindow.loadFile(path.join(__dirname, 'index.html'));
  mainWindow.setTitle(`TMMARC Meshtastic APRS Gateway (TMAG) v${appVersion}`);

  if (lastCallmeshStateSnapshot) {
    sendCallmeshStateToRenderer(lastCallmeshStateSnapshot);
  }
  if (bridge) {
    const nodes = bridge.getNodeSnapshot();
    if (Array.isArray(nodes)) {
      mainWindow.webContents.send('meshtastic:node-snapshot', nodes);
    }
  }
}

function setupBridgeListeners() {
  if (!bridge) return;
  bridge.removeAllListeners('state');
  bridge.removeAllListeners('log');
  bridge.removeAllListeners('aprs-uplink');
  bridge.removeAllListeners('telemetry');
  bridge.removeAllListeners('node');

  bridge.on('state', (state) => {
    lastCallmeshStateSnapshot = state;
    sendCallmeshStateToRenderer(state);
  });

  bridge.on('log', (entry) => {
    sendCallmeshLog(entry);
  });

  bridge.on('aprs-uplink', (info) => {
    sendAprsUplink(info);
  });

  bridge.on('telemetry', (payload) => {
    sendTelemetryUpdate(payload);
  });

  bridge.on('node', (payload) => {
    sendNodeInfo(payload);
  });
}

async function initialiseBridge() {
  callmeshRestoreAllowed = shouldIncludeEnvApiKey();

  const preferences = await getCachedClientPreferences().catch(() => ({}));
  const storedSharePreference = (() => {
    if (!preferences || typeof preferences !== 'object') return null;
    if (!Object.prototype.hasOwnProperty.call(preferences, 'shareWithTenmanMap')) return null;
    const value = preferences.shareWithTenmanMap;
    if (value === null) return null;
    return Boolean(value);
  })();

  let restoredKey = '';
  if (callmeshRestoreAllowed) {
    const persisted = await loadPersistedApiKey();
    if (persisted) {
      restoredKey = persisted;
    }
  }

  const envKey = callmeshRestoreAllowed ? (process.env.CALLMESH_API_KEY || '') : '';
  const initialKey = envKey || restoredKey || '';

  const bridgeOptions = {
    storageDir: getCallMeshDataDir(),
    appVersion,
    apiKey: initialKey,
    verified: Boolean(initialKey),
    heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
    agentProduct: 'callmesh-client'
  };

  if (storedSharePreference !== null) {
    bridgeOptions.shareWithTenmanMap = storedSharePreference;
  }
  const ignoreTenmanInboundNodes = parseEnvBoolean(process.env.TENMAN_IGNORE_INBOUND_NODES, true);
  bridgeOptions.tenmanIgnoreInboundNodes = ignoreTenmanInboundNodes;

  bridge = new CallMeshAprsBridge(bridgeOptions);

  setupBridgeListeners();
  await bridge.init({ allowRestore: callmeshRestoreAllowed });

  lastCallmeshStateSnapshot = bridge.getStateSnapshot();

  if (initialKey) {
    bridge.startHeartbeatLoop();
    bridge.performHeartbeatTick().catch((err) => {
      console.error('啟動後初次 CallMesh Heartbeat 失敗:', err);
    });
  }
}

function buildCallmeshSummary() {
  if (!lastCallmeshStateSnapshot) {
    return {
      statusText: 'CallMesh: 未設定 Key',
      agent: '',
      hasKey: false,
      verified: false,
      verifiedKey: '',
      degraded: false,
      lastHeartbeatAt: null,
      lastMappingHash: null,
      lastMappingSyncedAt: null,
      provision: null,
      mappingItems: []
    };
  }
  const state = lastCallmeshStateSnapshot;
  return {
    statusText: state.lastStatus,
    agent: state.agent,
    hasKey: Boolean(state.verified && state.apiKey),
    verified: Boolean(state.verified),
    verifiedKey: state.verifiedKey || '',
    degraded: state.degraded,
    lastHeartbeatAt: state.lastHeartbeatAt,
    lastMappingHash: state.lastMappingHash,
    lastMappingSyncedAt: state.lastMappingSyncedAt,
    provision: state.provision,
    mappingItems: state.mappingItems
  };
}

async function initialiseApp() {
  await initialiseBridge();
  await loadMessageLog();
  await createWindow();
  await ensureWebDashboardState();

  app.on('activate', async () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      await createWindow();
    }
  });
}

function cleanupMeshtasticClient() {
  if (bridgeSummaryListener && bridge && typeof bridge.removeListener === 'function') {
    bridge.removeListener('summary', bridgeSummaryListener);
    bridgeSummaryListener = null;
  }
  if (client) {
    bridge?.detachMeshtasticClient(client);
    client.stop();
    client.removeAllListeners();
    client = null;
  }
}

async function startWebDashboard() {
  if (webServer) {
    return true;
  }
  try {
    const options = {
      appVersion,
      relayStatsPath: path.join(getCallMeshDataDir(), 'relay-link-stats.json'),
      relayStatsStore: bridge?.getDataStore?.(),
      messageLogPath: getMessageLogPath(),
      messageLogStore: bridge?.getDataStore?.(),
      telemetryProvider: bridge
    };
    const telemetryMaxTotalOverride = resolveTelemetryMaxTotalRecords();
    if (telemetryMaxTotalOverride) {
      options.telemetryMaxTotalRecords = telemetryMaxTotalOverride;
    }
    const server = new WebDashboardServer(options);
    await server.start();
    server.setAppVersion(appVersion);
    webServer = server;
    return true;
  } catch (err) {
    console.error('啟動 Web Dashboard 失敗:', err);
    return false;
  }
}

async function shutdownWebDashboard() {
  if (!webServer) return;
  try {
    await webServer.stop();
  } catch (err) {
    console.warn('關閉 Web Dashboard 失敗:', err);
  } finally {
    webServer = null;
  }
}

function getWebDashboardEnvOverride() {
  const value = process.env.TMAG_WEB_DASHBOARD;
  if (value === '0') return false;
  if (value === '1') return true;
  return null;
}

async function shouldEnableWebDashboard() {
  const override = getWebDashboardEnvOverride();
  if (override !== null) {
    return override;
  }
  const preferences = await getCachedClientPreferences();
  return Boolean(preferences?.webDashboardEnabled);
}

async function ensureWebDashboardState() {
  const desired = await shouldEnableWebDashboard();
  if (desired) {
    const started = await startWebDashboard();
    if (started) {
      if (lastMeshtasticStatus) {
        webServer?.publishStatus(lastMeshtasticStatus);
      }
      if (lastCallmeshStateSnapshot) {
        const snapshotPayload = toRendererCallmeshState(lastCallmeshStateSnapshot);
        webServer?.publishCallmesh(snapshotPayload);
      }
      if (bridge) {
        const nodes = bridge.getNodeSnapshot();
        const nodeMeta = bridge.getNodeDatabaseSourceInfo();
        webServer?.seedNodeSnapshot(nodes, nodeMeta);
      }
      webServer?.seedMessageSnapshot(getMessageSnapshot());
      await syncWebTelemetrySnapshot();
    }
  } else {
    await shutdownWebDashboard();
  }
}

function waitForInitialMeshtasticConnection(nativeClient, { timeoutMs = 15_000 } = {}) {
  if (!nativeClient) {
    return Promise.resolve({ success: false, error: new Error('client not initialised') });
  }

  return new Promise((resolve) => {
    let settled = false;

    const cleanup = () => {
      if (settled) return;
      settled = true;
      nativeClient.off('connected', onConnected);
      nativeClient.off('error', onError);
      if (timer) clearTimeout(timer);
    };

    const onConnected = () => {
      cleanup();
      resolve({ success: true });
    };

    const onError = (err) => {
      cleanup();
      resolve({ success: false, error: err });
    };

    const timer = Number.isFinite(timeoutMs) && timeoutMs > 0
      ? setTimeout(() => {
          cleanup();
          resolve({
            success: false,
            error: new Error(`Meshtastic connect timeout after ${timeoutMs}ms`)
          });
        }, timeoutMs)
      : null;

    nativeClient.once('connected', onConnected);
    nativeClient.once('error', onError);
  });
}

ipcMain.handle('app:get-info', async () => ({
  version: appVersion
}));

ipcMain.handle('nodes:get-snapshot', async () => {
  if (!bridge) {
    return [];
  }
  try {
    return bridge.getNodeSnapshot();
  } catch (err) {
    console.error('取得節點快照失敗:', err);
    return [];
  }
});

ipcMain.handle('nodes:clear', async () => {
  if (!bridge) {
    return { success: false, error: 'bridge not initialised' };
  }
  try {
    const result = bridge.clearNodeDatabase();
    const snapshot = Array.isArray(result?.nodes) ? result.nodes : [];
    mainWindow?.webContents.send('meshtastic:node-snapshot', snapshot);
    const nodeMeta = bridge.getNodeDatabaseSourceInfo();
    webServer?.seedNodeSnapshot(snapshot, nodeMeta);
    return {
      success: true,
      cleared: Number.isFinite(result?.cleared) ? result.cleared : 0,
      nodes: snapshot
    };
  } catch (err) {
    console.error('清除節點資料庫失敗:', err);
    return {
      success: false,
      error: err.message
    };
  }
});

ipcMain.handle('app:get-preferences', async () => {
  try {
    const preferences = await getCachedClientPreferences();
    return preferences;
  } catch (err) {
    console.error('載入客戶端偏好失敗:', err);
    return {};
  }
});

ipcMain.handle('app:update-preferences', async (_event, updates) => {
  try {
    const next = await updateClientPreferences(updates);
    if (updates && Object.prototype.hasOwnProperty.call(updates, 'webDashboardEnabled')) {
      await ensureWebDashboardState();
    }
    return {
      success: true,
      preferences: next
    };
  } catch (err) {
    console.error('更新客戶端偏好失敗:', err);
    return {
      success: false,
      error: err.message
    };
  }
});

ipcMain.handle('web:set-enabled', async (_event, enabledInput) => {
  const desired = Boolean(enabledInput);
  const override = getWebDashboardEnvOverride();
  if (override !== null && override !== desired) {
    const reason =
      override === true
        ? '環境變數 TMAG_WEB_DASHBOARD=1 已強制啟用 Web UI'
        : '環境變數 TMAG_WEB_DASHBOARD=0 已停用 Web UI';
    return {
      success: false,
      error: reason,
      override: true,
      enabled: override
    };
  }
  try {
    const preferences = await updateClientPreferences({ webDashboardEnabled: desired });
    await ensureWebDashboardState();
    return {
      success: true,
      enabled: desired,
      preferences
    };
  } catch (err) {
    console.error('更新 Web Dashboard 狀態失敗:', err);
    return {
      success: false,
      error: err.message
    };
  }
});

ipcMain.handle('meshtastic:connect', async (_event, options) => {
  if (!mainWindow) {
    throw new Error('主視窗尚未建立');
  }

  cleanupMeshtasticClient();

  const connectingPayload = { status: 'connecting' };
  mainWindow.webContents.send('meshtastic:status', connectingPayload);
  lastMeshtasticStatus = connectingPayload;
  webServer?.publishStatus(connectingPayload);

  const parseSerialHost = (value) => {
    if (!value || typeof value !== 'string') return null;
    const trimmed = value.trim();
    if (!trimmed.toLowerCase().startsWith('serial:')) return null;
    let remainder = trimmed.slice(trimmed.indexOf(':') + 1);
    if (remainder.startsWith('//')) {
      remainder = remainder.slice(2);
    }
    let query = '';
    const queryIndex = remainder.indexOf('?');
    if (queryIndex >= 0) {
      query = remainder.slice(queryIndex + 1);
      remainder = remainder.slice(0, queryIndex);
    }
    let baudRate = null;
    const atIndex = remainder.lastIndexOf('@');
    if (atIndex > 0) {
      const candidate = remainder.slice(atIndex + 1).trim();
      if (/^\d+$/.test(candidate)) {
        baudRate = Number(candidate);
        remainder = remainder.slice(0, atIndex);
      }
    }
    if (query) {
      const params = new URLSearchParams(query);
      for (const key of ['baud', 'baudrate']) {
        if (params.has(key)) {
          const valueNum = Number(params.get(key));
          if (Number.isFinite(valueNum) && valueNum > 0) {
            baudRate = valueNum;
            break;
          }
        }
      }
    }
    const path = remainder.trim();
    if (!path) {
      return null;
    }
    return {
      path,
      baudRate: Number.isFinite(baudRate) && baudRate > 0 ? baudRate : null
    };
  };

  const determineTransport = () => {
    if (typeof options.transport === 'string') {
      const mode = options.transport.trim().toLowerCase();
      if (mode === 'serial') return 'serial';
      if (mode === 'tcp') return 'tcp';
    }
    if (options.serialPath) return 'serial';
    if (options.host && parseSerialHost(options.host)) return 'serial';
    return 'tcp';
  };

  const transport = determineTransport();
  const relayStatsPath = path.join(getCallMeshDataDir(), 'relay-link-stats.json');
  const clientOptions = {
    transport,
    maxLength: options.maxLength ?? 512,
    handshake: options.handshake ?? true,
    heartbeat: options.heartbeat ?? 0,
    relayStatsPath
  };
  if (bridge?.getDataStore) {
    clientOptions.relayStatsStore = bridge.getDataStore();
  }

  if (Number.isFinite(options.connectTimeoutMs) && options.connectTimeoutMs > 0) {
    clientOptions.connectTimeout = options.connectTimeoutMs;
  } else if (Number.isFinite(options.connectTimeout) && options.connectTimeout > 0) {
    clientOptions.connectTimeout = options.connectTimeout;
  }

  if (transport === 'serial') {
    const serialSpec = parseSerialHost(options.host);
    let serialPath = typeof options.serialPath === 'string' ? options.serialPath.trim() : '';
    if (!serialPath && serialSpec?.path) {
      serialPath = serialSpec.path;
    }
    clientOptions.serialPath = serialPath;
    const baudCandidates = [
      options.serialBaudRate,
      options.serialBaud,
      serialSpec?.baudRate
    ]
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value) && value > 0);
    if (baudCandidates.length) {
      clientOptions.serialBaudRate = baudCandidates[0];
    }
    if (options.serialOpenOptions && typeof options.serialOpenOptions === 'object') {
      clientOptions.serialOpenOptions = { ...options.serialOpenOptions };
    }
    clientOptions.keepAlive = false;
    clientOptions.idleTimeoutMs = Number.isFinite(options.idleTimeoutMs)
      ? options.idleTimeoutMs
      : 0;
  } else {
    clientOptions.host =
      typeof options.host === 'string' && options.host.trim() ? options.host.trim() : '127.0.0.1';
    clientOptions.port = Number.isFinite(options.port) ? options.port : 4403;
    clientOptions.keepAlive = options.keepAlive ?? true;
    clientOptions.keepAliveDelayMs = options.keepAliveDelayMs ?? 15000;
    clientOptions.idleTimeoutMs = options.idleTimeoutMs ?? 0;
  }

  if (transport === 'serial' && !clientOptions.serialPath) {
    const message = 'Serial 連線需要指定裝置路徑';
    const payload = { status: 'error', message };
    mainWindow?.webContents.send('meshtastic:status', payload);
    lastMeshtasticStatus = payload;
    webServer?.publishStatus(payload);
    throw new Error(message);
  }

  client = new MeshtasticClient(clientOptions);
  bridge?.attachMeshtasticClient(client);

  client.on('connected', () => {
    const payload = { status: 'connected' };
    mainWindow?.webContents.send('meshtastic:status', payload);
    lastMeshtasticStatus = payload;
    webServer?.publishStatus(payload);
  });

  client.on('disconnected', () => {
    const payload = { status: 'disconnected' };
    mainWindow?.webContents.send('meshtastic:status', payload);
    lastMeshtasticStatus = payload;
    webServer?.publishStatus(payload);
  });

  const processSummary = (summary, { synthetic = false } = {}) => {
    if (!summary) return;
    let messageEntry = null;
    try {
      if (!synthetic) {
        bridge?.handleMeshtasticSummary(summary);
      }
    } catch (err) {
      console.error('處理 APRS Summary 時發生錯誤:', err);
    }
    try {
      messageEntry = persistMessageSummary(summary);
    } catch (err) {
      console.error('寫入訊息紀錄失敗:', err);
    }
    mainWindow?.webContents.send('meshtastic:summary', summary);
    webServer?.publishSummary(summary);
    if (messageEntry) {
      webServer?.publishMessage(messageEntry);
    }
  };

  if (bridge && typeof bridge.on === 'function') {
    if (bridgeSummaryListener && typeof bridge.removeListener === 'function') {
      bridge.removeListener('summary', bridgeSummaryListener);
    }
    bridgeSummaryListener = (summary) => {
      processSummary(summary, { synthetic: true });
    };
    bridge.on('summary', bridgeSummaryListener);
  }

  client.on('summary', (summary) => {
    processSummary(summary);
  });

  client.on('fromRadio', ({ message }) => {
    if (!message) return;
    try {
      const plainObject = client.toObject(message, {
        bytes: String
      });
      mainWindow?.webContents.send('meshtastic:fromRadio', plainObject);
    } catch (err) {
      console.error('序列化 Meshtastic 訊息失敗:', err);
    }
  });

  client.on('myInfo', (info) => {
    bridge?.handleMeshtasticMyInfo(info);
    mainWindow?.webContents.send('meshtastic:myInfo', info);
    webServer?.publishSelf(info);
  });

  client.on('error', (err) => {
    const payload = {
      status: 'error',
      message: err.message
    };
    mainWindow?.webContents.send('meshtastic:status', payload);
    lastMeshtasticStatus = payload;
    webServer?.publishStatus(payload);
  });

  const connectPromise = waitForInitialMeshtasticConnection(client, {
    timeoutMs: options?.connectTimeoutMs ?? 15_000
  });
  connectPromise.catch(() => {});

  let startError = null;
  try {
    await client.start();
  } catch (err) {
    startError = err;
  }

  if (startError) {
    mainWindow.webContents.send('meshtastic:status', {
      status: 'error',
      message: startError.message
    });
    cleanupMeshtasticClient();
    return { success: false, error: startError.message };
  }

  const connectWait = await connectPromise;

  if (!connectWait.success) {
    const message = connectWait.error?.message || 'Meshtastic 連線失敗';
    mainWindow.webContents.send('meshtastic:status', {
      status: 'error',
      message
    });
    cleanupMeshtasticClient();
    return { success: false, error: message };
  }

  return { success: true };
});

ipcMain.handle('meshtastic:disconnect', async () => {
  cleanupMeshtasticClient();
  mainWindow?.webContents.send('meshtastic:status', { status: 'disconnected' });
});

ipcMain.handle('meshtastic:discover', async (_event, options) => {
  const devices = await discoverMeshtasticDevices(options);
  return devices;
});

ipcMain.handle('meshtastic:list-serial', async () => {
  try {
    const ports = await SerialPort.list();
    if (!Array.isArray(ports)) {
      return [];
    }
    return ports.map((port) => ({
      path: port.path || '',
      manufacturer: port.manufacturer || null,
      friendlyName: port.friendlyName || null,
      productId: port.productId || null,
      vendorId: port.vendorId || null,
      serialNumber: port.serialNumber || null,
      locationId: port.locationId || null,
      pnpId: port.pnpId || null
    }));
  } catch (err) {
    console.error('列出 Serial 裝置失敗:', err);
    throw err;
  }
});

ipcMain.handle('messages:get-snapshot', async () => {
  return { channels: getMessageSnapshot() };
});

ipcMain.handle('callmesh:save-key', async (_event, apiKey) => {
  if (!bridge) {
    throw new Error('CallMesh bridge 尚未建立');
  }

  const trimmed = (apiKey || '').trim();
  if (!trimmed) {
    bridge.clearApiKey();
    await bridge.clearArtifacts();
    await clearPersistedApiKey();
    await markSkipEnvKey();
    callmeshRestoreAllowed = false;
    lastCallmeshStateSnapshot = bridge.getStateSnapshot();
    sendCallmeshStateToRenderer(lastCallmeshStateSnapshot);
    return {
      statusText: lastCallmeshStateSnapshot.lastStatus,
      agent: lastCallmeshStateSnapshot.agent,
      hasKey: false,
      success: true,
      verified: false,
      degraded: false
    };
  }

  const currentState = bridge.getStateSnapshot();
  const sameKeyAsVerified = Boolean(currentState.verified && currentState.verifiedKey === trimmed);

  try {
    const verifyResult = await bridge.verifyApiKey(trimmed, {
      allowDegraded: sameKeyAsVerified
    });

    if (verifyResult.success && !verifyResult.degraded) {
      if (!sameKeyAsVerified) {
        await bridge.clearArtifacts();
      }
      await clearSkipEnvKey();
      callmeshRestoreAllowed = true;
      await persistVerifiedApiKey(trimmed);
      bridge.startHeartbeatLoop();
      bridge.performHeartbeatTick().catch((err) => {
        console.error('API Key 驗證後初次 CallMesh Heartbeat 失敗:', err);
      });
      lastCallmeshStateSnapshot = bridge.getStateSnapshot();
      await syncWebTelemetrySnapshot();
      return {
        statusText: lastCallmeshStateSnapshot.lastStatus,
        agent: lastCallmeshStateSnapshot.agent,
        hasKey: true,
        success: true,
        verified: true,
        degraded: false
      };
    }

    if (verifyResult.success && verifyResult.degraded) {
      await clearSkipEnvKey();
      callmeshRestoreAllowed = true;
      await persistVerifiedApiKey(trimmed);
      bridge.startHeartbeatLoop();
      lastCallmeshStateSnapshot = bridge.getStateSnapshot();
      await syncWebTelemetrySnapshot();
      return {
        statusText: lastCallmeshStateSnapshot.lastStatus,
        agent: lastCallmeshStateSnapshot.agent,
        hasKey: true,
        success: true,
        verified: true,
        degraded: true
      };
    }

    if (verifyResult.authError) {
      await clearPersistedApiKey();
      bridge.stopHeartbeatLoop();
      lastCallmeshStateSnapshot = bridge.getStateSnapshot();
      await syncWebTelemetrySnapshot();
      return {
        statusText: lastCallmeshStateSnapshot.lastStatus,
        agent: lastCallmeshStateSnapshot.agent,
        hasKey: false,
        success: false,
        verified: false,
        degraded: false,
        error: verifyResult.error?.message || '驗證失敗'
      };
    }

    throw verifyResult.error || new Error('CallMesh 驗證失敗');
  } catch (err) {
    bridge.clearApiKey();
    await clearPersistedApiKey();
    bridge.stopHeartbeatLoop();
    lastCallmeshStateSnapshot = bridge.getStateSnapshot();
    await syncWebTelemetrySnapshot();
    return {
      statusText: lastCallmeshStateSnapshot.lastStatus,
      agent: lastCallmeshStateSnapshot.agent,
      hasKey: false,
      success: false,
      verified: false,
      degraded: false,
      error: err.message
    };
  }
});

ipcMain.handle('callmesh:get-status', async () => {
  lastCallmeshStateSnapshot = bridge?.getStateSnapshot() ?? lastCallmeshStateSnapshot;
  return buildCallmeshSummary();
});

ipcMain.handle('telemetry:get-snapshot', async (_event, options = {}) => {
  if (!bridge) {
    return {
      updatedAt: Date.now(),
      nodes: [],
      summary: [],
      stats: {
        totalRecords: 0,
        totalNodes: 0,
        diskBytes: 0
      }
    };
  }
  try {
    const summary = bridge.getTelemetrySummary();
    const updatedAt =
      Number.isFinite(summary?.updatedAt) && summary.updatedAt > 0
        ? Number(summary.updatedAt)
        : bridge.telemetryUpdatedAt;
    const nodes = Array.isArray(summary?.nodes) ? summary.nodes : [];
    const stats =
      summary?.stats && typeof summary.stats === 'object'
        ? summary.stats
        : bridge.getTelemetryStats();
    return {
      updatedAt,
      nodes,
      summary: nodes,
      stats
    };
  } catch (err) {
    console.error('取得遙測快照失敗:', err);
    return {
      updatedAt: Date.now(),
      nodes: [],
      summary: [],
      stats: {
        totalRecords: 0,
        totalNodes: 0,
        diskBytes: 0
      },
      error: err.message
    };
  }
});

ipcMain.handle('telemetry:get-available', async () => {
  const fallback = {
    updatedAt: Date.now(),
    nodes: [],
    summary: [],
    stats: {
      totalRecords: 0,
      totalNodes: 0,
      diskBytes: 0
    }
  };
  if (!bridge) {
    return fallback;
  }
  try {
    const summary = bridge.getTelemetrySummary();
    const updatedAt =
      Number.isFinite(summary?.updatedAt) && summary.updatedAt > 0
        ? Number(summary.updatedAt)
        : bridge.telemetryUpdatedAt;
    const nodes = Array.isArray(summary?.nodes) ? summary.nodes : [];
    const stats =
      summary?.stats && typeof summary.stats === 'object'
        ? summary.stats
        : bridge.getTelemetryStats();
    return {
      updatedAt,
      nodes,
      summary: nodes,
      stats
    };
  } catch (err) {
    console.error('取得遙測節點清單失敗:', err);
    return fallback;
  }
});

ipcMain.handle('telemetry:fetch-range', async (_event, options = {}) => {
  const meshId = options?.meshId || options?.mesh_id || null;
  const fallback = {
    meshId,
    rawMeshId: meshId,
    meshIdNormalized: meshId,
    node: null,
    records: [],
    totalRecords: 0,
    filteredCount: 0,
    latestSampleMs: null,
    earliestSampleMs: null,
    availableMetrics: [],
    range: {
      startMs: options?.startMs ?? options?.start ?? null,
      endMs: options?.endMs ?? options?.end ?? null
    },
    requestedLimit: options?.limit ?? null,
    updatedAt: Date.now(),
    stats: {
      totalRecords: 0,
      totalNodes: 0,
      diskBytes: 0
    },
    error: bridge ? null : 'bridge not initialised'
  };
  if (!bridge) {
    return fallback;
  }
  try {
    const limit = Number.isFinite(options?.limit) ? Number(options.limit) : undefined;
    const startMs = Number.isFinite(options?.startMs ?? options?.start)
      ? Number(options?.startMs ?? options?.start)
      : null;
    const endMs = Number.isFinite(options?.endMs ?? options?.end)
      ? Number(options?.endMs ?? options?.end)
      : null;
    return bridge.getTelemetryRecordsForMesh(meshId, {
      limit,
      startMs,
      endMs
    });
  } catch (err) {
    console.error('取得遙測範圍資料失敗:', err);
    return {
      ...fallback,
      error: err.message || 'unknown error'
    };
  }
});

ipcMain.handle('telemetry:clear', async () => {
  if (!bridge) {
    return { success: false, error: 'bridge not initialised' };
  }
  try {
    await bridge.clearTelemetryStore({ silent: false });
    return { success: true };
  } catch (err) {
    console.error('清除遙測資料失敗:', err);
    return { success: false, error: err.message };
  }
});

ipcMain.handle('callmesh:reset', async () => {
  if (!bridge) {
    return { success: false, error: 'bridge not initialised' };
  }
  bridge.stopHeartbeatLoop();
  bridge.clearApiKey();
  await bridge.clearArtifacts();
  await clearPersistedApiKey();
  await markSkipEnvKey();
  callmeshRestoreAllowed = false;
  lastCallmeshStateSnapshot = bridge.getStateSnapshot();
  sendCallmeshStateToRenderer(lastCallmeshStateSnapshot);
  return { success: true };
});

ipcMain.handle('callmesh:should-auto-validate', async () => {
  if (process.env.CALLMESH_API_KEY || process.env.CALLMESH_VERIFICATION_FILE) {
    return false;
  }
  return Boolean(callmeshRestoreAllowed);
});

ipcMain.handle('aprs:set-server', async (_event, server) => {
  bridge?.updateAprsServer(server);
  return { success: true };
});

ipcMain.handle('aprs:set-beacon-interval', async (_event, minutes) => {
  const numeric = Number(minutes);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    throw new Error('invalid beacon interval');
  }
  const ms = Math.round(numeric * 60_000);
  bridge?.setAprsBeaconIntervalMs(ms);
  lastCallmeshStateSnapshot = bridge?.getStateSnapshot() ?? lastCallmeshStateSnapshot;
  return {
    success: true,
    beaconIntervalMs: lastCallmeshStateSnapshot?.aprs?.beaconIntervalMs ?? null
  };
});

app.whenReady().then(() => {
  initialiseApp().catch((err) => {
    console.error('初始化失敗:', err);
    app.quit();
  });
});

app.on('before-quit', () => {
  messageWritePromise = messageWritePromise
    .then(() => flushMessageLog())
    .catch((err) => {
      console.error('關閉前寫入訊息紀錄失敗:', err);
    });
});

app.on('window-all-closed', () => {
  cleanupMeshtasticClient();
  if (bridge) {
    bridge.destroy();
  }
  shutdownWebDashboard();
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('will-quit', () => {
  shutdownWebDashboard();
});
