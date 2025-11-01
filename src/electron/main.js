'use strict';

const path = require('path');
const fs = require('fs/promises');
const fsSync = require('fs');
const { app, BrowserWindow, ipcMain, Menu } = require('electron');
const { version: appVersion } = require('../../package.json');
const MeshtasticClient = require('../meshtasticClient');
const { discoverMeshtasticDevices } = require('../discovery');
const { CallMeshAprsBridge } = require('../callmesh/aprsBridge');
const { WebDashboardServer } = require('../web/server');

const HEARTBEAT_INTERVAL_MS = 60_000;

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

async function syncWebTelemetrySnapshot() {
  if (!bridge || !webServer || typeof bridge.getTelemetrySnapshot !== 'function') {
    return;
  }
  try {
    const snapshot = await bridge.getTelemetrySnapshot({
      limitPerNode: webServer.telemetryMaxPerNode
    });
    webServer.seedTelemetrySnapshot(snapshot);
  } catch (err) {
    console.warn('同步 Web Telemetry Snapshot 失敗:', err);
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
}

function setupBridgeListeners() {
  if (!bridge) return;
  bridge.removeAllListeners('state');
  bridge.removeAllListeners('log');
  bridge.removeAllListeners('aprs-uplink');
  bridge.removeAllListeners('telemetry');

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
}

async function initialiseBridge() {
  callmeshRestoreAllowed = shouldIncludeEnvApiKey();

  let restoredKey = '';
  if (callmeshRestoreAllowed) {
    const persisted = await loadPersistedApiKey();
    if (persisted) {
      restoredKey = persisted;
    }
  }

  const envKey = callmeshRestoreAllowed ? (process.env.CALLMESH_API_KEY || '') : '';
  const initialKey = envKey || restoredKey || '';

  bridge = new CallMeshAprsBridge({
    storageDir: getCallMeshDataDir(),
    appVersion,
    apiKey: initialKey,
    verified: Boolean(initialKey),
    heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
    agentProduct: 'callmesh-client'
  });

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
  await createWindow();
  await ensureWebDashboardState();

  app.on('activate', async () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      await createWindow();
    }
  });
}

function cleanupMeshtasticClient() {
  if (client) {
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
    const server = new WebDashboardServer({ appVersion });
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

  client = new MeshtasticClient({
    host: options.host,
    port: options.port,
    maxLength: options.maxLength ?? 512,
    handshake: options.handshake ?? true,
    heartbeat: options.heartbeat ?? 0,
    keepAlive: options.keepAlive ?? true,
    keepAliveDelayMs: options.keepAliveDelayMs ?? 15000,
    idleTimeoutMs: options.idleTimeoutMs ?? 0
  });

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

  client.on('summary', (summary) => {
    if (!summary) return;
    try {
      bridge?.handleMeshtasticSummary(summary);
    } catch (err) {
      console.error('處理 APRS Summary 時發生錯誤:', err);
    }
    mainWindow?.webContents.send('meshtastic:summary', summary);
    webServer?.publishSummary(summary);
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
      nodes: []
    };
  }
  try {
    const limit = Number.isFinite(options?.limitPerNode) ? options.limitPerNode : undefined;
    return bridge.getTelemetrySnapshot({
      limitPerNode: limit
    });
  } catch (err) {
    console.error('取得遙測快照失敗:', err);
    return {
      updatedAt: Date.now(),
      nodes: []
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
