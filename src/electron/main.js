'use strict';

const path = require('path');
const fs = require('fs/promises');
const fsSync = require('fs');
const { app, BrowserWindow, ipcMain, Menu } = require('electron');
const { version: appVersion } = require('../../package.json');
const MeshtasticClient = require('../meshtasticClient');
const { discoverMeshtasticDevices } = require('../discovery');
const { CallMeshAprsBridge } = require('../callmesh/aprsBridge');

const HEARTBEAT_INTERVAL_MS = 60_000;

let mainWindow = null;
let client = null;
let bridge = null;
let callmeshRestoreAllowed = true;
let lastCallmeshStateSnapshot = null;

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
  if (!mainWindow) return;
  mainWindow.webContents.send('callmesh:status', toRendererCallmeshState(state));
}

function sendCallmeshLog(entry) {
  if (!mainWindow) return;
  mainWindow.webContents.send('callmesh:log', entry);
}

function sendAprsUplink(info) {
  if (!mainWindow || !info) return;
  mainWindow.webContents.send('meshtastic:aprs-uplink', info);
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

ipcMain.handle('meshtastic:connect', async (_event, options) => {
  if (!mainWindow) {
    throw new Error('主視窗尚未建立');
  }

  cleanupMeshtasticClient();

  mainWindow.webContents.send('meshtastic:status', { status: 'connecting' });

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
    mainWindow?.webContents.send('meshtastic:status', { status: 'connected' });
  });

  client.on('disconnected', () => {
    mainWindow?.webContents.send('meshtastic:status', { status: 'disconnected' });
  });

  client.on('summary', (summary) => {
    if (!summary) return;
    try {
      bridge?.handleMeshtasticSummary(summary);
    } catch (err) {
      console.error('處理 APRS Summary 時發生錯誤:', err);
    }
    mainWindow?.webContents.send('meshtastic:summary', summary);
  });

  client.on('myInfo', (info) => {
    bridge?.handleMeshtasticMyInfo(info);
    mainWindow?.webContents.send('meshtastic:myInfo', info);
  });

  client.on('error', (err) => {
    mainWindow?.webContents.send('meshtastic:status', {
      status: 'error',
      message: err.message
    });
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
  if (process.platform !== 'darwin') {
    app.quit();
  }
});
