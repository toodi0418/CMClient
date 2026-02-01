'use strict';

const electronModule = require('electron');

if (!electronModule || typeof electronModule !== 'object' || typeof electronModule.app === 'undefined') {
  // 若以 Node 方式啟動（常見於 ELECTRON_RUN_AS_NODE=1），自動重新以 GUI 模式啟動。
  const { spawnSync } = require('child_process');
  const electronBinary =
    typeof electronModule === 'string' && electronModule.length
      ? electronModule
      : process.execPath;
  const env = { ...process.env };
  delete env.ELECTRON_RUN_AS_NODE;
  const result = spawnSync(electronBinary, [__filename, ...process.argv.slice(2)], {
    stdio: 'inherit',
    env
  });
  if (result.error) {
    console.error('重新啟動 Electron 失敗:', result.error);
    process.exit(1);
  }
  process.exit(result.status ?? 0);
}

const { app, BrowserWindow, Menu, shell, ipcMain } = electronModule;
const { spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs/promises');
const http = require('http');
const https = require('https');
const net = require('net');
const { version: appVersion } = require('../../package.json');
const { CallMeshClient } = require('../callmesh/client');
const { discoverMeshtasticDevices } = require('../discovery');
const { SerialPort } = require('serialport');

const DEFAULT_WEB_PORT = 7080;
const DATA_DIR_NAME = 'callmesh';
const VERIFICATION_FILENAME = 'verification.json';
const MONITOR_FILENAME = 'monitor.json';
const PREFERENCES_FILENAME = 'client-preferences.json';
const STARTUP_TIMEOUT_MS = 20_000;
const SELF_CACHE_TTL_MS = 12_000;

let mainWindow = null;
let settingsWindow = null;
let backendProcess = null;
let backendPort = null;
let shuttingDown = false;
let backendHasApiKey = false;
let lastStatusMessage = '';
let cachedSelfInfo = null;
let cachedNodeSnapshot = null;
let cachedSelfFetchedAt = 0;
let sseRequest = null;
let sseResponse = null;
let sseRetryTimer = null;
let lastWebDashboardEnabled = true;
let lastRxAt = 0;
let lastCallmeshPayload = null;

process.on('uncaughtException', (err) => {
  console.error('未攔截的例外:', err);
});

process.on('unhandledRejection', (reason) => {
  console.error('未處理的 Promise 拒絕:', reason);
});

function getDataDir() {
  return path.join(app.getPath('userData'), DATA_DIR_NAME);
}

async function readJsonSafe(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function isAuthError(error) {
  const message = error?.message ? String(error.message) : '';
  return /\((401|403)\)/.test(message);
}

function getDefaultCliVerificationPath() {
  return path.join(os.homedir(), '.config', 'callmesh', MONITOR_FILENAME);
}

async function readApiKeyFromFile(filePath) {
  if (!filePath) return null;
  const payload = await readJsonSafe(filePath);
  const key = payload?.apiKey;
  if (typeof key === 'string' && key.trim()) {
    return key.trim();
  }
  return null;
}

async function loadPersistedApiKey({ verificationPath } = {}) {
  const dataDir = getDataDir();
  const candidates = [];
  if (verificationPath) {
    candidates.push(verificationPath);
  }
  if (process.env.CALLMESH_VERIFICATION_FILE) {
    candidates.push(process.env.CALLMESH_VERIFICATION_FILE);
  }
  candidates.push(
    path.join(dataDir, MONITOR_FILENAME),
    path.join(dataDir, VERIFICATION_FILENAME),
    getDefaultCliVerificationPath()
  );

  for (const candidate of candidates) {
    // eslint-disable-next-line no-await-in-loop
    const key = await readApiKeyFromFile(candidate);
    if (key) {
      return key;
    }
  }
  return null;
}

async function loadClientPreferences() {
  const payload = await readJsonSafe(path.join(getDataDir(), PREFERENCES_FILENAME));
  return payload && typeof payload === 'object' ? payload : {};
}

async function saveClientPreferences(updates) {
  const filePath = path.join(getDataDir(), PREFERENCES_FILENAME);
  const current = await loadClientPreferences();
  const next = { ...current };

  if (updates && typeof updates === 'object') {
    if (Object.prototype.hasOwnProperty.call(updates, 'connectionMode')) {
      const mode =
        typeof updates.connectionMode === 'string'
          ? updates.connectionMode.trim().toLowerCase()
          : '';
      if (mode === 'serial' || mode === 'tcp') {
        next.connectionMode = mode;
      } else {
        delete next.connectionMode;
      }
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'host')) {
      const host = typeof updates.host === 'string' ? updates.host.trim() : '';
      if (host) {
        next.host = host;
      } else {
        delete next.host;
      }
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'serialPath')) {
      const serialPath = typeof updates.serialPath === 'string' ? updates.serialPath.trim() : '';
      if (serialPath) {
        next.serialPath = serialPath;
      } else {
        delete next.serialPath;
      }
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'serialBaudRate')) {
      const baud = Number(updates.serialBaudRate);
      if (Number.isFinite(baud) && baud > 0) {
        next.serialBaudRate = Math.round(baud);
      } else {
        delete next.serialBaudRate;
      }
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'webDashboardEnabled')) {
      next.webDashboardEnabled = Boolean(updates.webDashboardEnabled);
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'shareWithTenmanMap')) {
      next.shareWithTenmanMap = updates.shareWithTenmanMap === false ? false : true;
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'autoTracerouteEnabled')) {
      next.autoTracerouteEnabled = updates.autoTracerouteEnabled === false ? false : true;
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'tracerouteRateMinutes')) {
      const minutes = Number(updates.tracerouteRateMinutes);
      if (Number.isFinite(minutes) && minutes > 0) {
        next.tracerouteRateMinutes = Math.max(15, Math.round(minutes));
      } else {
        delete next.tracerouteRateMinutes;
      }
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'tracerouteIntervalSeconds')) {
      const seconds = Number(updates.tracerouteIntervalSeconds);
      if (Number.isFinite(seconds) && seconds > 0) {
        next.tracerouteIntervalSeconds = Math.max(15, Math.round(seconds));
      } else {
        delete next.tracerouteIntervalSeconds;
      }
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'aprsServer')) {
      const server = typeof updates.aprsServer === 'string' ? updates.aprsServer.trim() : '';
      if (server) {
        next.aprsServer = server;
      } else {
        delete next.aprsServer;
      }
    }
    if (Object.prototype.hasOwnProperty.call(updates, 'aprsBeaconMinutes')) {
      const minutes = Number(updates.aprsBeaconMinutes);
      if (Number.isFinite(minutes) && minutes > 0) {
        next.aprsBeaconMinutes = Math.min(Math.max(1, Math.round(minutes)), 1440);
      } else {
        delete next.aprsBeaconMinutes;
      }
    }
  }

  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(next, null, 2), 'utf8');
  return next;
}

async function verifyCallmeshApiKey(apiKey, { allowDegraded = false } = {}) {
  const trimmed = typeof apiKey === 'string' ? apiKey.trim() : '';
  if (!trimmed) {
    throw new Error('API Key 不可為空');
  }
  const client = new CallMeshClient({
    apiKey: trimmed,
    product: 'callmesh-client-electron',
    version: appVersion
  });

  try {
    const heartbeat = await client.heartbeat({ timeout: 8000 });
    return {
      success: true,
      degraded: false,
      agent: client.agentString,
      heartbeat
    };
  } catch (err) {
    if (isAuthError(err)) {
      return { success: false, authError: true, error: err };
    }
    if (allowDegraded) {
      return {
        success: true,
        degraded: true,
        agent: client.agentString,
        error: err
      };
    }
    return { success: false, error: err };
  }
}

function buildMeshtasticHostFromPreferences(preferences) {
  if (!preferences || typeof preferences !== 'object') return null;
  const mode =
    typeof preferences.connectionMode === 'string'
      ? preferences.connectionMode.trim().toLowerCase()
      : '';
  const host = typeof preferences.host === 'string' ? preferences.host.trim() : '';
  const serialPath =
    typeof preferences.serialPath === 'string' ? preferences.serialPath.trim() : '';
  const serialBaud = Number(preferences.serialBaudRate);

  if (mode === 'serial' || serialPath) {
    if (!serialPath) {
      if (host && host.toLowerCase().startsWith('serial:')) {
        return host;
      }
      return null;
    }
    if (Number.isFinite(serialBaud) && serialBaud > 0) {
      return `serial://${serialPath}@${Math.round(serialBaud)}`;
    }
    return `serial://${serialPath}`;
  }

  if (host) {
    return host;
  }

  return null;
}

function resolveUiHost() {
  const raw = typeof process.env.TMAG_WEB_HOST === 'string' ? process.env.TMAG_WEB_HOST.trim() : '';
  if (!raw || raw === '0.0.0.0' || raw === '::') {
    return '127.0.0.1';
  }
  return raw;
}

function parseEnvPort(raw) {
  if (!raw) return null;
  const value = Number(raw);
  if (!Number.isFinite(value)) return null;
  const port = Math.floor(value);
  if (port < 1 || port > 65535) return null;
  return port;
}

function findAvailablePort(preferredPort) {
  return new Promise((resolve) => {
    const port = Number.isFinite(preferredPort) && preferredPort > 0 ? preferredPort : 0;
    const server = net.createServer();
    server.unref();
    server.on('error', () => {
      if (port !== 0) {
        resolve(findAvailablePort(0));
        return;
      }
      resolve(null);
    });
    server.listen(port, '127.0.0.1', () => {
      const address = server.address();
      const resolvedPort = address && typeof address === 'object' ? address.port : null;
      server.close(() => resolve(resolvedPort));
    });
  });
}

function waitForWebReady(url, timeoutMs = STARTUP_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  const target = new URL(url);

  return new Promise((resolve, reject) => {
    const attempt = () => {
      const req = http.request(
        {
          method: 'GET',
          hostname: target.hostname,
          port: target.port,
          path: target.pathname,
          timeout: 2000
        },
        (res) => {
          res.resume();
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 500) {
            resolve();
            return;
          }
          if (Date.now() > deadline) {
            reject(new Error('Web UI 啟動逾時'));
            return;
          }
          setTimeout(attempt, 500);
        }
      );
      req.on('error', () => {
        if (Date.now() > deadline) {
          reject(new Error('Web UI 啟動逾時'));
          return;
        }
        setTimeout(attempt, 500);
      });
      req.on('timeout', () => {
        req.destroy();
      });
      req.end();
    };

    attempt();
  });
}

function buildFallbackHtml({ title, message, details, uiUrl } = {}) {
  const safeTitle = title || 'Web UI 尚未就緒';
  const safeMessage = message || '請確認後端已啟動並設定 CallMesh API Key。';
  const safeDetails = details || '';
  const safeUrl = uiUrl || '';

  return `<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${safeTitle}</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background:#0f1115; color:#f5f5f5; margin:0; padding:40px; }
    .card { max-width:720px; margin:0 auto; background:#171a21; border:1px solid #2a2f3a; border-radius:16px; padding:28px; }
    h1 { font-size:20px; margin:0 0 12px; }
    p { line-height:1.6; margin:8px 0; color:#cbd3e1; }
    code { background:#20242e; padding:2px 6px; border-radius:6px; color:#f4d06f; }
    .muted { color:#8892a6; font-size:13px; }
    a { color:#8ac7ff; }
  </style>
</head>
<body>
  <div class="card">
    <h1>${safeTitle}</h1>
    <p>${safeMessage}</p>
    ${safeUrl ? `<p>預期網址：<code>${safeUrl}</code></p>` : ''}
    ${safeDetails ? `<p class="muted">${safeDetails}</p>` : ''}
    <p class="muted">版本：v${appVersion}</p>
  </div>
</body>
</html>`;
}

async function startBackend() {
  const dataDir = getDataDir();
  await fs.mkdir(dataDir, { recursive: true });

  const env = { ...process.env, ELECTRON_RUN_AS_NODE: '1' };
  env.CALLMESH_ARTIFACTS_DIR = dataDir;
  env.TMAG_ALLOW_NO_API_KEY = '1';
  if (!env.CALLMESH_VERIFICATION_FILE) {
    env.CALLMESH_VERIFICATION_FILE = path.join(dataDir, MONITOR_FILENAME);
  }

  const persistedKey = await loadPersistedApiKey({
    verificationPath: env.CALLMESH_VERIFICATION_FILE
  });
  if (!env.CALLMESH_API_KEY && persistedKey) {
    env.CALLMESH_API_KEY = persistedKey;
  }
  backendHasApiKey = Boolean(env.CALLMESH_API_KEY);

  const preferences = await loadClientPreferences();
  const preferredHost = buildMeshtasticHostFromPreferences(preferences);
  if (!env.MESHTASTIC_HOST && preferredHost) {
    env.MESHTASTIC_HOST = preferredHost;
  }
  const webUiEnabled = preferences.webDashboardEnabled === false ? false : true;
  lastWebDashboardEnabled = webUiEnabled;

  const envPort = parseEnvPort(env.TMAG_WEB_PORT);
  if (envPort) {
    backendPort = envPort;
  } else {
    backendPort = (await findAvailablePort(DEFAULT_WEB_PORT)) || DEFAULT_WEB_PORT;
    env.TMAG_WEB_PORT = String(backendPort);
  }

  if (!env.TMAG_WEB_DASHBOARD) {
    env.TMAG_WEB_DASHBOARD = webUiEnabled ? '1' : '0';
  }

  const entryPath = path.join(__dirname, '..', 'index.js');
  const args = [entryPath];
  if (webUiEnabled) {
    args.push('--web-ui');
  }
  if (preferences.shareWithTenmanMap === false) {
    args.push('--no-share-with-tenmanmap');
  }
  if (preferences.autoTracerouteEnabled === false) {
    args.push('--no-auto-traceroute');
  }
  if (Number.isFinite(Number(preferences.tracerouteRateMinutes))) {
    args.push('--traceroute-rate-minutes', String(Math.max(15, Math.round(Number(preferences.tracerouteRateMinutes)))));
  }
  if (Number.isFinite(Number(preferences.tracerouteIntervalSeconds))) {
    args.push('--traceroute-interval-seconds', String(Math.max(15, Math.round(Number(preferences.tracerouteIntervalSeconds)))));
  }

  backendProcess = spawn(process.execPath, args, {
    env,
    stdio: 'inherit'
  });

  const spawnedProcess = backendProcess;
  spawnedProcess.on('exit', (code, signal) => {
    if (backendProcess === spawnedProcess) {
      backendProcess = null;
    }
    if (shuttingDown) return;
    const detail = signal ? `signal=${signal}` : `code=${code}`;
    console.error(`後端程序結束 (${detail})`);
    if (mainWindow && !mainWindow.isDestroyed()) {
      const message = backendHasApiKey
        ? `後端程序已結束，請檢查 CallMesh / Meshtastic 設定。(${detail})`
        : `尚未設定 CallMesh API Key，請先輸入 Key。(${detail})`;
      sendStatusMessage(message);
    }
  });

  if (webUiEnabled) {
    scheduleSseReconnect(2000);
  } else {
    stopSseListener();
  }

  return backendPort;
}

function buildWebUrl() {
  const host = resolveUiHost();
  const port = backendPort || DEFAULT_WEB_PORT;
  return `http://${host}:${port}/`;
}

function fetchSelfSnapshot(url, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    let finished = false;
    const finish = (payload, error) => {
      if (finished) return;
      finished = true;
      if (error) {
        reject(error);
        return;
      }
      resolve(payload);
    };

    let target;
    try {
      target = new URL('/api/events', url);
    } catch (err) {
      finish(null, err);
      return;
    }

    const client = target.protocol === 'https:' ? https : http;
    const req = client.request(
      {
        method: 'GET',
        hostname: target.hostname,
        port: target.port,
        path: target.pathname + target.search,
        headers: { Accept: 'text/event-stream' }
      },
      (res) => {
        if (res.statusCode && res.statusCode >= 400) {
          finish(null, new Error(`Web UI 回應錯誤 (${res.statusCode})`));
          res.resume();
          return;
        }
        res.setEncoding('utf8');
        let buffer = '';
        let dataLines = [];
        let selfInfo = null;
        let nodeSnapshot = null;
        const tryFinish = () => {
          if (selfInfo && nodeSnapshot) {
            finish({ selfInfo, nodeSnapshot });
            req.destroy();
            res.destroy();
          }
        };
        res.on('data', (chunk) => {
          buffer += chunk;
          let idx;
          while ((idx = buffer.indexOf('\n')) >= 0) {
            const line = buffer.slice(0, idx).replace(/\r$/, '');
            buffer = buffer.slice(idx + 1);
            if (!line) {
              if (dataLines.length) {
                const payload = dataLines.join('\n');
                dataLines = [];
                try {
                  const event = JSON.parse(payload);
                  if (event?.type === 'self') {
                    selfInfo = event.payload || null;
                  } else if (event?.type === 'node-snapshot') {
                    nodeSnapshot = Array.isArray(event.payload) ? event.payload : [];
                  }
                } catch {
                  // ignore parse errors
                }
                tryFinish();
              }
              continue;
            }
            if (line.startsWith(':')) {
              continue;
            }
            if (line.startsWith('data:')) {
              dataLines.push(line.slice(5).trim());
            }
          }
        });
        res.on('end', () => {
          finish({ selfInfo, nodeSnapshot });
        });
      }
    );

    req.on('error', (err) => finish(null, err));
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      finish({ selfInfo: null, nodeSnapshot: null });
    });
    req.end();
  });
}

function sendStatusMessage(message) {
  if (!message) return;
  lastStatusMessage = message;
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('status:message', message);
  }
  if (settingsWindow && !settingsWindow.isDestroyed()) {
    settingsWindow.webContents.send('settings:status', message);
  }
}

function sendRxPulse() {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  const now = Date.now();
  if (now - lastRxAt < 150) return;
  lastRxAt = now;
  mainWindow.webContents.send('status:rx', { at: now });
}

function sendAprsPulse() {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  mainWindow.webContents.send('status:aprs-rx', { at: Date.now() });
}

function stopSseListener() {
  if (sseRetryTimer) {
    clearTimeout(sseRetryTimer);
    sseRetryTimer = null;
  }
  if (sseRequest) {
    try {
      sseRequest.destroy();
    } catch {
      // ignore
    }
    sseRequest = null;
  }
  if (sseResponse) {
    try {
      sseResponse.destroy();
    } catch {
      // ignore
    }
    sseResponse = null;
  }
}

function scheduleSseReconnect(delayMs = 2000) {
  if (shuttingDown) return;
  if (sseRetryTimer) {
    clearTimeout(sseRetryTimer);
  }
  sseRetryTimer = setTimeout(() => {
    sseRetryTimer = null;
    startSseListener();
  }, delayMs);
}

function handleSseEvent(event) {
  if (!event || typeof event !== 'object') return;
  const type = event.type;
  if (type === 'summary' || type === 'message-append' || type === 'telemetry-append') {
    sendRxPulse();
    return;
  }
  if (type === 'callmesh') {
    lastCallmeshPayload = event.payload || null;
    return;
  }
  if (type === 'aprs') {
    const direction = event?.payload?.direction;
    if (direction === 'downlink') {
      sendAprsPulse();
    }
  }
}

function startSseListener() {
  if (sseRequest || sseResponse) return;
  if (!lastWebDashboardEnabled) return;
  const url = buildWebUrl();
  let target;
  try {
    target = new URL('/api/events', url);
  } catch {
    scheduleSseReconnect(3000);
    return;
  }
  const client = target.protocol === 'https:' ? https : http;
  sseRequest = client.request(
    {
      method: 'GET',
      hostname: target.hostname,
      port: target.port,
      path: target.pathname + target.search,
      headers: { Accept: 'text/event-stream' }
    },
    (res) => {
      if (res.statusCode && res.statusCode >= 400) {
        res.resume();
        stopSseListener();
        scheduleSseReconnect(3000);
        return;
      }
      sseResponse = res;
      res.setEncoding('utf8');
      let buffer = '';
      let dataLines = [];
      res.on('data', (chunk) => {
        buffer += chunk;
        let idx;
        while ((idx = buffer.indexOf('\n')) >= 0) {
          const line = buffer.slice(0, idx).replace(/\r$/, '');
          buffer = buffer.slice(idx + 1);
          if (!line) {
            if (dataLines.length) {
              const payload = dataLines.join('\n');
              dataLines = [];
              try {
                const event = JSON.parse(payload);
                handleSseEvent(event);
              } catch {
                // ignore parse errors
              }
            }
            continue;
          }
          if (line.startsWith(':')) {
            continue;
          }
          if (line.startsWith('data:')) {
            dataLines.push(line.slice(5).trim());
          }
        }
      });
      res.on('end', () => {
        stopSseListener();
        scheduleSseReconnect(2000);
      });
      res.on('error', () => {
        stopSseListener();
        scheduleSseReconnect(2000);
      });
    }
  );

  sseRequest.on('error', () => {
    stopSseListener();
    scheduleSseReconnect(2500);
  });
  sseRequest.end();
}

async function loadStatusInMainWindow() {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  await mainWindow.loadFile(path.join(__dirname, 'status.html'));
  mainWindow.setTitle('TMAG 狀態');
}

async function openSettingsWindow() {
  if (settingsWindow && !settingsWindow.isDestroyed()) {
    settingsWindow.focus();
    return;
  }
  settingsWindow = new BrowserWindow({
    width: 980,
    height: 740,
    minWidth: 860,
    minHeight: 620,
    backgroundColor: '#0a0c10',
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      preload: path.join(__dirname, 'settings-preload.js')
    }
  });
  settingsWindow.setMenuBarVisibility(false);
  await settingsWindow.loadFile(path.join(__dirname, 'settings.html'));
  settingsWindow.on('closed', () => {
    settingsWindow = null;
  });
}

async function stopBackend() {
  if (!backendProcess) return;
  stopSseListener();
  const proc = backendProcess;
  backendProcess = null;
  await new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      resolve();
    };
    proc.once('exit', finish);
    try {
      proc.kill('SIGTERM');
    } catch {
      finish();
      return;
    }
    setTimeout(() => {
      try {
        proc.kill('SIGKILL');
      } catch {
        // ignore
      }
      finish();
    }, 4000);
  });
}

async function restartBackend() {
  await stopBackend();
  await startBackend();
  sendStatusMessage('後端已重新啟動，請在瀏覽器開啟 Web UI。');
}

async function resetDataStore() {
  const dataDir = getDataDir();
  await stopBackend();
  await fs.rm(dataDir, { recursive: true, force: true });
  await fs.mkdir(dataDir, { recursive: true });
  backendHasApiKey = false;
  await startBackend();
  sendStatusMessage('已重置資料，請重新設定 API Key 與連線參數。');
}

async function createWindow() {
  mainWindow = new BrowserWindow({
    width: 480,
    height: 240,
    minWidth: 420,
    minHeight: 200,
    show: false,
    backgroundColor: '#0f1115',
    frame: false,
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      preload: path.join(__dirname, 'status-preload.js')
    }
  });

  const menuTemplate = [
    {
      label: 'TMAG',
      submenu: [
        {
          label: '設定',
          accelerator: 'CmdOrCtrl+,',
          click: () => openSettingsWindow()
        },
        {
          label: '在瀏覽器開啟 Web UI',
          accelerator: 'CmdOrCtrl+Shift+W',
          click: () => shell.openExternal(buildWebUrl())
        },
        { type: 'separator' },
        { role: 'quit' }
      ]
    },
    {
      label: '檢視',
      submenu: [
        { role: 'reload' },
        { role: 'toggledevtools' }
      ]
    }
  ];
  Menu.setApplicationMenu(Menu.buildFromTemplate(menuTemplate));
  mainWindow.setMenuBarVisibility(false);

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  mainWindow.once('ready-to-show', () => {
    if (!mainWindow) return;
    mainWindow.show();
  });

  await loadStatusInMainWindow();
}

async function initialiseApp() {
  await startBackend();
  await createWindow();

  app.on('activate', async () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      await createWindow();
    }
  });
}

app.on('before-quit', () => {
  shuttingDown = true;
  if (backendProcess && !backendProcess.killed) {
    try {
      backendProcess.kill('SIGTERM');
    } catch (err) {
      console.warn('關閉後端程序失敗:', err);
    }
  }
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.whenReady().then(() => {
  initialiseApp().catch((err) => {
    console.error('初始化失敗:', err);
  });
});

ipcMain.handle('settings:get', async () => {
  const preferences = await loadClientPreferences();
  const verificationPath =
    process.env.CALLMESH_VERIFICATION_FILE || path.join(getDataDir(), MONITOR_FILENAME);
  const verification = (await readJsonSafe(verificationPath)) || {};
  const apiKey = await loadPersistedApiKey({ verificationPath });
  return {
    apiKey: apiKey || '',
    preferences: {
      connectionMode: preferences.connectionMode || 'tcp',
      host: preferences.host || '',
      serialPath: preferences.serialPath || '',
      serialBaudRate: preferences.serialBaudRate || '',
      webDashboardEnabled: preferences.webDashboardEnabled !== false,
      shareWithTenmanMap: preferences.shareWithTenmanMap !== false,
      autoTracerouteEnabled: preferences.autoTracerouteEnabled !== false,
      tracerouteRateMinutes: preferences.tracerouteRateMinutes || null,
      tracerouteIntervalSeconds: preferences.tracerouteIntervalSeconds || null,
      aprsServer: preferences.aprsServer || null,
      aprsBeaconMinutes: preferences.aprsBeaconMinutes || null
    },
    verification: {
      verified: Boolean(verification.verified),
      degraded: Boolean(verification.degraded),
      lastHeartbeatAt: verification.lastHeartbeatAt || null
    },
    web: {
      host: resolveUiHost(),
      port: backendPort || DEFAULT_WEB_PORT
    }
  };
});

ipcMain.handle('status:get', async () => {
  const preferences = await loadClientPreferences();
  return {
    appVersion,
    backendRunning: Boolean(backendProcess && !backendProcess.killed),
    backendPid: backendProcess?.pid || null,
    hasApiKey: backendHasApiKey,
    lastStatusMessage,
    callmesh: lastCallmeshPayload,
    preferences: {
      connectionMode: preferences.connectionMode || 'tcp',
      host: preferences.host || '',
      serialPath: preferences.serialPath || '',
      webDashboardEnabled: preferences.webDashboardEnabled !== false
    },
    web: {
      url: buildWebUrl(),
      host: resolveUiHost(),
      port: backendPort || DEFAULT_WEB_PORT
    }
  };
});

ipcMain.handle('status:get-self', async () => {
  const now = Date.now();
  if (cachedSelfFetchedAt && now - cachedSelfFetchedAt < SELF_CACHE_TTL_MS) {
    return {
      selfInfo: cachedSelfInfo,
      nodeSnapshot: cachedNodeSnapshot
    };
  }
  try {
    const payload = await fetchSelfSnapshot(buildWebUrl());
    cachedSelfFetchedAt = Date.now();
    if (payload?.selfInfo) {
      cachedSelfInfo = payload.selfInfo;
    }
    if (payload?.nodeSnapshot) {
      cachedNodeSnapshot = payload.nodeSnapshot;
    }
    return {
      selfInfo: cachedSelfInfo,
      nodeSnapshot: cachedNodeSnapshot
    };
  } catch (err) {
    return { error: err.message || String(err) };
  }
});

ipcMain.handle('status:open-settings', async () => {
  await openSettingsWindow();
  return { success: true };
});

ipcMain.handle('status:open-web', async () => {
  const url = buildWebUrl();
  await shell.openExternal(url);
  return { success: true, url };
});

ipcMain.handle('status:window-minimize', async () => {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.minimize();
  }
  return { success: true };
});

ipcMain.handle('status:window-close', async () => {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.close();
  }
  return { success: true };
});

ipcMain.handle('status:resize', async (_event, payload) => {
  if (!mainWindow || mainWindow.isDestroyed()) {
    return { success: false };
  }
  const height = Number(payload?.height);
  if (!Number.isFinite(height) || height <= 0) {
    return { success: false };
  }
  const [currentWidth] = mainWindow.getContentSize();
  const nextHeight = Math.max(Math.round(height), 200);
  mainWindow.setContentSize(currentWidth, nextHeight, true);
  return { success: true };
});

ipcMain.handle('settings:save', async (_event, payload) => {
  if (!payload || typeof payload !== 'object') {
    return { success: false, error: 'invalid payload' };
  }

  const verificationPath =
    process.env.CALLMESH_VERIFICATION_FILE || path.join(getDataDir(), MONITOR_FILENAME);
  const apiKeyAction = payload.apiKeyAction || 'keep';
  const wantsRestart = Boolean(payload.restart);
  const existingRecord = (await readJsonSafe(verificationPath)) || {};
  const previousKey = typeof existingRecord.apiKey === 'string' ? existingRecord.apiKey.trim() : '';
  let statusText = '';
  let verified = false;
  let degraded = false;

  if (apiKeyAction === 'set') {
    const value = typeof payload.apiKey === 'string' ? payload.apiKey.trim() : '';
    if (!value) {
      return { success: false, error: 'API Key 不可為空' };
    }
    const allowDegraded = Boolean(existingRecord.verified && previousKey && previousKey === value);
    const verifyResult = await verifyCallmeshApiKey(value, { allowDegraded });
    if (!verifyResult.success) {
      const failureMessage = verifyResult.error?.message || '驗證失敗';
      statusText = `API Key 驗證失敗：${failureMessage}`;
      // 清除無效 key
      const nextRecord = { ...existingRecord, apiKey: '', verified: false, degraded: false };
      nextRecord.lastHeartbeatAt = null;
      await fs.mkdir(path.dirname(verificationPath), { recursive: true });
      await fs.writeFile(verificationPath, JSON.stringify(nextRecord, null, 2), 'utf8');
      backendHasApiKey = false;
      return { success: false, error: failureMessage, statusText };
    }

    const nowIso = new Date().toISOString();
    const nextRecord = { ...existingRecord };
    nextRecord.apiKey = value;
    nextRecord.verified = true;
    nextRecord.verifiedAt = nextRecord.verifiedAt || nowIso;
    nextRecord.degraded = Boolean(verifyResult.degraded);
    if (!nextRecord.degraded) {
      nextRecord.lastHeartbeatAt = nowIso;
    }
    await fs.mkdir(path.dirname(verificationPath), { recursive: true });
    await fs.writeFile(verificationPath, JSON.stringify(nextRecord, null, 2), 'utf8');
    backendHasApiKey = true;
    verified = true;
    degraded = Boolean(verifyResult.degraded);
    statusText = degraded ? 'CallMesh 暫時無回應（已套用 Key）' : 'API Key 驗證成功';
  } else if (apiKeyAction === 'clear') {
    const nextRecord = { ...existingRecord, apiKey: '', verified: false, degraded: false };
    nextRecord.lastHeartbeatAt = null;
    await fs.mkdir(path.dirname(verificationPath), { recursive: true });
    await fs.writeFile(verificationPath, JSON.stringify(nextRecord, null, 2), 'utf8');
    backendHasApiKey = false;
    statusText = 'API Key 已清除';
  }

  await saveClientPreferences({
    connectionMode: payload.connectionMode,
    host: payload.host,
    serialPath: payload.serialPath,
    serialBaudRate: payload.serialBaudRate,
    webDashboardEnabled: payload.webDashboardEnabled,
    shareWithTenmanMap: payload.shareWithTenmanMap,
    autoTracerouteEnabled: payload.autoTracerouteEnabled,
    tracerouteRateMinutes: payload.tracerouteRateMinutes,
    tracerouteIntervalSeconds: payload.tracerouteIntervalSeconds,
    aprsServer: payload.aprsServer,
    aprsBeaconMinutes: payload.aprsBeaconMinutes
  });

  if (wantsRestart) {
    await restartBackend();
  }

  return {
    success: true,
    statusText: statusText || '已儲存',
    verified,
    degraded
  };
});

ipcMain.handle('settings:restart-backend', async () => {
  await restartBackend();
  return { success: true };
});

ipcMain.handle('settings:open-web', async () => {
  const url = buildWebUrl();
  await shell.openExternal(url);
  return { success: true, url };
});

ipcMain.handle('settings:list-serial', async () => {
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

ipcMain.handle('settings:discover', async (_event, options) => {
  const devices = await discoverMeshtasticDevices(options);
  return devices;
});

ipcMain.handle('settings:reset-data', async () => {
  await resetDataStore();
  return { success: true };
});
