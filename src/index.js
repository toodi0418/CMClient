'use strict';

const fs = require('fs/promises');
const { existsSync } = require('fs');
const path = require('path');
const os = require('os');
const { hideBin } = require('yargs/helpers');
const yargs = require('yargs/yargs');
const MeshtasticClient = require('./meshtasticClient');
const { discoverMeshtasticDevices } = require('./discovery');
const { CallMeshClient, buildAgentString } = require('./callmesh/client');
const { CallMeshAprsBridge, normalizeMeshId } = require('./callmesh/aprsBridge');
const { WebDashboardServer } = require('./web/server');
const pkg = require('../package.json');

const MESSAGE_LOG_FILENAME = 'message-log.jsonl';
let bridgeSummaryListener = null;

function getMessageLogPath() {
  return path.join(getArtifactsDir(), MESSAGE_LOG_FILENAME);
}

function tryPublishWebMessage(webServer, summary) {
  if (!webServer || !summary || typeof summary !== 'object') {
    return;
  }
  const type = typeof summary.type === 'string' ? summary.type.trim().toLowerCase() : '';
  if (type !== 'text') {
    return;
  }
  const channelId = Number(summary.channel);
  if (!Number.isFinite(channelId) || channelId < 0) {
    return;
  }
  const sanitizeStringArray = (arr) =>
    Array.isArray(arr)
      ? arr
          .map((line) => (typeof line === 'string' ? line.trim() : ''))
          .filter(Boolean)
      : [];
  const detail = typeof summary.detail === 'string' ? summary.detail : '';
  const extraLines = sanitizeStringArray(summary.extraLines);
  const timestampMs = Number.isFinite(summary.timestampMs) ? Number(summary.timestampMs) : Date.now();
  const timestampLabel =
    typeof summary.timestampLabel === 'string' && summary.timestampLabel.trim()
      ? summary.timestampLabel.trim()
      : new Date(timestampMs).toISOString();
  const flowId =
    typeof summary.flowId === 'string' && summary.flowId.trim()
      ? summary.flowId.trim()
      : `${channelId}-${timestampMs}-${Math.random().toString(16).slice(2, 10)}`;
  const entry = {
    type: 'Text',
    channel: channelId,
    detail,
    extraLines,
    from: summary.from ? { ...summary.from } : null,
    relay: summary.relay ? { ...summary.relay } : null,
    relayMeshId: summary.relay?.meshId ?? summary.relayMeshId ?? null,
    relayMeshIdNormalized: summary.relay?.meshIdNormalized ?? summary.relayMeshIdNormalized ?? null,
    hops: summary.hops ? { ...summary.hops } : null,
    timestampMs,
    timestampLabel,
    flowId,
    meshPacketId: Number.isFinite(summary.meshPacketId) ? Number(summary.meshPacketId) : null,
    replyId: Number.isFinite(summary.replyId) ? Number(summary.replyId) : null,
    replyTo: typeof summary.replyTo === 'string' ? summary.replyTo : null,
    scope: typeof summary.scope === 'string' ? summary.scope : null,
    synthetic: Boolean(summary.synthetic)
  };
  try {
    webServer.publishMessage(entry);
  } catch (err) {
    console.warn(`推播文字訊息失敗：${err.message}`);
  }
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

function toWebCallmeshState(state) {
  if (!state || typeof state !== 'object') {
    return null;
  }
  return {
    statusText: state.lastStatus,
    agent: state.agent,
    hasKey: Boolean(state.verified && state.apiKey),
    verified: Boolean(state.verified),
    degraded: Boolean(state.degraded),
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

async function main() {
  await yargs(hideBin(process.argv))
    .version(pkg.version || 'unknown')
    .command(
      'discover',
      '自動搜尋區網內的 Meshtastic TCP 裝置',
      (cmd) =>
        cmd.option('timeout', {
          alias: 't',
          type: 'number',
          default: 5000,
          describe: '掃描持續時間 (毫秒)'
        }),
      async (argv) => {
        const devices = await discoverMeshtasticDevices({ timeout: argv.timeout });
        if (!devices.length) {
          console.log('未發現 Meshtastic 裝置，可確認裝置是否與此機器在同一網段。');
          return;
        }
        console.log(`在 ${argv.timeout}ms 內發現 ${devices.length} 台裝置：`);
        for (const device of devices) {
          console.log(`- ${device.name || '(未命名)'} @ ${device.host}:${device.port}`);
          if (device.txt) {
            const entries = Object.entries(device.txt)
              .filter(([, value]) => value)
              .map(([key, value]) => `${key}: ${value}`);
            if (entries.length) {
              console.log(`  ${entries.join(', ')}`);
            }
          }
          if (device.addresses?.length) {
            console.log(`  addresses: ${device.addresses.join(', ')}`);
          }
        }
      }
    )
    .command(
      '$0',
      '連線並監看 Meshtastic 封包',
      (cmd) =>
        cmd
          .option('api-key', {
            alias: 'K',
            type: 'string',
            describe: 'CallMesh API Key（若未帶入將使用環境變數 CALLMESH_API_KEY）'
          })
          .option('host', {
            alias: 'H',
            type: 'string',
            default: '127.0.0.1',
            describe: 'Meshtastic TCP 伺服器主機位置'
          })
          .option('port', {
            alias: 'P',
            type: 'number',
            default: 4403,
            describe: 'Meshtastic TCP 伺服器埠號'
          })
          .option('connection', {
            alias: 'C',
            choices: ['tcp', 'serial'],
            describe: 'Meshtastic 連線方式（未指定時會依 host 判斷）'
          })
          .option('serial-path', {
            type: 'string',
            describe: 'Serial 連線時的裝置路徑（例如 /dev/ttyUSB0）'
          })
          .option('serial-baud', {
            type: 'number',
            default: 115200,
            describe: 'Serial 連線時的鮑率'
          })
          .option('no-share-with-tenmanmap', {
            type: 'boolean',
            describe: '停用 TenManMap 分享（預設為啟用）'
          })
          .option('max-length', {
            alias: 'm',
            type: 'number',
            default: 512,
            describe: '允許的最大封包大小 (位元組)'
          })
          .option('show-raw', {
            alias: 'r',
            type: 'boolean',
            default: false,
            describe: '在摘要輸出時同時列印 payload 十六進位'
          })
          .option('format', {
            alias: 'f',
            choices: ['summary', 'json'],
            default: 'summary',
            describe: '輸出格式：summary 顯示表格，json 顯示完整資料'
          })
          .option('pretty', {
            alias: 'p',
            type: 'boolean',
            default: true,
            describe: '搭配 --format json 時使用縮排輸出'
          })
          .option('web-ui', {
            type: 'boolean',
            default: false,
            describe: '啟用內建 Web Dashboard（預設為關閉）'
          }),
      async (argv) => {
        await startMonitor(argv);
      }
    )
    .strict()
    .help()
    .parse();
}

async function startMonitor(argv) {
  const apiKey = argv.apiKey || process.env.CALLMESH_API_KEY;
  if (!apiKey) {
    console.error('未設定 CallMesh API Key，請先設定環境變數 CALLMESH_API_KEY 後再執行。');
    process.exitCode = 1;
    return;
  }

  const webUiEnv = process.env.TMAG_WEB_DASHBOARD;
  const webUiEnabled = webUiEnv === '1' ? true : webUiEnv === '0' ? false : Boolean(argv.webUi);
  let webServer = null;

  const verificationPath = getVerificationPath();
  const previousVerification = await loadVerificationFile(verificationPath);
  const previouslyVerified =
    previousVerification &&
    previousVerification.apiKey === apiKey &&
    previousVerification.verified === true;

  const HEARTBEAT_INTERVAL_MS = 60_000;
  const HEARTBEAT_INTERVAL_SECONDS = HEARTBEAT_INTERVAL_MS / 1000;
  const artifactsDir = getArtifactsDir();
  const shareWithTenmanMapOverride =
    argv.noShareWithTenmanmap ? false : null;

  const bridgeOptions = {
    storageDir: artifactsDir,
    appVersion: pkg.version || '0.0.0',
    apiKey: previouslyVerified ? apiKey : '',
    verified: previouslyVerified,
    heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
    agentProduct: 'callmesh-client-cli'
  };
  if (shareWithTenmanMapOverride !== null) {
    bridgeOptions.shareWithTenmanMap = shareWithTenmanMapOverride;
  }

  const bridge = new CallMeshAprsBridge(bridgeOptions);

  const verificationRecord = { ...(previousVerification || {}) };
  verificationRecord.apiKey = apiKey;
  let lastPersistedHeartbeat = verificationRecord.lastHeartbeatAt || null;
  let lastDegradedFlag = Boolean(verificationRecord.degraded);
  let selfMeshId = null;

  const formatTimestampLabel = (date) => {
    const pad = (value, length = 2) => String(value).padStart(length, '0');
    const year = date.getFullYear();
    const month = pad(date.getMonth() + 1);
    const day = pad(date.getDate());
    const hours = pad(date.getHours());
    const minutes = pad(date.getMinutes());
    const seconds = pad(date.getSeconds());
    return `${year}/${month}/${day} ${hours}:${minutes}:${seconds}`;
  };

  bridge.on('log', (entry) => {
    const timestamp = entry?.timestamp ? new Date(entry.timestamp) : new Date();
    const timeLabel = formatTimestampLabel(timestamp);
    const prefix = entry.tag ? `[${entry.tag}]` : '[LOG]';
    console.log(`[${timeLabel}] ${prefix} ${entry.message}`);
    webServer?.publishLog(entry);
  });

  bridge.on('aprs-uplink', (info) => {
    if (!info || !info.frame) return;
    const timestamp = info?.timestamp ? new Date(info.timestamp) : new Date();
    const timeLabel = formatTimestampLabel(timestamp);
    console.log(`[${timeLabel}] [APRS] ${info.frame}`);
    webServer?.publishAprs(info);
  });

  bridge.on('state', (state) => {
    if (!state || state.apiKey !== apiKey) return;
    let shouldPersist = false;
    if (state.lastHeartbeatAt && state.lastHeartbeatAt !== lastPersistedHeartbeat) {
      lastPersistedHeartbeat = state.lastHeartbeatAt;
      verificationRecord.lastHeartbeatAt = state.lastHeartbeatAt;
      verificationRecord.verified = Boolean(state.verified);
      shouldPersist = true;
    }
    const degradedFlag = Boolean(state.degraded);
    if (degradedFlag !== lastDegradedFlag) {
      lastDegradedFlag = degradedFlag;
      verificationRecord.degraded = degradedFlag;
      verificationRecord.verified = Boolean(state.verified);
      shouldPersist = true;
    }
    if (webServer) {
      const payload = toWebCallmeshState(state);
      if (payload) {
        webServer.publishCallmesh(payload);
      }
    }
    if (shouldPersist) {
      saveVerificationFile(verificationPath, verificationRecord).catch((err) => {
        console.warn(`寫入驗證紀錄失敗：${err.message}`);
      });
    }
  });

  bridge.on('telemetry', (payload) => {
    webServer?.publishTelemetry(payload);
  });

  bridge.on('node', (payload) => {
    if (payload) {
      webServer?.publishNode(payload);
    }
  });

  const connectionOptions = {};

  await bridge.init({ allowRestore: true });
  const sharedDataStore = bridge.getDataStore?.();
  if (sharedDataStore) {
    connectionOptions.relayStatsStore = sharedDataStore;
  }

  try {
    const verifyResult = await bridge.verifyApiKey(apiKey, {
      allowDegraded: previouslyVerified
    });

    if (!verifyResult.success) {
      if (verifyResult.authError) {
        console.error(`CallMesh API Key 驗證失敗：${verifyResult.error?.message ?? 'unauthorised'}`);
        process.exitCode = 1;
        return;
      }
      throw verifyResult.error || new Error('CallMesh 驗證失敗');
    }

    const nowIso = new Date().toISOString();
    verificationRecord.verified = true;
    verificationRecord.verifiedAt = verificationRecord.verifiedAt || nowIso;
    verificationRecord.degraded = Boolean(verifyResult.degraded);

    if (verifyResult.degraded) {
      console.warn(
        `CallMesh 伺服器暫時無回應 (${verifyResult.error?.message ?? 'unknown'})，沿用先前驗證結果。`
      );
    } else {
      verificationRecord.lastHeartbeatAt = nowIso;
    }

    await saveVerificationFile(verificationPath, verificationRecord);
  } catch (err) {
    if (isAuthError(err)) {
      console.error(`CallMesh API Key 驗證失敗：${err.message}`);
      process.exitCode = 1;
      return;
    }

    if (previouslyVerified) {
      console.warn(`CallMesh 伺服器暫時無回應 (${err.message})，沿用先前驗證結果。`);
      verificationRecord.verified = true;
      verificationRecord.degraded = true;
      await saveVerificationFile(verificationPath, verificationRecord);
    } else {
      console.error(`CallMesh 伺服器無法連線：${err.message}`);
      process.exitCode = 1;
      return;
    }
  }

  bridge.startHeartbeatLoop();
  bridge.performHeartbeatTick().catch((err) => {
    console.error('CallMesh Heartbeat 失敗：', err);
  });

  const relayStatsPath = path.join(getArtifactsDir(), 'relay-link-stats.json');

  if (webUiEnabled && !webServer) {
    try {
      const webDashboardOptions = {
        appVersion: pkg.version || '0.0.0',
        relayStatsPath,
        relayStatsStore: sharedDataStore,
        messageLogPath: getMessageLogPath(),
        messageLogStore: sharedDataStore,
        telemetryProvider: bridge
      };
      const telemetryMaxTotalOverride = resolveTelemetryMaxTotalRecords();
      if (telemetryMaxTotalOverride) {
        webDashboardOptions.telemetryMaxTotalRecords = telemetryMaxTotalOverride;
      }
      webServer = new WebDashboardServer(webDashboardOptions);
      await webServer.start();
      webServer.setAppVersion(pkg.version || '0.0.0');
      webServer.publishStatus({ status: 'disconnected' });
      const snapshot = toWebCallmeshState(bridge.getStateSnapshot());
      if (snapshot) {
        webServer.publishCallmesh(snapshot);
      }
      try {
        const nodeSnapshot = bridge.getNodeSnapshot();
        if (Array.isArray(nodeSnapshot) && nodeSnapshot.length) {
          webServer.seedNodeSnapshot(nodeSnapshot);
        }
      } catch (err) {
        console.warn(`初始化 Web 節點快照失敗：${err.message}`);
      }
      // 避免同步讀取大量遙測資料阻塞事件迴圈，啟動時改以背景程序載入 Telemetry 快照。
      setTimeout(async () => {
        try {
          const snapshot = await bridge.getTelemetrySnapshot({
            limitPerNode: webServer.telemetryMaxPerNode
          });
          webServer.seedTelemetrySnapshot(snapshot);
        } catch (err) {
          console.warn(`初始化 Web 遙測快照失敗：${err.message}`);
        }
      }, 0);
    } catch (err) {
      console.warn(`啟動 Web Dashboard 失敗：${err.message}`);
      webServer = null;
    }
  }

  const DEFAULT_SERIAL_BAUD = 115_200;

  const parseSerialEndpoint = (input) => {
    if (!input || typeof input !== 'string') return null;
    const trimmed = input.trim();
    if (!trimmed.toLowerCase().startsWith('serial:')) {
      return null;
    }
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
          const value = Number(params.get(key));
          if (Number.isFinite(value) && value > 0) {
            baudRate = value;
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

  const hostInput = typeof argv.host === 'string' ? argv.host.trim() : '';
  const normalizedConnectionArg =
    typeof argv.connection === 'string' ? argv.connection.trim().toLowerCase() : '';
  const serialSpec = parseSerialEndpoint(hostInput);
  let transport = 'tcp';
  if (normalizedConnectionArg === 'serial') {
    transport = 'serial';
  } else if (normalizedConnectionArg === 'tcp') {
    transport = 'tcp';
  } else if (
    (serialSpec && serialSpec.path) ||
    (typeof argv.serialPath === 'string' && argv.serialPath.trim())
  ) {
    transport = 'serial';
  }

  let serialPath = typeof argv.serialPath === 'string' ? argv.serialPath.trim() : '';
  if (!serialPath && serialSpec?.path) {
    serialPath = serialSpec.path;
  }
  let serialBaudRate = DEFAULT_SERIAL_BAUD;
  if (serialSpec?.baudRate) {
    serialBaudRate = serialSpec.baudRate;
  }
  const baudArg = Number(argv.serialBaud);
  if (Number.isFinite(baudArg) && baudArg > 0) {
    serialBaudRate = baudArg;
  }

  if (transport === 'serial' && !serialPath) {
    console.error(
      'Serial 連線時必須指定裝置路徑，可使用 --serial-path 或 host=serial://<device>'
    );
    process.exitCode = 1;
    return;
  }
  if (transport === 'tcp' && serialSpec && normalizedConnectionArg === 'tcp') {
    console.error(
      'host 使用 serial:// 前綴時需搭配 --connection serial，或請改用一般 TCP 主機位址。'
    );
    process.exitCode = 1;
    return;
  }

  const tcpHost = hostInput || '127.0.0.1';
  const tcpPort = Number.isFinite(argv.port) ? argv.port : 4403;
  const connectionSummary =
    transport === 'serial'
      ? `Serial ${serialPath} @ ${serialBaudRate}`
      : `TCP ${tcpHost}:${tcpPort}`;

  Object.assign(connectionOptions, {
    transport,
    maxLength: argv.maxLength,
    handshake: true,
    heartbeat: HEARTBEAT_INTERVAL_SECONDS,
    relayStatsPath
  });
  if (transport === 'serial') {
    Object.assign(connectionOptions, {
      serialPath,
      serialBaudRate,
      keepAlive: false,
      idleTimeoutMs: 0
    });
  } else {
    Object.assign(connectionOptions, {
      host: tcpHost,
      port: tcpPort,
      keepAlive: true,
      keepAliveDelayMs: 15_000,
      idleTimeoutMs: 90_000
    });
  }

  const RECONNECT_DELAY_MS = 30_000;
  let reconnectTimer = null;
  let currentClient = null;
  let headerPrinted = false;
  let stopRequested = false;

  const clearReconnectTimer = () => {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  };

  let bridgeDestroyed = false;
  const destroyBridge = () => {
    if (bridgeDestroyed) return;
    bridgeDestroyed = true;
    bridge.destroy();
  };

  const stopAll = () => {
    if (stopRequested) return;
    stopRequested = true;
    clearReconnectTimer();
    if (currentClient) {
      try {
        currentClient.stop();
      } catch {
        // ignore
      }
    }
    destroyBridge();
    if (webServer) {
      try {
        webServer.publishStatus({ status: 'disconnected' });
      } catch {
        // ignore publish errors during shutdown
      }
      webServer
        .stop()
        .catch(() => {
          // ignore stop errors
        });
      webServer = null;
    }
  };

  process.once('SIGINT', () => {
    const label = formatTimestampLabel(new Date());
    console.log(`[${label}] [MESHTASTIC] 收到中斷訊號，準備關閉`);
    stopAll();
  });

  const logWithTag = (tag, message) => {
    const label = formatTimestampLabel(new Date());
    console.log(`[${label}] [${tag}] ${message}`);
  };

  const waitForDelay = async (ms) => {
    if (stopRequested) return;
    await new Promise((resolve) => {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        resolve();
      }, ms);
    });
  };

  const runClientOnce = async (attempt) => {
    headerPrinted = false;
    selfMeshId = null;
    const client = new MeshtasticClient(connectionOptions);
    currentClient = client;
    bridge.attachMeshtasticClient(client);
    webServer?.publishStatus({ status: 'connecting' });

    return new Promise((resolve) => {
      let settled = false;
      const cleanup = () => {
        if (settled) return;
        settled = true;
        if (bridgeSummaryListener && typeof bridge.removeListener === 'function') {
          bridge.removeListener('summary', bridgeSummaryListener);
          bridgeSummaryListener = null;
        }
        bridge.detachMeshtasticClient(client);
        client.removeAllListeners();
        try {
          client.stop();
        } catch {
          // ignore
        }
        if (currentClient === client) {
          currentClient = null;
        }
        resolve();
      };

      client.on('connected', () => {
        logWithTag(
          'MESHTASTIC',
          `已連線至 ${connectionSummary} (嘗試第 ${attempt} 次)，開始接收封包`
        );
        webServer?.publishStatus({ status: 'connected' });
      });

      client.on('disconnected', () => {
        logWithTag('MESHTASTIC', '連線已關閉');
        webServer?.publishStatus({ status: 'disconnected' });
        cleanup();
      });

      client.on('handshake', ({ nonce }) => {
        logWithTag('MESHTASTIC', `已送出 want_config 請求 (nonce=${nonce})`);
      });

      client.on('error', (err) => {
        logWithTag('MESHTASTIC', `錯誤：${err.message}`);
        webServer?.publishStatus({ status: 'error', message: err.message });
      });

  const handleSummary = (summary, { synthetic = false } = {}) => {
    if (!summary) return;
    if (!synthetic) {
      bridge.handleMeshtasticSummary(summary);
    }
    webServer?.publishSummary(summary);
    tryPublishWebMessage(webServer, summary);

    if (argv.format !== 'summary') {
      return;
    }

    if (!headerPrinted) {
      console.log('Date               | Nodes                      | Relay        | Ch |   SNR | RSSI | Type         | Hops   | Details');
      console.log('-------------------+---------------------------+--------------+----+-------+------+--------------+--------+------------------------------');
      headerPrinted = true;
    }

    const nodesLabel = formatNodes(summary);
    const relayLabel = computeRelayLabel(summary, { selfMeshId });
    const relayCol = padEnd(relayLabel, 12);
    const channelCol = padValue(summary.channel ?? '', 2);
    const snrCol = formatSignal(summary.snr, 2, 6, summary, { selfMeshId });
    const rssiCol = formatSignal(summary.rssi, 0, 5, summary, { selfMeshId });
    const typeCol = String(summary.type || '').padEnd(12);
    const hopsCol = (summary.hops?.label || '').padEnd(7);
    const detail = summary.detail || '';

    const line = `${(summary.timestampLabel ?? '').padEnd(19)} | ${nodesLabel.padEnd(27)} | ${relayCol} | ${channelCol} | ${snrCol} | ${rssiCol} | ${typeCol} | ${hopsCol} | ${detail}`;
    console.log(line.trimEnd());

    if (argv['show-raw'] && summary.rawHex) {
      console.log(`  raw: ${summary.rawHex}`);
    }

    if (Array.isArray(summary.extraLines)) {
      for (const extra of summary.extraLines) {
        console.log(`  ${extra}`);
      }
    }
  };

      if (bridgeSummaryListener && typeof bridge.removeListener === 'function') {
        bridge.removeListener('summary', bridgeSummaryListener);
        bridgeSummaryListener = null;
      }
      if (typeof bridge.on === 'function') {
        bridgeSummaryListener = (summary) => {
          handleSummary(summary, { synthetic: true });
        };
        bridge.on('summary', bridgeSummaryListener);
      }

  client.on('summary', (summary) => {
    handleSummary(summary);
  });

  if (argv.format !== 'summary') {
    client.on('fromRadio', ({ message }) => {
      const object = client.toObject(message, {
        bytes: String
      });
      const spacing = argv.pretty ? 2 : 0;
      console.log(JSON.stringify(object, null, spacing));
    });
  }

      client.on('myInfo', (info) => {
        bridge.handleMeshtasticMyInfo(info);
        webServer?.publishSelf(info);
        const meshCandidate = normalizeMeshId(info?.node?.meshId || info?.meshId);
        if (meshCandidate) {
          selfMeshId = meshCandidate;
        }
      });

      client.start().catch((err) => {
        logWithTag('MESHTASTIC', `啟動失敗：${err.message}`);
        webServer?.publishStatus({ status: 'error', message: err.message });
        cleanup();
      });
    });
  };

  let attempt = 0;
  while (!stopRequested) {
    attempt += 1;
    const startLabel = formatTimestampLabel(new Date());
    console.log(
      `[${startLabel}] [MESHTASTIC] 開始連線流程 (第 ${attempt} 次嘗試，目標=${connectionSummary})`
    );
    await runClientOnce(attempt);
    if (stopRequested) {
      break;
    }
    logWithTag('MESHTASTIC', `將在 ${Math.round(RECONNECT_DELAY_MS / 1000)} 秒後重試連線`);
    await waitForDelay(RECONNECT_DELAY_MS);
  }

  stopAll();
}

function formatNodes(summary) {
  const from = summary.from?.label || 'unknown';
  const to = summary.to?.label;
  if (!to) {
    return from;
  }
  return `${from} -> ${to}`;
}

function padValue(value, width) {
  return String(value ?? '').padStart(width);
}

function padEnd(value, width) {
  return String(value ?? '').padEnd(width);
}

function formatRelayLabel(entry) {
  if (!entry || typeof entry !== 'object') return '';
  const candidates = [
    entry.label,
    entry.longName,
    entry.shortName,
    entry.meshId,
    entry.meshIdNormalized
  ];
  let display = '';
  for (const candidate of candidates) {
    if (typeof candidate === 'string' && candidate.trim()) {
      const trimmed = candidate.trim();
      if (trimmed.toLowerCase() === 'unknown') {
        continue;
      }
      display = trimmed;
      break;
    }
  }
  const meshIdRaw =
    (typeof entry.meshId === 'string' && entry.meshId.trim()) ||
    (typeof entry.meshIdNormalized === 'string' && entry.meshIdNormalized.trim()) ||
    '';
  const meshId = meshIdRaw;
  const normalized = meshId.startsWith('!') ? meshId.slice(1) : meshId;
  if (normalized && /^0{6}[0-9a-fA-F]{2}$/.test(normalized.toLowerCase())) {
    return display || meshId || '未知';
  }
  return display || meshId || '未知';
}

function extractHopInfo(summary) {
  const hops = summary?.hops || {};
  const label = typeof hops.label === 'string' ? hops.label.trim() : '';
  const hopStartProvided = hops.start !== undefined && hops.start !== null;
  const hopLimitProvided = hops.limit !== undefined && hops.limit !== null;
  const toFiniteOrNull = (value) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  };
  const hopStart = hopStartProvided ? toFiniteOrNull(hops.start) : null;
  const hopLimit = hopLimitProvided ? toFiniteOrNull(hops.limit) : null;
  const limitOnly =
    Boolean(hops.limitOnly) ||
    (!hopStartProvided && hopLimitProvided && label && !label.includes('/') && !label.includes('?'));

  let used = null;
  let total = hopStart != null ? hopStart : null;

  if (!limitOnly) {
    if (hopStart != null && hopLimit != null) {
      used = Math.max(hopStart - hopLimit, 0);
    } else if (hopStart != null && hopLimit == null) {
      used = 0;
    } else {
      const match = label.match(/^(\d+)\s*\/\s*(\d+)/);
      if (match) {
        used = Number(match[1]);
        if (!Number.isFinite(total)) {
          total = Number(match[2]);
        }
      } else if (/^\d+$/.test(label) && hopStart === 0) {
        used = 0;
        total = Number.isFinite(total) ? total : 0;
      }
    }

    if (!Number.isFinite(total)) {
      const match = label.match(/\/\s*(\d+)/);
      if (match) {
        total = Number(match[1]);
      }
    }
  } else {
    used = null;
    total = null;
  }

  return {
    usedHops: Number.isFinite(used) ? used : null,
    totalHops: Number.isFinite(total) ? total : null,
    hopsLabel: label,
    limitOnly
  };
}

function computeRelayLabel(summary, { selfMeshId } = {}) {
  if (!summary) return '';

  const normalizedSelf = normalizeMeshId(selfMeshId);
  const isSelfMesh = (meshId) => {
    if (!meshId || !normalizedSelf) return false;
    const normalized = normalizeMeshId(meshId);
    return normalized && normalized === normalizedSelf;
  };

  const fromMeshId = summary.from?.meshId || summary.from?.meshIdNormalized || '';
  const fromNormalized = normalizeMeshId(fromMeshId);
  if (fromMeshId && isSelfMesh(fromMeshId)) {
    return 'Self';
  }

  let relayMeshIdRaw = summary.relay?.meshId || summary.relay?.meshIdNormalized || '';
  if (relayMeshIdRaw && isSelfMesh(relayMeshIdRaw)) {
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

  const hopInfo = extractHopInfo(summary);
  if (summary.relayInvalid || hopInfo.limitOnly) {
    return '無效';
  }
  const { usedHops, hopsLabel } = hopInfo;
  const zeroHop =
    usedHops === 0 ||
    hopsLabel === '0/0' ||
    (typeof hopsLabel === 'string' && hopsLabel.startsWith('0/'));

  if (summary.relay?.label) {
    if (zeroHop) {
      return '直收';
    }
    return formatRelayLabel(summary.relay);
  }

  if (relayMeshIdRaw) {
    if (zeroHop) {
      return '直收';
    }
    return formatRelayLabel({
      label: summary.relay?.label || relayMeshIdRaw,
      meshId: relayMeshIdRaw
    });
  }

  if (zeroHop) {
    return '直收';
  }

  if (usedHops > 0) {
    return '未知';
  }

  if (!hopsLabel) {
    return '直收';
  }

  if (typeof hopsLabel === 'string' && hopsLabel.includes('?')) {
    return '未知';
  }

  return '';
}

function shouldSuppressSignal(summary, { selfMeshId } = {}) {
  if (!summary) return false;
  const type = String(summary.type || '').toLowerCase();
  if (
    type.includes('telemetry') ||
    type.includes('status') ||
    type.includes('env') ||
    type.includes('remotehardware') ||
    type.includes('admin')
  ) {
    return true;
  }
  if (!selfMeshId) return false;
  const fromNormalized = normalizeMeshId(summary.from?.meshId || summary.from?.meshIdNormalized);
  return Boolean(fromNormalized && fromNormalized === normalizeMeshId(selfMeshId));
}

function formatSignal(value, digits, width, summary, { selfMeshId } = {}) {
  const suppress = shouldSuppressSignal(summary, { selfMeshId });
  if (value === undefined || value === null || Number.isNaN(value)) {
    return ''.padStart(width);
  }
  if (suppress && Math.abs(value) < Number.EPSILON) {
    return ''.padStart(width);
  }
  return value.toFixed(digits).padStart(width);
}

async function handleCallMeshCommand(argv) {
  const statePath = path.resolve(argv.stateFile);
  const state = await loadState(statePath);

  const agentString = argv.agent || buildAgentString({
    product: argv.product,
    version: argv.clientVersion,
    platformOverride: argv.platform
  });

  const apiKey = argv.apiKey || process.env.CALLMESH_API_KEY;
  if (!apiKey) {
    console.error('CallMesh API Key 未設定，請加上 --api-key 或匯出環境變數 CALLMESH_API_KEY');
    process.exitCode = 1;
    return;
  }

  const client = new CallMeshClient({
    apiKey,
    baseUrl: argv.baseUrl,
    product: argv.product,
    version: argv.clientVersion,
    agent: argv.agent || agentString,
    platform: argv.platform,
    fetchImpl: globalThis.fetch
  });

  const localHash = state.hash ?? null;
  const action = argv.action;

  if (action === 'mappings') {
    await fetchAndStoreMappings({ client, state, statePath, argv, localHash });
    return;
  }

  let heartbeatRes = null;
  try {
    heartbeatRes = await client.heartbeat({ localHash, timeout: argv.timeout });
    console.log('Heartbeat 成功');
    console.log(`  needs_update: ${heartbeatRes.needs_update}`);
    console.log(`  hash: ${heartbeatRes.hash ?? 'null'}`);
    if (heartbeatRes.server_time) {
      console.log(`  server_time: ${heartbeatRes.server_time}`);
    }
    if (heartbeatRes.provision) {
      console.log('  provision 收到，內容已寫入 state');
      state.provision = heartbeatRes.provision;
    }
    state.hash = heartbeatRes.hash ?? state.hash ?? null;
    state.serverTime = heartbeatRes.server_time ?? null;
    state.callmeshVerified = true;
    state.lastVerifiedAt = new Date().toISOString();
  } catch (err) {
    if (isAuthError(err)) {
      console.error(`CallMesh API Key 驗證失敗：${err.message}`);
      process.exitCode = 1;
      return;
    }
    const previouslyVerified = state.callmeshVerified && state.apiKey === apiKey;
    if (previouslyVerified) {
      console.warn(`Heartbeat 無法送出 (${err.message})，沿用上次驗證結果。`);
    } else {
      console.error(`Heartbeat 無法送出：${err.message}`);
      process.exitCode = 1;
      return;
    }
  }

  state.apiKey = apiKey;
  state.lastHeartbeat = new Date().toISOString();
  state.agent = client.agentString;
  state.baseUrl = argv.baseUrl;

  await saveState(statePath, state);

  const shouldUpdateMappings =
    heartbeatRes && action !== 'heartbeat' && (argv.force || heartbeatRes.needs_update || localHash == null);

  if (shouldUpdateMappings) {
    await fetchAndStoreMappings({
      client,
      state,
      statePath,
      argv,
      localHash
    });
  } else if (action !== 'heartbeat' && !heartbeatRes) {
    console.warn('略過 mapping 下載：CallMesh 伺服器暫時無法連線。');
  }
}

async function fetchAndStoreMappings({ client, state, statePath, argv, localHash }) {
  const response = await client.fetchMappings({ knownHash: localHash, timeout: argv.timeout });
  const items = response.items || [];
  console.log(`取得 mapping 成功，共 ${items.length} 筆，hash=${response.hash}`);
  state.hash = response.hash ?? state.hash ?? null;
  state.mappings = items;
  state.mappingsUpdatedAt = new Date().toISOString();
  state.callmeshVerified = true;
  await saveState(statePath, state);

  if (argv.mappingsOutput) {
    const outPath = path.resolve(argv.mappingsOutput);
    await fs.writeFile(outPath, JSON.stringify(response, null, 2), 'utf8');
    console.log(`已將 mapping 寫入 ${outPath}`);
  }
}

async function loadState(filePath) {
  if (!existsSync(filePath)) {
    return {};
  }
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return raw ? JSON.parse(raw) : {};
  } catch (err) {
    console.warn(`讀取 ${filePath} 失敗，將以空白狀態重新建立 (${err.message})`);
    return {};
  }
}

async function saveState(filePath, data) {
  const dir = path.dirname(filePath);
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function getVerificationPath() {
  const custom = process.env.CALLMESH_VERIFICATION_FILE;
  if (custom) return path.resolve(custom);
  return path.resolve(os.homedir(), '.config', 'callmesh', 'monitor.json');
}

function getArtifactsDir() {
  const custom = process.env.CALLMESH_ARTIFACTS_DIR;
  if (custom) return path.resolve(custom);
  return path.dirname(getVerificationPath());
}

async function loadVerificationFile(filePath) {
  if (!existsSync(filePath)) {
    return null;
  }
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

async function saveVerificationFile(filePath, data) {
  const dir = path.dirname(filePath);
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function isAuthError(err) {
  const message = (err?.message || '').toLowerCase();
  return message.includes('401') || message.includes('invalid');
}

main().catch((err) => {
  console.error(`初始化失敗: ${err.message}`);
  process.exitCode = 1;
});
