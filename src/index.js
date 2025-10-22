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
const pkg = require('../package.json');

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

  const verificationPath = getVerificationPath();
  const previousVerification = await loadVerificationFile(verificationPath);
  const previouslyVerified =
    previousVerification &&
    previousVerification.apiKey === apiKey &&
    previousVerification.verified === true;

  const HEARTBEAT_INTERVAL_MS = 60_000;
  const HEARTBEAT_INTERVAL_SECONDS = HEARTBEAT_INTERVAL_MS / 1000;
  const artifactsDir = getArtifactsDir();

  const bridge = new CallMeshAprsBridge({
    storageDir: artifactsDir,
    appVersion: pkg.version || '0.0.0',
    apiKey: previouslyVerified ? apiKey : '',
    verified: previouslyVerified,
    heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
    agentProduct: 'callmesh-client-cli'
  });

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
  });

  bridge.on('aprs-uplink', (info) => {
    if (!info || !info.frame) return;
    const timestamp = info?.timestamp ? new Date(info.timestamp) : new Date();
    const timeLabel = formatTimestampLabel(timestamp);
    console.log(`[${timeLabel}] [APRS] ${info.frame}`);
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
    if (shouldPersist) {
      saveVerificationFile(verificationPath, verificationRecord).catch((err) => {
        console.warn(`寫入驗證紀錄失敗：${err.message}`);
      });
    }
  });

  await bridge.init({ allowRestore: true });

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

  const connectionOptions = {
    host: argv.host,
    port: argv.port,
    maxLength: argv.maxLength,
    handshake: true,
    heartbeat: HEARTBEAT_INTERVAL_SECONDS
  };

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

    return new Promise((resolve) => {
      let settled = false;
      const cleanup = () => {
        if (settled) return;
        settled = true;
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
        logWithTag('MESHTASTIC', `已連線至 ${argv.host}:${argv.port} (嘗試第 ${attempt} 次)，開始接收封包`);
      });

      client.on('disconnected', () => {
        logWithTag('MESHTASTIC', '連線已關閉');
        cleanup();
      });

      client.on('handshake', ({ nonce }) => {
        logWithTag('MESHTASTIC', `已送出 want_config 請求 (nonce=${nonce})`);
      });

      client.on('error', (err) => {
        logWithTag('MESHTASTIC', `錯誤：${err.message}`);
      });

      if (argv.format === 'summary') {
        client.on('summary', (summary) => {
          if (!summary) return;
          bridge.handleMeshtasticSummary(summary);
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

          const line = `${summary.timestampLabel.padEnd(19)} | ${nodesLabel.padEnd(27)} | ${relayCol} | ${channelCol} | ${snrCol} | ${rssiCol} | ${typeCol} | ${hopsCol} | ${detail}`;
          console.log(line.trimEnd());

          if (argv['show-raw'] && summary.rawHex) {
            console.log(`  raw: ${summary.rawHex}`);
          }

          if (Array.isArray(summary.extraLines)) {
            for (const extra of summary.extraLines) {
              console.log(`  ${extra}`);
            }
          }
        });
      } else {
        client.on('summary', (summary) => {
          if (!summary) return;
          bridge.handleMeshtasticSummary(summary);
        });
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
        const meshCandidate = normalizeMeshId(info?.node?.meshId || info?.meshId);
        if (meshCandidate) {
          selfMeshId = meshCandidate;
        }
      });

      client.start().catch((err) => {
        logWithTag('MESHTASTIC', `啟動失敗：${err.message}`);
        cleanup();
      });
    });
  };

  let attempt = 0;
  while (!stopRequested) {
    attempt += 1;
    const startLabel = formatTimestampLabel(new Date());
    console.log(`[${startLabel}] [MESHTASTIC] 開始連線流程 (第 ${attempt} 次嘗試)`);
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
  if (!entry) return '';
  const label = entry.label || '';
  const meshId = entry.meshId || '';
  if (!meshId) return label;
  const normalized = meshId.startsWith('!') ? meshId.slice(1) : meshId;
  if (/^0{6}[0-9a-fA-F]{2}$/.test(normalized)) {
    return label ? `${label}?` : `${meshId}?`;
  }
  return label || meshId;
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

  const { usedHops, hopsLabel } = extractHopInfo(summary);

  if (summary.relay?.label) {
    return formatRelayLabel(summary.relay);
  }

  if (relayMeshIdRaw) {
    return formatRelayLabel({
      label: summary.relay?.label || relayMeshIdRaw,
      meshId: relayMeshIdRaw
    });
  }

  if (usedHops === 0 || hopsLabel === '0/0' || (hopsLabel && hopsLabel.startsWith('0/'))) {
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
