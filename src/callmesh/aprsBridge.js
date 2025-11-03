'use strict';

const path = require('path');
const fs = require('fs/promises');
const fsSync = require('fs');
const EventEmitter = require('events');
const readline = require('readline');
const WebSocket = require('ws');
const { CallMeshClient, buildAgentString } = require('./client');
const { APRSClient } = require('../aprs/client');
const { nodeDatabase } = require('../nodeDatabase');

const DEFAULT_APRS_SERVER = 'asia.aprs2.net';
const DEFAULT_APRS_PORT = 14580;
const DEFAULT_HEARTBEAT_INTERVAL_MS = 60_000;
const DEFAULT_BEACON_INTERVAL_MS = 10 * 60_000;
const MIN_BEACON_INTERVAL_MS = 60_000;
const MAX_BEACON_INTERVAL_MS = 24 * 60 * 60_000;
const APRS_BEACON_DEST = 'APTMAG';
const APRS_BEACON_PATH = 'TCPIP*';
const APRS_STATUS_INTERVAL_MS = 60 * 60_000;
const APRS_MESH_PATH = 'MESHD*';
const APRS_Q_CONSTRUCT = 'qAR';
const APRS_POSITION_CACHE_LIMIT = 256;
const APRS_POSITION_DEDUP_WINDOW_MS = 30_000;
const APRS_TELEMETRY_INTERVAL_MS = 6 * 60 * 60_000;
const APRS_TELEMETRY_DATA_INTERVAL_MS = 10 * 60_000;
const TELEMETRY_BUCKET_MS = 60_000;
const TELEMETRY_WINDOW_MS = APRS_TELEMETRY_DATA_INTERVAL_MS;

const TENMAN_FORWARD_NODE_IDS = new Set(
  [
    '!b29f440c',
    '!c1ede368'
    // 如需新增節點，可直接在此陣列加入 `!xxxxxxxx` Mesh ID
  ]
    .map((id) => normalizeMeshId(id))
    .filter(Boolean)
);
const TENMAN_FORWARD_WS_ENDPOINT =
  process.env.TENMAN_WS_URL || 'wss://tenmanmap.yakumo.tw/ws';
const TENMAN_FORWARD_TIMEZONE_OFFSET_MINUTES = 8 * 60;
const TENMAN_FORWARD_QUEUE_LIMIT = 64;
const TENMAN_FORWARD_RECONNECT_DELAY_MS = 5000;
const TENMAN_FORWARD_GATEWAY_ID = process.env.TENMAN_GATEWAY_ID || null;
const TENMAN_FORWARD_API_KEY = process.env.CALLMESH_API_KEY || process.env.TENMAN_API_KEY || null;

const PROTO_DIR = path.resolve(__dirname, '..', '..', 'proto');

function extractEnumBlock(source, enumName) {
  const enumIndex = source.indexOf(`enum ${enumName}`);
  if (enumIndex === -1) {
    return '';
  }
  const braceStart = source.indexOf('{', enumIndex);
  if (braceStart === -1) {
    return '';
  }
  let depth = 1;
  let cursor = braceStart + 1;
  while (cursor < source.length && depth > 0) {
    const char = source[cursor];
    if (char === '{') {
      depth += 1;
    } else if (char === '}') {
      depth -= 1;
    }
    cursor += 1;
  }
  if (depth !== 0) {
    return '';
  }
  return source.slice(braceStart + 1, cursor - 1);
}

function buildEnumMap(relativePath, enumName) {
  const map = Object.create(null);
  try {
    const protoPath = path.resolve(PROTO_DIR, relativePath);
    const content = fsSync.readFileSync(protoPath, 'utf8');
    const block = extractEnumBlock(content, enumName);
    if (!block) {
      return map;
    }
    const lines = block.split('\n');
    for (const line of lines) {
      const match = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(\d+)/);
      if (match) {
        const [, label, number] = match;
        map[Number(number)] = label;
      }
    }
  } catch (err) {
    console.warn(`無法解析枚舉 ${enumName}: ${err.message}`);
  }
  return map;
}

const HARDWARE_MODEL_ENUM = buildEnumMap(path.join('meshtastic', 'mesh.proto'), 'HardwareModel');
const DEVICE_ROLE_ENUM = buildEnumMap(path.join('meshtastic', 'config.proto'), 'Role');

function formatEnumLabel(value) {
  if (!value) return '';
  return String(value).replace(/_/g, ' ').trim();
}

function resolveEnum(map, value) {
  if (value === undefined || value === null) {
    return { code: null, label: null };
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return { code: null, label: null };
    }
    const upper = trimmed.toUpperCase();
    return {
      code: upper,
      label: formatEnumLabel(upper) || upper
    };
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    const fallback = String(value);
    return {
      code: fallback,
      label: formatEnumLabel(fallback) || fallback
    };
  }
  const code = map[numeric];
  if (!code) {
    const fallback = String(numeric);
    return {
      code: fallback,
      label: `未知 (${fallback})`
    };
  }
  return {
    code,
    label: formatEnumLabel(code) || code
  };
}

function resolveHardwareModel(value) {
  return resolveEnum(HARDWARE_MODEL_ENUM, value);
}

function resolveDeviceRole(value) {
  return resolveEnum(DEVICE_ROLE_ENUM, value);
}

class CallMeshAprsBridge extends EventEmitter {
  constructor(options = {}) {
    super();
    const {
      storageDir,
      appVersion = '0.0.0',
      heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS,
      apiKey = '',
      verified = false,
      agentProduct = 'callmesh-client',
      aprsServer = DEFAULT_APRS_SERVER,
      aprsPort = DEFAULT_APRS_PORT,
      fetchImpl = globalThis.fetch,
      telemetryMaxEntriesPerNode = 500
    } = options;

    if (!storageDir) {
      throw new Error('storageDir is required for CallMeshAprsBridge');
    }

    this.storageDir = storageDir;
    this.appVersion = appVersion;
    this.fetchImpl = fetchImpl;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.agentProduct = agentProduct;

    this.callmeshState = createInitialCallmeshState({
      apiKey,
      verified,
      agentProduct
    });

    this.aprsState = {
      server: aprsServer || DEFAULT_APRS_SERVER,
      port: aprsPort || DEFAULT_APRS_PORT,
      callsignBase: null,
      callsign: null,
      ssid: null,
      passcode: null,
      actualServer: null,
      beaconIntervalMs: DEFAULT_BEACON_INTERVAL_MS
    };

    this.selfMeshId = null;
    this.aprsClient = null;
    this.aprsBeaconTimer = null;
    this.aprsBeaconPending = false;
    this.aprsStatusTimer = null;
    this.aprsLoginVerified = false;
    this.aprsLastBeaconAt = 0;
    this.aprsLastStatusAt = 0;
    this.aprsTelemetryDefinitionTimer = null;
    this.aprsLastTelemetryAt = 0;
    this.aprsTelemetryDataTimer = null;
    this.aprsLastTelemetryDataAt = 0;
    this.aprsTelemetrySequence = 0;
    this.aprsLastPositionDigest = new Map();
    this.aprsSkippedMeshIds = new Set();
    this.nodeDatabase = nodeDatabase;
    this.nodeDatabasePersistTimer = null;
    this.nodeDatabasePersistDelayMs = 250;

    this.telemetryBuckets = new Map();
    this.telemetryStore = new Map();
    this.telemetryRecordIds = new Set();
    this.telemetryUpdatedAt = 0;
    this.telemetryMaxEntriesPerNode = Number.isFinite(telemetryMaxEntriesPerNode)
      ? Math.max(10, Math.floor(telemetryMaxEntriesPerNode))
      : 500;

    this.tenmanForwardState = {
      lastKey: null,
      websocket: null,
      connecting: false,
      queue: [],
      pendingKeys: new Set(),
      sending: false,
      reconnectTimer: null,
      missingGatewayWarned: false,
      missingApiKeyWarned: false
    };

    this.heartbeatTimer = null;
    this.heartbeatRunning = false;

    this.lastTelemetryPersistAt = 0;
  }

  getMappingFilePath() {
    return path.join(this.storageDir, 'mappings.json');
  }

  getProvisionFilePath() {
    return path.join(this.storageDir, 'provision.json');
  }

  getTelemetryStateFilePath() {
    return path.join(this.storageDir, 'telemetry-state.json');
  }

  getTelemetryStorePath() {
    return path.join(this.storageDir, 'telemetry-records.jsonl');
  }

  async init({ allowRestore = true } = {}) {
    await this.ensureStorageDir();
    await this.restoreArtifacts({ allowRestore });
    await this.restoreTelemetryState();
    await this.loadTelemetryStore();
    await this.restoreNodeDatabase();
    this.emitState();
  }

  getStateSnapshot() {
    return {
      ...this.callmeshState,
      aprs: {
        ...this.aprsState,
        connected: Boolean(this.aprsClient?.connected)
      }
    };
  }

  getNodeSnapshot() {
    return this.nodeDatabase.list();
  }

  clearNodeDatabase() {
    const cleared = this.nodeDatabase.clear();
    this.scheduleNodeDatabasePersist();
    this.emitLog('NODE-DB', `cleared node database count=${cleared}`);
    return {
      cleared,
      nodes: this.nodeDatabase.list()
    };
  }

  getNodeDatabaseFilePath() {
    return path.join(this.storageDir, 'node-database.json');
  }

  async restoreNodeDatabase() {
    if (!this.storageDir) {
      return;
    }
    try {
      const filePath = this.getNodeDatabaseFilePath();
      const content = await fs.readFile(filePath, 'utf8');
      const payload = JSON.parse(content);
      const entries = Array.isArray(payload?.nodes)
        ? payload.nodes
        : Array.isArray(payload)
          ? payload
          : [];
      const restored = this.nodeDatabase.replace(entries);
      if (restored.length) {
        this.emitLog('NODE-DB', `restored ${restored.length} nodes from disk`);
      }
    } catch (err) {
      if (err && err.code !== 'ENOENT') {
        console.warn('載入節點資料庫失敗:', err);
      }
    }
  }

  scheduleNodeDatabasePersist() {
    if (!this.storageDir) {
      return;
    }
    if (this.nodeDatabasePersistTimer) {
      return;
    }
    this.nodeDatabasePersistTimer = setTimeout(() => {
      this.nodeDatabasePersistTimer = null;
      this.persistNodeDatabase().catch((err) => {
        console.error('寫入節點資料庫失敗:', err);
      });
    }, this.nodeDatabasePersistDelayMs);
    this.nodeDatabasePersistTimer.unref?.();
  }

  cancelNodeDatabasePersist() {
    if (this.nodeDatabasePersistTimer) {
      clearTimeout(this.nodeDatabasePersistTimer);
      this.nodeDatabasePersistTimer = null;
    }
  }

  async persistNodeDatabase({ sync = false } = {}) {
    if (!this.storageDir) {
      return;
    }
    const snapshot = this.nodeDatabase.serialize();
    const payload = {
      version: 1,
      updatedAt: new Date().toISOString(),
      nodes: snapshot
    };
    const filePath = this.getNodeDatabaseFilePath();
    const dir = path.dirname(filePath);
    if (sync) {
      try {
        fsSync.mkdirSync(dir, { recursive: true });
        fsSync.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf8');
      } catch (err) {
        console.error('寫入節點資料庫失敗:', err);
      }
      return;
    }
    try {
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(filePath, JSON.stringify(payload, null, 2), 'utf8');
    } catch (err) {
      console.error('寫入節點資料庫失敗:', err);
    }
  }

  flushNodeDatabasePersistSync() {
    if (!this.storageDir) {
      return;
    }
    this.cancelNodeDatabasePersist();
    const maybePromise = this.persistNodeDatabase({ sync: true });
    if (maybePromise && typeof maybePromise.catch === 'function') {
      maybePromise.catch((err) => {
        console.error('寫入節點資料庫失敗:', err);
      });
    }
  }

  setSelfMeshId(meshId) {
    this.selfMeshId = normalizeMeshId(meshId);
  }

  upsertNodeInfo(node, { timestamp } = {}) {
    if (!node || typeof node !== 'object') {
      return null;
    }
    const meshIdCandidate =
      node.meshId ?? node.mesh_id ?? node.meshIdNormalized ?? node.mesh_id_normalized ?? null;
    const normalized = normalizeMeshId(meshIdCandidate);
    if (!normalized) {
      return null;
    }
    const parseNumber = (value) => {
      const num = Number(value);
      return Number.isFinite(num) ? num : null;
    };
    const position = node.position ?? node.gps ?? node.location ?? null;
    const latitude = parseNumber(
      node.latitude ?? node.lat ?? position?.latitude ?? position?.lat
    );
    const longitude = parseNumber(
      node.longitude ?? node.lon ?? position?.longitude ?? position?.lon
    );
    const altitude = parseNumber(
      node.altitude ?? node.alt ?? position?.altitude ?? position?.alt
    );
    const hwModelRaw = node.hwModel ?? node.hw_model ?? null;
    const roleRaw = node.role ?? null;
    const hwModelInfo = resolveHardwareModel(hwModelRaw);
    const roleInfo = resolveDeviceRole(roleRaw);
    const info = {
      meshIdOriginal: meshIdCandidate ?? null,
      shortName: node.shortName ?? node.short_name ?? null,
      longName: node.longName ?? node.long_name ?? null,
      hwModel: hwModelInfo.code ?? (hwModelRaw != null ? String(hwModelRaw) : null),
      hwModelLabel: hwModelInfo.label ?? null,
      role: roleInfo.code ?? (roleRaw != null ? String(roleRaw) : null),
      roleLabel: roleInfo.label ?? null,
      latitude,
      longitude,
      altitude,
      lastSeenAt: Number.isFinite(timestamp) ? Number(timestamp) : Date.now()
    };
    const result = this.nodeDatabase.upsert(normalized, info);
    if (result.changed) {
      const payload = {
        meshId: normalized,
        shortName: result.node.shortName,
        longName: result.node.longName,
        hwModel: result.node.hwModel,
        hwModelLabel: result.node.hwModelLabel,
        role: result.node.role,
        roleLabel: result.node.roleLabel,
        latitude: result.node.latitude,
        longitude: result.node.longitude,
        altitude: result.node.altitude,
        label: buildNodeLabel(result.node),
        lastSeenAt: result.node.lastSeenAt
      };
      this.emit('node', payload);
      this.scheduleNodeDatabasePersist();
    }
    return result.node;
  }

  setApiKey(apiKey, { markVerified = true } = {}) {
    const trimmed = typeof apiKey === 'string' ? apiKey.trim() : '';
    this.callmeshState.apiKey = trimmed;
    if (markVerified && trimmed) {
      this.callmeshState.verified = true;
      this.callmeshState.verifiedKey = trimmed;
      if (!this.callmeshState.lastStatus || this.callmeshState.lastStatus.startsWith('CallMesh: 未設定')) {
        this.callmeshState.lastStatus = 'CallMesh: 尚未同步';
      }
    } else {
      this.callmeshState.verified = false;
      this.callmeshState.verifiedKey = '';
      this.callmeshState.lastStatus = 'CallMesh: 未設定 Key';
    }
    this.callmeshState.agent = buildAgentString({ product: this.agentProduct });
    this.emitState();
  }

  clearApiKey() {
    this.callmeshState.apiKey = '';
    this.callmeshState.verified = false;
    this.callmeshState.verifiedKey = '';
    this.callmeshState.degraded = false;
    this.callmeshState.lastStatus = 'CallMesh: 未設定 Key';
    this.callmeshState.lastHeartbeatAt = null;
    this.stopHeartbeatLoop();
    this.updateAprsProvision(null);
    this.emitState();
  }

  async verifyApiKey(apiKey, { localHash = null, allowDegraded = false, timeout = 8000 } = {}) {
    const trimmed = typeof apiKey === 'string' ? apiKey.trim() : '';
    if (!trimmed) {
      throw new Error('API key is required');
    }
    const cmClient = this.createCallMeshClient(trimmed);
    try {
      const heartbeatRes = await cmClient.heartbeat({ localHash, timeout });
      this.callmeshState.apiKey = trimmed;
      this.callmeshState.verified = true;
      this.callmeshState.verifiedKey = trimmed;
      this.callmeshState.lastStatus = 'CallMesh: Key 驗證成功';
      this.callmeshState.degraded = false;
      this.callmeshState.lastHeartbeatAt = new Date().toISOString();
      this.callmeshState.agent = cmClient.agentString;
      this.emitLog('CALLMESH', 'API key verified');
      this.emitState();
      return { success: true, heartbeat: heartbeatRes };
    } catch (err) {
      if (isAuthError(err)) {
        this.callmeshState.apiKey = '';
        this.callmeshState.verified = false;
        this.callmeshState.verifiedKey = '';
        this.callmeshState.degraded = false;
        this.callmeshState.lastStatus = `CallMesh: Key 驗證失敗 - ${err.message}`;
        this.emitLog('CALLMESH', `API key auth failure: ${err.message}`);
        this.emitState();
        return {
          success: false,
          error: err,
          authError: true
        };
      }

      if (allowDegraded && this.callmeshState.verified && this.callmeshState.verifiedKey === trimmed) {
        this.callmeshState.apiKey = trimmed;
        this.callmeshState.verified = true;
        this.callmeshState.verifiedKey = trimmed;
        this.callmeshState.degraded = true;
        this.callmeshState.lastStatus = 'CallMesh: 驗證逾時';
        this.callmeshState.agent = cmClient.agentString;
        const applied = this.applyCachedProvisionFallback('verify degraded');
        if (!applied) {
          this.emitState();
        }
        return {
          success: true,
          degraded: true,
          error: err
        };
      }

      this.emitLog('CALLMESH', `API key verify error: ${err.message}`);
      throw err;
    }
  }

  async restoreArtifacts({ allowRestore = true } = {}) {
    if (!allowRestore) {
      this.callmeshState.lastMappingHash = null;
      this.callmeshState.lastMappingSyncedAt = null;
      this.callmeshState.mappingItems = [];
      this.callmeshState.cachedProvision = null;
      this.callmeshState.provision = null;
      this.updateAprsProvision(null);
      return;
    }

    const mapping = await this.loadJsonSafe(this.getMappingFilePath());
    if (mapping) {
      this.callmeshState.lastMappingHash = mapping.hash ?? null;
      this.callmeshState.lastMappingSyncedAt = mapping.updatedAt ?? null;
      if (Array.isArray(mapping.items)) {
        this.callmeshState.mappingItems = mapping.items;
      }
      this.emitLog('CALLMESH', `restore mapping hash=${this.callmeshState.lastMappingHash ?? 'null'} count=${this.callmeshState.mappingItems.length}`);
    }

    const provision = await this.loadJsonSafe(this.getProvisionFilePath());
    if (provision?.provision) {
      this.callmeshState.cachedProvision = cloneProvision(provision.provision);
      this.emitLog('CALLMESH', 'restore provision cache from disk');
    }
    this.callmeshState.provision = null;
    this.updateAprsProvision(null);
  }

  async clearArtifacts() {
    await fs.rm(this.getMappingFilePath(), { force: true });
    await fs.rm(this.getProvisionFilePath(), { force: true });
    await fs.rm(this.getTelemetryStateFilePath(), { force: true });
    await fs.rm(this.getNodeDatabaseFilePath(), { force: true });
    this.callmeshState.lastMappingHash = null;
    this.callmeshState.lastMappingSyncedAt = null;
    this.callmeshState.provision = null;
    this.callmeshState.cachedProvision = null;
    this.callmeshState.lastProvisionRaw = null;
    this.callmeshState.mappingItems = [];
    this.aprsLastPositionDigest.clear();
    this.aprsSkippedMeshIds.clear();
    this.aprsTelemetrySequence = 0;
    this.aprsLastTelemetryAt = 0;
    this.aprsLastTelemetryDataAt = 0;
    this.nodeDatabase.clear();
    this.scheduleNodeDatabasePersist();
    await this.clearTelemetryStore({ silent: false });
    this.emitLog('CALLMESH', 'cleared local mapping/provision cache');
    this.updateAprsProvision(null);
  }

  async restoreTelemetryState() {
    try {
      const payload = await this.loadJsonSafe(this.getTelemetryStateFilePath());
      const sequenceValue = Number(payload?.sequence ?? payload?.seq ?? payload?.lastSequence);
      if (Number.isFinite(sequenceValue) && sequenceValue >= 0) {
        const normalized = Math.floor(sequenceValue) % 1000;
        this.aprsTelemetrySequence = normalized;
      }
    } catch (err) {
      this.emitLog('CALLMESH', `restore telemetry sequence failed: ${err.message}`);
    }
  }

  async loadTelemetryStore() {
    const filePath = this.getTelemetryStorePath();
    try {
      await fs.access(filePath);
    } catch (err) {
      if (err && err.code === 'ENOENT') {
        return;
      }
      this.emitLog('CALLMESH', `telemetry history access failed: ${err.message}`);
      return;
    }

    let loadedCount = 0;
    try {
      await new Promise((resolve, reject) => {
        const stream = fsSync.createReadStream(filePath, { encoding: 'utf8' });
        stream.on('error', reject);
        const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
        rl.on('line', (line) => {
          const trimmed = line.trim();
          if (!trimmed) return;
          let parsed;
          try {
            parsed = JSON.parse(trimmed);
          } catch (err) {
            return;
          }
          const normalized = this.normalizeTelemetryRecordFromDisk(parsed);
          if (!normalized) {
            return;
          }
          this.addTelemetryRecord(normalized.meshId, normalized, {
            node: normalized.node || null,
            persist: false,
            emitEvent: false
          });
          loadedCount += 1;
        });
        rl.once('close', resolve);
      });
    } catch (err) {
      this.emitLog('CALLMESH', `restore telemetry history failed: ${err.message}`);
      return;
    }

    if (loadedCount > 0) {
      this.emitLog('CALLMESH', `restored telemetry history count=${loadedCount}`);
    }
  }

  async persistTelemetryState() {
    try {
      const payload = {
        sequence: this.aprsTelemetrySequence,
        savedAt: new Date().toISOString()
      };
      await this.saveJson(this.getTelemetryStateFilePath(), payload);
    } catch (err) {
      this.emitLog('CALLMESH', `儲存 Telemetry 序號失敗: ${err.message}`);
    }
  }

  async performHeartbeatTick() {
    if (!this.callmeshState.apiKey || !this.callmeshState.verified) {
      return;
    }
    if (this.heartbeatRunning) {
      return;
    }

    this.heartbeatRunning = true;
    try {
      const cmClient = this.createCallMeshClient(this.callmeshState.apiKey, {
        agent: this.callmeshState.agent
      });
      const localHash = this.callmeshState.lastMappingHash ?? null;
      const heartbeatRes = await cmClient.heartbeat({ localHash, timeout: 8000 });
      const nowIso = new Date().toISOString();
      this.callmeshState.lastHeartbeatAt = nowIso;
      this.callmeshState.lastStatus = 'CallMesh: Heartbeat 正常';
      this.callmeshState.degraded = false;
      this.callmeshState.verified = true;
      this.callmeshState.verifiedKey = this.callmeshState.apiKey;
      this.callmeshState.agent = cmClient.agentString;
      this.emitLog('CALLMESH', `heartbeat ok hash=${heartbeatRes?.hash ?? 'null'} needs_update=${heartbeatRes?.needs_update ? 'true' : 'false'}`);

      const shouldSyncMapping =
        Boolean(heartbeatRes?.needs_update) || !this.callmeshState.lastMappingSyncedAt;
      let mappingHash = this.callmeshState.lastMappingHash ?? null;

      if (shouldSyncMapping) {
        const mappingResult = await this.syncMappings(cmClient, localHash);
        if (mappingResult?.hash) {
          mappingHash = mappingResult.hash;
        }
      } else if (heartbeatRes?.hash) {
        mappingHash = heartbeatRes.hash;
      }

      this.callmeshState.lastMappingHash = mappingHash;

      if (heartbeatRes?.provision) {
        await this.persistProvision(heartbeatRes.provision);
      }

      this.emitState();
    } catch (err) {
      let shouldEmit = true;
      if (isAuthError(err)) {
        this.callmeshState.apiKey = '';
        this.callmeshState.verified = false;
        this.callmeshState.verifiedKey = '';
        this.callmeshState.lastStatus = `CallMesh: Key 驗證失效 - ${err.message}`;
        this.callmeshState.agent = buildAgentString({ product: this.agentProduct });
        this.callmeshState.degraded = false;
        this.callmeshState.lastHeartbeatAt = null;
        this.stopHeartbeatLoop();
        this.emitLog('CALLMESH', `heartbeat auth failure: ${err.message}`);
        this.updateAprsProvision(null);
      } else {
        this.callmeshState.degraded = true;
        this.callmeshState.lastStatus = 'CallMesh: Heartbeat 失敗';
        this.emitLog('CALLMESH', `heartbeat error: ${err.message}`);
        if (this.applyCachedProvisionFallback('heartbeat failure')) {
          shouldEmit = false;
        }
      }
      if (shouldEmit) {
        this.emitState();
      }
    } finally {
      this.heartbeatRunning = false;
    }
  }

  startHeartbeatLoop() {
    if (!this.callmeshState.apiKey || !this.callmeshState.verified) {
      this.stopHeartbeatLoop();
      return;
    }

    if (this.heartbeatTimer) {
      return;
    }

    this.heartbeatTimer = setInterval(() => {
      this.performHeartbeatTick().catch((err) => {
        this.emitLog('CALLMESH', `定期 CallMesh Heartbeat 發生未攔截的錯誤: ${err.message}`);
      });
    }, this.heartbeatIntervalMs);
  }

  stopHeartbeatLoop() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.heartbeatRunning = false;
  }

  handleMeshtasticSummary(summary) {
    if (!summary) return;
    const timestampMs = Number.isFinite(summary.timestampMs) ? Number(summary.timestampMs) : Date.now();
    this.captureSummaryNodeInfo(summary, timestampMs);
    this.ensureSummaryFlowMetadata(summary);
    this.recordTelemetryPacket(summary);
    if (summary.type === 'Position') {
      this.handleAprsSummary(summary);
    }
    this.forwardTenmanPosition(summary);
  }

  async forwardTenmanPosition(summary) {
    if (!summary) {
      return;
    }

    try {
      const meshIdNormalized = normalizeMeshId(
        summary?.from?.meshIdNormalized ?? summary?.from?.meshId ?? summary?.from?.mesh_id
      );
      if (!TENMAN_FORWARD_NODE_IDS.has(meshIdNormalized)) {
        return;
      }

      const position = summary.position;
      if (!position) {
        return;
      }

      const latitude = toFiniteNumber(position.latitude ?? position.lat);
      const longitude = toFiniteNumber(position.longitude ?? position.lon);
      if (latitude == null || longitude == null) {
        return;
      }

      const altitudeValue = toFiniteNumber(
        position.altitude ?? position.alt ?? position.altitudeHae ?? position.altitudeGeoidalSeparation
      );
      const altitude = roundTo(altitudeValue ?? 0, 2);
      const speed = roundTo(resolveSpeed(position), 2);
      const heading = resolveHeading(position);

      const timestampSource =
        summary.timestamp ??
        (Number.isFinite(summary.timestampMs) ? new Date(Number(summary.timestampMs)).toISOString() : null) ??
        new Date().toISOString();
      const timestamp = formatTimestampWithOffset(
        timestampSource,
        TENMAN_FORWARD_TIMEZONE_OFFSET_MINUTES
      );
      if (!timestamp) {
        return;
      }

      const state = this.tenmanForwardState;
      const dedupeKey = `${meshIdNormalized}:${timestamp}:${latitude.toFixed(6)}:${longitude.toFixed(6)}`;
      if (state?.lastKey === dedupeKey || state?.pendingKeys?.has?.(dedupeKey)) {
        return;
      }

      const gatewayId = TENMAN_FORWARD_GATEWAY_ID || this.selfMeshId;
      if (!gatewayId) {
        if (state && !state.missingGatewayWarned) {
          state.missingGatewayWarned = true;
          this.emitLog('TENMAN', '缺少 gateway_id，無法送出 publish，請設定 TENMAN_GATEWAY_ID 或確認 selfMeshId');
        }
        return;
      }
      if (state) {
        state.missingGatewayWarned = false;
      }

      const apiKey = TENMAN_FORWARD_API_KEY || this.callmeshState?.apiKey || null;
      if (!apiKey) {
        if (state && !state.missingApiKeyWarned) {
          state.missingApiKeyWarned = true;
          this.emitLog(
            'TENMAN',
            '缺少 api_key，無法送出 publish，請設定 CALLMESH_API_KEY/TENMAN_API_KEY 或完成 CallMesh 驗證'
          );
        }
        return;
      }
      if (state) {
        state.missingApiKeyWarned = false;
      }

      const payload = {
        device_id: meshIdNormalized,
        timestamp,
        latitude,
        longitude,
        altitude,
        speed,
        heading,
        gateway_id: gatewayId,
        api_key: apiKey,
        extra: {
          source: 'TMAG',
          mesh_id: meshIdNormalized
        }
      };

      const message = {
        action: 'publish',
        payload
      };

      this.enqueueTenmanPublish(message, dedupeKey);
    } catch (err) {
      this.emitLog('TENMAN', `位置回報處理失敗: ${err.message}`);
    }
  }

  enqueueTenmanPublish(message, dedupeKey) {
    if (!message || !dedupeKey) {
      return;
    }
    const state = this.tenmanForwardState;
    if (!state) {
      return;
    }

    if (!state.pendingKeys) {
      state.pendingKeys = new Set();
    }

    if (state.lastKey === dedupeKey || state.pendingKeys.has(dedupeKey)) {
      return;
    }

    if (!Array.isArray(state.queue)) {
      state.queue = [];
    }

    if (state.queue.length >= TENMAN_FORWARD_QUEUE_LIMIT) {
      const dropped = state.queue.shift();
      if (dropped?.key) {
        state.pendingKeys.delete(dropped.key);
      }
      this.emitLog('TENMAN', '佇列已滿，將移除最舊的 publish 訊息');
    }

    const entry = {
      key: dedupeKey,
      message,
      serialized: JSON.stringify(message)
    };
    state.queue.push(entry);
    state.pendingKeys.add(dedupeKey);

    this.ensureTenmanWebsocket();
    this.flushTenmanQueue();
  }

  ensureTenmanWebsocket() {
    const state = this.tenmanForwardState;
    if (!state || !Array.isArray(state.queue)) {
      return;
    }
    if (state.websocket && state.websocket.readyState === WebSocket.OPEN) {
      return;
    }
    if (state.connecting) {
      return;
    }

    this.clearTenmanReconnectTimer();
    state.connecting = true;

    try {
      const ws = new WebSocket(TENMAN_FORWARD_WS_ENDPOINT);
      state.websocket = ws;

      ws.on('open', () => {
        state.connecting = false;
        this.emitLog('TENMAN', 'WebSocket 已連線');
        this.flushTenmanQueue();
      });

      ws.on('close', (code, reason) => {
        state.websocket = null;
        state.connecting = false;
        state.sending = false;
        this.emitLog(
          'TENMAN',
          `WebSocket 已關閉 code=${code}${reason ? ` reason=${reason.toString()}` : ''}`
        );
        this.scheduleTenmanReconnect('closed');
      });

      ws.on('error', (err) => {
        this.emitLog('TENMAN', `WebSocket 錯誤: ${err.message}`);
      });

      ws.on('unexpected-response', (_req, res) => {
        this.emitLog('TENMAN', `WebSocket unexpected status=${res?.statusCode ?? 'unknown'}`);
      });

      ws.on('message', (data) => {
        try {
          const text = typeof data === 'string' ? data : data.toString('utf8');
          if (!text) return;
          const parsed = JSON.parse(text);
          if (parsed?.type === 'ack') {
            const deviceId = parsed?.payload?.device_id ?? parsed?.device_id ?? '';
            this.emitLog('TENMAN', `收到 ack${deviceId ? ` device=${deviceId}` : ''}`);
          }
        } catch (err) {
          this.emitLog('TENMAN', `WebSocket 訊息解析失敗: ${err.message}`);
        }
      });
    } catch (err) {
      state.connecting = false;
      this.emitLog('TENMAN', `WebSocket 建立失敗: ${err.message}`);
      this.scheduleTenmanReconnect('connect-error');
    }
  }

  flushTenmanQueue() {
    const state = this.tenmanForwardState;
    if (!state || !Array.isArray(state.queue) || state.queue.length === 0) {
      return;
    }

    const ws = state.websocket;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      this.ensureTenmanWebsocket();
      return;
    }

    if (state.sending) {
      return;
    }

    const entry = state.queue[0];
    if (!entry) {
      return;
    }

    state.sending = true;
    try {
      ws.send(entry.serialized, (err) => {
        state.sending = false;
        if (err) {
          this.emitLog('TENMAN', `位置回報失敗: ${err.message}`);
          this.scheduleTenmanReconnect('send-error');
          this.resetTenmanWebsocket();
          return;
        }

        state.queue.shift();
        state.pendingKeys.delete(entry.key);
        state.lastKey = entry.key;
        this.flushTenmanQueue();
      });
    } catch (err) {
      state.sending = false;
      this.emitLog('TENMAN', `位置回報失敗: ${err.message}`);
      this.scheduleTenmanReconnect('exception');
      this.resetTenmanWebsocket();
    }
  }

  resetTenmanWebsocket() {
    const state = this.tenmanForwardState;
    if (!state) {
      return;
    }
    const ws = state.websocket;
    if (ws) {
      try {
        ws.removeAllListeners();
        ws.terminate();
      } catch {
        // ignore
      }
    }
    state.websocket = null;
    state.connecting = false;
    state.sending = false;
  }

  scheduleTenmanReconnect(reason = 'retry') {
    const state = this.tenmanForwardState;
    if (!state || !Array.isArray(state.queue) || state.queue.length === 0) {
      return;
    }
    if (state.reconnectTimer) {
      return;
    }
    this.emitLog(
      'TENMAN',
      `WebSocket 將於 ${Math.round(TENMAN_FORWARD_RECONNECT_DELAY_MS / 1000)} 秒後重試 (${reason})`
    );
    state.reconnectTimer = setTimeout(() => {
      state.reconnectTimer = null;
      this.ensureTenmanWebsocket();
    }, TENMAN_FORWARD_RECONNECT_DELAY_MS);
    state.reconnectTimer?.unref?.();
  }

  clearTenmanReconnectTimer() {
    const state = this.tenmanForwardState;
    if (!state?.reconnectTimer) {
      return;
    }
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }

  handleMeshtasticMyInfo(info) {
    if (!info) return;
    const meshCandidate = normalizeMeshId(info?.node?.meshId || info?.meshId);
    if (meshCandidate) {
      this.selfMeshId = meshCandidate;
    }
    if (info?.node) {
      const merged = this.upsertNodeInfo(info.node, { timestamp: Date.now() });
      if (merged) {
        info.node = mergeNodeInfo(info.node, merged) || info.node;
      }
    }
  }

  captureSummaryNodeInfo(summary, timestampMs) {
    const candidates = [];
    if (summary.from) candidates.push(summary.from);
    if (summary.to) candidates.push(summary.to);
    if (summary.relay) candidates.push(summary.relay);
    if (summary.nextHop) candidates.push(summary.nextHop);
    for (const node of candidates) {
      if (!node || typeof node !== 'object') continue;
      let sourceNode = node;
      if (summary.type === 'Position' && summary.position && node === summary.from) {
        const position = summary.position;
        sourceNode = {
          ...node,
          position: {
            ...(typeof node.position === 'object' && node.position ? node.position : {}),
            latitude: position.latitude ?? position.lat ?? node.position?.latitude,
            longitude: position.longitude ?? position.lon ?? node.position?.longitude,
            altitude: position.altitude ?? position.alt ?? node.position?.altitude
          },
          latitude: node.latitude ?? position.latitude ?? position.lat ?? null,
          longitude: node.longitude ?? position.longitude ?? position.lon ?? null,
          altitude: node.altitude ?? position.altitude ?? position.alt ?? null
        };
      }
      const merged = this.upsertNodeInfo(sourceNode, { timestamp: timestampMs });
      if (merged) {
        const enriched = mergeNodeInfo(node, merged);
        if (enriched) {
          Object.assign(node, enriched);
          node.label = enriched.label || node.label || buildNodeLabel(enriched);
          if (enriched.meshId) {
            node.meshId = node.meshId ?? enriched.meshId;
            node.meshIdNormalized = enriched.meshId;
          }
        }
      }
    }
  }

  updateAprsServer(server) {
    const normalized = server?.trim() || DEFAULT_APRS_SERVER;
    if (normalized !== this.aprsState.server) {
      this.aprsState.server = normalized;
      this.stopAprsBeaconLoop();
      this.aprsBeaconPending = true;
      this.aprsLoginVerified = false;
      this.aprsState.actualServer = null;
      this.emitState();
      this.emitLog('APRS', `server set to ${normalized}`);
      this.ensureAprsConnection();
    }
  }

  setAprsBeaconIntervalMs(intervalMs) {
    const clamped = clamp(intervalMs, MIN_BEACON_INTERVAL_MS, MAX_BEACON_INTERVAL_MS);
    if (clamped !== this.aprsState.beaconIntervalMs) {
      this.aprsState.beaconIntervalMs = clamped;
      this.emitState();
      this.emitLog('APRS', `beacon interval set to ${Math.round(clamped / 1000)}s`);
      if (this.aprsClient?.connected) {
        this.restartAprsBeaconLoop('config-update');
      }
    }
  }

  ensureAprsConnection() {
    if (!this.callmeshState.apiKey || !this.callmeshState.verified) {
      return;
    }

    if (!this.aprsState.server || !this.aprsState.callsign || this.aprsState.passcode == null) {
      this.teardownAprsConnection();
      return;
    }

    const config = {
      server: this.aprsState.server,
      port: this.aprsState.port,
      callsign: this.aprsState.callsign,
      passcode: this.aprsState.passcode,
      version: this.appVersion,
      softwareName: 'TMAG'
    };

    if (!this.aprsClient) {
      const logForwarder = (tag, message) => {
        if (tag === 'APRS') {
          const text = String(message || '');
          if (/^rx\s+#\s*aprsc\b/i.test(text)) {
            return;
          }
        }
        this.emitLog(tag, message);
      };
      this.aprsClient = new APRSClient({
        ...config,
        log: logForwarder
      });
      this.aprsClient.on('line', (line) => this.handleAprsLine(line));
      this.aprsClient.on('connected', () => this.handleAprsConnected());
      this.aprsClient.on('disconnected', () => this.handleAprsDisconnected());
      this.aprsState.actualServer = null;
      this.emitState();
      this.aprsClient.connect();
      return;
    }

    this.aprsClient.updateConfig(config);
    if (!this.aprsClient.connected) {
      this.aprsClient.connect();
    }
  }

  teardownAprsConnection() {
    if (this.aprsClient) {
      this.aprsClient.disconnect();
      this.aprsClient = null;
      this.emitLog('APRS', 'disconnected');
    }
    this.stopAprsBeaconLoop();
    this.stopAprsStatusLoop();
    this.stopAprsTelemetryDefinitionLoop();
    this.stopAprsTelemetryDataLoop();
    this.aprsBeaconPending = true;
    this.aprsLoginVerified = false;
    this.aprsState.actualServer = null;
    this.emitState();
  }

  destroy() {
    this.stopHeartbeatLoop();
    this.teardownAprsConnection();
    this.flushNodeDatabasePersistSync();
  }

  // Internal helpers

  async ensureStorageDir() {
    if (!this.storageDir) return;
    try {
      await fs.mkdir(this.storageDir, { recursive: true });
    } catch (err) {
      if (err && err.code !== 'EEXIST') {
        throw err;
      }
    }
  }

  createCallMeshClient(apiKey, overrides = {}) {
    return new CallMeshClient({
      apiKey,
      fetchImpl: this.fetchImpl,
      product: this.agentProduct,
      ...overrides
    });
  }

  async saveJson(filePath, data) {
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
  }

  async loadJsonSafe(filePath) {
    try {
      const content = await fs.readFile(filePath, 'utf8');
      return JSON.parse(content);
    } catch (err) {
      if (err && err.code !== 'ENOENT') {
        this.emitLog('CALLMESH', `load json failed ${filePath}: ${err.message}`);
      }
      return null;
    }
  }

  async persistProvision(provision) {
    const incomingRaw = stableStringify(provision || {});
    if (this.callmeshState.lastProvisionRaw === incomingRaw) {
      this.callmeshState.cachedProvision = cloneProvision(provision);
      return;
    }
    const canonical = normalizeProvision(provision);
    this.callmeshState.provision = canonical;
    this.callmeshState.cachedProvision = cloneProvision(provision);
    this.callmeshState.lastProvisionRaw = incomingRaw;
    this.aprsLastPositionDigest.clear();
    const payload = {
      provision: canonical,
      savedAt: new Date().toISOString()
    };
    await this.saveJson(this.getProvisionFilePath(), payload);
    this.emitLog(
      'CALLMESH',
      `provision received callsign=${provision?.callsign_base ?? 'N/A'} ssid=${provision?.ssid ?? 'N/A'}`
    );
    this.emitState();
    this.updateAprsProvision(canonical);
  }

  async syncMappings(cmClient, knownHash) {
    try {
      this.emitLog('CALLMESH', `fetching mappings known_hash=${knownHash ?? 'null'}`);
      const response = await cmClient.fetchMappings({ knownHash, timeout: 15_000 });
      if (!response || !Array.isArray(response.items)) {
        this.emitLog('CALLMESH', 'mapping response 無 items，略過');
        return null;
      }
      const updatedAt = new Date().toISOString();
      const payload = {
        hash: response.hash ?? null,
        items: response.items,
        updatedAt
      };
      await this.saveJson(this.getMappingFilePath(), payload);
      this.callmeshState.lastMappingHash = response.hash ?? this.callmeshState.lastMappingHash ?? null;
      this.callmeshState.lastMappingSyncedAt = updatedAt;
      this.callmeshState.mappingItems = response.items;
      this.emitLog(
        'CALLMESH',
        `mapping synced hash=${this.callmeshState.lastMappingHash ?? 'null'} items=${response.items.length}`
      );
      this.emitState();
      return { hash: this.callmeshState.lastMappingHash, count: response.items.length };
    } catch (err) {
      this.emitLog('CALLMESH', `mapping 下載失敗: ${err.message}`);
      return null;
    }
  }

  emitState() {
    const snapshot = this.getStateSnapshot();
    this.emit('state', snapshot);
  }

  emitLog(tag, message) {
    const entry = {
      tag,
      message,
      timestamp: new Date().toISOString()
    };
    this.emit('log', entry);
  }

  emitAprsUplink(info) {
    if (!info) return;
    this.emit('aprs-uplink', info);
  }

  stopAprsBeaconLoop() {
    if (this.aprsBeaconTimer) {
      clearTimeout(this.aprsBeaconTimer);
      this.aprsBeaconTimer = null;
    }
  }

  restartAprsBeaconLoop(reason = 'interval') {
    this.stopAprsBeaconLoop();
    const delay = this.aprsState.beaconIntervalMs;
    this.aprsBeaconTimer = setTimeout(() => {
      this.aprsBeaconTimer = null;
      try {
        this.sendAprsBeacon(reason);
      } catch (err) {
        this.emitLog('APRS', `beacon schedule error: ${err.message}`);
      }
    }, delay);
  }

  stopAprsStatusLoop() {
    if (this.aprsStatusTimer) {
      clearInterval(this.aprsStatusTimer);
      this.aprsStatusTimer = null;
    }
  }

  startAprsStatusLoop() {
    this.stopAprsStatusLoop();
    this.aprsStatusTimer = setInterval(() => {
      try {
        this.sendAprsStatus('interval');
      } catch (err) {
        this.emitLog('APRS', `status schedule error: ${err.message}`);
      }
    }, APRS_STATUS_INTERVAL_MS);
  }

  stopAprsTelemetryDefinitionLoop() {
    if (this.aprsTelemetryDefinitionTimer) {
      clearTimeout(this.aprsTelemetryDefinitionTimer);
      this.aprsTelemetryDefinitionTimer = null;
    }
  }

  scheduleAprsTelemetryDefinitions(reason = 'interval') {
    this.stopAprsTelemetryDefinitionLoop();
    let delay = APRS_TELEMETRY_INTERVAL_MS;
    if (this.aprsLastTelemetryAt) {
      const elapsed = Date.now() - this.aprsLastTelemetryAt;
      if (elapsed < APRS_TELEMETRY_INTERVAL_MS) {
        delay = APRS_TELEMETRY_INTERVAL_MS - Math.max(0, elapsed);
      } else {
        delay = 0;
      }
    }
    this.aprsTelemetryDefinitionTimer = setTimeout(() => {
      this.aprsTelemetryDefinitionTimer = null;
      try {
        this.sendAprsTelemetryDefinitions(reason);
      } catch (err) {
        this.emitLog('APRS', `telemetry definition error: ${err.message}`);
      }
    }, Math.max(0, delay));
  }

  stopAprsTelemetryDataLoop() {
    if (this.aprsTelemetryDataTimer) {
      clearTimeout(this.aprsTelemetryDataTimer);
      this.aprsTelemetryDataTimer = null;
    }
  }

  scheduleAprsTelemetryData(reason = 'interval') {
    this.stopAprsTelemetryDataLoop();
    let delay = APRS_TELEMETRY_DATA_INTERVAL_MS;
    if (this.aprsLastTelemetryDataAt) {
      const elapsed = Date.now() - this.aprsLastTelemetryDataAt;
      if (elapsed < APRS_TELEMETRY_DATA_INTERVAL_MS) {
        delay = APRS_TELEMETRY_DATA_INTERVAL_MS - Math.max(0, elapsed);
      } else {
        delay = 0;
      }
    }
    this.aprsTelemetryDataTimer = setTimeout(() => {
      this.aprsTelemetryDataTimer = null;
      try {
        this.sendAprsTelemetryData(reason);
      } catch (err) {
        this.emitLog('APRS', `telemetry data error: ${err.message}`);
      }
    }, Math.max(0, delay));
  }

  handleAprsLine(line) {
    if (!line) return;
    const normalized = line.trim();
    if (!normalized) return;

    if (/logresp/i.test(normalized)) {
      const serverMatch = normalized.match(/\bserver\s+([A-Za-z0-9_-]+)/i);
      if (serverMatch) {
        this.updateAprsActualServer(serverMatch[1]);
      }
      if (!this.aprsLoginVerified && /\bverified\b/i.test(normalized)) {
      this.aprsLoginVerified = true;
      this.emitLog('APRS', 'login verified');
      const shouldSendInitialBeacon = this.aprsLastBeaconAt === 0;
      if (shouldSendInitialBeacon) {
        this.sendAprsBeacon('aprs-verified');
      } else {
        this.restartAprsBeaconLoop('interval');
      }

      const shouldSendInitialDefinitions = this.aprsLastTelemetryAt === 0;
      const shouldSendInitialTelemetryData = this.aprsLastTelemetryDataAt === 0;

      if (!shouldSendInitialDefinitions) {
        this.scheduleAprsTelemetryDefinitions('interval');
      }
      if (!shouldSendInitialTelemetryData) {
        this.scheduleAprsTelemetryData('interval');
      }

      if (shouldSendInitialDefinitions) {
        this.sendAprsTelemetryDefinitions('login', { force: true });
      }
      if (shouldSendInitialTelemetryData) {
        this.sendAprsTelemetryData('login', { force: true });
      }

      if (this.aprsLastStatusAt === 0) {
        const statusSent = this.sendAprsStatus('login');
        if (!statusSent) {
          this.emitLog('APRS', 'status send skipped at login');
        }
      }

      if (this.aprsLastStatusAt !== 0) {
        this.aprsLastStatusAt = Date.now();
      }
      this.startAprsStatusLoop();
    }
      return;
    }

    if (/^#\s*aprsc\b/i.test(normalized)) {
      const serverFromAprsc = normalized.match(/GMT\s+([A-Za-z0-9_-]+)\s+[0-9.]+:\d+/i);
      if (serverFromAprsc) {
        this.updateAprsActualServer(serverFromAprsc[1]);
      }
      return;
    }
  }

  handleAprsConnected() {
    this.aprsBeaconPending = true;
    this.aprsLoginVerified = false;
    this.aprsState.actualServer = null;
    this.emitState();
  }

  handleAprsDisconnected() {
    this.aprsLoginVerified = false;
    this.aprsState.actualServer = null;
    this.emitState();
  }

  updateAprsActualServer(server) {
    if (!server) return;
    if (server !== this.aprsState.actualServer) {
      this.aprsState.actualServer = server;
      this.emitState();
    }
  }

  sendAprsBeacon(reason = 'interval') {
    if (!this.callmeshState.apiKey || !this.callmeshState.verified) return false;
    if (!this.aprsClient?.connected) return false;
    if (!this.aprsState.callsign) return false;
    if (!this.aprsLoginVerified) {
      this.aprsBeaconPending = true;
      return false;
    }

    const provision = this.callmeshState.provision;
    const payload = buildAprsPayload(provision, { includePhg: true });
    if (!payload) {
      this.aprsBeaconPending = true;
      return false;
    }

    const frame = `${this.aprsState.callsign}>${APRS_BEACON_DEST},${APRS_BEACON_PATH}:${payload}`;
    const sent = this.aprsClient.sendLine(frame);
    if (sent) {
      this.aprsLastBeaconAt = Date.now();
      this.aprsBeaconPending = false;
      this.emitLog('APRS', `beacon sent reason=${reason}`);
      this.restartAprsBeaconLoop('interval');
      if (this.callmeshState.provision) {
        this.emitAprsUplink({
          flowId: null,
          payload,
          frame,
          timestamp: Date.now(),
          provision: true
        });
      }
      return true;
    }
    this.aprsBeaconPending = true;
    this.emitLog('APRS', `beacon send failed reason=${reason}`);
    return false;
  }

  sendAprsStatus(reason = 'interval') {
    if (!this.callmeshState.apiKey || !this.callmeshState.verified) return false;
    if (!this.aprsClient?.connected) return false;
    if (!this.aprsState.callsign) return false;
    if (!this.aprsLoginVerified) return false;

    const version = this.appVersion || '0.0.0';
    const agentKey = String(this.agentProduct || '').toLowerCase();
    const isCliAgent = agentKey === 'callmesh-client-cli' || agentKey.endsWith('-cli');
    const agentLabel = isCliAgent ? 'Client CLI' : 'Client';
    const payload = `>TMAG ${agentLabel} v${version}`;
    const frame = `${this.aprsState.callsign}>${APRS_BEACON_DEST},${APRS_BEACON_PATH}:${payload}`;
    const sent = this.aprsClient.sendLine(frame);
    if (sent) {
      this.aprsLastStatusAt = Date.now();
      this.emitLog('APRS', `status sent reason=${reason} version=${version}`);
      return true;
    }
    return false;
  }

  sendAprsTelemetryDefinitions(reason = 'manual', { force = false } = {}) {
    if (!this.callmeshState.apiKey || !this.callmeshState.verified) return false;
    if (!this.aprsClient?.connected) return false;
    if (!this.aprsState.callsign) return false;
    if (!this.aprsLoginVerified) return false;

    const now = Date.now();
    if (!force && this.aprsLastTelemetryAt && (now - this.aprsLastTelemetryAt) < APRS_TELEMETRY_INTERVAL_MS) {
      return false;
    }

    const destination = formatAprsMessageDestination(this.aprsState.callsign);
    const header = `${this.aprsState.callsign}>${APRS_BEACON_DEST},${APRS_BEACON_PATH}`;
    const messages = [
      `::${destination}:PARM.ALL_PKTS_10M,FWD_APRS_10M,POS_PKTS_10M,MSG_PKTS_10M,CTRL_PKTS_10M`,
      `::${destination}:UNIT.cnt,cnt,cnt,cnt,cnt`,
      `::${destination}:EQNS.0,1,0,0,1,0,0,1,0,0,1,0,0,1,0`
    ];

    let sentCount = 0;
    for (const info of messages) {
      const frame = `${header}${info}`;
      const sent = this.aprsClient.sendLine(frame);
      if (sent) {
        sentCount += 1;
      } else {
        this.emitLog('APRS', `telemetry frame send failed reason=${reason} payload=${info.slice(1)}`);
        break;
      }
    }

    if (sentCount === messages.length) {
      this.aprsLastTelemetryAt = now;
      this.emitLog('APRS', `telemetry definition sent reason=${reason}`);
      this.scheduleAprsTelemetryDefinitions('interval');
      return true;
    }

    this.scheduleAprsTelemetryDefinitions('retry');
    return false;
  }

  sendAprsTelemetryData(reason = 'interval', { force = false } = {}) {
    if (!this.callmeshState.apiKey || !this.callmeshState.verified) return false;
    if (!this.aprsClient?.connected) return false;
    if (!this.aprsState.callsign) return false;
    if (!this.aprsLoginVerified) return false;

    const now = Date.now();
    if (!force && this.aprsLastTelemetryDataAt) {
      const elapsed = now - this.aprsLastTelemetryDataAt;
      if (elapsed < APRS_TELEMETRY_DATA_INTERVAL_MS) {
        this.scheduleAprsTelemetryData('interval');
        return false;
      }
    }

    const totals = this.getTelemetryWindowSummary(now);
    this.aprsTelemetrySequence = (this.aprsTelemetrySequence % 999) + 1;
    const seqLabel = String(this.aprsTelemetrySequence).padStart(3, '0');

    const fields = [
      formatTelemetryValue(totals.all),
      formatTelemetryValue(totals.aprs),
      formatTelemetryValue(totals.pos),
      formatTelemetryValue(totals.msg),
      formatTelemetryValue(totals.ctrl)
    ];

    const payload = `T#${seqLabel},${fields.join(',')},00000000`;
    const frame = `${this.aprsState.callsign}>${APRS_BEACON_DEST},${APRS_BEACON_PATH}:${payload}`;
    const sent = this.aprsClient.sendLine(frame);
    if (sent) {
      this.aprsLastTelemetryDataAt = now;
      this.emitLog(
        'APRS',
        `telemetry data sent reason=${reason} seq=${seqLabel} all=${fields[0]} aprs=${fields[1]} pos=${fields[2]} msg=${fields[3]} ctrl=${fields[4]}`
      );
      this.scheduleAprsTelemetryData('interval');
      this.persistTelemetryState().catch(() => {});
      return true;
    }
    this.emitLog('APRS', `telemetry data send failed reason=${reason}`);
    this.scheduleAprsTelemetryData('retry');
    return false;
  }

  handleAprsSummary(summary) {
    if (!summary || summary.type !== 'Position') return;
    if (!this.callmeshState.apiKey || !this.callmeshState.verified) return;
    if (!this.callmeshState.provision) return;
    if (!this.aprsClient?.connected || !this.aprsState.callsign) return;

    const pos = summary.position;
    if (!pos || !Number.isFinite(pos.latitude) || !Number.isFinite(pos.longitude)) {
      return;
    }

    const meshId = normalizeMeshId(summary.from?.meshId);
    const isSelfMesh = meshId && this.selfMeshId && meshId === this.selfMeshId;

    const mapping = meshId ? this.findMappingForMeshId(meshId) : null;

    if (!mapping) {
      if (meshId && !this.aprsSkippedMeshIds.has(meshId)) {
        this.aprsSkippedMeshIds.add(meshId);
      }
      return;
    }

    const mappingCallsign = mapping ? deriveAprsCallsignFromMapping(mapping) : null;
    const sourceCallsign = mappingCallsign;
    if (!sourceCallsign) {
      if (meshId && !this.aprsSkippedMeshIds.has(meshId)) {
        this.aprsSkippedMeshIds.add(meshId);
      }
      return;
    }
    if (meshId && this.aprsSkippedMeshIds.has(meshId)) {
      this.aprsSkippedMeshIds.delete(meshId);
    }

    let commentSource = '';
    if (mapping) {
      const mappingComment = mapping.aprs_comment ?? mapping.aprsComment ?? mapping.comment;
      if (mappingComment != null && String(mappingComment).trim() !== '') {
        commentSource = String(mappingComment);
      }
    }

    const sanitizedComment = commentSource ? sanitizeAprsComment(commentSource) : '';

    const courseDegrees =
      Number.isFinite(pos.course) ? pos.course
        : Number.isFinite(pos.heading) ? pos.heading
          : Number.isFinite(pos.velHeading) ? pos.velHeading
            : null;

    let speedKnots = null;
    if (Number.isFinite(pos.speedKnots)) {
      speedKnots = pos.speedKnots;
    } else if (Number.isFinite(pos.speedMps)) {
      speedKnots = pos.speedMps * 1.943844;
    } else if (Number.isFinite(pos.velocityHoriz)) {
      speedKnots = pos.velocityHoriz * 1.943844;
    } else if (Number.isFinite(pos.speedKph)) {
      speedKnots = pos.speedKph / 1.852;
    }

    const courseInt = Number.isFinite(courseDegrees)
      ? Math.round(((courseDegrees % 360) + 360) % 360)
      : null;
    const speedInt = Number.isFinite(speedKnots)
      ? Math.max(0, Math.min(999, Math.round(speedKnots)))
      : null;

    const mappingAltitude = Number(
      mapping?.altitude_m ?? mapping?.altitudeMeters ?? mapping?.altitude
    );
    const provisionAltitude = Number(this.callmeshState.provision?.altitude);
    const altitudeMeters = Number.isFinite(pos.altitude)
      ? pos.altitude
      : Number.isFinite(mappingAltitude)
        ? mappingAltitude
        : (isSelfMesh && Number.isFinite(provisionAltitude) ? provisionAltitude : undefined);

    const symbolInfo = resolveAprsSymbol({ mapping, provision: this.callmeshState.provision });
    const overlayCharResolved = pickSymbolChar(symbolInfo.symbolOverlay);
    const tableCharResolved = pickSymbolChar(symbolInfo.symbolTable) ?? '/';
    const symbolCharResolved = pickSymbolChar(symbolInfo.symbolCode) ?? '>';

    const latDigest = pos.latitude.toFixed(5);
    const lonDigest = pos.longitude.toFixed(5);
    const altitudeFeetDigest = Number.isFinite(altitudeMeters)
      ? Math.round(Number(altitudeMeters) * 3.28084)
      : 'null';
    const digest = `${sourceCallsign}|${overlayCharResolved ?? 'null'}|${tableCharResolved}|${symbolCharResolved}|${latDigest}|${lonDigest}|${courseInt ?? 'null'}|${speedInt ?? 'null'}|${altitudeFeetDigest}|${sanitizedComment || 'null'}`;
    if (meshId) {
      const lastEntry = this.aprsLastPositionDigest.get(meshId);
      if (lastEntry && lastEntry.digest === digest && (Date.now() - lastEntry.timestamp) < APRS_POSITION_DEDUP_WINDOW_MS) {
        return;
      }
    }

    const payload = buildAprsPayload(this.callmeshState.provision, {
      latitude: pos.latitude,
      longitude: pos.longitude,
      altitudeMeters,
      ...symbolInfo,
      comment: sanitizedComment,
      courseDegrees,
      speedKnots
    });
    if (!payload) return;

    const qCallsign = this.getProvisionAprsCallsign() || this.aprsState.callsign;
    const pathParts = [APRS_MESH_PATH, APRS_Q_CONSTRUCT];
    if (qCallsign) pathParts.push(qCallsign);
    const frame = `${sourceCallsign}>${APRS_BEACON_DEST},${pathParts.join(',')}:${payload}`;
    const sent = this.aprsClient.sendLine(frame);
    if (sent) {
      this.recordTelemetryAprsForward(summary.timestampMs);
      if (meshId) {
        this.rememberAprsPositionDigest(meshId, digest);
      }
      const sourceLabel = summary.from?.label || meshId || sourceCallsign;
      this.emitLog(
        'APRS',
        `uplink ${sourceCallsign} from ${sourceLabel} lat=${pos.latitude.toFixed(4)} lon=${pos.longitude.toFixed(4)}`
      );
      if (summary?.flowId) {
        this.emitAprsUplink({
          flowId: summary.flowId,
          payload,
          frame,
          timestamp: Date.now()
        });
      }
    }
  }

  rememberAprsPositionDigest(meshId, digest) {
    if (!meshId) return;
    const now = Date.now();
    this.aprsLastPositionDigest.set(meshId, { digest, timestamp: now });
    if (this.aprsLastPositionDigest.size > APRS_POSITION_CACHE_LIMIT) {
      const firstKey = this.aprsLastPositionDigest.keys().next().value;
      if (firstKey) {
        this.aprsLastPositionDigest.delete(firstKey);
      }
    }
  }

  getProvisionAprsCallsign() {
    if (!this.callmeshState.provision) return null;
    return formatAprsCallsign(
      this.callmeshState.provision.callsign_base ?? this.callmeshState.provision.callsignBase,
      this.callmeshState.provision.ssid
    );
  }

  findMappingForMeshId(meshId) {
    if (!meshId || !Array.isArray(this.callmeshState.mappingItems)) return null;
    for (const item of this.callmeshState.mappingItems) {
      if (!item) continue;
      const candidate = normalizeMeshId(item.mesh_id ?? item.meshId);
      if (candidate && candidate === meshId) {
        return item;
      }
    }
    return null;
  }

  updateAprsProvision(provision) {
    if (!provision) {
      this.aprsState.callsignBase = null;
      this.aprsState.callsign = null;
      this.aprsState.ssid = null;
      this.aprsState.passcode = null;
      this.aprsLastTelemetryAt = 0;
      this.aprsLastTelemetryDataAt = 0;
      this.aprsTelemetrySequence = 0;
      this.stopAprsTelemetryDefinitionLoop();
      this.stopAprsTelemetryDataLoop();
      this.stopAprsBeaconLoop();
      this.aprsBeaconPending = false;
      this.teardownAprsConnection();
      return;
    }

    const prevBase = this.aprsState.callsignBase || null;
    const prevSsid = this.aprsState.ssid ?? null;
    const prevCallsign = this.aprsState.callsign || null;

    const baseRaw = provision.callsign_base || provision.callsignBase || provision.base || '';
    const normalizedBase = baseRaw ? baseRaw.toUpperCase().replace(/[^A-Z0-9]/g, '') : '';
    const nextSsid = provision.ssid;
    const formatted = formatAprsCallsign(normalizedBase, nextSsid);
    if (!formatted) {
      this.emitLog('APRS', 'provision 缺少合法呼號，無法登入 APRS');
      this.teardownAprsConnection();
      return;
    }

    const nextCallsign = formatted.toUpperCase();
    const shouldReconnect =
      Boolean(this.aprsClient?.connected && this.callmeshState.verified) &&
      Boolean(
        !prevCallsign ||
        prevCallsign !== nextCallsign ||
        prevBase !== normalizedBase ||
        prevSsid !== nextSsid
      );

    this.aprsState.callsignBase = normalizedBase;
    this.aprsState.callsign = nextCallsign;
    this.aprsState.ssid = nextSsid;
    this.aprsState.passcode = computeAprsPasscode(this.aprsState.callsignBase);
    this.emitLog('APRS', `provision 為 ${this.aprsState.callsign} pass=${this.aprsState.passcode}`);

    if (shouldReconnect) {
      this.aprsState.actualServer = null;
      this.emitState();
      if (!this.callmeshState.apiKey || !this.callmeshState.verified) {
        return;
      }
      this.teardownAprsConnection();
      this.ensureAprsConnection();
      return;
    }

    this.emitState();
    if (!this.callmeshState.apiKey || !this.callmeshState.verified) {
      return;
    }

    this.ensureAprsConnection();
    if (this.aprsClient?.connected && this.aprsLoginVerified) {
      const beaconSent = this.sendAprsBeacon('provision-update');
      this.aprsBeaconPending = !beaconSent;
    } else {
      this.aprsBeaconPending = true;
    }
  }

  applyCachedProvisionFallback(reason) {
    if (!this.callmeshState.cachedProvision) return false;
    const current = JSON.stringify(this.callmeshState.provision || {});
    const cached = JSON.stringify(this.callmeshState.cachedProvision || {});
    if (current === cached) return false;
    this.callmeshState.provision = cloneProvision(this.callmeshState.cachedProvision);
    this.callmeshState.lastProvisionRaw = stableStringify(this.callmeshState.cachedProvision || {});
    this.updateAprsProvision(this.callmeshState.provision);
    this.emitLog('CALLMESH', `using cached provision${reason ? ` (${reason})` : ''}`);
    this.emitState();
    return true;
  }

  getTelemetryBucket(timestampMs) {
    const bucketStart = Math.floor(timestampMs / TELEMETRY_BUCKET_MS) * TELEMETRY_BUCKET_MS;
    let bucket = this.telemetryBuckets.get(bucketStart);
    if (!bucket) {
      bucket = {
        all: 0,
        aprs: 0,
        pos: 0,
        msg: 0,
        ctrl: 0
      };
      this.telemetryBuckets.set(bucketStart, bucket);
    }
    return bucket;
  }

  pruneTelemetryBuckets(now = Date.now()) {
    const cutoff = now - TELEMETRY_WINDOW_MS;
    for (const [key] of this.telemetryBuckets.entries()) {
      if ((key + TELEMETRY_BUCKET_MS) <= cutoff) {
        this.telemetryBuckets.delete(key);
      }
    }
  }

  addTelemetryMetric(metric, timestampMs = Date.now(), amount = 1) {
    const bucket = this.getTelemetryBucket(timestampMs);
    if (bucket[metric] == null) {
      bucket[metric] = 0;
    }
    bucket[metric] += amount;
    this.pruneTelemetryBuckets(timestampMs);
  }

  storeTelemetrySummary(summary, timestampMs = Date.now()) {
    if (!summary?.telemetry || !summary.telemetry.metrics) {
      return;
    }
    const fromMeshId = normalizeMeshId(summary.from?.meshId || summary.from?.meshIdNormalized);
    if (!fromMeshId) {
      return;
    }
    if (this.selfMeshId && fromMeshId === this.selfMeshId) {
      return;
    }
    const record = this.buildTelemetryRecord(summary, {
      meshId: fromMeshId,
      timestampMs
    });
    if (!record) {
      return;
    }
    const nodeInfo = extractTelemetryNode(summary.from);
    this.addTelemetryRecord(fromMeshId, record, {
      node: nodeInfo,
      persist: true,
      emitEvent: true
    });
  }

  addTelemetryRecord(meshId, record, { node = null, persist = false, emitEvent = false } = {}) {
    if (!meshId || !record) {
      return;
    }
    const key = meshId;
    const storedRecord = cloneTelemetryRecord(record) || record;
    storedRecord.meshId = key;
    if (!storedRecord.id || typeof storedRecord.id !== 'string' || !storedRecord.id.trim()) {
      storedRecord.id = `${key}-${storedRecord.timestampMs ?? Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
    }
    if (this.telemetryRecordIds.has(storedRecord.id)) {
      return;
    }

    const registryNode = this.nodeDatabase.get(key);
    const mergedNode = mergeNodeInfo(
      storedRecord.node,
      node,
      registryNode,
      { meshId: key, meshIdOriginal: registryNode?.meshIdOriginal ?? key }
    );
    if (mergedNode) {
      storedRecord.node = mergedNode;
    }

    let bucket = this.telemetryStore.get(key);
    if (!bucket) {
      bucket = {
        meshId: key,
        node: mergedNode,
        records: []
      };
      this.telemetryStore.set(key, bucket);
    } else if (mergedNode) {
      bucket.node = mergeNodeInfo(bucket.node, mergedNode);
    }

    bucket.records.push(storedRecord);
    this.telemetryRecordIds.add(storedRecord.id);

    const overflow = bucket.records.length - this.telemetryMaxEntriesPerNode;
    if (overflow > 0) {
      const removed = bucket.records.splice(0, overflow);
      for (const removedRecord of removed) {
        if (removedRecord?.id) {
          this.telemetryRecordIds.delete(removedRecord.id);
        }
      }
    }

    this.telemetryUpdatedAt = Date.now();

    if (persist) {
      this.appendTelemetryRecord(storedRecord).catch((err) => {
        this.emitLog('CALLMESH', `append telemetry failed: ${err.message}`);
      });
    }

    if (emitEvent) {
      const nodePayload = bucket.node ? { ...bucket.node } : null;
      this.emitTelemetryUpdate({
        meshId: key,
        node: nodePayload,
        record: storedRecord
      });
    }
  }

  normalizeTelemetryRecordFromDisk(record) {
    if (!record || typeof record !== 'object') {
      return null;
    }
    const meshId = normalizeMeshId(record.meshId || record.mesh_id || record.meshIdNormalized);
    if (!meshId) {
      return null;
    }
    if (!record.telemetry || typeof record.telemetry !== 'object' || !record.telemetry.metrics) {
      return null;
    }
    const cloned = cloneTelemetryRecord(record) || record;
    cloned.meshId = meshId;
    const timestampMs = Number.isFinite(cloned.timestampMs) ? Number(cloned.timestampMs) : Date.now();
    cloned.timestampMs = timestampMs;
    cloned.timestamp = cloned.timestamp || new Date(timestampMs).toISOString();
    const sampleTimeMs = Number.isFinite(cloned.sampleTimeMs)
      ? Number(cloned.sampleTimeMs)
      : Number.isFinite(cloned.telemetry?.timeMs)
        ? Number(cloned.telemetry.timeMs)
        : timestampMs;
    cloned.sampleTimeMs = sampleTimeMs;
    cloned.sampleTime = cloned.sampleTime || new Date(sampleTimeMs).toISOString();
    if (!cloned.id || typeof cloned.id !== 'string' || !cloned.id.trim()) {
      cloned.id = `${meshId}-${timestampMs}-${Math.random().toString(16).slice(2, 10)}`;
    }
    cloned.telemetry = cloned.telemetry || {};
    cloned.telemetry.kind = cloned.telemetry.kind || 'unknown';
    cloned.telemetry.metrics = cloneTelemetryMetrics(cloned.telemetry.metrics || {});
    cloned.node = mergeNodeInfo(
      extractTelemetryNode(cloned.node),
      this.nodeDatabase.get(meshId),
      { meshId }
    );
    return cloned;
  }

  async appendTelemetryRecord(record) {
    const filePath = this.getTelemetryStorePath();
    const payload = cloneTelemetryRecord(record) || record;
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    await fs.appendFile(filePath, `${JSON.stringify(payload)}\n`, 'utf8');
  }

  buildTelemetryRecord(summary, { meshId, timestampMs = Date.now() } = {}) {
    if (!summary || !meshId || !summary.telemetry || !summary.telemetry.metrics) {
      return null;
    }
    const metricsKeys = Object.keys(summary.telemetry.metrics || {});
    if (!metricsKeys.length) {
      return null;
    }
    const baseTimestampMs = Number.isFinite(timestampMs) ? Number(timestampMs) : Date.now();
    const telemetry = summary.telemetry;
    const sampleTimeMs = Number.isFinite(telemetry.timeMs) ? Number(telemetry.timeMs) : null;
    const recordTimestampIso = new Date(baseTimestampMs).toISOString();
    const sampleIso = sampleTimeMs != null ? new Date(sampleTimeMs).toISOString() : null;
    const node = mergeNodeInfo(
      extractTelemetryNode(summary.from),
      this.nodeDatabase.get(meshId),
      { meshId }
    );
    const recordId = `${meshId}-${baseTimestampMs}-${Math.random().toString(16).slice(2, 10)}`;

    return {
      id: recordId,
      meshId,
      node,
      timestampMs: baseTimestampMs,
      timestamp: recordTimestampIso,
      sampleTimeMs: sampleTimeMs != null ? sampleTimeMs : baseTimestampMs,
      sampleTime: sampleIso ?? recordTimestampIso,
      type: summary.type || '',
      detail: summary.detail || '',
      channel: summary.channel ?? null,
      snr: Number.isFinite(summary.snr) ? summary.snr : null,
      rssi: Number.isFinite(summary.rssi) ? summary.rssi : null,
      flowId: summary.flowId || null,
      telemetry: {
        kind: telemetry.kind || 'unknown',
        timeSeconds: Number.isFinite(telemetry.timeSeconds) ? telemetry.timeSeconds : null,
        timeMs: Number.isFinite(telemetry.timeMs) ? telemetry.timeMs : null,
        metrics: cloneTelemetryMetrics(telemetry.metrics)
      }
    };
  }

  emitTelemetryUpdate({ meshId, node, record, reset = false }) {
    if (reset) {
      this.emit('telemetry', {
        type: 'reset',
        updatedAt: this.telemetryUpdatedAt,
        stats: this.getTelemetryStats()
      });
      return;
    }
    this.emit('telemetry', {
      type: 'append',
      meshId,
      node: node ? { ...node } : null,
      record: cloneTelemetryRecord(record),
      updatedAt: this.telemetryUpdatedAt,
      stats: this.getTelemetryStats()
    });
  }

  async clearTelemetryStore({ silent = false } = {}) {
    this.telemetryStore.clear();
    this.telemetryRecordIds.clear();
    this.telemetryUpdatedAt = Date.now();
    try {
      await fs.rm(this.getTelemetryStorePath(), { force: true });
    } catch (err) {
      if (err && err.code !== 'ENOENT') {
        this.emitLog('CALLMESH', `remove telemetry store failed: ${err.message}`);
      }
    }
    if (!silent) {
      this.emitTelemetryUpdate({ reset: true });
    }
  }

  getTelemetrySnapshot({ limitPerNode } = {}) {
    const limit =
      Number.isFinite(limitPerNode) && limitPerNode > 0
        ? Math.floor(limitPerNode)
        : this.telemetryMaxEntriesPerNode;
    const nodes = [];
    for (const [meshId, bucket] of this.telemetryStore.entries()) {
      const records = bucket.records || [];
      const start = limit > 0 && records.length > limit ? records.length - limit : 0;
      const slice = records.slice(start).map((record) => cloneTelemetryRecord(record));
      nodes.push({
        meshId,
        node: bucket.node ? { ...bucket.node } : null,
        records: slice
      });
    }
    nodes.sort((a, b) => {
      const labelA = (a.node?.label || a.meshId || '').toLowerCase();
      const labelB = (b.node?.label || b.meshId || '').toLowerCase();
      if (labelA < labelB) return -1;
      if (labelA > labelB) return 1;
      return 0;
    });
    return {
      updatedAt: this.telemetryUpdatedAt,
      nodes,
      stats: this.getTelemetryStats({ cachedNodes: nodes })
    };
  }

  getTelemetryStats({ cachedNodes = null } = {}) {
    let totalRecords = 0;
    const source = cachedNodes;
    if (Array.isArray(source)) {
      totalRecords = source.reduce((acc, item) => acc + (Array.isArray(item.records) ? item.records.length : 0), 0);
    } else {
      for (const bucket of this.telemetryStore.values()) {
        totalRecords += Array.isArray(bucket.records) ? bucket.records.length : 0;
      }
    }
    let diskBytes = 0;
    try {
      const stats = fsSync.statSync(this.getTelemetryStorePath());
      diskBytes = Number.isFinite(stats.size) ? stats.size : 0;
    } catch (err) {
      if (err && err.code !== 'ENOENT') {
        this.emitLog('CALLMESH', `stat telemetry store failed: ${err.message}`);
      }
    }
    return {
      totalRecords,
      totalNodes: this.telemetryStore.size,
      diskBytes
    };
  }

  recordTelemetryPacket(summary) {
    if (!summary) return;
    const timestampMs = Number.isFinite(summary.timestampMs) ? Number(summary.timestampMs) : Date.now();
    const fromMeshId = normalizeMeshId(summary.from?.meshId || summary.from?.meshIdNormalized);
    if (fromMeshId && this.selfMeshId && fromMeshId === this.selfMeshId) {
      return;
    }
    this.addTelemetryMetric('all', timestampMs);

    const typeRaw = typeof summary.type === 'string' ? summary.type : '';
    const type = typeRaw.toLowerCase();

    if (TELEMETRY_POSITION_TYPES.has(type)) {
      this.addTelemetryMetric('pos', timestampMs);
    } else if (TELEMETRY_MESSAGE_TYPES.has(type)) {
      this.addTelemetryMetric('msg', timestampMs);
    } else if (TELEMETRY_CONTROL_TYPES.has(type)) {
      this.addTelemetryMetric('ctrl', timestampMs);
    }
    this.storeTelemetrySummary(summary, timestampMs);
  }

  recordTelemetryAprsForward(timestampMs = Date.now()) {
    this.addTelemetryMetric('aprs', timestampMs);
  }

  getTelemetryWindowSummary(now = Date.now()) {
    this.pruneTelemetryBuckets(now);
    const totals = {
      all: 0,
      aprs: 0,
      pos: 0,
      msg: 0,
      ctrl: 0
    };
    const cutoff = now - TELEMETRY_WINDOW_MS;
    for (const [bucketStart, bucket] of this.telemetryBuckets.entries()) {
      if ((bucketStart + TELEMETRY_BUCKET_MS) <= cutoff) continue;
      totals.all += bucket.all || 0;
      totals.aprs += bucket.aprs || 0;
      totals.pos += bucket.pos || 0;
      totals.msg += bucket.msg || 0;
      totals.ctrl += bucket.ctrl || 0;
    }
    return totals;
  }

  ensureSummaryFlowMetadata(summary) {
    if (!summary) return;
    const now = Date.now();
    let timestampMs = Number.isFinite(Number(summary.timestampMs)) ? Number(summary.timestampMs) : NaN;
    if (Number.isNaN(timestampMs)) {
      if (typeof summary.timestamp === 'string') {
        const parsed = Date.parse(summary.timestamp);
        timestampMs = Number.isFinite(parsed) ? parsed : now;
      } else if (Number.isFinite(summary.timestamp)) {
        timestampMs = Number(summary.timestamp);
      } else {
        timestampMs = now;
      }
    }
    summary.timestampMs = timestampMs;
    if (!summary.flowId) {
      summary.flowId = `${timestampMs}-${Math.random().toString(16).slice(2, 10)}`;
    }
  }
}

function createInitialCallmeshState({ apiKey = '', verified = false, agentProduct = 'callmesh-client' } = {}) {
  const trimmed = typeof apiKey === 'string' ? apiKey.trim() : '';
  const hasKey = Boolean(trimmed);
  const stateVerified = Boolean(verified && hasKey);
  return {
    apiKey: trimmed,
    agent: buildAgentString({ product: agentProduct }),
    verified: stateVerified,
    verifiedKey: stateVerified ? trimmed : '',
    lastStatus: stateVerified ? 'CallMesh: 尚未同步' : 'CallMesh: 未設定 Key',
    degraded: false,
    lastHeartbeatAt: null,
    lastMappingHash: null,
    lastMappingSyncedAt: null,
    provision: null,
    cachedProvision: null,
    lastProvisionRaw: null,
    mappingItems: []
  };
}

function isAuthError(err) {
  const message = (err?.message || '').toLowerCase();
  return message.includes('401') || message.includes('invalid');
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
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
  value = value.replace(/[^0-9a-f]/gi, '').toLowerCase();
  if (!value) return null;
  return `!${value}`;
}

function extractTelemetryNode(node) {
  if (!node || typeof node !== 'object') {
    return null;
  }
  const meshId = typeof node.meshId === 'string' ? node.meshId : null;
  const normalized = normalizeMeshId(
    meshId || node.meshIdNormalized || node.mesh_id || node.meshIdHex
  );
  return {
    label: node.label ?? null,
    meshId,
    meshIdNormalized: normalized,
    meshIdOriginal: node.meshIdOriginal ?? node.meshId ?? null,
    shortName: node.shortName ?? null,
    longName: node.longName ?? null,
    hwModel: node.hwModel ?? null,
    role: node.role ?? null,
    raw: Number.isFinite(node.raw) ? Number(node.raw) : null
  };
}

function buildNodeLabel(node) {
  if (!node || typeof node !== 'object') return null;
  const name = node.longName || node.shortName || null;
  const meshOriginal = node.meshIdOriginal || node.meshId || null;
  const meshNormalized = node.meshId || node.meshIdNormalized || null;
  const meshLabel = meshOriginal || meshNormalized || null;
  if (name && meshLabel) {
    return `${name} (${meshLabel})`;
  }
  return name || meshLabel || null;
}

function mergeNodeInfo(...sources) {
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
    raw: null,
    lastSeenAt: null
  };
  let hasValue = false;
  for (const source of sources) {
    if (!source || typeof source !== 'object') continue;
    const entries = Array.isArray(source) ? source : [source];
    for (const item of entries) {
      if (!item || typeof item !== 'object') continue;
      for (const [key, value] of Object.entries(item)) {
        if (value === undefined || value === null) {
          continue;
        }
        if (Object.prototype.hasOwnProperty.call(result, key)) {
          result[key] = value;
          hasValue = true;
        }
        if (key === 'lastSeenAt' && Number.isFinite(value)) {
          result.lastSeenAt = Number(value);
        }
      }
      const position = item.position ?? item.gps ?? item.location ?? null;
      if (position && typeof position === 'object') {
        if (Number.isFinite(position.latitude)) {
          result.latitude = Number(position.latitude);
          hasValue = true;
        }
        if (Number.isFinite(position.longitude)) {
          result.longitude = Number(position.longitude);
          hasValue = true;
        }
        if (Number.isFinite(position.altitude)) {
          result.altitude = Number(position.altitude);
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
  if (!result.meshId && result.meshIdNormalized) {
    result.meshId = result.meshIdNormalized;
  }
  if (result.hwModel && !result.hwModelLabel) {
    const resolved = resolveHardwareModel(result.hwModel);
    result.hwModel = resolved.code ?? result.hwModel;
    result.hwModelLabel = resolved.label ?? result.hwModelLabel ?? null;
  }
  if (result.role && !result.roleLabel) {
    const resolvedRole = resolveDeviceRole(result.role);
    result.role = resolvedRole.code ?? result.role;
    result.roleLabel = resolvedRole.label ?? result.roleLabel ?? null;
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
    result.label = buildNodeLabel(result);
  }
  return result;
}

function deepCloneTelemetryValue(value) {
  if (value == null) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => deepCloneTelemetryValue(item));
  }
  if (value && typeof value === 'object') {
    const result = {};
    for (const [key, nested] of Object.entries(value)) {
      result[key] = deepCloneTelemetryValue(nested);
    }
    return result;
  }
  return value;
}

function cloneTelemetryMetrics(metrics) {
  if (!metrics || typeof metrics !== 'object') {
    return {};
  }
  return deepCloneTelemetryValue(metrics);
}

function cloneTelemetryRecord(record) {
  if (!record) {
    return null;
  }
  return {
    ...record,
    node: record.node ? { ...record.node } : null,
    telemetry: record.telemetry
      ? {
          ...record.telemetry,
          metrics: cloneTelemetryMetrics(record.telemetry.metrics)
        }
      : null
  };
}

function cloneProvision(provision) {
  if (!provision) return null;
  try {
    return normalizeProvision(provision);
  } catch {
    return null;
  }
}

function normalizeProvision(provision) {
  if (!provision) return null;
  return sortObject(provision);
}

function stableStringify(value) {
  return JSON.stringify(sortObject(value));
}

function sortObject(input) {
  if (Array.isArray(input)) {
    return input.map((item) => sortObject(item));
  }
  if (input && typeof input === 'object' && !(input instanceof Date)) {
    const sorted = {};
    for (const key of Object.keys(input).sort()) {
      sorted[key] = sortObject(input[key]);
    }
    return sorted;
  }
  return input;
}

function computeAprsPasscode(callsignBase) {
  if (!callsignBase) return 0;
  const base = String(callsignBase).toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (!base) return 0;
  let hash = 0x73e2;
  for (let i = 0; i < base.length; i++) {
    hash ^= base.charCodeAt(i) << 8;
    if (++i >= base.length) break;
    hash ^= base.charCodeAt(i);
  }
  return hash & 0x7fff;
}

function formatAprsSsid(ssid) {
  if (ssid === null || ssid === undefined) return '';
  if (ssid === 0) return '';
  if (ssid < 0) return `${ssid}`;
  return `-${ssid}`;
}

function formatAprsCallsign(callsignBase, ssid) {
  const base = (callsignBase || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (!base) return null;
  const suffix = formatAprsSsid(ssid);
  return suffix ? `${base}${suffix}` : base;
}

function buildAprsPayload(provision, overrides = {}) {
  const latValue =
    (hasOwn(overrides, 'latitude') ? overrides.latitude
      : hasOwn(overrides, 'lat') ? overrides.lat
        : provision?.latitude ?? provision?.lat);
  const lonValue =
    (hasOwn(overrides, 'longitude') ? overrides.longitude
      : hasOwn(overrides, 'lon') ? overrides.lon
        : provision?.longitude ?? provision?.lon);
  const latStr = formatAprsLatitude(latValue);
  const lonStr = formatAprsLongitude(lonValue);
  if (!latStr || !lonStr) return null;

  const overlayProvided = hasOwn(overrides, 'symbolOverlay');
  const tableProvided = hasOwn(overrides, 'symbolTable');
  const codeProvided = hasOwn(overrides, 'symbolCode');

  const overlayChar = overlayProvided
    ? overrides.symbolOverlay
    : (provision?.symbol_overlay ?? provision?.symbolOverlay);
  const symbolTableChar = tableProvided
    ? overrides.symbolTable
    : (provision?.symbol_table ?? provision?.symbolTable);
  const tableChar = (overlayChar != null && overlayChar !== '' && String(overlayChar)[0])
    || (symbolTableChar && String(symbolTableChar)[0])
    || '/';
  const symbolSource = codeProvided
    ? overrides.symbolCode
    : (provision?.symbol_code ?? provision?.symbolCode);
  const symbolChar = (symbolSource && String(symbolSource)[0]) || '>';

  let comment = '';
  if (hasOwn(overrides, 'comment')) {
    comment = overrides.comment != null ? sanitizeAprsComment(overrides.comment) : '';
  } else if (provision?.comment) {
    comment = sanitizeAprsComment(provision.comment);
  }
  if (comment) {
    comment = comment.trim();
  }

  let phgDigits = null;
  const includePhg = overrides.includePhg === true;
  if (includePhg) {
    const phgSource = hasOwn(overrides, 'phg') ? overrides.phg : (provision?.phg ?? null);
    if (phgSource != null) {
      const raw = String(phgSource).trim().toUpperCase();
      if (/^[0-9]{3,4}$/.test(raw)) {
        const normalized = raw.length === 3 ? `${raw}0` : raw.slice(0, 4);
        phgDigits = normalized;
      }
    }
  }

  const courseDegrees =
    hasOwn(overrides, 'courseDegrees') ? overrides.courseDegrees
      : hasOwn(overrides, 'course') ? overrides.course
        : hasOwn(overrides, 'heading') ? overrides.heading
          : null;
  const speedKnots =
    hasOwn(overrides, 'speedKnots') ? overrides.speedKnots
      : hasOwn(overrides, 'speed') ? overrides.speed
        : null;
  const altitudeMeters =
    hasOwn(overrides, 'altitudeMeters') ? overrides.altitudeMeters
      : hasOwn(overrides, 'altitude') ? overrides.altitude
        : (provision?.altitude ?? null);
  let altitudeSection = '';
  if (Number.isFinite(altitudeMeters)) {
    const feet = Math.round(Number(altitudeMeters) * 3.28084);
    const clamped = Math.max(0, Math.min(999999, feet));
    altitudeSection = `/A=${String(clamped).padStart(6, '0')}`;
  }

  let frame = `!${latStr}${tableChar}${lonStr}${symbolChar}`;
  if (Number.isFinite(courseDegrees) || Number.isFinite(speedKnots)) {
    const course = Number.isFinite(courseDegrees)
      ? Math.round(((courseDegrees % 360) + 360) % 360)
      : 0;
    const speed = Number.isFinite(speedKnots)
      ? Math.max(0, Math.min(999, Math.round(speedKnots)))
      : 0;
    frame += `${String(course).padStart(3, '0')}/${String(speed).padStart(3, '0')}`;
  }
  frame += altitudeSection;
  if (phgDigits) {
    comment = comment ? `PHG${phgDigits}${comment}` : `PHG${phgDigits}`;
  }
  if (comment) {
    frame += comment;
  }
  return frame;
}

function hasOwn(obj, key) {
  return Object.prototype.hasOwnProperty.call(obj, key);
}

function formatAprsLatitude(value) {
  if (!Number.isFinite(value)) return null;
  const hemisphere = value >= 0 ? 'N' : 'S';
  const abs = Math.abs(value);
  let degrees = Math.floor(abs);
  let minutes = (abs - degrees) * 60;
  if (minutes >= 59.995) {
    minutes = 0;
    degrees += 1;
  }
  if (degrees > 90) degrees = 90;
  const degStr = String(degrees).padStart(2, '0');
  const minStr = formatAprsMinutes(minutes);
  return `${degStr}${minStr}${hemisphere}`;
}

function formatAprsLongitude(value) {
  if (!Number.isFinite(value)) return null;
  const hemisphere = value >= 0 ? 'E' : 'W';
  const abs = Math.abs(value);
  let degrees = Math.floor(abs);
  let minutes = (abs - degrees) * 60;
  if (minutes >= 59.995) {
    minutes = 0;
    degrees += 1;
  }
  if (degrees > 180) degrees = 180;
  const degStr = String(degrees).padStart(3, '0');
  const minStr = formatAprsMinutes(minutes);
  return `${degStr}${minStr}${hemisphere}`;
}

function formatAprsMinutes(minutes) {
  const value = Number(minutes);
  if (!Number.isFinite(value)) return '00.00';
  return value.toFixed(2).padStart(5, '0');
}

function formatAprsMessageDestination(callsign) {
  const raw = String(callsign || '').toUpperCase();
  if (!raw) return ''.padEnd(9, ' ');
  const sanitized = raw.replace(/[^A-Z0-9\-]/g, '').slice(0, 9);
  return sanitized.padEnd(9, ' ');
}

const TELEMETRY_POSITION_TYPES = new Set([
  'position',
  'waypoint',
  'envtelemetry',
  'telemetry',
  'remotetelemetry',
  'remoteposition'
]);

const TELEMETRY_MESSAGE_TYPES = new Set([
  'text',
  'message',
  'data',
  'storeforward'
]);

const TELEMETRY_CONTROL_TYPES = new Set([
  'nodeinfo',
  'routing',
  'routerequest',
  'routereply',
  'routeerror',
  'admin',
  'config',
  'traceroute',
  'remotehardware',
  'neighborinfo',
  'keyverification'
]);

function sanitizeAprsComment(comment) {
  if (!comment) return '';
  const cleaned = String(comment).replace(/[\r\n]+/g, ' ').replace(/\s+/g, ' ').trim();
  return cleaned;
}

function pickSymbolChar(value) {
  if (!value && value !== 0) return null;
  const str = String(value);
  return str.length > 0 ? str[0] : null;
}

function analyzeSymbolSource(source, { preferSymbolString = false } = {}) {
  const info = {
    overlay: null,
    overlayProvided: false,
    table: null,
    tableProvided: false,
    code: null,
    codeProvided: false,
    symbolStringProvided: false
  };
  if (!source) return info;

  const hasOwnProp = (key) => Object.prototype.hasOwnProperty.call(source, key);

  const setOverlay = (value) => {
    info.overlay = value != null ? pickSymbolChar(value) : null;
    info.overlayProvided = true;
  };
  const setTable = (value, { force = false } = {}) => {
    if (force || !info.tableProvided) {
      info.table = value != null ? pickSymbolChar(value) : null;
    }
    info.tableProvided = true;
  };
  const setCode = (value, { force = false } = {}) => {
    if (force || !info.codeProvided) {
      info.code = value != null ? pickSymbolChar(value) : null;
    }
    info.codeProvided = true;
  };

  if (hasOwnProp('symbol')) {
    info.symbolStringProvided = true;
    const str = source.symbol;
    const normalized = str != null ? String(str) : '';
    const tableChar = normalized.length >= 1 ? normalized[0] : null;
    const codeChar = normalized.length >= 2 ? normalized[1] : null;
    setTable(tableChar, { force: true });
    setCode(codeChar, { force: true });
  }

  const overlayKeys = ['symbol_overlay', 'symbolOverlay', 'aprs_symbol_overlay', 'aprsSymbolOverlay'];
  for (const key of overlayKeys) {
    if (hasOwnProp(key)) {
      setOverlay(source[key]);
      break;
    }
  }

  const tableKeys = ['symbol_table', 'symbolTable', 'aprs_symbol_table', 'aprsSymbolTable'];
  for (const key of tableKeys) {
    if (hasOwnProp(key)) {
      setTable(source[key], { force: !(preferSymbolString && info.symbolStringProvided) });
      break;
    }
  }

  const codeKeys = ['symbol_code', 'symbolCode', 'aprs_symbol_code', 'aprsSymbolCode'];
  for (const key of codeKeys) {
    if (hasOwnProp(key)) {
      setCode(source[key], { force: !(preferSymbolString && info.symbolStringProvided) });
      break;
    }
  }

  return info;
}

function resolveAprsSymbol({ mapping, provision }) {
  const mappingSymbol = analyzeSymbolSource(mapping, { preferSymbolString: true });
  const provisionSymbol = analyzeSymbolSource(provision || null, { preferSymbolString: false });

  const symbolOverlay = mappingSymbol.overlayProvided
    ? mappingSymbol.overlay
    : (provisionSymbol.overlayProvided ? provisionSymbol.overlay : null);

  const symbolTable = mappingSymbol.tableProvided
    ? (mappingSymbol.table ?? '/')
    : (provisionSymbol.tableProvided ? (provisionSymbol.table ?? '/') : '/');

  const symbolCode = mappingSymbol.codeProvided
    ? (mappingSymbol.code ?? '>')
    : (provisionSymbol.codeProvided ? (provisionSymbol.code ?? '>') : '>');

  return {
    symbolOverlay,
    symbolTable,
    symbolCode
  };
}

function deriveAprsCallsignFromMapping(mapping) {
  if (!mapping) return null;
  if (mapping.aprs_callsign) return String(mapping.aprs_callsign).toUpperCase();
  if (mapping.aprsCallsign) return String(mapping.aprsCallsign).toUpperCase();
  if (mapping.aprs_callsign_with_ssid) return String(mapping.aprs_callsign_with_ssid).toUpperCase();
  if (mapping.aprsCallsignWithSsid) return String(mapping.aprsCallsignWithSsid).toUpperCase();
  if (mapping.callsign_with_ssid) return String(mapping.callsign_with_ssid).toUpperCase();
  if (mapping.callsignWithSsid) return String(mapping.callsignWithSsid).toUpperCase();
  const base =
    mapping.aprs_callsign_base ??
    mapping.aprsCallsignBase ??
    mapping.callsign_base ??
    mapping.callsignBase ??
    mapping.callsignBase;
  const ssid =
    mapping.aprs_ssid ?? mapping.aprsSsid ?? mapping.ssid ?? mapping.SSID;
  if (base) {
    const formatted = formatAprsCallsign(base, ssid);
    if (formatted) return formatted;
  }
  if (mapping.callsign) return String(mapping.callsign).toUpperCase();
  return null;
}

function formatTelemetryValue(value) {
  if (!Number.isFinite(value)) return '0';
  const clamped = Math.max(0, Math.min(999, Math.round(value)));
  return String(clamped);
}

function toFiniteNumber(value) {
  if (value == null) return null;
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return null;
  }
  return number;
}

function roundTo(value, digits = 2) {
  if (!Number.isFinite(value)) {
    return value;
  }
  const factor = 10 ** Math.max(0, digits);
  return Math.round(value * factor) / factor;
}

function resolveSpeed(position) {
  if (!position || typeof position !== 'object') {
    return 0;
  }
  const candidates = [
    position.speedMps,
    position.groundSpeed,
    position.speed,
    position.airSpeed,
    position.velHoriz
  ];
  for (const value of candidates) {
    const numeric = toFiniteNumber(value);
    if (numeric != null) {
      return numeric;
    }
  }
  const speedKph = toFiniteNumber(position.speedKph);
  if (speedKph != null) {
    return speedKph / 3.6;
  }
  const speedKnots = toFiniteNumber(position.speedKnots);
  if (speedKnots != null) {
    return speedKnots * 0.514444;
  }
  return 0;
}

function resolveHeading(position) {
  if (!position || typeof position !== 'object') {
    return 0;
  }
  const raw = toFiniteNumber(position.heading ?? position.course ?? position.velHeading);
  if (raw == null) {
    return 0;
  }
  const normalized = raw % 360;
  return normalized < 0 ? normalized + 360 : normalized;
}

function formatTimestampWithOffset(source, offsetMinutes) {
  if (source == null || !Number.isFinite(offsetMinutes)) {
    return null;
  }

  let date;
  if (source instanceof Date) {
    date = new Date(source.getTime());
  } else {
    const parsed = new Date(source);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    date = parsed;
  }

  const shifted = new Date(date.getTime() + offsetMinutes * 60_000);
  const iso = shifted.toISOString().replace(/\.\d{3}Z$/, '');
  const sign = offsetMinutes >= 0 ? '+' : '-';
  const abs = Math.abs(offsetMinutes);
  const hours = String(Math.floor(abs / 60)).padStart(2, '0');
  const minutes = String(abs % 60).padStart(2, '0');
  return `${iso}${sign}${hours}:${minutes}`;
}

module.exports = {
  CallMeshAprsBridge,
  createInitialCallmeshState,
  normalizeMeshId,
  deriveAprsCallsignFromMapping
};
