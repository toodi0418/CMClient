'use strict';

const path = require('path');
const fs = require('fs/promises');
const fsSync = require('fs');
const EventEmitter = require('events');
const crypto = require('crypto');
const readline = require('readline');
const { CallMeshClient, buildAgentString } = require('./client');
const { APRSClient } = require('../aprs/client');
const { nodeDatabase } = require('../nodeDatabase');
const { TelemetryDatabase } = require('../storage/telemetryDatabase');
const { CallMeshDataStore } = require('../storage/callmeshDataStore');
const WebSocket = require('ws');

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

const TENMAN_FORWARD_WS_ENDPOINT = 'wss://tenmanmap.yakumo.tw/ws';
const TENMAN_FORWARD_DEFAULT_ENABLED =
  !['1', 'true', 'yes', 'on'].includes(
    String(process.env.TENMAN_DISABLE || '').trim().toLowerCase()
  );
const TENMAN_FORWARD_TIMEZONE_OFFSET_MINUTES = 8 * 60;
const TENMAN_FORWARD_QUEUE_LIMIT = 64;
const TENMAN_FORWARD_RECONNECT_DELAY_MS = 5000;
const TENMAN_FORWARD_AUTH_ACTION = 'authenticate';
const TENMAN_FORWARD_SUPPRESS_ACK = ['1', 'true', 'yes', 'on'].includes(
  String(process.env.TENMAN_SUPPRESS_ACK ?? 'true').trim().toLowerCase()
);
const TENMAN_FORWARD_VERBOSE_LOG = ['1', 'true', 'yes', 'on'].includes(
  String(process.env.TENMAN_VERBOSE_LOG ?? 'false').trim().toLowerCase()
);
const TENMAN_INBOUND_MIN_INTERVAL_MS = 5000;
const TENMAN_FORWARD_NODE_UPDATE_BUCKET_MS = 30_000;
const MESHTASTIC_BROADCAST_ADDR = 0xffffffff;

const PROTO_DIR = path.resolve(__dirname, '..', '..', 'proto');

function formatTimestampLabel(date) {
  const pad = (value) => String(value).padStart(2, '0');
  return `${pad(date.getMonth() + 1)}/${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

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
    this.telemetryDb = null;
    this.callmeshDataStorePath = path.join(this.storageDir, 'callmesh-data.sqlite');
    this.dataStore = new CallMeshDataStore(this.callmeshDataStorePath);
    this.dataStoreInitialized = false;

    this.tenmanForwardOverride =
      typeof options.shareWithTenmanMap === 'boolean' ? options.shareWithTenmanMap : null;
    this.tenmanForwardState = {
      lastKey: null,
      websocket: null,
      connecting: false,
      queue: [],
      pendingKeys: new Set(),
      sending: false,
      reconnectTimer: null,
      authenticated: false,
      authenticating: false,
      missingApiKeyWarned: false,
      gatewayId: null,
      gatewayMeshId: null,
      nodeId: null,
      disabledLogged: false,
      suppressAck: TENMAN_FORWARD_SUPPRESS_ACK,
      nodeSync: {
        signatures: new Map(),
        pendingSnapshot: true,
        pendingSnapshotReason: 'init',
        lastSnapshotAt: 0
      }
    };
    this.tenmanInboundState = {
      lastAcceptedAt: 0
    };

    this.meshtasticClients = new Set();

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
    return path.join(this.storageDir, 'telemetry-records.sqlite');
  }

  getLegacyTelemetryStorePath() {
    return path.join(this.storageDir, 'telemetry-records.jsonl');
  }

  initializeDataStore() {
    if (!this.dataStore || this.dataStoreInitialized) {
      return;
    }
    try {
      this.dataStore.init();
      this.dataStoreInitialized = true;
    } catch (err) {
      this.emitLog('CALLMESH', `init callmesh data store failed: ${err.message}`);
      throw err;
    }
  }

  getDataStore() {
    if (!this.dataStore) {
      return null;
    }
    if (!this.dataStoreInitialized) {
      this.initializeDataStore();
    }
    return this.dataStore;
  }

  async initializeTelemetryDatabase({ migrateLegacy = true } = {}) {
    if (this.telemetryDb) {
      return;
    }
    const dbPath = this.getTelemetryStorePath();
    try {
      this.telemetryDb = new TelemetryDatabase(dbPath);
      this.telemetryDb.init();
    } catch (err) {
      this.emitLog('CALLMESH', `init telemetry database failed: ${err.message}`);
      throw err;
    }
    if (migrateLegacy) {
      await this.migrateLegacyTelemetryStore();
    }
  }

  async migrateLegacyTelemetryStore() {
    const legacyPath = this.getLegacyTelemetryStorePath();
    if (!this.telemetryDb || !legacyPath) {
      return;
    }
    let legacyExists = false;
    try {
      await fs.access(legacyPath);
      legacyExists = true;
    } catch (err) {
      if (err && err.code !== 'ENOENT') {
        this.emitLog('CALLMESH', `check legacy telemetry file failed: ${err.message}`);
      }
    }
    if (!legacyExists) {
      return;
    }

    this.emitLog(
      'CALLMESH',
      '開始遷移遙測歷史紀錄至資料庫，過程可能需要一些時間，請勿關閉程式。'
    );

    let migrated = 0;
    await new Promise((resolve, reject) => {
      const stream = fsSync.createReadStream(legacyPath, { encoding: 'utf8' });
      const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

      const handleRecord = (line) => {
        const trimmed = line.trim();
        if (!trimmed) return;
        let parsed;
        try {
          parsed = JSON.parse(trimmed);
        } catch {
          return;
        }
        const normalized = this.normalizeTelemetryRecordFromDisk(parsed);
        if (!normalized) return;
        try {
          this.telemetryDb.insertRecord(normalized);
          migrated += 1;
        } catch (err) {
          this.emitLog('CALLMESH', `legacy telemetry insert failed: ${err.message}`);
        }
      };

      rl.on('line', handleRecord);
      rl.once('close', resolve);
      rl.once('error', reject);
      stream.once('error', reject);
    }).catch((err) => {
      this.emitLog('CALLMESH', `migrate telemetry jsonl failed: ${err.message}`);
    });

    if (migrated > 0) {
      this.emitLog('CALLMESH', `migrated telemetry history from jsonl count=${migrated}`);
    }

    const migratedPath = `${legacyPath}.migrated`;
    try {
      await fs.rename(legacyPath, migratedPath);
    } catch (err) {
      if (err && err.code === 'ENOENT') {
        return;
      }
      if (err && err.code === 'EEXIST') {
        const timestamped = `${legacyPath}.migrated-${Date.now()}`;
        try {
          await fs.rename(legacyPath, timestamped);
        } catch (renameErr) {
          this.emitLog('CALLMESH', `cleanup legacy telemetry jsonl failed: ${renameErr.message}`);
        }
      } else if (err) {
        this.emitLog('CALLMESH', `rename legacy telemetry jsonl failed: ${err.message}`);
      }
    }
  }

  async init({ allowRestore = true } = {}) {
    await this.ensureStorageDir();
    this.initializeDataStore();
    await this.restoreArtifacts({ allowRestore });
    await this.restoreTelemetryState();
    await this.initializeTelemetryDatabase({ migrateLegacy: allowRestore });
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
    this.resetTenmanNodeSyncSignatures();
    this.requestTenmanNodeSnapshot('cleared');
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
    let entries = null;
    const store = this.getDataStore();
    if (store) {
      try {
        entries = store.listNodes();
      } catch (err) {
        console.warn('從 SQLite 載入節點資料庫失敗:', err);
      }
    }
    if (!entries || !entries.length) {
      try {
        const filePath = this.getNodeDatabaseFilePath();
        const content = await fs.readFile(filePath, 'utf8');
        const payload = JSON.parse(content);
        entries = Array.isArray(payload?.nodes)
          ? payload.nodes
          : Array.isArray(payload)
            ? payload
            : [];
        if (entries.length && store) {
          try {
            store.replaceNodes(entries);
            await fs.rm(this.getNodeDatabaseFilePath(), { force: true });
          } catch (err) {
            console.warn('節點資料庫遷移至 SQLite 失敗:', err);
          }
        }
      } catch (err) {
        if (err && err.code !== 'ENOENT') {
          console.warn('載入節點資料庫失敗:', err);
        }
        entries = [];
      }
    }
    const restored = this.nodeDatabase.replace(entries || []);
    this.resetTenmanNodeSyncSignatures();
    this.requestTenmanNodeSnapshot('restore');
    if (restored.length) {
      this.emitLog('NODE-DB', `restored ${restored.length} nodes from disk`);
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
    const store = this.getDataStore();
    if (store) {
      try {
        store.replaceNodes(snapshot);
        return;
      } catch (err) {
        console.error('寫入節點資料庫失敗:', err);
      }
    }
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
    if (/^!0{6}[0-9a-f]{2}$/i.test(info.meshIdOriginal || '') || /^!0{6}[0-9a-f]{2}$/i.test(normalized)) {
      return null;
    }
    const result = this.nodeDatabase.upsert(normalized, info);
    if (result.changed) {
      this.forwardTenmanNodeUpdate(result.node);
      const label = buildNodeLabel(result.node);
      const rawId = result.node.meshId || result.node.meshIdOriginal || '';
      const normalizedId = normalizeMeshId(rawId);
      const looksLikePlaceholder =
        (label && label.includes('?')) ||
        (rawId && /^!0{6}[0-9a-f]{2}$/.test(rawId.toLowerCase())) ||
        (normalizedId && /^!0{6}[0-9a-f]{2}$/.test(normalizedId.toLowerCase()));
      if (!label || looksLikePlaceholder) {
        return result.node;
      }
      if (label && label.includes('?')) {
        return result.node;
      }
      if (!label) {
        return result.node;
      }
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
        label,
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

    const store = this.getDataStore();

    let mapping = store ? store.getKv('mapping') : null;
    if (!mapping) {
      const legacyMapping = await this.loadJsonSafe(this.getMappingFilePath());
      if (legacyMapping && store) {
        mapping = legacyMapping;
        try {
          store.setKv('mapping', mapping);
          await fs.rm(this.getMappingFilePath(), { force: true });
        } catch (err) {
          this.emitLog('CALLMESH', `archive legacy mapping failed: ${err.message}`);
        }
      } else if (legacyMapping) {
        mapping = legacyMapping;
      }
    }
    if (mapping) {
      this.callmeshState.lastMappingHash = mapping.hash ?? null;
      this.callmeshState.lastMappingSyncedAt = mapping.updatedAt ?? null;
      if (Array.isArray(mapping.items)) {
        this.callmeshState.mappingItems = mapping.items;
      }
      this.emitLog(
        'CALLMESH',
        `restore mapping hash=${this.callmeshState.lastMappingHash ?? 'null'} count=${this.callmeshState.mappingItems.length}`
      );
    }

    let provisionPayload = store ? store.getKv('provision') : null;
    if (!provisionPayload) {
      const legacyProvision = await this.loadJsonSafe(this.getProvisionFilePath());
      if (legacyProvision && store) {
        provisionPayload = legacyProvision;
        try {
          store.setKv('provision', provisionPayload);
          await fs.rm(this.getProvisionFilePath(), { force: true });
        } catch (err) {
          this.emitLog('CALLMESH', `archive legacy provision failed: ${err.message}`);
        }
      } else if (legacyProvision) {
        provisionPayload = legacyProvision;
      }
    }
    if (provisionPayload?.provision) {
      this.callmeshState.cachedProvision = cloneProvision(provisionPayload.provision);
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
    const store = this.getDataStore();
    if (store) {
      try {
        store.deleteKv('mapping');
        store.deleteKv('provision');
        store.deleteKv('telemetry_state');
        store.clearNodes();
        store.saveMessageLog([]);
        store.replaceRelayStats([]);
      } catch (err) {
        this.emitLog('CALLMESH', `clear sqlite artifacts failed: ${err.message}`);
      }
    }
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
    this.resetTenmanNodeSyncSignatures();
    this.requestTenmanNodeSnapshot('artifacts-cleared');
    await this.clearTelemetryStore({ silent: false });
    this.emitLog('CALLMESH', 'cleared local mapping/provision cache');
    this.updateAprsProvision(null);
  }

  async restoreTelemetryState() {
    const store = this.getDataStore();
    let payload = null;
    if (store) {
      try {
        payload = store.getKv('telemetry_state');
      } catch (err) {
        this.emitLog('CALLMESH', `load telemetry state from sqlite failed: ${err.message}`);
      }
    }
    if (!payload) {
      try {
        payload = await this.loadJsonSafe(this.getTelemetryStateFilePath());
        if (payload && store) {
          try {
            store.setKv('telemetry_state', payload);
            await fs.rm(this.getTelemetryStateFilePath(), { force: true });
          } catch (err) {
            this.emitLog('CALLMESH', `archive legacy telemetry state failed: ${err.message}`);
          }
        }
      } catch (err) {
        this.emitLog('CALLMESH', `restore telemetry sequence failed: ${err.message}`);
      }
    }
    const sequenceValue = Number(payload?.sequence ?? payload?.seq ?? payload?.lastSequence);
    if (Number.isFinite(sequenceValue) && sequenceValue >= 0) {
      const normalized = Math.floor(sequenceValue) % 1000;
      this.aprsTelemetrySequence = normalized;
    }
  }

  async loadTelemetryStore() {
    this.telemetryStore.clear();
    this.telemetryRecordIds.clear();
    this.telemetryUpdatedAt = Date.now();
    if (this.telemetryDb) {
      this.emitLog('CALLMESH', '遙測快取將於查詢時由資料庫動態載入');
    }
  }

  async persistTelemetryState() {
    const payload = {
      sequence: this.aprsTelemetrySequence,
      savedAt: new Date().toISOString()
    };
    const store = this.getDataStore();
    if (store) {
      try {
        store.setKv('telemetry_state', payload);
        return;
      } catch (err) {
        this.emitLog('CALLMESH', `儲存 Telemetry 序號失敗: ${err.message}`);
      }
    }
    await this.saveJson(this.getTelemetryStateFilePath(), payload);
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
    const timestampMs = Date.now();
    this.captureSummaryNodeInfo(summary, timestampMs);
    this.ensureSummaryFlowMetadata(summary);
    this.recordTelemetryPacket(summary);
    if (summary.type === 'Position') {
      this.handleAprsSummary(summary);
    }
    this.forwardTenmanPosition(summary);
    this.forwardTenmanMessage(summary);
  }

  attachMeshtasticClient(client) {
    if (!client || typeof client !== 'object') {
      return;
    }
    this.meshtasticClients.add(client);
  }

  detachMeshtasticClient(client) {
    if (!client) {
      return;
    }
    this.meshtasticClients.delete(client);
  }

  _getWritableMeshtasticClient() {
    if (!this.meshtasticClients || this.meshtasticClients.size === 0) {
      return null;
    }
    for (const client of this.meshtasticClients) {
      if (client && typeof client.sendTextMessage === 'function') {
        return client;
      }
    }
    return null;
  }

  async forwardTenmanPosition(summary) {
    if (!summary) {
      return;
    }

    try {
      const state = this.tenmanForwardState;
      if (!this.isTenmanForwardEnabled()) {
        if (state && !state.disabledLogged) {
          state.disabledLogged = true;
          this.emitLog('TENMAN', 'TenManMap 轉發已停用');
        }
        return;
      }
      if (state) {
        state.disabledLogged = false;
      }

      const meshIdNormalized = normalizeMeshId(
        summary?.from?.meshIdNormalized ?? summary?.from?.meshId ?? summary?.from?.mesh_id
      );

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

      const apiKey = this.getTenmanApiKey();
      if (!apiKey) {
        if (state && !state.missingApiKeyWarned) {
          state.missingApiKeyWarned = true;
          this.emitLog('TENMAN', '缺少 CallMesh API Key，請先完成驗證');
        }
        return;
      }
      if (state) {
        state.missingApiKeyWarned = false;
      }

      const deviceId =
        meshIdNormalized ||
        state.gatewayMeshId ||
        state.gatewayId ||
        state.nodeId ||
        'unknown';
      const dedupeKey = `${deviceId}:${timestamp}:${latitude.toFixed(6)}:${longitude.toFixed(6)}`;
      if (
        this.tenmanForwardState.lastKey === dedupeKey ||
        this.tenmanForwardState.pendingKeys?.has?.(dedupeKey)
      ) {
        return;
      }

      const nodeName =
        summary?.from?.longName || summary?.from?.shortName || summary?.from?.label || null;
      const extraPayload = {
        source: 'TMAG'
      };
      if (meshIdNormalized) {
        extraPayload.mesh_id = meshIdNormalized;
      }
      if (summary?.from?.shortName && summary.from.shortName !== nodeName) {
        extraPayload.short_name = summary.from.shortName;
      }
      if (summary?.from?.longName && summary.from.longName !== nodeName) {
        extraPayload.long_name = summary.from.longName;
      }

      const payload = {
        device_id: deviceId,
        timestamp,
        latitude,
        longitude,
        altitude,
        speed,
        heading
      };
      if (nodeName) {
        payload.node_name = nodeName;
      }
      if (Object.keys(extraPayload).length > 0) {
        payload.extra = extraPayload;
      }

      const message = {
        action: 'publish',
        payload
      };

      this.enqueueTenmanPublish(message, dedupeKey);
    } catch (err) {
      this.emitLog('TENMAN', `位置回報處理失敗: ${err.message}`);
    }
  }

  forwardTenmanMessage(summary) {
    if (!summary) {
      return;
    }

    try {
      const state = this.tenmanForwardState;
      if (!this.isTenmanForwardEnabled()) {
        if (state && !state.disabledLogged) {
          state.disabledLogged = true;
          this.emitLog('TENMAN', 'TenManMap 轉發已停用');
        }
        return;
      }
      if (state) {
        state.disabledLogged = false;
      }

      const typeLabel = typeof summary.type === 'string' ? summary.type.trim().toLowerCase() : '';
      if (!typeLabel || !typeLabel.includes('text')) {
        return;
      }
      const text =
        typeof summary.detail === 'string'
          ? summary.detail
          : summary.detail != null
            ? String(summary.detail)
            : '';
      if (!text) {
        return;
      }

      const messageId =
        typeof summary.flowId === 'string' && summary.flowId.trim()
          ? summary.flowId.trim()
          : `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
      const timestampSource =
        summary.timestamp ??
        (Number.isFinite(summary.timestampMs) ? new Date(Number(summary.timestampMs)).toISOString() : null) ??
        new Date().toISOString();
      const receivedAt = formatTimestampWithOffset(
        timestampSource,
        TENMAN_FORWARD_TIMEZONE_OFFSET_MINUTES
      );
      const channel = Number.isFinite(Number(summary.channel)) ? Number(summary.channel) : 0;
      const scope = summary?.to ? 'directed' : 'broadcast';

      const payload = {
        message_id: messageId,
        channel,
        text,
        encoding: 'utf-8',
        scope
      };
      if (receivedAt) {
        payload.received_at = receivedAt;
      }
      if (Number.isFinite(summary.meshPacketId)) {
        payload.mesh_packet_id = Number(summary.meshPacketId) >>> 0;
      }
      if (Number.isFinite(summary.replyId)) {
        payload.reply_id = Number(summary.replyId) >>> 0;
      }

      const fromDescriptor = this.buildTenmanNodeDescriptor(summary.from);
      if (fromDescriptor) {
        payload.from = fromDescriptor;
      }
      const toDescriptor = this.buildTenmanNodeDescriptor(summary.to);
      if (toDescriptor) {
        payload.to = toDescriptor;
      }
      const relayDescriptor = this.buildTenmanRelayDescriptor(summary);
      if (relayDescriptor) {
        payload.relay = relayDescriptor;
      }
      const hopsDescriptor = this.buildTenmanHopsDescriptor(summary?.hops);
      if (hopsDescriptor) {
        payload.hops = hopsDescriptor;
      }
      if (Number.isFinite(summary.rssi)) {
        payload.rssi = Number(summary.rssi);
      }
      if (Number.isFinite(summary.snr)) {
        payload.snr = Number(summary.snr);
      }
      if (Array.isArray(summary.extraLines) && summary.extraLines.length > 0) {
        payload.extra_lines = summary.extraLines;
      }
      if (summary.rawHex) {
        payload.raw_hex = summary.rawHex;
      }
      const rawLength = Number.isFinite(summary.rawLength)
        ? Number(summary.rawLength)
        : Buffer.byteLength(text, 'utf8');
      if (Number.isFinite(rawLength) && rawLength >= 0) {
        payload.raw_length = rawLength;
      }

      const dedupeKey = `msg:${messageId}`;
      const message = {
        action: 'message.publish',
        payload
      };
      this.enqueueTenmanPublish(message, dedupeKey);
    } catch (err) {
      this.emitLog('TENMAN', `文字訊息轉發失敗: ${err.message}`);
    }
  }

  forwardTenmanNodeSnapshot({ reason = null } = {}) {
    try {
      if (!this.isTenmanForwardEnabled()) {
        return false;
      }
      const syncState = this.getTenmanNodeSyncState();
      if (!syncState) {
        return false;
      }
      const rawNodes =
        typeof this.nodeDatabase?.list === 'function' ? this.nodeDatabase.list() : [];
      const nodes = Array.isArray(rawNodes) ? rawNodes : [];
      const nodePayloads = [];
      const signatures = new Map();
      for (const node of nodes) {
        const payload = this.buildTenmanNodePayload(node);
        if (!payload) continue;
        nodePayloads.push(payload);
        const signature = this.computeTenmanNodeSignature(node);
        if (signature) {
          signatures.set(payload.mesh_id, signature);
        }
      }
      const now = new Date();
      const generatedAt =
        formatTimestampWithOffset(now, TENMAN_FORWARD_TIMEZONE_OFFSET_MINUTES) ?? toIsoString(now);
      const message = {
        action: 'node.snapshot',
        payload: {
          version: 1,
          source: 'TMAG',
          generated_at: generatedAt,
          total: nodePayloads.length,
          nodes: nodePayloads
        }
      };
      if (reason) {
        message.payload.reason = reason;
      }
      const queued = this.enqueueTenmanPublish(
        message,
        `node:snapshot:${generatedAt ?? now.getTime()}`
      );
      if (queued) {
        syncState.signatures = signatures;
      }
      return queued;
    } catch (err) {
      this.emitLog('TENMAN', `節點快照同步失敗: ${err.message}`);
      return false;
    }
  }

  forwardTenmanNodeUpdate(node, { reason = null } = {}) {
    if (!node) {
      return;
    }
    try {
      if (!this.isTenmanForwardEnabled()) {
        return;
      }
      const payload = this.buildTenmanNodePayload(node);
      if (!payload) {
        return;
      }
      const signature = this.computeTenmanNodeSignature(node);
      if (!signature) {
        return;
      }
      const syncState = this.getTenmanNodeSyncState();
      const previousSignature = syncState.signatures.get(payload.mesh_id);
      if (previousSignature === signature) {
        return;
      }
      payload.version = 1;
      payload.source = 'TMAG';
      const syncedAt =
        formatTimestampWithOffset(new Date(), TENMAN_FORWARD_TIMEZONE_OFFSET_MINUTES) ??
        toIsoString(Date.now());
      if (syncedAt) {
        payload.synced_at = syncedAt;
      }
      if (reason) {
        payload.reason = reason;
      }
      const queued = this.enqueueTenmanPublish(
        {
          action: 'node.update',
          payload
        },
        `node:update:${payload.mesh_id}:${signature}`
      );
      if (queued) {
        syncState.signatures.set(payload.mesh_id, signature);
      }
    } catch (err) {
      this.emitLog('TENMAN', `節點資料同步失敗: ${err.message}`);
    }
  }

  buildTenmanNodePayload(node) {
    if (!node || typeof node !== 'object') {
      return null;
    }
    const meshId =
      normalizeMeshId(node.meshId ?? node.meshIdNormalized ?? node.meshIdOriginal ?? null) || null;
    if (!meshId) {
      return null;
    }
    const payload = {
      mesh_id: meshId
    };
    const original =
      typeof node.meshIdOriginal === 'string' && node.meshIdOriginal.trim()
        ? node.meshIdOriginal.trim()
        : null;
    if (original && original !== meshId) {
      payload.mesh_id_original = original;
    }
    const shortName =
      typeof node.shortName === 'string' && node.shortName.trim()
        ? node.shortName.trim()
        : null;
    if (shortName) {
      payload.short_name = shortName;
    }
    const longName =
      typeof node.longName === 'string' && node.longName.trim()
        ? node.longName.trim()
        : null;
    const hwModel =
      node.hwModel != null && String(node.hwModel).trim() ? String(node.hwModel).trim() : null;
    const hwModelLabel =
      node.hwModelLabel != null && String(node.hwModelLabel).trim()
        ? String(node.hwModelLabel).trim()
        : null;
    const role =
      node.role != null && String(node.role).trim() ? String(node.role).trim() : null;
    const roleLabel =
      node.roleLabel != null && String(node.roleLabel).trim()
        ? String(node.roleLabel).trim()
        : null;
    if (longName) {
      payload.long_name = longName;
    }
    if (hwModel) {
      payload.hw_model = hwModel;
    }
    if (hwModelLabel) {
      payload.hw_model_label = hwModelLabel;
    }
    if (role) {
      payload.role = role;
    }
    if (roleLabel) {
      payload.role_label = roleLabel;
    }
    const lastSeenIso = toIsoString(node.lastSeenAt ?? node.last_seen_at ?? null);
    if (lastSeenIso) {
      payload.last_seen_at = lastSeenIso;
    }
    const latitude = toFiniteNumber(node.latitude);
    const longitude = toFiniteNumber(node.longitude);
    const altitude = toFiniteNumber(node.altitude);
    if (latitude != null) {
      payload.latitude = roundTo(latitude, 6);
    }
    if (longitude != null) {
      payload.longitude = roundTo(longitude, 6);
    }
    if (altitude != null) {
      payload.altitude = roundTo(altitude, 2);
    }
    const labelCandidate = buildNodeLabel(node);
    const label = typeof labelCandidate === 'string' ? labelCandidate.trim() : '';
    if (label) {
      payload.label = label;
    }
    return payload;
  }

  computeTenmanNodeSignature(node) {
    if (!node || typeof node !== 'object') {
      return null;
    }
    const meshId =
      normalizeMeshId(node.meshId ?? node.meshIdNormalized ?? node.meshIdOriginal ?? null) || null;
    if (!meshId) {
      return null;
    }
    const original =
      typeof node.meshIdOriginal === 'string' && node.meshIdOriginal.trim()
        ? node.meshIdOriginal.trim()
        : null;
    const lastSeenCandidate = node.lastSeenAt ?? node.last_seen_at ?? null;
    let lastSeenMs = null;
    if (Number.isFinite(lastSeenCandidate)) {
      lastSeenMs = Number(lastSeenCandidate);
    } else if (typeof lastSeenCandidate === 'string' && lastSeenCandidate.trim()) {
      const parsed = Date.parse(lastSeenCandidate.trim());
      if (!Number.isNaN(parsed)) {
        lastSeenMs = parsed;
      }
    }
    const bucket =
      lastSeenMs != null
        ? Math.floor(lastSeenMs / TENMAN_FORWARD_NODE_UPDATE_BUCKET_MS)
        : null;
    const latitude = toFiniteNumber(node.latitude);
    const longitude = toFiniteNumber(node.longitude);
    const altitude = toFiniteNumber(node.altitude);
    const shortName =
      typeof node.shortName === 'string' && node.shortName.trim()
        ? node.shortName.trim()
        : null;
    const longName =
      typeof node.longName === 'string' && node.longName.trim()
        ? node.longName.trim()
        : null;
    const hwModel =
      node.hwModel != null && String(node.hwModel).trim() ? String(node.hwModel).trim() : null;
    const hwModelLabel =
      node.hwModelLabel != null && String(node.hwModelLabel).trim()
        ? String(node.hwModelLabel).trim()
        : null;
    const role =
      node.role != null && String(node.role).trim() ? String(node.role).trim() : null;
    const roleLabel =
      node.roleLabel != null && String(node.roleLabel).trim()
        ? String(node.roleLabel).trim()
        : null;
    const signaturePayload = {
      meshId,
      meshIdOriginal: original,
      shortName,
      longName,
      hwModel,
      hwModelLabel,
      role,
      roleLabel,
      latitude: latitude != null ? roundTo(latitude, 6) : null,
      longitude: longitude != null ? roundTo(longitude, 6) : null,
      altitude: altitude != null ? roundTo(altitude, 2) : null,
      lastSeenBucket: bucket
    };
    try {
      return crypto.createHash('sha1').update(stableStringify(signaturePayload)).digest('hex');
    } catch {
      return null;
    }
  }

  getTenmanNodeSyncState() {
    const state = this.tenmanForwardState;
    if (!state) {
      return null;
    }
    if (!state.nodeSync) {
      state.nodeSync = {
        signatures: new Map(),
        pendingSnapshot: false,
        pendingSnapshotReason: null,
        lastSnapshotAt: 0
      };
    }
    if (!(state.nodeSync.signatures instanceof Map)) {
      state.nodeSync.signatures = new Map();
    }
    return state.nodeSync;
  }

  resetTenmanNodeSyncSignatures() {
    const syncState = this.getTenmanNodeSyncState();
    if (syncState?.signatures instanceof Map) {
      syncState.signatures.clear();
    }
  }

  requestTenmanNodeSnapshot(reason = null) {
    const syncState = this.getTenmanNodeSyncState();
    if (!syncState) {
      return;
    }
    syncState.pendingSnapshot = true;
    if (reason) {
      syncState.pendingSnapshotReason = reason;
    }
    this.queueTenmanNodeSnapshot();
  }

  queueTenmanNodeSnapshot() {
    const syncState = this.getTenmanNodeSyncState();
    if (!syncState?.pendingSnapshot) {
      return;
    }
    const reason = syncState.pendingSnapshotReason || null;
    const queued = this.forwardTenmanNodeSnapshot({ reason });
    if (queued) {
      syncState.pendingSnapshot = false;
      syncState.pendingSnapshotReason = null;
      syncState.lastSnapshotAt = Date.now();
    }
  }

  buildTenmanNodeDescriptor(node) {
    if (!node || typeof node !== 'object') {
      return null;
    }
    const meshIdCandidate =
      node.meshId ?? node.meshIdNormalized ?? node.mesh_id ?? node.meshIdOriginal ?? null;
    const meshIdNormalized = normalizeMeshId(meshIdCandidate);
    const descriptor = {};
    if (meshIdNormalized) {
      descriptor.mesh_id = meshIdNormalized;
    }
    const meshIdOriginal =
      node.meshIdOriginal && typeof node.meshIdOriginal === 'string'
        ? node.meshIdOriginal
        : meshIdCandidate && meshIdCandidate !== meshIdNormalized
          ? meshIdCandidate
          : null;
    if (meshIdOriginal && meshIdOriginal !== meshIdNormalized) {
      descriptor.mesh_id_original = meshIdOriginal;
    }

    const registry =
      meshIdNormalized && this.nodeDatabase
        ? this.nodeDatabase.get(meshIdNormalized)
        : null;
    const shortName =
      node.shortName ??
      node.short_name ??
      registry?.shortName ??
      null;
    if (shortName) {
      descriptor.short_name = shortName;
    }
    const longName =
      node.longName ??
      node.long_name ??
      registry?.longName ??
      null;
    if (longName) {
      descriptor.long_name = longName;
    }
    const lastSeenCandidate =
      node.lastSeenAt ??
      node.last_seen_at ??
      registry?.lastSeenAt ??
      null;
    const lastSeenIso = toIsoString(lastSeenCandidate);
    if (lastSeenIso) {
      descriptor.last_seen_at = lastSeenIso;
    }
    if (Number.isFinite(node.raw)) {
      descriptor.raw = Number(node.raw);
    }
    return Object.keys(descriptor).length > 0 ? descriptor : null;
  }

  buildTenmanRelayDescriptor(summary) {
    const relay = summary?.relay;
    const descriptor = relay ? this.buildTenmanNodeDescriptor(relay) : null;
    const payload = descriptor ? { ...descriptor } : {};
    const guessed =
      relay?.guessed ??
      summary?.relayGuess ??
      null;
    if (guessed != null) {
      payload.guessed = Boolean(guessed);
    }
    const reason = relay?.reason ?? summary?.relayGuessReason ?? null;
    if (reason) {
      payload.reason = String(reason);
    }
    return Object.keys(payload).length > 0 ? payload : null;
  }

  buildTenmanHopsDescriptor(hops) {
    if (!hops || typeof hops !== 'object') {
      return null;
    }
    const payload = {};
    const start = Number.isFinite(hops.start) ? Number(hops.start) : null;
    const limit = Number.isFinite(hops.limit) ? Number(hops.limit) : null;
    if (start != null) {
      payload.start = start;
    }
    if (limit != null) {
      payload.limit = limit;
    }
    if (start != null && limit != null) {
      payload.used = Math.max(start - limit, 0);
    } else if (start != null && limit == null) {
      payload.used = 0;
    }
    const label = typeof hops.label === 'string' ? hops.label.trim() : '';
    if (label) {
      payload.label = label;
    }
    return Object.keys(payload).length > 0 ? payload : null;
  }

  buildSummaryNodeForMesh(meshId, { fallbackLabel = null } = {}) {
    const normalized = normalizeMeshId(meshId);
    if (!normalized) {
      if (!fallbackLabel) {
        return null;
      }
      return {
        label: fallbackLabel,
        meshId: null,
        meshIdNormalized: null,
        meshIdOriginal: null,
        shortName: null,
        longName: null,
        raw: null
      };
    }
    const registry = this.nodeDatabase?.get(normalized) || null;
    const shortName = registry?.shortName ?? null;
    const longName = registry?.longName ?? null;
    const name = longName || shortName || null;
    const label = name ? `${name} (${normalized})` : normalized;
    const raw = meshIdToUint32(normalized);
    const node = {
      label,
      meshId: normalized,
      meshIdNormalized: normalized,
      meshIdOriginal: registry?.meshIdOriginal ?? null,
      shortName,
      longName,
      hwModel: registry?.hwModel ?? null,
      role: registry?.role ?? null,
      raw: Number.isFinite(raw) ? raw >>> 0 : null
    };
    if (registry?.hwModelLabel) {
      node.hwModelLabel = registry.hwModelLabel;
    }
    if (registry?.roleLabel) {
      node.roleLabel = registry.roleLabel;
    }
    if (registry?.lastSeenAt) {
      node.lastSeenAt = registry.lastSeenAt;
    }
    return node;
  }

  emitTenmanSyntheticSummary({
    text,
    channel,
    flowId,
    meshPacketId,
    scope,
    replyTo,
    replyId,
    meshDestination
  }) {
    const timestampMs = Date.now();
    const timestamp = new Date(timestampMs);
    const fromNode =
      this.buildSummaryNodeForMesh(this.selfMeshId, { fallbackLabel: '本機節點' }) ||
      {
        label: '本機節點',
        meshId: this.selfMeshId || null,
        meshIdNormalized: this.selfMeshId || null,
        meshIdOriginal: this.selfMeshId || null,
        shortName: null,
        longName: null,
        raw: meshIdToUint32(this.selfMeshId)
      };
    let toNode = null;
    if (scope === 'directed' && meshDestination && meshDestination !== 'broadcast') {
      toNode =
        this.buildSummaryNodeForMesh(meshDestination) ||
        {
          label: meshDestination,
          meshId: meshDestination,
          meshIdNormalized: meshDestination,
          meshIdOriginal: meshDestination,
          shortName: null,
          longName: null,
          raw: meshIdToUint32(meshDestination)
        };
    }
    const safeText = typeof text === 'string' ? text : text != null ? String(text) : '';
    const buffer = Buffer.from(safeText, 'utf8');
    const summary = {
      type: 'Text',
      detail: safeText,
      channel,
      timestamp: timestamp.toISOString(),
      timestampLabel: formatTimestampLabel(timestamp),
      timestampMs,
      from: fromNode,
      to: toNode,
      relay: null,
      hops: null,
      snr: null,
      rssi: null,
      meshPacketId: Number.isFinite(meshPacketId) ? meshPacketId >>> 0 : null,
      replyId: Number.isFinite(replyId) ? replyId >>> 0 : null,
      replyTo: replyTo || null,
      flowId,
      scope,
      synthetic: true,
      rawHex: buffer.length > 0 ? buffer.toString('hex') : null,
      rawLength: buffer.length,
      extraLines: []
    };
    if (scope === 'broadcast' && replyTo) {
      summary.extraLines.push(`回覆對象: ${replyTo}`);
    }
    this.emit('summary', summary);
  }

  sendTenmanControlMessage(message) {
    const state = this.tenmanForwardState;
    if (!state || !state.websocket || state.websocket.readyState !== WebSocket.OPEN) {
      return false;
    }
    try {
      const serialized = JSON.stringify(message);
      state.websocket.send(serialized);
      return true;
    } catch (err) {
      this.emitLog('TENMAN', `控制訊息傳送失敗: ${err.message}`);
      return false;
    }
  }

  sendTenmanAck(action, details = {}) {
    const message = {
      type: 'ack',
      action,
      status: details.status ?? 'ok'
    };
    for (const [key, value] of Object.entries(details)) {
      if (value !== undefined) {
        message[key] = value;
      }
    }
    return this.sendTenmanControlMessage(message);
  }

  sendTenmanError(action, errorCode, errorMessage, extra = {}) {
    const payload = {
      type: 'error',
      action,
      status: 'error',
      error_code: errorCode,
      message: errorMessage
    };
    for (const [key, value] of Object.entries(extra || {})) {
      if (value !== undefined) {
        payload[key] = value;
      }
    }
    return this.sendTenmanControlMessage(payload);
  }

  async handleTenmanSendMessageCommand(message) {
    const payload = message?.payload || {};
    const clientMessageIdRaw =
      typeof payload.client_message_id === 'string' ? payload.client_message_id.trim() : '';
    const clientMessageId = clientMessageIdRaw || null;
    const respondError = (code, msg, extra = {}) =>
      this.sendTenmanError('send_message', code, msg, {
        ...extra,
        client_message_id: clientMessageId ?? undefined
      });

    if (!this.isTenmanForwardEnabled()) {
      respondError('DISABLED', 'TenManMap 分享已停用');
      return;
    }
    if (!this.callmeshState?.apiKey || !this.callmeshState?.verified) {
      respondError('UNAUTHORIZED', 'CallMesh API Key 尚未驗證');
      return;
    }

    const client = this._getWritableMeshtasticClient();
    if (!client) {
      respondError('ROUTING_UNAVAILABLE', '沒有可用的 Meshtastic 客戶端');
      return;
    }

    const encodingRaw =
      typeof payload.encoding === 'string' ? payload.encoding.trim().toLowerCase() : 'utf-8';
    let text;
    if (!encodingRaw || encodingRaw === 'utf-8' || encodingRaw === 'utf8' || encodingRaw === 'text') {
      text =
        typeof payload.text === 'string'
          ? payload.text
          : payload.text != null
            ? String(payload.text)
            : '';
    } else if (encodingRaw === 'base64') {
      try {
        text = Buffer.from(String(payload.text ?? ''), 'base64').toString('utf8');
      } catch (err) {
        respondError('INVALID_PAYLOAD', `Base64 內容解碼失敗: ${err.message}`);
        return;
      }
    } else {
      respondError('INVALID_PAYLOAD', `不支援的 encoding：${encodingRaw}`);
      return;
    }

    if (!text) {
      respondError('INVALID_PAYLOAD', '文字內容不可為空');
      return;
    }

    const channelValue = Number(payload.channel);
    const channel = Number.isFinite(channelValue) ? Math.max(0, Math.floor(channelValue)) : 0;
    const scopeRaw =
      typeof payload.scope === 'string' ? payload.scope.trim().toLowerCase() : 'broadcast';
    const scope = scopeRaw || 'broadcast';
    const wantAck = Boolean(payload.want_ack);
    let replyToNormalized = null;
    let replyIdNumeric = null;
    const replyIdCandidateRaw = payload.reply_id ?? payload.replyId ?? null;
    if (replyIdCandidateRaw != null) {
      const parsed = Number(replyIdCandidateRaw);
      if (!Number.isFinite(parsed) || parsed < 0) {
        respondError('INVALID_PAYLOAD', 'reply_id 必須為非負整數');
        return;
      }
      replyIdNumeric = parsed >>> 0;
    }

    let destination = MESHTASTIC_BROADCAST_ADDR;
    let meshDestinationLabel = 'broadcast';
    if (scope === 'broadcast') {
      destination = MESHTASTIC_BROADCAST_ADDR;
      meshDestinationLabel = 'broadcast';
      const replyToCandidate =
        payload.reply_to ??
        payload.destination ??
        payload.mesh_destination ??
        payload.meshId ??
        payload.mesh_id ??
        payload.to ??
        null;
      replyToNormalized = normalizeMeshId(replyToCandidate);
    } else if (scope === 'directed') {
      const destinationRaw =
        payload.destination ??
        payload.mesh_destination ??
        payload.meshId ??
        payload.mesh_id ??
        payload.to ??
        null;
      const normalizedDestination = normalizeMeshId(destinationRaw);
      if (!normalizedDestination) {
        respondError('INVALID_DESTINATION', 'directed 範圍需提供有效的 destination mesh id');
        return;
      }
      const destinationNumeric = meshIdToUint32(normalizedDestination);
      if (!Number.isFinite(destinationNumeric)) {
        respondError('INVALID_DESTINATION', 'destination mesh id 無法轉換為數值');
        return;
      }
      destination = destinationNumeric >>> 0;
      meshDestinationLabel = normalizedDestination;
    } else if (scope) {
      respondError('UNSUPPORTED_SCOPE', `scope=${scope} 尚未支援`);
      return;
    }

    const maxPayloadLength =
      Number.isFinite(client?.constantsEnum?.values?.DATA_PAYLOAD_LEN)
        ? Number(client.constantsEnum.values.DATA_PAYLOAD_LEN)
        : 233;
    const textBytes = Buffer.byteLength(text, 'utf8');
    if (textBytes > maxPayloadLength) {
      respondError('MESSAGE_TOO_LONG', `文字長度 ${textBytes} bytes 超過上限 ${maxPayloadLength}`, {
        max_bytes: maxPayloadLength,
        actual_bytes: textBytes
      });
      return;
    }

    const inboundState = this.tenmanInboundState || (this.tenmanInboundState = { lastAcceptedAt: 0 });
    const now = Date.now();
    if (now - inboundState.lastAcceptedAt < TENMAN_INBOUND_MIN_INTERVAL_MS) {
      const retryAfterMs = TENMAN_INBOUND_MIN_INTERVAL_MS - (now - inboundState.lastAcceptedAt);
      respondError('RATE_LIMITED', 'TenManMap 訊息傳送過於頻繁', {
        retry_after_ms: Math.max(0, retryAfterMs)
      });
      return;
    }

    try {
      const packetId = await client.sendTextMessage({
        text,
        channel,
        destination,
        wantAck,
        replyId: replyIdNumeric
      });
      inboundState.lastAcceptedAt = Date.now();
      const flowId = `tenman-${inboundState.lastAcceptedAt}-${Math.random().toString(16).slice(2, 10)}`;
      const queuedAt = formatTimestampWithOffset(
        inboundState.lastAcceptedAt,
        TENMAN_FORWARD_TIMEZONE_OFFSET_MINUTES
      );
      this.sendTenmanAck('send_message', {
        status: 'accepted',
        client_message_id: clientMessageId ?? undefined,
        flow_id: flowId,
        mesh_destination: meshDestinationLabel,
        channel,
        want_ack: wantAck,
        scope,
        queued_at: queuedAt,
        encoding: 'utf-8',
        bytes: textBytes,
        mesh_packet_id: Number.isFinite(packetId) ? Number(packetId) >>> 0 : undefined,
        reply_to: replyToNormalized ?? undefined,
        reply_id: replyIdNumeric ?? undefined
      });
      const destinationLog =
        scope === 'directed' ? `destination=${meshDestinationLabel}` : 'broadcast';
      const replyLog =
        scope === 'broadcast' && replyToNormalized ? ` reply_to=${replyToNormalized}` : '';
      const replyIdLog =
        replyIdNumeric != null ? ` reply_id=${replyIdNumeric}` : '';
      const packetIdLabel =
        Number.isFinite(packetId) ? ` packet_id=${Number(packetId) >>> 0}` : '';
      this.emitLog(
        'TENMAN',
        `已接受 TenManMap 訊息 scope=${scope} channel=${channel} ${destinationLog}${replyLog}${replyIdLog}${packetIdLabel} bytes=${textBytes}`
      );
      this.emitTenmanSyntheticSummary({
        text,
        channel,
        flowId,
        meshPacketId: Number.isFinite(packetId) ? Number(packetId) >>> 0 : null,
        scope,
        replyTo: replyToNormalized ?? null,
        replyId: replyIdNumeric ?? null,
        meshDestination: scope === 'directed' ? meshDestinationLabel : null
      });
    } catch (err) {
      this.emitLog('TENMAN', `TenManMap 訊息轉送失敗: ${err.message}`);
      respondError('INTERNAL_ERROR', err.message || '傳送 Meshtastic 訊息失敗');
    }
  }

  isTenmanForwardEnabled() {
    if (typeof this.tenmanForwardOverride === 'boolean') {
      return this.tenmanForwardOverride;
    }
    return TENMAN_FORWARD_DEFAULT_ENABLED;
  }

  setTenmanShareEnabled(enabled) {
    const override = typeof enabled === 'boolean' ? enabled : null;
    if (this.tenmanForwardOverride === override) {
      return this.isTenmanForwardEnabled();
    }
    this.tenmanForwardOverride = override;
    if (!this.isTenmanForwardEnabled()) {
      this.tenmanForwardState.queue = [];
      this.tenmanForwardState.pendingKeys?.clear?.();
      this.tenmanForwardState.lastKey = null;
      this.tenmanForwardState.disabledLogged = false;
      this.emitLog('TENMAN', 'TenManMap 轉發已停用');
      this.resetTenmanWebsocket();
      return false;
    }
    this.tenmanForwardState.disabledLogged = false;
    this.emitLog('TENMAN', 'TenManMap 轉發已啟用');
    this.resetTenmanNodeSyncSignatures();
    this.requestTenmanNodeSnapshot('share-enabled');
    this.ensureTenmanWebsocket();
    this.flushTenmanQueue();
    return true;
  }

  enqueueTenmanPublish(message, dedupeKey) {
    if (!message || !dedupeKey) {
      return false;
    }
    if (!this.isTenmanForwardEnabled()) {
      return false;
    }
    const state = this.tenmanForwardState;
    if (!state) {
      return false;
    }

    if (!state.pendingKeys) {
      state.pendingKeys = new Set();
    }

    if (state.lastKey === dedupeKey || state.pendingKeys.has(dedupeKey)) {
      return false;
    }

    if (!Array.isArray(state.queue)) {
      state.queue = [];
    }

    if (state.queue.length >= TENMAN_FORWARD_QUEUE_LIMIT) {
      const dropped = state.queue.shift();
      if (dropped?.key) {
        state.pendingKeys.delete(dropped.key);
      }
      if (TENMAN_FORWARD_VERBOSE_LOG) {
        this.emitLog('TENMAN', '佇列已滿，將移除最舊的 publish 訊息');
      }
    }

    const entry = {
      key: dedupeKey,
      message,
      serialized: JSON.stringify(message)
    };
    state.queue.push(entry);
    state.pendingKeys.add(dedupeKey);

    if (TENMAN_FORWARD_VERBOSE_LOG) {
      const payload = message?.payload || {};
      if (message?.action === 'message.publish') {
        const previewSource = typeof payload.text === 'string' ? payload.text : '';
        const preview = previewSource.replace(/\s+/g, ' ').slice(0, 64);
        this.emitLog(
          'TENMAN',
          `佇列 message.publish channel=${payload.channel ?? ''} text=${preview || '[空白]'}`
        );
      } else if (message?.action === 'node.update') {
        this.emitLog(
          'TENMAN',
          `佇列 node.update mesh=${payload.mesh_id ?? ''} last_seen=${payload.last_seen_at ?? 'N/A'}`
        );
      } else if (message?.action === 'node.snapshot') {
        const total = Number.isFinite(payload.total) ? payload.total : payload.nodes?.length ?? 0;
        this.emitLog(
          'TENMAN',
          `佇列 node.snapshot total=${total} generated_at=${payload.generated_at ?? 'N/A'}`
        );
      } else {
        const latLog =
          typeof payload.latitude === 'number'
            ? payload.latitude.toFixed(6)
            : String(payload.latitude ?? '');
        const lonLog =
          typeof payload.longitude === 'number'
            ? payload.longitude.toFixed(6)
            : String(payload.longitude ?? '');
        this.emitLog(
          'TENMAN',
          `佇列 publish device=${payload.device_id ?? ''} lat=${latLog} lon=${lonLog}`
        );
      }
    }

    this.ensureTenmanWebsocket();
    this.flushTenmanQueue();
    return true;
  }

  ensureTenmanWebsocket() {
    if (!this.isTenmanForwardEnabled()) {
      return;
    }
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
    state.authenticated = false;
    state.authenticating = false;

    try {
      const ws = new WebSocket(TENMAN_FORWARD_WS_ENDPOINT);
      state.websocket = ws;

      ws.on('open', () => {
        state.connecting = false;
        this.emitLog('TENMAN', 'WebSocket 已連線，開始驗證');
        this.sendTenmanAuth();
      });

      ws.on('close', (code, reason) => {
        state.websocket = null;
        state.connecting = false;
        state.sending = false;
        state.authenticating = false;
        state.authenticated = false;
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
        this.scheduleTenmanReconnect('unexpected-response');
        this.resetTenmanWebsocket();
      });

      ws.on('message', (data) => {
        this.handleTenmanWebsocketMessage(data);
      });
    } catch (err) {
      state.connecting = false;
      this.emitLog('TENMAN', `WebSocket 建立失敗: ${err.message}`);
      this.scheduleTenmanReconnect('connect-error');
    }
  }

  flushTenmanQueue() {
    if (!this.isTenmanForwardEnabled()) {
      return;
    }
    const state = this.tenmanForwardState;
    if (!state || !Array.isArray(state.queue) || state.queue.length === 0) {
      return;
    }

    const ws = state.websocket;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      this.ensureTenmanWebsocket();
      return;
    }

    if (!state.authenticated) {
      this.sendTenmanAuth();
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
          this.emitLog('TENMAN', `TenManMap 傳送失敗: ${err.message}`);
          state.queue.shift();
          state.pendingKeys?.delete(entry.key);
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
      this.emitLog('TENMAN', `TenManMap 傳送失敗: ${err.message}`);
      state.queue.shift();
      state.pendingKeys?.delete(entry.key);
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
    state.authenticating = false;
    state.authenticated = false;
  }

  scheduleTenmanReconnect(reason = 'retry') {
    if (!this.isTenmanForwardEnabled()) {
      return;
    }
    const state = this.tenmanForwardState;
    if (!state) {
      return;
    }
    if (!Array.isArray(state.queue)) {
      state.queue = [];
    }
    if (state.reconnectTimer) {
      return;
    }
    const shouldLogQueueState = state.queue.length === 0;
    if (shouldLogQueueState && TENMAN_FORWARD_VERBOSE_LOG) {
      this.emitLog('TENMAN', '佇列為空，但仍將排程 TenManMap 重新連線');
    }
    this.emitLog(
      'TENMAN',
      `WebSocket 將於 ${Math.round(TENMAN_FORWARD_RECONNECT_DELAY_MS / 1000)} 秒後重試 (${reason})`
    );
    state.reconnectTimer = setTimeout(() => {
      state.reconnectTimer = null;
      if (!this.isTenmanForwardEnabled()) {
        return;
      }
      this.ensureTenmanWebsocket();
      if (state.queue.length > 0) {
        this.flushTenmanQueue();
      }
    }, TENMAN_FORWARD_RECONNECT_DELAY_MS);
    state.reconnectTimer?.unref?.();
  }

  getTenmanApiKey() {
    return this.callmeshState?.apiKey || null;
  }

  sendTenmanAuth() {
    const state = this.tenmanForwardState;
    if (
      !state ||
      !state.websocket ||
      state.websocket.readyState !== WebSocket.OPEN ||
      state.authenticated ||
      state.authenticating
    ) {
      return;
    }

    const apiKey = this.getTenmanApiKey();
    if (!apiKey) {
      if (!state.missingApiKeyWarned) {
        state.missingApiKeyWarned = true;
        this.emitLog('TENMAN', '缺少 CallMesh API Key，無法進行驗證');
      }
      return;
    }
    state.missingApiKeyWarned = false;

    const authMessage = {
      action: TENMAN_FORWARD_AUTH_ACTION,
      api_key: apiKey
    };
    if (state.suppressAck) {
      authMessage.suppress_ack = true;
    }

    const serialized = JSON.stringify(authMessage);
    state.authenticating = true;
    try {
      state.websocket.send(serialized, (err) => {
        if (err) {
          state.authenticating = false;
          this.emitLog('TENMAN', `驗證傳送失敗: ${err.message}`);
          this.scheduleTenmanReconnect('auth-send-error');
          this.resetTenmanWebsocket();
        } else {
          this.emitLog('TENMAN', '已送出驗證請求');
        }
      });
    } catch (err) {
      state.authenticating = false;
      this.emitLog('TENMAN', `驗證傳送失敗: ${err.message}`);
      this.scheduleTenmanReconnect('auth-exception');
      this.resetTenmanWebsocket();
    }
  }

  handleTenmanWebsocketMessage(data) {
    const state = this.tenmanForwardState;
    if (!state) return;

    let text;
    try {
      text = typeof data === 'string' ? data : data.toString('utf8');
    } catch (err) {
      this.emitLog('TENMAN', `無法解析伺服器訊息: ${err.message}`);
      return;
    }
    if (!text) {
      return;
    }

    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch (err) {
      this.emitLog('TENMAN', `伺服器訊息解析失敗: ${err.message} 原始=${text}`);
      return;
    }

    if (parsed?.error) {
      this.emitLog('TENMAN', `伺服器錯誤: ${text}`);
      if (parsed.action === TENMAN_FORWARD_AUTH_ACTION || parsed.type === 'auth') {
        state.authenticating = false;
        state.authenticated = false;
        this.scheduleTenmanReconnect('auth-error');
        this.resetTenmanWebsocket();
      }
      return;
    }

    if (parsed?.action === TENMAN_FORWARD_AUTH_ACTION || parsed?.type === 'auth') {
      state.authenticating = false;
      const status = String(parsed?.status ?? parsed?.result ?? parsed?.ok ?? 'ok').toLowerCase();
      if (status === 'pass' || status === 'ok' || status === 'true') {
        const gatewayIdRawCandidate = parsed?.gateway_id ?? parsed?.gatewayId ?? null;
        const gatewayIdRaw =
          gatewayIdRawCandidate != null && String(gatewayIdRawCandidate).trim()
            ? String(gatewayIdRawCandidate).trim()
            : null;
        const gatewayMeshId = normalizeMeshId(gatewayIdRaw);
        const nodeIdCandidate = parsed?.node_id ?? parsed?.nodeId ?? null;
        const nodeId =
          nodeIdCandidate != null && String(nodeIdCandidate).trim()
            ? String(nodeIdCandidate).trim()
            : null;
        state.gatewayId = gatewayIdRaw;
        state.gatewayMeshId = gatewayMeshId ?? null;
        state.nodeId = nodeId;
        state.authenticated = true;
        const infoParts = [];
        if (gatewayIdRaw) infoParts.push(`gateway=${gatewayIdRaw}`);
        if (gatewayMeshId && gatewayMeshId !== gatewayIdRaw) {
          infoParts.push(`mesh=${gatewayMeshId}`);
        }
        if (nodeId) infoParts.push(`node=${nodeId}`);
        this.emitLog('TENMAN', `驗證通過${infoParts.length ? ` (${infoParts.join(', ')})` : ''}`);
        this.resetTenmanNodeSyncSignatures();
        this.requestTenmanNodeSnapshot('auth');
        this.flushTenmanQueue();
      } else {
        state.authenticated = false;
        this.emitLog('TENMAN', `驗證失敗: ${text}`);
        this.scheduleTenmanReconnect('auth-failed');
        this.resetTenmanWebsocket();
      }
      return;
    }

    if (parsed?.type === 'ack') {
      const action = parsed?.action ?? parsed?.payload?.action;
      const deviceId = parsed?.payload?.device_id ?? parsed?.device_id ?? '';
      const gatewayId =
        parsed?.gateway_id ??
        parsed?.payload?.gateway_id ??
        state.gatewayId ??
        state.gatewayMeshId ??
        '';
      if (TENMAN_FORWARD_VERBOSE_LOG) {
        this.emitLog(
          'TENMAN',
          `收到 ack${action ? ` action=${action}` : ''}${deviceId ? ` device=${deviceId}` : ''}${gatewayId ? ` gateway=${gatewayId}` : ''}`
        );
      }
      return;
    }

    if (parsed?.action === 'send_message') {
      const maybePromise = this.handleTenmanSendMessageCommand(parsed);
      if (maybePromise && typeof maybePromise.catch === 'function') {
        maybePromise.catch((err) => {
          this.emitLog('TENMAN', `處理 send_message 指令失敗: ${err.message}`);
        });
      }
      return;
    }

    this.emitLog('TENMAN', `伺服器訊息: ${text}`);
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
    const hops = summary?.hops || {};
    const hopStartProvided = Number.isFinite(hops.start);
    const hopLimitProvided = Number.isFinite(hops.limit);
    const hopLabel = typeof hops.label === 'string' ? hops.label.trim() : '';
    const relayLikelyInvalid =
      Boolean(summary?.relayInvalid) ||
      Boolean(hops.limitOnly) ||
      (!hopStartProvided && hopLimitProvided && hopLabel && !hopLabel.includes('/') && !hopLabel.includes('?'));
    if (!relayLikelyInvalid) {
      if (summary.relay) {
        candidates.push(summary.relay);
      }
      if (summary.nextHop) {
        candidates.push(summary.nextHop);
      }
    }
    for (const node of candidates) {
      if (!node || typeof node !== 'object') continue;
      let sourceNode = node;
      const guessedFlag =
        sourceNode?.guessed === true ||
        sourceNode?.guess === true ||
        sourceNode?.relayGuessed === true;
      if (guessedFlag) {
        continue;
      }
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
    this.meshtasticClients.clear();
    if (this.dataStoreInitialized && this.dataStore) {
      try {
        this.dataStore.close();
      } catch (err) {
        this.emitLog('CALLMESH', `close callmesh data store failed: ${err.message}`);
      }
      this.dataStoreInitialized = false;
    }
    if (this.telemetryDb) {
      try {
        this.telemetryDb.close();
      } catch (err) {
        this.emitLog('CALLMESH', `close telemetry db failed: ${err.message}`);
      }
      this.telemetryDb = null;
    }
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
    const store = this.getDataStore();
    if (store) {
      try {
        store.setKv('provision', payload);
      } catch (err) {
        this.emitLog('CALLMESH', `儲存 provision 失敗: ${err.message}`);
      }
    } else {
      await this.saveJson(this.getProvisionFilePath(), payload);
    }
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
      const store = this.getDataStore();
      if (store) {
        try {
          store.setKv('mapping', payload);
        } catch (err) {
          this.emitLog('CALLMESH', `儲存 mapping 失敗: ${err.message}`);
        }
      } else {
        await this.saveJson(this.getMappingFilePath(), payload);
      }
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

    if (summary.telemetry && typeof summary.telemetry === 'object') {
      summary.telemetry = {
        ...summary.telemetry,
        timeMs: timestampMs,
        timeSeconds: Math.floor(timestampMs / 1000)
      };
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
    const relayFields = prepareTelemetryRelayFields(cloned);
    cloned.relay = relayFields.relay;
    cloned.relayLabel = relayFields.relayLabel;
    cloned.relayMeshId = relayFields.relayMeshId;
    cloned.relayMeshIdNormalized = relayFields.relayMeshIdNormalized;
    cloned.relayGuessed = relayFields.relayGuessed;
    cloned.relayGuessReason = relayFields.relayGuessReason;
    const hopFields = prepareTelemetryHopsFields({
      hops: cloned.hops,
      hopsLabel: cloned.hopsLabel,
      hopsStart: cloned.hops?.start ?? cloned.hopsStart,
      hopsLimit: cloned.hops?.limit ?? cloned.hopsLimit
    });
    cloned.hops = hopFields.hops;
    cloned.hopsLabel = hopFields.hopsLabel;
    cloned.hopsUsed = hopFields.hopsUsed;
    cloned.hopsTotal = hopFields.hopsTotal;
    return cloned;
  }

  async appendTelemetryRecord(record) {
    if (!this.telemetryDb) {
      throw new Error('telemetry database not initialized');
    }
    const payload = cloneTelemetryRecord(record) || record;
    this.telemetryDb.insertRecord(payload);
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
    const sampleTimeMs = baseTimestampMs;
    const recordTimestampIso = new Date(baseTimestampMs).toISOString();
    const sampleIso = new Date(sampleTimeMs).toISOString();
    const node = mergeNodeInfo(
      extractTelemetryNode(summary.from),
      this.nodeDatabase.get(meshId),
      { meshId }
    );
    const recordId = `${meshId}-${baseTimestampMs}-${Math.random().toString(16).slice(2, 10)}`;
    const relayFields = prepareTelemetryRelayFields({
      relay: summary.relay,
      relayMeshId: summary.relayMeshId,
      relayMeshIdNormalized: summary.relayMeshIdNormalized,
      relayLabel: summary.relay?.label,
      relayGuess: summary.relayGuess,
      relayGuessReason: summary.relayGuessReason
    });
    const hopFields = prepareTelemetryHopsFields(summary.hops);

    return {
      id: recordId,
      meshId,
      node,
      timestampMs: baseTimestampMs,
      timestamp: recordTimestampIso,
      sampleTimeMs,
      sampleTime: sampleIso || recordTimestampIso,
      type: summary.type || '',
      detail: summary.detail || '',
      channel: summary.channel ?? null,
      snr: Number.isFinite(summary.snr) ? summary.snr : null,
      rssi: Number.isFinite(summary.rssi) ? summary.rssi : null,
      flowId: summary.flowId || null,
      relay: relayFields.relay,
      relayLabel: relayFields.relayLabel,
      relayMeshId: relayFields.relayMeshId,
      relayMeshIdNormalized: relayFields.relayMeshIdNormalized,
      relayGuessed: relayFields.relayGuessed,
      relayGuessReason: relayFields.relayGuessReason,
      hops: hopFields.hops,
      hopsLabel: hopFields.hopsLabel,
      hopsUsed: hopFields.hopsUsed,
      hopsTotal: hopFields.hopsTotal,
      telemetry: {
        kind: telemetry.kind || 'unknown',
        timeSeconds: Number.isFinite(sampleTimeMs) ? Math.floor(sampleTimeMs / 1000) : null,
        timeMs: sampleTimeMs,
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
      if (this.telemetryDb) {
        this.telemetryDb.clear();
      }
    } catch (err) {
      this.emitLog('CALLMESH', `clear telemetry db failed: ${err.message}`);
    }
    try {
      await fs.rm(this.getLegacyTelemetryStorePath(), { force: true });
    } catch (err) {
      if (err && err.code !== 'ENOENT') {
        this.emitLog('CALLMESH', `remove telemetry store failed: ${err.message}`);
      }
    }
    if (!silent) {
      this.emitTelemetryUpdate({ reset: true });
    }
  }

  getTelemetrySummary() {
    const summaryMap = new Map();
    const ensureEntry = (meshIdNormalized, rawMeshId = null) => {
      const key = meshIdNormalized || normalizeMeshId(rawMeshId) || rawMeshId;
      if (!key) return null;
      let entry = summaryMap.get(key);
      if (!entry) {
        entry = {
          meshIdNormalized: meshIdNormalized || normalizeMeshId(rawMeshId) || key,
          rawMeshId: rawMeshId || meshIdNormalized || key,
          totalRecords: 0,
          latestSampleMs: null,
          earliestSampleMs: null,
          node: null,
          metricsSet: new Set(),
          hasDatabaseCount: false
        };
        summaryMap.set(entry.meshIdNormalized || entry.rawMeshId || key, entry);
      } else if (rawMeshId && !entry.rawMeshId) {
        entry.rawMeshId = rawMeshId;
      }
      return entry;
    };

    const mergeNode = (entry, candidate) => {
      if (!entry || !candidate) return;
      entry.node = mergeNodeInfo(entry.node, candidate);
    };

    if (this.telemetryDb) {
      try {
        const rows = this.telemetryDb.listMeshRecordCounts();
        for (const row of rows) {
          const normalized = normalizeMeshId(row.meshId);
          if (!normalized && !row.meshId) {
            continue;
          }
          const entry = ensureEntry(normalized, row.meshId);
          if (!entry) continue;
          entry.totalRecords = Number(row.count || 0);
          entry.hasDatabaseCount = true;
          const latest = Number(row.latestTimestampMs);
          if (Number.isFinite(latest)) {
            entry.latestSampleMs =
              entry.latestSampleMs != null ? Math.max(entry.latestSampleMs, latest) : latest;
            if (entry.earliestSampleMs == null) {
              entry.earliestSampleMs = latest;
            }
          }
        }
      } catch (err) {
        this.emitLog('CALLMESH', `count telemetry records failed: ${err.message}`);
      }
    }

    for (const [meshId, bucket] of this.telemetryStore.entries()) {
      if (!bucket) continue;
      const normalized = normalizeMeshId(meshId);
      const entry = ensureEntry(normalized, bucket.rawMeshId || meshId);
      if (!entry) continue;
      const records = Array.isArray(bucket.records) ? bucket.records : [];
      if (!entry.hasDatabaseCount) {
        entry.totalRecords = records.length;
      }
      if (records.length) {
        const latestRecord = records[records.length - 1];
        const earliestRecord = records[0];
        const latestSample =
          Number(latestRecord?.sampleTimeMs) ??
          Number(latestRecord?.timestampMs) ??
          Number(latestRecord?.telemetry?.timeMs);
        const earliestSample =
          Number(earliestRecord?.sampleTimeMs) ??
          Number(earliestRecord?.timestampMs) ??
          Number(earliestRecord?.telemetry?.timeMs);
        if (Number.isFinite(latestSample)) {
          entry.latestSampleMs =
            entry.latestSampleMs != null ? Math.max(entry.latestSampleMs, latestSample) : latestSample;
        }
        if (Number.isFinite(earliestSample)) {
          entry.earliestSampleMs =
            entry.earliestSampleMs != null
              ? Math.min(entry.earliestSampleMs, earliestSample)
              : earliestSample;
        }
        for (const record of records) {
          const metrics = record?.telemetry?.metrics;
          if (metrics && typeof metrics === 'object') {
            for (const key of Object.keys(metrics)) {
              entry.metricsSet.add(key);
            }
          }
        }
      }
      mergeNode(entry, bucket.node);
    }

    const meshIdsNeedingLatest = new Set();
    for (const entry of summaryMap.values()) {
      if (entry.metricsSet.size === 0 || entry.latestSampleMs == null || entry.earliestSampleMs == null) {
        const candidate = entry.rawMeshId || entry.meshIdNormalized;
        if (candidate) {
          meshIdsNeedingLatest.add(candidate);
        }
      }
    }

    if (this.telemetryDb && meshIdsNeedingLatest.size) {
      try {
        const latestRows = this.telemetryDb.fetchRecentSnapshot({
          limitPerNode: 1,
          meshIds: Array.from(meshIdsNeedingLatest)
        });
        for (const row of latestRows) {
          const normalized = normalizeMeshId(row.meshId);
          const entry = ensureEntry(normalized, row.meshId);
          if (!entry) continue;
          const sample =
            Number(row.sampleTimeMs) ?? Number(row.timestampMs) ?? Number(row.telemetry?.timeMs);
          if (Number.isFinite(sample)) {
            entry.latestSampleMs =
              entry.latestSampleMs != null ? Math.max(entry.latestSampleMs, sample) : sample;
            entry.earliestSampleMs =
              entry.earliestSampleMs != null ? Math.min(entry.earliestSampleMs, sample) : sample;
          }
          if (row?.telemetry?.metrics && typeof row.telemetry.metrics === 'object') {
            for (const key of Object.keys(row.telemetry.metrics)) {
              entry.metricsSet.add(key);
            }
          }
          mergeNode(entry, row.node);
        }
      } catch (err) {
        this.emitLog('CALLMESH', `load telemetry latest metrics failed: ${err.message}`);
      }
    }

    for (const entry of summaryMap.values()) {
      const nodeInfo = this.nodeDatabase.get(entry.meshIdNormalized);
      if (nodeInfo) {
        mergeNode(entry, nodeInfo);
      }
    }

    const nodes = Array.from(summaryMap.values()).map((entry) => ({
      meshId: entry.rawMeshId || entry.meshIdNormalized,
      meshIdNormalized: entry.meshIdNormalized || normalizeMeshId(entry.rawMeshId),
      rawMeshId: entry.rawMeshId || entry.meshIdNormalized,
      node: entry.node ? { ...entry.node } : null,
      totalRecords: Number.isFinite(entry.totalRecords) ? Number(entry.totalRecords) : 0,
      latestSampleMs: Number.isFinite(entry.latestSampleMs) ? Number(entry.latestSampleMs) : null,
      earliestSampleMs: Number.isFinite(entry.earliestSampleMs) ? Number(entry.earliestSampleMs) : null,
      availableMetrics: Array.from(entry.metricsSet)
    }));

    nodes.sort((a, b) => {
      const timeA = Number.isFinite(a.latestSampleMs) ? a.latestSampleMs : -Infinity;
      const timeB = Number.isFinite(b.latestSampleMs) ? b.latestSampleMs : -Infinity;
      if (timeB !== timeA) {
        return timeB - timeA;
      }
      const countA = Number.isFinite(a.totalRecords) ? a.totalRecords : 0;
      const countB = Number.isFinite(b.totalRecords) ? b.totalRecords : 0;
      if (countB !== countA) {
        return countB - countA;
      }
      const labelA = (a.node?.label || a.meshId || '').toLowerCase();
      const labelB = (b.node?.label || b.meshId || '').toLowerCase();
      if (labelA < labelB) return -1;
      if (labelA > labelB) return 1;
      return 0;
    });

    const updatedAt =
      Number.isFinite(this.telemetryUpdatedAt) && this.telemetryUpdatedAt > 0
        ? this.telemetryUpdatedAt
        : Date.now();

    return {
      updatedAt,
      nodes,
      stats: this.getTelemetryStats()
    };
  }

  getTelemetryRecordsForMesh(meshId, { limit = null, startMs = null, endMs = null } = {}) {
    const normalized = normalizeMeshId(meshId);
    if (!normalized) {
      return {
        meshId,
        rawMeshId: meshId,
        node: null,
        records: [],
        totalRecords: 0,
        availableMetrics: [],
        latestSampleMs: null,
        earliestSampleMs: null
      };
    }

    const effectiveLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : null;

    const recordsMap = new Map();
    const considerRecord = (record) => {
      if (!record) return;
      const cloned = cloneTelemetryRecord(record);
      if (!cloned) return;
      if (cloned.id) {
        recordsMap.set(cloned.id, cloned);
      } else {
        const key = `${cloned.meshId || normalized}-${cloned.timestampMs || cloned.sampleTimeMs || Math.random()}`;
        recordsMap.set(key, cloned);
      }
    };

    if (this.telemetryDb) {
      const candidates = [
        normalized,
        normalizeMeshId(meshId),
        meshId,
        this.telemetryStore.get(meshId)?.rawMeshId || null
      ]
        .filter(Boolean)
        .map((value) => normalizeMeshId(value) || value)
        .filter(Boolean);
      const tried = new Set();
      for (const candidate of candidates) {
        if (tried.has(candidate)) continue;
        tried.add(candidate);
        try {
          const rows = this.telemetryDb.fetchRecordsForMesh({
            meshId: candidate,
            limit: effectiveLimit,
            startMs,
            endMs
          });
          for (const row of rows) {
            considerRecord(row);
          }
          if (effectiveLimit != null && recordsMap.size >= effectiveLimit) {
            break;
          }
        } catch (err) {
          this.emitLog('CALLMESH', `fetch telemetry records for ${candidate} failed: ${err.message}`);
        }
      }
    }

    const bucketCandidates = [];
    const bucket = this.telemetryStore.get(normalized) || this.telemetryStore.get(meshId);
    if (bucket) {
      bucketCandidates.push(bucket);
    } else {
      for (const [key, candidateBucket] of this.telemetryStore.entries()) {
        if (!candidateBucket) continue;
        const keyNormalized = normalizeMeshId(key);
        if (keyNormalized === normalized) {
          bucketCandidates.push(candidateBucket);
        } else {
          const rawNormalized = normalizeMeshId(candidateBucket.rawMeshId);
          if (rawNormalized === normalized) {
            bucketCandidates.push(candidateBucket);
          }
        }
      }
    }
    for (const candidateBucket of bucketCandidates) {
      const records = Array.isArray(candidateBucket.records) ? candidateBucket.records : [];
      for (const record of records) {
        considerRecord(record);
      }
    }

    const combined = Array.from(recordsMap.values());
    combined.sort((a, b) => {
      const aSample = Number(a.sampleTimeMs ?? a.timestampMs ?? 0);
      const bSample = Number(b.sampleTimeMs ?? b.timestampMs ?? 0);
      if (!Number.isFinite(aSample) && !Number.isFinite(bSample)) return 0;
      if (!Number.isFinite(aSample)) return -1;
      if (!Number.isFinite(bSample)) return 1;
      return aSample - bSample;
    });

    const limited =
      effectiveLimit != null && combined.length > effectiveLimit
        ? combined.slice(combined.length - effectiveLimit)
        : combined;

    const nodeCandidates = [];
    const nodeFromDb = this.nodeDatabase.get(normalized);
    if (nodeFromDb) {
      nodeCandidates.push(nodeFromDb);
    }
    if (bucketCandidates.length) {
      for (const candidate of bucketCandidates) {
        if (candidate?.node) {
          nodeCandidates.push(candidate.node);
        }
      }
    }
    if (limited.length) {
      for (let i = limited.length - 1; i >= 0; i -= 1) {
        const nodeCandidate = limited[i]?.node;
        if (nodeCandidate) {
          nodeCandidates.push(nodeCandidate);
          break;
        }
      }
    }
    const node = nodeCandidates.length ? mergeNodeInfo({}, ...nodeCandidates) : null;

    let totalRecords = limited.length;
    let latestSample = limited.length
      ? limited[limited.length - 1].sampleTimeMs ?? limited[limited.length - 1].timestampMs ?? null
      : null;
    let rawMeshId = bucketCandidates.find((bucket) => bucket?.rawMeshId)?.rawMeshId || meshId;
    let availableMetrics = new Set();

    const countCandidates = [normalized, meshId, rawMeshId].filter(Boolean);
    if (this.telemetryDb) {
      try {
        const rows = this.telemetryDb.listMeshRecordCounts({ meshIds: countCandidates });
        if (rows.length) {
          const best = rows[0];
          totalRecords = Number.isFinite(best?.count) ? Number(best.count) : totalRecords;
          const dbLatest = Number(best?.latestTimestampMs);
          if (Number.isFinite(dbLatest)) {
            latestSample =
              latestSample != null ? Math.max(latestSample, dbLatest) : dbLatest;
          }
          rawMeshId = best?.meshId || rawMeshId;
        }
      } catch (err) {
        this.emitLog('CALLMESH', `inspect telemetry count failed: ${err.message}`);
      }
    }

    for (const record of limited) {
      const metrics = record?.telemetry?.metrics;
      if (metrics && typeof metrics === 'object') {
        for (const key of Object.keys(metrics)) {
          availableMetrics.add(key);
        }
      }
    }
    const earliestSample = limited.length
      ? limited[0].sampleTimeMs ?? limited[0].timestampMs ?? null
      : null;

    return {
      meshId: rawMeshId || meshId || normalized,
      rawMeshId: rawMeshId || meshId || normalized,
      meshIdNormalized: normalized,
      node: node ? { ...node } : null,
      records: limited.map((record) => cloneTelemetryRecord(record)),
      totalRecords,
      filteredCount: limited.length,
      latestSampleMs: Number.isFinite(latestSample) ? Number(latestSample) : null,
      earliestSampleMs: Number.isFinite(earliestSample) ? Number(earliestSample) : null,
      availableMetrics: Array.from(availableMetrics)
    };
  }

  getTelemetrySnapshot({ limitPerNode } = {}) {
    const summary = this.getTelemetrySummary();
    const limit =
      Number.isFinite(limitPerNode) && limitPerNode > 0
        ? Math.floor(limitPerNode)
        : this.telemetryMaxEntriesPerNode;
    const nodes = Array.isArray(summary?.nodes) ? summary.nodes : [];
    const detailedNodes = nodes.map((entry) => {
      const detail = this.getTelemetryRecordsForMesh(entry.meshId || entry.rawMeshId, {
        limit
      });
      return {
        meshId: detail.meshId,
        rawMeshId: detail.rawMeshId,
        node: detail.node,
        records: detail.records,
        totalRecords: detail.totalRecords,
        filteredCount: detail.filteredCount,
        latestSampleMs: detail.latestSampleMs,
        earliestSampleMs: detail.earliestSampleMs,
        availableMetrics: detail.availableMetrics
      };
    });
    return {
      updatedAt: summary?.updatedAt ?? this.telemetryUpdatedAt,
      nodes: detailedNodes,
      stats:
        summary?.stats && typeof summary.stats === 'object'
          ? summary.stats
          : this.getTelemetryStats({ cachedNodes: detailedNodes })
    };
  }

  getTelemetryStats({ cachedNodes = null } = {}) {
    let totalRecords = null;
    let totalNodes = null;

    if (this.telemetryDb) {
      try {
        totalRecords = this.telemetryDb.getRecordCount();
      } catch (err) {
        this.emitLog('CALLMESH', `count telemetry records failed: ${err.message}`);
      }
      try {
        totalNodes = this.telemetryDb.getDistinctMeshCount();
      } catch (err) {
        this.emitLog('CALLMESH', `count telemetry mesh failed: ${err.message}`);
      }
    }

    if (!Number.isFinite(totalRecords) || totalRecords < 0) {
      const source = cachedNodes;
      totalRecords = 0;
      if (Array.isArray(source)) {
        totalRecords = source.reduce(
          (acc, item) => acc + (Array.isArray(item.records) ? item.records.length : 0),
          0
        );
      } else {
        for (const bucket of this.telemetryStore.values()) {
          totalRecords += Array.isArray(bucket.records) ? bucket.records.length : 0;
        }
      }
    }

    if (!Number.isFinite(totalNodes) || totalNodes < 0) {
      if (Array.isArray(cachedNodes)) {
        totalNodes = cachedNodes.length;
      } else {
        totalNodes = this.telemetryStore.size;
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
      totalNodes,
      diskBytes
    };
  }

  findTelemetryBucket(meshId) {
    if (!meshId) {
      return { bucket: null, key: null };
    }
    const normalized = normalizeMeshId(meshId);
    if (normalized) {
      const direct = this.telemetryStore.get(normalized);
      if (direct) {
        return { bucket: direct, key: normalized };
      }
    }
    const directByRaw = this.telemetryStore.get(meshId);
    if (directByRaw) {
      return { bucket: directByRaw, key: meshId };
    }
    if (normalized) {
      for (const [key, bucket] of this.telemetryStore.entries()) {
        if (normalizeMeshId(key) === normalized) {
          return { bucket, key };
        }
      }
    }
    return { bucket: null, key: null };
  }

  getTelemetryNodesSummary() {
    return this.getTelemetrySummary();
  }

  async getTelemetryRecordsForRange(options = {}) {
    const meshId = options?.meshId || options?.mesh_id || null;
    if (!meshId) {
      throw new Error('meshId is required');
    }

    const startRaw = options?.startMs ?? options?.start ?? null;
    const endRaw = options?.endMs ?? options?.end ?? null;
    const limitRaw = options?.limit ?? null;

    const startMs = Number.isFinite(Number(startRaw)) ? Number(startRaw) : null;
    const endMs = Number.isFinite(Number(endRaw)) ? Number(endRaw) : null;
    if (startMs != null && endMs != null && startMs > endMs) {
      throw new Error('startMs must be less than or equal to endMs');
    }
    const limit =
      Number.isFinite(Number(limitRaw)) && Number(limitRaw) > 0 ? Math.floor(Number(limitRaw)) : null;

    const { bucket, key } = this.findTelemetryBucket(meshId);
    const bucketRecords = Array.isArray(bucket?.records) ? bucket.records : [];
    const normalizedMeshId = normalizeMeshId(meshId);

    const diskResult = await this._readTelemetryRecordsFromDisk({
      meshId,
      normalizedMeshId,
      startMs,
      endMs,
      limit: limit == null ? null : limit + bucketRecords.length
    });

    const aggregated = [];
    const seenIds = new Set();
    const metricsSet = new Set();

    const considerRecord = (candidate) => {
      if (!candidate) {
        return;
      }
      const sample = Number(candidate?.sampleTimeMs ?? candidate?.timestampMs);
      if (startMs != null && Number.isFinite(sample) && sample < startMs) {
        return;
      }
      if (endMs != null && Number.isFinite(sample) && sample > endMs) {
        return;
      }
      const cloned = cloneTelemetryRecord(candidate);
      const recordId = typeof cloned?.id === 'string' && cloned.id.trim() ? cloned.id.trim() : null;
      if (recordId && seenIds.has(recordId)) {
        return;
      }
      if (recordId) {
        seenIds.add(recordId);
      }
      if (cloned?.telemetry?.metrics && typeof cloned.telemetry.metrics === 'object') {
        for (const metricKey of Object.keys(cloned.telemetry.metrics)) {
          metricsSet.add(metricKey);
        }
      }
      aggregated.push(cloned);
    };

    for (const record of diskResult.records) {
      considerRecord(record);
    }
    for (const record of bucketRecords) {
      considerRecord(record);
    }

    aggregated.sort((a, b) => {
      const aSample = Number(a?.sampleTimeMs ?? a?.timestampMs ?? 0);
      const bSample = Number(b?.sampleTimeMs ?? b?.timestampMs ?? 0);
      if (!Number.isFinite(aSample) && !Number.isFinite(bSample)) {
        return 0;
      }
      if (!Number.isFinite(aSample)) {
        return -1;
      }
      if (!Number.isFinite(bSample)) {
        return 1;
      }
      return aSample - bSample;
    });

    const limitedRecords =
      limit != null && aggregated.length > limit
        ? aggregated.slice(aggregated.length - limit)
        : aggregated;

    let filteredLatestMs = null;
    let filteredEarliestMs = null;
    for (const record of limitedRecords) {
      const sample = Number(record?.sampleTimeMs ?? record?.timestampMs);
      if (Number.isFinite(sample)) {
        if (filteredLatestMs == null || sample > filteredLatestMs) {
          filteredLatestMs = sample;
        }
        if (filteredEarliestMs == null || sample < filteredEarliestMs) {
          filteredEarliestMs = sample;
        }
      }
    }

    const bucketNode = bucket?.node ? { ...bucket.node } : null;
    let resolvedNode = bucketNode;
    if (!resolvedNode) {
      for (let i = limitedRecords.length - 1; i >= 0; i -= 1) {
        const candidateNode = limitedRecords[i]?.node;
        if (candidateNode) {
          resolvedNode = { ...candidateNode };
          break;
        }
      }
    }

    const totalFromDisk = diskResult.totalMatching;
    let totalRecords = totalFromDisk;
    if (bucketRecords.length) {
      for (const record of bucketRecords) {
        const recordId = typeof record?.id === 'string' && record.id.trim() ? record.id.trim() : null;
        if (!recordId || !diskResult.recordIdSet.has(recordId)) {
          const sample = Number(record?.sampleTimeMs ?? record?.timestampMs);
          if (
            Number.isFinite(sample) &&
            (startMs == null || sample >= startMs) &&
            (endMs == null || sample <= endMs)
          ) {
            totalRecords += 1;
          }
        }
      }
    }

    const effectiveMeshKey = key || meshId;
    const rawMeshId =
      bucket?.rawMeshId ||
      bucket?.meshIdOriginal ||
      bucket?.meshId ||
      limitedRecords.find((record) => typeof record?.meshId === 'string')?.meshId ||
      meshId;

    return {
      meshId: effectiveMeshKey,
      rawMeshId,
      meshIdNormalized: normalizeMeshId(effectiveMeshKey),
      node: resolvedNode || null,
      records: limitedRecords,
      totalRecords,
      filteredCount: limitedRecords.length,
      latestSampleMs: filteredLatestMs,
      earliestSampleMs: filteredEarliestMs,
      availableMetrics: Array.from(metricsSet),
      range: {
        startMs: startMs != null ? startMs : null,
        endMs: endMs != null ? endMs : null
      },
      requestedLimit: limit,
      updatedAt: this.telemetryUpdatedAt,
      stats: this.getTelemetryStats()
    };
  }

  async _readTelemetryRecordsFromDisk({ meshId, normalizedMeshId, startMs, endMs, limit = null }) {
    const results = [];
    const recordIdSet = new Set();
    let totalMatching = 0;
    for await (const parsed of this._iterateTelemetryRecordsFromDisk({
      meshId,
      normalizedMeshId,
      startMs,
      endMs
    })) {
      totalMatching += 1;
      const recordId =
        typeof parsed?.id === 'string' && parsed.id.trim() ? parsed.id.trim() : null;
      if (recordId) {
        recordIdSet.add(recordId);
      }
      results.push(parsed);
      if (limit && results.length > limit) {
        const removed = results.shift();
        if (removed?.id) {
          recordIdSet.delete(removed.id);
        }
      }
    }

    return {
      records: results,
      totalMatching,
      recordIdSet
    };
  }

  async *_iterateTelemetryRecordsFromDisk({
    meshId,
    normalizedMeshId,
    startMs,
    endMs
  }) {
    const filePath = this.getTelemetryStorePath();
    let stream;
    try {
      await fs.access(filePath, fsSync.constants.F_OK);
    } catch (err) {
      if (err && err.code === 'ENOENT') {
        return;
      }
      throw err;
    }

    stream = fsSync.createReadStream(filePath, { encoding: 'utf8' });
    const rl = readline.createInterface({
      input: stream,
      crlfDelay: Infinity
    });

    const matchesMeshId = (candidate) => {
      if (!candidate) {
        return false;
      }
      if (candidate === meshId) {
        return true;
      }
      const normalizedCandidate = normalizeMeshId(candidate);
      if (normalizedCandidate && normalizedMeshId) {
        return normalizedCandidate === normalizedMeshId;
      }
      return false;
    };

    try {
      for await (const line of rl) {
        if (!line || !line.trim()) {
          continue;
        }
        let parsed = null;
        try {
          parsed = JSON.parse(line);
        } catch {
          continue;
        }
        const candidateMeshId =
          parsed?.meshId ||
          parsed?.rawMeshId ||
          parsed?.mesh_id ||
          parsed?.node?.meshId ||
          parsed?.node?.mesh_id;
        if (!matchesMeshId(candidateMeshId)) {
          const normalizedCandidate = parsed?.meshIdNormalized || parsed?.mesh_id_normalized;
          if (!matchesMeshId(normalizedCandidate)) {
            continue;
          }
        }
        const sample = Number(parsed?.sampleTimeMs ?? parsed?.timestampMs);
        if (startMs != null && Number.isFinite(sample) && sample < startMs) {
          continue;
        }
        if (endMs != null && Number.isFinite(sample) && sample > endMs) {
          continue;
        }
        yield parsed;
      }
    } finally {
      rl.close();
      stream.destroy();
    }
  }

  async *streamTelemetryRecords(options = {}) {
    const meshId = options?.meshId || options?.mesh_id || null;
    if (!meshId) {
      throw new Error('meshId is required');
    }

    const startRaw = options?.startMs ?? options?.start ?? null;
    const endRaw = options?.endMs ?? options?.end ?? null;

    const startMs = Number.isFinite(Number(startRaw)) ? Number(startRaw) : null;
    const endMs = Number.isFinite(Number(endRaw)) ? Number(endRaw) : null;
    if (startMs != null && endMs != null && startMs > endMs) {
      throw new Error('startMs must be less than or equal to endMs');
    }

    const normalizedMeshId = normalizeMeshId(meshId);
    const seenIds = new Set();

    for await (const parsed of this._iterateTelemetryRecordsFromDisk({
      meshId,
      normalizedMeshId,
      startMs,
      endMs
    })) {
      const cloned = cloneTelemetryRecord(parsed);
      const recordId =
        typeof cloned?.id === 'string' && cloned.id.trim() ? cloned.id.trim() : null;
      if (recordId) {
        seenIds.add(recordId);
      }
      yield cloned;
    }

    const { bucket } = this.findTelemetryBucket(meshId);
    const bucketRecords = Array.isArray(bucket?.records) ? bucket.records : [];
    if (!bucketRecords.length) {
      return;
    }
    for (const record of bucketRecords) {
      if (!record) continue;
      const recordId =
        typeof record?.id === 'string' && record.id.trim() ? record.id.trim() : null;
      if (recordId && seenIds.has(recordId)) {
        continue;
      }
      const sample = Number(record?.sampleTimeMs ?? record?.timestampMs);
      if (startMs != null && Number.isFinite(sample) && sample < startMs) {
        continue;
      }
      if (endMs != null && Number.isFinite(sample) && sample > endMs) {
        continue;
      }
      yield cloneTelemetryRecord(record);
    }
  }

  recordTelemetryPacket(summary) {
    if (!summary) return;
    const timestampMs = Number.isFinite(summary.timestampMs) ? Number(summary.timestampMs) : Date.now();
    const fromMeshId = normalizeMeshId(summary.from?.meshId || summary.from?.meshIdNormalized);
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
    const timestampMs = now;
    summary.timestampMs = timestampMs;
    const timestampDate = new Date(timestampMs);
    summary.timestamp = timestampDate.toISOString();
    summary.timestampLabel = formatTimestampLabel(timestampDate);
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

function meshIdToUint32(meshId) {
  const normalized = normalizeMeshId(meshId);
  if (!normalized) {
    return null;
  }
  const hex = normalized.slice(1);
  if (!hex) {
    return null;
  }
  const padded = hex.length > 8 ? hex.slice(-8) : hex.padStart(8, '0');
  const parsed = Number.parseInt(padded, 16);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return parsed >>> 0;
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

function prepareTelemetryRelayFields(source) {
  const relaySource = source?.relay && typeof source.relay === 'object' ? source.relay : null;
  const baseRelay = extractTelemetryNode(relaySource);
  const meshCandidates = [
    source?.relayMeshId,
    source?.relayMeshIdNormalized,
    relaySource?.meshId,
    relaySource?.meshIdNormalized,
    baseRelay?.meshId,
    baseRelay?.meshIdNormalized
  ].filter(Boolean);
  let meshIdNormalized = null;
  let meshIdRaw = null;
  for (const candidate of meshCandidates) {
    const normalized = normalizeMeshId(candidate);
    if (!normalized) continue;
    if (!meshIdNormalized) {
      meshIdNormalized = normalized;
    }
    if (!meshIdRaw) {
      const candidateStr = String(candidate);
      meshIdRaw =
        candidateStr.startsWith('!') || candidateStr.toLowerCase().startsWith('0x')
          ? normalized
          : candidateStr;
    }
  }
  if (!meshIdRaw && meshIdNormalized) {
    meshIdRaw = meshIdNormalized;
  }
  const labelCandidates = [
    typeof source?.relayLabel === 'string' ? source.relayLabel.trim() : '',
    typeof relaySource?.label === 'string' ? relaySource.label.trim() : '',
    baseRelay?.label ?? ''
  ];
  const relayLabel = labelCandidates.find((value) => value) || '';
  const guessedFlag =
    source?.relayGuessed ??
    source?.relayGuess ??
    relaySource?.guessed ??
    false;
  const relayReason =
    source?.relayGuessReason ??
    relaySource?.reason ??
    relaySource?.guessReason ??
    null;

  let relay = null;
  if (baseRelay || meshIdRaw || meshIdNormalized || relayLabel) {
    relay = mergeNodeInfo(baseRelay, {
      meshId: meshIdRaw || meshIdNormalized || null,
      meshIdNormalized: meshIdNormalized || null,
      label: relayLabel || null
    });
    if (relay) {
      if (!relay.label) {
        relay.label = buildNodeLabel(relay);
      }
      relay.guessed = Boolean(guessedFlag);
      if (relayReason) {
        relay.guessReason = relayReason;
      } else {
        delete relay.guessReason;
      }
    } else if (relayLabel || meshIdRaw || meshIdNormalized) {
      relay = {
        label: relayLabel || null,
        meshId: meshIdRaw || meshIdNormalized || null,
        meshIdNormalized: meshIdNormalized || null,
        guessed: Boolean(guessedFlag)
      };
      if (relayReason) {
        relay.guessReason = relayReason;
      }
    }
  }

  return {
    relay: relay || null,
    relayLabel: (relay && relay.label) || relayLabel || null,
    relayMeshId: (relay && relay.meshId) || meshIdRaw || meshIdNormalized || null,
    relayMeshIdNormalized: (relay && relay.meshIdNormalized) || meshIdNormalized || null,
    relayGuessed: Boolean(guessedFlag) && Boolean(relayLabel || meshIdRaw || meshIdNormalized),
    relayGuessReason: relayReason || (relay && relay.guessReason) || null
  };
}

function deriveHopUsage({ start, limit, label }) {
  let used = null;
  let total = Number.isFinite(start) ? start : null;

  if (Number.isFinite(start) && Number.isFinite(limit)) {
    used = Math.max(start - limit, 0);
  } else if (label) {
    const match = label.match(/^(\d+)\s*\/\s*(\d+)/);
    if (match) {
      used = Number(match[1]);
      if (!Number.isFinite(total)) {
        total = Number(match[2]);
      }
    } else if (/^\d+$/.test(label)) {
      used = 0;
      if (!Number.isFinite(total)) {
        total = Number(label);
      }
    }
    if (!Number.isFinite(total)) {
      const totalMatch = label.match(/\/\s*(\d+)/);
      if (totalMatch) {
        total = Number(totalMatch[1]);
      }
    }
  }

  return {
    used: Number.isFinite(used) ? used : null,
    total: Number.isFinite(total) ? total : null
  };
}

function prepareTelemetryHopsFields(source) {
  const hopSource = source && typeof source === 'object'
    ? source.hops && typeof source.hops === 'object'
      ? source.hops
      : source
    : null;
  const startRaw = Number(hopSource?.start);
  const startCandidate = Number.isFinite(startRaw)
    ? startRaw
    : Number.isFinite(Number(source?.hopsStart))
      ? Number(source.hopsStart)
      : null;
  const limitRaw = Number(hopSource?.limit);
  const limitCandidate = Number.isFinite(limitRaw)
    ? limitRaw
    : Number.isFinite(Number(source?.hopsLimit))
      ? Number(source.hopsLimit)
      : null;
  const labelCandidateRaw =
    (typeof hopSource?.label === 'string' && hopSource.label.trim()) ||
    (typeof source?.hopsLabel === 'string' && source.hopsLabel.trim()) ||
    '';
  const labelCandidate = labelCandidateRaw || null;
  const { used, total } = deriveHopUsage({
    start: startCandidate,
    limit: limitCandidate,
    label: labelCandidate || ''
  });

  if (
    startCandidate == null &&
    limitCandidate == null &&
    !labelCandidate &&
    used == null &&
    total == null
  ) {
    return {
      hops: null,
      hopsLabel: null,
      hopsUsed: null,
      hopsTotal: null
    };
  }

  return {
    hops: {
      start: Number.isFinite(startCandidate) ? startCandidate : null,
      limit: Number.isFinite(limitCandidate) ? limitCandidate : null,
      label: labelCandidate
    },
    hopsLabel: labelCandidate,
    hopsUsed: used,
    hopsTotal: total
  };
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
    relay: record.relay ? { ...record.relay } : null,
    hops: record.hops ? { ...record.hops } : null,
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

function toIsoString(value) {
  if (value == null) {
    return null;
  }
  let date;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return null;
    }
    date = value;
  } else if (typeof value === 'number' && Number.isFinite(value)) {
    date = new Date(value);
  } else if (typeof value === 'string' && value.trim()) {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    date = parsed;
  } else {
    return null;
  }
  return date.toISOString();
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
