'use strict';

const path = require('path');
const fs = require('fs/promises');
const EventEmitter = require('events');
const { CallMeshClient, buildAgentString } = require('./client');
const { APRSClient } = require('../aprs/client');

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
      fetchImpl = globalThis.fetch
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

    this.telemetryBuckets = new Map();

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

  async init({ allowRestore = true } = {}) {
    await this.ensureStorageDir();
    await this.restoreArtifacts({ allowRestore });
    await this.restoreTelemetryState();
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

  setSelfMeshId(meshId) {
    this.selfMeshId = normalizeMeshId(meshId);
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
    this.ensureSummaryFlowMetadata(summary);
    this.recordTelemetryPacket(summary);
    if (summary.type === 'Position') {
      this.handleAprsSummary(summary);
    }
  }

  handleMeshtasticMyInfo(info) {
    if (!info) return;
    const meshCandidate = normalizeMeshId(info?.node?.meshId || info?.meshId);
    if (meshCandidate) {
      this.selfMeshId = meshCandidate;
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
    comment = comment ? `PHG${phgDigits} ${comment}` : `PHG${phgDigits}`;
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
  return cleaned.slice(0, 60);
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

module.exports = {
  CallMeshAprsBridge,
  createInitialCallmeshState,
  normalizeMeshId,
  deriveAprsCallsignFromMapping
};
