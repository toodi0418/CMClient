'use strict';

const http = require('http');
const path = require('path');
const fs = require('fs');
const { URL } = require('url');
const fsPromises = fs.promises;

const DEFAULT_PORT = Number(process.env.TMAG_WEB_PORT) || 7080;
const DEFAULT_HOST = process.env.TMAG_WEB_HOST || '0.0.0.0';
const PACKET_WINDOW_MS = 10 * 60 * 1000;
const PACKET_BUCKET_MS = 60 * 1000;
const MAX_SUMMARY_ROWS = 200;
const MAX_LOG_ENTRIES = 200;
const APRS_HISTORY_MAX = 5000;
const APRS_RECORD_HISTORY_MAX = 5000;
const DEFAULT_TELEMETRY_MAX_PER_NODE = 500;
const DEFAULT_TELEMETRY_MAX_TOTAL_RECORDS = 20000;
const MESSAGE_MAX_PER_CHANNEL = 200;
const MESSAGE_PERSIST_INTERVAL_MS = 2000;
const TELEMETRY_METRIC_DEFINITIONS = {
  batteryLevel: { label: '電量', unit: '%', decimals: 0, clamp: [0, 150] },
  voltage: { label: '電壓', unit: 'V', decimals: 2 },
  channelUtilization: { label: '通道使用率', unit: '%', decimals: 1, clamp: [0, 100] },
  airUtilTx: { label: '空中時間 (TX)', unit: '%', decimals: 1, clamp: [0, 100] },
  temperature: { label: '溫度', unit: '°C', decimals: 1 },
  relativeHumidity: { label: '濕度', unit: '%', decimals: 0, clamp: [0, 100] },
  barometricPressure: { label: '氣壓', unit: 'hPa', decimals: 1 },
  uptimeSeconds: {
    label: '運行時間',
    formatter: (value) => formatSecondsAsDuration(value)
  }
};
const CHANNEL_CONFIG = [
  { id: 0, code: 'CH0', name: 'Primary Channel', note: '日常主要通訊頻道' },
  { id: 1, code: 'CH1', name: 'Mesh TW', note: '跨節點廣播與共通交換' },
  { id: 2, code: 'CH2', name: 'Signal Test', note: '訊號測試、天線調校專用' },
  { id: 3, code: 'CH3', name: 'Emergency', note: '緊急狀況 / 救援聯絡' }
];

function cloneJson(value) {
  if (value === null || value === undefined) {
    return value;
  }
  if (typeof structuredClone === 'function') {
    try {
      return structuredClone(value);
    } catch {
      // fall through to JSON fallback
    }
  }
  return JSON.parse(JSON.stringify(value));
}

function sanitizeTelemetryNode(node) {
  if (!node || typeof node !== 'object') {
    return null;
  }
  return {
    label: node.label ?? null,
    meshId: node.meshId ?? null,
    meshIdNormalized: node.meshIdNormalized ?? null,
    shortName: node.shortName ?? null,
    longName: node.longName ?? null,
    hwModel: node.hwModel ?? null,
    hwModelLabel: node.hwModelLabel ?? null,
    role: node.role ?? null,
    roleLabel: node.roleLabel ?? null,
    latitude: Number.isFinite(node.latitude) ? Number(node.latitude) : null,
    longitude: Number.isFinite(node.longitude) ? Number(node.longitude) : null,
    altitude: Number.isFinite(node.altitude) ? Number(node.altitude) : null,
    lastSeenAt: Number.isFinite(node.lastSeenAt) ? Number(node.lastSeenAt) : node.lastSeenAt ?? null
  };
}

function formatEnumLabel(value) {
  if (value === undefined || value === null) {
    return null;
  }
  return String(value).replace(/_/g, ' ').trim();
}

function sanitizeTelemetryRecord(record) {
  if (!record || typeof record !== 'object') {
    return null;
  }
  const metrics = record.telemetry?.metrics;
  if (!metrics || typeof metrics !== 'object' || !Object.keys(metrics).length) {
    return null;
  }
  return cloneJson(record);
}

const FALLBACK_VERSION = (() => {
  try {
    // eslint-disable-next-line global-require
    const pkg = require('../../package.json');
    if (pkg && typeof pkg.version === 'string' && pkg.version.trim()) {
      return pkg.version.trim();
    }
  } catch {
    // ignore - fallback below
  }
  return '0.0.0';
})();

class WebDashboardServer {
  constructor(options = {}) {
    this.port = options.port ?? DEFAULT_PORT;
    this.host = options.host ?? DEFAULT_HOST;
    this.inactivityPingMs = options.inactivityPingMs ?? 15_000;
    this.appVersion = typeof options.appVersion === 'string' && options.appVersion.trim()
      ? options.appVersion.trim()
      : FALLBACK_VERSION;
    this.relayStatsPath =
      typeof options.relayStatsPath === 'string' && options.relayStatsPath.trim()
        ? options.relayStatsPath.trim()
        : null;

    this.server = null;
    this.clients = new Set();
    this._publicDir = path.resolve(__dirname, 'public');
    this._pingTimer = null;

    this.summaryRows = [];
    this.packetBuckets = new Map();
    this.metrics = {
      packetLast10Min: 0,
      aprsUploaded: 0,
      mappingCount: 0
    };
    this.lastStatus = null;
    this.lastCallmesh = null;
    this.lastAprsInfo = null;
    this.logEntries = [];
    this.selfMeshId = null;
    this.aprsFlowIds = new Set();
    this.aprsFlowQueue = [];
    this.aprsFlowRecords = new Map();
    this.aprsRecordQueue = [];
    this.lastAppInfo = this.appVersion ? { version: this.appVersion } : null;
    this.telemetryMaxPerNode =
      Number.isFinite(options.telemetryMaxPerNode) && options.telemetryMaxPerNode > 0
        ? Math.floor(options.telemetryMaxPerNode)
        : DEFAULT_TELEMETRY_MAX_PER_NODE;
    this.telemetryMaxTotalRecords =
      Number.isFinite(options.telemetryMaxTotalRecords) && options.telemetryMaxTotalRecords > 0
        ? Math.floor(options.telemetryMaxTotalRecords)
        : DEFAULT_TELEMETRY_MAX_TOTAL_RECORDS;
    this.telemetryProvider =
      options && typeof options.telemetryProvider === 'object' ? options.telemetryProvider : null;
    this.telemetryStore = new Map();
    this.telemetryRecordIds = new Set();
    this.telemetryRecordOrder = [];
    this.telemetryUpdatedAt = null;
    this.telemetryStats = {
      totalRecords: 0,
      totalNodes: 0,
      diskBytes: 0
    };
    this.nodeRegistry = new Map();
    this.channelConfig = CHANNEL_CONFIG;
    this.messageStore = new Map();
    this.messageLogPath =
      typeof options.messageLogPath === 'string' && options.messageLogPath.trim()
        ? options.messageLogPath.trim()
        : null;
    this._messageDirty = false;
    this._messagePersistTimer = null;
    this._ensureConfiguredChannels();
  }

  async start() {
    if (this.server) {
      return;
    }

    await this._loadMessageLog();

    this.server = http.createServer((req, res) => {
      const parsedUrl = (() => {
        try {
          return new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
        } catch {
          return { pathname: req.url };
        }
      })();
      const pathname = parsedUrl.pathname || req.url;
      if (pathname === '/api/events') {
        this._handleEventStream(req, res);
        return;
      }
      if (pathname === '/api/telemetry/export.csv') {
        this._handleTelemetryExportRequest(req, res);
        return;
      }
      if (pathname === '/api/telemetry') {
        this._handleTelemetryRequest(req, res);
        return;
      }
      if (pathname === '/debug') {
        this._handleDebugRequest(req, res);
        return;
      }
      this._serveStatic(req, res);
    });

    await new Promise((resolve, reject) => {
      const cleanup = (err) => {
        this.server?.off('listening', onListening);
        this.server?.off('error', onError);
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      };

      const onListening = () => cleanup();
      const onError = (err) => cleanup(err);

      this.server.once('listening', onListening);
      this.server.once('error', onError);
      this.server.listen(this.port, this.host);
    });

    this._startPing();
    // eslint-disable-next-line no-console
    console.log(`[WEB] Dashboard listening at http://${this.host}:${this.port}`);
  }

  async stop() {
    this._stopPing();
    this._flushMessagePersistSync();

    for (const client of this.clients) {
      try {
        client.end();
      } catch {
        // ignore
      }
    }
    this.clients.clear();

    if (!this.server) return;

    await new Promise((resolve) => {
      this.server.close(() => resolve());
    });
    this.server = null;
  }

  publishStatus(info) {
    if (!info) return;
    this.lastStatus = info;
    this._broadcast({ type: 'status', payload: info });
  }

  publishSummary(summary) {
    if (!summary) return;
    const base = this.selfMeshId ? { ...summary, selfMeshId: this.selfMeshId } : { ...summary };
    const payload = this._hydrateSummaryNodes(base);
    this._appendSummary(payload);
    this._broadcast({ type: 'summary', payload });
    this._broadcastMetrics();
  }

  publishCallmesh(info) {
    if (!info) return;
    const sanitized = sanitizeCallmeshPayload(info);
    if (!sanitized) return;
    this.lastCallmesh = sanitized;
    const mappingItems = Array.isArray(sanitized.mappingItems) ? sanitized.mappingItems : [];
    const unique = new Set(
      mappingItems
        .map((item) => normalizeMeshId(item?.mesh_id))
        .filter(Boolean)
    );
    this.metrics.mappingCount = unique.size;
    this._broadcast({ type: 'callmesh', payload: sanitized });
    this._broadcastMetrics();
  }

  publishAprs(info) {
    if (!info) return;
    const timestamp =
      Number.isFinite(Number(info.timestamp))
        ? Number(info.timestamp)
        : Number.isFinite(Number(info.timestampMs))
          ? Number(info.timestampMs)
          : Date.now();
    const sanitized = cloneJson({
      ...info,
      timestamp,
      timestampMs: Number.isFinite(Number(info.timestampMs)) ? Number(info.timestampMs) : timestamp
    });
    this.lastAprsInfo = sanitized;
    if (sanitized?.flowId) {
      this._rememberAprsRecord(sanitized);
    }
    this._broadcast({ type: 'aprs', payload: sanitized });
    this._updateAprsMetrics(sanitized);
    this._broadcastMetrics();
  }

  publishTelemetry(payload) {
    if (!payload || !payload.type) {
      return;
    }
    this._applyTelemetryStatsPayload(payload.stats);
    if (payload.type === 'reset') {
      this.telemetryStore.clear();
      this.telemetryRecordIds.clear();
      this.telemetryRecordOrder = [];
      this.telemetryUpdatedAt =
        Number.isFinite(payload.updatedAt) && payload.updatedAt > 0
          ? Number(payload.updatedAt)
          : Date.now();
      const stats = this._computeTelemetryStats();
      this._broadcast({
        type: 'telemetry-reset',
        payload: { updatedAt: this.telemetryUpdatedAt, stats, maxTotalRecords: this.telemetryMaxTotalRecords }
      });
      return;
    }
    if (payload.type !== 'append') {
      return;
    }
    const meshId = payload.meshId || payload.record?.meshId;
    const record = sanitizeTelemetryRecord(payload.record);
    if (!meshId || !record) {
      return;
    }
    if (record.id && this.telemetryRecordIds.has(record.id)) {
      return;
    }
    const nodeInfo = sanitizeTelemetryNode(payload.node) || sanitizeTelemetryNode(record.node);
    this._appendTelemetryRecord(meshId, nodeInfo, record);
    this.telemetryUpdatedAt =
      Number.isFinite(payload.updatedAt) && payload.updatedAt > 0
        ? Number(payload.updatedAt)
        : Date.now();
    const stats = this._computeTelemetryStats();
    this._broadcast({
      type: 'telemetry-append',
      payload: {
        meshId,
        node: nodeInfo,
        record,
        updatedAt: this.telemetryUpdatedAt,
        stats,
        maxTotalRecords: this.telemetryMaxTotalRecords
      }
    });
  }

  seedTelemetrySnapshot(snapshot = {}) {
    this.telemetryStore.clear();
    this.telemetryRecordIds.clear();
    this.telemetryRecordOrder = [];

    const nodes = Array.isArray(snapshot.nodes) ? snapshot.nodes : [];
    for (const node of nodes) {
      const meshId = node?.meshId;
      if (!meshId) continue;
      const nodeInfo = sanitizeTelemetryNode(node.node);
      const records = Array.isArray(node.records) ? node.records : [];
      for (const record of records) {
        const sanitized = sanitizeTelemetryRecord(record);
        if (!sanitized) continue;
        if (sanitized.id && this.telemetryRecordIds.has(sanitized.id)) {
          continue;
        }
        this._appendTelemetryRecord(meshId, nodeInfo, sanitized);
      }
    }

    this.telemetryUpdatedAt =
      Number.isFinite(snapshot.updatedAt) && snapshot.updatedAt > 0
        ? Number(snapshot.updatedAt)
        : this.telemetryUpdatedAt ?? Date.now();
    this._applyTelemetryStatsPayload(snapshot.stats);
    this._computeTelemetryStats();

    if (this.clients.size) {
      this._broadcast({
        type: 'telemetry-snapshot',
        payload: this._buildTelemetrySnapshot()
      });
    }
  }

  publishSelf(info) {
    if (!info) return;
    const meshCandidate = info?.node?.meshId || info?.meshId || null;
    if (meshCandidate) {
      this.selfMeshId = meshCandidate;
      this._broadcast({ type: 'self', payload: { meshId: meshCandidate } });
    }
  }

  setAppVersion(version) {
    const normalized = typeof version === 'string' && version.trim() ? version.trim() : '';
    if (!normalized) {
      if (this.appVersion || this.lastAppInfo) {
        this.appVersion = '';
        this.lastAppInfo = { version: '' };
        this._broadcast({ type: 'app-info', payload: this.lastAppInfo });
      }
      return;
    }
    if (normalized === this.appVersion) {
      return;
    }
    this.appVersion = normalized;
    this.lastAppInfo = { version: this.appVersion };
    this._broadcast({ type: 'app-info', payload: this.lastAppInfo });
  }

  publishLog(entry) {
    if (!entry) return;
    this.logEntries.unshift(entry);
    while (this.logEntries.length > MAX_LOG_ENTRIES) {
      this.logEntries.pop();
    }
    this._broadcast({ type: 'log', payload: entry });
  }

  publishMessage(entry) {
    const sanitized = this._cloneMessageEntry(entry);
    if (!sanitized) {
      return;
    }
    const channelId = sanitized.channel;
    const store = this._getChannelStore(channelId);
    const existingIndex = store.findIndex((item) => item.flowId === sanitized.flowId);
    if (existingIndex !== -1) {
      store.splice(existingIndex, 1);
    }
    store.unshift(sanitized);
    if (store.length > MESSAGE_MAX_PER_CHANNEL) {
      store.length = MESSAGE_MAX_PER_CHANNEL;
    }
    const cloned = this._cloneMessageEntry(sanitized);
    this._broadcast({
      type: 'message-append',
      payload: {
        channelId,
        entry: cloned
      }
    });
    this._scheduleMessagePersist();
  }

  _handleDebugRequest(req, res) {
    if (req.method && req.method.toUpperCase() !== 'GET') {
      res.writeHead(405, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'Method Not Allowed', allowed: ['GET'] }));
      return;
    }
    this._readRelayStats()
      .then(({ stats, message }) => {
        const payload = {
          relayLinkStats: stats,
          message: message || undefined,
          generatedAt: new Date().toISOString()
        };
        res.writeHead(200, {
          'Content-Type': 'application/json; charset=utf-8',
          'Cache-Control': 'no-store, max-age=0'
        });
        res.end(JSON.stringify(payload, null, 2));
      })
      .catch((err) => {
        res.writeHead(500, {
          'Content-Type': 'application/json; charset=utf-8',
          'Cache-Control': 'no-store, max-age=0'
        });
        res.end(
          JSON.stringify(
            {
              error: 'Failed to load relayLinkStats',
              message: err.message
            },
            null,
            2
          )
        );
      });
  }

  async _readRelayStats() {
    if (!this.relayStatsPath) {
      return { stats: null, message: 'relayStatsPath not configured' };
    }
    try {
      const raw = await fsPromises.readFile(this.relayStatsPath, 'utf8');
      if (!raw || !raw.trim()) {
        return { stats: {}, message: null };
      }
      try {
        return { stats: JSON.parse(raw), message: null };
      } catch (parseErr) {
        throw new Error(`Invalid relay stats JSON: ${parseErr.message}`);
      }
    } catch (err) {
      if (err && err.code === 'ENOENT') {
        return { stats: {}, message: null };
      }
      throw err;
    }
  }

  seedMessageSnapshot(snapshot = {}) {
    this.messageStore.clear();
    const channels = snapshot.channels || snapshot;
    if (channels && typeof channels === 'object') {
      for (const [key, list] of Object.entries(channels)) {
        const channelId = Number(key);
        if (!Number.isFinite(channelId) || channelId < 0) {
          continue;
        }
        const store = [];
        if (Array.isArray(list)) {
          for (const entry of list) {
            const cloned = this._cloneMessageEntry(entry);
            if (!cloned) {
              continue;
            }
            const duplicateIndex = store.findIndex((item) => item.flowId === cloned.flowId);
            if (duplicateIndex !== -1) {
              store.splice(duplicateIndex, 1);
            }
            store.push(cloned);
            if (store.length >= MESSAGE_MAX_PER_CHANNEL) {
              break;
            }
          }
        }
        this.messageStore.set(channelId, store);
      }
    }
    this._ensureConfiguredChannels();
    if (this.clients.size) {
      this._broadcast({
        type: 'message-snapshot',
        payload: this._buildMessageSnapshotPayload()
      });
    }
  }

  _appendSummary(summary) {
    const copy = cloneJson(summary);
    this.summaryRows.unshift(copy);
    while (this.summaryRows.length > MAX_SUMMARY_ROWS) {
      this.summaryRows.pop();
    }
    this._updatePacketMetrics(summary);
  }

  _extractSummaryTimestamp(summary) {
    if (!summary) return Date.now();
    const rawTs = summary.timestamp;
    if (typeof rawTs === 'string') {
      const parsed = Date.parse(rawTs);
      if (!Number.isNaN(parsed)) {
        return parsed;
      }
    }
    if (Number.isFinite(rawTs)) {
      return Number(rawTs);
    }
    if (typeof summary.timestampLabel === 'string') {
      const parsed = Date.parse(summary.timestampLabel);
      if (!Number.isNaN(parsed)) {
        return parsed;
      }
    }
    return Date.now();
  }

  _prunePacketBuckets() {
    const cutoff = Date.now() - PACKET_WINDOW_MS;
    for (const key of Array.from(this.packetBuckets.keys())) {
      if (key < cutoff) {
        this.packetBuckets.delete(key);
      }
    }
  }

  _serveStatic(req, res) {
    const urlPath = req.url === '/' ? '/index.html' : req.url;
    const safePath = urlPath.split('?')[0].replace(/(\.\.(\/|\\))/g, '');
    const filePath = path.join(this._publicDir, safePath);
    if (!filePath.startsWith(this._publicDir)) {
      res.writeHead(403);
      res.end('Forbidden');
      return;
    }

    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(err.code === 'ENOENT' ? 404 : 500);
        res.end(err.code === 'ENOENT' ? 'Not found' : 'Server error');
        return;
      }
      res.writeHead(200, {
        'Content-Type': this._getMimeType(filePath)
      });
      res.end(data);
    });
  }

  _handleEventStream(_req, res) {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*'
    });
    res.write('\n');

    this._sendInitialSnapshot(res);

    this.clients.add(res);
    res.on('close', () => {
      this.clients.delete(res);
    });
  }

  _sendInitialSnapshot(res) {
    if (this.lastAppInfo) {
      this._write(res, { type: 'app-info', payload: this.lastAppInfo });
    }
    if (this.lastStatus) {
      this._write(res, { type: 'status', payload: this.lastStatus });
    }
    if (this.lastCallmesh) {
      this._write(res, { type: 'callmesh', payload: this.lastCallmesh });
    }
    if (this.selfMeshId) {
      this._write(res, { type: 'self', payload: { meshId: this.selfMeshId } });
    }
    this._write(res, { type: 'metrics', payload: this.metrics });
    if (this.summaryRows.length) {
      this._write(res, { type: 'summary-batch', payload: this.summaryRows });
    }
    if (this.logEntries.length) {
      this._write(res, { type: 'log-batch', payload: this.logEntries });
    }
    if (this.aprsRecordQueue.length) {
      for (const flowId of this.aprsRecordQueue) {
        const record = this.aprsFlowRecords.get(flowId);
        if (record) {
          this._write(res, { type: 'aprs', payload: record });
        }
      }
    }
    if (this.lastAprsInfo) {
      this._write(res, { type: 'aprs', payload: this.lastAprsInfo });
    }
    const nodeSnapshot = this._buildNodeSnapshot();
    if (nodeSnapshot.length) {
      this._write(res, { type: 'node-snapshot', payload: nodeSnapshot });
    }
    this._write(res, {
      type: 'message-snapshot',
      payload: this._buildMessageSnapshotPayload()
    });
    this._write(res, {
      type: 'telemetry-snapshot',
      payload: this._buildTelemetrySnapshot()
    });
  }

  async _handleTelemetryExportRequest(req, res) {
    const respond = (status, payload) => {
      if (res.headersSent) {
        res.end();
        return;
      }
      res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(payload));
    };

    if (!req.method || req.method.toUpperCase() !== 'GET') {
      respond(405, { error: 'Method Not Allowed', allowed: ['GET'] });
      return;
    }

    let url;
    try {
      url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    } catch {
      respond(400, { error: 'Invalid URL' });
      return;
    }

    const meshIdParam = url.searchParams.get('meshId') || url.searchParams.get('mesh_id');
    if (!meshIdParam) {
      respond(400, { error: 'meshId is required' });
      return;
    }
    const startRaw = url.searchParams.get('startMs') ?? url.searchParams.get('start');
    const endRaw = url.searchParams.get('endMs') ?? url.searchParams.get('end');
    const searchRaw = url.searchParams.get('search');

    const startMs = Number.isFinite(Number(startRaw)) ? Number(startRaw) : null;
    const endMs = Number.isFinite(Number(endRaw)) ? Number(endRaw) : null;
    if (startMs != null && endMs != null && startMs > endMs) {
      respond(400, { error: 'startMs must be less than or equal to endMs' });
      return;
    }
    const searchTerm = typeof searchRaw === 'string' && searchRaw.trim() ? searchRaw.trim().toLowerCase() : '';

    if (!this.telemetryProvider) {
      respond(503, { error: 'telemetry provider unavailable' });
      return;
    }

    const iteratorOptions = {
      meshId: meshIdParam,
      startMs,
      endMs
    };

    const createIterator = () => {
      if (typeof this.telemetryProvider.streamTelemetryRecords === 'function') {
        return this.telemetryProvider.streamTelemetryRecords(iteratorOptions);
      }
      return (async function* fallback(self) {
        try {
          const payload = await self.telemetryProvider.getTelemetryRecordsForRange({
            ...iteratorOptions,
            limit: null
          });
          if (Array.isArray(payload?.records)) {
            for (const record of payload.records) {
              yield record;
            }
          }
        } catch (err) {
          throw err;
        }
      })(this);
    };

    const metricKeysInUseSet = new Set();
    const extraMetricKeySet = new Set();
    let totalRecords = 0;

    try {
      for await (const record of createIterator()) {
        if (!record) continue;
        if (searchTerm && !recordMatchesSearch(record, searchTerm)) {
          continue;
        }
        totalRecords += 1;
        collectTelemetryMetricKeys(record, metricKeysInUseSet, extraMetricKeySet);
      }
    } catch (err) {
      console.warn(`預備遙測匯出失敗：${err.message}`);
      respond(500, { error: 'failed to prepare export' });
      return;
    }

    const baseMetricOrder = Object.keys(TELEMETRY_METRIC_DEFINITIONS);
    const metricKeysInUse = baseMetricOrder.filter((key) => metricKeysInUseSet.has(key));
    const extraMetricKeys = Array.from(extraMetricKeySet).sort((a, b) => a.localeCompare(b));

    const normalizedName = normalizeMeshId(meshIdParam) || meshIdParam || 'telemetry';
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const safeName = normalizedName.replace(/[^0-9a-z_-]/gi, '');
    const fileName = `telemetry-${safeName || 'export'}-${timestamp}.csv`;

    res.writeHead(200, {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="${fileName}"`,
      'Cache-Control': 'no-store, max-age=0',
      'X-Total-Records': String(totalRecords)
    });
    res.write('\ufeff');

    const headerColumns = [
      '時間 (ISO)',
      'Unix 秒',
      'MeshID',
      '節點',
      'Channel',
      'SNR',
      'RSSI',
      '詳細',
      '最後轉發',
      '最後轉發 MeshID',
      '最後轉發為推測',
      '最後轉發說明',
      '跳數',
      'Hop Start',
      'Hop Limit',
      ...metricKeysInUse.map((key) => {
        const def = TELEMETRY_METRIC_DEFINITIONS[key] || {};
        const label = def.label || key;
        return def.unit ? `${label} (${def.unit})` : label;
      }),
      ...extraMetricKeys
    ];
    res.write(headerColumns.map(escapeCsvValue).join(',') + '\n');

    try {
      for await (const record of createIterator()) {
        if (!record) continue;
        if (searchTerm && !recordMatchesSearch(record, searchTerm)) {
          continue;
        }
        res.write(
          buildTelemetryCsvLine(record, {
            metricKeysInUse,
            extraMetricKeys
          })
        );
      }
    } catch (err) {
      console.warn(`遙測匯出中斷：${err.message}`);
      res.end();
      return;
    }

    res.end();
  }

  _handleTelemetryRequest(req, res) {
    const respond = (status, payload) => {
      res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(payload));
    };

    const handler = async () => {
      if (!req.method || req.method.toUpperCase() !== 'GET') {
        respond(405, { error: 'Method Not Allowed', allowed: ['GET'] });
        return;
      }
      let url;
      try {
        url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
      } catch {
        respond(400, { error: 'Invalid URL' });
        return;
      }
      const meshIdParam = url.searchParams.get('meshId') || url.searchParams.get('mesh_id');
      if (!meshIdParam) {
        respond(400, { error: 'meshId is required' });
        return;
      }
      const startRaw = url.searchParams.get('startMs') ?? url.searchParams.get('start');
      const endRaw = url.searchParams.get('endMs') ?? url.searchParams.get('end');
      const limitRaw = url.searchParams.get('limit');
      const startMs = Number.isFinite(Number(startRaw)) ? Number(startRaw) : null;
      const endMs = Number.isFinite(Number(endRaw)) ? Number(endRaw) : null;
      if (startMs != null && endMs != null && startMs > endMs) {
        respond(400, { error: 'startMs must be less than or equal to endMs' });
        return;
      }
      const limit =
        Number.isFinite(Number(limitRaw)) && Number(limitRaw) > 0 ? Math.floor(Number(limitRaw)) : null;

      if (
        this.telemetryProvider &&
        typeof this.telemetryProvider.getTelemetryRecordsForRange === 'function'
      ) {
        try {
          const payload = await this.telemetryProvider.getTelemetryRecordsForRange({
            meshId: meshIdParam,
            startMs,
            endMs,
            limit
          });
          if (!payload || typeof payload !== 'object') {
            throw new Error('invalid telemetry payload');
          }
          res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Cache-Control': 'no-store, max-age=0'
          });
          res.end(JSON.stringify(payload));
          return;
        } catch (err) {
          console.warn(`遙測下載透過橋接層失敗：${err.message}`);
        }
      }

      const { bucket, key } = this._findTelemetryBucket(meshIdParam);
      if (!bucket) {
        respond(404, { error: 'Telemetry not found for provided meshId' });
        return;
      }

      const records = Array.isArray(bucket.records) ? bucket.records : [];
      const filtered = records.filter((record) => {
        const sample = Number(record?.sampleTimeMs);
        if (!Number.isFinite(sample)) {
          return false;
        }
        if (startMs != null && sample < startMs) {
          return false;
        }
        if (endMs != null && sample > endMs) {
          return false;
        }
        return true;
      });

      const sorted = filtered.slice().sort((a, b) => a.sampleTimeMs - b.sampleTimeMs);
      const limited =
        limit && sorted.length > limit ? sorted.slice(sorted.length - limit) : sorted;
      const clonedRecords = limited.map((record) => cloneJson(record));

      let filteredLatestMs = null;
      let filteredEarliestMs = null;
      const metricsSet = new Set();
      for (const record of clonedRecords) {
        const sample = Number(record?.sampleTimeMs);
        if (Number.isFinite(sample)) {
          if (filteredLatestMs == null || sample > filteredLatestMs) {
            filteredLatestMs = sample;
          }
          if (filteredEarliestMs == null || sample < filteredEarliestMs) {
            filteredEarliestMs = sample;
          }
        }
        const metrics = record?.telemetry?.metrics;
        if (metrics && typeof metrics === 'object') {
          for (const metricKey of Object.keys(metrics)) {
            metricsSet.add(metricKey);
          }
        }
      }

      const response = {
        meshId: bucket.meshId,
        rawMeshId: bucket.rawMeshId || bucket.meshId,
        meshIdNormalized: normalizeMeshId(bucket.meshId),
        node: bucket.node ? cloneJson(bucket.node) : null,
        records: clonedRecords,
        totalRecords: records.length,
        filteredCount: clonedRecords.length,
        latestSampleMs: filteredLatestMs != null ? filteredLatestMs : null,
        earliestSampleMs: filteredEarliestMs != null ? filteredEarliestMs : null,
        availableMetrics: Array.from(metricsSet),
        range: {
          startMs: startMs != null ? startMs : null,
          endMs: endMs != null ? endMs : null
        },
        requestedLimit: limit,
        updatedAt: this.telemetryUpdatedAt,
        sourceKey: key
      };

      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store, max-age=0'
      });
      res.end(JSON.stringify(response));
    };

    handler().catch((err) => {
      console.error(`處理遙測下載失敗：${err.message}`);
      respond(500, { error: 'internal error' });
    });
  }

  _appendTelemetryRecord(meshId, node, record) {
    if (!meshId || !record) {
      return;
    }
    const key = String(meshId);
    const normalizedMeshId = normalizeMeshId(meshId);
    const sanitizedNode = node ? sanitizeTelemetryNode(node) : null;
    let registryNode = null;
    if (sanitizedNode && normalizedMeshId) {
      registryNode = this._upsertNode({
        ...sanitizedNode,
        meshId: sanitizedNode.meshId ?? meshId,
        meshIdNormalized: sanitizedNode.meshIdNormalized ?? normalizedMeshId
      });
    } else if (normalizedMeshId) {
      registryNode = this.nodeRegistry.get(normalizedMeshId) || null;
    }
    let bucket = this.telemetryStore.get(key);
    const mergedNode = mergeNodeInfo(
      bucket?.node || {},
      sanitizedNode ? { ...sanitizedNode, meshId: sanitizedNode.meshId ?? meshId } : null,
      record.node ? sanitizeTelemetryNode(record.node) : null,
      registryNode
    );
    const rawMeshId = record.rawMeshId || record.meshId || meshId;
    if (!bucket) {
      bucket = {
        meshId: key,
        rawMeshId: rawMeshId || meshId,
        node: mergedNode,
        records: []
      };
      this.telemetryStore.set(key, bucket);
    } else if (mergedNode) {
      bucket.node = mergedNode;
    }
    if (rawMeshId && !bucket.rawMeshId) {
      bucket.rawMeshId = rawMeshId;
    }
    if (mergedNode) {
      record.node = mergeNodeInfo(record.node ? sanitizeTelemetryNode(record.node) : {}, mergedNode);
    }
    const sampleMs = Number.isFinite(record.sampleTimeMs)
      ? Number(record.sampleTimeMs)
      : Number.isFinite(record.timestampMs)
        ? Number(record.timestampMs)
        : Date.now();
    record.sampleTimeMs = Number.isFinite(sampleMs) ? sampleMs : Date.now();
    if (!Number.isFinite(record.timestampMs)) {
      record.timestampMs = record.sampleTimeMs;
    }
    if (!record.id) {
      record.id = `${key}-${record.sampleTimeMs}-${Math.random().toString(16).slice(2, 10)}`;
    }
    if (record.id) {
      this.telemetryRecordIds.add(record.id);
    }
    bucket.records.push(record);
    if (this.telemetryMaxPerNode > 0 && bucket.records.length > this.telemetryMaxPerNode) {
      const excess = bucket.records.length - this.telemetryMaxPerNode;
      const removed = bucket.records.splice(0, excess);
      for (const item of removed) {
        if (item?.id) {
          this.telemetryRecordIds.delete(item.id);
          this._removeTelemetryOrderEntry(key, item.id);
        }
      }
    }
    if (record.id) {
      this._trackTelemetryRecord(key, record.id);
    }
    this._enforceTelemetryGlobalLimit();
  }

  _findTelemetryBucket(meshId) {
    if (!meshId) {
      return { bucket: null, key: null };
    }
    if (this.telemetryStore.has(meshId)) {
      return { bucket: this.telemetryStore.get(meshId), key: meshId };
    }
    const normalized = normalizeMeshId(meshId);
    if (!normalized) {
      return { bucket: null, key: null };
    }
    for (const [key, bucket] of this.telemetryStore.entries()) {
      if (!bucket) continue;
      const keyNormalized = normalizeMeshId(key);
      const rawNormalized = normalizeMeshId(bucket.rawMeshId);
      if (keyNormalized === normalized || rawNormalized === normalized) {
        return { bucket, key };
      }
    }
    return { bucket: null, key: null };
  }

  _trackTelemetryRecord(meshKey, recordId) {
    if (!recordId) {
      return;
    }
    this.telemetryRecordOrder.push({ meshId: meshKey, recordId });
  }

  _removeTelemetryOrderEntry(meshKey, recordId) {
    if (!recordId || this.telemetryRecordOrder.length === 0) {
      return;
    }
    for (let i = this.telemetryRecordOrder.length - 1; i >= 0; i -= 1) {
      const entry = this.telemetryRecordOrder[i];
      if (entry.recordId === recordId && (meshKey == null || entry.meshId === meshKey)) {
        this.telemetryRecordOrder.splice(i, 1);
        break;
      }
    }
  }

  _enforceTelemetryGlobalLimit() {
    const maxTotal = this.telemetryMaxTotalRecords;
    if (!Number.isFinite(maxTotal) || maxTotal <= 0) {
      return;
    }
    while (this.telemetryRecordOrder.length > maxTotal) {
      const oldest = this.telemetryRecordOrder.shift();
      if (!oldest) {
        break;
      }
      const bucket = this.telemetryStore.get(oldest.meshId);
      if (!bucket || !Array.isArray(bucket.records) || !bucket.records.length) {
        this.telemetryRecordIds.delete(oldest.recordId);
        continue;
      }
      const index = bucket.records.findIndex((item) => item?.id === oldest.recordId);
      if (index === -1) {
        this.telemetryRecordIds.delete(oldest.recordId);
        continue;
      }
      const [removed] = bucket.records.splice(index, 1);
      if (removed?.id) {
        this.telemetryRecordIds.delete(removed.id);
      }
      if (!bucket.records.length) {
        this.telemetryStore.delete(oldest.meshId);
      }
    }
  }

  _buildTelemetrySnapshot(limitPerNode = this.telemetryMaxPerNode) {
    const nodes = [];
    for (const bucket of this.telemetryStore.values()) {
      if (!bucket) {
        continue;
      }
      const records = Array.isArray(bucket.records) ? bucket.records : [];
      let latestSampleMs = null;
      let earliestSampleMs = null;
      const metricsSet = new Set();
      for (const record of records) {
        const sample = Number(record?.sampleTimeMs);
        if (Number.isFinite(sample)) {
          if (latestSampleMs == null || sample > latestSampleMs) {
            latestSampleMs = sample;
          }
          if (earliestSampleMs == null || sample < earliestSampleMs) {
            earliestSampleMs = sample;
          }
        }
        const metrics = record?.telemetry?.metrics;
        if (metrics && typeof metrics === 'object') {
          for (const key of Object.keys(metrics)) {
            metricsSet.add(key);
          }
        }
      }
      nodes.push({
        meshId: bucket.meshId,
        rawMeshId: bucket.rawMeshId || bucket.meshId,
        node: bucket.node ? cloneJson(bucket.node) : null,
        totalRecords: records.length,
        latestSampleMs,
        earliestSampleMs,
        metrics: Array.from(metricsSet)
      });
    }
    nodes.sort((a, b) => {
      const labelA = (a.node?.label || a.meshId || '').toLowerCase();
      const labelB = (b.node?.label || b.meshId || '').toLowerCase();
      if (labelA < labelB) return -1;
      if (labelA > labelB) return 1;
      return 0;
    });
    const stats = this._computeTelemetryStats();
    return {
      updatedAt: this.telemetryUpdatedAt,
      nodes,
      stats,
      maxTotalRecords: this.telemetryMaxTotalRecords
    };
  }

  _applyTelemetryStatsPayload(stats) {
    if (!stats || typeof stats !== 'object') {
      return;
    }
    if (Number.isFinite(stats.diskBytes)) {
      this.telemetryStats.diskBytes = Number(stats.diskBytes);
    }
  }

  _computeTelemetryStats() {
    let totalRecords = 0;
    for (const bucket of this.telemetryStore.values()) {
      if (!bucket || !Array.isArray(bucket.records)) continue;
      totalRecords += bucket.records.length;
    }
    const totalNodes = this.telemetryStore.size;
    const diskBytes = Number.isFinite(this.telemetryStats.diskBytes) ? this.telemetryStats.diskBytes : 0;
    this.telemetryStats = {
      totalRecords,
      totalNodes,
      diskBytes
    };
    return { ...this.telemetryStats };
  }

  publishNode(info) {
    const merged = this._upsertNode(info);
    if (!merged) {
      return;
    }
    this._broadcast({ type: 'node', payload: merged });
  }

  seedNodeSnapshot(list = []) {
    this.nodeRegistry.clear();
    const snapshot = [];
    if (Array.isArray(list)) {
      for (const entry of list) {
        const merged = this._upsertNode(entry);
        if (merged) {
          snapshot.push(merged);
        }
      }
    }
    const payload = snapshot.length ? snapshot : this._buildNodeSnapshot();
    if (this.clients.size) {
      this._broadcast({ type: 'node-snapshot', payload });
    }
  }

  _buildNodeSnapshot() {
    const nodes = [];
    for (const value of this.nodeRegistry.values()) {
      nodes.push(cloneJson(value));
    }
    nodes.sort((a, b) => {
      const labelA = (a.label || a.longName || a.shortName || a.meshId || '').toLowerCase();
      const labelB = (b.label || b.longName || b.shortName || b.meshId || '').toLowerCase();
      if (labelA < labelB) return -1;
      if (labelA > labelB) return 1;
      return 0;
    });
    return nodes;
  }

  _upsertNode(info) {
    if (!info || typeof info !== 'object') {
      return null;
    }
    const candidate = info.meshId || info.meshIdNormalized || info.meshIdOriginal;
    const normalized = normalizeMeshId(candidate);
    if (!normalized) {
      return null;
    }
    const existing = this.nodeRegistry.get(normalized) || {
      meshId: normalized,
      meshIdNormalized: normalized
    };
    const merged = mergeNodeInfo(existing, {
      ...info,
      meshIdNormalized: normalized,
      meshId: info.meshId || existing.meshId || normalized
    });
    this.nodeRegistry.set(normalized, merged);
    return cloneJson(merged);
  }

  _broadcast(event) {
    if (!event) return;
    for (const client of this.clients) {
      this._write(client, event);
    }
  }

  _ensureConfiguredChannels() {
    if (!this.channelConfig || !Array.isArray(this.channelConfig)) {
      return;
    }
    for (const channel of this.channelConfig) {
      if (!channel || !Number.isFinite(channel.id)) {
        continue;
      }
      if (!this.messageStore.has(channel.id)) {
        this.messageStore.set(channel.id, []);
      }
    }
  }

  _getChannelStore(channelId) {
    const key = Number(channelId);
    if (!Number.isFinite(key) || key < 0) {
      return [];
    }
    if (!this.messageStore.has(key)) {
      this.messageStore.set(key, []);
    }
    return this.messageStore.get(key);
  }

  _cloneMessageEntry(entry) {
    if (!entry || typeof entry !== 'object') {
      return null;
    }
    const channelId = Number(entry.channel);
    if (!Number.isFinite(channelId) || channelId < 0) {
      return null;
    }
    const timestampMs = Number.isFinite(entry.timestampMs) ? Number(entry.timestampMs) : Date.now();
    const flowId =
      typeof entry.flowId === 'string' && entry.flowId.trim()
        ? entry.flowId.trim()
        : `${channelId}-${timestampMs}-${Math.random().toString(16).slice(2, 10)}`;
    const detail = typeof entry.detail === 'string' ? entry.detail : '';
    const extraLines = Array.isArray(entry.extraLines)
      ? entry.extraLines.filter((line) => typeof line === 'string' && line.trim())
      : [];
    const timestampLabel =
      typeof entry.timestampLabel === 'string' && entry.timestampLabel.trim()
        ? entry.timestampLabel.trim()
        : new Date(timestampMs).toISOString();
    return {
      ...entry,
      type: entry.type === 'Text' ? 'Text' : 'Text',
      channel: channelId,
      detail,
      extraLines,
      from: entry.from ? cloneJson(entry.from) : null,
      relay: entry.relay ? cloneJson(entry.relay) : null,
      relayMeshId: entry.relayMeshId ?? null,
      relayMeshIdNormalized: entry.relayMeshIdNormalized ?? null,
      hops: entry.hops ? cloneJson(entry.hops) : null,
      timestampMs,
      timestampLabel,
      flowId
    };
  }

  _buildMessageSnapshotPayload(limitPerChannel = MESSAGE_MAX_PER_CHANNEL) {
    const channels = {};
    for (const [channelId, entries] of this.messageStore.entries()) {
      if (!Number.isFinite(channelId) || channelId < 0) {
        continue;
      }
      const list = Array.isArray(entries) ? entries : [];
      const limit =
        Number.isFinite(limitPerChannel) && limitPerChannel > 0
          ? Math.floor(limitPerChannel)
          : list.length;
      const slice =
        list.length > limit ? list.slice(0, limit) : list.slice();
      channels[channelId] = slice.map((entry) => this._cloneMessageEntry(entry)).filter(Boolean);
    }
    this._ensureConfiguredChannels();
    for (const channel of this.channelConfig || []) {
      if (!Number.isFinite(channel?.id)) continue;
      if (!Object.prototype.hasOwnProperty.call(channels, channel.id)) {
        channels[channel.id] = [];
      }
    }
    return { channels };
  }

  async _loadMessageLog() {
    if (!this.messageLogPath) {
      return;
    }
    this.messageStore.clear();
    try {
      const content = await fsPromises.readFile(this.messageLogPath, 'utf8');
      if (!content) {
        this._ensureConfiguredChannels();
        return;
      }
      const lines = content.split(/\n+/).filter((line) => line.trim().length > 0);
      for (const line of lines) {
        let parsed;
        try {
          parsed = JSON.parse(line);
        } catch (err) {
          // eslint-disable-next-line no-console
          console.warn(`跳過無法解析的訊息紀錄: ${err.message}`);
          continue;
        }
        const entry = this._cloneMessageEntry(parsed);
        if (!entry) {
          continue;
        }
        const channelId = entry.channel;
        const store = this._getChannelStore(channelId);
        const duplicateIndex = store.findIndex((item) => item.flowId === entry.flowId);
        if (duplicateIndex !== -1) {
          store.splice(duplicateIndex, 1);
        }
        store.push(entry);
        if (store.length > MESSAGE_MAX_PER_CHANNEL) {
          store.splice(0, store.length - MESSAGE_MAX_PER_CHANNEL);
        }
      }
      this._ensureConfiguredChannels();
    } catch (err) {
      if (err?.code !== 'ENOENT') {
        // eslint-disable-next-line no-console
        console.warn(`載入訊息紀錄失敗: ${err.message}`);
      }
    }
  }

  _scheduleMessagePersist() {
    if (!this.messageLogPath) {
      return;
    }
    this._messageDirty = true;
    if (this._messagePersistTimer) {
      return;
    }
    this._messagePersistTimer = setTimeout(() => {
      this._messagePersistTimer = null;
      this._persistMessageLog().catch((err) => {
        console.error(`寫入訊息紀錄失敗: ${err.message}`);
        this._scheduleMessagePersist();
      });
    }, MESSAGE_PERSIST_INTERVAL_MS);
    this._messagePersistTimer.unref?.();
  }

  async _persistMessageLog() {
    if (!this.messageLogPath || !this._messageDirty) {
      return;
    }
    this._messageDirty = false;
    const lines = [];
    const sortedChannels = Array.from(this.messageStore.entries()).sort((a, b) => a[0] - b[0]);
    for (const [, list] of sortedChannels) {
      const entries = Array.isArray(list) ? list : [];
      for (const entry of entries) {
        lines.push(JSON.stringify(entry));
      }
    }
    try {
      await fsPromises.mkdir(path.dirname(this.messageLogPath), { recursive: true });
      await fsPromises.writeFile(this.messageLogPath, lines.join('\n'), 'utf8');
    } catch (err) {
      this._messageDirty = true;
      throw err;
    }
  }

  _flushMessagePersistSync() {
    if (!this.messageLogPath) {
      return;
    }
    if (this._messagePersistTimer) {
      clearTimeout(this._messagePersistTimer);
      this._messagePersistTimer = null;
    }
    if (!this._messageDirty) {
      return;
    }
    const lines = [];
    const sortedChannels = Array.from(this.messageStore.entries()).sort((a, b) => a[0] - b[0]);
    for (const [, list] of sortedChannels) {
      const entries = Array.isArray(list) ? list : [];
      for (const entry of entries) {
        lines.push(JSON.stringify(entry));
      }
    }
    try {
      fs.mkdirSync(path.dirname(this.messageLogPath), { recursive: true });
      fs.writeFileSync(this.messageLogPath, lines.join('\n'), 'utf8');
      this._messageDirty = false;
    } catch (err) {
      console.error(`同步寫入訊息紀錄失敗: ${err.message}`);
    }
  }

  _broadcastMetrics() {
    this._broadcast({
      type: 'metrics',
      payload: this.metrics
    });
  }

  _write(stream, event) {
    if (!stream || !event) return;
    try {
      stream.write(`data: ${JSON.stringify(event)}\n\n`);
    } catch {
      this.clients.delete(stream);
    }
  }

  _startPing() {
    this._stopPing();
    if (!this.inactivityPingMs || this.inactivityPingMs <= 0) return;
    this._pingTimer = setInterval(() => {
      for (const client of this.clients) {
        try {
          client.write(': ping\n\n');
        } catch {
          this.clients.delete(client);
        }
      }
    }, this.inactivityPingMs);
    this._pingTimer.unref?.();
  }

  _stopPing() {
    if (this._pingTimer) {
      clearInterval(this._pingTimer);
      this._pingTimer = null;
    }
  }

  _getMimeType(filePath) {
    if (filePath.endsWith('.html')) return 'text/html; charset=utf-8';
    if (filePath.endsWith('.js')) return 'application/javascript; charset=utf-8';
    if (filePath.endsWith('.css')) return 'text/css; charset=utf-8';
    if (filePath.endsWith('.json')) return 'application/json; charset=utf-8';
    return 'text/plain; charset=utf-8';
  }

  _updatePacketMetrics(summary) {
    if (!summary) return;
    if (this._isSelfSummary(summary)) {
      return;
    }
    const timestamp = this._extractSummaryTimestamp(summary);
    if (!Number.isFinite(timestamp)) {
      return;
    }
    const bucketKey = Math.floor(timestamp / PACKET_BUCKET_MS) * PACKET_BUCKET_MS;
    const current = this.packetBuckets.get(bucketKey) ?? 0;
    this.packetBuckets.set(bucketKey, current + 1);
    this._prunePacketBuckets();
    const total = Array.from(this.packetBuckets.values()).reduce((acc, val) => acc + val, 0);
    this.metrics.packetLast10Min = total;
  }

  _updateAprsMetrics(info) {
    if (!info) return;
    const flowId = info.flowId;
    if (!flowId) return;
    if (this.aprsFlowIds.has(flowId)) {
      return;
    }
    this.aprsFlowIds.add(flowId);
    this.aprsFlowQueue.push(flowId);
    if (this.aprsFlowQueue.length > APRS_HISTORY_MAX) {
      const oldest = this.aprsFlowQueue.shift();
      if (oldest) {
        this.aprsFlowIds.delete(oldest);
      }
    }
    this.metrics.aprsUploaded += 1;
  }

  _rememberAprsRecord(info) {
    if (!info || !info.flowId) {
      return;
    }
    const flowId = String(info.flowId).trim();
    if (!flowId) {
      return;
    }
    if (this.aprsFlowRecords.has(flowId)) {
      this.aprsFlowRecords.set(flowId, info);
      return;
    }
    this.aprsFlowRecords.set(flowId, info);
    this.aprsRecordQueue.push(flowId);
    while (this.aprsRecordQueue.length > APRS_RECORD_HISTORY_MAX) {
      const oldest = this.aprsRecordQueue.shift();
      if (oldest) {
        this.aprsFlowRecords.delete(oldest);
      }
    }
  }

  _isSelfSummary(summary) {
    const meshId = normalizeMeshId(summary?.from?.meshId || summary?.from?.meshIdNormalized);
    if (!meshId) return false;
    const inlineSelf = normalizeMeshId(summary?.selfMeshId);
    if (inlineSelf && inlineSelf === meshId) {
      return true;
    }
    const self = normalizeMeshId(this.selfMeshId);
    if (!self) return false;
    return meshId === self;
  }

  _hydrateSummaryNodes(summary) {
    if (!summary || typeof summary !== 'object') {
      return summary;
    }
    const next = { ...summary };
    next.from = this._hydrateSummaryNode(next.from, next.fromMeshId || next.fromMeshIdNormalized);
    next.to = this._hydrateSummaryNode(next.to, next.toMeshId || next.toMeshIdNormalized);
    next.relay = this._hydrateSummaryNode(next.relay, next.relayMeshId || next.relayMeshIdNormalized);
    next.nextHop = this._hydrateSummaryNode(next.nextHop, next.nextHopMeshId || next.nextHopMeshIdNormalized);
    return next;
  }

  _hydrateSummaryNode(node, fallbackMeshId = null) {
    const meshCandidate =
      node?.meshId || node?.meshIdNormalized || node?.meshIdOriginal || fallbackMeshId;
    const normalized = normalizeMeshId(meshCandidate);

    let registryNode = normalized ? this.nodeRegistry.get(normalized) : null;
    if (node && normalized) {
      registryNode = this._upsertNode({
        meshId: meshCandidate,
        meshIdNormalized: normalized,
        ...node
      });
    }

    if (!registryNode && !node) {
      return null;
    }

    return mergeNodeInfo({}, registryNode || {}, node || {}, {
      meshId: meshCandidate || registryNode?.meshId || null,
      meshIdNormalized: normalized || registryNode?.meshIdNormalized || null
    });
  }
}

module.exports = {
  WebDashboardServer
};

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

function buildNodeLabel(node) {
  if (!node || typeof node !== 'object') return null;
  const name = node.longName || node.shortName || null;
  const meshLabel = node.meshIdOriginal || node.meshId || node.meshIdNormalized || null;
  if (name && meshLabel) {
    return `${name} (${meshLabel})`;
  }
  return name || meshLabel || null;
}

function mergeNodeInfo(existing = {}, incoming = {}) {
  const result = {
    meshId: existing.meshId ?? null,
    meshIdOriginal: existing.meshIdOriginal ?? null,
    meshIdNormalized: existing.meshIdNormalized ?? null,
    shortName: existing.shortName ?? null,
    longName: existing.longName ?? null,
    hwModel: existing.hwModel ?? null,
    hwModelLabel: existing.hwModelLabel ?? null,
    role: existing.role ?? null,
    roleLabel: existing.roleLabel ?? null,
    latitude: existing.latitude ?? null,
    longitude: existing.longitude ?? null,
    altitude: existing.altitude ?? null,
    label: existing.label ?? null,
    lastSeenAt: existing.lastSeenAt ?? null
  };
  const sources = Array.isArray(incoming) ? incoming : [incoming];
  for (const source of sources) {
    if (!source || typeof source !== 'object') continue;
    if (source.meshIdNormalized) {
      result.meshIdNormalized = source.meshIdNormalized;
    }
    if (source.meshIdOriginal) {
      result.meshIdOriginal = source.meshIdOriginal;
    }
    if (source.meshId) {
      result.meshId = source.meshId;
    }
    if (source.shortName != null) {
      result.shortName = source.shortName;
    }
    if (source.longName != null) {
      result.longName = source.longName;
    }
    if (source.hwModel != null) {
      result.hwModel = source.hwModel;
    }
    if (source.hwModelLabel != null) {
      result.hwModelLabel = source.hwModelLabel;
    }
    if (source.role != null) {
      result.role = source.role;
    }
    if (source.roleLabel != null) {
      result.roleLabel = source.roleLabel;
    }
    if (source.position && typeof source.position === 'object') {
      const pos = source.position;
      if (Number.isFinite(pos.latitude)) {
        result.latitude = Number(pos.latitude);
      }
      if (Number.isFinite(pos.longitude)) {
        result.longitude = Number(pos.longitude);
      }
      if (Number.isFinite(pos.altitude)) {
        result.altitude = Number(pos.altitude);
      }
    }
    if (source.latitude != null) {
      const numeric = Number(source.latitude);
      if (Number.isFinite(numeric)) {
        result.latitude = numeric;
      }
    }
    if (source.longitude != null) {
      const numeric = Number(source.longitude);
      if (Number.isFinite(numeric)) {
        result.longitude = numeric;
      }
    }
    if (source.altitude != null) {
      const numeric = Number(source.altitude);
      if (Number.isFinite(numeric)) {
        result.altitude = numeric;
      }
    }
    if (source.lastSeenAt != null && Number.isFinite(source.lastSeenAt)) {
      result.lastSeenAt = Number(source.lastSeenAt);
    }
    if (source.label) {
      result.label = source.label;
    }
  }
  if (!result.meshId && result.meshIdNormalized) {
    result.meshId = result.meshIdNormalized;
  }
  if (!result.meshIdNormalized && result.meshId) {
    result.meshIdNormalized = normalizeMeshId(result.meshId);
  }
  if (result.hwModel && !result.hwModelLabel) {
    result.hwModelLabel = formatEnumLabel(result.hwModel);
  }
  if (result.role && !result.roleLabel) {
    result.roleLabel = formatEnumLabel(result.role);
  }
  if (result.latitude != null && !Number.isFinite(result.latitude)) {
    result.latitude = null;
  }
  if (result.longitude != null && !Number.isFinite(result.longitude)) {
    result.longitude = null;
  }
  if (result.altitude != null && !Number.isFinite(result.altitude)) {
    result.altitude = null;
  }
  if (!result.label) {
    result.label = buildNodeLabel(result);
  }
  return result;
}

function escapeCsvValue(value) {
  if (value === null || value === undefined) return '';
  const str = String(value);
  if (str.includes('"') || str.includes(',') || str.includes('\n') || str.includes('\r')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function formatSecondsAsDuration(seconds) {
  const numeric = Number(seconds);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return '';
  }
  let remaining = Math.floor(numeric);
  const units = [
    { label: '年', seconds: 365 * 24 * 60 * 60 },
    { label: '天', seconds: 24 * 60 * 60 },
    { label: '時', seconds: 60 * 60 },
    { label: '分', seconds: 60 },
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

function formatNumericForCsv(value, digits = null) {
  if (!Number.isFinite(value)) return '';
  if (digits == null) {
    return String(value);
  }
  const fixed = value.toFixed(digits);
  return fixed.replace(/0+$/, '').replace(/\.$/, '') || '0';
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
  if (typeof rawValue === 'boolean') {
    return rawValue ? 'true' : 'false';
  }
  return String(rawValue);
}

function buildTelemetryRelayDescriptor(record) {
  if (!record) return '';
  const relay = record.relay || {};
  const label =
    (typeof record.relayLabel === 'string' && record.relayLabel.trim()) ||
    (typeof relay.label === 'string' && relay.label.trim()) ||
    (typeof relay.longName === 'string' && relay.longName.trim()) ||
    (typeof relay.shortName === 'string' && relay.shortName.trim()) ||
    (typeof record.relayMeshId === 'string' && record.relayMeshId.trim()) ||
    (typeof record.relayMeshIdNormalized === 'string' && record.relayMeshIdNormalized.trim()) ||
    '';
  if (!label) {
    return '';
  }
  const guessed = Boolean(record.relayGuessed || relay.guessed);
  return guessed ? `${label} (?)` : label;
}

function buildTelemetryHopsDescriptor(record) {
  if (!record) return '';
  if (Number.isFinite(record.hopsUsed) && Number.isFinite(record.hopsTotal)) {
    return `${record.hopsUsed}/${record.hopsTotal}`;
  }
  if (Number.isFinite(record.hopsUsed)) {
    return String(record.hopsUsed);
  }
  if (typeof record.hopsLabel === 'string' && record.hopsLabel.trim()) {
    return record.hopsLabel.trim();
  }
  const hops = record.hops || {};
  if (Number.isFinite(hops.start) && Number.isFinite(hops.limit)) {
    const used = Math.max(hops.start - hops.limit, 0);
    return `${used}/${hops.start}`;
  }
  return '';
}

function collectTelemetryMetricKeys(record, metricSet, extraSet) {
  if (!record || typeof record !== 'object') {
    return;
  }
  const metrics = record.telemetry?.metrics;
  if (!metrics || typeof metrics !== 'object') {
    return;
  }
  for (const key of Object.keys(TELEMETRY_METRIC_DEFINITIONS)) {
    if (metrics[key] != null) {
      metricSet.add(key);
    }
  }
  for (const [path] of flattenTelemetryMetrics(metrics)) {
    if (!TELEMETRY_METRIC_DEFINITIONS[path]) {
      extraSet.add(path);
    }
  }
}

function buildTelemetryCsvLine(record, { metricKeysInUse, extraMetricKeys }) {
  const timeMs = Number(record?.sampleTimeMs ?? record?.timestampMs);
  const isoTime = Number.isFinite(timeMs) ? new Date(timeMs).toISOString() : '';
  const unixSeconds = Number.isFinite(timeMs) ? Math.floor(timeMs / 1000) : '';
  const meshId = record.meshId || record.node?.meshId || '';
  const nodeLabel = buildNodeLabel(record.node) || meshId || '';
  const channelValue = Number.isFinite(record.channel) ? record.channel : '';
  const snrValue = Number.isFinite(record.snr) ? formatNumericForCsv(record.snr, 2) : '';
  const rssiValue = Number.isFinite(record.rssi) ? formatNumericForCsv(record.rssi, 0) : '';
  const detailValue = record.detail || '';
  const relayDescriptor = buildTelemetryRelayDescriptor(record) || '';
  const relayMeshValue =
    record.relayMeshId ||
    record.relayMeshIdNormalized ||
    record.relay?.meshId ||
    record.relay?.meshIdNormalized ||
    '';
  const relayGuessFlag = record.relayGuessed ? 'true' : '';
  const relayReason = record.relayGuessReason || '';
  const hopsDescriptor = buildTelemetryHopsDescriptor(record) || '';
  const hopStart = Number.isFinite(record.hops?.start) ? record.hops.start : '';
  const hopLimit = Number.isFinite(record.hops?.limit) ? record.hops.limit : '';

  const row = [
    escapeCsvValue(isoTime),
    escapeCsvValue(unixSeconds),
    escapeCsvValue(meshId),
    escapeCsvValue(nodeLabel),
    escapeCsvValue(channelValue),
    escapeCsvValue(snrValue),
    escapeCsvValue(rssiValue),
    escapeCsvValue(detailValue),
    escapeCsvValue(relayDescriptor),
    escapeCsvValue(relayMeshValue),
    escapeCsvValue(relayGuessFlag),
    escapeCsvValue(relayReason),
    escapeCsvValue(hopsDescriptor),
    escapeCsvValue(hopStart),
    escapeCsvValue(hopLimit)
  ];

  const metrics = record.telemetry?.metrics || {};
  for (const key of metricKeysInUse) {
    const raw = metrics[key];
    const formatted = raw == null ? '' : formatTelemetryValue(key, raw);
    row.push(escapeCsvValue(formatted));
  }

  const flatMetrics = new Map(flattenTelemetryMetrics(metrics));
  for (const key of extraMetricKeys) {
    let value = flatMetrics.has(key) ? flatMetrics.get(key) : '';
    if (typeof value === 'number') {
      value = trimTrailingZeros(value.toFixed(2));
    } else if (typeof value === 'boolean') {
      value = value ? 'true' : 'false';
    }
    row.push(escapeCsvValue(value));
  }

  return `${row.join(',')}\n`;
}

function recordMatchesSearch(record, searchTerm) {
  if (!searchTerm) return true;
  const haystack = collectSearchTokens(record);
  if (!haystack.length) {
    return false;
  }
  return haystack.some((value) => {
    if (value == null) return false;
    return String(value).toLowerCase().includes(searchTerm);
  });
}

function collectSearchTokens(record) {
  if (!record) return [];
  const tokens = [];
  const node = record.node || {};
  tokens.push(
    node.label,
    node.longName,
    node.shortName,
    node.hwModelLabel,
    node.roleLabel,
    record.meshId,
    node.meshId,
    node.meshIdOriginal,
    node.meshIdNormalized
  );
  const relay = record.relay || {};
  tokens.push(
    record.relayLabel,
    record.relayMeshId,
    record.relayMeshIdNormalized,
    relay.label,
    relay.longName,
    relay.shortName
  );
  if (record.relayGuessReason) {
    tokens.push(record.relayGuessReason);
  }
  if (record.detail) tokens.push(record.detail);
  if (record.channel != null) tokens.push(`ch ${record.channel}`);
  if (Number.isFinite(record.snr)) tokens.push(`snr ${record.snr}`);
  if (Number.isFinite(record.rssi)) tokens.push(`rssi ${record.rssi}`);
  if (record.hopsLabel) tokens.push(record.hopsLabel);
  if (Number.isFinite(record.hopsUsed)) tokens.push(`hops ${record.hopsUsed}`);
  if (Number.isFinite(record.hopsTotal)) tokens.push(`hops ${record.hopsUsed ?? ''}/${record.hopsTotal}`);
  if (record.relayGuessReason) tokens.push(record.relayGuessReason);
  const metrics = record.telemetry?.metrics || {};
  for (const [path, value] of flattenTelemetryMetrics(metrics)) {
    tokens.push(path);
    tokens.push(value);
  }
  return tokens;
}
function sanitizeCallmeshProvision(provision) {
  if (!provision || typeof provision !== 'object') {
    return null;
  }
  const pickString = (...keys) => {
    for (const key of keys) {
      if (Object.prototype.hasOwnProperty.call(provision, key)) {
        const value = provision[key];
        if (value !== undefined && value !== null) {
          const text = String(value).trim();
          if (text) {
            return text;
          }
        }
      }
    }
    return null;
  };
  const pickNumber = (...keys) => {
    for (const key of keys) {
      if (Object.prototype.hasOwnProperty.call(provision, key)) {
        const value = Number(provision[key]);
        if (Number.isFinite(value)) {
          return value;
        }
      }
    }
    return null;
  };

  return {
    callsign: pickString('callsign', 'callsign_base', 'callsignBase'),
    callsign_base: pickString('callsign_base', 'callsignBase'),
    callsign_with_ssid: pickString('callsign_with_ssid', 'callsignWithSsid'),
    aprs_callsign: pickString('aprs_callsign', 'aprsCallsign'),
    aprs_ssid: pickString('aprs_ssid', 'aprsSsid', 'ssid'),
    symbol_table: pickString('symbol_table', 'symbolTable'),
    symbol_overlay: pickString('symbol_overlay', 'symbolOverlay'),
    symbol_code: pickString('symbol_code', 'symbolCode'),
    latitude: pickNumber('latitude', 'lat'),
    longitude: pickNumber('longitude', 'lon'),
    phg: pickString('phg'),
    comment: pickString('comment', 'provision_comment', 'notes', 'description')
  };
}

function sanitizeCallmeshMappingItem(item) {
  if (!item || typeof item !== 'object') {
    return null;
  }
  const meshId = item.mesh_id ?? item.meshId ?? null;
  const callsignBase =
    item.callsign_base ??
    item.callsignBase ??
    item.callsign ??
    null;
  const ssidRaw = item.aprs_ssid ?? item.aprsSsid ?? item.ssid ?? null;
  const ssidNumeric = Number(ssidRaw);
  return {
    mesh_id: meshId != null ? String(meshId) : null,
    callsign_base: callsignBase != null ? String(callsignBase) : null,
    ssid: Number.isFinite(ssidNumeric) ? ssidNumeric : (ssidRaw != null ? ssidRaw : null),
    symbol_table: item.symbol_table ?? item.symbolTable ?? null,
    symbol_code: item.symbol_code ?? item.symbolCode ?? null,
    symbol_overlay: item.symbol_overlay ?? item.symbolOverlay ?? null,
    comment: item.comment ?? null,
    enabled: item.enabled !== false
  };
}

function sanitizeCallmeshAprs(aprs) {
  if (!aprs || typeof aprs !== 'object') {
    return {
      connected: false,
      actualServer: null,
      server: null,
      callsign: null,
      beaconIntervalMs: null
    };
  }
  const beaconMs = Number(aprs.beaconIntervalMs);
  return {
    connected: Boolean(aprs.connected),
    actualServer: aprs.actualServer ?? null,
    server: aprs.server ?? null,
    callsign:
      typeof aprs.callsign === 'string' && aprs.callsign.trim()
        ? aprs.callsign.trim()
        : null,
    beaconIntervalMs: Number.isFinite(beaconMs) ? beaconMs : null
  };
}

function sanitizeCallmeshPayload(info) {
  if (!info || typeof info !== 'object') {
    return null;
  }
  const mappingItems = Array.isArray(info.mappingItems)
    ? info.mappingItems
        .map((item) => sanitizeCallmeshMappingItem(item))
        .filter(Boolean)
    : [];
  return {
    statusText: info.statusText ?? '',
    hasKey: Boolean(info.hasKey),
    verified: Boolean(info.verified),
    degraded: Boolean(info.degraded),
    lastHeartbeatAt: info.lastHeartbeatAt ?? null,
    lastMappingSyncedAt: info.lastMappingSyncedAt ?? null,
    provision: sanitizeCallmeshProvision(info.provision),
    mappingItems,
    aprs: sanitizeCallmeshAprs(info.aprs)
  };
}
