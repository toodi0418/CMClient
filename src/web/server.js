'use strict';

const http = require('http');
const path = require('path');
const fs = require('fs');

const DEFAULT_PORT = Number(process.env.TMAG_WEB_PORT) || 7080;
const DEFAULT_HOST = process.env.TMAG_WEB_HOST || '0.0.0.0';
const PACKET_WINDOW_MS = 10 * 60 * 1000;
const PACKET_BUCKET_MS = 60 * 1000;
const MAX_SUMMARY_ROWS = 200;
const MAX_LOG_ENTRIES = 200;
const APRS_HISTORY_MAX = 5000;
const DEFAULT_TELEMETRY_MAX_PER_NODE = 500;

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
    role: node.role ?? null
  };
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
    this.lastAppInfo = this.appVersion ? { version: this.appVersion } : null;
    this.telemetryMaxPerNode =
      Number.isFinite(options.telemetryMaxPerNode) && options.telemetryMaxPerNode > 0
        ? Math.floor(options.telemetryMaxPerNode)
        : DEFAULT_TELEMETRY_MAX_PER_NODE;
    this.telemetryStore = new Map();
    this.telemetryRecordIds = new Set();
    this.telemetryUpdatedAt = null;
  }

  async start() {
    if (this.server) {
      return;
    }

    this.server = http.createServer((req, res) => {
      if (req.url === '/api/events') {
        this._handleEventStream(req, res);
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
    const payload = this.selfMeshId ? { ...summary, selfMeshId: this.selfMeshId } : summary;
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
    this.lastAprsInfo = {
      ...info,
      timestamp: info.timestamp ?? Date.now()
    };
    this._broadcast({ type: 'aprs', payload: this.lastAprsInfo });
    this._updateAprsMetrics(info);
    this._broadcastMetrics();
  }

  publishTelemetry(payload) {
    if (!payload || !payload.type) {
      return;
    }
    if (payload.type === 'reset') {
      this.telemetryStore.clear();
      this.telemetryRecordIds.clear();
      this.telemetryUpdatedAt =
        Number.isFinite(payload.updatedAt) && payload.updatedAt > 0
          ? Number(payload.updatedAt)
          : Date.now();
      this._broadcast({
        type: 'telemetry-reset',
        payload: { updatedAt: this.telemetryUpdatedAt }
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
    this._broadcast({
      type: 'telemetry-append',
      payload: {
        meshId,
        node: nodeInfo,
        record,
        updatedAt: this.telemetryUpdatedAt
      }
    });
  }

  seedTelemetrySnapshot(snapshot = {}) {
    this.telemetryStore.clear();
    this.telemetryRecordIds.clear();

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

  _appendSummary(summary) {
    this.summaryRows.unshift(summary);
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
    if (this.lastAprsInfo) {
      this._write(res, { type: 'aprs', payload: this.lastAprsInfo });
    }
    this._write(res, {
      type: 'telemetry-snapshot',
      payload: this._buildTelemetrySnapshot()
    });
  }

  _appendTelemetryRecord(meshId, node, record) {
    if (!meshId || !record) {
      return;
    }
    const key = String(meshId);
    let bucket = this.telemetryStore.get(key);
    if (!bucket) {
      bucket = {
        meshId: key,
        node: node ? sanitizeTelemetryNode(node) : null,
        records: []
      };
      this.telemetryStore.set(key, bucket);
    } else if (node) {
      const sanitizedNode = sanitizeTelemetryNode(node);
      if (sanitizedNode) {
        bucket.node = {
          ...(bucket.node || {}),
          ...sanitizedNode
        };
      }
    }
    if (record.node) {
      const recordNode = sanitizeTelemetryNode(record.node);
      if (recordNode) {
        bucket.node = {
          ...(bucket.node || {}),
          ...recordNode
        };
      }
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
        }
      }
    }
  }

  _buildTelemetrySnapshot(limitPerNode = this.telemetryMaxPerNode) {
    const nodes = [];
    for (const bucket of this.telemetryStore.values()) {
      if (!bucket || !Array.isArray(bucket.records) || !bucket.records.length) {
        continue;
      }
      const records = bucket.records;
      const limit =
        Number.isFinite(limitPerNode) && limitPerNode > 0
          ? Math.floor(limitPerNode)
          : records.length;
      const start = records.length > limit ? records.length - limit : 0;
      const slice = records.slice(start).map((record) => cloneJson(record));
      nodes.push({
        meshId: bucket.meshId,
        node: bucket.node ? cloneJson(bucket.node) : null,
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
      nodes
    };
  }

  _broadcast(event) {
    if (!event) return;
    for (const client of this.clients) {
      this._write(client, event);
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
