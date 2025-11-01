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

class WebDashboardServer {
  constructor(options = {}) {
    this.port = options.port ?? DEFAULT_PORT;
    this.host = options.host ?? DEFAULT_HOST;
    this.inactivityPingMs = options.inactivityPingMs ?? 15_000;

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
    this.lastCallmesh = info;
    if (Array.isArray(info.mappingItems)) {
      const unique = new Set(
        info.mappingItems
          .map((item) => normalizeMeshId(item?.mesh_id ?? item?.meshId))
          .filter(Boolean)
      );
      this.metrics.mappingCount = unique.size;
    } else {
      this.metrics.mappingCount = 0;
    }
    this._broadcast({ type: 'callmesh', payload: info });
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

  publishSelf(info) {
    if (!info) return;
    const meshCandidate = info?.node?.meshId || info?.meshId || null;
    if (meshCandidate) {
      this.selfMeshId = meshCandidate;
      this._broadcast({ type: 'self', payload: { meshId: meshCandidate } });
    }
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
