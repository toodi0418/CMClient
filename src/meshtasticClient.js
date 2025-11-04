'use strict';

const { EventEmitter } = require('events');
const net = require('net');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const fsPromises = require('fs/promises');
const protobuf = require('protobufjs');

const { unishox2_decompress_simple } = require('unishox2.siara.cc');

const MAGIC = 0x94c3;
const HEADER_SIZE = 4;
const DEFAULT_MAX_PACKET = 512;
const BROADCAST_ADDR = 0xffffffff;
const RELAY_GUESS_EXPLANATION =
  '最後轉發節點由 SNR/RSSI 推測（韌體僅提供節點尾碼），結果可能不完全準確。';

function removeSocketListener(socket, eventName, handler) {
  if (!socket || !handler) {
    return;
  }
  if (typeof socket.off === 'function') {
    socket.off(eventName, handler);
  } else {
    socket.removeListener(eventName, handler);
  }
}

const TO_OBJECT_OPTIONS = {
  longs: Number,
  enums: String,
  bytes: Buffer,
  defaults: false,
  oneofs: true
};

class MeshtasticClient extends EventEmitter {
  constructor(options = {}) {
    super();
    this.options = {
      host: '127.0.0.1',
      port: 4403,
      maxLength: DEFAULT_MAX_PACKET,
      handshake: true,
      heartbeat: 0,
      keepAlive: true,
      keepAliveDelayMs: 15000,
      idleTimeoutMs: 0,
      protoDir: path.resolve(__dirname, '..', 'proto'),
      ...options
    };

    this.nodeMap = new Map();
    this._relayLinkStats = new Map();
    this._relayStatsPath = options.relayStatsPath ? path.resolve(options.relayStatsPath) : null;
    this._relayStatsPersistIntervalMs = Number.isFinite(options.relayStatsPersistIntervalMs)
      ? Math.max(Number(options.relayStatsPersistIntervalMs), 1000)
      : 30_000;
    this._relayStatsPersistTimer = null;
    this._relayStatsPersisting = false;
    this._relayStatsDirty = false;
    this._decoder = null;
    this._socket = null;
    this._heartbeatTimer = null;
    this._connected = false;
    this._seenPacketKeys = new Map();
    this._packetKeyQueue = [];
    this._handleIdleTimeoutBound = null;
    this._currentIdleTimeout = null;
    this._socketClosed = true;

    this._loadRelayStatsFromDisk();
  }

  _normalizeRelayNode(relayNode, { snr = null, rssi = null } = {}) {
    const raw = Number(relayNode) >>> 0;
    // the firmware sets relay_node to the full node id, but in some cases only
    // the low byte is populated (e.g. 0x24 for node ending with 0x24).
    if (raw === 0) return { nodeId: 0, guessed: false };
    if (raw > 0xff) {
      this._recordRelayLinkMetrics(raw, { snr, rssi });
      return { nodeId: raw >>> 0, guessed: false };
    }
    const matches = new Set();
    for (const [num, entry] of this.nodeMap.entries()) {
      const numeric = Number(num) >>> 0;
      if ((numeric & 0xff) === raw && !matches.has(numeric)) {
        matches.add(numeric);
      }
      const idStr = typeof entry?.id === 'string' ? entry.id : null;
      if (idStr) {
        const cleaned = idStr.replace(/[^0-9a-fA-F]/g, '');
        if (cleaned.length === 8) {
          const parsed = parseInt(cleaned, 16) >>> 0;
          if ((parsed & 0xff) === raw && !matches.has(parsed)) {
            matches.add(parsed);
          }
        }
      }
    }
    if (matches.size === 1) {
      const [match] = matches;
      this._recordRelayLinkMetrics(match, { snr, rssi });
      return { nodeId: match >>> 0, guessed: false };
    }
    const bestGuess = this._guessRelayCandidate(Array.from(matches), { snr, rssi });
    if (bestGuess != null) {
      return { nodeId: bestGuess >>> 0, guessed: true };
    }
    return { nodeId: raw, guessed: false };
  }

  _recordRelayLinkMetrics(nodeId, { snr = null, rssi = null } = {}) {
    if (nodeId == null) return;
    const numeric = Number(nodeId);
    if (!Number.isFinite(numeric) || numeric <= 0) {
      return;
    }
    const toNumber = (value) => {
      if (value === null || value === undefined) return null;
      const num = Number(value);
      return Number.isFinite(num) ? num : null;
    };
    const snrValue = toNumber(snr);
    const rssiValue = toNumber(rssi);
    if (snrValue === null && rssiValue === null) {
      return;
    }
    const key = numeric >>> 0;
    const now = Date.now();
    const alpha = 0.25;
    let stats = this._relayLinkStats.get(key);
    if (!stats) {
      stats = {
        snr: snrValue,
        rssi: rssiValue,
        count: 1,
        updatedAt: now
      };
      this._relayLinkStats.set(key, stats);
      return;
    }
    if (snrValue !== null) {
      stats.snr = stats.snr == null ? snrValue : stats.snr + alpha * (snrValue - stats.snr);
    }
    if (rssiValue !== null) {
      stats.rssi = stats.rssi == null ? rssiValue : stats.rssi + alpha * (rssiValue - stats.rssi);
    }
    stats.count = (stats.count || 0) + 1;
    stats.updatedAt = now;
    this._scheduleRelayStatsPersist();
  }

  _guessRelayCandidate(candidates, { snr = null, rssi = null } = {}) {
    if (!Array.isArray(candidates) || candidates.length === 0) {
      return null;
    }
    const toNumber = (value) => {
      if (value === null || value === undefined) return null;
      const num = Number(value);
      return Number.isFinite(num) ? num : null;
    };
    const snrValue = toNumber(snr);
    const rssiValue = toNumber(rssi);
    if (snrValue === null && rssiValue === null) {
      return null;
    }
    const now = Date.now();
    let bestCandidate = null;
    let bestScore = Infinity;
    let bestStats = null;
    for (const candidate of candidates) {
      const stats = this._relayLinkStats.get(candidate >>> 0);
      if (!stats) continue;
      let score = 0;
      let components = 0;
      if (snrValue !== null && stats.snr != null) {
        score += Math.abs(snrValue - stats.snr);
        components += 1;
      }
      if (rssiValue !== null && stats.rssi != null) {
        // scale RSSI diff down so it doesn't dominate SNR
        score += Math.abs(rssiValue - stats.rssi) * 0.1;
        components += 1;
      }
      if (components === 0) continue;
      let normalizedScore = score / components;
      const ageMs = now - (stats.updatedAt || 0);
      if (Number.isFinite(ageMs) && ageMs > 0) {
        normalizedScore += Math.min(ageMs / 600000, 5); // +1 per 10 分鐘，最多 +5
      }
      const sampleBonus = Math.min(stats.count || 0, 10) * 0.05;
      normalizedScore -= sampleBonus;
      if (
        normalizedScore < bestScore - 0.05 ||
        (Math.abs(normalizedScore - bestScore) <= 0.05 &&
          bestStats &&
          ((stats.count || 0) > (bestStats.count || 0) || (stats.updatedAt || 0) > (bestStats.updatedAt || 0)))
      ) {
        bestCandidate = candidate >>> 0;
        bestScore = normalizedScore;
        bestStats = stats;
      }
    }
    return bestCandidate;
  }

  _loadRelayStatsFromDisk() {
    if (!this._relayStatsPath) {
      return;
    }
    try {
      const raw = fs.readFileSync(this._relayStatsPath, 'utf8');
      if (!raw) {
        return;
      }
      const data = JSON.parse(raw);
      if (!data || typeof data !== 'object') {
        return;
      }
      this._relayLinkStats.clear();
      const now = Date.now();
      for (const [key, value] of Object.entries(data)) {
        if (!value || typeof value !== 'object') continue;
        const numericKey = Number(key);
        if (!Number.isFinite(numericKey) || numericKey <= 0) continue;
        const entry = {
          snr: Number.isFinite(value.snr) ? Number(value.snr) : null,
          rssi: Number.isFinite(value.rssi) ? Number(value.rssi) : null,
          count: Number.isFinite(value.count) ? Math.max(1, Number(value.count)) : 1,
          updatedAt: Number.isFinite(value.updatedAt) ? Number(value.updatedAt) : now
        };
        this._relayLinkStats.set(numericKey >>> 0, entry);
      }
    } catch (err) {
      if (err?.code !== 'ENOENT') {
        console.warn(`載入 relay link stats 失敗: ${err.message}`);
      }
    }
  }

  _scheduleRelayStatsPersist() {
    if (!this._relayStatsPath) {
      return;
    }
    this._relayStatsDirty = true;
    if (this._relayStatsPersistTimer) {
      return;
    }
    this._relayStatsPersistTimer = setTimeout(() => {
      this._relayStatsPersistTimer = null;
      this._persistRelayStats().catch((err) => {
        console.error(`持久化 relay link stats 失敗: ${err.message}`);
        this._scheduleRelayStatsPersist();
      });
    }, this._relayStatsPersistIntervalMs);
    this._relayStatsPersistTimer.unref?.();
  }

  async _persistRelayStats() {
    if (!this._relayStatsPath || !this._relayStatsDirty || this._relayStatsPersisting) {
      return;
    }
    this._relayStatsPersisting = true;
    const payload = {};
    for (const [key, stats] of this._relayLinkStats.entries()) {
      payload[key] = {
        snr: Number.isFinite(stats.snr) ? Number(stats.snr) : null,
        rssi: Number.isFinite(stats.rssi) ? Number(stats.rssi) : null,
        count: Number.isFinite(stats.count) ? Math.max(1, Math.round(Number(stats.count))) : 1,
        updatedAt: Number.isFinite(stats.updatedAt) ? Number(stats.updatedAt) : Date.now()
      };
    }
    try {
      await fsPromises.mkdir(path.dirname(this._relayStatsPath), { recursive: true });
      if (Object.keys(payload).length === 0) {
        try {
          await fsPromises.unlink(this._relayStatsPath);
        } catch (err) {
          if (err?.code !== 'ENOENT') {
            throw err;
          }
        }
      } else {
        await fsPromises.writeFile(this._relayStatsPath, JSON.stringify(payload, null, 2), 'utf8');
      }
      this._relayStatsDirty = false;
    } catch (err) {
      this._relayStatsDirty = true;
      throw err;
    } finally {
      this._relayStatsPersisting = false;
    }
  }

  _flushRelayStatsPersistSync() {
    if (!this._relayStatsPath) {
      return;
    }
    if (this._relayStatsPersistTimer) {
      clearTimeout(this._relayStatsPersistTimer);
      this._relayStatsPersistTimer = null;
    }
    if (!this._relayStatsDirty || this._relayStatsPersisting) {
      return;
    }
    const payload = {};
    for (const [key, stats] of this._relayLinkStats.entries()) {
      payload[key] = {
        snr: Number.isFinite(stats.snr) ? Number(stats.snr) : null,
        rssi: Number.isFinite(stats.rssi) ? Number(stats.rssi) : null,
        count: Number.isFinite(stats.count) ? Math.max(1, Math.round(Number(stats.count))) : 1,
        updatedAt: Number.isFinite(stats.updatedAt) ? Number(stats.updatedAt) : Date.now()
      };
    }
    try {
      fs.mkdirSync(path.dirname(this._relayStatsPath), { recursive: true });
      if (Object.keys(payload).length === 0) {
        try {
          fs.unlinkSync(this._relayStatsPath);
        } catch (err) {
          if (err?.code !== 'ENOENT') {
            throw err;
          }
        }
      } else {
        fs.writeFileSync(this._relayStatsPath, JSON.stringify(payload, null, 2), 'utf8');
      }
      this._relayStatsDirty = false;
    } catch (err) {
      console.error(`同步寫入 relay link stats 失敗: ${err.message}`);
    }
  }

  async start() {
    if (!this.root) {
      await this._loadProtobufs();
    }
    this._connect();
  }

  stop() {
    this._clearHeartbeat();
    this._flushRelayStatsPersistSync();
    if (this._socket) {
      if (this._handleIdleTimeoutBound) {
        removeSocketListener(this._socket, 'timeout', this._handleIdleTimeoutBound);
      }
      try {
        this._socket.setKeepAlive(false);
      } catch {
        // ignore errors disabling keepalive during shutdown
      }
      this._socket.destroy();
      this._socket.removeAllListeners();
      this._socket = null;
    }
    this._socketClosed = true;
    this._currentIdleTimeout = null;
    this._connected = false;
    this._connectionStartedAt = null;
    this._seenPacketKeys.clear();
    this._packetKeyQueue.length = 0;
  }

  toObject(message, overrides = {}) {
    if (!this.fromRadioType) {
      throw new Error('Protobuf schema not loaded yet');
    }
    return this.fromRadioType.toObject(message, {
      ...TO_OBJECT_OPTIONS,
      ...overrides
    });
  }

  async _loadProtobufs() {
    const protoFiles = [
      'meshtastic/mesh.proto',
      'meshtastic/admin.proto',
      'meshtastic/remote_hardware.proto',
      'meshtastic/storeforward.proto',
      'meshtastic/paxcount.proto'
    ].map((file) => path.resolve(this.options.protoDir, file));
    const root = new protobuf.Root();
    root.resolvePath = (origin, target) => {
      const candidate = path.resolve(this.options.protoDir, target);
      if (fs.existsSync(candidate)) {
        return candidate;
      }
      if (origin) {
        const alt = path.resolve(path.dirname(origin), target);
        if (fs.existsSync(alt)) {
          return alt;
        }
      }
      return protobuf.util.path.resolve(origin, target);
    };

    await root.load(protoFiles);

    this.root = root;
    this.fromRadioType = root.lookupType('meshtastic.FromRadio');
    this.toRadioType = root.lookupType('meshtastic.ToRadio');
    this.portEnum = root.lookupEnum('meshtastic.PortNum');
    this.types = {
      position: root.lookupType('meshtastic.Position'),
      routing: root.lookupType('meshtastic.Routing'),
      telemetry: root.lookupType('meshtastic.Telemetry'),
      user: root.lookupType('meshtastic.User'),
      waypoint: root.lookupType('meshtastic.Waypoint'),
      admin: root.lookupType('meshtastic.AdminMessage'),
      keyVerification: root.lookupType('meshtastic.KeyVerification'),
      remoteHardware: root.lookupType('meshtastic.HardwareMessage'),
      storeForward: root.lookupType('meshtastic.StoreAndForward'),
      paxcount: root.lookupType('meshtastic.Paxcount'),
      neighborInfo: root.lookupType('meshtastic.NeighborInfo'),
      routeDiscovery: root.lookupType('meshtastic.RouteDiscovery')
    };
  }

  _connect() {
    this._decoder = new MeshtasticStreamDecoder({
      maxPacketLength: this.options.maxLength,
      onPacket: (payload) => this._handlePayload(payload),
      onError: (err) => this.emit('error', err)
    });

    const connectionOptions = {
      host: this.options.host,
      port: this.options.port,
      timeout: this.options.connectTimeout ?? 15000
    };
    const connectTimeoutMs = this.options.connectTimeout ?? 15000;
    const handleConnectTimeout = () => {
      const timeoutError = new Error(`connect timeout after ${connectTimeoutMs}ms`);
      if (this._socket) {
        this._socket.destroy(timeoutError);
      } else {
        this.emit('error', timeoutError);
      }
    };

    try {
      this._socketClosed = false;
      this._socket = net.createConnection(connectionOptions, () => {
        this._connected = true;
        this._connectionStartedAt = Date.now();
        if (this._socket) {
          removeSocketListener(this._socket, 'timeout', handleConnectTimeout);
          this._socket.setTimeout(0);
          this._applySocketOptions();
        }
        this.emit('connected');
        if (this.options.handshake) {
          this._sendWantConfig();
        }
        this._setupHeartbeat();
      });
    } catch (err) {
      this._socketClosed = true;
      process.nextTick(() => this.emit('error', err));
      return;
    }

    this._socket.on('data', (chunk) => this._decoder.push(chunk));
    this._socket.setTimeout(connectTimeoutMs);
    this._socket.once('timeout', handleConnectTimeout);

    this._socket.on('error', (err) => {
      this.emit('error', err);
    });

    this._socket.on('end', () => {
      this._handleSocketClosed();
    });

    this._socket.on('close', () => {
      this._handleSocketClosed();
    });
  }

  _applySocketOptions() {
    this._enableTcpKeepAlive();
    this._configureSocketIdleTimeout();
  }

  _enableTcpKeepAlive() {
    if (!this._socket || typeof this._socket.setKeepAlive !== 'function') {
      return;
    }
    if (this.options.keepAlive === false) {
      try {
        this._socket.setKeepAlive(false);
      } catch {
        // ignore keepalive configuration errors
      }
      return;
    }
    const delayValue = Number(this.options.keepAliveDelayMs);
    try {
      if (Number.isFinite(delayValue) && delayValue >= 0) {
        const delay = Math.max(0, Math.floor(delayValue));
        this._socket.setKeepAlive(true, delay);
      } else {
        this._socket.setKeepAlive(true);
      }
    } catch {
      // ignore keepalive configuration errors
    }
  }

  _configureSocketIdleTimeout() {
    if (!this._socket) {
      return;
    }
    const idleTimeoutMs = Number(this.options.idleTimeoutMs);
    if (!Number.isFinite(idleTimeoutMs) || idleTimeoutMs <= 0) {
      if (this._handleIdleTimeoutBound) {
        removeSocketListener(this._socket, 'timeout', this._handleIdleTimeoutBound);
      }
      this._socket.setTimeout(0);
      this._currentIdleTimeout = null;
      return;
    }
    if (!this._handleIdleTimeoutBound) {
      this._handleIdleTimeoutBound = () => this._handleIdleTimeout();
    }
    this._currentIdleTimeout = idleTimeoutMs;
    removeSocketListener(this._socket, 'timeout', this._handleIdleTimeoutBound);
    this._socket.setTimeout(idleTimeoutMs);
    this._socket.on('timeout', this._handleIdleTimeoutBound);
  }

  _handleIdleTimeout() {
    const timeoutMs = this._currentIdleTimeout;
    const message =
      timeoutMs && Number.isFinite(timeoutMs)
        ? `connection idle timeout after ${timeoutMs}ms`
        : 'connection idle timeout';
    const timeoutError = new Error(message);
    timeoutError.code = 'MESHTASTIC_IDLE_TIMEOUT';
    if (this._socket) {
      this._socket.destroy(timeoutError);
    } else {
      this.emit('error', timeoutError);
      this._handleSocketClosed();
    }
  }

  _handleSocketClosed() {
    if (this._socketClosed) {
      return;
    }
    this._socketClosed = true;
    this._connected = false;
    this._connectionStartedAt = null;
    if (this._socket && this._handleIdleTimeoutBound) {
      removeSocketListener(this._socket, 'timeout', this._handleIdleTimeoutBound);
    }
    this._currentIdleTimeout = null;
    this._clearHeartbeat();
    this.emit('disconnected');
  }

  _handlePayload(payload) {
    let message;
    try {
      message = this.fromRadioType.decode(payload);
    } catch (err) {
      this.emit('error', new Error(`FromRadio 解碼失敗: ${err.message}`));
      return;
    }

    this._updateNodeCache(message);

    const summary = this._buildSummary(message);
    this.emit('fromRadio', {
      message,
      summary,
      rawPayload: payload
    });
    if (summary) {
      this.emit('summary', summary);
    }
  }

  _updateNodeCache(message) {
    switch (message.payloadVariant) {
      case 'nodeInfo': {
        const info = message.nodeInfo;
        if (!info) break;
        const num = info.num >>> 0;
        const user = info.user || {};
        const existing = this.nodeMap.get(num) || {};
        const updated = {
          ...existing,
          id: user.id || existing.id,
          shortName: user.shortName || existing.shortName,
          longName: user.longName || existing.longName,
          hwModel: user.hwModel || existing.hwModel,
          role: user.role || existing.role
        };
    this.nodeMap.set(num, updated);
    break;
  }
  case 'myInfo': {
    const info = message.myInfo;
    if (!info) break;
    const num = info.myNodeNum >>> 0;
    const existing = this.nodeMap.get(num) || {};
    this.nodeMap.set(num, {
      id: existing.id || formatHexId(num),
      shortName: existing.shortName,
      longName: existing.longName,
      hwModel: existing.hwModel,
      role: existing.role
    });
    const nodeInfo = this._formatNode(num);
    this.emit('myInfo', {
      raw: num,
      node: nodeInfo
    });
    break;
  }
      default:
        break;
    }
  }

  _buildSummary(message) {
    if (!message || message.payloadVariant !== 'packet') {
      return null;
    }

    const packet = message.packet;
    if (!packet || packet.payloadVariant !== 'decoded') {
      return null;
    }
    if (this._connectionStartedAt && packet.rxTime) {
      const packetTime = packet.rxTime * 1000;
      if (packetTime <= this._connectionStartedAt) {
        return null;
      }
    }

    const portInfo = this._resolvePortnum(packet.decoded.portnum);
    const payload = packet.decoded.payload;
    const decodeInfo = this._decodePortPayload(portInfo.name, payload);
    const extraLines = Array.isArray(decodeInfo?.extraLines)
      ? [...decodeInfo.extraLines]
      : [];
    const relayNodeId = packet.relayNode != null && packet.relayNode !== 0 ? packet.relayNode : null;
    const nextHopId = packet.nextHop != null && packet.nextHop !== 0 ? packet.nextHop : null;

    if (!this._shouldEmitPacket(packet)) {
      return null;
    }

    const timestamp = packet.rxTime ? new Date(packet.rxTime * 1000) : new Date();
    const fromInfo = this._formatNode(packet.from);
    const toInfo = packet.to === BROADCAST_ADDR ? null : this._formatNode(packet.to);
    const linkMetrics = {
      snr: Number.isFinite(packet.rxSnr) ? Number(packet.rxSnr) : null,
      rssi: Number.isFinite(packet.rxRssi) ? Number(packet.rxRssi) : null
    };
    let relayResult = null;
    if (relayNodeId != null) {
      relayResult = this._normalizeRelayNode(relayNodeId, linkMetrics);
    } else if (packet.from != null) {
      this._recordRelayLinkMetrics(packet.from, linkMetrics);
    }
    const relayInfo =
      relayNodeId != null && relayResult ? this._formatNode(relayResult.nodeId) : null;
    if (relayInfo && relayResult) {
      relayInfo.guessed = Boolean(relayResult.guessed);
    }
    const nextHopInfo = nextHopId != null ? this._formatNode(nextHopId) : null;

    if (relayInfo && relayInfo.meshId) {
      // relay info exposed via summary.relay for UI display
    }
    if (nextHopInfo && nextHopInfo.meshId) {
      // next hop info exposed via summary.nextHop for UI display
    }

    const rawHex =
      Buffer.isBuffer(payload) && payload.length > 0 ? payload.toString('hex') : null;

    const summary = {
      timestamp: timestamp.toISOString(),
      timestampLabel: formatTimestamp(timestamp),
      channel: packet.channel ?? 0,
      snr: packet.rxSnr ?? null,
      rssi: packet.rxRssi ?? null,
      hops: {
        limit: packet.hopLimit ?? null,
        start: packet.hopStart ?? null,
        label: formatHops(packet.hopLimit, packet.hopStart)
      },
      type: decodeInfo?.type || friendlyPortLabel(portInfo.name, portInfo.id),
      detail: decodeInfo?.details || '',
      extraLines,
      port: portInfo,
      from: fromInfo,
      to: toInfo,
      relay: relayInfo,
      relayGuess: Boolean(relayResult?.guessed),
      relayGuessReason: relayResult?.guessed ? RELAY_GUESS_EXPLANATION : undefined,
      nextHop: nextHopInfo,
      position: decodeInfo?.position || null,
      telemetry: decodeInfo?.telemetry || null,
      rawHex,
      rawLength: Buffer.isBuffer(payload) ? payload.length : 0
    };

    return summary;
  }

  _decodePortPayload(portName, payload) {
    if (!portName || !Buffer.isBuffer(payload) || payload.length === 0) {
      return null;
    }

    try {
      if (TEXT_PORTS.has(portName)) {
        return decodeTextPayload(payload, false);
      }

      if (COMPRESSED_TEXT_PORTS.has(portName)) {
        return decodeTextPayload(payload, true);
      }

      switch (portName) {
        case 'POSITION_APP': {
          const message = this.types.position.decode(payload);
          const position = this.types.position.toObject(message, TO_OBJECT_OPTIONS);
          const lat = extractCoordinate(position.latitudeI, position.latitude);
          const lon = extractCoordinate(position.longitudeI, position.longitude);
          const alt =
            position.altitude ?? position.altitudeHae ?? position.altitudeGeoidalSeparation;
          const course =
            position.bearing ?? position.course ?? position.heading ?? position.velHeading;
          const speedMps =
            position.groundSpeed ?? position.speed ?? position.airSpeed ?? position.velHoriz;
          const speedKph =
            position.speedKph ?? (Number.isFinite(speedMps) ? speedMps * 3.6 : null);
          const speedKnots =
            position.speedKnots ??
            (Number.isFinite(speedMps) ? speedMps * 1.943844 : null) ??
            (Number.isFinite(speedKph) ? speedKph / 1.852 : null);
          const coordStr =
            lat != null && lon != null ? `(${lat.toFixed(6)}, ${lon.toFixed(6)})` : '';
          const altStr = Number.isFinite(alt) ? `ALT ${Math.round(alt)}m` : '';
          const detail = [coordStr, altStr].filter(Boolean).join(' · ');
          const extraLines = [];
          const courseInt = Number.isFinite(course)
            ? Math.round(((course % 360) + 360) % 360)
            : null;
          if (courseInt !== null) {
            extraLines.push(`航向 ${courseInt}°`);
          }
          // speed & vertical speed handled in UI chips, skip textual extra lines
          // satsInView handled in UI chips
          if (Number.isFinite(position.seqNumber)) {
            extraLines.push(`序列編號 #${position.seqNumber}`);
          }
          const timestampSeconds = Number.isFinite(position.timestamp) ? position.timestamp : null;
          const timestampAdjustMs = Number.isFinite(position.timestampMillisAdjust)
            ? position.timestampMillisAdjust
            : 0;
          if (timestampSeconds !== null) {
            // timestamp available but no longer displayed in extra lines
          }
          return {
            type: 'Position',
            details: detail,
            extraLines,
            position: {
              latitude: lat ?? null,
              longitude: lon ?? null,
              altitude: alt ?? null,
              course: course ?? null,
              heading: position.heading ?? null,
              speedMps: Number.isFinite(speedMps) ? speedMps : null,
              speedKph: Number.isFinite(speedKph) ? speedKph : null,
              speedKnots: Number.isFinite(speedKnots) ? speedKnots : null,
              velocityHoriz: position.velHoriz ?? null,
              velocityVert: position.velVert ?? null,
              velHeading: position.velHeading ?? null,
              precisionBits: Number.isFinite(position.precisionBits) ? position.precisionBits : null,
              locationSource: position.locationSource ?? null,
              satsInView: Number.isFinite(position.satsInView) ? position.satsInView : null,
              seqNumber: Number.isFinite(position.seqNumber) ? position.seqNumber : null,
              timestamp: timestampSeconds,
              timestampMillisAdjust: Number.isFinite(position.timestampMillisAdjust)
                ? position.timestampMillisAdjust
                : null,
              gpsAccuracy: Number.isFinite(position.gpsAccuracy) ? position.gpsAccuracy : null,
              fixType: Number.isFinite(position.fixType) ? position.fixType : null,
              fixQuality: Number.isFinite(position.fixQuality) ? position.fixQuality : null,
              sensorId: Number.isFinite(position.sensorId) ? position.sensorId : null,
              nextUpdate: Number.isFinite(position.nextUpdate) ? position.nextUpdate : null
            }
          };
        }
        case 'ROUTING_APP': {
          const message = this.types.routing.decode(payload);
          const routing = this.types.routing.toObject(message, TO_OBJECT_OPTIONS);
          if (routing.routeRequest) {
            const path = (routing.routeRequest.route || []).map((n) =>
              this._formatNode(n).label
            );
            const detail = path.length ? `path: ${path.join(' -> ')}` : '';
            return { type: 'RouteRequest', details: detail };
          }
          if (routing.routeReply) {
            const path = (routing.routeReply.routeBack || routing.routeReply.route || []).map(
              (n) => this._formatNode(n).label
            );
            const detail = path.length ? `reply: ${path.join(' -> ')}` : '';
            return { type: 'RouteReply', details: detail };
          }
          if (routing.errorReason) {
            const error = routing.errorReason.error || 'UNKNOWN';
            return { type: 'RouteError', details: `error=${error}` };
          }
          return { type: 'Routing' };
        }
        case 'TELEMETRY_APP': {
          const message = this.types.telemetry.decode(payload);
          const telemetry = this.types.telemetry.toObject(message, TO_OBJECT_OPTIONS);
          const telemetryInfo = buildTelemetryInfo(telemetry);
          if (telemetry.deviceMetrics) {
            const metrics = telemetryInfo?.metrics || telemetry.deviceMetrics;
            const parts = [];
            const batt = metrics.batteryLevel;
            if (batt != null && Number.isFinite(Number(batt))) {
              parts.push(`battery ${Number(batt).toFixed(0)}%`);
            }
            const voltage = metrics.voltage;
            if (voltage != null && Number.isFinite(Number(voltage))) {
              parts.push(`${Number(voltage).toFixed(2)}V`);
            }
            const cu = metrics.channelUtilization;
            if (cu != null) {
              const formatted = formatPercent(cu);
              if (formatted) {
                parts.push(`CU ${formatted}`);
              }
            }
            const air = metrics.airUtilTx;
            if (air != null) {
              const formatted = formatPercent(air);
              if (formatted) {
                parts.push(`AirTx ${formatted}`);
              }
            }
            const uptime = metrics.uptimeSeconds ?? metrics.uptime;
            if (uptime != null && Number.isFinite(Number(uptime))) {
              parts.push(`uptime ${formatDuration(Number(uptime))}`);
            }
            return { type: 'Telemetry', details: parts.join(' '), telemetry: telemetryInfo };
          }
          if (telemetry.environmentMetrics) {
            const metrics = telemetryInfo?.metrics || telemetry.environmentMetrics;
            const parts = [];
            const temperature = metrics.temperature;
            if (temperature != null && Number.isFinite(Number(temperature))) {
              parts.push(`${Number(temperature).toFixed(1)}°C`);
            }
            const humidity = metrics.relativeHumidity ?? metrics.humidity;
            if (humidity != null && Number.isFinite(Number(humidity))) {
              parts.push(`RH ${Number(humidity).toFixed(0)}%`);
            }
            const pressure = metrics.barometricPressure ?? metrics.pressure;
            if (pressure != null && Number.isFinite(Number(pressure))) {
              parts.push(`${Number(pressure).toFixed(1)}hPa`);
            }
            return { type: 'EnvTelemetry', details: parts.join(' '), telemetry: telemetryInfo };
          }
          if (telemetryInfo) {
            const detail = formatGenericTelemetryDetail(telemetryInfo);
            return {
              type: 'Telemetry',
              details: detail,
              telemetry: telemetryInfo
            };
          }
          return { type: 'Telemetry' };
        }
        case 'WAYPOINT_APP': {
          const message = this.types.waypoint.decode(payload);
          const waypoint = this.types.waypoint.toObject(message, TO_OBJECT_OPTIONS);
          const lat = extractCoordinate(waypoint.latitudeI, waypoint.latitude);
          const lon = extractCoordinate(waypoint.longitudeI, waypoint.longitude);
          const detailParts = [];
          if (waypoint.name) detailParts.push(waypoint.name);
          if (lat != null && lon != null) {
            detailParts.push(`(${lat.toFixed(6)}, ${lon.toFixed(6)})`);
          }
          const extras = [];
          if (waypoint.description) extras.push(waypoint.description);
          if (waypoint.icon) extras.push(`icon: ${formatUnicode(waypoint.icon)}`);
          if (waypoint.expire) extras.push(`expire: ${new Date(waypoint.expire * 1000).toISOString()}`);
          return {
            type: 'Waypoint',
            details: detailParts.join(' '),
            extraLines: extras
          };
        }
        case 'ADMIN_APP': {
          const message = this.types.admin.decode(payload);
          const admin = this.types.admin.toObject(message, { ...TO_OBJECT_OPTIONS, oneofs: true });
          const variant = admin.payloadVariant;
          const detail = variant ? variant.replace(/([A-Z])/g, ' $1').trim() : 'Admin';
          const extras = summarizeObject(admin, ['payloadVariant']);
          return { type: 'Admin', details: detail, extraLines: extras };
        }
        case 'KEY_VERIFICATION_APP': {
          const message = this.types.keyVerification.decode(payload);
          const keyInfo = this.types.keyVerification.toObject(message, TO_OBJECT_OPTIONS);
          return {
            type: 'KeyVerification',
            details: `nonce ${keyInfo.nonce ?? ''}`,
            extraLines: summarizeObject(keyInfo, [])
          };
        }
        case 'REMOTE_HARDWARE_APP': {
          const message = this.types.remoteHardware.decode(payload);
          const hardware = this.types.remoteHardware.toObject(message, TO_OBJECT_OPTIONS);
          const detail = hardware.type ? hardware.type : 'RemoteHardware';
          const extras = [];
          if (hardware.gpioMask != null) extras.push(`mask: 0x${hardware.gpioMask.toString(16)}`);
          if (hardware.gpioValue != null) extras.push(`value: 0x${hardware.gpioValue.toString(16)}`);
          return { type: 'RemoteHardware', details: detail, extraLines: extras };
        }
        case 'STORE_FORWARD_APP': {
          const message = this.types.storeForward.decode(payload);
          const store = this.types.storeForward.toObject(message, TO_OBJECT_OPTIONS);
          const detail = store.rr ? store.rr : 'StoreForward';
          const extras = summarizeObject(store, ['rr']);
          return { type: 'StoreForward', details: detail, extraLines: extras };
        }
        case 'PAXCOUNTER_APP': {
          const message = this.types.paxcount.decode(payload);
          const pax = this.types.paxcount.toObject(message, TO_OBJECT_OPTIONS);
          const detail = `WiFi ${pax.wifi ?? 0}, BLE ${pax.ble ?? 0}`;
          const extras = [];
          if (pax.uptime != null) extras.push(`uptime ${formatDuration(pax.uptime)}`);
          return { type: 'PaxCounter', details: detail, extraLines: extras };
        }
        case 'TRACEROUTE_APP': {
          const message = this.types.routeDiscovery.decode(payload);
          const trace = this.types.routeDiscovery.toObject(message, TO_OBJECT_OPTIONS);
          const forwardPath = (trace.route || []).map((n) => this._formatNode(n).label);
          const returnPath = (trace.routeBack || []).map((n) => this._formatNode(n).label);
          const detail = forwardPath.length ? `forward: ${forwardPath.join(' -> ')}` : 'Traceroute';
          const extras = [];
          if (returnPath.length) extras.push(`return: ${returnPath.join(' -> ')}`);
          if (trace.snrTowards?.length) extras.push(`SNR→: ${trace.snrTowards.map(formatSnr).join(', ')}`);
          if (trace.snrBack?.length) extras.push(`SNR←: ${trace.snrBack.map(formatSnr).join(', ')}`);
          return { type: 'Traceroute', details: detail, extraLines: extras };
        }
        case 'NEIGHBORINFO_APP': {
          const message = this.types.neighborInfo.decode(payload);
          const info = this.types.neighborInfo.toObject(message, TO_OBJECT_OPTIONS);
          const origin = this._formatNode(info.nodeId);
          const neighbors = (info.neighbors || []).map((n) => {
            const node = this._formatNode(n.nodeId);
            const snr = n.snr != null ? `${n.snr.toFixed(1)}dB` : '';
            return `${node.label} ${snr}`.trim();
          });
          return {
            type: 'NeighborInfo',
            details: origin.label,
            extraLines: neighbors.length ? neighbors : undefined
          };
        }
        case 'NODEINFO_APP': {
          const message = this.types.user.decode(payload);
          const user = this.types.user.toObject(message, TO_OBJECT_OPTIONS);
          const parts = [];
          if (user.longName) {
            parts.push(user.longName);
          }
          if (user.id) {
            parts.push(`(${user.id})`);
          }
        const extras = [];
        if (user.hwModel) extras.push(`model: ${String(user.hwModel)}`);
        if (user.role) extras.push(`role: ${String(user.role)}`);
        if (user.shortName && user.shortName !== user.longName) {
          extras.push(`short: ${user.shortName}`);
        }
        return {
          type: 'NodeInfo',
          details: parts.join(' '),
          extraLines: extras
        };
      }
        case 'CAYENNE_APP': {
          return decodeCayennePayload(payload);
        }
        default:
          return { type: friendlyPortLabel(portName) };
      }
    } catch (err) {
      return {
        type: friendlyPortLabel(portName),
        details: `解析失敗: ${err.message}`
      };
    }
  }

  _resolvePortnum(portnum) {
    if (portnum == null) {
      return { name: undefined, id: undefined };
    }
    if (typeof portnum === 'string') {
      return { name: portnum, id: this.portEnum?.values?.[portnum] };
    }
    const enumType = this.portEnum;
    const name = enumType?.valuesById?.[portnum];
    return { name, id: portnum };
  }

  _formatNode(nodeNum) {
    if (nodeNum == null) {
      return { label: 'unknown', meshId: null };
    }
    const num = nodeNum >>> 0;
    const entry = this.nodeMap.get(num);
    let meshId = entry?.id || formatHexId(num);
    if (meshId && meshId.startsWith('0x')) {
      meshId = `!${meshId.slice(2)}`;
    }
    const name = entry?.shortName || entry?.longName;
    const label = name && meshId ? `${name} (${meshId})` : name || meshId;
    return {
      label,
      meshId,
      shortName: entry?.shortName,
      longName: entry?.longName,
      hwModel: entry?.hwModel ?? null,
      role: entry?.role ?? null,
      raw: num
    };
  }

  _shouldEmitPacket(packet) {
    const id = packet.id >>> 0;
    const from = packet.from >>> 0;

    if (!id) {
      return true;
    }

    const key = `${from}:${id}`;
    if (this._seenPacketKeys.has(key)) {
      return false;
    }

    this._seenPacketKeys.set(key, Date.now());
    this._packetKeyQueue.push(key);

    const MAX_TRACKED = 5000;
    if (this._packetKeyQueue.length > MAX_TRACKED) {
      const oldKey = this._packetKeyQueue.shift();
      if (oldKey) {
        this._seenPacketKeys.delete(oldKey);
      }
    }
    return true;
  }

  _sendWantConfig() {
    if (!this._socket || !this._connected) return;
    const nonce = crypto.randomBytes(2).readUInt16BE(0);
    const message = this.toRadioType.create({ wantConfigId: nonce });
    const encoded = this.toRadioType.encode(message).finish();
    const framed = framePacket(encoded);
    this._socket.write(framed, (err) => {
      if (err) {
        this.emit('error', new Error(`want_config 傳送失敗: ${err.message}`));
      } else {
        this.emit('handshake', { nonce });
      }
    });
  }

  _setupHeartbeat() {
    this._clearHeartbeat();
    if (this.options.heartbeat > 0 && this._socket) {
      const intervalMs = this.options.heartbeat * 1000;
      this._heartbeatTimer = setInterval(() => this._sendHeartbeat(), intervalMs);
      this._heartbeatTimer.unref?.();
    }
  }

  _clearHeartbeat() {
    if (this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer);
      this._heartbeatTimer = null;
    }
  }

  _sendHeartbeat() {
    if (!this._socket || !this._connected) return;
    const message = this.toRadioType.create({ heartbeat: {} });
    const encoded = this.toRadioType.encode(message).finish();
    const framed = framePacket(encoded);
    this._socket.write(framed, (err) => {
      if (err) {
        this.emit('error', new Error(`heartbeat 傳送失敗: ${err.message}`));
      }
    });
  }
}

class MeshtasticStreamDecoder {
  constructor(options) {
    this.buffer = Buffer.alloc(0);
    this.maxPacketLength = options.maxPacketLength ?? DEFAULT_MAX_PACKET;
    this.onPacket = options.onPacket;
    this.onError = options.onError;
  }

  push(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    this._process();
  }

  _process() {
    while (this.buffer.length >= HEADER_SIZE) {
      if (!this._headerLooksValid(this.buffer)) {
        this.buffer = this.buffer.slice(1);
        continue;
      }

      const packetLength = this.buffer.readUInt16BE(2);
      if (packetLength <= 0 || packetLength > this.maxPacketLength) {
        this.buffer = this.buffer.slice(2);
        continue;
      }

      if (this.buffer.length < HEADER_SIZE + packetLength) {
        return;
      }

      const payload = this.buffer.slice(HEADER_SIZE, HEADER_SIZE + packetLength);
      this.buffer = this.buffer.slice(HEADER_SIZE + packetLength);

      try {
        this.onPacket(payload);
      } catch (err) {
        // Propagate asynchronously so parent can handle
        setImmediate(() => {
          if (this.onError) {
            this.onError(err);
          }
        });
      }
    }
  }

  _headerLooksValid(buf) {
    return buf.readUInt16BE(0) === MAGIC;
  }
}

function extractCoordinate(intValue, floatValue) {
  if (intValue != null) {
    return intValue / 1e7;
  }
  if (floatValue != null) {
    return floatValue;
  }
  return null;
}

function friendlyPortLabel(portName, portId) {
  if (portName) {
    const map = {
      POSITION_APP: 'Position',
      ROUTING_APP: 'Routing',
      TELEMETRY_APP: 'Telemetry',
      TEXT_MESSAGE_APP: 'Text',
      NODEINFO_APP: 'NodeInfo',
      ADMIN_APP: 'Admin',
      TRACEROUTE_APP: 'Traceroute',
      WAYPOINT_APP: 'Waypoint',
      STORE_FORWARD_APP: 'StoreForward',
      PAXCOUNTER_APP: 'PaxCounter',
      REMOTE_HARDWARE_APP: 'RemoteHardware',
      KEY_VERIFICATION_APP: 'KeyVerification',
      TEXT_MESSAGE_COMPRESSED_APP: 'Text',
      DETECTION_SENSOR_APP: 'Detection',
      ALERT_APP: 'Alert',
      RANGE_TEST_APP: 'RangeTest',
      REPLY_APP: 'Reply',
      NEIGHBORINFO_APP: 'NeighborInfo'
    };
    return map[portName] || portName;
  }
  if (portId != null) {
    return `Port ${portId}`;
  }
  return 'Unknown';
}

function formatHexId(num) {
  return `!${num.toString(16).padStart(8, '0')}`;
}

function formatTimestamp(date) {
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const mi = String(date.getMinutes()).padStart(2, '0');
  const ss = String(date.getSeconds()).padStart(2, '0');
  return `${mm}/${dd} ${hh}:${mi}:${ss}`;
}

function formatHops(hopLimit, hopStart) {
  if (hopStart != null && hopStart !== 0) {
    if (hopLimit != null) {
      const used = Math.max(hopStart - hopLimit, 0);
      return `${used}/${hopStart}`;
    }
    return `?/${hopStart}`;
  }
  if (hopLimit != null) {
    return `${hopLimit}`;
  }
  return '';
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return `${seconds}s`;
  }
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (h > 0) {
    return `${h}h${m}m`;
  }
  if (m > 0) {
    return `${m}m${s}s`;
  }
  return `${s}s`;
}

function formatPercent(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return null;
  }
  const abs = Math.abs(num);
  if (abs >= 100) {
    return `${num.toFixed(0)}%`;
  }
  if (abs >= 10) {
    return `${num.toFixed(1)}%`;
  }
  if (abs >= 1) {
    return `${num.toFixed(2)}%`;
  }
  return `${num.toFixed(3)}%`;
}

function formatMetricNumber(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return null;
  }
  const abs = Math.abs(num);
  if (abs >= 100) {
    return num.toFixed(0);
  }
  if (abs >= 10) {
    return num.toFixed(1);
  }
  if (abs >= 1) {
    return num.toFixed(2);
  }
  return num.toFixed(3);
}

function normalizeTelemetryValue(value) {
  if (value == null) {
    return null;
  }
  if (Buffer.isBuffer(value)) {
    return null;
  }
  if (Array.isArray(value)) {
    const mapped = value
      .map((item) => normalizeTelemetryValue(item))
      .filter((item) => item != null);
    return mapped.length ? mapped : null;
  }
  if (typeof value === 'object') {
    const nested = {};
    for (const [key, nestedValue] of Object.entries(value)) {
      const normalized = normalizeTelemetryValue(nestedValue);
      if (normalized != null) {
        nested[key] = normalized;
      }
    }
    return Object.keys(nested).length ? nested : null;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length ? trimmed : null;
  }
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return numeric;
  }
  return String(value);
}

function sanitizeTelemetryMetrics(input) {
  if (!input || typeof input !== 'object') {
    return {};
  }
  const result = {};
  for (const [key, value] of Object.entries(input)) {
    const normalized = normalizeTelemetryValue(value);
    if (normalized != null) {
      result[key] = normalized;
    }
  }
  return result;
}

function buildTelemetryInfo(telemetry) {
  if (!telemetry || typeof telemetry !== 'object') {
    return null;
  }
  const timeSeconds = Number.isFinite(telemetry.time) ? Number(telemetry.time) : null;
  const timeMs = timeSeconds != null ? timeSeconds * 1000 : null;
  const variants = [
    ['deviceMetrics', 'device'],
    ['environmentMetrics', 'environment'],
    ['airQualityMetrics', 'airQuality'],
    ['powerMetrics', 'power'],
    ['localStats', 'local'],
    ['healthMetrics', 'health'],
    ['hostMetrics', 'host']
  ];
  for (const [key, kind] of variants) {
    const metrics = telemetry[key];
    if (metrics && typeof metrics === 'object') {
      return {
        kind,
        timeSeconds,
        timeMs,
        metrics: sanitizeTelemetryMetrics(metrics)
      };
    }
  }
  if (timeMs != null) {
    return {
      kind: 'unknown',
      timeSeconds,
      timeMs,
      metrics: {}
    };
  }
  return null;
}

function formatGenericTelemetryDetail(info) {
  if (!info || !info.metrics) {
    return '';
  }
  const parts = [];
  for (const [key, value] of Object.entries(info.metrics)) {
    if (value == null) continue;
    if (typeof value === 'number') {
      const numLabel = formatMetricNumber(value);
      if (numLabel != null) {
        parts.push(`${key} ${numLabel}`);
      } else {
        parts.push(`${key} ${value}`);
      }
    } else if (typeof value === 'boolean') {
      parts.push(`${key} ${value ? 'true' : 'false'}`);
    } else if (typeof value === 'string') {
      parts.push(`${key} ${value}`);
    } else {
      parts.push(`${key}=${JSON.stringify(value)}`);
    }
  }
  return parts.join(' ');
}

function framePacket(payload) {
  const framed = Buffer.alloc(HEADER_SIZE + payload.length);
  framed.writeUInt16BE(MAGIC, 0);
  framed.writeUInt16BE(payload.length, 2);
  payload.copy(framed, HEADER_SIZE);
  return framed;
}

const TEXT_PORTS = new Set([
  'TEXT_MESSAGE_APP',
  'DETECTION_SENSOR_APP',
  'ALERT_APP',
  'RANGE_TEST_APP',
  'REPLY_APP'
]);

const COMPRESSED_TEXT_PORTS = new Set(['TEXT_MESSAGE_COMPRESSED_APP']);

function decodeCayennePayload(payload) {
  if (!Buffer.isBuffer(payload) || payload.length < 3) {
    return { type: 'Cayenne', details: 'payload 太短' };
  }

  const readings = [];
  const extras = [];
  let offset = 0;
  while (offset + 2 <= payload.length) {
    const channel = payload[offset++];
    if (offset >= payload.length) {
      extras.push(`ch${channel}: 資料截斷`);
      break;
    }
    const type = payload[offset++];
    const spec = CAYENNE_TYPES[type];
    if (!spec) {
      const { label, extraLines } = describeUnknownCayenneType(channel, type, payload.slice(offset));
      if (label) readings.push(label);
      if (extraLines.length) extras.push(...extraLines);
      break;
    }
    if (offset + spec.length > payload.length) {
      extras.push(`ch${channel}: ${spec.name} 資料不足`);
      break;
    }
    const segment = payload.slice(offset, offset + spec.length);
    offset += spec.length;
    try {
      const label = spec.decode(segment);
      readings.push(`ch${channel} ${spec.name} ${label}`);
    } catch (err) {
      extras.push(`ch${channel}: ${spec.name} 解析失敗 (${err.message})`);
    }
  }

  if (!readings.length && !extras.length) {
    extras.push('未解析任何 Cayenne 感測資料');
  }

  return {
    type: 'Cayenne',
    details: readings.join('，') || 'Cayenne payload',
    extraLines: extras.length ? extras : undefined
  };
}

const CAYENNE_TYPES = {
  0x00: createCayenneSpec('DigitalInput', 1, (buf) => buf.readUInt8(0)),
  0x01: createCayenneSpec('DigitalOutput', 1, (buf) => buf.readUInt8(0)),
  0x02: createCayenneSpec('AnalogInput', 2, (buf) => (buf.readInt16BE(0) / 100).toFixed(2)),
  0x03: createCayenneSpec('AnalogOutput', 2, (buf) => (buf.readInt16BE(0) / 100).toFixed(2)),
  0x04: createCayenneSpec('Illuminance', 2, (buf) => `${buf.readUInt16BE(0)} lux`),
  0x05: createCayenneSpec('Presence', 1, (buf) => (buf.readUInt8(0) ? 'detected' : 'clear')),
  0x06: createCayenneSpec('Temperature', 2, (buf) => `${(buf.readInt16BE(0) / 10).toFixed(1)}°C`),
  0x07: createCayenneSpec('Humidity', 1, (buf) => `${(buf.readUInt8(0) / 2).toFixed(1)}%`),
  0x08: createCayenneSpec('Accelerometer', 6, (buf) => {
    const x = buf.readInt16BE(0) / 1000;
    const y = buf.readInt16BE(2) / 1000;
    const z = buf.readInt16BE(4) / 1000;
    return `x:${x.toFixed(3)}g y:${y.toFixed(3)}g z:${z.toFixed(3)}g`;
  }),
  0x09: createCayenneSpec('Barometer', 2, (buf) => `${(buf.readUInt16BE(0) / 10).toFixed(1)}hPa`),
  0x0a: createCayenneSpec('Gyrometer', 6, (buf) => {
    const x = buf.readInt16BE(0) / 100;
    const y = buf.readInt16BE(2) / 100;
    const z = buf.readInt16BE(4) / 100;
    return `x:${x.toFixed(2)}°/s y:${y.toFixed(2)}°/s z:${z.toFixed(2)}°/s`;
  }),
  0x0c: createCayenneSpec('GPS', 9, (buf) => {
    const lat = readInt24BE(buf, 0) / 1e4;
    const lng = readInt24BE(buf, 3) / 1e4;
    const alt = readInt24BE(buf, 6) / 100;
    return `lat:${lat.toFixed(6)} lon:${lng.toFixed(6)} alt:${alt.toFixed(1)}m`;
  }),
  0x65: createCayenneSpec('Illuminance', 2, (buf) => `${buf.readUInt16BE(0)} lux`),
  0x66: createCayenneSpec('Presence', 1, (buf) => (buf.readUInt8(0) ? 'detected' : 'clear')),
  0x67: createCayenneSpec('Temperature', 2, (buf) => `${(buf.readInt16BE(0) / 10).toFixed(1)}°C`),
  0x68: createCayenneSpec('Humidity', 1, (buf) => `${(buf.readUInt8(0) / 2).toFixed(1)}%`),
  0x71: createCayenneSpec('Accelerometer', 6, (buf) => {
    const x = buf.readInt16BE(0) / 1000;
    const y = buf.readInt16BE(2) / 1000;
    const z = buf.readInt16BE(4) / 1000;
    return `x:${x.toFixed(3)}g y:${y.toFixed(3)}g z:${z.toFixed(3)}g`;
  }),
  0x73: createCayenneSpec('Barometer', 2, (buf) => `${(buf.readUInt16BE(0) / 10).toFixed(1)}hPa`),
  0x86: createCayenneSpec('Gyrometer', 6, (buf) => {
    const x = buf.readInt16BE(0) / 100;
    const y = buf.readInt16BE(2) / 100;
    const z = buf.readInt16BE(4) / 100;
    return `x:${x.toFixed(2)}°/s y:${y.toFixed(2)}°/s z:${z.toFixed(2)}°/s`;
  }),
  0x88: createCayenneSpec('GPS', 9, (buf) => {
    const lat = readInt24BE(buf, 0) / 1e4;
    const lng = readInt24BE(buf, 3) / 1e4;
    const alt = readInt24BE(buf, 6) / 100;
    return `lat:${lat.toFixed(6)} lon:${lng.toFixed(6)} alt:${alt.toFixed(1)}m`;
  })
};

function createCayenneSpec(name, length, decoder) {
  return {
    name,
    length,
    decode: decoder
  };
}

function readInt24BE(buf, offset) {
  const b0 = buf.readUInt8(offset);
  const b1 = buf.readUInt8(offset + 1);
  const b2 = buf.readUInt8(offset + 2);
  let value = (b0 << 16) | (b1 << 8) | b2;
  if (value & 0x800000) {
    value |= 0xff000000;
  }
  return value;
}

function describeUnknownCayenneType(channel, type, buffer) {
  const label = `ch${channel} 類型 0x${type.toString(16).padStart(2, '0')} (${buffer.length} bytes)`;
  const extraLines = [];
  if (buffer.length) {
    extraLines.push(`hex: ${buffer.toString('hex')}`);
    extraLines.push(`base64: ${buffer.toString('base64')}`);
    const ascii = bufferToAscii(buffer);
    if (ascii) {
      extraLines.push(`ascii: ${ascii}`);
    }
    const floats = [];
    for (let i = 0; i + 4 <= buffer.length && floats.length < 6; i += 4) {
      const value = buffer.readFloatLE(i);
      if (!Number.isNaN(value) && Number.isFinite(value)) {
        floats.push(value.toFixed(3));
      }
    }
    if (floats.length) {
      extraLines.push(`float32LE: ${floats.join(', ')}`);
    }
  }
  return { label, extraLines };
}

function bufferToAscii(buffer) {
  let result = '';
  for (const byte of buffer) {
    if (byte >= 32 && byte <= 126) {
      result += String.fromCharCode(byte);
    } else {
      result += '.';
    }
  }
  result = result.trim();
  return result ? result : '';
}

function decodeTextPayload(payload, compressed) {
  try {
    const text = compressed
      ? unishox2_decompress_simple(payload, payload.length)
      : payload.toString('utf8');
    return { type: 'Text', details: text };
  } catch (err) {
    return {
      type: 'Text',
      details: payload.toString('utf8'),
      extraLines: [`解碼失敗: ${err.message}`]
    };
  }
}

function summarizeObject(obj, excludeKeys = []) {
  const lines = [];
  for (const [key, value] of Object.entries(obj || {})) {
    if (excludeKeys.includes(key) || key.startsWith('_') || value == null || value === '') {
      continue;
    }
    if (typeof value === 'object' && !Buffer.isBuffer(value)) {
      lines.push(`${key}: ${JSON.stringify(value)}`);
    } else if (Buffer.isBuffer(value)) {
      lines.push(`${key}: ${value.toString('hex')}`);
    } else {
      lines.push(`${key}: ${value}`);
    }
  }
  return lines;
}

function formatUnicode(codePoint) {
  try {
    return String.fromCodePoint(codePoint);
  } catch {
    return `U+${codePoint.toString(16)}`;
  }
}

function formatSnr(value) {
  return `${(value / 4).toFixed(2)}dB`;
}

module.exports = MeshtasticClient;
