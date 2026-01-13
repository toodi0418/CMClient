'use strict';

const fs = require('fs');
const path = require('path');

let DatabaseSync;
try {
  ({ DatabaseSync } = require('node:sqlite'));
} catch (err) {
  throw new Error('CallMeshDataStore 需要 Node.js 22 的 node:sqlite 模組，請升級執行環境。');
}

function ensureDirectory(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function toJson(value) {
  if (value === undefined) {
    return JSON.stringify(null);
  }
  return JSON.stringify(value);
}

function fromJson(payload) {
  if (payload == null) return null;
  try {
    return JSON.parse(payload);
  } catch {
    return null;
  }
}

function sanitizeText(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const str = String(value).trim();
  return str.length ? str : null;
}

function normalizeMeshIdValue(meshId) {
  const text = sanitizeText(meshId);
  if (!text) {
    return null;
  }
  if (text.startsWith('!')) {
    return text.toLowerCase();
  }
  if (/^0x[0-9a-f]+$/i.test(text)) {
    return `!${text.slice(2).padStart(8, '0')}`.toLowerCase();
  }
  if (/^[0-9a-f]{8}$/i.test(text)) {
    return `!${text.toLowerCase()}`;
  }
  return text;
}

function toFiniteNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function toFiniteInteger(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return Math.trunc(numeric);
}

function sanitizeMessageNode(node) {
  if (!node || typeof node !== 'object') {
    return null;
  }
  const meshId = sanitizeText(node.meshId ?? node.mesh_id);
  const meshIdNormalized =
    sanitizeText(node.meshIdNormalized ?? node.mesh_id_normalized) || normalizeMeshIdValue(meshId);
  const meshIdOriginal = sanitizeText(node.meshIdOriginal ?? node.mesh_id_original);
  const longName = sanitizeText(node.longName ?? node.long_name);
  const shortName = sanitizeText(node.shortName ?? node.short_name);
  const label = sanitizeText(node.label);
  if (
    !meshId &&
    !meshIdNormalized &&
    !meshIdOriginal &&
    !longName &&
    !shortName &&
    !label
  ) {
    return null;
  }
  return {
    meshId,
    meshIdNormalized,
    meshIdOriginal,
    longName,
    shortName,
    label
  };
}

function sanitizeMessageHops(hops) {
  if (!hops || typeof hops !== 'object') {
    return null;
  }
  const start = toFiniteInteger(hops.start);
  const limit = toFiniteInteger(hops.limit);
  const label = sanitizeText(hops.label);
  if (start == null && limit == null && !label) {
    return null;
  }
  return {
    start,
    limit,
    label
  };
}

function sanitizeExtraLines(lines) {
  if (!Array.isArray(lines)) {
    return [];
  }
  const result = [];
  for (const line of lines) {
    const text = sanitizeText(line);
    if (text) {
      result.push(text);
    }
  }
  return result;
}

function sanitizeMessageEntry(entry, { fallbackFlowId, fallbackChannel, fallbackTimestampMs }) {
  if (!entry || typeof entry !== 'object') {
    return null;
  }
  const channel = Number.isFinite(entry.channel)
    ? Math.trunc(entry.channel)
    : Number.isFinite(fallbackChannel)
      ? Math.trunc(fallbackChannel)
      : 0;
  if (!Number.isFinite(channel) || channel < 0) {
    return null;
  }
  const timestampMs = Number.isFinite(entry.timestampMs)
    ? Math.trunc(entry.timestampMs)
    : Number.isFinite(fallbackTimestampMs)
      ? Math.trunc(fallbackTimestampMs)
      : Date.now();
  const timestampLabel =
    sanitizeText(entry.timestampLabel) ?? new Date(timestampMs).toISOString();
  const detail =
    typeof entry.detail === 'string'
      ? entry.detail
      : entry.detail != null
        ? String(entry.detail)
        : '';
  const flowIdRaw = sanitizeText(entry.flowId);
  const flowId =
    flowIdRaw ??
    sanitizeText(fallbackFlowId) ??
    `${channel}-${timestampMs}-${Math.random().toString(16).slice(2, 10)}`;

  const relayMeshIdRaw =
    sanitizeText(entry.relayMeshId) ??
    sanitizeText(entry.relay?.meshId) ??
    sanitizeText(entry.relay?.mesh_id);
  const relayMeshIdNormalized =
    sanitizeText(entry.relayMeshIdNormalized) ??
    sanitizeText(entry.relay?.meshIdNormalized) ??
    sanitizeText(entry.relay?.mesh_id_normalized) ??
    normalizeMeshIdValue(relayMeshIdRaw);

  const hops = sanitizeMessageHops(entry.hops);
  const extraLines = sanitizeExtraLines(entry.extraLines);
  const meshPacketId = toFiniteInteger(entry.meshPacketId);
  const replyId = toFiniteInteger(entry.replyId);
  const replyTo = sanitizeText(entry.replyTo);
  const scope = sanitizeText(entry.scope);
  const snr = toFiniteNumber(entry.snr);
  const rssi = toFiniteNumber(entry.rssi);
  const rawHex = sanitizeText(entry.rawHex);
  const rawLength = toFiniteInteger(entry.rawLength);

  const fromNode = sanitizeMessageNode(entry.from);
  const relayNode = sanitizeMessageNode(entry.relay);

  return {
    flowId,
    channel,
    timestampMs,
    timestampLabel,
    type: 'Text',
    detail,
    extraLines,
    fromNode,
    relayNode,
    relayMeshId: relayMeshIdRaw ?? null,
    relayMeshIdNormalized: relayMeshIdNormalized ?? null,
    meshPacketId,
    replyId,
    replyTo,
    scope,
    synthetic: Boolean(entry.synthetic),
    hops,
    snr,
    rssi,
    rawHex,
    rawLength
  };
}

function sanitizeAprsPacketSnapshotEntry(entry) {
  if (!entry || typeof entry !== 'object') {
    return null;
  }
  const packetKey = sanitizeText(entry.packetKey ?? entry.key);
  const lastSeenMs = toFiniteInteger(entry.lastSeenMs ?? entry.lastSeen);
  if (!packetKey || lastSeenMs == null) {
    return null;
  }
  const callsign = sanitizeText(entry.callsign ?? entry.callSign);
  let infoValue = entry.infoString;
  if (infoValue === undefined || infoValue === null) {
    infoValue = entry.info;
  }
  const info =
    infoValue === undefined || infoValue === null ? null : String(infoValue);
  return {
    packetKey,
    callsign,
    info,
    lastSeenMs
  };
}

function sanitizeAprsCallsignSummaryEntry(entry) {
  if (!entry || typeof entry !== 'object') {
    return null;
  }
  const callsign = sanitizeText(entry.callsign ?? entry.callSign);
  if (!callsign) {
    return null;
  }
  const lastSeenMs = toFiniteInteger(entry.lastSeenMs ?? entry.lastSeen);
  let infoValue = entry.lastInfo;
  if (infoValue === undefined || infoValue === null) {
    infoValue = entry.lastInfoText ?? entry.info ?? null;
  }
  const lastInfo =
    infoValue === undefined || infoValue === null ? null : String(infoValue);
  return {
    callsign,
    lastSeenMs,
    lastInfo
  };
}

function sanitizeAprsPositionDigestEntry(entry) {
  if (!entry || typeof entry !== 'object') {
    return null;
  }
  const meshId = sanitizeText(entry.meshId ?? entry.mesh_id);
  if (!meshId) {
    return null;
  }
  const digest = sanitizeText(entry.digest);
  const timestampMs = toFiniteInteger(entry.timestampMs ?? entry.timestamp);
  return {
    meshId,
    digest,
    timestampMs
  };
}

function sanitizeAprsBacktrackPosition(entry) {
  if (!entry || typeof entry !== 'object') {
    return null;
  }
  const lat = toFiniteNumber(entry.lat ?? entry.latitude);
  const lon = toFiniteNumber(entry.lon ?? entry.longitude);
  const timestampMs = toFiniteInteger(
    entry.timestampMs ?? entry.timestamp_ms ?? entry.timestamp ?? entry.at ?? entry.time
  );
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  return {
    lat,
    lon,
    timestampMs
  };
}

function sanitizeAprsBacktrackHistory(history) {
  if (!Array.isArray(history)) {
    return [];
  }
  const result = [];
  for (const item of history) {
    const pos = sanitizeAprsBacktrackPosition(item);
    if (pos) {
      result.push(pos);
    }
  }
  return result;
}

function sanitizeAprsBacktrackStateEntry(entry) {
  if (!entry || typeof entry !== 'object') {
    return null;
  }
  const callsign = sanitizeText(entry.callsign ?? entry.callSign);
  if (!callsign) {
    return null;
  }
  const lastUploaded = sanitizeAprsBacktrackPosition(entry.lastUploaded ?? entry.last);
  const prevUploaded = sanitizeAprsBacktrackPosition(entry.prevUploaded ?? entry.prev);
  const pending = sanitizeAprsBacktrackPosition(entry.pending ?? entry.pendingPos);
  const pendingFirstSeenMs = toFiniteInteger(
    entry.pendingFirstSeenMs ??
      entry.pending_first_seen_ms ??
      entry.pendingFirstSeen ??
      entry.pending_first_seen ??
      entry.pending?.firstSeenMs ??
      entry.pending?.firstSeen
  );
  const pendingReason = sanitizeText(
    entry.pendingReason ?? entry.pending_reason ?? entry.pending?.reason
  );
  const pendingLastSeenMs = toFiniteInteger(
    entry.pendingLastSeenMs ??
      entry.pending_last_seen_ms ??
      entry.pendingLastSeen ??
      entry.pending?.lastSeenMs ??
      entry.pending?.lastSeen
  );
  const modeRaw = sanitizeText(entry.mode);
  const mode =
    modeRaw && ['car', 'hsr'].includes(modeRaw.toLowerCase())
      ? modeRaw.toLowerCase()
      : null;
  const modeUpdatedMs = toFiniteInteger(entry.modeUpdatedMs ?? entry.mode_updated_ms);
  let historySource = entry.history ?? entry.historyEntries;
  if (!historySource && typeof entry.history_json === 'string') {
    historySource = fromJson(entry.history_json);
  }
  const history = sanitizeAprsBacktrackHistory(historySource);
  return {
    callsign,
    lastUploaded,
    prevUploaded,
    pending,
    pendingFirstSeenMs,
    pendingReason,
    pendingLastSeenMs,
    mode,
    modeUpdatedMs,
    history
  };
}

function buildMessageNodeFromRow(row) {
  if (!row) {
    return null;
  }
  const meshId = sanitizeText(row.meshId);
  const meshIdNormalized =
    sanitizeText(row.meshIdNormalized) || normalizeMeshIdValue(meshId);
  const meshIdOriginal = sanitizeText(row.meshIdOriginal);
  const longName = sanitizeText(row.longName);
  const shortName = sanitizeText(row.shortName);
  const label = sanitizeText(row.label);

  if (
    !meshId &&
    !meshIdNormalized &&
    !meshIdOriginal &&
    !longName &&
    !shortName &&
    !label
  ) {
    return null;
  }
  return {
    meshId: meshId ?? null,
    meshIdNormalized: meshIdNormalized ?? null,
    meshIdOriginal: meshIdOriginal ?? null,
    longName: longName ?? null,
    shortName: shortName ?? null,
    label: label ?? null
  };
}

class CallMeshDataStore {
  constructor(filePath, options = {}) {
    if (!filePath) {
      throw new Error('CallMeshDataStore 需要提供資料庫路徑');
    }
    this.filePath = filePath;
    this.options = options;
    this.db = null;
    this.statements = Object.create(null);
  }

  init() {
    if (this.db) return;
    ensureDirectory(this.filePath);
    this.db = new DatabaseSync(this.filePath);
    this.db.exec('PRAGMA journal_mode = WAL;');
    this.db.exec('PRAGMA foreign_keys = ON;');
    this._migrateLegacyMessageLog();
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS kv_store (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS nodes (
        mesh_id TEXT PRIMARY KEY,
        mesh_id_original TEXT,
        short_name TEXT,
        long_name TEXT,
        hw_model TEXT,
        hw_model_label TEXT,
        role TEXT,
        role_label TEXT,
        latitude REAL,
        longitude REAL,
        altitude REAL,
        last_seen_at INTEGER,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS message_log (
        flow_id TEXT PRIMARY KEY,
        channel INTEGER NOT NULL,
        timestamp_ms INTEGER NOT NULL,
        timestamp_label TEXT,
        type TEXT,
        detail TEXT,
        scope TEXT,
        mesh_packet_id INTEGER,
        reply_id INTEGER,
        reply_to TEXT,
        relay_mesh_id TEXT,
        relay_mesh_id_normalized TEXT,
        hops_start INTEGER,
        hops_limit INTEGER,
        hops_label TEXT,
        synthetic INTEGER NOT NULL DEFAULT 0,
        position INTEGER NOT NULL,
        snr REAL,
        rssi REAL,
        raw_hex TEXT,
        raw_length INTEGER
      );

      CREATE INDEX IF NOT EXISTS idx_message_log_channel_position
        ON message_log(channel, position);

      CREATE TABLE IF NOT EXISTS message_nodes (
        flow_id TEXT NOT NULL,
        role TEXT NOT NULL,
        mesh_id TEXT,
        mesh_id_normalized TEXT,
        mesh_id_original TEXT,
        long_name TEXT,
        short_name TEXT,
        label TEXT,
        PRIMARY KEY (flow_id, role),
        FOREIGN KEY (flow_id) REFERENCES message_log(flow_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS message_extra_lines (
        flow_id TEXT NOT NULL,
        line_index INTEGER NOT NULL,
        content TEXT NOT NULL,
        PRIMARY KEY (flow_id, line_index),
        FOREIGN KEY (flow_id) REFERENCES message_log(flow_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS relay_stats (
        mesh_key TEXT PRIMARY KEY,
        snr REAL,
        rssi REAL,
        count INTEGER,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS aprs_packet_cache (
        packet_key TEXT PRIMARY KEY,
        callsign TEXT,
        info TEXT,
        last_seen_ms INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_aprs_packet_cache_callsign
        ON aprs_packet_cache(callsign);

      CREATE TABLE IF NOT EXISTS aprs_local_tx (
        packet_key TEXT PRIMARY KEY,
        callsign TEXT,
        info TEXT,
        last_seen_ms INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_aprs_local_tx_callsign
        ON aprs_local_tx(callsign);

      CREATE TABLE IF NOT EXISTS aprs_callsign_summary (
        callsign TEXT PRIMARY KEY,
        last_seen_ms INTEGER,
        last_info TEXT
      );

      CREATE TABLE IF NOT EXISTS aprs_position_digest (
        mesh_id TEXT PRIMARY KEY,
        digest TEXT,
        timestamp_ms INTEGER
      );

      CREATE TABLE IF NOT EXISTS aprs_backtrack_state (
        callsign TEXT PRIMARY KEY,
        last_lat REAL,
        last_lon REAL,
        last_at INTEGER,
        prev_lat REAL,
        prev_lon REAL,
        prev_at INTEGER,
        pending_lat REAL,
        pending_lon REAL,
        pending_first_seen_ms INTEGER,
        pending_reason TEXT,
        pending_last_seen_ms INTEGER,
        mode TEXT,
        mode_updated_ms INTEGER,
        history_json TEXT
      );
    `);

    this.statements.upsertKv = this.db.prepare(`
      INSERT INTO kv_store (key, value, updated_at)
      VALUES (@key, @value, @updatedAt)
      ON CONFLICT(key) DO UPDATE SET
        value = excluded.value,
        updated_at = excluded.updated_at
    `);

    this.statements.getKv = this.db.prepare('SELECT value FROM kv_store WHERE key = ?');
    this.statements.deleteKv = this.db.prepare('DELETE FROM kv_store WHERE key = ?');

    this.statements.deleteNodes = this.db.prepare('DELETE FROM nodes');
    this.statements.insertNode = this.db.prepare(`
      INSERT INTO nodes (
        mesh_id,
        mesh_id_original,
        short_name,
        long_name,
        hw_model,
        hw_model_label,
        role,
        role_label,
        latitude,
        longitude,
        altitude,
        last_seen_at,
        updated_at
      )
      VALUES (
        @meshId,
        @meshIdOriginal,
        @shortName,
        @longName,
        @hwModel,
        @hwModelLabel,
        @role,
        @roleLabel,
        @latitude,
        @longitude,
        @altitude,
        @lastSeenAt,
        @updatedAt
      )
      ON CONFLICT(mesh_id) DO UPDATE SET
        mesh_id_original = excluded.mesh_id_original,
        short_name = excluded.short_name,
        long_name = excluded.long_name,
        hw_model = excluded.hw_model,
        hw_model_label = excluded.hw_model_label,
        role = excluded.role,
        role_label = excluded.role_label,
        latitude = excluded.latitude,
        longitude = excluded.longitude,
        altitude = excluded.altitude,
        last_seen_at = excluded.last_seen_at,
        updated_at = excluded.updated_at
    `);
    this.statements.selectNodes = this.db.prepare(`
      SELECT
        mesh_id AS meshId,
        mesh_id_original AS meshIdOriginal,
        short_name AS shortName,
        long_name AS longName,
        hw_model AS hwModel,
        hw_model_label AS hwModelLabel,
        role,
        role_label AS roleLabel,
        latitude,
        longitude,
        altitude,
        last_seen_at AS lastSeenAt
      FROM nodes
    `);

    this.statements.deleteMessages = this.db.prepare('DELETE FROM message_log');
    this.statements.deleteMessageNodes = this.db.prepare('DELETE FROM message_nodes');
    this.statements.deleteMessageExtraLines = this.db.prepare('DELETE FROM message_extra_lines');
    this.statements.insertMessageEntry = this.db.prepare(INSERT_MESSAGE_ENTRY_SQL);
    this.statements.insertMessageNode = this.db.prepare(INSERT_MESSAGE_NODE_SQL);
    this.statements.insertMessageExtraLine = this.db.prepare(INSERT_MESSAGE_EXTRA_LINE_SQL);
    this.statements.selectMessages = this.db.prepare(`
      SELECT
        flow_id AS flowId,
        channel,
        timestamp_ms AS timestampMs,
        timestamp_label AS timestampLabel,
        type,
        detail,
        scope,
        mesh_packet_id AS meshPacketId,
        reply_id AS replyId,
        reply_to AS replyTo,
        relay_mesh_id AS relayMeshId,
        relay_mesh_id_normalized AS relayMeshIdNormalized,
        hops_start AS hopsStart,
        hops_limit AS hopsLimit,
        hops_label AS hopsLabel,
        synthetic,
        position,
        snr,
        rssi,
        raw_hex AS rawHex,
        raw_length AS rawLength
      FROM message_log
      ORDER BY channel ASC, position ASC
    `);
    this.statements.selectMessageNodes = this.db.prepare(`
      SELECT
        flow_id AS flowId,
        role,
        mesh_id AS meshId,
        mesh_id_normalized AS meshIdNormalized,
        mesh_id_original AS meshIdOriginal,
        long_name AS longName,
        short_name AS shortName,
        label
      FROM message_nodes
    `);
    this.statements.selectMessageExtraLines = this.db.prepare(`
      SELECT flow_id AS flowId, line_index AS lineIndex, content
      FROM message_extra_lines
      ORDER BY flow_id ASC, line_index ASC
    `);

    this.statements.clearRelayStats = this.db.prepare('DELETE FROM relay_stats');
    this.statements.upsertRelayStat = this.db.prepare(`
      INSERT INTO relay_stats (mesh_key, snr, rssi, count, updated_at)
      VALUES (@meshKey, @snr, @rssi, @count, @updatedAt)
      ON CONFLICT(mesh_key) DO UPDATE SET
        snr = excluded.snr,
        rssi = excluded.rssi,
        count = excluded.count,
        updated_at = excluded.updated_at
    `);
    this.statements.selectRelayStats = this.db.prepare(`
      SELECT mesh_key AS meshKey, snr, rssi, count, updated_at AS updatedAt
      FROM relay_stats
    `);

    this.statements.clearAprsPacketCache = this.db.prepare('DELETE FROM aprs_packet_cache');
    this.statements.insertAprsPacketCache = this.db.prepare(`
      INSERT INTO aprs_packet_cache (packet_key, callsign, info, last_seen_ms)
      VALUES (@packetKey, @callsign, @info, @lastSeenMs)
    `);
    this.statements.selectAprsPacketCache = this.db.prepare(`
      SELECT packet_key AS packetKey, callsign, info, last_seen_ms AS lastSeenMs
      FROM aprs_packet_cache
    `);

    this.statements.clearAprsLocalTx = this.db.prepare('DELETE FROM aprs_local_tx');
    this.statements.insertAprsLocalTx = this.db.prepare(`
      INSERT INTO aprs_local_tx (packet_key, callsign, info, last_seen_ms)
      VALUES (@packetKey, @callsign, @info, @lastSeenMs)
    `);
    this.statements.selectAprsLocalTx = this.db.prepare(`
      SELECT packet_key AS packetKey, callsign, info, last_seen_ms AS lastSeenMs
      FROM aprs_local_tx
    `);

    this.statements.clearAprsCallsignSummary = this.db.prepare('DELETE FROM aprs_callsign_summary');
    this.statements.insertAprsCallsignSummary = this.db.prepare(`
      INSERT INTO aprs_callsign_summary (callsign, last_seen_ms, last_info)
      VALUES (@callsign, @lastSeenMs, @lastInfo)
      ON CONFLICT(callsign) DO UPDATE SET
        last_seen_ms = excluded.last_seen_ms,
        last_info = excluded.last_info
    `);
    this.statements.selectAprsCallsignSummary = this.db.prepare(`
      SELECT callsign, last_seen_ms AS lastSeenMs, last_info AS lastInfo
      FROM aprs_callsign_summary
    `);

    this.statements.clearAprsPositionDigest = this.db.prepare('DELETE FROM aprs_position_digest');
    this.statements.insertAprsPositionDigest = this.db.prepare(`
      INSERT INTO aprs_position_digest (mesh_id, digest, timestamp_ms)
      VALUES (@meshId, @digest, @timestampMs)
      ON CONFLICT(mesh_id) DO UPDATE SET
        digest = excluded.digest,
        timestamp_ms = excluded.timestamp_ms
    `);
    this.statements.selectAprsPositionDigest = this.db.prepare(`
      SELECT mesh_id AS meshId, digest, timestamp_ms AS timestampMs
      FROM aprs_position_digest
    `);

    this.statements.clearAprsBacktrackState = this.db.prepare('DELETE FROM aprs_backtrack_state');
    this.statements.insertAprsBacktrackState = this.db.prepare(`
      INSERT INTO aprs_backtrack_state (
        callsign,
        last_lat,
        last_lon,
        last_at,
        prev_lat,
        prev_lon,
        prev_at,
        pending_lat,
        pending_lon,
        pending_first_seen_ms,
        pending_reason,
        pending_last_seen_ms,
        mode,
        mode_updated_ms,
        history_json
      )
      VALUES (
        @callsign,
        @lastLat,
        @lastLon,
        @lastAt,
        @prevLat,
        @prevLon,
        @prevAt,
        @pendingLat,
        @pendingLon,
        @pendingFirstSeenMs,
        @pendingReason,
        @pendingLastSeenMs,
        @mode,
        @modeUpdatedMs,
        @historyJson
      )
      ON CONFLICT(callsign) DO UPDATE SET
        last_lat = excluded.last_lat,
        last_lon = excluded.last_lon,
        last_at = excluded.last_at,
        prev_lat = excluded.prev_lat,
        prev_lon = excluded.prev_lon,
        prev_at = excluded.prev_at,
        pending_lat = excluded.pending_lat,
        pending_lon = excluded.pending_lon,
        pending_first_seen_ms = excluded.pending_first_seen_ms,
        pending_reason = excluded.pending_reason,
        pending_last_seen_ms = excluded.pending_last_seen_ms,
        mode = excluded.mode,
        mode_updated_ms = excluded.mode_updated_ms,
        history_json = excluded.history_json
    `);
    this.statements.selectAprsBacktrackState = this.db.prepare(`
      SELECT
        callsign,
        last_lat AS lastLat,
        last_lon AS lastLon,
        last_at AS lastAt,
        prev_lat AS prevLat,
        prev_lon AS prevLon,
        prev_at AS prevAt,
        pending_lat AS pendingLat,
        pending_lon AS pendingLon,
        pending_first_seen_ms AS pendingFirstSeenMs,
        pending_reason AS pendingReason,
        pending_last_seen_ms AS pendingLastSeenMs,
        mode,
        mode_updated_ms AS modeUpdatedMs,
        history_json AS historyJson
      FROM aprs_backtrack_state
    `);
  }

  _createTransaction(fn) {
    return (...args) => {
      if (!this.db) {
        throw new Error('CallMeshDataStore 尚未初始化');
      }
      this.db.exec('BEGIN IMMEDIATE');
      try {
        const result = fn(...args);
        this.db.exec('COMMIT');
        return result;
      } catch (err) {
        try {
          if (this.db?.isOpen) {
            this.db.exec('ROLLBACK');
          }
        } catch (rollbackErr) {
          // eslint-disable-next-line no-console
          console.warn(`CallMeshDataStore: rollback failed (${rollbackErr.message})`);
        }
        throw err;
      }
    };
  }

  _migrateLegacyMessageLog() {
    const tableInfo = this.db
      .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='message_log'")
      .get();
    if (!tableInfo) {
      return;
    }
    const columns = this.db.prepare('PRAGMA table_info(message_log)').all();
    const hasLegacyDataColumn = columns.some((col) => col.name === 'data');
    if (!hasLegacyDataColumn) {
      return;
    }

    const legacyRows = this.db
      .prepare(
        'SELECT flow_id, channel, timestamp_ms, position, data FROM message_log ORDER BY channel ASC, position ASC'
      )
      .all();

    this.db.exec('BEGIN');
    try {
      this.db.exec('ALTER TABLE message_log RENAME TO message_log_legacy');
      this.db.exec('DROP TABLE IF EXISTS message_nodes');
      this.db.exec('DROP TABLE IF EXISTS message_extra_lines');
      this.db.exec(`
        CREATE TABLE message_log (
          flow_id TEXT PRIMARY KEY,
          channel INTEGER NOT NULL,
          timestamp_ms INTEGER NOT NULL,
          timestamp_label TEXT,
          type TEXT,
          detail TEXT,
          scope TEXT,
          mesh_packet_id INTEGER,
          reply_id INTEGER,
          reply_to TEXT,
          relay_mesh_id TEXT,
          relay_mesh_id_normalized TEXT,
          hops_start INTEGER,
          hops_limit INTEGER,
          hops_label TEXT,
          synthetic INTEGER NOT NULL DEFAULT 0,
          position INTEGER NOT NULL,
          snr REAL,
          rssi REAL,
          raw_hex TEXT,
          raw_length INTEGER
        );
      `);
      this.db.exec(`
        CREATE INDEX IF NOT EXISTS idx_message_log_channel_position
          ON message_log(channel, position);
      `);
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS message_nodes (
          flow_id TEXT NOT NULL,
          role TEXT NOT NULL,
          mesh_id TEXT,
          mesh_id_normalized TEXT,
          mesh_id_original TEXT,
          long_name TEXT,
          short_name TEXT,
          label TEXT,
          PRIMARY KEY (flow_id, role),
          FOREIGN KEY (flow_id) REFERENCES message_log(flow_id) ON DELETE CASCADE
        );
      `);
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS message_extra_lines (
          flow_id TEXT NOT NULL,
          line_index INTEGER NOT NULL,
          content TEXT NOT NULL,
          PRIMARY KEY (flow_id, line_index),
          FOREIGN KEY (flow_id) REFERENCES message_log(flow_id) ON DELETE CASCADE
        );
      `);

      const insertEntry = this.db.prepare(INSERT_MESSAGE_ENTRY_SQL);
      const insertNode = this.db.prepare(INSERT_MESSAGE_NODE_SQL);
      const insertExtraLine = this.db.prepare(INSERT_MESSAGE_EXTRA_LINE_SQL);

      const migrate = this._createTransaction((rows) => {
        let fallbackPosition = 0;
        for (const row of rows) {
          if (!row) {
            fallbackPosition += 1;
            continue;
          }
          let parsed = null;
          if (row.data) {
            try {
              parsed = JSON.parse(row.data);
            } catch {
              parsed = null;
            }
          }
          const fallbackChannel = Number.isFinite(row.channel) ? Math.trunc(row.channel) : 0;
          const fallbackTimestamp = Number.isFinite(row.timestamp_ms)
            ? Math.trunc(row.timestamp_ms)
            : Date.now();
          const fallbackFlowId =
            sanitizeText(row.flow_id) ||
            `${fallbackChannel}-${fallbackTimestamp}-${fallbackPosition}`;
          const sanitized = sanitizeMessageEntry(parsed || {}, {
            fallbackFlowId,
            fallbackChannel,
            fallbackTimestampMs: fallbackTimestamp
          });
          if (!sanitized) {
            fallbackPosition += 1;
            continue;
          }
          const positionValue = Number.isFinite(row.position)
            ? Math.trunc(row.position)
            : fallbackPosition;
          insertEntry.run({
            flowId: sanitized.flowId,
            channel: sanitized.channel,
            timestampMs: sanitized.timestampMs,
            timestampLabel: sanitized.timestampLabel,
            type: sanitized.type,
            detail: sanitized.detail,
            scope: sanitized.scope,
            meshPacketId: sanitized.meshPacketId,
            replyId: sanitized.replyId,
            replyTo: sanitized.replyTo,
            relayMeshId: sanitized.relayMeshId,
            relayMeshIdNormalized: sanitized.relayMeshIdNormalized,
            hopsStart: sanitized.hops?.start ?? null,
            hopsLimit: sanitized.hops?.limit ?? null,
            hopsLabel: sanitized.hops?.label ?? null,
            synthetic: sanitized.synthetic ? 1 : 0,
            position: positionValue,
            snr: sanitized.snr,
            rssi: sanitized.rssi,
            rawHex: sanitized.rawHex,
            rawLength: sanitized.rawLength
          });
          if (sanitized.fromNode) {
            insertNode.run({
              flowId: sanitized.flowId,
              role: 'from',
              meshId: sanitized.fromNode.meshId ?? null,
              meshIdNormalized: sanitized.fromNode.meshIdNormalized ?? null,
              meshIdOriginal: sanitized.fromNode.meshIdOriginal ?? null,
              longName: sanitized.fromNode.longName ?? null,
              shortName: sanitized.fromNode.shortName ?? null,
              label: sanitized.fromNode.label ?? null
            });
          }
          if (sanitized.relayNode) {
            insertNode.run({
              flowId: sanitized.flowId,
              role: 'relay',
              meshId: sanitized.relayNode.meshId ?? null,
              meshIdNormalized: sanitized.relayNode.meshIdNormalized ?? null,
              meshIdOriginal: sanitized.relayNode.meshIdOriginal ?? null,
              longName: sanitized.relayNode.longName ?? null,
              shortName: sanitized.relayNode.shortName ?? null,
              label: sanitized.relayNode.label ?? null
            });
          }
          sanitized.extraLines.forEach((line, index) => {
            insertExtraLine.run({
              flowId: sanitized.flowId,
              lineIndex: index,
              content: line
            });
          });
          fallbackPosition = positionValue + 1;
        }
      });

      migrate(legacyRows);
      this.db.exec('DROP TABLE message_log_legacy');
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw new Error(`遷移舊版 message_log 失敗: ${err.message}`);
    }
  }


  close() {
    if (this.db) {
      this.db.close();
      this.db = null;
      this.statements = Object.create(null);
    }
  }

  setKv(key, value) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    this.statements.upsertKv.run({
      key,
      value: toJson(value),
      updatedAt: Date.now()
    });
  }

  deleteKv(key) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    this.statements.deleteKv.run(key);
  }

  getKv(key) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const row = this.statements.getKv.get(key);
    if (!row) return null;
    return fromJson(row.value);
  }

  replaceNodes(nodes = []) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const now = Date.now();
    const insert = this.statements.insertNode;
    const exec = this._createTransaction((entries) => {
      this.statements.deleteNodes.run();
      for (const entry of entries) {
        if (!entry || typeof entry !== 'object') continue;
        insert.run({
          meshId: entry.meshId,
          meshIdOriginal: entry.meshIdOriginal ?? null,
          shortName: entry.shortName ?? null,
          longName: entry.longName ?? null,
          hwModel: entry.hwModel ?? null,
          hwModelLabel: entry.hwModelLabel ?? null,
          role: entry.role ?? null,
          roleLabel: entry.roleLabel ?? null,
          latitude: Number.isFinite(entry.latitude) ? entry.latitude : null,
          longitude: Number.isFinite(entry.longitude) ? entry.longitude : null,
          altitude: Number.isFinite(entry.altitude) ? entry.altitude : null,
          lastSeenAt: Number.isFinite(entry.lastSeenAt) ? Math.floor(entry.lastSeenAt) : null,
          updatedAt: now
        });
      }
    });
    exec(nodes);
  }

  listNodes() {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    return this.statements.selectNodes.all().map((row) => ({
      ...row,
      latitude: Number.isFinite(row.latitude) ? row.latitude : null,
      longitude: Number.isFinite(row.longitude) ? row.longitude : null,
      altitude: Number.isFinite(row.altitude) ? row.altitude : null,
      lastSeenAt: Number.isFinite(row.lastSeenAt) ? row.lastSeenAt : null
    }));
  }

  clearNodes() {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    this.statements.deleteNodes.run();
  }

  saveMessageLog(entries = []) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const insertEntry = this.statements.insertMessageEntry;
    const insertNode = this.statements.insertMessageNode;
    const insertExtraLine = this.statements.insertMessageExtraLine;
    const exec = this._createTransaction((items) => {
      this.statements.deleteMessages.run();
      this.statements.deleteMessageNodes.run();
      this.statements.deleteMessageExtraLines.run();
      let position = 0;
      for (const entry of items) {
        if (!entry || typeof entry !== 'object') {
          continue;
        }
        const fallbackChannel = Number.isFinite(entry.channel) ? Number(entry.channel) : 0;
        const fallbackTimestamp = Number.isFinite(entry.timestampMs)
          ? Number(entry.timestampMs)
          : Date.now();
        const fallbackFlowId =
          typeof entry.flowId === 'string' && entry.flowId.trim()
            ? entry.flowId.trim()
            : `${fallbackChannel}-${fallbackTimestamp}-${position}`;
        const sanitized = sanitizeMessageEntry(entry, {
          fallbackFlowId,
          fallbackChannel,
          fallbackTimestampMs: fallbackTimestamp
        });
        if (!sanitized) {
          continue;
        }
        insertEntry.run({
          flowId: sanitized.flowId,
          channel: sanitized.channel,
          timestampMs: sanitized.timestampMs,
          timestampLabel: sanitized.timestampLabel,
          type: sanitized.type,
          detail: sanitized.detail,
          scope: sanitized.scope,
          meshPacketId: sanitized.meshPacketId,
          replyId: sanitized.replyId,
          replyTo: sanitized.replyTo,
          relayMeshId: sanitized.relayMeshId,
          relayMeshIdNormalized: sanitized.relayMeshIdNormalized,
          hopsStart: sanitized.hops?.start ?? null,
          hopsLimit: sanitized.hops?.limit ?? null,
          hopsLabel: sanitized.hops?.label ?? null,
          synthetic: sanitized.synthetic ? 1 : 0,
          position,
          snr: sanitized.snr,
          rssi: sanitized.rssi,
          rawHex: sanitized.rawHex,
          rawLength: sanitized.rawLength
        });
        if (sanitized.fromNode) {
          insertNode.run({
            flowId: sanitized.flowId,
            role: 'from',
            meshId: sanitized.fromNode.meshId ?? null,
            meshIdNormalized: sanitized.fromNode.meshIdNormalized ?? null,
            meshIdOriginal: sanitized.fromNode.meshIdOriginal ?? null,
            longName: sanitized.fromNode.longName ?? null,
            shortName: sanitized.fromNode.shortName ?? null,
            label: sanitized.fromNode.label ?? null
          });
        }
        if (sanitized.relayNode) {
          insertNode.run({
            flowId: sanitized.flowId,
            role: 'relay',
            meshId: sanitized.relayNode.meshId ?? null,
            meshIdNormalized: sanitized.relayNode.meshIdNormalized ?? null,
            meshIdOriginal: sanitized.relayNode.meshIdOriginal ?? null,
            longName: sanitized.relayNode.longName ?? null,
            shortName: sanitized.relayNode.shortName ?? null,
            label: sanitized.relayNode.label ?? null
          });
        }
        sanitized.extraLines.forEach((line, index) => {
          insertExtraLine.run({
            flowId: sanitized.flowId,
            lineIndex: index,
            content: line
          });
        });
        position += 1;
      }
    });
    exec(entries);
  }

  loadMessageLog() {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const rows = this.statements.selectMessages.all();
    if (!rows.length) {
      return [];
    }
    const nodeRows = this.statements.selectMessageNodes.all();
    const extraLineRows = this.statements.selectMessageExtraLines.all();

    const nodeMap = new Map();
    for (const row of nodeRows) {
      if (!row || !row.flowId || !row.role) continue;
      const key = `${row.flowId}:${row.role}`;
      nodeMap.set(key, buildMessageNodeFromRow(row));
    }

    const extraLineMap = new Map();
    for (const row of extraLineRows) {
      if (!row || !row.flowId) continue;
      const content = sanitizeText(row.content);
      if (!content) continue;
      if (!extraLineMap.has(row.flowId)) {
        extraLineMap.set(row.flowId, []);
      }
      extraLineMap.get(row.flowId).push({ index: Number(row.lineIndex) || 0, content });
    }
    for (const [, list] of extraLineMap.entries()) {
      list.sort((a, b) => a.index - b.index);
    }

    return rows.map((row) => {
      const flowId = row.flowId;
      const channel = Number.isFinite(row.channel) ? Number(row.channel) : 0;
      const timestampMs = Number.isFinite(row.timestampMs) ? Number(row.timestampMs) : Date.now();
      const timestampLabel =
        sanitizeText(row.timestampLabel) || new Date(timestampMs).toISOString();
      const relayMeshId = sanitizeText(row.relayMeshId);
      const relayMeshIdNormalized =
        sanitizeText(row.relayMeshIdNormalized) || normalizeMeshIdValue(relayMeshId);
      const hopsStart = toFiniteInteger(row.hopsStart);
      const hopsLimit = toFiniteInteger(row.hopsLimit);
      const hopsLabel = sanitizeText(row.hopsLabel);
      const hops =
        hopsStart != null || hopsLimit != null || hopsLabel
          ? {
              start: hopsStart,
              limit: hopsLimit,
              label: hopsLabel ?? null
            }
          : null;
      const extraLinesRaw = extraLineMap.get(flowId) || [];
      const extraLines = extraLinesRaw.map((item) => item.content);
      const fromNode = nodeMap.get(`${flowId}:from`) || null;
      const relayNode = nodeMap.get(`${flowId}:relay`) || null;
      const result = {
        type: row.type || 'Text',
        channel,
        detail: row.detail || '',
        extraLines,
        from: fromNode,
        relay: relayNode,
        relayMeshId: relayMeshId ?? null,
        relayMeshIdNormalized: relayMeshIdNormalized ?? null,
        hops,
        timestampMs,
        timestampLabel,
        flowId,
        meshPacketId: toFiniteInteger(row.meshPacketId),
        replyId: toFiniteInteger(row.replyId),
        replyTo: sanitizeText(row.replyTo),
        scope: sanitizeText(row.scope),
        synthetic: Boolean(row.synthetic)
      };
      const snr = toFiniteNumber(row.snr);
      if (snr != null) {
        result.snr = snr;
      }
      const rssi = toFiniteNumber(row.rssi);
      if (rssi != null) {
        result.rssi = rssi;
      }
      const rawHex = sanitizeText(row.rawHex);
      if (rawHex) {
        result.rawHex = rawHex;
      }
      const rawLength = toFiniteInteger(row.rawLength);
      if (rawLength != null) {
        result.rawLength = rawLength;
      }
      return result;
    });
  }

  replaceRelayStats(entries = []) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const upsert = this.statements.upsertRelayStat;
    const exec = this._createTransaction((items) => {
      this.statements.clearRelayStats.run();
      for (const entry of items) {
        if (!entry || typeof entry !== 'object' || !entry.meshKey) continue;
        upsert.run({
          meshKey: entry.meshKey,
          snr: Number.isFinite(entry.snr) ? entry.snr : null,
          rssi: Number.isFinite(entry.rssi) ? entry.rssi : null,
          count: Number.isFinite(entry.count) ? Math.max(1, Math.round(entry.count)) : 1,
          updatedAt: Number.isFinite(entry.updatedAt) ? Math.floor(entry.updatedAt) : Date.now()
        });
      }
    });
    exec(entries);
  }

  clearRelayStats() {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    this.statements.clearRelayStats.run();
  }

  upsertRelayStat(entry) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    if (!entry || typeof entry !== 'object' || !entry.meshKey) {
      return;
    }
    this.statements.upsertRelayStat.run({
      meshKey: entry.meshKey,
      snr: Number.isFinite(entry.snr) ? entry.snr : null,
      rssi: Number.isFinite(entry.rssi) ? entry.rssi : null,
      count: Number.isFinite(entry.count) ? Math.max(1, Math.round(entry.count)) : 1,
      updatedAt: Number.isFinite(entry.updatedAt) ? Math.floor(entry.updatedAt) : Date.now()
    });
  }

  listRelayStats() {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    return this.statements.selectRelayStats.all();
  }

  saveAprsDedupSnapshot(snapshot = {}) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const packetEntries = Array.isArray(snapshot.packetCache) ? snapshot.packetCache : [];
    const localEntries = Array.isArray(snapshot.localTxHistory) ? snapshot.localTxHistory : [];
    const callsignEntries = Array.isArray(snapshot.callsignSummary) ? snapshot.callsignSummary : [];
    const digestEntries = Array.isArray(snapshot.positionDigest) ? snapshot.positionDigest : [];
    const exec = this._createTransaction((payload) => {
      this.statements.clearAprsPacketCache.run();
      for (const entry of payload.packetEntries) {
        const sanitized = sanitizeAprsPacketSnapshotEntry(entry);
        if (!sanitized) continue;
        this.statements.insertAprsPacketCache.run(sanitized);
      }

      this.statements.clearAprsLocalTx.run();
      for (const entry of payload.localEntries) {
        const sanitized = sanitizeAprsPacketSnapshotEntry(entry);
        if (!sanitized) continue;
        this.statements.insertAprsLocalTx.run(sanitized);
      }

      this.statements.clearAprsCallsignSummary.run();
      for (const entry of payload.callsignEntries) {
        const sanitized = sanitizeAprsCallsignSummaryEntry(entry);
        if (!sanitized) continue;
        this.statements.insertAprsCallsignSummary.run({
          callsign: sanitized.callsign,
          lastSeenMs: sanitized.lastSeenMs,
          lastInfo: sanitized.lastInfo
        });
      }

      this.statements.clearAprsPositionDigest.run();
      for (const entry of payload.digestEntries) {
        const sanitized = sanitizeAprsPositionDigestEntry(entry);
        if (!sanitized) continue;
        this.statements.insertAprsPositionDigest.run({
          meshId: sanitized.meshId,
          digest: sanitized.digest,
          timestampMs: sanitized.timestampMs
        });
      }
    });
    exec({
      packetEntries,
      localEntries,
      callsignEntries,
      digestEntries
    });
  }

  loadAprsDedupSnapshot() {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const packetCache = this.statements.selectAprsPacketCache.all().map((row) => ({
      packetKey: sanitizeText(row.packetKey),
      callsign: sanitizeText(row.callsign),
      infoString:
        row.info === undefined || row.info === null ? null : String(row.info),
      lastSeenMs: toFiniteInteger(row.lastSeenMs)
    }));
    const localTxHistory = this.statements.selectAprsLocalTx.all().map((row) => ({
      packetKey: sanitizeText(row.packetKey),
      callsign: sanitizeText(row.callsign),
      infoString:
        row.info === undefined || row.info === null ? null : String(row.info),
      lastSeenMs: toFiniteInteger(row.lastSeenMs)
    }));
    const callsignSummary = this.statements.selectAprsCallsignSummary.all().map((row) => ({
      callsign: sanitizeText(row.callsign),
      lastSeenMs: toFiniteInteger(row.lastSeenMs),
      lastInfo:
        row.lastInfo === undefined || row.lastInfo === null ? null : String(row.lastInfo)
    }));
    const positionDigest = this.statements.selectAprsPositionDigest.all().map((row) => ({
      meshId: sanitizeText(row.meshId),
      digest: sanitizeText(row.digest),
      timestampMs: toFiniteInteger(row.timestampMs)
    }));
    return {
      packetCache: packetCache.filter((entry) => entry.packetKey && entry.lastSeenMs != null),
      localTxHistory: localTxHistory.filter(
        (entry) => entry.packetKey && entry.lastSeenMs != null
      ),
      callsignSummary: callsignSummary.filter((entry) => entry.callsign),
      positionDigest: positionDigest.filter((entry) => entry.meshId)
    };
  }

  saveAprsBacktrackState(entries = []) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const exec = this._createTransaction((items) => {
      this.statements.clearAprsBacktrackState.run();
      for (const entry of items) {
        const sanitized = sanitizeAprsBacktrackStateEntry(entry);
        if (!sanitized) continue;
        const historyJson =
          Array.isArray(sanitized.history) && sanitized.history.length
            ? toJson(sanitized.history)
            : null;
        this.statements.insertAprsBacktrackState.run({
          callsign: sanitized.callsign,
          lastLat: sanitized.lastUploaded?.lat ?? null,
          lastLon: sanitized.lastUploaded?.lon ?? null,
          lastAt: sanitized.lastUploaded?.timestampMs ?? null,
          prevLat: sanitized.prevUploaded?.lat ?? null,
          prevLon: sanitized.prevUploaded?.lon ?? null,
          prevAt: sanitized.prevUploaded?.timestampMs ?? null,
          pendingLat: sanitized.pending?.lat ?? null,
          pendingLon: sanitized.pending?.lon ?? null,
          pendingFirstSeenMs: sanitized.pendingFirstSeenMs ?? null,
          pendingReason: sanitized.pendingReason ?? null,
          pendingLastSeenMs: sanitized.pendingLastSeenMs ?? null,
          mode: sanitized.mode ?? null,
          modeUpdatedMs: sanitized.modeUpdatedMs ?? null,
          historyJson
        });
      }
    });
    exec(entries);
  }

  loadAprsBacktrackState() {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const rows = this.statements.selectAprsBacktrackState.all();
    return rows.map((row) => {
      const history = sanitizeAprsBacktrackHistory(fromJson(row.historyJson));
      return {
        callsign: sanitizeText(row.callsign),
        lastUploaded: sanitizeAprsBacktrackPosition({
          lat: row.lastLat,
          lon: row.lastLon,
          timestampMs: row.lastAt
        }),
        prevUploaded: sanitizeAprsBacktrackPosition({
          lat: row.prevLat,
          lon: row.prevLon,
          timestampMs: row.prevAt
        }),
        pending: sanitizeAprsBacktrackPosition({
          lat: row.pendingLat,
          lon: row.pendingLon,
          timestampMs: row.pendingLastSeenMs ?? row.pendingFirstSeenMs
        }),
        pendingFirstSeenMs: toFiniteInteger(row.pendingFirstSeenMs),
        pendingReason: sanitizeText(row.pendingReason),
        pendingLastSeenMs: toFiniteInteger(row.pendingLastSeenMs),
        mode: sanitizeText(row.mode),
        modeUpdatedMs: toFiniteInteger(row.modeUpdatedMs),
        history
      };
    });
  }
}

const INSERT_MESSAGE_ENTRY_SQL = `
  INSERT INTO message_log (
    flow_id,
    channel,
    timestamp_ms,
    timestamp_label,
    type,
    detail,
    scope,
    mesh_packet_id,
    reply_id,
    reply_to,
    relay_mesh_id,
    relay_mesh_id_normalized,
    hops_start,
    hops_limit,
    hops_label,
    synthetic,
    position,
    snr,
    rssi,
    raw_hex,
    raw_length
  )
  VALUES (
    @flowId,
    @channel,
    @timestampMs,
    @timestampLabel,
    @type,
    @detail,
    @scope,
    @meshPacketId,
    @replyId,
    @replyTo,
    @relayMeshId,
    @relayMeshIdNormalized,
    @hopsStart,
    @hopsLimit,
    @hopsLabel,
    @synthetic,
    @position,
    @snr,
    @rssi,
    @rawHex,
    @rawLength
  )
  ON CONFLICT(flow_id) DO UPDATE SET
    channel = excluded.channel,
    timestamp_ms = excluded.timestamp_ms,
    timestamp_label = excluded.timestamp_label,
    type = excluded.type,
    detail = excluded.detail,
    scope = excluded.scope,
    mesh_packet_id = excluded.mesh_packet_id,
    reply_id = excluded.reply_id,
    reply_to = excluded.reply_to,
    relay_mesh_id = excluded.relay_mesh_id,
    relay_mesh_id_normalized = excluded.relay_mesh_id_normalized,
    hops_start = excluded.hops_start,
    hops_limit = excluded.hops_limit,
    hops_label = excluded.hops_label,
    synthetic = excluded.synthetic,
    position = excluded.position,
    snr = excluded.snr,
    rssi = excluded.rssi,
    raw_hex = excluded.raw_hex,
    raw_length = excluded.raw_length
`;

const INSERT_MESSAGE_NODE_SQL = `
  INSERT INTO message_nodes (
    flow_id,
    role,
    mesh_id,
    mesh_id_normalized,
    mesh_id_original,
    long_name,
    short_name,
    label
  )
  VALUES (
    @flowId,
    @role,
    @meshId,
    @meshIdNormalized,
    @meshIdOriginal,
    @longName,
    @shortName,
    @label
  )
  ON CONFLICT(flow_id, role) DO UPDATE SET
    mesh_id = excluded.mesh_id,
    mesh_id_normalized = excluded.mesh_id_normalized,
    mesh_id_original = excluded.mesh_id_original,
    long_name = excluded.long_name,
    short_name = excluded.short_name,
    label = excluded.label
`;

const INSERT_MESSAGE_EXTRA_LINE_SQL = `
  INSERT INTO message_extra_lines (flow_id, line_index, content)
  VALUES (@flowId, @lineIndex, @content)
  ON CONFLICT(flow_id, line_index) DO UPDATE SET
    content = excluded.content
`;

module.exports = {
  CallMeshDataStore
};
