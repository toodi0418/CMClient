'use strict';

const fs = require('fs');
const path = require('path');

let DatabaseSync;
try {
  ({ DatabaseSync } = require('node:sqlite'));
} catch (err) {
  throw new Error('TelemetryDatabase 需要 Node.js 22 的 node:sqlite 模組，請升級執行環境。');
}

const INSERT_RECORD_SQL = `
  INSERT INTO telemetry_records (
    id,
    mesh_id,
    mesh_id_normalized,
    timestamp_ms,
    timestamp_iso,
    sample_time_ms,
    sample_time_iso,
    type,
    detail,
    channel,
    snr,
    rssi,
    flow_id,
    relay_mesh_id,
    relay_mesh_id_normalized,
    relay_label,
    relay_guessed,
    relay_guess_reason,
    hops_start,
    hops_limit,
    hops_label,
    hops_used,
    hops_total,
    telemetry_kind,
    telemetry_time_ms,
    telemetry_time_seconds,
    node_mesh_id,
    node_mesh_id_normalized,
    node_short_name,
    node_long_name,
    node_label,
    node_hw_model,
    node_hw_model_label,
    node_role,
    node_role_label,
    node_latitude,
    node_longitude,
    node_altitude,
    node_last_seen_at,
    created_at
  )
  VALUES (
    @id,
    @meshId,
    @meshIdNormalized,
    @timestampMs,
    @timestampIso,
    @sampleTimeMs,
    @sampleTimeIso,
    @type,
    @detail,
    @channel,
    @snr,
    @rssi,
    @flowId,
    @relayMeshId,
    @relayMeshIdNormalized,
    @relayLabel,
    @relayGuessed,
    @relayGuessReason,
    @hopsStart,
    @hopsLimit,
    @hopsLabel,
    @hopsUsed,
    @hopsTotal,
    @telemetryKind,
    @telemetryTimeMs,
    @telemetryTimeSeconds,
    @nodeMeshId,
    @nodeMeshIdNormalized,
    @nodeShortName,
    @nodeLongName,
    @nodeLabel,
    @nodeHwModel,
    @nodeHwModelLabel,
    @nodeRole,
    @nodeRoleLabel,
    @nodeLatitude,
    @nodeLongitude,
    @nodeAltitude,
    @nodeLastSeenAt,
    @createdAt
  )
  ON CONFLICT(id) DO UPDATE SET
    mesh_id = excluded.mesh_id,
    mesh_id_normalized = excluded.mesh_id_normalized,
    timestamp_ms = excluded.timestamp_ms,
    timestamp_iso = excluded.timestamp_iso,
    sample_time_ms = excluded.sample_time_ms,
    sample_time_iso = excluded.sample_time_iso,
    type = excluded.type,
    detail = excluded.detail,
    channel = excluded.channel,
    snr = excluded.snr,
    rssi = excluded.rssi,
    flow_id = excluded.flow_id,
    relay_mesh_id = excluded.relay_mesh_id,
    relay_mesh_id_normalized = excluded.relay_mesh_id_normalized,
    relay_label = excluded.relay_label,
    relay_guessed = excluded.relay_guessed,
    relay_guess_reason = excluded.relay_guess_reason,
    hops_start = excluded.hops_start,
    hops_limit = excluded.hops_limit,
    hops_label = excluded.hops_label,
    hops_used = excluded.hops_used,
    hops_total = excluded.hops_total,
    telemetry_kind = excluded.telemetry_kind,
    telemetry_time_ms = excluded.telemetry_time_ms,
    telemetry_time_seconds = excluded.telemetry_time_seconds,
    node_mesh_id = excluded.node_mesh_id,
    node_mesh_id_normalized = excluded.node_mesh_id_normalized,
    node_short_name = excluded.node_short_name,
    node_long_name = excluded.node_long_name,
    node_label = excluded.node_label,
    node_hw_model = excluded.node_hw_model,
    node_hw_model_label = excluded.node_hw_model_label,
    node_role = excluded.node_role,
    node_role_label = excluded.node_role_label,
    node_latitude = excluded.node_latitude,
    node_longitude = excluded.node_longitude,
    node_altitude = excluded.node_altitude,
    node_last_seen_at = excluded.node_last_seen_at,
    created_at = MIN(excluded.created_at, created_at)
`;

const INSERT_METRIC_SQL = `
  INSERT INTO telemetry_metrics (
    record_id,
    metric_key,
    number_value,
    text_value,
    json_value
  )
  VALUES (@recordId, @metricKey, @numberValue, @textValue, @jsonValue)
  ON CONFLICT(record_id, metric_key) DO UPDATE SET
    number_value = excluded.number_value,
    text_value = excluded.text_value,
    json_value = excluded.json_value
`;

const DELETE_METRICS_SQL = 'DELETE FROM telemetry_metrics WHERE record_id = ?';
const SELECT_RECORDS_ASC_SQL = 'SELECT * FROM telemetry_records ORDER BY timestamp_ms ASC';
const SELECT_METRICS_ALL_SQL =
  'SELECT record_id, metric_key, number_value, text_value, json_value FROM telemetry_metrics';
const CLEAR_METRICS_SQL = 'DELETE FROM telemetry_metrics';
const CLEAR_RECORDS_SQL = 'DELETE FROM telemetry_records';
const COUNT_RECORDS_SQL = 'SELECT COUNT(*) AS count FROM telemetry_records';
const COUNT_DISTINCT_MESH_SQL = 'SELECT COUNT(DISTINCT mesh_id) AS nodes FROM telemetry_records';
const LIST_MESH_IDS_SQL = 'SELECT DISTINCT mesh_id FROM telemetry_records';
const SELECT_RECORDS_FOR_MESH_SQL = `
  SELECT *
  FROM telemetry_records
  WHERE mesh_id = @meshId
  ORDER BY timestamp_ms DESC
  LIMIT @limit
`;
const MESH_RECORD_COUNT_SQL = `
  SELECT
    mesh_id,
    COUNT(*) AS count,
    MAX(timestamp_ms) AS latest_timestamp_ms
  FROM telemetry_records
  GROUP BY mesh_id
`;
const FETCH_RECENT_SNAPSHOT_SQL = `
  SELECT *
  FROM (
    SELECT
      tr.*,
      ROW_NUMBER() OVER (PARTITION BY mesh_id ORDER BY timestamp_ms DESC) AS rn
    FROM telemetry_records tr
  )
  WHERE rn <= @limit
  ORDER BY mesh_id ASC, timestamp_ms DESC
`;

function ensureDirectory(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function toFiniteNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function toTimestampMs(value, fallback = Date.now()) {
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return Math.floor(numeric);
  }
  return Math.floor(fallback);
}

function normalizeMeshId(input) {
  if (!input) return null;
  const value = String(input).trim();
  if (!value || value === 'null' || value === 'undefined') {
    return null;
  }
  if (value.startsWith('!')) {
    return value.toLowerCase();
  }
  if (/^0x[0-9a-f]+$/i.test(value)) {
    return `!${value.slice(2).padStart(8, '0')}`.toLowerCase();
  }
  if (/^[0-9a-f]{8}$/i.test(value)) {
    return `!${value.toLowerCase()}`;
  }
  return value;
}

function serializeMetricValue(value) {
  if (value == null) {
    return {
      numberValue: null,
      textValue: null,
      jsonValue: null
    };
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return {
      numberValue: value,
      textValue: null,
      jsonValue: null
    };
  }
  if (typeof value === 'boolean') {
    return {
      numberValue: value ? 1 : 0,
      textValue: null,
      jsonValue: null
    };
  }
  if (typeof value === 'string') {
    return {
      numberValue: null,
      textValue: value,
      jsonValue: null
    };
  }
  return {
    numberValue: null,
    textValue: null,
    jsonValue: JSON.stringify(value)
  };
}

function deserializeMetricValue(row) {
  if (row == null) {
    return null;
  }
  if (row.number_value != null) {
    return row.number_value;
  }
  if (row.text_value != null) {
    return row.text_value;
  }
  if (row.json_value != null) {
    try {
      return JSON.parse(row.json_value);
    } catch {
      return row.json_value;
    }
  }
  return null;
}

function sanitizeText(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const str = String(value).trim();
  return str.length ? str : null;
}

function chunkArray(items, chunkSize) {
  if (!Array.isArray(items) || chunkSize <= 0) {
    return [];
  }
  const result = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    result.push(items.slice(i, i + chunkSize));
  }
  return result;
}

class TelemetryDatabase {
  constructor(filePath, options = {}) {
    if (!filePath) {
      throw new Error('TelemetryDatabase 需要提供檔案路徑');
    }
    this.filePath = filePath;
    this.options = options;
    this.db = null;
    this.statements = Object.create(null);
    this.supportsWindowFunctions = false;
  }

  init() {
    if (this.db) {
      return;
    }
    ensureDirectory(this.filePath);
    const openOptions = {};
    if (this.options.readonly) {
      openOptions.readOnly = true;
    }
    this.db = new DatabaseSync(this.filePath, openOptions);
    if (!openOptions.readOnly) {
      this.db.exec('PRAGMA journal_mode = WAL;');
    }
    this.db.exec('PRAGMA foreign_keys = ON;');
    this._applyMigrations();
    this._prepareStatements();
  }

  _prepareStatements() {
    this.statements.insertRecord = this.db.prepare(INSERT_RECORD_SQL);
    this.statements.deleteMetricsForRecord = this.db.prepare(DELETE_METRICS_SQL);
    this.statements.insertMetric = this.db.prepare(INSERT_METRIC_SQL);
    this.statements.selectRecordsAsc = this.db.prepare(SELECT_RECORDS_ASC_SQL);
    this.statements.selectMetricsAll = this.db.prepare(SELECT_METRICS_ALL_SQL);
    this.statements.clearMetrics = this.db.prepare(CLEAR_METRICS_SQL);
    this.statements.clearRecords = this.db.prepare(CLEAR_RECORDS_SQL);
    this.statements.countRecords = this.db.prepare(COUNT_RECORDS_SQL);
    this.statements.countDistinctMesh = this.db.prepare(COUNT_DISTINCT_MESH_SQL);
    this.statements.listMeshIds = this.db.prepare(LIST_MESH_IDS_SQL);
    this.statements.selectRecordsForMesh = this.db.prepare(SELECT_RECORDS_FOR_MESH_SQL);
    try {
      this.statements.fetchRecentSnapshot = this.db.prepare(FETCH_RECENT_SNAPSHOT_SQL);
      this.supportsWindowFunctions = true;
    } catch (err) {
      this.supportsWindowFunctions = false;
      this.statements.fetchRecentSnapshot = null;
      // eslint-disable-next-line no-console
      console.warn(`TelemetryDatabase: window functions unavailable, falling back to per-mesh queries (${err.message})`);
    }
  }

  _createTransaction(fn) {
    return (...args) => {
      if (!this.db) {
        throw new Error('TelemetryDatabase 尚未初始化');
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
          console.warn(`TelemetryDatabase: rollback failed (${rollbackErr.message})`);
        }
        throw err;
      }
    };
  }

  _applyMigrations() {
    const hasRecordsTable = this.db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='telemetry_records'"
      )
      .all();
    if (!hasRecordsTable.length) {
      this._createSchema();
      return;
    }
    const columnInfo = this.db.prepare('PRAGMA table_info(telemetry_records)').all();
    const hasLegacyDataColumn = columnInfo.some((col) => col.name === 'data');
    if (hasLegacyDataColumn) {
      this._migrateFromLegacyInlineJson();
    } else {
      this._ensureIndexes();
    }
  }

  _createSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS telemetry_records (
        id TEXT PRIMARY KEY,
        mesh_id TEXT NOT NULL,
        mesh_id_normalized TEXT,
        timestamp_ms INTEGER NOT NULL,
        timestamp_iso TEXT NOT NULL,
        sample_time_ms INTEGER NOT NULL,
        sample_time_iso TEXT NOT NULL,
        type TEXT,
        detail TEXT,
        channel INTEGER,
        snr REAL,
        rssi REAL,
        flow_id TEXT,
        relay_mesh_id TEXT,
        relay_mesh_id_normalized TEXT,
        relay_label TEXT,
        relay_guessed INTEGER,
        relay_guess_reason TEXT,
        hops_start INTEGER,
        hops_limit INTEGER,
        hops_label TEXT,
        hops_used INTEGER,
        hops_total INTEGER,
        telemetry_kind TEXT,
        telemetry_time_ms INTEGER,
        telemetry_time_seconds INTEGER,
        node_mesh_id TEXT,
        node_mesh_id_normalized TEXT,
        node_short_name TEXT,
        node_long_name TEXT,
        node_label TEXT,
        node_hw_model TEXT,
        node_hw_model_label TEXT,
        node_role TEXT,
        node_role_label TEXT,
        node_latitude REAL,
        node_longitude REAL,
        node_altitude REAL,
        node_last_seen_at INTEGER,
        created_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS telemetry_metrics (
        record_id TEXT NOT NULL,
        metric_key TEXT NOT NULL,
        number_value REAL,
        text_value TEXT,
        json_value TEXT,
        PRIMARY KEY (record_id, metric_key),
        FOREIGN KEY (record_id) REFERENCES telemetry_records(id) ON DELETE CASCADE
      );
    `);
    this._ensureIndexes();
  }

  _ensureIndexes() {
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_telemetry_records_mesh_time
        ON telemetry_records(mesh_id, timestamp_ms);

      CREATE INDEX IF NOT EXISTS idx_telemetry_records_time
        ON telemetry_records(timestamp_ms);

      CREATE INDEX IF NOT EXISTS idx_telemetry_metrics_record
        ON telemetry_metrics(record_id);
    `);
  }

  _migrateFromLegacyInlineJson() {
    const selectLegacy = this.db.prepare(
      'SELECT id, mesh_id, timestamp_ms, data FROM telemetry_records ORDER BY timestamp_ms ASC'
    );
    const legacyRows = selectLegacy.all();

    this.db.exec('BEGIN');
    try {
      this.db.exec('ALTER TABLE telemetry_records RENAME TO telemetry_records_legacy');
      this.db.exec('DROP TABLE IF EXISTS telemetry_metrics');
      this._createSchema();

      const insertRecord = this.db.prepare(INSERT_RECORD_SQL);
      const insertMetric = this.db.prepare(INSERT_METRIC_SQL);
      const deleteMetrics = this.db.prepare(DELETE_METRICS_SQL);

      const migrate = this._createTransaction((rows) => {
        for (const row of rows) {
          if (!row || !row.data) continue;
          let parsed = null;
          try {
            parsed = JSON.parse(row.data);
          } catch {
            parsed = null;
          }
          if (!parsed || typeof parsed !== 'object') {
            continue;
          }
          const serialized = this._serializeRecord(parsed);
          if (!serialized) {
            continue;
          }
          const { payload, metrics } = serialized;
          insertRecord.run(payload);
          deleteMetrics.run(payload.id);
          for (const metric of metrics) {
            insertMetric.run(metric);
          }
        }
      });

      migrate(legacyRows);
      this.db.exec('DROP TABLE telemetry_records_legacy');
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw new Error(`遷移舊版 telemetry_records 失敗: ${err.message}`);
    }
  }

  getPath() {
    return this.filePath;
  }

  insertRecord(record) {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    const normalized = this._serializeRecord(record);
    if (!normalized) {
      return;
    }
    const { payload, metrics } = normalized;
    const insertRecord = this.statements.insertRecord;
    const insertMetric = this.statements.insertMetric;
    const deleteMetrics = this.statements.deleteMetricsForRecord;

    const run = this._createTransaction(() => {
      insertRecord.run(payload);
      deleteMetrics.run(payload.id);
      for (const metric of metrics) {
        insertMetric.run(metric);
      }
    });
    run();
  }

  iterateAll(callback) {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    if (typeof callback !== 'function') {
      return;
    }
    const rows = this.statements.selectRecordsAsc.all();
    if (!rows.length) {
      return;
    }
    const metricsRows = this.statements.selectMetricsAll.all();
    const metricsMap = new Map();
    for (const metricRow of metricsRows) {
      if (!metricRow || !metricRow.record_id) continue;
      const list = metricsMap.get(metricRow.record_id) || [];
      list.push(metricRow);
      metricsMap.set(metricRow.record_id, list);
    }
    for (const row of rows) {
      const metricsForRecord = metricsMap.get(row.id) || [];
      const reconstructed = this._composeRecord(row, metricsForRecord);
      if (reconstructed) {
        callback(reconstructed);
      }
    }
  }

  clear() {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    const clear = this._createTransaction(() => {
      this.statements.clearMetrics.run();
      this.statements.clearRecords.run();
    });
    clear();
    this.db.exec('VACUUM');
  }

  getRecordCount() {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    const row = this.statements.countRecords.get();
    return Number(row?.count || 0);
  }

  getDistinctMeshCount() {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    const row = this.statements.countDistinctMesh.get();
    return Number(row?.nodes || 0);
  }

  close() {
    if (this.db) {
      this.db.close();
      this.db = null;
      this.statements = Object.create(null);
    }
  }

  fetchRecentSnapshot({ limitPerNode = 500, meshIds = null } = {}) {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    const limit =
      Number.isFinite(limitPerNode) && limitPerNode > 0 ? Math.floor(limitPerNode) : 500;
    let rows = [];
    if (Array.isArray(meshIds) && meshIds.length) {
      const sanitizedIds = meshIds.map((id) => sanitizeText(id)).filter(Boolean);
      if (!sanitizedIds.length) {
        return [];
      }
      if (this.supportsWindowFunctions && this.statements.fetchRecentSnapshot) {
        const placeholders = sanitizedIds.map((_, index) => `@mesh${index}`).join(', ');
        const sql = `
          SELECT *
          FROM (
            SELECT
              tr.*,
              ROW_NUMBER() OVER (PARTITION BY mesh_id ORDER BY timestamp_ms DESC) AS rn
            FROM telemetry_records tr
            WHERE mesh_id IN (${placeholders})
          )
          WHERE rn <= @limit
          ORDER BY mesh_id ASC, timestamp_ms DESC
        `;
        const stmt = this.db.prepare(sql);
        const params = { limit };
        sanitizedIds.forEach((id, index) => {
          params[`mesh${index}`] = id;
        });
        rows = stmt.all(params);
      } else {
        const stmt = this.statements.selectRecordsForMesh;
        for (const meshId of sanitizedIds) {
          const part = stmt.all({ meshId, limit });
          rows.push(...part);
        }
        rows.sort((a, b) => {
          if (a.mesh_id === b.mesh_id) {
            return b.timestamp_ms - a.timestamp_ms;
          }
          return a.mesh_id.localeCompare(b.mesh_id);
        });
      }
    } else {
      if (this.supportsWindowFunctions && this.statements.fetchRecentSnapshot) {
        rows = this.statements.fetchRecentSnapshot.all({ limit });
      } else {
        const meshList = this.statements.listMeshIds.all();
        const stmt = this.statements.selectRecordsForMesh;
        for (const row of meshList) {
          const meshId = sanitizeText(row?.mesh_id);
          if (!meshId) continue;
          const part = stmt.all({ meshId, limit });
          rows.push(...part);
        }
        rows.sort((a, b) => {
          if (a.mesh_id === b.mesh_id) {
            return b.timestamp_ms - a.timestamp_ms;
          }
          return a.mesh_id.localeCompare(b.mesh_id);
        });
      }
    }
    if (!rows.length) {
      return [];
    }
    const metricsMap = this._fetchMetricsForRecords(rows.map((row) => row.id));
    return rows.map((row) => this._composeRecord(row, metricsMap.get(row.id) || []));
  }

  listMeshRecordCounts({ meshIds = null } = {}) {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    let rows = [];
    if (Array.isArray(meshIds) && meshIds.length) {
      const sanitized = meshIds.map((id) => sanitizeText(id)).filter(Boolean);
      if (!sanitized.length) {
        return [];
      }
      const placeholders = sanitized.map((_, index) => `@mesh${index}`).join(', ');
      const sql = `
        SELECT
          mesh_id,
          COUNT(*) AS count,
          MAX(timestamp_ms) AS latest_timestamp_ms
        FROM telemetry_records
        WHERE mesh_id IN (${placeholders})
        GROUP BY mesh_id
      `;
      const stmt = this.db.prepare(sql);
      const params = {};
      sanitized.forEach((value, index) => {
        params[`mesh${index}`] = value;
      });
      rows = stmt.all(params);
    } else {
      rows = this.db.prepare(MESH_RECORD_COUNT_SQL).all();
    }
    return rows.map((row) => ({
      meshId: sanitizeText(row?.mesh_id),
      count: Number(row?.count || 0),
      latestTimestampMs: toTimestampMs(row?.latest_timestamp_ms, null)
    }));
  }

  fetchRecordsForMesh({ meshId, limit = null, startMs = null, endMs = null } = {}) {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    const normalizedMeshId = sanitizeText(meshId);
    if (!normalizedMeshId) {
      return [];
    }
    const effectiveLimit =
      Number.isFinite(Number(limit)) && Number(limit) > 0 ? Math.floor(Number(limit)) : null;
    const conditions = ['mesh_id = @meshId'];
    const params = {
      meshId: normalizedMeshId
    };
    if (Number.isFinite(Number(startMs))) {
      params.startMs = Math.floor(Number(startMs));
      conditions.push('timestamp_ms >= @startMs');
    }
    if (Number.isFinite(Number(endMs))) {
      params.endMs = Math.floor(Number(endMs));
      conditions.push('timestamp_ms <= @endMs');
    }
    let limitClause = '';
    if (effectiveLimit != null) {
      params.limit = effectiveLimit;
      limitClause = ' LIMIT @limit';
    }
    const sql = `
      SELECT *
      FROM telemetry_records
      WHERE ${conditions.join(' AND ')}
      ORDER BY timestamp_ms DESC${limitClause}
    `;
    const rows = this.db.prepare(sql).all(params);
    if (!rows.length) {
      return [];
    }
    const metricsMap = this._fetchMetricsForRecords(rows.map((row) => row.id));
    return rows.map((row) => this._composeRecord(row, metricsMap.get(row.id) || []));
  }

  *streamRecords({ meshId, startMs = null, endMs = null, order = 'asc' } = {}) {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    const normalizedMeshId = sanitizeText(meshId);
    if (!normalizedMeshId) {
      return;
    }
    const params = { meshId: normalizedMeshId };
    const conditions = ['mesh_id = @meshId'];
    if (Number.isFinite(Number(startMs))) {
      params.startMs = Math.floor(Number(startMs));
      conditions.push('timestamp_ms >= @startMs');
    }
    if (Number.isFinite(Number(endMs))) {
      params.endMs = Math.floor(Number(endMs));
      conditions.push('timestamp_ms <= @endMs');
    }
    const direction = String(order).toUpperCase() === 'DESC' ? 'DESC' : 'ASC';
    const sql = `
      SELECT *
      FROM telemetry_records
      WHERE ${conditions.join(' AND ')}
      ORDER BY timestamp_ms ${direction}
    `;
    const stmt = this.db.prepare(sql);
    const rowsIterable =
      typeof stmt.iterate === 'function' ? stmt.iterate(params) : stmt.all(params);
    const batch = [];
    const batchSize = 256;
    const flushBatch = (rows) => {
      if (!rows.length) {
        return [];
      }
      const metricsMap = this._fetchMetricsForRecords(rows.map((row) => row.id));
      return rows.map((row) => this._composeRecord(row, metricsMap.get(row.id) || []));
    };
    for (const row of rowsIterable) {
      batch.push(row);
      if (batch.length >= batchSize) {
        const composed = flushBatch(batch);
        batch.length = 0;
        for (const record of composed) {
          if (record) {
            yield record;
          }
        }
      }
    }
    if (batch.length) {
      const composed = flushBatch(batch);
      for (const record of composed) {
        if (record) {
          yield record;
        }
      }
    }
  }

  _serializeRecord(record) {
    if (!record || typeof record !== 'object') {
      return null;
    }
    const meshId = sanitizeText(record.meshId || record.mesh_id || record.meshID);
    if (!meshId) {
      return null;
    }

    const timestampMs = toTimestampMs(record.timestampMs ?? record.timestamp_ms);
    const sampleTimeMs = toTimestampMs(
      record.sampleTimeMs ?? record.sample_time_ms ?? record.telemetry?.timeMs,
      timestampMs
    );
    const telemetryTimeMs = toTimestampMs(record.telemetry?.timeMs, sampleTimeMs);
    const telemetryTimeSeconds = toFiniteNumber(
      record.telemetry?.timeSeconds ?? Math.floor(telemetryTimeMs / 1000)
    );
    const node = record.node && typeof record.node === 'object' ? record.node : {};
    const relay = record.relay && typeof record.relay === 'object' ? record.relay : {};
    const hops = record.hops && typeof record.hops === 'object' ? record.hops : {};

    const payload = {
      id: sanitizeText(record.id) || `${meshId}-${timestampMs}`,
      meshId,
      meshIdNormalized: normalizeMeshId(meshId),
      timestampMs,
      timestampIso: sanitizeText(record.timestamp) || new Date(timestampMs).toISOString(),
      sampleTimeMs,
      sampleTimeIso: sanitizeText(record.sampleTime) || new Date(sampleTimeMs).toISOString(),
      type: sanitizeText(record.type),
      detail: sanitizeText(record.detail),
      channel: toFiniteNumber(record.channel),
      snr: toFiniteNumber(record.snr),
      rssi: toFiniteNumber(record.rssi),
      flowId: sanitizeText(record.flowId),
      relayMeshId: sanitizeText(
        record.relayMeshId || record.relay_mesh_id || relay.meshId || relay.mesh_id
      ),
      relayMeshIdNormalized: normalizeMeshId(
        record.relayMeshIdNormalized ||
          record.relay_mesh_id_normalized ||
          relay.meshIdNormalized ||
          relay.mesh_id_normalized ||
          record.relayMeshId ||
          relay.meshId
      ),
      relayLabel: sanitizeText(record.relayLabel || relay.label),
      relayGuessed: record.relayGuessed ?? relay.guessed ? 1 : 0,
      relayGuessReason: sanitizeText(record.relayGuessReason || relay.guessReason),
      hopsStart: toFiniteNumber(hops.start ?? record.hopsStart),
      hopsLimit: toFiniteNumber(hops.limit ?? record.hopsLimit),
      hopsLabel: sanitizeText(hops.label ?? record.hopsLabel),
      hopsUsed: toFiniteNumber(record.hopsUsed),
      hopsTotal: toFiniteNumber(record.hopsTotal),
      telemetryKind: sanitizeText(record.telemetry?.kind) || 'unknown',
      telemetryTimeMs,
      telemetryTimeSeconds,
      nodeMeshId: sanitizeText(node.meshId || node.mesh_id || meshId),
      nodeMeshIdNormalized: normalizeMeshId(
        node.meshIdNormalized || node.mesh_id_normalized || node.meshId || meshId
      ),
      nodeShortName: sanitizeText(node.shortName || node.short_name),
      nodeLongName: sanitizeText(node.longName || node.long_name),
      nodeLabel: sanitizeText(node.label),
      nodeHwModel: sanitizeText(node.hwModel || node.hw_model),
      nodeHwModelLabel: sanitizeText(node.hwModelLabel || node.hw_model_label),
      nodeRole: sanitizeText(node.role),
      nodeRoleLabel: sanitizeText(node.roleLabel || node.role_label),
      nodeLatitude: toFiniteNumber(node.latitude),
      nodeLongitude: toFiniteNumber(node.longitude),
      nodeAltitude: toFiniteNumber(node.altitude),
      nodeLastSeenAt: toFiniteNumber(node.lastSeenAt || node.last_seen_at),
      createdAt: toTimestampMs(record.createdAt, timestampMs)
    };

    if (payload.relayMeshId && !payload.relayMeshIdNormalized) {
      payload.relayMeshIdNormalized = normalizeMeshId(payload.relayMeshId);
    }
    if (payload.nodeMeshId && !payload.nodeMeshIdNormalized) {
      payload.nodeMeshIdNormalized = normalizeMeshId(payload.nodeMeshId);
    }

    const metricsSource =
      record.telemetry && typeof record.telemetry === 'object'
        ? record.telemetry.metrics
        : null;
    const metrics = [];
    if (metricsSource && typeof metricsSource === 'object') {
      for (const [key, value] of Object.entries(metricsSource)) {
        const metricKey = sanitizeText(key);
        if (!metricKey) continue;
        const serialized = serializeMetricValue(value);
        metrics.push({
          recordId: payload.id,
          metricKey,
          numberValue: serialized.numberValue,
          textValue: serialized.textValue,
          jsonValue: serialized.jsonValue
        });
      }
    }

    return { payload, metrics };
  }

  _composeRecord(row, metricRows) {
    if (!row) {
      return null;
    }
    const timestampMs = Number.isFinite(row.timestamp_ms) ? row.timestamp_ms : Date.now();
    const sampleTimeMs = Number.isFinite(row.sample_time_ms) ? row.sample_time_ms : timestampMs;
    const telemetryTimeMs = Number.isFinite(row.telemetry_time_ms)
      ? row.telemetry_time_ms
      : sampleTimeMs;
    const telemetryTimeSeconds = Number.isFinite(row.telemetry_time_seconds)
      ? row.telemetry_time_seconds
      : Math.floor(telemetryTimeMs / 1000);

    const metrics = {};
    for (const metric of metricRows) {
      if (!metric || !metric.metric_key) continue;
      metrics[metric.metric_key] = deserializeMetricValue(metric);
    }

    const node = (() => {
      if (
        !row.node_mesh_id &&
        !row.node_mesh_id_normalized &&
        !row.node_label &&
        !row.node_short_name &&
        !row.node_long_name &&
        !row.node_hw_model &&
        !row.node_role
      ) {
        return null;
      }
      return {
        meshId: row.node_mesh_id || row.mesh_id || null,
        meshIdNormalized:
          row.node_mesh_id_normalized ||
          normalizeMeshId(row.node_mesh_id || row.mesh_id) ||
          null,
        label: row.node_label || null,
        shortName: row.node_short_name || null,
        longName: row.node_long_name || null,
        hwModel: row.node_hw_model || null,
        hwModelLabel: row.node_hw_model_label || null,
        role: row.node_role || null,
        roleLabel: row.node_role_label || null,
        latitude: Number.isFinite(row.node_latitude) ? row.node_latitude : null,
        longitude: Number.isFinite(row.node_longitude) ? row.node_longitude : null,
        altitude: Number.isFinite(row.node_altitude) ? row.node_altitude : null,
        lastSeenAt: Number.isFinite(row.node_last_seen_at) ? row.node_last_seen_at : null
      };
    })();

    const relayMeshId =
      row.relay_mesh_id ||
      row.relay_mesh_id_normalized ||
      null;
    const relayMeshIdNormalized =
      row.relay_mesh_id_normalized ||
      normalizeMeshId(row.relay_mesh_id) ||
      null;
    const relay =
      relayMeshId || row.relay_label
        ? {
            meshId: relayMeshId,
            meshIdNormalized: relayMeshIdNormalized,
            label: row.relay_label || null,
            guessed: Boolean(row.relay_guessed),
            guessReason: row.relay_guess_reason || null
          }
        : null;

    const hops =
      row.hops_start != null ||
      row.hops_limit != null ||
      row.hops_label
        ? {
            start: Number.isFinite(row.hops_start) ? row.hops_start : null,
            limit: Number.isFinite(row.hops_limit) ? row.hops_limit : null,
            label: row.hops_label || null
          }
        : null;

    return {
      id: row.id,
      meshId: row.mesh_id,
      timestampMs,
      timestamp: row.timestamp_iso || new Date(timestampMs).toISOString(),
      sampleTimeMs,
      sampleTime: row.sample_time_iso || new Date(sampleTimeMs).toISOString(),
      type: row.type || null,
      detail: row.detail || null,
      channel: Number.isFinite(row.channel) ? row.channel : null,
      snr: Number.isFinite(row.snr) ? row.snr : null,
      rssi: Number.isFinite(row.rssi) ? row.rssi : null,
      flowId: row.flow_id || null,
      relay,
      relayLabel: row.relay_label || null,
      relayGuessed: Boolean(row.relay_guessed),
      relayGuessReason: row.relay_guess_reason || null,
      relayMeshId,
      relayMeshIdNormalized,
      hops,
      hopsLabel: row.hops_label || null,
      hopsUsed: Number.isFinite(row.hops_used) ? row.hops_used : null,
      hopsTotal: Number.isFinite(row.hops_total) ? row.hops_total : null,
      telemetry: {
        kind: row.telemetry_kind || 'unknown',
        timeMs: telemetryTimeMs,
        timeSeconds: telemetryTimeSeconds,
        metrics
      },
      node,
      createdAt: Number.isFinite(row.created_at) ? row.created_at : null
    };
  }

  _fetchMetricsForRecords(recordIds) {
    const metricsMap = new Map();
    if (!Array.isArray(recordIds) || recordIds.length === 0) {
      return metricsMap;
    }
    const chunks = chunkArray(recordIds, 400);
    for (const chunk of chunks) {
      if (!chunk.length) continue;
      const placeholders = chunk.map((_, index) => `@id${index}`).join(', ');
      const sql = `
        SELECT record_id, metric_key, number_value, text_value, json_value
        FROM telemetry_metrics
        WHERE record_id IN (${placeholders})
      `;
      const stmt = this.db.prepare(sql);
      const params = {};
      chunk.forEach((id, index) => {
        params[`id${index}`] = id;
      });
      const rows = stmt.all(params);
      for (const row of rows) {
        if (!row || !row.record_id || !row.metric_key) continue;
        const list = metricsMap.get(row.record_id) || [];
        list.push(row);
        metricsMap.set(row.record_id, list);
      }
    }
    return metricsMap;
  }
}

module.exports = {
  TelemetryDatabase
};
