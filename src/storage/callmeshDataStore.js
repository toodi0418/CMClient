'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

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
    this.db = new Database(this.filePath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('foreign_keys = ON');
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
        position INTEGER NOT NULL,
        data TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_message_log_channel_position
        ON message_log(channel, position);

      CREATE TABLE IF NOT EXISTS relay_stats (
        mesh_key TEXT PRIMARY KEY,
        snr REAL,
        rssi REAL,
        count INTEGER,
        updated_at INTEGER NOT NULL
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
    this.statements.insertMessage = this.db.prepare(`
      INSERT INTO message_log (flow_id, channel, timestamp_ms, position, data)
      VALUES (@flowId, @channel, @timestampMs, @position, @data)
      ON CONFLICT(flow_id) DO UPDATE SET
        channel = excluded.channel,
        timestamp_ms = excluded.timestamp_ms,
        position = excluded.position,
        data = excluded.data
    `);
    this.statements.selectMessages = this.db.prepare(`
      SELECT flow_id AS flowId, channel, timestamp_ms AS timestampMs, position, data
      FROM message_log
      ORDER BY channel ASC, position ASC
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
    const exec = this.db.transaction((entries) => {
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
    const insert = this.statements.insertMessage;
    const exec = this.db.transaction((items) => {
      this.statements.deleteMessages.run();
      let position = 0;
      for (const entry of items) {
        if (!entry || typeof entry !== 'object') continue;
        insert.run({
          flowId: entry.flowId || `${entry.channel}-${entry.timestampMs}-${position}`,
          channel: Number.isFinite(entry.channel) ? entry.channel : 0,
          timestampMs: Number.isFinite(entry.timestampMs) ? entry.timestampMs : Date.now(),
          position,
          data: toJson(entry)
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
    return this.statements.selectMessages.all().map((row) => {
      const parsed = fromJson(row.data);
      if (parsed && typeof parsed === 'object') {
        if (!parsed.flowId) parsed.flowId = row.flowId;
        parsed.channel = Number.isFinite(parsed.channel) ? parsed.channel : row.channel;
        parsed.timestampMs = Number.isFinite(parsed.timestampMs)
          ? parsed.timestampMs
          : row.timestampMs;
        return parsed;
      }
      return {
        flowId: row.flowId,
        channel: row.channel,
        timestampMs: row.timestampMs
      };
    });
  }

  replaceRelayStats(entries = []) {
    if (!this.db) {
      throw new Error('CallMeshDataStore 尚未初始化');
    }
    const upsert = this.statements.upsertRelayStat;
    const exec = this.db.transaction((items) => {
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
}

module.exports = {
  CallMeshDataStore
};
