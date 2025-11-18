'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

class TelemetryDatabase {
  constructor(filePath, options = {}) {
    if (!filePath) {
      throw new Error('TelemetryDatabase 需要提供檔案路徑');
    }
    this.filePath = filePath;
    this.options = options;
    this.db = null;
    this.statements = Object.create(null);
  }

  init() {
    if (this.db) {
      return;
    }
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    const openOptions = {};
    if (this.options.readonly) {
      openOptions.readonly = true;
    }
    this.db = new Database(this.filePath, openOptions);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('foreign_keys = ON');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS telemetry_records (
        id TEXT PRIMARY KEY,
        mesh_id TEXT NOT NULL,
        timestamp_ms INTEGER NOT NULL,
        data TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_telemetry_records_mesh_time
        ON telemetry_records(mesh_id, timestamp_ms);

      CREATE INDEX IF NOT EXISTS idx_telemetry_records_time
        ON telemetry_records(timestamp_ms);
    `);

    this.statements.insert = this.db.prepare(`
      INSERT OR REPLACE INTO telemetry_records (id, mesh_id, timestamp_ms, data)
      VALUES (@id, @meshId, @timestampMs, @data)
    `);
    this.statements.deleteAll = this.db.prepare('DELETE FROM telemetry_records');
    this.statements.selectAll = this.db.prepare('SELECT data FROM telemetry_records ORDER BY timestamp_ms ASC');
    this.statements.count = this.db.prepare('SELECT COUNT(*) AS count FROM telemetry_records');
    this.statements.countDistinctMesh = this.db.prepare('SELECT COUNT(DISTINCT mesh_id) AS nodes FROM telemetry_records');
  }

  getPath() {
    return this.filePath;
  }

  insertRecord(record) {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    if (!record || typeof record !== 'object') {
      return;
    }
    const { id, meshId, timestampMs } = record;
    if (!id || !meshId) {
      return;
    }
    const payload = {
      id,
      meshId,
      timestampMs: Number.isFinite(timestampMs) ? Number(timestampMs) : Date.now(),
      data: JSON.stringify(record)
    };
    this.statements.insert.run(payload);
  }

  iterateAll(callback) {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    if (typeof callback !== 'function') {
      return;
    }
    for (const row of this.statements.selectAll.iterate()) {
      if (!row || !row.data) continue;
      try {
        const parsed = JSON.parse(row.data);
        callback(parsed);
      } catch {
        // 忽略單筆損壞的資料
      }
    }
  }

  clear() {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    this.statements.deleteAll.run();
    this.db.exec('VACUUM');
  }

  getRecordCount() {
    if (!this.db) {
      throw new Error('TelemetryDatabase 尚未初始化');
    }
    const row = this.statements.count.get();
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
}

module.exports = {
  TelemetryDatabase
};
