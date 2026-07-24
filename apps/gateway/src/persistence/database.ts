import { DatabaseSync } from "node:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

import {
  BACKLOG_CLASSIFICATIONS,
  TRANSPORT_KINDS,
  type JobDetail,
  type JobError,
  type JobStatus,
  type MeshMessage,
  type MeshNode,
  type MeshObservation,
  type MeshTelemetry,
} from "@cmclient/contracts";

import { PositionRepository } from "../position.js";
import { AprsOutboxRepository } from "../aprs-outbox.js";
import { CallMeshMappingRepository } from "../callmesh.js";
import {
  gatewayMigrations,
  validateMigrationManifest,
  type SqlMigration,
} from "./migrations.js";

export {
  createSqlMigration,
  gatewayMigrations,
  MigrationManifestError,
  validateMigrationManifest,
} from "./migrations.js";

export type Migration = SqlMigration;

export class DatabaseMigrationError extends Error {
  readonly code: string;

  constructor(code = "DATABASE_MIGRATION_FAILED") {
    super(code);
    this.code = code;
    this.name = "DatabaseMigrationError";
  }
}

export class DatabaseMigrationRetryableError extends DatabaseMigrationError {
  constructor() {
    super("DATABASE_MIGRATION_RETRYABLE");
    this.name = "DatabaseMigrationRetryableError";
  }
}

export class DatabaseSchemaHistoryError extends DatabaseMigrationError {
  constructor() {
    super("DATABASE_SCHEMA_HISTORY_DRIFT");
    this.name = "DatabaseSchemaHistoryError";
  }
}

export class DatabaseIntegrityError extends Error {
  readonly code = "DATABASE_INTEGRITY_CHECK_FAILED";
}

export class DatabaseCheckpointError extends Error {
  readonly code = "DATABASE_CHECKPOINT_FAILED";
}

export interface WalCheckpointResult {
  busy: number;
  checkpointedFrames: number;
  logFrames: number;
}

export interface GatewayDatabaseOptions {
  readonly atomicMigrationBatch?: boolean;
  readonly busyTimeoutMilliseconds?: number;
}

export interface SchemaHistoryEntry {
  readonly version: number;
  readonly name: string;
  readonly sha256: string;
}

export interface SchemaHistoryReport {
  readonly digestStatus: "recorded" | "legacy_name_only" | "absent";
  readonly entries: readonly SchemaHistoryEntry[];
}

export class GatewayDatabase {
  readonly connection: DatabaseSync;
  readonly jobs: JobRepository;
  readonly meshMessages: MeshMessageRepository;
  readonly meshNodes: MeshNodeRepository;
  readonly meshObservations: MeshObservationRepository;
  readonly meshTelemetry: MeshTelemetryRepository;
  readonly positions: PositionRepository;
  readonly aprsOutbox: AprsOutboxRepository;
  readonly callmeshMappings: CallMeshMappingRepository;
  readonly settings: SettingsRepository;

  constructor(
    path: string,
    migrations: readonly Migration[] = gatewayMigrations,
    options: GatewayDatabaseOptions = {},
  ) {
    if (path !== ":memory:") {
      mkdirSync(dirname(path), { recursive: true });
    }
    const busyTimeoutMilliseconds = options.busyTimeoutMilliseconds ?? 5_000;
    if (
      !Number.isInteger(busyTimeoutMilliseconds) ||
      busyTimeoutMilliseconds < 0 ||
      busyTimeoutMilliseconds > 1_800_000
    ) {
      throw new DatabaseMigrationError();
    }
    this.connection = new DatabaseSync(path);
    try {
      this.connection.exec(`PRAGMA busy_timeout = ${busyTimeoutMilliseconds}`);
      this.connection.exec("PRAGMA journal_mode = WAL");
      this.connection.exec("PRAGMA foreign_keys = ON");
      runMigrations(
        this.connection,
        migrations,
        options.atomicMigrationBatch ?? false,
      );
      inspectMigrationHistory(this.connection, migrations, true);
    } catch (error) {
      this.connection.close();
      throw error;
    }
    this.jobs = new JobRepository(this.connection);
    this.meshMessages = new MeshMessageRepository(this.connection);
    this.meshNodes = new MeshNodeRepository(this.connection);
    this.meshObservations = new MeshObservationRepository(this.connection);
    this.meshTelemetry = new MeshTelemetryRepository(this.connection);
    this.positions = new PositionRepository(this.connection);
    this.aprsOutbox = new AprsOutboxRepository(this.connection);
    this.callmeshMappings = new CallMeshMappingRepository(this.connection);
    this.settings = new SettingsRepository(this.connection);
  }

  close(): void {
    this.connection.close();
  }

  integrityCheck(): "ok" {
    const rows = this.connection.prepare("PRAGMA integrity_check").all();
    if (
      rows.length !== 1 ||
      String(rows[0]?.integrity_check ?? "").toLowerCase() !== "ok"
    ) {
      throw new DatabaseIntegrityError();
    }
    return "ok";
  }

  checkpoint(): WalCheckpointResult {
    const row = this.connection.prepare("PRAGMA wal_checkpoint(PASSIVE)").get();
    const busy = Number(row?.busy);
    const logFrames = Number(row?.log);
    const checkpointedFrames = Number(row?.checkpointed);
    if (
      !Number.isInteger(busy) ||
      busy < 0 ||
      !Number.isInteger(logFrames) ||
      logFrames < -1 ||
      !Number.isInteger(checkpointedFrames) ||
      checkpointedFrames < -1
    ) {
      throw new DatabaseCheckpointError();
    }
    return { busy, checkpointedFrames, logFrames };
  }
}

export function runMigrations(
  database: DatabaseSync,
  migrations: readonly Migration[],
  atomicBatch = false,
): void {
  validateMigrationManifest(migrations);
  if (atomicBatch) {
    runAtomicMigrationBatch(database, migrations);
    return;
  }

  runMigrationTransaction(database, "IMMEDIATE", () => {
    ensureMigrationHistory(database, migrations);
  });
  const history = inspectMigrationHistory(database, migrations, false);
  for (const migration of migrations.slice(history.entries.length)) {
    runMigrationTransaction(database, "IMMEDIATE", () => {
      applyMigration(database, migration);
    });
  }
  inspectMigrationHistory(database, migrations, true);
}

export function inspectMigrationHistory(
  database: DatabaseSync,
  migrations: readonly Migration[] = gatewayMigrations,
  requireComplete = true,
): SchemaHistoryReport {
  validateMigrationManifest(migrations);
  if (!hasSchemaMigrationTable(database)) {
    if (requireComplete) {
      throw new DatabaseSchemaHistoryError();
    }
    return { digestStatus: "absent", entries: [] };
  }

  const columns = database
    .prepare("PRAGMA table_info(schema_migrations)")
    .all()
    .map((row) => String(row.name));
  const columnSet = new Set(columns);
  const legacyColumns = ["version", "name", "applied_at"];
  const digestColumns = [...legacyColumns, "sha256"];
  const hasDigest = exactColumnSet(columnSet, digestColumns);
  if (!hasDigest && !exactColumnSet(columnSet, legacyColumns)) {
    throw new DatabaseSchemaHistoryError();
  }

  const rows = database
    .prepare(
      hasDigest
        ? "SELECT version, name, sha256 FROM schema_migrations ORDER BY version ASC"
        : "SELECT version, name FROM schema_migrations ORDER BY version ASC",
    )
    .all();
  if (rows.length > migrations.length) {
    throw new DatabaseSchemaHistoryError();
  }
  const entries = rows.map((row, index) => {
    const expected = migrations[index];
    const version = Number(row.version);
    const name = String(row.name);
    if (!expected || version !== expected.version || name !== expected.name) {
      throw new DatabaseSchemaHistoryError();
    }
    if (hasDigest && String(row.sha256) !== expected.sha256) {
      throw new DatabaseSchemaHistoryError();
    }
    return Object.freeze({
      version,
      name,
      sha256: expected.sha256,
    });
  });
  if (requireComplete && entries.length !== migrations.length) {
    throw new DatabaseSchemaHistoryError();
  }
  return Object.freeze({
    digestStatus: hasDigest ? "recorded" : "legacy_name_only",
    entries: Object.freeze(entries),
  });
}

function runAtomicMigrationBatch(
  database: DatabaseSync,
  migrations: readonly Migration[],
): void {
  try {
    database.exec("BEGIN EXCLUSIVE");
    ensureMigrationHistory(database, migrations);
    const history = inspectMigrationHistory(database, migrations, false);
    for (const migration of migrations.slice(history.entries.length)) {
      applyMigration(database, migration);
    }
    inspectMigrationHistory(database, migrations, true);
    database.exec("COMMIT");
  } catch (error) {
    rollbackMigration(database);
    if (isSqliteBusyOrLocked(error)) {
      throw new DatabaseMigrationRetryableError();
    }
    if (error instanceof DatabaseMigrationError) {
      throw error;
    }
    throw new DatabaseMigrationError();
  }
}

function runMigrationTransaction(
  database: DatabaseSync,
  mode: "IMMEDIATE" | "EXCLUSIVE",
  operation: () => void,
): void {
  try {
    database.exec(`BEGIN ${mode}`);
    operation();
    database.exec("COMMIT");
  } catch (error) {
    rollbackMigration(database);
    if (error instanceof DatabaseMigrationError) {
      throw error;
    }
    throw new DatabaseMigrationError();
  }
}

function ensureMigrationHistory(
  database: DatabaseSync,
  migrations: readonly Migration[],
): void {
  if (!hasSchemaMigrationTable(database)) {
    database.exec(
      "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^a-f0-9]*'), applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    );
    return;
  }
  const report = inspectMigrationHistory(database, migrations, false);
  if (report.digestStatus !== "legacy_name_only") {
    return;
  }
  assertLegacySchemaPrefix(database, migrations, report.entries.length);
  database.exec("ALTER TABLE schema_migrations ADD COLUMN sha256 TEXT");
  const update = database.prepare(
    "UPDATE schema_migrations SET sha256 = ? WHERE version = ? AND name = ? AND sha256 IS NULL",
  );
  for (const entry of report.entries) {
    const result = update.run(entry.sha256, entry.version, entry.name);
    if (Number(result.changes) !== 1) {
      throw new DatabaseSchemaHistoryError();
    }
  }
  inspectMigrationHistory(database, migrations, false);
}

function assertLegacySchemaPrefix(
  database: DatabaseSync,
  migrations: readonly Migration[],
  appliedMigrationCount: number,
): void {
  const reference = new DatabaseSync(":memory:");
  try {
    reference.exec("PRAGMA foreign_keys = ON");
    for (const migration of migrations.slice(0, appliedMigrationCount)) {
      reference.exec(migration.sql);
    }
    if (schemaPrefixSnapshot(database) !== schemaPrefixSnapshot(reference)) {
      throw new DatabaseSchemaHistoryError();
    }
  } catch (error) {
    if (error instanceof DatabaseSchemaHistoryError) {
      throw error;
    }
    throw new DatabaseSchemaHistoryError();
  } finally {
    reference.close();
  }
}

function schemaPrefixSnapshot(database: DatabaseSync): string {
  const catalog = database
    .prepare(
      "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name <> 'schema_migrations' AND tbl_name <> 'schema_migrations' AND name NOT LIKE 'sqlite_%' ORDER BY type ASC, name ASC",
    )
    .all()
    .map((row) => ({
      name: String(row.name),
      sql: normalizeSchemaSql(row.sql),
      table: String(row.tbl_name),
      type: String(row.type),
    }));
  const tables = catalog
    .filter((entry) => entry.type === "table")
    .map((entry) => {
      const columns = database
        .prepare(
          'SELECT cid, name, type, "notnull" AS not_null, dflt_value, pk, hidden FROM pragma_table_xinfo(?) ORDER BY cid ASC',
        )
        .all(entry.name)
        .map((row) => ({
          cid: Number(row.cid),
          defaultValue: nullableSchemaString(row.dflt_value),
          hidden: Number(row.hidden),
          name: String(row.name),
          notNull: Number(row.not_null),
          primaryKey: Number(row.pk),
          type: String(row.type),
        }));
      const foreignKeys = database
        .prepare(
          'SELECT id, seq, "table" AS target_table, "from" AS source_column, "to" AS target_column, on_update, on_delete, match FROM pragma_foreign_key_list(?) ORDER BY id ASC, seq ASC',
        )
        .all(entry.name)
        .map((row) => ({
          id: Number(row.id),
          match: String(row.match),
          onDelete: String(row.on_delete),
          onUpdate: String(row.on_update),
          sequence: Number(row.seq),
          sourceColumn: String(row.source_column),
          targetColumn: nullableSchemaString(row.target_column),
          targetTable: String(row.target_table),
        }));
      const indexes = database
        .prepare(
          'SELECT name, "unique" AS is_unique, origin, partial FROM pragma_index_list(?) ORDER BY name ASC',
        )
        .all(entry.name)
        .map((row) => {
          const name = String(row.name);
          const sqlRow = database
            .prepare(
              "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
            )
            .get(name);
          const columns = database
            .prepare(
              "SELECT seqno, cid, name, desc, coll, key FROM pragma_index_xinfo(?) ORDER BY seqno ASC",
            )
            .all(name)
            .map((column) => ({
              cid: Number(column.cid),
              collation: nullableSchemaString(column.coll),
              descending: Number(column.desc),
              key: Number(column.key),
              name: nullableSchemaString(column.name),
              sequence: Number(column.seqno),
            }));
          return {
            columns,
            name,
            origin: String(row.origin),
            partial: Number(row.partial),
            sql: normalizeSchemaSql(sqlRow?.sql),
            unique: Number(row.is_unique),
          };
        });
      return { columns, foreignKeys, indexes, name: entry.name };
    });
  return JSON.stringify({ catalog, tables });
}

function normalizeSchemaSql(value: unknown): string | null {
  return value === null || value === undefined
    ? null
    : String(value).trim().replace(/\s+/g, " ");
}

function nullableSchemaString(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}

function hasSchemaMigrationTable(database: DatabaseSync): boolean {
  return Boolean(
    database
      .prepare(
        "SELECT 1 AS present FROM sqlite_schema WHERE type = 'table' AND name = 'schema_migrations'",
      )
      .get()?.present,
  );
}

function exactColumnSet(
  actual: Set<string>,
  expected: readonly string[],
): boolean {
  return (
    actual.size === expected.length &&
    expected.every((column) => actual.has(column))
  );
}

function applyMigration(database: DatabaseSync, migration: Migration): void {
  database.exec(migration.sql);
  database
    .prepare(
      "INSERT INTO schema_migrations (version, name, sha256) VALUES (?, ?, ?)",
    )
    .run(migration.version, migration.name, migration.sha256);
}

function rollbackMigration(database: DatabaseSync): void {
  try {
    database.exec("ROLLBACK");
  } catch {
    // BEGIN can fail before a transaction exists.
  }
}

export function isSqliteBusyOrLocked(error: unknown): boolean {
  if (typeof error !== "object" || error === null) {
    return false;
  }
  const errcode = "errcode" in error ? Number(error.errcode) : Number.NaN;
  const code = "code" in error ? String(error.code) : "";
  return (
    errcode === 5 ||
    errcode === 6 ||
    code.includes("BUSY") ||
    code.includes("LOCKED")
  );
}

export class SettingsRepository {
  constructor(private readonly database: DatabaseSync) {}

  set(key: string, value: unknown): void {
    this.database
      .prepare(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP",
      )
      .run(key, JSON.stringify(value));
  }

  get<T>(key: string): T | undefined {
    const row = this.database
      .prepare("SELECT value FROM settings WHERE key = ?")
      .get(key);
    return row ? (JSON.parse(String(row.value)) as T) : undefined;
  }
}

export interface StoredJob extends JobDetail {
  input: Record<string, unknown>;
  idempotencyKey?: string;
  result?: Record<string, unknown>;
  cancelRequested: boolean;
}

export interface CreateJobInput {
  id: string;
  type: string;
  input: Record<string, unknown>;
  idempotencyKey?: string;
  now: string;
}

export interface JobTransition {
  startedAt?: string;
  completedAt?: string;
  result?: Record<string, unknown>;
  error?: JobError;
  cancelRequested?: boolean;
}

export class JobRepository {
  constructor(private readonly database: DatabaseSync) {}

  create(input: CreateJobInput): { created: boolean; job: StoredJob } {
    if (input.idempotencyKey) {
      const existing = this.findByIdempotency(input.type, input.idempotencyKey);
      if (existing) {
        return { created: false, job: existing };
      }
    }
    try {
      this.database
        .prepare(
          "INSERT INTO jobs (id, type, status, input, idempotency_key, created_at, updated_at) VALUES (?, ?, 'queued', ?, ?, ?, ?)",
        )
        .run(
          input.id,
          input.type,
          JSON.stringify(input.input),
          input.idempotencyKey ?? null,
          input.now,
          input.now,
        );
    } catch {
      if (input.idempotencyKey) {
        const existing = this.findByIdempotency(
          input.type,
          input.idempotencyKey,
        );
        if (existing) {
          return { created: false, job: existing };
        }
      }
      throw new JobPersistenceError();
    }
    const job = this.find(input.id);
    if (!job) {
      throw new JobPersistenceError();
    }
    return { created: true, job };
  }

  find(id: string): StoredJob | undefined {
    const row = this.database
      .prepare("SELECT * FROM jobs WHERE id = ?")
      .get(id);
    return row ? toStoredJob(row) : undefined;
  }

  findByIdempotency(
    type: string,
    idempotencyKey: string,
  ): StoredJob | undefined {
    const row = this.database
      .prepare("SELECT * FROM jobs WHERE type = ? AND idempotency_key = ?")
      .get(type, idempotencyKey);
    return row ? toStoredJob(row) : undefined;
  }

  findByStatuses(statuses: readonly JobStatus[], limit?: number): StoredJob[] {
    if (statuses.length === 0) {
      return [];
    }
    if (
      limit !== undefined &&
      (!Number.isInteger(limit) || limit < 1 || limit > 10_000)
    ) {
      throw new JobPersistenceError();
    }
    const placeholders = statuses.map(() => "?").join(", ");
    const limitClause = limit === undefined ? "" : " LIMIT ?";
    return this.database
      .prepare(
        `SELECT * FROM jobs WHERE status IN (${placeholders}) ORDER BY created_at ASC, id ASC${limitClause}`,
      )
      .all(...statuses, ...(limit === undefined ? [] : [limit]))
      .map(toStoredJob);
  }

  findQueued(limit: number): StoredJob[] {
    if (!Number.isInteger(limit) || limit < 1 || limit > 20_000) {
      throw new JobPersistenceError();
    }
    return this.database
      .prepare(
        "SELECT * FROM jobs INDEXED BY jobs_queued_created_at_index WHERE status = 'queued' ORDER BY created_at ASC, id ASC LIMIT ?",
      )
      .all(limit)
      .map(toStoredJob);
  }

  findQueuedByTypes(types: readonly string[], limit: number): StoredJob[] {
    if (types.length === 0) {
      return [];
    }
    if (
      types.length > 10_000 ||
      types.some((type) => !/^[a-z][a-z0-9_.-]{0,127}$/.test(type)) ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 20_000
    ) {
      throw new JobPersistenceError();
    }
    const placeholders = types.map(() => "?").join(", ");
    return this.database
      .prepare(
        `SELECT * FROM jobs INDEXED BY jobs_queued_type_created_at_index WHERE status = 'queued' AND type IN (${placeholders}) ORDER BY created_at ASC, id ASC LIMIT ?`,
      )
      .all(...types, limit)
      .map(toStoredJob);
  }

  transition(
    id: string,
    expected: readonly JobStatus[],
    status: JobStatus,
    now: string,
    transition: JobTransition = {},
  ): StoredJob | undefined {
    const current = this.find(id);
    if (!current || !expected.includes(current.status)) {
      return current;
    }
    const assignments = ["status = ?", "updated_at = ?"];
    const values: Array<string | number> = [status, now];
    if (transition.startedAt) {
      assignments.push("started_at = ?");
      values.push(transition.startedAt);
    }
    if (transition.completedAt) {
      assignments.push("completed_at = ?");
      values.push(transition.completedAt);
    }
    if (transition.result) {
      assignments.push("result = ?");
      values.push(JSON.stringify(transition.result));
    }
    if (transition.error) {
      assignments.push("error_code = ?", "error_params = ?");
      values.push(
        transition.error.code,
        JSON.stringify(transition.error.params),
      );
    }
    if (transition.cancelRequested !== undefined) {
      assignments.push("cancel_requested = ?");
      values.push(transition.cancelRequested ? 1 : 0);
    }
    values.push(id);
    this.database
      .prepare(`UPDATE jobs SET ${assignments.join(", ")} WHERE id = ?`)
      .run(...values);
    return this.find(id);
  }

  deleteTerminalBefore(cutoffExclusive: string, limit = 1_000): number {
    const cutoff = canonicalTimestamp(cutoffExclusive);
    if (
      cutoff === undefined ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 10_000
    ) {
      throw new JobPersistenceError();
    }
    try {
      const result = this.database
        .prepare(
          "DELETE FROM jobs WHERE id IN (SELECT id FROM jobs INDEXED BY jobs_terminal_retention_index WHERE status IN ('succeeded', 'failed', 'cancelled', 'rolled_back') AND completed_at IS NOT NULL AND completed_at < ? ORDER BY completed_at ASC, id ASC LIMIT ?)",
        )
        .run(cutoff, limit);
      return Number(result.changes);
    } catch {
      throw new JobPersistenceError();
    }
  }
}

export class JobPersistenceError extends Error {
  readonly code = "JOB_PERSISTENCE_FAILED";
}

export class MeshObservationPersistenceError extends Error {
  readonly code = "MESH_OBSERVATION_PERSISTENCE_FAILED";
}

export class MeshDomainPersistenceError extends Error {
  readonly code = "MESH_DOMAIN_PERSISTENCE_FAILED";
}

export class MeshObservationRepository {
  constructor(private readonly database: DatabaseSync) {}

  insert(observation: MeshObservation): MeshObservation {
    try {
      this.database
        .prepare(
          "INSERT INTO mesh_observations (id, schema_version, transport, session_connected_at, ingested_at, server_ingested_at, device_rx_time_seconds, backlog_classification, normalized_from_radio) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          observation.id,
          observation.schemaVersion,
          observation.transport,
          observation.sessionConnectedAt,
          observation.ingestedAt,
          observation.serverIngestedAt,
          observation.deviceRxTimeSeconds ?? null,
          observation.backlogClassification,
          JSON.stringify(observation.normalizedFromRadio),
        );
    } catch {
      throw new MeshObservationPersistenceError();
    }
    const stored = this.find(observation.id);
    if (!stored) {
      throw new MeshObservationPersistenceError();
    }
    return stored;
  }

  find(id: string): MeshObservation | undefined {
    const row = this.database
      .prepare("SELECT * FROM mesh_observations WHERE id = ?")
      .get(id);
    return row ? toMeshObservation(row) : undefined;
  }

  deleteUnreferencedBefore(cutoffExclusive: string, limit = 1_000): number {
    const cutoff = canonicalTimestamp(cutoffExclusive);
    if (
      cutoff === undefined ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 40_000
    ) {
      throw new MeshObservationPersistenceError();
    }
    try {
      const result = this.database
        .prepare(
          "DELETE FROM mesh_observations WHERE id IN (SELECT observation.id FROM mesh_observations AS observation WHERE observation.ingested_at < ? AND NOT EXISTS (SELECT 1 FROM nodes WHERE nodes.last_observation_id = observation.id) AND NOT EXISTS (SELECT 1 FROM messages WHERE messages.observation_id = observation.id) AND NOT EXISTS (SELECT 1 FROM telemetry WHERE telemetry.observation_id = observation.id) AND NOT EXISTS (SELECT 1 FROM position_observations WHERE position_observations.mesh_observation_id = observation.id) ORDER BY observation.ingested_at ASC, observation.id ASC LIMIT ?)",
        )
        .run(cutoff, limit);
      return Number(result.changes);
    } catch {
      throw new MeshObservationPersistenceError();
    }
  }
}

export class MeshNodeRepository {
  constructor(private readonly database: DatabaseSync) {}

  upsert(node: MeshNode): MeshNode {
    try {
      this.database
        .prepare(
          "INSERT INTO nodes (mesh_network_id, node_num, user_id, long_name, short_name, hardware_model, role, first_seen_at, last_seen_at, last_observation_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(mesh_network_id, node_num) DO UPDATE SET user_id = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.user_id, nodes.user_id) ELSE nodes.user_id END, long_name = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.long_name, nodes.long_name) ELSE nodes.long_name END, short_name = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.short_name, nodes.short_name) ELSE nodes.short_name END, hardware_model = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.hardware_model, nodes.hardware_model) ELSE nodes.hardware_model END, role = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN COALESCE(excluded.role, nodes.role) ELSE nodes.role END, last_seen_at = CASE WHEN excluded.last_seen_at > nodes.last_seen_at THEN excluded.last_seen_at ELSE nodes.last_seen_at END, last_observation_id = CASE WHEN excluded.last_seen_at >= nodes.last_seen_at THEN excluded.last_observation_id ELSE nodes.last_observation_id END",
        )
        .run(
          node.meshNetworkId,
          node.nodeNum,
          node.userId ?? null,
          node.longName ?? null,
          node.shortName ?? null,
          node.hardwareModel ?? null,
          node.role ?? null,
          node.firstSeenAt,
          node.lastSeenAt,
          node.lastObservationId,
        );
    } catch {
      throw new MeshDomainPersistenceError();
    }
    const stored = this.find(node.meshNetworkId, node.nodeNum);
    if (!stored) {
      throw new MeshDomainPersistenceError();
    }
    return stored;
  }

  find(meshNetworkId: string, nodeNum: number): MeshNode | undefined {
    const row = this.database
      .prepare("SELECT * FROM nodes WHERE mesh_network_id = ? AND node_num = ?")
      .get(meshNetworkId, nodeNum);
    return row ? toMeshNode(row) : undefined;
  }

  list(limit: number): MeshNode[] {
    return this.database
      .prepare(
        "SELECT * FROM nodes ORDER BY last_seen_at DESC, node_num ASC LIMIT ?",
      )
      .all(limit)
      .map((row) => toMeshNode(row as Record<string, unknown>));
  }
}

export class MeshMessageRepository {
  constructor(private readonly database: DatabaseSync) {}

  insert(message: MeshMessage): MeshMessage {
    try {
      this.database
        .prepare(
          "INSERT INTO messages (id, observation_id, mesh_network_id, sender, destination, packet_id, channel, text, observed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          message.id,
          message.observationId,
          message.meshNetworkId,
          message.sender,
          message.destination ?? null,
          message.packetId ?? null,
          message.channel ?? null,
          message.text,
          message.observedAt,
        );
    } catch {
      throw new MeshDomainPersistenceError();
    }
    const stored = this.find(message.id);
    if (!stored) {
      throw new MeshDomainPersistenceError();
    }
    return stored;
  }

  find(id: string): MeshMessage | undefined {
    const row = this.database
      .prepare("SELECT * FROM messages WHERE id = ?")
      .get(id);
    return row ? toMeshMessage(row) : undefined;
  }

  findByObservation(observationId: string): MeshMessage | undefined {
    const row = this.database
      .prepare("SELECT * FROM messages WHERE observation_id = ?")
      .get(observationId);
    return row ? toMeshMessage(row) : undefined;
  }

  list(limit: number): MeshMessage[] {
    return this.database
      .prepare(
        "SELECT * FROM messages ORDER BY observed_at DESC, id ASC LIMIT ?",
      )
      .all(limit)
      .map((row) => toMeshMessage(row as Record<string, unknown>));
  }

  deleteBefore(cutoffExclusive: string, limit = 1_000): number {
    const cutoff = canonicalTimestamp(cutoffExclusive);
    if (
      cutoff === undefined ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 10_000
    ) {
      throw new MeshDomainPersistenceError();
    }
    try {
      const result = this.database
        .prepare(
          "DELETE FROM messages WHERE id IN (SELECT id FROM messages INDEXED BY messages_retention_index WHERE observed_at < ? ORDER BY observed_at ASC, id ASC LIMIT ?)",
        )
        .run(cutoff, limit);
      return Number(result.changes);
    } catch {
      throw new MeshDomainPersistenceError();
    }
  }
}

export class MeshTelemetryRepository {
  constructor(private readonly database: DatabaseSync) {}

  insert(telemetry: MeshTelemetry): MeshTelemetry {
    try {
      this.database
        .prepare(
          "INSERT INTO telemetry (id, observation_id, mesh_network_id, node_num, packet_id, metric_kind, metrics, observed_at, telemetry_time_seconds) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          telemetry.id,
          telemetry.observationId,
          telemetry.meshNetworkId,
          telemetry.nodeNum,
          telemetry.packetId ?? null,
          telemetry.metricKind,
          JSON.stringify(telemetry.metrics),
          telemetry.observedAt,
          telemetry.telemetryTimeSeconds ?? null,
        );
    } catch {
      throw new MeshDomainPersistenceError();
    }
    const stored = this.find(telemetry.id);
    if (!stored) {
      throw new MeshDomainPersistenceError();
    }
    return stored;
  }

  find(id: string): MeshTelemetry | undefined {
    const row = this.database
      .prepare("SELECT * FROM telemetry WHERE id = ?")
      .get(id);
    return row ? toMeshTelemetry(row) : undefined;
  }

  findByObservation(observationId: string): MeshTelemetry | undefined {
    const row = this.database
      .prepare("SELECT * FROM telemetry WHERE observation_id = ?")
      .get(observationId);
    return row ? toMeshTelemetry(row) : undefined;
  }

  list(limit: number): MeshTelemetry[] {
    return this.database
      .prepare(
        "SELECT * FROM telemetry ORDER BY observed_at DESC, id ASC LIMIT ?",
      )
      .all(limit)
      .map((row) => toMeshTelemetry(row as Record<string, unknown>));
  }

  query(input: MeshTelemetryRangeQuery): MeshTelemetry[] {
    if (!validTelemetryRangeQuery(input)) {
      throw new MeshDomainPersistenceError();
    }
    const clauses: string[] = [];
    const parameters: Array<string | number> = [];
    const from = canonicalTimestamp(input.from);
    const to = canonicalTimestamp(input.to);
    if (input.meshNetworkId !== undefined) {
      clauses.push("mesh_network_id = ?");
      parameters.push(input.meshNetworkId);
    }
    if (input.nodeNum !== undefined) {
      clauses.push("node_num = ?");
      parameters.push(input.nodeNum);
    }
    if (input.metricKind !== undefined) {
      clauses.push("metric_kind = ?");
      parameters.push(input.metricKind);
    }
    if (from !== undefined) {
      clauses.push("observed_at >= ?");
      parameters.push(from);
    }
    if (to !== undefined) {
      clauses.push("observed_at <= ?");
      parameters.push(to);
    }
    const where = clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
    return this.database
      .prepare(
        `SELECT * FROM telemetry${where} ORDER BY observed_at DESC, id ASC LIMIT ?`,
      )
      .all(...parameters, input.limit)
      .map((row) => toMeshTelemetry(row as Record<string, unknown>));
  }

  deleteBefore(cutoffExclusive: string, limit = 1_000): number {
    const cutoff = canonicalTimestamp(cutoffExclusive);
    if (
      cutoff === undefined ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 10_000
    ) {
      throw new MeshDomainPersistenceError();
    }
    try {
      const result = this.database
        .prepare(
          "DELETE FROM telemetry WHERE id IN (SELECT id FROM telemetry WHERE observed_at < ? ORDER BY observed_at ASC, id ASC LIMIT ?)",
        )
        .run(cutoff, limit);
      return Number(result.changes);
    } catch {
      throw new MeshDomainPersistenceError();
    }
  }
}

export interface MeshTelemetryRangeQuery {
  limit: number;
  meshNetworkId?: string;
  nodeNum?: number;
  metricKind?: string;
  from?: string;
  to?: string;
}

function validTelemetryRangeQuery(input: MeshTelemetryRangeQuery): boolean {
  const validTimestamp = (value: string | undefined) =>
    value === undefined ||
    (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value) &&
      Number.isFinite(Date.parse(value)));
  return (
    Number.isInteger(input.limit) &&
    input.limit >= 1 &&
    input.limit <= 200 &&
    (input.meshNetworkId === undefined ||
      (input.meshNetworkId.length >= 1 && input.meshNetworkId.length <= 128)) &&
    (input.nodeNum === undefined ||
      (input.meshNetworkId !== undefined &&
        Number.isInteger(input.nodeNum) &&
        input.nodeNum >= 0 &&
        input.nodeNum <= 4_294_967_295)) &&
    (input.metricKind === undefined ||
      (input.metricKind.length >= 1 && input.metricKind.length <= 64)) &&
    validTimestamp(input.from) &&
    validTimestamp(input.to) &&
    (input.from === undefined ||
      input.to === undefined ||
      Date.parse(input.from) <= Date.parse(input.to))
  );
}

function canonicalTimestamp(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp)
    ? new Date(timestamp).toISOString()
    : undefined;
}

function toStoredJob(row: Record<string, unknown>): StoredJob {
  const errorCode = optionalString(row.error_code);
  return {
    id: String(row.id),
    type: String(row.type),
    status: String(row.status) as JobStatus,
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
    ...(optionalString(row.started_at)
      ? { startedAt: String(row.started_at) }
      : {}),
    ...(optionalString(row.completed_at)
      ? { completedAt: String(row.completed_at) }
      : {}),
    ...(errorCode
      ? {
          error: {
            code: errorCode,
            params: parseErrorParams(row.error_params),
          },
        }
      : {}),
    input: parseJsonRecord(row.input),
    ...(optionalString(row.idempotency_key)
      ? { idempotencyKey: String(row.idempotency_key) }
      : {}),
    ...(optionalString(row.result)
      ? { result: parseJsonRecord(row.result) }
      : {}),
    cancelRequested: Number(row.cancel_requested) === 1,
  };
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function parseJsonRecord(value: unknown): Record<string, unknown> {
  const parsed = JSON.parse(String(value)) as unknown;
  if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
    throw new JobPersistenceError();
  }
  return parsed as Record<string, unknown>;
}

function parseErrorParams(
  value: unknown,
): Record<string, string | number | boolean | null> {
  const params = parseJsonRecord(value);
  for (const parameter of Object.values(params)) {
    if (
      parameter !== null &&
      typeof parameter !== "string" &&
      typeof parameter !== "number" &&
      typeof parameter !== "boolean"
    ) {
      throw new JobPersistenceError();
    }
  }
  return params as Record<string, string | number | boolean | null>;
}

function toMeshObservation(row: Record<string, unknown>): MeshObservation {
  const transport = String(row.transport);
  const backlogClassification = String(row.backlog_classification);
  const deviceRxTimeSeconds = optionalNonNegativeInteger(
    row.device_rx_time_seconds,
  );
  if (
    !TRANSPORT_KINDS.includes(transport as (typeof TRANSPORT_KINDS)[number]) ||
    !BACKLOG_CLASSIFICATIONS.includes(
      backlogClassification as (typeof BACKLOG_CLASSIFICATIONS)[number],
    ) ||
    (row.device_rx_time_seconds !== null && deviceRxTimeSeconds === undefined)
  ) {
    throw new MeshObservationPersistenceError();
  }
  return {
    schemaVersion: 1,
    id: String(row.id),
    transport: transport as MeshObservation["transport"],
    sessionConnectedAt: String(row.session_connected_at),
    ingestedAt: String(row.ingested_at),
    serverIngestedAt: String(row.server_ingested_at),
    ...(deviceRxTimeSeconds !== undefined ? { deviceRxTimeSeconds } : {}),
    backlogClassification:
      backlogClassification as MeshObservation["backlogClassification"],
    normalizedFromRadio: parseNormalizedFromRadio(row.normalized_from_radio),
  };
}

function toMeshNode(row: Record<string, unknown>): MeshNode {
  const nodeNum = optionalNonNegativeInteger(row.node_num);
  if (nodeNum === undefined) {
    throw new MeshDomainPersistenceError();
  }
  return {
    schemaVersion: 1,
    meshNetworkId: String(row.mesh_network_id),
    nodeNum,
    firstSeenAt: String(row.first_seen_at),
    lastSeenAt: String(row.last_seen_at),
    lastObservationId: String(row.last_observation_id),
    ...optionalStringProperty("userId", row.user_id),
    ...optionalStringProperty("longName", row.long_name),
    ...optionalStringProperty("shortName", row.short_name),
    ...optionalStringProperty("hardwareModel", row.hardware_model),
    ...optionalStringProperty("role", row.role),
  };
}

function toMeshMessage(row: Record<string, unknown>): MeshMessage {
  const sender = optionalNonNegativeInteger(row.sender);
  const destination = optionalNullableNonNegativeInteger(row.destination);
  const packetId = optionalNullableNonNegativeInteger(row.packet_id);
  const channel = optionalNullableNonNegativeInteger(row.channel);
  if (
    sender === undefined ||
    destination === "invalid" ||
    packetId === "invalid" ||
    channel === "invalid"
  ) {
    throw new MeshDomainPersistenceError();
  }
  return {
    schemaVersion: 1,
    id: String(row.id),
    observationId: String(row.observation_id),
    meshNetworkId: String(row.mesh_network_id),
    sender,
    ...(destination === undefined ? {} : { destination }),
    ...(packetId === undefined ? {} : { packetId }),
    ...(typeof channel === "number" ? { channel } : {}),
    text: String(row.text),
    observedAt: String(row.observed_at),
  };
}

function toMeshTelemetry(row: Record<string, unknown>): MeshTelemetry {
  const nodeNum = optionalNonNegativeInteger(row.node_num);
  const packetId = optionalNullableNonNegativeInteger(row.packet_id);
  const telemetryTimeSeconds = optionalNullableNonNegativeInteger(
    row.telemetry_time_seconds,
  );
  if (
    nodeNum === undefined ||
    packetId === "invalid" ||
    telemetryTimeSeconds === "invalid"
  ) {
    throw new MeshDomainPersistenceError();
  }
  return {
    schemaVersion: 1,
    id: String(row.id),
    observationId: String(row.observation_id),
    meshNetworkId: String(row.mesh_network_id),
    nodeNum,
    ...(packetId === undefined ? {} : { packetId }),
    metricKind: String(row.metric_kind),
    metrics: parseMetricRecord(row.metrics),
    observedAt: String(row.observed_at),
    ...(telemetryTimeSeconds === undefined ? {} : { telemetryTimeSeconds }),
  };
}

function optionalNonNegativeInteger(value: unknown): number | undefined {
  return typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 0 &&
    value <= 4_294_967_295
    ? value
    : undefined;
}

function optionalNullableNonNegativeInteger(
  value: unknown,
): number | undefined | "invalid" {
  if (value === null) {
    return undefined;
  }
  return optionalNonNegativeInteger(value) ?? "invalid";
}

function optionalStringProperty<Key extends string>(
  key: Key,
  value: unknown,
): Record<Key, string> | Record<never, never> {
  const text = optionalString(value);
  return text ? ({ [key]: text } as Record<Key, string>) : {};
}

function parseMetricRecord(value: unknown): MeshTelemetry["metrics"] {
  try {
    const parsed = JSON.parse(String(value)) as unknown;
    if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
      throw new MeshDomainPersistenceError();
    }
    const metrics: MeshTelemetry["metrics"] = {};
    for (const [key, metric] of Object.entries(parsed)) {
      if (
        (typeof metric === "string" && metric.length <= 512) ||
        typeof metric === "boolean" ||
        (typeof metric === "number" && Number.isFinite(metric))
      ) {
        metrics[key] = metric;
        continue;
      }
      throw new MeshDomainPersistenceError();
    }
    return metrics;
  } catch (error) {
    if (error instanceof MeshDomainPersistenceError) {
      throw error;
    }
    throw new MeshDomainPersistenceError();
  }
}

function parseNormalizedFromRadio(
  value: unknown,
): MeshObservation["normalizedFromRadio"] {
  try {
    const parsed = JSON.parse(String(value)) as unknown;
    if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
      throw new MeshObservationPersistenceError();
    }
    return parsed as MeshObservation["normalizedFromRadio"];
  } catch (error) {
    if (error instanceof MeshObservationPersistenceError) {
      throw error;
    }
    throw new MeshObservationPersistenceError();
  }
}
