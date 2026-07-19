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

export interface Migration {
  version: number;
  name: string;
  up(database: DatabaseSync): void;
}

export class DatabaseMigrationError extends Error {
  readonly code = "DATABASE_MIGRATION_FAILED";
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

  constructor(path: string, migrations: Migration[] = gatewayMigrations) {
    if (path !== ":memory:") {
      mkdirSync(dirname(path), { recursive: true });
    }
    this.connection = new DatabaseSync(path);
    this.connection.exec("PRAGMA journal_mode = WAL");
    this.connection.exec("PRAGMA foreign_keys = ON");
    this.connection.exec("PRAGMA busy_timeout = 5000");
    runMigrations(this.connection, migrations);
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
  migrations: Migration[],
): void {
  database.exec(
    "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
  );
  const ordered = [...migrations].sort(
    (left, right) => left.version - right.version,
  );
  if (
    new Set(ordered.map((migration) => migration.version)).size !==
    ordered.length
  ) {
    throw new DatabaseMigrationError();
  }
  const applied = new Set(
    database
      .prepare("SELECT version FROM schema_migrations ORDER BY version")
      .all()
      .map((row) => Number(row.version)),
  );

  for (const migration of ordered) {
    if (applied.has(migration.version)) {
      continue;
    }
    try {
      database.exec("BEGIN IMMEDIATE");
      migration.up(database);
      database
        .prepare("INSERT INTO schema_migrations (version, name) VALUES (?, ?)")
        .run(migration.version, migration.name);
      database.exec("COMMIT");
    } catch {
      database.exec("ROLLBACK");
      throw new DatabaseMigrationError();
    }
  }
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

export const gatewayMigrations: Migration[] = [
  {
    version: 1,
    name: "settings",
    up(database) {
      database.exec(
        "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
      );
    },
  },
  {
    version: 2,
    name: "jobs",
    up(database) {
      database.exec(
        "CREATE TABLE jobs (id TEXT PRIMARY KEY, type TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'waiting', 'succeeded', 'failed', 'cancelling', 'cancelled', 'rolling_back', 'rolled_back')), input TEXT NOT NULL, result TEXT, error_code TEXT, error_params TEXT, idempotency_key TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)), created_at TEXT NOT NULL, updated_at TEXT NOT NULL, started_at TEXT, completed_at TEXT)",
      );
      database.exec(
        "CREATE UNIQUE INDEX jobs_type_idempotency_key_unique ON jobs (type, idempotency_key) WHERE idempotency_key IS NOT NULL",
      );
      database.exec(
        "CREATE INDEX jobs_status_updated_at_index ON jobs (status, updated_at)",
      );
    },
  },
  {
    version: 3,
    name: "mesh_observations",
    up(database) {
      database.exec(
        "CREATE TABLE mesh_observations (id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK (schema_version = 1), transport TEXT NOT NULL CHECK (transport IN ('tcp', 'serial', 'simulator')), session_connected_at TEXT NOT NULL, ingested_at TEXT NOT NULL, server_ingested_at TEXT NOT NULL, device_rx_time_seconds INTEGER CHECK (device_rx_time_seconds IS NULL OR (device_rx_time_seconds >= 0 AND device_rx_time_seconds <= 4294967295)), backlog_classification TEXT NOT NULL CHECK (backlog_classification IN ('backlog', 'live', 'unknown')), normalized_from_radio TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
      );
      database.exec(
        "CREATE INDEX mesh_observations_session_ingested_at_index ON mesh_observations (session_connected_at, ingested_at)",
      );
    },
  },
  {
    version: 4,
    name: "mesh_domain_records",
    up(database) {
      database.exec(
        "CREATE TABLE nodes (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), user_id TEXT, long_name TEXT, short_name TEXT, hardware_model TEXT, role TEXT, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, last_observation_id TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num))",
      );
      database.exec(
        "CREATE INDEX nodes_last_seen_at_index ON nodes (mesh_network_id, last_seen_at DESC)",
      );
      database.exec(
        "CREATE TABLE messages (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE REFERENCES mesh_observations(id), mesh_network_id TEXT NOT NULL, sender INTEGER NOT NULL CHECK (sender >= 0 AND sender <= 4294967295), destination INTEGER CHECK (destination IS NULL OR (destination >= 0 AND destination <= 4294967295)), packet_id INTEGER CHECK (packet_id IS NULL OR (packet_id >= 0 AND packet_id <= 4294967295)), channel INTEGER CHECK (channel IS NULL OR (channel >= 0 AND channel <= 255)), text TEXT NOT NULL, observed_at TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
      );
      database.exec(
        "CREATE INDEX messages_network_sender_observed_at_index ON messages (mesh_network_id, sender, observed_at DESC)",
      );
      database.exec(
        "CREATE TABLE telemetry (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE REFERENCES mesh_observations(id), mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), packet_id INTEGER CHECK (packet_id IS NULL OR (packet_id >= 0 AND packet_id <= 4294967295)), metric_kind TEXT NOT NULL, metrics TEXT NOT NULL, observed_at TEXT NOT NULL, telemetry_time_seconds INTEGER CHECK (telemetry_time_seconds IS NULL OR (telemetry_time_seconds > 0 AND telemetry_time_seconds <= 4294967295)), created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
      );
      database.exec(
        "CREATE INDEX telemetry_network_node_observed_at_index ON telemetry (mesh_network_id, node_num, observed_at DESC)",
      );
    },
  },
  {
    version: 5,
    name: "position_domain_records",
    up(database) {
      database.exec(
        "CREATE TABLE position_observations (id TEXT PRIMARY KEY, mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), mesh_observation_id TEXT NOT NULL REFERENCES mesh_observations(id), gateway_id TEXT NOT NULL, transport TEXT NOT NULL CHECK (transport IN ('tcp', 'serial', 'simulator')), session_connected_at TEXT NOT NULL, ingested_at TEXT NOT NULL, server_ingested_at TEXT NOT NULL, device_rx_time_seconds INTEGER CHECK (device_rx_time_seconds IS NULL OR (device_rx_time_seconds >= 0 AND device_rx_time_seconds <= 4294967295)), backlog_classification TEXT NOT NULL CHECK (backlog_classification IN ('backlog', 'live', 'unknown')), packet_id INTEGER CHECK (packet_id IS NULL OR (packet_id >= 0 AND packet_id <= 4294967295)), payload_hash TEXT NOT NULL, via_mqtt INTEGER CHECK (via_mqtt IS NULL OR via_mqtt IN (0, 1)), rx_snr REAL, rx_rssi INTEGER, hop_limit INTEGER CHECK (hop_limit IS NULL OR hop_limit >= 0), hop_start INTEGER CHECK (hop_start IS NULL OR hop_start >= 0), position TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
      );
      database.exec(
        "CREATE INDEX position_observations_network_node_ingested_at_index ON position_observations (mesh_network_id, node_num, ingested_at DESC)",
      );
      database.exec(
        "CREATE TABLE position_events (id TEXT PRIMARY KEY, canonical_key TEXT NOT NULL UNIQUE, mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), source_observation_id TEXT NOT NULL REFERENCES position_observations(id), payload_hash TEXT NOT NULL, event_time TEXT, event_time_source TEXT CHECK (event_time_source IS NULL OR event_time_source IN ('position_timestamp', 'position_time', 'sequence')), sequence_epoch INTEGER CHECK (sequence_epoch IS NULL OR sequence_epoch >= 0), sequence_number INTEGER CHECK (sequence_number IS NULL OR (sequence_number >= 0 AND sequence_number <= 4294967295)), position TEXT NOT NULL, created_at TEXT NOT NULL)",
      );
      database.exec(
        "CREATE INDEX position_events_network_node_event_time_index ON position_events (mesh_network_id, node_num, event_time DESC)",
      );
      database.exec(
        "CREATE TABLE position_decisions (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL REFERENCES position_observations(id), canonical_event_id TEXT REFERENCES position_events(id), code TEXT NOT NULL, decided_at TEXT NOT NULL, parameters TEXT NOT NULL)",
      );
      database.exec(
        "CREATE INDEX position_decisions_observation_decided_at_index ON position_decisions (observation_id, decided_at DESC)",
      );
      database.exec(
        "CREATE TABLE node_position_state (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), callsign TEXT NOT NULL, mapping_version TEXT NOT NULL, latest_canonical_event_id TEXT REFERENCES position_events(id), latest_event_time TEXT, latest_sequence_epoch INTEGER CHECK (latest_sequence_epoch IS NULL OR latest_sequence_epoch >= 0), latest_sequence_number INTEGER CHECK (latest_sequence_number IS NULL OR (latest_sequence_number >= 0 AND latest_sequence_number <= 4294967295)), latest_latitude_i INTEGER, latest_longitude_i INTEGER, updated_at TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num, callsign, mapping_version))",
      );
    },
  },
  {
    version: 6,
    name: "aprs_outbox",
    up(database) {
      database.exec(
        "CREATE TABLE aprs_outbox (id TEXT PRIMARY KEY, callsign TEXT NOT NULL, canonical_event_id TEXT NOT NULL REFERENCES position_events(id), data TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('queued', 'sending', 'sent', 'failed')), attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0), next_attempt_at TEXT NOT NULL, last_error_code TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, sent_at TEXT, UNIQUE (callsign, canonical_event_id))",
      );
      database.exec(
        "CREATE INDEX aprs_outbox_due_index ON aprs_outbox (status, next_attempt_at)",
      );
    },
  },
  {
    version: 7,
    name: "aprs_remote_high_water",
    up(database) {
      database.exec(
        "CREATE TABLE aprs_remote_high_water (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), callsign TEXT NOT NULL, mapping_version TEXT NOT NULL, latest_event_time TEXT NOT NULL, latest_event_marker TEXT NOT NULL, received_at TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num, callsign, mapping_version))",
      );
      database.exec(
        "CREATE INDEX aprs_remote_high_water_callsign_event_time_index ON aprs_remote_high_water (callsign, latest_event_time DESC)",
      );
    },
  },
  {
    version: 8,
    name: "callmesh_mappings",
    up(database) {
      database.exec(
        "CREATE TABLE callmesh_mappings (version TEXT NOT NULL, effective_at TEXT NOT NULL, mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), callsign TEXT NOT NULL, PRIMARY KEY (version, effective_at, mesh_network_id, node_num, callsign))",
      );
      database.exec(
        "CREATE INDEX callmesh_mappings_target_index ON callmesh_mappings (mesh_network_id, node_num, effective_at DESC)",
      );
    },
  },
  {
    version: 9,
    name: "telemetry_range_query_index",
    up(database) {
      database.exec(
        "CREATE INDEX telemetry_metric_observed_at_index ON telemetry (metric_kind, observed_at DESC)",
      );
    },
  },
  {
    version: 10,
    name: "load_retention_indexes",
    up(database) {
      database.exec(
        "CREATE INDEX mesh_observations_ingested_at_index ON mesh_observations (ingested_at, id)",
      );
      database.exec(
        "CREATE INDEX nodes_last_observation_id_index ON nodes (last_observation_id)",
      );
      database.exec(
        "CREATE INDEX position_observations_mesh_observation_id_index ON position_observations (mesh_observation_id)",
      );
      database.exec(
        "CREATE INDEX telemetry_observed_at_index ON telemetry (observed_at, id)",
      );
      database.exec(
        "CREATE INDEX jobs_terminal_retention_index ON jobs (completed_at, id) WHERE status IN ('succeeded', 'failed', 'cancelled', 'rolled_back')",
      );
      database.exec(
        "CREATE INDEX jobs_queued_created_at_index ON jobs (created_at, id) WHERE status = 'queued'",
      );
    },
  },
  {
    version: 11,
    name: "read_projection_indexes",
    up(database) {
      database.exec(
        "CREATE INDEX nodes_recent_projection_index ON nodes (last_seen_at DESC, node_num ASC)",
      );
      database.exec(
        "CREATE INDEX messages_recent_projection_index ON messages (observed_at DESC, id ASC)",
      );
      database.exec(
        "CREATE INDEX telemetry_recent_projection_index ON telemetry (observed_at DESC, id ASC)",
      );
      database.exec(
        "CREATE INDEX position_events_recent_projection_index ON position_events (COALESCE(event_time, created_at) DESC, id ASC)",
      );
      database.exec(
        "CREATE INDEX aprs_outbox_recent_projection_index ON aprs_outbox (updated_at DESC, id ASC)",
      );
      database.exec(
        "CREATE INDEX aprs_outbox_due_order_index ON aprs_outbox (next_attempt_at ASC, created_at ASC, id ASC) WHERE status IN ('queued', 'failed')",
      );
      database.exec(
        "CREATE INDEX aprs_outbox_sent_retention_index ON aprs_outbox (sent_at ASC, id ASC) WHERE status = 'sent'",
      );
    },
  },
  {
    version: 12,
    name: "bounded_domain_retention_and_delivery_high_water",
    up(database) {
      database.exec(
        "CREATE TABLE aprs_delivery_high_water (mesh_network_id TEXT NOT NULL, node_num INTEGER NOT NULL CHECK (node_num >= 0 AND node_num <= 4294967295), callsign TEXT NOT NULL, latest_canonical_event_id TEXT NOT NULL REFERENCES position_events(id), latest_event_time TEXT NOT NULL, latest_sequence_epoch INTEGER CHECK (latest_sequence_epoch IS NULL OR latest_sequence_epoch >= 0), latest_sequence_number INTEGER CHECK (latest_sequence_number IS NULL OR (latest_sequence_number >= 0 AND latest_sequence_number <= 4294967295)), delivered_at TEXT NOT NULL, PRIMARY KEY (mesh_network_id, node_num, callsign))",
      );
      database.exec(
        "INSERT INTO aprs_delivery_high_water (mesh_network_id, node_num, callsign, latest_canonical_event_id, latest_event_time, latest_sequence_epoch, latest_sequence_number, delivered_at) SELECT mesh_network_id, node_num, callsign, canonical_event_id, event_time, sequence_epoch, sequence_number, sent_at FROM (SELECT event.mesh_network_id, event.node_num, outbox.callsign, event.id AS canonical_event_id, event.event_time, event.sequence_epoch, event.sequence_number, outbox.sent_at, ROW_NUMBER() OVER (PARTITION BY event.mesh_network_id, event.node_num, outbox.callsign ORDER BY event.event_time DESC, outbox.sent_at DESC, event.id DESC) AS delivery_rank FROM aprs_outbox AS outbox JOIN position_events AS event ON event.id = outbox.canonical_event_id WHERE outbox.status = 'sent' AND outbox.sent_at IS NOT NULL AND event.event_time IS NOT NULL) WHERE delivery_rank = 1",
      );
      database.exec(
        "CREATE INDEX aprs_delivery_high_water_latest_event_id_index ON aprs_delivery_high_water (latest_canonical_event_id)",
      );
      database.exec(
        "CREATE INDEX node_position_state_latest_event_id_index ON node_position_state (latest_canonical_event_id)",
      );
      database.exec(
        "CREATE INDEX jobs_queued_type_created_at_index ON jobs (type, created_at ASC, id ASC) WHERE status = 'queued'",
      );

      database.exec(
        "CREATE INDEX messages_retention_index ON messages (observed_at ASC, id ASC)",
      );
      database.exec(
        "CREATE INDEX position_decisions_retention_index ON position_decisions (decided_at ASC, id ASC)",
      );
      database.exec(
        "CREATE INDEX position_decisions_canonical_event_id_index ON position_decisions (canonical_event_id)",
      );
      database.exec(
        "CREATE INDEX position_events_retention_index ON position_events (created_at ASC, id ASC)",
      );
      database.exec(
        "CREATE INDEX position_events_source_observation_id_index ON position_events (source_observation_id)",
      );
      database.exec(
        "CREATE INDEX position_observations_retention_index ON position_observations (ingested_at ASC, id ASC)",
      );
      database.exec(
        "CREATE INDEX aprs_outbox_canonical_event_id_index ON aprs_outbox (canonical_event_id)",
      );
    },
  },
  {
    version: 13,
    name: "aprs_outbox_order_snapshots",
    up(database) {
      database.exec("ALTER TABLE aprs_outbox ADD COLUMN mesh_network_id TEXT");
      database.exec("ALTER TABLE aprs_outbox ADD COLUMN node_num INTEGER");
      database.exec("ALTER TABLE aprs_outbox ADD COLUMN mapping_version TEXT");
      database.exec("ALTER TABLE aprs_outbox ADD COLUMN event_time TEXT");
      database.exec(
        "ALTER TABLE aprs_outbox ADD COLUMN sequence_epoch INTEGER",
      );
      database.exec(
        "ALTER TABLE aprs_outbox ADD COLUMN sequence_number INTEGER",
      );
      database.exec(
        "UPDATE aprs_outbox SET mesh_network_id = (SELECT event.mesh_network_id FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id), node_num = (SELECT event.node_num FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id), event_time = (SELECT event.event_time FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id), sequence_epoch = (SELECT event.sequence_epoch FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id), sequence_number = (SELECT event.sequence_number FROM position_events AS event WHERE event.id = aprs_outbox.canonical_event_id)",
      );
      database.exec(
        "ALTER TABLE aprs_delivery_high_water ADD COLUMN latest_mapping_version TEXT",
      );
      database.exec(
        "CREATE INDEX aprs_outbox_active_order_index ON aprs_outbox (mesh_network_id, node_num, callsign, event_time DESC, sequence_epoch DESC, sequence_number DESC, id ASC) WHERE status IN ('queued', 'sending', 'failed')",
      );
    },
  },
  {
    version: 14,
    name: "callmesh_sync_high_water",
    up(database) {
      database.exec(
        "CREATE TABLE callmesh_sync_state (id INTEGER PRIMARY KEY CHECK (id = 1), active INTEGER NOT NULL CHECK (active IN (0, 1)), mapping_hash TEXT NOT NULL CHECK (length(mapping_hash) BETWEEN 1 AND 128), accepted_server_time TEXT NOT NULL, mappings_fingerprint TEXT NOT NULL CHECK (length(mappings_fingerprint) = 64), last_heartbeat_at TEXT NOT NULL, mapping_synced_at TEXT NOT NULL, provision_json TEXT, provision_expires_at TEXT, provision_fingerprint TEXT CHECK (provision_fingerprint IS NULL OR length(provision_fingerprint) = 64), updated_at TEXT NOT NULL, CHECK ((provision_json IS NULL AND provision_expires_at IS NULL AND provision_fingerprint IS NULL) OR (provision_json IS NOT NULL AND provision_expires_at IS NOT NULL AND provision_fingerprint IS NOT NULL)))",
      );
      database.exec(
        "CREATE TABLE callmesh_sync_history (mapping_hash TEXT PRIMARY KEY CHECK (length(mapping_hash) BETWEEN 1 AND 128), first_server_time TEXT NOT NULL, last_server_time TEXT NOT NULL, mappings_fingerprint TEXT NOT NULL CHECK (length(mappings_fingerprint) = 64))",
      );
    },
  },
];
