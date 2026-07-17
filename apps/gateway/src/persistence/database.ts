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

export interface Migration {
  version: number;
  name: string;
  up(database: DatabaseSync): void;
}

export class DatabaseMigrationError extends Error {
  readonly code = "DATABASE_MIGRATION_FAILED";
}

export class GatewayDatabase {
  readonly connection: DatabaseSync;
  readonly jobs: JobRepository;
  readonly meshMessages: MeshMessageRepository;
  readonly meshNodes: MeshNodeRepository;
  readonly meshObservations: MeshObservationRepository;
  readonly meshTelemetry: MeshTelemetryRepository;
  readonly settings: SettingsRepository;

  constructor(path: string, migrations: Migration[] = defaultMigrations) {
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
    this.settings = new SettingsRepository(this.connection);
  }

  close(): void {
    this.connection.close();
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

  findByStatuses(statuses: readonly JobStatus[]): StoredJob[] {
    if (statuses.length === 0) {
      return [];
    }
    const placeholders = statuses.map(() => "?").join(", ");
    return this.database
      .prepare(`SELECT * FROM jobs WHERE status IN (${placeholders})`)
      .all(...statuses)
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

const defaultMigrations: Migration[] = [
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
];
