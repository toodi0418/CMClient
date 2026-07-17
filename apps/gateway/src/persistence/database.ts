import { DatabaseSync } from "node:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

import type { JobDetail, JobError, JobStatus } from "@cmclient/contracts";

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
];
