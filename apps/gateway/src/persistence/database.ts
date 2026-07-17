import { DatabaseSync } from "node:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

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
];
