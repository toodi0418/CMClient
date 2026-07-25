import { DatabaseSync } from "node:sqlite";

import { describe, expect, it } from "vitest";

import {
  createSqlMigration,
  DatabaseMigrationError,
  DatabaseSchemaHistoryError,
  GatewayDatabase,
  gatewayMigrations,
  inspectMigrationHistory,
  runMigrations,
} from "./database";

describe("immutable Gateway migration history", () => {
  it("upgrades a legacy name-only v7 journal and retains its data", () => {
    const database = legacyDatabase(7);
    database
      .prepare("INSERT INTO settings (key, value) VALUES ('retained', 'true')")
      .run();

    expect(
      inspectMigrationHistory(database, gatewayMigrations, false),
    ).toMatchObject({
      digestStatus: "legacy_name_only",
      entries: expect.arrayContaining([
        expect.objectContaining({ version: 7, name: "aprs_remote_high_water" }),
      ]),
    });

    runMigrations(database, gatewayMigrations, true);

    const report = inspectMigrationHistory(database, gatewayMigrations, true);
    expect(report.digestStatus).toBe("recorded");
    expect(report.entries).toEqual(
      gatewayMigrations.map(({ version, name, sha256 }) => ({
        version,
        name,
        sha256,
      })),
    );
    expect(
      database
        .prepare("SELECT value FROM settings WHERE key = 'retained'")
        .get(),
    ).toEqual({ value: "true" });
    database.close();
  });

  it("rolls back the full atomic batch when a compiled SQL migration fails", () => {
    const database = legacyDatabase(7);
    const broken = createSqlMigration(
      8,
      "broken",
      "CREATE TABLE atomic_rollback_probe (id INTEGER); SELECT * FROM missing_atomic_fixture;",
    );

    expect(() =>
      runMigrations(database, [...gatewayMigrations.slice(0, 7), broken], true),
    ).toThrow(DatabaseMigrationError);

    const rolledBackHistory = inspectMigrationHistory(
      database,
      gatewayMigrations,
      false,
    );
    expect(rolledBackHistory.digestStatus).toBe("legacy_name_only");
    expect(rolledBackHistory.entries).toHaveLength(7);
    expect(
      database
        .prepare(
          "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'atomic_rollback_probe'",
        )
        .get(),
    ).toBeUndefined();
    database.close();
  });

  it("rejects a legacy name-only journal whose domain column is missing", () => {
    expectLegacySchemaDrift(1, (database) => {
      database.exec("ALTER TABLE settings DROP COLUMN updated_at");
    });
  });

  it("rejects a legacy name-only journal whose domain column changed", () => {
    expectLegacySchemaDrift(1, (database) => {
      database.exec(
        "ALTER TABLE settings RENAME COLUMN value TO changed_value",
      );
    });
  });

  it("rejects a legacy name-only journal whose compiled index is missing", () => {
    expectLegacySchemaDrift(7, (database) => {
      database.exec(
        "DROP INDEX aprs_remote_high_water_callsign_event_time_index",
      );
    });
  });

  it("rejects a legacy name-only journal whose foreign key changed", () => {
    expectLegacySchemaDrift(4, (database) => {
      database.exec("PRAGMA foreign_keys = OFF");
      database.exec("DROP INDEX messages_network_sender_observed_at_index");
      database.exec("ALTER TABLE messages RENAME TO messages_original");
      database.exec(
        "CREATE TABLE messages (id TEXT PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE, mesh_network_id TEXT NOT NULL, sender INTEGER NOT NULL CHECK (sender >= 0 AND sender <= 4294967295), destination INTEGER CHECK (destination IS NULL OR (destination >= 0 AND destination <= 4294967295)), packet_id INTEGER CHECK (packet_id IS NULL OR (packet_id >= 0 AND packet_id <= 4294967295)), channel INTEGER CHECK (channel IS NULL OR (channel >= 0 AND channel <= 255)), text TEXT NOT NULL, observed_at TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
      );
      database.exec(
        "CREATE INDEX messages_network_sender_observed_at_index ON messages (mesh_network_id, sender, observed_at DESC)",
      );
      database.exec("DROP TABLE messages_original");
      database.exec("PRAGMA foreign_keys = ON");
    });
  });

  it("rejects a legacy name-only journal with an uncompiled trigger", () => {
    expectLegacySchemaDrift(1, (database) => {
      database.exec(
        "CREATE TRIGGER settings_untrusted_trigger AFTER INSERT ON settings BEGIN SELECT 1; END",
      );
    });
  });

  it("rejects a changed migration name", () => {
    expectHistoryDrift((database) => {
      database
        .prepare(
          "UPDATE schema_migrations SET name = 'changed' WHERE version = 3",
        )
        .run();
    });
  });

  it("rejects a changed migration digest", () => {
    expectHistoryDrift((database) => {
      database
        .prepare("UPDATE schema_migrations SET sha256 = ? WHERE version = 3")
        .run("b".repeat(64));
    });
  });

  it("rejects a future migration version", () => {
    expectHistoryDrift((database) => {
      database
        .prepare(
          "INSERT INTO schema_migrations (version, name, sha256) VALUES (19, 'future', ?)",
        )
        .run("c".repeat(64));
    });
  });

  it("rejects a migration history gap", () => {
    expectHistoryDrift((database) => {
      database.prepare("DELETE FROM schema_migrations WHERE version = 4").run();
    });
  });

  it("rejects an unexpected migration-history column", () => {
    expectHistoryDrift((database) => {
      database.exec("ALTER TABLE schema_migrations ADD COLUMN untrusted TEXT");
    });
  });
});

function legacyDatabase(throughVersion: number): DatabaseSync {
  const database = new DatabaseSync(":memory:");
  database.exec(
    "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
  );
  const insert = database.prepare(
    "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
  );
  for (const migration of gatewayMigrations.slice(0, throughVersion)) {
    database.exec(migration.sql);
    insert.run(migration.version, migration.name);
  }
  database.exec("PRAGMA foreign_keys = ON");
  return database;
}

function expectHistoryDrift(mutate: (database: DatabaseSync) => void): void {
  const gateway = new GatewayDatabase(":memory:");
  mutate(gateway.connection);
  expect(() =>
    inspectMigrationHistory(gateway.connection, gatewayMigrations, true),
  ).toThrow(DatabaseSchemaHistoryError);
  gateway.close();
}

function expectLegacySchemaDrift(
  throughVersion: number,
  mutate: (database: DatabaseSync) => void,
): void {
  const database = legacyDatabase(throughVersion);
  mutate(database);
  expect(() => runMigrations(database, gatewayMigrations, true)).toThrow(
    DatabaseSchemaHistoryError,
  );
  expect(
    database
      .prepare("PRAGMA table_info(schema_migrations)")
      .all()
      .map((row) => String(row.name)),
  ).not.toContain("sha256");
  database.close();
}
