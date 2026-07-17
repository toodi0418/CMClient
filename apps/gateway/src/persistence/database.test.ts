import { existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  DatabaseMigrationError,
  GatewayDatabase,
  runMigrations,
} from "./database";

function databasePath(name: string): string {
  return join(tmpdir(), `cmclient-gateway-${process.pid}-${name}.sqlite`);
}

describe("GatewayDatabase", () => {
  it("enables WAL, journals migrations, and persists typed settings", () => {
    const path = databasePath("wal");
    rmSync(path, { force: true });
    const database = new GatewayDatabase(path);
    const journalMode = database.connection
      .prepare("PRAGMA journal_mode")
      .get();
    if (!journalMode) {
      throw new Error("SQLite did not return journal mode");
    }
    expect(String(journalMode.journal_mode).toLowerCase()).toBe("wal");
    database.settings.set("web.enabled", true);
    expect(database.settings.get<boolean>("web.enabled")).toBe(true);
    expect(
      database.connection
        .prepare("SELECT version FROM schema_migrations")
        .all(),
    ).toHaveLength(1);
    database.close();
    expect(existsSync(path)).toBe(true);
    rmSync(path, { force: true });
  });

  it("rejects duplicate migration versions before applying work", () => {
    const database = new GatewayDatabase(":memory:");
    expect(() =>
      runMigrations(database.connection, [
        { version: 2, name: "one", up: () => undefined },
        { version: 2, name: "two", up: () => undefined },
      ]),
    ).toThrow(DatabaseMigrationError);
    database.close();
  });

  it("rolls back a failed migration without advancing the journal", () => {
    const database = new GatewayDatabase(":memory:");
    expect(() =>
      runMigrations(database.connection, [
        {
          version: 2,
          name: "broken",
          up(connection) {
            connection.exec(
              "CREATE TABLE migration_rollback_probe (id INTEGER)",
            );
            throw new Error("fixture failure");
          },
        },
      ]),
    ).toThrow(DatabaseMigrationError);
    expect(
      database.connection
        .prepare("SELECT version FROM schema_migrations WHERE version = 2")
        .get(),
    ).toBeUndefined();
    expect(
      database.connection
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .get("migration_rollback_probe"),
    ).toBeUndefined();
    database.close();
  });
});
