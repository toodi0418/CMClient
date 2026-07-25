import { createHash } from "node:crypto";
import {
  link,
  lstat,
  mkdtemp,
  mkdir,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { Readable, Writable } from "node:stream";
import { DatabaseSync } from "node:sqlite";

import { afterEach, describe, expect, it, vi } from "vitest";

const maintenanceTestState = vi.hoisted<{
  afterBackup?: () => void | Promise<void>;
}>(() => ({}));

vi.mock("./backup", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./backup")>();
  return {
    ...actual,
    backupDatabaseToPath: async (
      ...arguments_: Parameters<typeof actual.backupDatabaseToPath>
    ): Promise<number> => {
      const pages = await actual.backupDatabaseToPath(...arguments_);
      await maintenanceTestState.afterBackup?.();
      return pages;
    },
  };
});

import { sha256File } from "./backup";
import {
  OFFLINE_MAINTENANCE_INPUT_MAX_BYTES,
  OFFLINE_MAINTENANCE_OUTPUT_MAX_BYTES,
  OfflineMaintenanceError,
  OfflineMaintenanceRetryableError,
  offlineMaintenanceExitCode,
  readOfflineMaintenanceRequest,
  runOfflineMaintenance,
  runOfflineMaintenanceCommand,
  validateOfflineMaintenanceRequest,
  type OfflineMaintenanceReport,
  type OfflineMaintenanceRequest,
} from "./offline-maintenance";
import { GatewayDatabase, gatewayMigrations } from "./persistence/database";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  delete maintenanceTestState.afterBackup;
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("Gateway offline maintenance", { timeout: 20_000 }, () => {
  it("backs up and migrates a populated v7 database without changing the source", async () => {
    const directory = await temporaryDirectory("cmclient-offline-v7-");
    const sourceDirectory = join(directory, "source");
    const targetDirectory = join(directory, "target");
    await Promise.all([mkdir(sourceDirectory), mkdir(targetDirectory)]);
    const sourcePath = join(sourceDirectory, "source.sqlite");
    const stagedPath = join(targetDirectory, "staged.sqlite");
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.settings.set("retained.setting", { enabled: true });
    source.connection
      .prepare(
        "INSERT INTO aprs_remote_high_water (mesh_network_id, node_num, callsign, mapping_version, latest_event_time, latest_event_marker, received_at) VALUES ('mesh', 42, 'N0CALL-7', 'mapping-v1', '2026-07-24T00:00:00.000Z', 'marker', '2026-07-24T00:00:01.000Z')",
      )
      .run();
    source.close();
    expect(typeof (await lstat(sourcePath, { bigint: true })).ino).toBe(
      "bigint",
    );
    const sourceBefore = await fileSnapshot(sourcePath);
    const sourceDirectoryBefore = await directorySnapshot(sourceDirectory);

    const output: Buffer[] = [];
    const sink = new Writable({
      write(chunk, _encoding, callback) {
        output.push(Buffer.from(chunk));
        callback();
      },
    });
    await runOfflineMaintenanceCommand(
      Readable.from([
        JSON.stringify(maintenanceRequest(sourcePath, stagedPath)),
      ]),
      sink,
    );
    const encodedReport = Buffer.concat(output);
    expect(encodedReport.length).toBeLessThanOrEqual(
      OFFLINE_MAINTENANCE_OUTPUT_MAX_BYTES,
    );
    const report = JSON.parse(
      encodedReport.toString("utf8"),
    ) as OfflineMaintenanceReport;

    expect(Object.keys(report).sort()).toEqual([
      "domainCounts",
      "foreignKeyViolations",
      "integrity",
      "operation",
      "schemaHistory",
      "schemaVersion",
      "sourceDatabaseSha256",
      "stagedDatabaseBytes",
      "stagedDatabaseSha256",
      "type",
    ]);
    expect(report).toMatchObject({
      schemaVersion: 1,
      type: "gateway.offline-maintenance-report",
      operation: "backup_migrate_verify",
      sourceDatabaseSha256: sourceBefore.sha256,
      stagedDatabaseSha256: expect.stringMatching(/^[a-f0-9]{64}$/),
      stagedDatabaseBytes: expect.any(Number),
      integrity: "ok",
      foreignKeyViolations: 0,
      domainCounts: {
        settings: 1,
        jobs: 0,
        mesh_observations: 0,
        nodes: 0,
        messages: 0,
        telemetry: 0,
        position_observations: 0,
        position_events: 0,
        position_decisions: 0,
        node_position_state: 0,
        aprs_outbox: 0,
        aprs_remote_high_water: 1,
        callmesh_mappings: 0,
        aprs_delivery_high_water: 0,
        callmesh_sync_state: 0,
        callmesh_sync_history: 0,
      },
    });
    expect(report.schemaHistory).toEqual(
      gatewayMigrations.map(({ version, name, sha256 }) => ({
        version,
        name,
        sha256,
      })),
    );
    expect(report.stagedDatabaseBytes).toBeGreaterThan(0);
    expect(report.stagedDatabaseSha256).toBe(await sha256File(stagedPath));
    expect(await fileSnapshot(sourcePath)).toEqual(sourceBefore);
    expect(await directorySnapshot(sourceDirectory)).toEqual(
      sourceDirectoryBefore,
    );
    expect(JSON.stringify(report)).not.toContain(sourcePath);
    expect(JSON.stringify(report)).not.toContain(stagedPath);

    const staged = new DatabaseSync(stagedPath, { readOnly: true });
    expect(
      staged
        .prepare("SELECT value FROM settings WHERE key = 'retained.setting'")
        .get(),
    ).toEqual({ value: '{"enabled":true}' });
    expect(
      staged.prepare("SELECT COUNT(*) AS count FROM schema_migrations").get(),
    ).toEqual({ count: 16 });
    staged.close();

    const untouchedSource = new DatabaseSync(sourcePath, { readOnly: true });
    expect(
      untouchedSource
        .prepare("SELECT MAX(version) AS version FROM schema_migrations")
        .get(),
    ).toEqual({ version: 7 });
    expect(
      untouchedSource
        .prepare(
          "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'callmesh_mappings'",
        )
        .get(),
    ).toBeUndefined();
    untouchedSource.close();
  });

  it("includes committed WAL data while leaving main, WAL, and SHM bytes untouched", async () => {
    const directory = await temporaryDirectory("cmclient-offline-wal-");
    const sourceDirectory = join(directory, "source");
    const targetDirectory = join(directory, "target");
    await Promise.all([mkdir(sourceDirectory), mkdir(targetDirectory)]);
    const sourcePath = join(sourceDirectory, "source.sqlite");
    const stagedPath = join(targetDirectory, "staged.sqlite");
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    try {
      source.connection.prepare("PRAGMA wal_checkpoint(TRUNCATE)").get();
      source.connection.exec("PRAGMA wal_autocheckpoint = 0");
      source.settings.set("wal.only.setting", { retained: true });
      const sourceDirectoryBefore = await directorySnapshot(sourceDirectory);
      expect(Object.keys(sourceDirectoryBefore).sort()).toEqual([
        "source.sqlite",
        "source.sqlite-shm",
        "source.sqlite-wal",
      ]);

      const report = await runOfflineMaintenance(
        maintenanceRequest(sourcePath, stagedPath),
      );

      expect(await directorySnapshot(sourceDirectory)).toEqual(
        sourceDirectoryBefore,
      );
      expect(report.sourceDatabaseSha256).toBe(
        (await fileSnapshot(sourcePath)).sha256,
      );
      const staged = new DatabaseSync(stagedPath, { readOnly: true });
      expect(
        staged
          .prepare("SELECT value FROM settings WHERE key = 'wal.only.setting'")
          .get(),
      ).toEqual({ value: '{"retained":true}' });
      staged.close();
    } finally {
      source.close();
    }
  });

  it("accepts an empty WAL left by a truncate checkpoint", async () => {
    const directory = await temporaryDirectory("cmclient-offline-empty-wal-");
    const sourceDirectory = join(directory, "source");
    const targetDirectory = join(directory, "target");
    await Promise.all([mkdir(sourceDirectory), mkdir(targetDirectory)]);
    const sourcePath = join(sourceDirectory, "source.sqlite");
    const stagedPath = join(targetDirectory, "staged.sqlite");
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    try {
      source.connection.prepare("PRAGMA wal_checkpoint(TRUNCATE)").get();
      const sourceDirectoryBefore = await directorySnapshot(sourceDirectory);
      expect(await stat(`${sourcePath}-wal`)).toMatchObject({ size: 0 });

      const report = await runOfflineMaintenance(
        maintenanceRequest(sourcePath, stagedPath),
      );

      expect(report.schemaHistory).toHaveLength(gatewayMigrations.length);
      expect(await directorySnapshot(sourceDirectory)).toEqual(
        sourceDirectoryBefore,
      );
      expect(await stat(stagedPath)).toMatchObject({
        size: report.stagedDatabaseBytes,
      });
    } finally {
      source.close();
    }
  });

  it("rejects foreign-key violations before publishing a staged database", async () => {
    const directory = await temporaryDirectory("cmclient-offline-fk-");
    const { sourcePath, stagedPath, targetDirectory } =
      await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.connection.exec("PRAGMA foreign_keys = OFF");
    source.connection
      .prepare(
        "INSERT INTO messages (id, observation_id, mesh_network_id, sender, text, observed_at) VALUES ('orphan', 'missing', 'mesh', 42, 'fixture', '2026-07-24T00:00:00.000Z')",
      )
      .run();
    source.close();

    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath)),
    ).rejects.toMatchObject({ code: "DATABASE_FOREIGN_KEY_CHECK_FAILED" });
    await expect(stat(stagedPath)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await readdir(targetDirectory)).toEqual([]);
  });

  it("rejects an integrity failure before publishing a staged database", async () => {
    const directory = await temporaryDirectory("cmclient-offline-integrity-");
    const { sourcePath, stagedPath } = await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    try {
      source.settings.set("orphaned", true);
      disableDefensiveModeForCorruptionFixture(source.connection);
      source.connection.exec("PRAGMA writable_schema = ON");
      source.connection.exec(
        "DELETE FROM sqlite_schema WHERE name = 'settings'",
      );
      source.connection.exec("PRAGMA writable_schema = OFF");
    } finally {
      source.close();
    }

    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath)),
    ).rejects.toMatchObject({ code: "DATABASE_INTEGRITY_CHECK_FAILED" });
    await expect(stat(stagedPath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("rejects an unrecognized user table as schema-history drift", async () => {
    const directory = await temporaryDirectory("cmclient-offline-extra-table-");
    const { sourcePath, stagedPath } = await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.connection.exec(
      "CREATE TABLE unrecognized_legacy_data (id INTEGER)",
    );
    source.close();

    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath)),
    ).rejects.toMatchObject({ code: "DATABASE_SCHEMA_HISTORY_DRIFT" });
    await expect(stat(stagedPath)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("accepts a truly empty v0 database and reports the compiled target schema", async () => {
    const directory = await temporaryDirectory("cmclient-offline-v0-");
    const { sourcePath, stagedPath, targetDirectory } =
      await separatedDatabasePaths(directory);
    const empty = new DatabaseSync(sourcePath);
    empty.exec("VACUUM");
    empty.close();

    const report = await runOfflineMaintenance(
      maintenanceRequest(sourcePath, stagedPath),
    );

    expect(report.schemaHistory).toHaveLength(gatewayMigrations.length);
    expect(
      Object.values(report.domainCounts).every((count) => count === 0),
    ).toBe(true);
    expect(await stat(stagedPath)).toMatchObject({
      size: report.stagedDatabaseBytes,
    });
    expect(await readdir(targetDirectory)).toEqual(["staged.sqlite"]);
  });

  it("rejects a source commit that occurs after the backup snapshot", async () => {
    const directory = await temporaryDirectory("cmclient-offline-source-race-");
    const { sourcePath, stagedPath, targetDirectory } =
      await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.close();
    let lateWriter: DatabaseSync | undefined;
    maintenanceTestState.afterBackup = () => {
      lateWriter = new DatabaseSync(sourcePath);
      lateWriter.exec("PRAGMA busy_timeout = 0");
      lateWriter
        .prepare("INSERT INTO settings (key, value) VALUES ('late', 'commit')")
        .run();
    };

    try {
      await expect(
        runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath)),
      ).rejects.toMatchObject({
        code: "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_CHANGED",
      });
    } finally {
      lateWriter?.close();
    }
    await expect(stat(stagedPath)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await readdir(targetDirectory)).toEqual([]);
  }, 20_000);

  it("preserves the primary source-changed error when owned cleanup also fails", async () => {
    const directory = await temporaryDirectory(
      "cmclient-offline-primary-before-cleanup-",
    );
    const { sourcePath, stagedPath } = await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.close();
    const foreignWorkFile = join(
      maintenanceWorkDirectoryPath(stagedPath),
      "foreign.txt",
    );
    let lateWriter: DatabaseSync | undefined;
    maintenanceTestState.afterBackup = async () => {
      await writeFile(foreignWorkFile, "do-not-delete", "utf8");
      lateWriter = new DatabaseSync(sourcePath);
      lateWriter
        .prepare("INSERT INTO settings (key, value) VALUES ('late', 'commit')")
        .run();
    };

    try {
      await expect(
        runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath)),
      ).rejects.toMatchObject({
        code: "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_CHANGED",
      });
    } finally {
      lateWriter?.close();
    }
    expect(await readFile(foreignWorkFile, "utf8")).toBe("do-not-delete");
    await expect(stat(stagedPath)).rejects.toMatchObject({ code: "ENOENT" });
  }, 20_000);

  it("reports a stable cleanup failure after a successful publish", async () => {
    const directory = await temporaryDirectory(
      "cmclient-offline-publish-cleanup-failure-",
    );
    const { sourcePath, stagedPath } = await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.close();
    const foreignWorkFile = join(
      maintenanceWorkDirectoryPath(stagedPath),
      "foreign.txt",
    );
    maintenanceTestState.afterBackup = async () => {
      await writeFile(foreignWorkFile, "do-not-delete", "utf8");
    };

    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath)),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_CLEANUP_FAILED",
    });
    expect(await readFile(foreignWorkFile, "utf8")).toBe("do-not-delete");
    expect((await stat(stagedPath)).isFile()).toBe(true);
  });

  it("rejects a byte-identical source replacement after the backup snapshot", async () => {
    const directory = await temporaryDirectory(
      "cmclient-offline-source-replaced-",
    );
    const { sourceDirectory, sourcePath, stagedPath, targetDirectory } =
      await separatedDatabasePaths(directory);
    const replacementPath = join(sourceDirectory, "replacement.sqlite");
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.close();
    const sourceBytes = await readFile(sourcePath);
    maintenanceTestState.afterBackup = async () => {
      await writeFile(replacementPath, sourceBytes);
      await rm(sourcePath);
      await rename(replacementPath, sourcePath);
    };

    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath)),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_CHANGED",
    });
    await expect(stat(stagedPath)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await readdir(targetDirectory)).toEqual([]);
  });

  it("rejects same-path, existing staged, hard-linked source, and linked parent paths", async () => {
    const directory = await temporaryDirectory("cmclient-offline-paths-");
    const { sourceDirectory, sourcePath, targetDirectory } =
      await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.close();

    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, sourcePath)),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_PATH_CONFLICT",
    });

    for (const sourceFamilyMember of [
      `${sourcePath}-wal`,
      `${sourcePath}-shm`,
      `${sourcePath}-journal`,
    ]) {
      await expect(
        runOfflineMaintenance(
          maintenanceRequest(sourcePath, sourceFamilyMember),
        ),
      ).rejects.toMatchObject({
        code: "GATEWAY_OFFLINE_MAINTENANCE_PATH_CONFLICT",
      });
      await expect(stat(sourceFamilyMember)).rejects.toMatchObject({
        code: "ENOENT",
      });
    }

    const existingPath = join(targetDirectory, "existing.sqlite");
    await writeFile(existingPath, "occupied", "utf8");
    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, existingPath)),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_STAGED_CONFLICT",
    });

    const hardLinkPath = join(sourceDirectory, "source-hardlink.sqlite");
    await link(sourcePath, hardLinkPath);
    await expect(
      runOfflineMaintenance(
        maintenanceRequest(
          hardLinkPath,
          join(targetDirectory, "hardlink-stage.sqlite"),
        ),
      ),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_INVALID",
    });
    await rm(hardLinkPath);

    const walBackingPath = join(sourceDirectory, "wal-backing");
    const walPath = `${sourcePath}-wal`;
    await writeFile(walBackingPath, "not-a-safe-wal", "utf8");
    await link(walBackingPath, walPath);
    await expect(
      runOfflineMaintenance(
        maintenanceRequest(
          sourcePath,
          join(targetDirectory, "wal-stage.sqlite"),
        ),
      ),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_INVALID",
    });
    await Promise.all([rm(walPath), rm(walBackingPath)]);

    await writeFile(`${sourcePath}-journal`, "rollback-journal", "utf8");
    await expect(
      runOfflineMaintenance(
        maintenanceRequest(
          sourcePath,
          join(targetDirectory, "journal-stage.sqlite"),
        ),
      ),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_INVALID",
    });
    await rm(`${sourcePath}-journal`);

    for (const suffix of ["-wal", "-shm", "-journal"]) {
      const sidecarStagedPath = join(
        targetDirectory,
        `sidecar${suffix}.sqlite`,
      );
      const existingSidecarPath = `${sidecarStagedPath}${suffix}`;
      await writeFile(existingSidecarPath, `existing${suffix}`, "utf8");
      await expect(
        runOfflineMaintenance(
          maintenanceRequest(sourcePath, sidecarStagedPath),
        ),
      ).rejects.toMatchObject({
        code: "GATEWAY_OFFLINE_MAINTENANCE_STAGED_CONFLICT",
      });
      expect(await readFile(existingSidecarPath, "utf8")).toBe(
        `existing${suffix}`,
      );
      await rm(existingSidecarPath);
    }

    const sameParentStage = join(sourceDirectory, "same-parent.sqlite");
    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, sameParentStage)),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_PATH_CONFLICT",
    });
    await expect(stat(sameParentStage)).rejects.toMatchObject({
      code: "ENOENT",
    });

    const descendantTarget = join(sourceDirectory, "nested-target");
    await mkdir(descendantTarget);
    await expect(
      runOfflineMaintenance(
        maintenanceRequest(
          sourcePath,
          join(descendantTarget, "descendant-stage.sqlite"),
        ),
      ),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_PATH_CONFLICT",
    });
    expect(await readdir(descendantTarget)).toEqual([]);

    await expect(
      runOfflineMaintenance(
        maintenanceRequest(
          sourcePath,
          join(directory, "ancestor-stage.sqlite"),
        ),
      ),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_PATH_CONFLICT",
    });
    await expect(
      stat(join(directory, "ancestor-stage.sqlite")),
    ).rejects.toMatchObject({
      code: "ENOENT",
    });

    const realParent = join(directory, "real-parent");
    const linkedParent = join(directory, "linked-parent");
    await mkdir(realParent);
    await symlink(
      realParent,
      linkedParent,
      process.platform === "win32" ? "junction" : "dir",
    );
    await expect(
      runOfflineMaintenance(
        maintenanceRequest(
          sourcePath,
          join(linkedParent, "new/child/staged.sqlite"),
        ),
      ),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
    });
    expect(await readdir(realParent)).toEqual([]);
  });

  it("rejects a Windows namespace alias of the source parent by identity", async () => {
    if (process.platform !== "win32") {
      return;
    }
    const directory = await temporaryDirectory(
      "cmclient-offline-parent-alias-",
    );
    const { sourceDirectory, sourcePath } =
      await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.close();
    const sourceBefore = await directorySnapshot(sourceDirectory);
    const namespacedStage = `\\\\?\\${join(
      sourceDirectory,
      "alias-stage.sqlite",
    )}`;

    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, namespacedStage)),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_PATH_CONFLICT",
    });
    expect(await directorySnapshot(sourceDirectory)).toEqual(sourceBefore);
  });

  it("removes only its owned work directory and preserves target siblings", async () => {
    const directory = await temporaryDirectory("cmclient-offline-owned-work-");
    const { sourcePath, stagedPath, targetDirectory } =
      await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.settings.set("retained", true);
    source.close();

    const siblingDirectory = join(targetDirectory, "preexisting-work");
    const siblingFile = join(siblingDirectory, "sentinel.txt");
    await mkdir(siblingDirectory);
    await writeFile(siblingFile, "preserve", "utf8");

    await runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath));

    expect((await readdir(targetDirectory)).sort()).toEqual([
      "preexisting-work",
      "staged.sqlite",
    ]);
    expect(await readFile(siblingFile, "utf8")).toBe("preserve");
  });

  it("reconciles a bounded prior-crash work directory before retrying", async () => {
    const directory = await temporaryDirectory("cmclient-offline-stale-work-");
    const { sourcePath, stagedPath, targetDirectory } =
      await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.close();

    const staleWork = maintenanceWorkDirectoryPath(stagedPath);
    await mkdir(staleWork);
    await Promise.all([
      writeFile(join(staleWork, "source.sqlite"), "partial-source", "utf8"),
      writeFile(join(staleWork, "source.sqlite-wal"), Buffer.alloc(0)),
      writeFile(join(staleWork, "staged.sqlite-journal"), "partial", "utf8"),
    ]);

    await runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath));

    expect(await readdir(targetDirectory)).toEqual(["staged.sqlite"]);
  });

  it("fails closed without deleting an unsafe preexisting reserved work directory", async () => {
    const directory = await temporaryDirectory(
      "cmclient-offline-unsafe-stale-work-",
    );
    const { sourcePath, stagedPath, targetDirectory } =
      await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.close();

    const unsafeWork = maintenanceWorkDirectoryPath(stagedPath);
    const sentinelPath = join(unsafeWork, "preexisting.txt");
    await mkdir(unsafeWork);
    await writeFile(sentinelPath, "do-not-delete", "utf8");

    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath)),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
    });
    expect(await readFile(sentinelPath, "utf8")).toBe("do-not-delete");
    expect(await readdir(targetDirectory)).toEqual([basename(unsafeWork)]);
  });

  it("preserves a hard-linked file in the reserved work directory", async () => {
    const directory = await temporaryDirectory(
      "cmclient-offline-hardlinked-stale-work-",
    );
    const { sourcePath, stagedPath, targetDirectory } =
      await separatedDatabasePaths(directory);
    const source = new GatewayDatabase(
      sourcePath,
      gatewayMigrations.filter((migration) => migration.version <= 7),
    );
    source.close();

    const unsafeWork = maintenanceWorkDirectoryPath(stagedPath);
    const backingPath = join(targetDirectory, "preexisting.bin");
    const hardLinkPath = join(unsafeWork, "source.sqlite");
    await mkdir(unsafeWork);
    await writeFile(backingPath, "do-not-delete", "utf8");
    await link(backingPath, hardLinkPath);

    await expect(
      runOfflineMaintenance(maintenanceRequest(sourcePath, stagedPath)),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
    });
    expect(await readFile(backingPath, "utf8")).toBe("do-not-delete");
    expect(await readFile(hardLinkPath, "utf8")).toBe("do-not-delete");
  });

  it("accepts only the exact bounded stdin JSON contract", async () => {
    const directory = await temporaryDirectory("cmclient-offline-input-");
    const sourcePath = join(directory, "source.sqlite");
    const stagedPath = join(directory, "staged.sqlite");
    const request = maintenanceRequest(sourcePath, stagedPath);

    await expect(
      readOfflineMaintenanceRequest(Readable.from([JSON.stringify(request)])),
    ).resolves.toEqual(request);
    expect(() =>
      validateOfflineMaintenanceRequest({ ...request, unexpected: true }),
    ).toThrowError(
      expect.objectContaining({
        code: "GATEWAY_OFFLINE_MAINTENANCE_INPUT_INVALID",
      }),
    );
    expect(() =>
      validateOfflineMaintenanceRequest({
        ...request,
        sourceDatabasePath: "relative.sqlite",
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "GATEWAY_OFFLINE_MAINTENANCE_INPUT_INVALID",
      }),
    );
    await expect(
      readOfflineMaintenanceRequest(
        Readable.from([Buffer.alloc(OFFLINE_MAINTENANCE_INPUT_MAX_BYTES + 1)]),
      ),
    ).rejects.toMatchObject({
      code: "GATEWAY_OFFLINE_MAINTENANCE_INPUT_OVERSIZED",
    });
  });

  it("maps retryable maintenance to exit 75 and permanent failure to exit 1", () => {
    expect(
      offlineMaintenanceExitCode(new OfflineMaintenanceRetryableError()),
    ).toBe(75);
    expect(
      offlineMaintenanceExitCode(
        new OfflineMaintenanceError("GATEWAY_OFFLINE_MAINTENANCE_FAILED"),
      ),
    ).toBe(1);
  });
});

function maintenanceRequest(
  sourceDatabasePath: string,
  stagedDatabasePath: string,
): OfflineMaintenanceRequest {
  return {
    schemaVersion: 1,
    type: "gateway.offline-maintenance",
    sourceDatabasePath,
    stagedDatabasePath,
  };
}

async function separatedDatabasePaths(directory: string): Promise<{
  sourceDirectory: string;
  sourcePath: string;
  stagedPath: string;
  targetDirectory: string;
}> {
  const sourceDirectory = join(directory, "source");
  const targetDirectory = join(directory, "target");
  await Promise.all([mkdir(sourceDirectory), mkdir(targetDirectory)]);
  return {
    sourceDirectory,
    sourcePath: join(sourceDirectory, "source.sqlite"),
    stagedPath: join(targetDirectory, "staged.sqlite"),
    targetDirectory,
  };
}

function maintenanceWorkDirectoryPath(stagedPath: string): string {
  const stagedName = basename(stagedPath);
  const hiddenPrefix = stagedName.startsWith(".") ? "" : ".";
  return join(
    dirname(stagedPath),
    `${hiddenPrefix}${stagedName}.maintenance-work`,
  );
}

async function temporaryDirectory(prefix: string): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), prefix));
  temporaryDirectories.push(directory);
  return directory;
}

function disableDefensiveModeForCorruptionFixture(
  database: DatabaseSync,
): void {
  const enableDefensive = Reflect.get(database, "enableDefensive");
  if (typeof enableDefensive === "function") {
    Reflect.apply(enableDefensive, database, [false]);
  }
}

async function fileSnapshot(path: string): Promise<{
  bytes: number;
  sha256: string;
}> {
  const contents = await readFile(path);
  return {
    bytes: contents.length,
    sha256: createHash("sha256").update(contents).digest("hex"),
  };
}

async function directorySnapshot(
  directory: string,
): Promise<Record<string, string>> {
  const names = (await readdir(directory)).sort();
  return Object.fromEntries(
    await Promise.all(
      names.map(async (name) => [
        name,
        (await readFile(join(directory, name))).toString("base64"),
      ]),
    ),
  );
}
