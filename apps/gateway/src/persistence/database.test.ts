import { existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  createSqlMigration,
  DatabaseMigrationError,
  GatewayDatabase,
  gatewayMigrations,
  MigrationManifestError,
  runMigrations,
} from "./database";
import { createMeshObservation } from "../observations";
import {
  PositionHighWaterStore,
  createCanonicalPositionEvent,
} from "../position";
import { AprsOutboxWorker } from "../aprs-outbox";

const PROVISION_FINGERPRINT = "a".repeat(64);

function databasePath(name: string): string {
  return join(tmpdir(), `cmclient-gateway-${process.pid}-${name}.sqlite`);
}

describe("GatewayDatabase", () => {
  it("enables WAL, journals migrations, persists typed settings, and checks integrity", () => {
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
    expect(database.integrityCheck()).toBe("ok");
    expect(database.checkpoint()).toMatchObject({
      busy: 0,
      checkpointedFrames: expect.any(Number),
      logFrames: expect.any(Number),
    });
    expect(
      database.connection
        .prepare("SELECT version FROM schema_migrations")
        .all(),
    ).toHaveLength(18);
    for (const [table, primaryKey] of [
      [
        "node_position_state",
        ["mesh_network_id", "node_num", "callsign", "mapping_version"],
      ],
      [
        "aprs_remote_high_water",
        ["mesh_network_id", "node_num", "callsign", "mapping_version"],
      ],
      ["aprs_delivery_high_water", ["mesh_network_id", "node_num", "callsign"]],
      ["callmesh_sync_state", ["id"]],
      ["callmesh_sync_history", ["mapping_hash"]],
    ] as const) {
      expect(
        database.connection
          .prepare(`PRAGMA table_info(${table})`)
          .all()
          .filter((row) => Number(row.pk) > 0)
          .sort((left, right) => Number(left.pk) - Number(right.pk))
          .map((row) => String(row.name)),
      ).toEqual(primaryKey);
    }
    expect(
      database.connection
        .prepare(
          "EXPLAIN QUERY PLAN DELETE FROM telemetry WHERE id IN (SELECT id FROM telemetry WHERE observed_at < ? ORDER BY observed_at ASC, id ASC LIMIT ?)",
        )
        .all("2026-01-01T00:00:00.000Z", 100)
        .map((row) => String(row.detail)),
    ).toEqual(
      expect.arrayContaining([
        expect.stringContaining("telemetry_observed_at_index"),
      ]),
    );
    for (const [sql, index] of [
      [
        "SELECT id FROM messages INDEXED BY messages_retention_index WHERE observed_at < ? ORDER BY observed_at ASC, id ASC LIMIT ?",
        "messages_retention_index",
      ],
      [
        "SELECT id FROM position_decisions INDEXED BY position_decisions_retention_index WHERE decided_at < ? ORDER BY decided_at ASC, id ASC LIMIT ?",
        "position_decisions_retention_index",
      ],
      [
        "SELECT id FROM position_events INDEXED BY position_events_retention_index WHERE created_at < ? ORDER BY created_at ASC, id ASC LIMIT ?",
        "position_events_retention_index",
      ],
      [
        "SELECT id FROM position_observations INDEXED BY position_observations_retention_index WHERE ingested_at < ? ORDER BY ingested_at ASC, id ASC LIMIT ?",
        "position_observations_retention_index",
      ],
    ] as const) {
      const plan = database.connection
        .prepare(`EXPLAIN QUERY PLAN ${sql}`)
        .all("2026-01-01T00:00:00.000Z", 100)
        .map((row) => String(row.detail));
      expect(plan).toEqual(
        expect.arrayContaining([expect.stringContaining(index)]),
      );
      expect(plan.join("\n")).not.toContain("USE TEMP B-TREE");
    }
    for (const [sql, index] of [
      [
        "SELECT * FROM nodes ORDER BY last_seen_at DESC, node_num ASC LIMIT ?",
        "nodes_recent_projection_index",
      ],
      [
        "SELECT * FROM messages ORDER BY observed_at DESC, id ASC LIMIT ?",
        "messages_recent_projection_index",
      ],
      [
        "SELECT * FROM telemetry ORDER BY observed_at DESC, id ASC LIMIT ?",
        "telemetry_recent_projection_index",
      ],
      [
        "SELECT * FROM position_events ORDER BY COALESCE(event_time, created_at) DESC, id ASC LIMIT ?",
        "position_events_recent_projection_index",
      ],
      [
        "SELECT * FROM aprs_outbox ORDER BY updated_at DESC, id ASC LIMIT ?",
        "aprs_outbox_recent_projection_index",
      ],
    ] as const) {
      const plan = database.connection
        .prepare(`EXPLAIN QUERY PLAN ${sql}`)
        .all(100)
        .map((row) => String(row.detail));
      expect(plan).toEqual(
        expect.arrayContaining([expect.stringContaining(index)]),
      );
      expect(plan.join("\n")).not.toContain("USE TEMP B-TREE");
    }
    const duePlan = database.connection
      .prepare(
        "EXPLAIN QUERY PLAN SELECT id FROM aprs_outbox INDEXED BY aprs_outbox_due_order_index WHERE status IN ('queued', 'failed') AND next_attempt_at <= ? ORDER BY next_attempt_at ASC, created_at ASC, id ASC LIMIT ?",
      )
      .all("2026-01-01T00:00:00.000Z", 100)
      .map((row) => String(row.detail));
    expect(duePlan).toEqual(
      expect.arrayContaining([
        expect.stringContaining("aprs_outbox_due_order_index"),
      ]),
    );
    expect(duePlan.join("\n")).not.toContain("USE TEMP B-TREE");
    const sentRetentionPlan = database.connection
      .prepare(
        "EXPLAIN QUERY PLAN DELETE FROM aprs_outbox WHERE id IN (SELECT outbox.id FROM aprs_outbox AS outbox INDEXED BY aprs_outbox_sent_retention_index WHERE outbox.status = 'sent' AND outbox.sent_at IS NOT NULL AND outbox.sent_at < ? AND EXISTS (SELECT 1 FROM aprs_delivery_high_water AS delivery WHERE delivery.mesh_network_id = outbox.mesh_network_id AND delivery.node_num = outbox.node_num AND delivery.callsign = outbox.callsign AND outbox.event_time IS NOT NULL AND (delivery.latest_canonical_event_id = outbox.canonical_event_id OR delivery.latest_event_time > outbox.event_time OR (delivery.latest_event_time = outbox.event_time AND delivery.latest_mapping_version IS outbox.mapping_version AND delivery.latest_sequence_epoch IS NOT NULL AND delivery.latest_sequence_number IS NOT NULL AND outbox.sequence_epoch IS NOT NULL AND outbox.sequence_number IS NOT NULL AND (delivery.latest_sequence_epoch > outbox.sequence_epoch OR (delivery.latest_sequence_epoch = outbox.sequence_epoch AND delivery.latest_sequence_number > outbox.sequence_number))))) ORDER BY outbox.sent_at ASC, outbox.id ASC LIMIT ?)",
      )
      .all("2026-01-01T00:00:00.000Z", 100)
      .map((row) => String(row.detail));
    expect(sentRetentionPlan).toEqual(
      expect.arrayContaining([
        expect.stringContaining("aprs_outbox_sent_retention_index"),
      ]),
    );
    expect(sentRetentionPlan.join("\n")).not.toContain("USE TEMP B-TREE");
    expect(
      database.connection
        .prepare(
          "EXPLAIN QUERY PLAN DELETE FROM jobs WHERE id IN (SELECT id FROM jobs INDEXED BY jobs_terminal_retention_index WHERE status IN ('succeeded', 'failed', 'cancelled', 'rolled_back') AND completed_at IS NOT NULL AND completed_at < ? ORDER BY completed_at ASC, id ASC LIMIT ?)",
        )
        .all("2026-01-01T00:00:00.000Z", 100)
        .map((row) => String(row.detail)),
    ).toEqual(
      expect.arrayContaining([
        expect.stringContaining("jobs_terminal_retention_index"),
      ]),
    );
    expect(
      database.connection
        .prepare(
          "EXPLAIN QUERY PLAN SELECT * FROM jobs INDEXED BY jobs_queued_created_at_index WHERE status = 'queued' ORDER BY created_at ASC, id ASC LIMIT ?",
        )
        .all(100)
        .map((row) => String(row.detail)),
    ).toEqual(
      expect.arrayContaining([
        expect.stringContaining("jobs_queued_created_at_index"),
      ]),
    );
    expect(
      database.connection
        .prepare(
          "EXPLAIN QUERY PLAN SELECT * FROM jobs INDEXED BY jobs_queued_type_created_at_index WHERE status = 'queued' AND type IN (?, ?) ORDER BY created_at ASC, id ASC LIMIT ?",
        )
        .all("backup.create", "diagnostics.integrity_check", 100)
        .map((row) => String(row.detail)),
    ).toEqual(
      expect.arrayContaining([
        expect.stringContaining("jobs_queued_type_created_at_index"),
      ]),
    );
    expect(
      database.meshObservations.deleteUnreferencedBefore(
        "2026-01-01T00:00:00.000Z",
        40_000,
      ),
    ).toBe(0);
    expect(() =>
      database.meshObservations.deleteUnreferencedBefore(
        "2026-01-01T00:00:00.000Z",
        40_001,
      ),
    ).toThrowError(
      expect.objectContaining({ code: "MESH_OBSERVATION_PERSISTENCE_FAILED" }),
    );
    database.close();
    expect(existsSync(path)).toBe(true);
    rmSync(path, { force: true });
  });

  it("persists setup generations and atomically fences job transitions", () => {
    const database = new GatewayDatabase(":memory:");
    const first = database.jobs.create({
      id: "generation-one",
      type: "diagnostics.noop",
      input: {},
      setupGeneration: 1,
      idempotencyKey: "same-operation",
      now: "2026-07-18T00:00:00.000Z",
    });
    const replayed = database.jobs.create({
      id: "generation-one-replay",
      type: "diagnostics.noop",
      input: {},
      setupGeneration: 1,
      idempotencyKey: "same-operation",
      now: "2026-07-18T00:00:01.000Z",
    });
    const nextGeneration = database.jobs.create({
      id: "generation-two",
      type: "diagnostics.noop",
      input: {},
      setupGeneration: 2,
      idempotencyKey: "same-operation",
      now: "2026-07-18T00:00:02.000Z",
    });

    expect(first.created).toBe(true);
    expect(replayed).toMatchObject({
      created: false,
      job: { id: first.job.id, setupGeneration: 1 },
    });
    expect(nextGeneration).toMatchObject({
      created: true,
      job: { setupGeneration: 2 },
    });
    expect(
      database.jobs.transition(
        first.job.id,
        ["queued"],
        "running",
        "2026-07-18T00:00:03.000Z",
        { startedAt: "2026-07-18T00:00:03.000Z" },
        1,
      ),
    ).toMatchObject({ status: "running" });
    expect(
      database.jobs.transition(
        first.job.id,
        ["queued"],
        "failed",
        "2026-07-18T00:00:04.000Z",
        {},
        1,
      ),
    ).toBeUndefined();
    expect(
      database.jobs.transition(
        first.job.id,
        ["running"],
        "succeeded",
        "2026-07-18T00:00:05.000Z",
        {},
        2,
      ),
    ).toBeUndefined();
    expect(database.jobs.find(first.job.id)).toMatchObject({
      status: "running",
      setupGeneration: 1,
    });
    database.close();
  });

  it("migrates existing jobs into setup generation one", () => {
    const database = new GatewayDatabase(
      ":memory:",
      gatewayMigrations.filter((migration) => migration.version <= 15),
    );
    database.connection
      .prepare(
        "INSERT INTO jobs (id, type, status, input, idempotency_key, created_at, updated_at) VALUES ('legacy-job', 'diagnostics.noop', 'queued', '{}', 'legacy-operation', '2026-07-18T00:00:00.000Z', '2026-07-18T00:00:00.000Z')",
      )
      .run();

    runMigrations(database.connection, gatewayMigrations);

    expect(database.jobs.find("legacy-job")).toMatchObject({
      setupGeneration: 1,
      idempotencyKey: "legacy-operation",
    });
    expect(
      database.jobs.create({
        id: "next-generation-job",
        type: "diagnostics.noop",
        input: {},
        setupGeneration: 2,
        idempotencyKey: "legacy-operation",
        now: "2026-07-18T00:00:01.000Z",
      }),
    ).toMatchObject({ created: true, job: { setupGeneration: 2 } });
    database.close();
  });

  it("rejects duplicate migration versions before applying work", () => {
    const database = new GatewayDatabase(":memory:");
    expect(() =>
      runMigrations(database.connection, [
        createSqlMigration(1, "one", "SELECT 1;"),
        createSqlMigration(1, "two", "SELECT 2;"),
      ]),
    ).toThrow(MigrationManifestError);
    database.close();
  });

  it("migrates a populated v11 database through delivery and outbox snapshots", () => {
    const database = new GatewayDatabase(
      ":memory:",
      gatewayMigrations.filter((migration) => migration.version <= 11),
    );
    const first = insertMigrationPosition(
      database,
      "migration-first",
      1_784_332_800,
      10,
      0,
    );
    const second = insertMigrationPosition(
      database,
      "migration-second",
      1_784_332_800,
      11,
      0,
    );
    const queued = insertMigrationPosition(
      database,
      "migration-queued",
      1_784_332_801,
      1,
      1,
    );
    const insertOutbox = database.connection.prepare(
      "INSERT INTO aprs_outbox (id, callsign, canonical_event_id, data, status, attempts, next_attempt_at, created_at, updated_at, sent_at) VALUES (?, 'N0CALL-7', ?, ?, ?, 0, ?, ?, ?, ?)",
    );
    insertOutbox.run(
      "outbox-first",
      first.id,
      "N0CALL-7>APCM20:first",
      "sent",
      "2026-07-18T00:00:01.000Z",
      "2026-07-18T00:00:01.000Z",
      "2026-07-18T00:00:01.000Z",
      "2026-07-18T00:00:02.000Z",
    );
    insertOutbox.run(
      "outbox-second",
      second.id,
      "N0CALL-7>APCM20:second",
      "sent",
      "2026-07-18T00:00:03.000Z",
      "2026-07-18T00:00:03.000Z",
      "2026-07-18T00:00:03.000Z",
      "2026-07-18T00:00:04.000Z",
    );
    insertOutbox.run(
      "outbox-queued",
      queued.id,
      "N0CALL-7>APCM20:queued",
      "queued",
      "2026-07-18T00:00:05.000Z",
      "2026-07-18T00:00:05.000Z",
      "2026-07-18T00:00:05.000Z",
      null,
    );

    runMigrations(database.connection, gatewayMigrations);

    expect(
      database.connection
        .prepare("SELECT MAX(version) AS version FROM schema_migrations")
        .get(),
    ).toEqual({ version: 18 });
    expect(
      database.connection
        .prepare("SELECT * FROM aprs_delivery_high_water")
        .get(),
    ).toMatchObject({
      mesh_network_id: "fixture-network",
      node_num: 42,
      callsign: "N0CALL-7",
      latest_canonical_event_id: second.id,
      latest_event_time: "2026-07-18T00:00:00.000Z",
      latest_sequence_epoch: 0,
      latest_sequence_number: 11,
      latest_mapping_version: null,
    });
    expect(
      database.connection
        .prepare(
          "SELECT id, mesh_network_id, node_num, mapping_version, event_time, sequence_epoch, sequence_number, provision_fingerprint FROM aprs_outbox ORDER BY id",
        )
        .all(),
    ).toEqual([
      {
        id: "outbox-first",
        mesh_network_id: "fixture-network",
        node_num: 42,
        mapping_version: null,
        event_time: "2026-07-18T00:00:00.000Z",
        sequence_epoch: 0,
        sequence_number: 10,
        provision_fingerprint: null,
      },
      {
        id: "outbox-queued",
        mesh_network_id: "fixture-network",
        node_num: 42,
        mapping_version: null,
        event_time: "2026-07-18T00:00:01.000Z",
        sequence_epoch: 1,
        sequence_number: 1,
        provision_fingerprint: null,
      },
      {
        id: "outbox-second",
        mesh_network_id: "fixture-network",
        node_num: 42,
        mapping_version: null,
        event_time: "2026-07-18T00:00:00.000Z",
        sequence_epoch: 0,
        sequence_number: 11,
        provision_fingerprint: null,
      },
    ]);
    let reenqueues = 0;
    const replay = new PositionHighWaterStore(database.connection).apply(
      second,
      { callsign: "N0CALL-7", mappingVersion: "mapping-v2" },
      "2026-07-18T00:00:06.000Z",
      { onAccepted: () => void (reenqueues += 1) },
    );
    expect(replay.decision.code).toBe("APRS_SKIPPED_OUT_OF_ORDER");
    expect(reenqueues).toBe(0);
    expect(
      database.aprsOutbox.deleteSentBefore("2027-01-01T00:00:00.000Z", 10),
    ).toBe(1);
    expect(database.aprsOutbox.find("outbox-first")?.status).toBe("sent");
    database.positions.deleteHistoryBefore("2027-01-01T00:00:00.000Z", 100);
    expect(
      database.connection
        .prepare("SELECT id FROM position_events WHERE id = ?")
        .get(second.id),
    ).toEqual({ id: second.id });
    expect(
      database.connection.prepare("PRAGMA foreign_key_check").all(),
    ).toEqual([]);
    expect(database.integrityCheck()).toBe("ok");
    database.close();
  });

  it("migrates v13 databases to a singleton CallMesh sync high-water", () => {
    const database = new GatewayDatabase(
      ":memory:",
      gatewayMigrations.filter((migration) => migration.version <= 13),
    );
    expect(
      database.connection
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'callmesh_sync_state'",
        )
        .get(),
    ).toBeUndefined();

    runMigrations(database.connection, gatewayMigrations);

    expect(
      database.connection
        .prepare("SELECT MAX(version) AS version FROM schema_migrations")
        .get(),
    ).toEqual({ version: 18 });
    const firstFingerprint = "a".repeat(64);
    const secondFingerprint = "b".repeat(64);
    const insertState = database.connection.prepare(
      "INSERT INTO callmesh_sync_state (id, active, mapping_hash, accepted_server_time, mappings_fingerprint, last_heartbeat_at, mapping_synced_at, provision_json, provision_expires_at, provision_fingerprint, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    insertState.run(
      1,
      1,
      "mapping-current",
      "2026-07-18T00:00:00.000Z",
      firstFingerprint,
      "2026-07-18T00:00:01.000Z",
      "2026-07-18T00:00:01.000Z",
      null,
      null,
      null,
      "2026-07-18T00:00:01.000Z",
    );
    expect(() =>
      insertState.run(
        2,
        1,
        "mapping-other",
        "2026-07-18T00:00:02.000Z",
        secondFingerprint,
        "2026-07-18T00:00:02.000Z",
        "2026-07-18T00:00:02.000Z",
        null,
        null,
        null,
        "2026-07-18T00:00:02.000Z",
      ),
    ).toThrow();
    database.connection
      .prepare("UPDATE callmesh_sync_state SET active = 0 WHERE id = 1")
      .run();
    expect(
      database.connection
        .prepare("SELECT active FROM callmesh_sync_state WHERE id = 1")
        .get(),
    ).toEqual({ active: 0 });
    expect(() =>
      database.connection
        .prepare("UPDATE callmesh_sync_state SET active = 2 WHERE id = 1")
        .run(),
    ).toThrow();
    expect(() =>
      database.connection
        .prepare(
          "UPDATE callmesh_sync_state SET provision_json = ?, provision_expires_at = NULL, provision_fingerprint = NULL WHERE id = 1",
        )
        .run('{"callsignBase":"N0CALL"}'),
    ).toThrow();

    const rememberHash = database.connection.prepare(
      "INSERT INTO callmesh_sync_history (mapping_hash, first_server_time, last_server_time, mappings_fingerprint) VALUES (?, ?, ?, ?) ON CONFLICT(mapping_hash) DO UPDATE SET last_server_time = excluded.last_server_time WHERE excluded.last_server_time > callmesh_sync_history.last_server_time",
    );
    rememberHash.run(
      "mapping-current",
      "2026-07-18T00:00:00.000Z",
      "2026-07-18T00:00:00.000Z",
      firstFingerprint,
    );
    rememberHash.run(
      "mapping-current",
      "2026-07-18T00:00:03.000Z",
      "2026-07-18T00:00:03.000Z",
      secondFingerprint,
    );
    rememberHash.run(
      "mapping-current",
      "2026-07-17T23:59:59.000Z",
      "2026-07-18T00:00:02.000Z",
      secondFingerprint,
    );
    expect(
      database.connection.prepare("SELECT * FROM callmesh_sync_history").get(),
    ).toEqual({
      mapping_hash: "mapping-current",
      first_server_time: "2026-07-18T00:00:00.000Z",
      last_server_time: "2026-07-18T00:00:03.000Z",
      mappings_fingerprint: firstFingerprint,
    });
    expect(database.integrityCheck()).toBe("ok");
    database.close();
  });

  it("keeps same-time legacy snapshots fail-closed when mapping cannot be recovered", async () => {
    const database = new GatewayDatabase(
      ":memory:",
      gatewayMigrations.filter((migration) => migration.version <= 11),
    );
    const delivered = insertMigrationPosition(
      database,
      "legacy-delivered",
      1_784_332_800,
      10,
      0,
    );
    const pending = insertMigrationPosition(
      database,
      "legacy-pending",
      1_784_332_800,
      11,
      0,
    );
    const insertOutbox = database.connection.prepare(
      "INSERT INTO aprs_outbox (id, callsign, canonical_event_id, data, status, attempts, next_attempt_at, created_at, updated_at, sent_at) VALUES (?, 'N0CALL-7', ?, ?, ?, 0, ?, ?, ?, ?)",
    );
    insertOutbox.run(
      "legacy-delivered",
      delivered.id,
      "N0CALL-7>APCM20:legacy-delivered",
      "sent",
      "2026-07-18T00:00:00.000Z",
      "2026-07-18T00:00:00.000Z",
      "2026-07-18T00:00:00.000Z",
      "2026-07-18T00:00:01.000Z",
    );
    insertOutbox.run(
      "legacy-pending",
      pending.id,
      "N0CALL-7>APCM20:legacy-pending",
      "queued",
      "2026-07-18T00:00:02.000Z",
      "2026-07-18T00:00:02.000Z",
      "2026-07-18T00:00:02.000Z",
      null,
    );
    runMigrations(database.connection, gatewayMigrations);
    let sends = 0;

    await expect(
      new AprsOutboxWorker(
        database.aprsOutbox,
        {
          send: async () => {
            sends += 1;
          },
        },
        {
          authorizationProvider: () => PROVISION_FINGERPRINT,
          clock: () => new Date("2026-07-18T00:00:02.000Z"),
        },
      ).flush(),
    ).resolves.toEqual([]);
    expect(sends).toBe(0);
    expect(database.aprsOutbox.find("legacy-pending")).toBeUndefined();
    expect(database.aprsOutbox.deleteSuperseded(10)).toBe(0);
    expect(
      database.connection
        .prepare(
          "SELECT latest_canonical_event_id, latest_mapping_version FROM aprs_delivery_high_water",
        )
        .get(),
    ).toEqual({
      latest_canonical_event_id: delivered.id,
      latest_mapping_version: null,
    });
    expect(
      database.aprsOutbox.deleteSentBefore("2027-01-01T00:00:00.000Z", 10),
    ).toBe(1);
    expect(database.integrityCheck()).toBe("ok");
    database.close();
  });

  it("rolls back a failed migration without advancing the journal", () => {
    const database = new GatewayDatabase(":memory:");
    expect(() =>
      runMigrations(database.connection, [
        ...gatewayMigrations,
        createSqlMigration(
          19,
          "broken",
          "CREATE TABLE migration_rollback_probe (id INTEGER); SELECT * FROM definitely_missing_table;",
        ),
      ]),
    ).toThrow(DatabaseMigrationError);
    expect(
      database.connection
        .prepare("SELECT version FROM schema_migrations WHERE version = 19")
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

function insertMigrationPosition(
  database: GatewayDatabase,
  suffix: string,
  positionTimestampSeconds: number,
  sequenceNumber: number,
  sequenceEpoch: number,
) {
  const timestamp = new Date(positionTimestampSeconds * 1_000).toISOString();
  const meshObservation = createMeshObservation({
    id: `mesh-${suffix}`,
    transport: "simulator",
    sessionConnectedAt: timestamp,
    ingestedAt: timestamp,
    serverIngestedAt: timestamp,
    normalizedFromRadio: { schemaVersion: 1, kind: "other" },
  });
  database.meshObservations.insert(meshObservation);
  const observation = database.positions.insertOrFindObservation({
    schemaVersion: 1,
    id: `position-${suffix}`,
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    meshObservationId: meshObservation.id,
    gatewayId: "fixture-gateway",
    transport: "simulator",
    sessionConnectedAt: timestamp,
    ingestedAt: timestamp,
    serverIngestedAt: timestamp,
    backlogClassification: "live",
    payloadHash: suffix.padEnd(64, "0").slice(0, 64),
    position: {
      latitudeI: 250_000_000,
      longitudeI: 1_215_000_000,
      precisionBits: 32,
      positionTimestampSeconds,
      sequenceNumber,
    },
  });
  return database.positions.insertOrFindEvent({
    ...createCanonicalPositionEvent(observation).event,
    sequenceEpoch,
  }).event;
}
