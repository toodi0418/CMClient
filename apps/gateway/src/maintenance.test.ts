import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import { DomainEventBus } from "./events";
import { GatewayMaintenanceRuntime } from "./maintenance";
import { createMeshObservation } from "./observations";
import { GatewayDatabase } from "./persistence/database";
import {
  PositionHighWaterStore,
  PositionRepository,
  createCanonicalPositionEvent,
} from "./position";

const PROVISION_FINGERPRINT = "a".repeat(64);

describe("GatewayMaintenanceRuntime", () => {
  it("removes telemetry incrementally without deleting retained history", () => {
    const database = new GatewayDatabase(":memory:");
    insertTelemetry(database, "old-a", "2026-05-01T00:00:00.000Z");
    insertTelemetry(database, "old-b", "2026-05-02T00:00:00.000Z");
    insertTelemetry(database, "current", "2026-07-17T00:00:00.000Z");
    const events = new DomainEventBus({
      eventIdFactory: () => "maintenance-event",
    });
    const completed: Record<string, unknown>[] = [];
    events.subscribe((event) => completed.push(event.payload));
    const runtime = new GatewayMaintenanceRuntime({
      database,
      eventBus: events,
      retentionDays: 30,
      telemetryBatchSize: 1,
      clock: () => new Date("2026-07-18T00:00:00.000Z"),
    });

    expect(runtime.runCycle()).toBe(1);
    expect(database.meshTelemetry.list(10).map((entry) => entry.id)).toEqual([
      "current",
      "old-b",
    ]);
    expect(runtime.runCycle()).toBe(1);
    expect(database.meshTelemetry.list(10).map((entry) => entry.id)).toEqual([
      "current",
    ]);
    expect(completed).toEqual([
      {
        cutoff: "2026-06-18T00:00:00.000Z",
        deleted: 1,
        batchSize: 1,
        messageCutoff: "2026-06-18T00:00:00.000Z",
        messagesDeleted: 0,
        messageBatchSize: 1_000,
        observationsDeleted: 1,
        observationBatchSize: 3_001,
        jobCutoff: "2026-04-19T00:00:00.000Z",
        terminalJobsDeleted: 0,
        jobBatchSize: 1_000,
        aprsOutboxCutoff: "2026-04-19T00:00:00.000Z",
        sentAprsOutboxDeleted: 0,
        supersededAprsOutboxDeleted: 0,
        aprsOutboxBatchSize: 1_000,
        positionCutoff: "2026-06-18T00:00:00.000Z",
        positionDecisionsDeleted: 0,
        positionEventsDeleted: 0,
        positionObservationsDeleted: 0,
        positionBatchSize: 1_000,
        observationCutoff: "2026-06-18T00:00:00.000Z",
        walCheckpoint: { busy: 0, checkpointedFrames: -1, logFrames: -1 },
      },
      {
        cutoff: "2026-06-18T00:00:00.000Z",
        deleted: 1,
        batchSize: 1,
        messageCutoff: "2026-06-18T00:00:00.000Z",
        messagesDeleted: 0,
        messageBatchSize: 1_000,
        observationsDeleted: 1,
        observationBatchSize: 3_001,
        jobCutoff: "2026-04-19T00:00:00.000Z",
        terminalJobsDeleted: 0,
        jobBatchSize: 1_000,
        aprsOutboxCutoff: "2026-04-19T00:00:00.000Z",
        sentAprsOutboxDeleted: 0,
        supersededAprsOutboxDeleted: 0,
        aprsOutboxBatchSize: 1_000,
        positionCutoff: "2026-06-18T00:00:00.000Z",
        positionDecisionsDeleted: 0,
        positionEventsDeleted: 0,
        positionObservationsDeleted: 0,
        positionBatchSize: 1_000,
        observationCutoff: "2026-06-18T00:00:00.000Z",
        walCheckpoint: { busy: 0, checkpointedFrames: -1, logFrames: -1 },
      },
    ]);
    database.close();
  });

  it("incrementally drains a large expired telemetry and terminal Job backlog", () => {
    const database = new GatewayDatabase(":memory:");
    const expired = 2_049;
    database.connection.exec("BEGIN IMMEDIATE");
    try {
      for (let index = 0; index < expired; index += 1) {
        insertTelemetry(
          database,
          `expired-${index}`,
          "2026-01-01T00:00:00.000Z",
        );
        insertTerminalJob(
          database,
          `expired-job-${index}`,
          "2026-01-01T00:00:00.000Z",
        );
      }
      for (let index = 0; index < 5; index += 1) {
        insertTelemetry(
          database,
          `retained-${index}`,
          "2026-07-17T00:00:00.000Z",
        );
        insertTerminalJob(
          database,
          `retained-job-${index}`,
          "2026-07-17T00:00:00.000Z",
        );
        database.jobs.create({
          id: `queued-job-${index}`,
          type: "load.wait",
          input: {},
          now: "2026-01-01T00:00:00.000Z",
        });
      }
      insertTerminalJob(
        database,
        "idempotent-job",
        "2026-01-01T00:00:00.000Z",
        "retention-stable-key",
      );
      database.connection.exec("COMMIT");
    } catch (error) {
      database.connection.exec("ROLLBACK");
      throw error;
    }
    const events = new DomainEventBus({
      eventIdFactory: (() => {
        let index = 0;
        return () => `retention-${++index}`;
      })(),
    });
    const runtime = new GatewayMaintenanceRuntime({
      database,
      eventBus: events,
      retentionDays: 30,
      telemetryBatchSize: 128,
      jobRetentionDays: 90,
      jobBatchSize: 128,
      clock: () => new Date("2026-07-18T00:00:00.000Z"),
    });

    const cycleDeletes = Array.from({ length: 17 }, () => runtime.runCycle());

    expect(cycleDeletes).toEqual([...Array(16).fill(128), 1]);
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM telemetry")
        .get()?.count,
    ).toBe(5);
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM mesh_observations")
        .get()?.count,
    ).toBe(5);
    expect(
      database.connection.prepare("SELECT COUNT(*) AS count FROM jobs").get()
        ?.count,
    ).toBe(10);
    expect(
      database.jobs.findByIdempotency("load.completed", "retention-stable-key"),
    ).toBeUndefined();
    expect(
      events
        .recent(17)
        .every(
          (event) =>
            Number(event.payload.deleted) <= 128 &&
            Number(event.payload.observationsDeleted) <= 3_128 &&
            event.payload.observationBatchSize === 3_128 &&
            Number(event.payload.terminalJobsDeleted) <= 128 &&
            Number(event.payload.sentAprsOutboxDeleted) <= 1_000,
        ),
    ).toBe(true);
    expect(database.integrityCheck()).toBe("ok");
    database.close();
  }, 20_000);

  it("uses the latest domain cutoff when draining unreferenced observations", () => {
    const database = new GatewayDatabase(":memory:");
    insertOrphanObservation(
      database,
      "older-than-message-policy",
      "2026-05-01T00:00:00.000Z",
    );
    insertOrphanObservation(
      database,
      "within-message-policy",
      "2026-07-01T00:00:00.000Z",
    );
    const events = new DomainEventBus({
      eventIdFactory: () => "asymmetric-retention",
    });
    const runtime = new GatewayMaintenanceRuntime({
      database,
      eventBus: events,
      retentionDays: 90,
      messageRetentionDays: 30,
      positionRetentionDays: 60,
      clock: () => new Date("2026-07-18T00:00:00.000Z"),
    });

    runtime.runCycle();

    expect(
      database.connection
        .prepare("SELECT id FROM mesh_observations ORDER BY id")
        .all(),
    ).toEqual([{ id: "observation-within-message-policy" }]);
    expect(events.recent(1)[0]?.payload).toMatchObject({
      cutoff: "2026-04-19T00:00:00.000Z",
      messageCutoff: "2026-06-18T00:00:00.000Z",
      positionCutoff: "2026-05-19T00:00:00.000Z",
      observationCutoff: "2026-06-18T00:00:00.000Z",
      observationsDeleted: 1,
    });
    database.close();
  });

  it("plateaus domain history and orphan observations across repeated large cycles", () => {
    const database = new GatewayDatabase(":memory:");
    const expiredAt = "2026-01-01T00:00:00.000Z";
    const retainedAt = "2026-07-17T00:00:00.000Z";
    insertMessage(database, "retained-message", retainedAt);
    const stateEvent = insertPositionHistory(
      database,
      "retained-state",
      expiredAt,
      false,
    );
    new PositionHighWaterStore(database.connection).apply(
      stateEvent,
      { callsign: "N0CALL-7", mappingVersion: "mapping-v1" },
      retainedAt,
    );
    const deliveredEvent = insertPositionHistory(
      database,
      "retained-delivery",
      expiredAt,
      false,
    );
    const deliveredResult = database.aprsOutbox.enqueue({
      callsign: "N2CALL-7",
      canonicalEventId: deliveredEvent.id,
      data: "N2CALL-7>APCM20:delivered",
      now: expiredAt,
      provisionFingerprint: PROVISION_FINGERPRINT,
    });
    if (!deliveredResult.entry) {
      throw new Error("fixture delivered outbox was suppressed");
    }
    const deliveredOutbox = deliveredResult.entry;
    database.aprsOutbox.claimDue(expiredAt, 1);
    database.aprsOutbox.markSent(
      deliveredOutbox.id,
      expiredAt,
      PROVISION_FINGERPRINT,
    );
    const activeOutboxEvent = insertPositionHistory(
      database,
      "retained-outbox",
      expiredAt,
      false,
    );
    database.aprsOutbox.enqueue({
      callsign: "N1CALL-7",
      canonicalEventId: activeOutboxEvent.id,
      data: "N1CALL-7>APCM20:retained",
      now: expiredAt,
      provisionFingerprint: PROVISION_FINGERPRINT,
    });
    const events = new DomainEventBus({
      eventIdFactory: (() => {
        let index = 0;
        return () => `plateau-${++index}`;
      })(),
    });
    const cycles = 33;
    const telemetryBatchSize = 17;
    const messageBatchSize = 64;
    const positionBatchSize = 63;
    const orphanHeadroomWork = 5;
    const observationBatchSize =
      telemetryBatchSize + messageBatchSize + positionBatchSize + 1_000;
    const runtime = new GatewayMaintenanceRuntime({
      database,
      eventBus: events,
      retentionDays: 30,
      telemetryBatchSize,
      messageRetentionDays: 30,
      messageBatchSize,
      positionRetentionDays: 30,
      positionBatchSize,
      clock: () => new Date("2026-07-18T00:00:00.000Z"),
    });

    for (let cycle = 0; cycle < cycles; cycle += 1) {
      database.connection.exec("BEGIN IMMEDIATE");
      try {
        for (let index = 0; index < telemetryBatchSize; index += 1) {
          insertTelemetry(
            database,
            `expired-telemetry-${cycle}-${index}`,
            expiredAt,
          );
        }
        for (let index = 0; index < messageBatchSize; index += 1) {
          const suffix = `${cycle}-${index}`;
          insertMessage(database, `expired-message-${suffix}`, expiredAt);
        }
        for (let index = 0; index < positionBatchSize; index += 1) {
          const suffix = `${cycle}-${index}`;
          insertPositionHistory(
            database,
            `expired-position-${suffix}`,
            expiredAt,
          );
        }
        for (let index = 0; index < orphanHeadroomWork; index += 1) {
          insertOrphanObservation(
            database,
            `expired-orphan-${cycle}-${index}`,
            expiredAt,
          );
        }
        database.connection.exec("COMMIT");
      } catch (error) {
        database.connection.exec("ROLLBACK");
        throw error;
      }

      runtime.runCycle();
      expect(tableCounts(database)).toEqual({
        aprsOutbox: 1,
        deliveryHighWater: 1,
        meshObservations: 4,
        messages: 1,
        nodePositionState: 1,
        positionDecisions: 1,
        positionEvents: 3,
        positionObservations: 3,
        telemetry: 0,
      });
    }

    expect(
      events
        .recent(cycles)
        .every(
          (event) =>
            event.payload.deleted === telemetryBatchSize &&
            event.payload.messagesDeleted === messageBatchSize &&
            event.payload.positionDecisionsDeleted === positionBatchSize &&
            event.payload.positionEventsDeleted === positionBatchSize &&
            event.payload.positionObservationsDeleted === positionBatchSize &&
            event.payload.observationsDeleted ===
              telemetryBatchSize +
                messageBatchSize +
                positionBatchSize +
                orphanHeadroomWork &&
            event.payload.observationBatchSize === observationBatchSize,
        ),
    ).toBe(true);
    expect(database.integrityCheck()).toBe("ok");
    database.close();
  }, 20_000);
});

function insertTelemetry(
  database: GatewayDatabase,
  id: string,
  observedAt: string,
): void {
  const observationId = `observation-${id}`;
  database.meshObservations.insert(
    createMeshObservation({
      id: observationId,
      transport: "simulator",
      sessionConnectedAt: observedAt,
      ingestedAt: observedAt,
      serverIngestedAt: observedAt,
      normalizedFromRadio: { schemaVersion: 1, kind: "other" },
    }),
  );
  database.meshTelemetry.insert({
    schemaVersion: 1,
    id,
    observationId,
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    metricKind: "deviceMetrics",
    metrics: { batteryLevel: 73 },
    observedAt,
  });
}

function insertOrphanObservation(
  database: GatewayDatabase,
  id: string,
  observedAt: string,
): void {
  database.meshObservations.insert(
    createMeshObservation({
      id: `observation-${id}`,
      transport: "simulator",
      sessionConnectedAt: observedAt,
      ingestedAt: observedAt,
      serverIngestedAt: observedAt,
      normalizedFromRadio: { schemaVersion: 1, kind: "other" },
    }),
  );
}

function insertTerminalJob(
  database: GatewayDatabase,
  id: string,
  completedAt: string,
  idempotencyKey?: string,
): void {
  database.jobs.create({
    id,
    type: "load.completed",
    input: {},
    now: completedAt,
    ...(idempotencyKey ? { idempotencyKey } : {}),
  });
  database.jobs.transition(id, ["queued"], "succeeded", completedAt, {
    completedAt,
    result: { outcome: "ok" },
  });
}

function insertMessage(
  database: GatewayDatabase,
  id: string,
  observedAt: string,
): void {
  const observationId = `observation-${id}`;
  database.meshObservations.insert(
    createMeshObservation({
      id: observationId,
      transport: "simulator",
      sessionConnectedAt: observedAt,
      ingestedAt: observedAt,
      serverIngestedAt: observedAt,
      normalizedFromRadio: { schemaVersion: 1, kind: "other" },
    }),
  );
  database.meshMessages.insert({
    schemaVersion: 1,
    id,
    observationId,
    meshNetworkId: "fixture-network",
    sender: 42,
    text: `fixture ${id}`,
    observedAt,
  });
}

function insertPositionHistory(
  database: GatewayDatabase,
  id: string,
  observedAt: string,
  withDecision = true,
) {
  const meshObservationId = `mesh-observation-${id}`;
  database.meshObservations.insert(
    createMeshObservation({
      id: meshObservationId,
      transport: "simulator",
      sessionConnectedAt: observedAt,
      ingestedAt: observedAt,
      serverIngestedAt: observedAt,
      normalizedFromRadio: { schemaVersion: 1, kind: "other" },
    }),
  );
  const repository = new PositionRepository(database.connection);
  const observation = repository.insertOrFindObservation({
    schemaVersion: 1,
    id: `position-observation-${id}`,
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    meshObservationId,
    gatewayId: "fixture-gateway",
    transport: "simulator",
    sessionConnectedAt: observedAt,
    ingestedAt: observedAt,
    serverIngestedAt: observedAt,
    backlogClassification: "live",
    payloadHash: createHash("sha256").update(id).digest("hex"),
    position: {
      latitudeI: 250000000,
      longitudeI: 1215000000,
      precisionBits: 32,
      positionTimestampSeconds: 1_767_225_600,
    },
  });
  const event = repository.insertOrFindEvent(
    createCanonicalPositionEvent(observation).event,
  ).event;
  if (withDecision) {
    repository.insertOrFindDecision({
      schemaVersion: 1,
      id: `position-decision-${id}`,
      observationId: observation.id,
      canonicalEventId: event.id,
      code: "POSITION_HISTORICAL",
      decidedAt: observedAt,
      parameters: {},
    });
  }
  return event;
}

function tableCounts(database: GatewayDatabase): Record<string, number> {
  const count = (table: string) =>
    Number(
      database.connection
        .prepare(`SELECT COUNT(*) AS count FROM ${table}`)
        .get()?.count,
    );
  return {
    aprsOutbox: count("aprs_outbox"),
    deliveryHighWater: count("aprs_delivery_high_water"),
    meshObservations: count("mesh_observations"),
    messages: count("messages"),
    nodePositionState: count("node_position_state"),
    positionDecisions: count("position_decisions"),
    positionEvents: count("position_events"),
    positionObservations: count("position_observations"),
    telemetry: count("telemetry"),
  };
}
