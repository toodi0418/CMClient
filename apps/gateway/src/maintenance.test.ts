import { describe, expect, it } from "vitest";

import { DomainEventBus } from "./events";
import { GatewayMaintenanceRuntime } from "./maintenance";
import { createMeshObservation } from "./observations";
import { GatewayDatabase } from "./persistence/database";

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
      },
      {
        cutoff: "2026-06-18T00:00:00.000Z",
        deleted: 1,
        batchSize: 1,
      },
    ]);
    database.close();
  });
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
