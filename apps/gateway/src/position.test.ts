import { describe, expect, it } from "vitest";

import type { PositionObservation } from "@cmclient/contracts";

import { createMeshObservation } from "./observations";
import {
  PositionDuplicateDetector,
  PositionHighWaterStore,
  PositionRepository,
  createCanonicalPositionEvent,
} from "./position";
import { GatewayDatabase } from "./persistence/database";

describe("canonical position identity", () => {
  it("excludes gateway observation metadata but includes payload hash", () => {
    const first = positionObservation("position-observation-a");
    const sameEventDifferentGateway: PositionObservation = {
      ...first,
      id: "position-observation-b",
      meshObservationId: "mesh-observation-b",
      gatewayId: "fixture-gateway-b",
      ingestedAt: "2026-07-18T00:00:07.000Z",
      serverIngestedAt: "2026-07-18T00:00:07.005Z",
      packetId: 1002,
      rxRssi: -95,
    };
    const reusedPacketIdDifferentPayload: PositionObservation = {
      ...sameEventDifferentGateway,
      id: "position-observation-c",
      meshObservationId: "mesh-observation-c",
      payloadHash: "b".repeat(64),
    };

    const firstEvent = createCanonicalPositionEvent(first).event;
    expect(
      createCanonicalPositionEvent(sameEventDifferentGateway).event,
    ).toMatchObject({
      id: firstEvent.id,
      canonicalKey: firstEvent.canonicalKey,
    });
    expect(
      createCanonicalPositionEvent(reusedPacketIdDifferentPayload).event,
    ).not.toMatchObject({
      canonicalKey: firstEvent.canonicalKey,
    });
  });
});

describe("PositionDuplicateDetector", () => {
  it("persists one canonical event and records duplicate decisions against it", () => {
    const database = new GatewayDatabase(":memory:");
    const first = positionObservation("position-observation-a");
    const duplicate: PositionObservation = {
      ...first,
      id: "position-observation-b",
      meshObservationId: "mesh-observation-b",
      gatewayId: "fixture-gateway-b",
      ingestedAt: "2026-07-18T00:00:07.000Z",
      serverIngestedAt: "2026-07-18T00:00:07.005Z",
      packetId: 1002,
    };
    const packetReuse: PositionObservation = {
      ...duplicate,
      id: "position-observation-c",
      meshObservationId: "mesh-observation-c",
      payloadHash: "b".repeat(64),
    };
    for (const observation of [first, duplicate, packetReuse]) {
      database.meshObservations.insert(
        meshObservation(observation.meshObservationId),
      );
    }
    const detector = new PositionDuplicateDetector(
      new PositionRepository(database.connection),
    );

    const firstResult = detector.observe(first);
    const duplicateResult = detector.observe(duplicate);
    const reuseResult = detector.observe(packetReuse);

    expect(firstResult).toMatchObject({
      kind: "new",
      event: { sourceObservationId: first.id },
    });
    expect(duplicateResult).toMatchObject({
      kind: "duplicate",
      event: { sourceObservationId: first.id },
      decision: {
        observationId: duplicate.id,
        code: "POSITION_DUPLICATE",
      },
    });
    expect(reuseResult).toMatchObject({
      kind: "new",
      event: { sourceObservationId: packetReuse.id },
    });
    expect(
      database.connection.prepare("SELECT * FROM position_events").all(),
    ).toHaveLength(2);
    expect(
      database.connection.prepare("SELECT * FROM position_decisions").all(),
    ).toHaveLength(1);
    database.close();
  });
});

describe("PositionHighWaterStore", () => {
  it("advances trusted time, preserves historical events, and opens a sequence epoch after reset", () => {
    const database = new GatewayDatabase(":memory:");
    const observations = [
      positionObservation("position-observation-a"),
      positionObservation("position-observation-b", 101, 1784332801, "b"),
      positionObservation("position-observation-c", 99, 1784332799, "c"),
      positionObservation("position-observation-d", 1, 1784332802, "d"),
    ];
    for (const observation of observations) {
      database.meshObservations.insert(
        meshObservation(observation.meshObservationId),
      );
    }
    const repository = new PositionRepository(database.connection);
    const detector = new PositionDuplicateDetector(repository);
    const events = observations.map((observation) => {
      const result = detector.observe(observation);
      if (result.kind !== "new") {
        throw new Error("fixture position unexpectedly duplicated");
      }
      return result.event;
    });
    const state = new PositionHighWaterStore(database.connection);
    const target = { callsign: "N0CALL-7", mappingVersion: "mapping-v1" };

    const first = state.apply(events[0]!, target, "2026-07-18T00:00:02.000Z");
    const second = state.apply(events[1]!, target, "2026-07-18T00:00:03.000Z");
    const historical = state.apply(
      events[2]!,
      target,
      "2026-07-18T00:00:04.000Z",
    );
    const reboot = state.apply(events[3]!, target, "2026-07-18T00:00:05.000Z");

    expect(first).toMatchObject({
      decision: { code: "POSITION_ACCEPTED" },
      state: { latestSequenceEpoch: 0, latestSequenceNumber: 100 },
    });
    expect(second).toMatchObject({
      decision: { code: "POSITION_ACCEPTED" },
      state: { latestSequenceEpoch: 0, latestSequenceNumber: 101 },
    });
    expect(historical).toMatchObject({
      decision: { code: "POSITION_HISTORICAL" },
      state: { latestSequenceNumber: 101 },
    });
    expect(reboot).toMatchObject({
      decision: { code: "POSITION_ACCEPTED" },
      event: { sequenceEpoch: 1 },
      state: { latestSequenceEpoch: 1, latestSequenceNumber: 1 },
    });
    expect(
      database.connection
        .prepare("SELECT sequence_epoch FROM position_events WHERE id = ?")
        .get(events[3]!.id),
    ).toEqual({ sequence_epoch: null });
    database.close();
  });

  it("fails closed when a cold start provides sequence but no source time", () => {
    const database = new GatewayDatabase(":memory:");
    const observation = positionObservation(
      "position-observation-sequence-only",
      7,
      0,
      "e",
    );
    database.meshObservations.insert(
      meshObservation(observation.meshObservationId),
    );
    const detected = new PositionDuplicateDetector(
      new PositionRepository(database.connection),
    ).observe(observation);
    if (detected.kind !== "new") {
      throw new Error("fixture position unexpectedly duplicated");
    }

    const result = new PositionHighWaterStore(database.connection).apply(
      detected.event,
      { callsign: "N0CALL-7", mappingVersion: "mapping-v1" },
      "2026-07-18T00:00:02.000Z",
    );
    expect(result).toMatchObject({
      decision: { code: "APRS_SKIPPED_OUT_OF_ORDER" },
    });
    expect(result.state).toBeUndefined();
    database.close();
  });

  it("allows a live duplicate to recover a canonical event first seen as backlog", () => {
    const database = new GatewayDatabase(":memory:");
    const backlog: PositionObservation = {
      ...positionObservation("position-observation-backlog"),
      backlogClassification: "backlog",
    };
    const live: PositionObservation = {
      ...backlog,
      id: "position-observation-live",
      meshObservationId: "mesh-position-observation-live",
      gatewayId: "fixture-gateway-b",
      backlogClassification: "live",
      serverIngestedAt: "2026-07-18T00:00:02.005Z",
    };
    for (const observation of [backlog, live]) {
      database.meshObservations.insert(
        meshObservation(observation.meshObservationId),
      );
    }
    const detector = new PositionDuplicateDetector(
      new PositionRepository(database.connection),
    );
    const canonical = detector.observe(backlog).event;
    const duplicate = detector.observe(live);
    const state = new PositionHighWaterStore(database.connection);
    const target = { callsign: "N0CALL-7", mappingVersion: "mapping-v1" };

    const rejected = state.apply(canonical, target, backlog.serverIngestedAt, {
      observationId: backlog.id,
    });
    const accepted = state.apply(
      duplicate.event,
      target,
      live.serverIngestedAt,
      { observationId: live.id },
    );

    expect(rejected).toMatchObject({
      decision: { observationId: backlog.id, code: "POSITION_BACKLOG" },
    });
    expect(accepted).toMatchObject({
      decision: { observationId: live.id, code: "POSITION_ACCEPTED" },
      state: { latestCanonicalEventId: canonical.id },
    });
    expect(accepted.event.canonicalKey).toBe(canonical.canonicalKey);
    database.close();
  });

  it("fails closed when the supplied observation does not match the event", () => {
    const database = new GatewayDatabase(":memory:");
    const expected = positionObservation("position-observation-expected");
    const unrelatedBase = positionObservation("position-observation-unrelated");
    const unrelated: PositionObservation = {
      ...unrelatedBase,
      nodeNum: 7,
      payloadHash: "b".repeat(64),
      position: {
        ...unrelatedBase.position,
        latitudeI: 260000000,
      },
    };
    for (const observation of [expected, unrelated]) {
      database.meshObservations.insert(
        meshObservation(observation.meshObservationId),
      );
    }
    const detector = new PositionDuplicateDetector(
      new PositionRepository(database.connection),
    );
    const event = detector.observe(expected).event;
    detector.observe(unrelated);
    const state = new PositionHighWaterStore(database.connection);

    expect(() =>
      state.apply(
        event,
        { callsign: "N0CALL-7", mappingVersion: "mapping-v1" },
        unrelated.serverIngestedAt,
        { observationId: unrelated.id },
      ),
    ).toThrow("POSITION_PERSISTENCE_FAILED");
    expect(
      database.connection.prepare("SELECT * FROM node_position_state").all(),
    ).toEqual([]);
    expect(
      database.connection.prepare("SELECT * FROM position_decisions").all(),
    ).toEqual([]);
    database.close();
  });
});

function positionObservation(
  id: string,
  sequenceNumber = 100,
  positionTimestampSeconds: number | undefined = 1784332800,
  payloadHashCharacter = "a",
): PositionObservation {
  return {
    schemaVersion: 1,
    id,
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    meshObservationId: `mesh-${id}`,
    gatewayId: "fixture-gateway-a",
    transport: "tcp",
    sessionConnectedAt: "2026-07-18T00:00:00.000Z",
    ingestedAt: "2026-07-18T00:00:01.000Z",
    serverIngestedAt: "2026-07-18T00:00:01.005Z",
    deviceRxTimeSeconds: 1784332800,
    backlogClassification: "live",
    packetId: 1001,
    payloadHash: payloadHashCharacter.repeat(64),
    rxRssi: -70,
    position: {
      latitudeI: 250000000,
      longitudeI: 1215000000,
      altitudeMslMeters: 0,
      ...(positionTimestampSeconds === undefined
        ? {}
        : { positionTimestampSeconds }),
      sequenceNumber,
      precisionBits: 32,
    },
  };
}

function meshObservation(id: string) {
  return createMeshObservation({
    id,
    transport: "tcp",
    sessionConnectedAt: "2026-07-18T00:00:00.000Z",
    ingestedAt: "2026-07-18T00:00:01.000Z",
    serverIngestedAt: "2026-07-18T00:00:01.005Z",
    normalizedFromRadio: { schemaVersion: 1, kind: "other" },
  });
}
