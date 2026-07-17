import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import type {
  BacklogClassification,
  PositionCanonicalEvent,
  PositionDecisionCode,
  PositionObservation,
} from "@cmclient/contracts";

import { encodeAprsPosition } from "./aprs-position";
import { AprsIsMonitor, AprsRemoteHighWaterStore } from "./aprs-monitor";
import { createMeshObservation } from "./observations";
import { GatewayDatabase } from "./persistence/database";
import {
  PositionDuplicateDetector,
  PositionHighWaterStore,
  PositionRepository,
} from "./position";
import { validatePositionForAprs } from "./position-validation";

interface ReplayRecord {
  id: string;
  gatewayId: string;
  meshNetworkId: string;
  nodeNum: number;
  transport?: "tcp" | "serial" | "simulator";
  viaMqtt?: boolean;
  eventTime?: string;
  sequenceNumber: number;
  latitudeI: number;
  longitudeI: number;
  altitudeMslMeters?: number;
  altitudeHaeMeters?: number;
  groundSpeedMetersPerSecond?: number;
  groundTrackDegrees?: number;
  precisionBits: number;
  payloadHashCharacter: string;
  backlogClassification: BacklogClassification;
  sessionConnectedAt: string;
  ingestedAt: string;
  serverIngestedAt: string;
}

interface ReplayFixture {
  schemaVersion: 1;
  dataset: "cmclient-position-aprs-replay-matrix";
  sanitized: true;
  privacy: {
    source: "synthetic";
    containsProductionTraffic: false;
    containsSecrets: false;
    containsPersonalLocations: false;
  };
  records: ReplayRecord[];
}

const fixture = JSON.parse(
  readFileSync("test/fixtures/position-aprs-replay.json", "utf8"),
) as ReplayFixture;
const target = {
  callsign: "N0CALL-7",
  mappingVersion: "mapping-v1",
  meshNetworkId: "fixture-network-alpha",
  nodeNum: 42,
};

describe("position/APRS replay matrix", () => {
  it("preserves fixture privacy and proves multi-iGate APRS bytes are identical", () => {
    expect(fixture).toMatchObject({
      schemaVersion: 1,
      dataset: "cmclient-position-aprs-replay-matrix",
      sanitized: true,
      privacy: {
        source: "synthetic",
        containsProductionTraffic: false,
        containsSecrets: false,
        containsPersonalLocations: false,
      },
    });
    for (const replayRecord of fixture.records) {
      expect(replayRecord.id).toMatch(/^[a-z0-9-]+$/);
      expect(replayRecord.gatewayId).toMatch(/^fixture-gateway-[a-z]+$/);
      expect(replayRecord.meshNetworkId).toMatch(/^fixture-network-[a-z]+$/);
    }
    expect(JSON.stringify(fixture.records)).not.toMatch(
      /(private|secret|passcode|token|password)/i,
    );
    const first = record("event-e-igate-a");
    const second = record("event-e-igate-b");
    const gatewayA = new GatewayDatabase(":memory:");
    const gatewayB = new GatewayDatabase(":memory:");
    const eventA = observe(gatewayA, first);
    const eventB = observe(gatewayB, second);

    expect(eventA.canonicalKey).toBe(eventB.canonicalKey);
    expect(encode(eventA).data).toBe(encode(eventB).data);
    expect(encode(eventA).eventMarker).toBe(encode(eventB).eventMarker);
    expect(first.transport).not.toBe(second.transport);
    expect(second.viaMqtt).toBe(true);
    gatewayA.close();
    gatewayB.close();
  });

  it("replays late, backlog, same-second, reboot, clock, and cold-start cases fail closed", () => {
    const database = new GatewayDatabase(":memory:");
    const highWater = new PositionHighWaterStore(database.connection);
    const outcomes: PositionDecisionCode[] = [];
    const records = [
      record("event-e-igate-a"),
      record("event-d-late-live"),
      record("event-c-api-backlog"),
      record("event-f-same-second-higher-sequence"),
      record("event-g-sequence-reboot"),
    ];
    let latest: PositionCanonicalEvent | undefined;

    for (const replayRecord of records.sort((a, b) =>
      a.serverIngestedAt.localeCompare(b.serverIngestedAt),
    )) {
      const event = observe(database, replayRecord);
      const result = highWater.apply(
        event,
        target,
        replayRecord.serverIngestedAt,
      );
      outcomes.push(result.decision.code);
      latest = result.event;
    }

    const future = observe(database, record("event-h-future-clock"));
    const futureValidation = validatePositionForAprs(future, {
      now: new Date("2030-01-02T03:14:00.000Z"),
    });
    const sequenceOnly = observe(database, record("event-i-sequence-only"));
    const coldStart = highWater.apply(
      sequenceOnly,
      { ...target, callsign: "N1CALL-7", mappingVersion: "mapping-v2" },
      "2030-01-02T03:14:00.010Z",
    );

    expect(outcomes).toEqual([
      "POSITION_ACCEPTED",
      "POSITION_HISTORICAL",
      "POSITION_BACKLOG",
      "POSITION_ACCEPTED",
      "POSITION_ACCEPTED",
    ]);
    expect(latest).toMatchObject({ sequenceEpoch: 1, sequenceNumber: 1 });
    expect(futureValidation).toEqual({
      accepted: false,
      code: "POSITION_CLOCK_INVALID",
    });
    expect(coldStart).toMatchObject({
      decision: { code: "APRS_SKIPPED_OUT_OF_ORDER" },
    });
    expect(coldStart.state).toBeUndefined();
    database.close();
  });

  it("uses remote deterministic markers to block a station that missed the newer event", () => {
    const database = new GatewayDatabase(":memory:");
    const remoteHighWater = new AprsRemoteHighWaterStore(database.connection);
    const monitor = new AprsIsMonitor([target], remoteHighWater);
    const newer = observe(database, record("event-e-igate-a"));
    const older = observe(database, record("event-d-late-live"));

    const monitored = monitor.observeLine(
      encode(newer).data,
      "2030-01-02T03:06:00.000Z",
    );

    expect(monitored).toMatchObject({ kind: "advanced" });
    expect(remoteHighWater.canUpload(older, target)).toBe(false);
    expect(remoteHighWater.canUpload(newer, target)).toBe(true);
    database.close();
  });

  it("isolates mapping versions while retaining per-mapping state", () => {
    const database = new GatewayDatabase(":memory:");
    const event = observe(database, record("event-e-igate-a"));
    const highWater = new PositionHighWaterStore(database.connection);
    const primary = highWater.apply(event, target, "2030-01-02T03:05:11.010Z");
    const remapped = highWater.apply(
      event,
      { ...target, callsign: "N2CALL-7", mappingVersion: "mapping-v2" },
      "2030-01-02T03:05:11.011Z",
    );

    expect(primary).toMatchObject({ decision: { code: "POSITION_ACCEPTED" } });
    expect(remapped).toMatchObject({
      decision: { code: "POSITION_ACCEPTED" },
      state: { callsign: "N2CALL-7", mappingVersion: "mapping-v2" },
    });
    database.close();
  });

  it("rejects reduced precision and preserves altitude zero while omitting a partial speed pair", () => {
    const database = new GatewayDatabase(":memory:");
    const reducedPrecision = observe(
      database,
      record("event-j-insufficient-precision"),
    );
    const partialSpeed = observe(
      database,
      record("event-k-altitude-zero-partial-track"),
    );
    const now = new Date("2030-01-02T03:20:00.000Z");

    expect(validatePositionForAprs(reducedPrecision, { now })).toEqual({
      accepted: false,
      code: "POSITION_PRECISION_INSUFFICIENT",
    });
    const validation = validatePositionForAprs(partialSpeed, { now });
    expect(validation).toMatchObject({
      accepted: true,
      speedTrackIncluded: false,
    });
    if (!validation.accepted) {
      throw new Error("fixture position should be eligible");
    }
    expect(validation.event.position).toMatchObject({
      altitudeMslMeters: 0,
      altitudeHaeMeters: 45,
    });
    expect(
      validation.event.position.groundSpeedMetersPerSecond,
    ).toBeUndefined();
    expect(validation.event.position.groundTrackDegrees).toBeUndefined();
    database.close();
  });
});

function record(id: string): ReplayRecord {
  const result = fixture.records.find((entry) => entry.id === id);
  if (!result) {
    throw new Error(`missing replay record ${id}`);
  }
  return structuredClone(result);
}

function observe(
  database: GatewayDatabase,
  replayRecord: ReplayRecord,
): PositionCanonicalEvent {
  const meshObservationId = `mesh-observation-${replayRecord.id}`;
  database.meshObservations.insert(
    createMeshObservation({
      id: meshObservationId,
      transport: replayRecord.transport ?? "simulator",
      sessionConnectedAt: replayRecord.sessionConnectedAt,
      ingestedAt: replayRecord.ingestedAt,
      serverIngestedAt: replayRecord.serverIngestedAt,
      normalizedFromRadio: { schemaVersion: 1, kind: "other" },
    }),
  );
  const observation: PositionObservation = {
    schemaVersion: 1,
    id: `position-observation-${replayRecord.id}`,
    meshNetworkId: replayRecord.meshNetworkId,
    nodeNum: replayRecord.nodeNum,
    meshObservationId,
    gatewayId: replayRecord.gatewayId,
    transport: replayRecord.transport ?? "simulator",
    sessionConnectedAt: replayRecord.sessionConnectedAt,
    ingestedAt: replayRecord.ingestedAt,
    serverIngestedAt: replayRecord.serverIngestedAt,
    backlogClassification: replayRecord.backlogClassification,
    payloadHash: replayRecord.payloadHashCharacter.repeat(64),
    ...(replayRecord.viaMqtt === undefined
      ? {}
      : { viaMqtt: replayRecord.viaMqtt }),
    position: {
      latitudeI: replayRecord.latitudeI,
      longitudeI: replayRecord.longitudeI,
      ...(replayRecord.altitudeMslMeters === undefined
        ? {}
        : { altitudeMslMeters: replayRecord.altitudeMslMeters }),
      ...(replayRecord.altitudeHaeMeters === undefined
        ? {}
        : { altitudeHaeMeters: replayRecord.altitudeHaeMeters }),
      ...(replayRecord.groundSpeedMetersPerSecond === undefined
        ? {}
        : {
            groundSpeedMetersPerSecond: replayRecord.groundSpeedMetersPerSecond,
          }),
      ...(replayRecord.groundTrackDegrees === undefined
        ? {}
        : { groundTrackDegrees: replayRecord.groundTrackDegrees }),
      precisionBits: replayRecord.precisionBits,
      sequenceNumber: replayRecord.sequenceNumber,
      ...(replayRecord.eventTime
        ? {
            positionTimestampSeconds: Math.floor(
              Date.parse(replayRecord.eventTime) / 1_000,
            ),
          }
        : {}),
    },
  };
  const result = new PositionDuplicateDetector(
    new PositionRepository(database.connection),
  ).observe(observation);
  return result.event;
}

function encode(event: PositionCanonicalEvent) {
  return encodeAprsPosition(event, {
    source: target.callsign,
    destination: "APCM20",
    symbolTable: "/",
    symbolCode: ">",
  });
}
