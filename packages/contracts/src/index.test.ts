import { describe, expect, it } from "vitest";

import { TypeCompiler } from "@sinclair/typebox/compiler";

import {
  DomainEventSchema,
  CallMeshOverviewSchema,
  JobDetailSchema,
  JOB_STATUSES,
  MeshMessageSchema,
  MeshNodeSchema,
  MeshObservationSchema,
  MeshTelemetrySchema,
  NormalizedFromRadioSchema,
  NodePositionStateSchema,
  PositionCanonicalEventSchema,
  PositionDecisionSchema,
  PositionObservationSchema,
  ProxyStatusSchema,
  SystemCapabilitiesSchema,
  SystemStatusSchema,
  SanitizedPacketFixtureSetSchema,
  TransportConnectionStateSchema,
} from "./index";

describe("job contracts", () => {
  it("includes terminal rollback states", () => {
    expect(JOB_STATUSES).toContain("rolled_back");
    expect(JOB_STATUSES).toContain("failed");
  });
});

describe("system capabilities contract", () => {
  it("requires every declared capability", () => {
    const check = TypeCompiler.Compile(SystemCapabilitiesSchema);
    expect(
      check.Check({
        schemaVersion: 1,
        platform: "linux",
        build: { version: "2.0.0-dev.0", commit: "abc123", channel: "dev" },
        capabilities: {
          managementWeb: { available: true },
          update: { available: true },
          tray: {
            available: false,
            reasonCode: "CAPABILITY_UNAVAILABLE_PLATFORM",
          },
          serial: { available: true },
          service: { available: true },
          autoStart: { available: true },
          docker: { available: true },
        },
      }),
    ).toBe(true);
    expect(
      check.Check({
        schemaVersion: 1,
        platform: "linux",
        build: { version: "2.0.0-dev.0", commit: "abc123", channel: "dev" },
        capabilities: {
          managementWeb: { available: true },
          update: { available: true },
          tray: { available: false },
          serial: { available: true },
          service: { available: true },
          autoStart: { available: true },
          docker: { available: true },
        },
      }),
    ).toBe(false);
  });
});

describe("system status contract", () => {
  it("keeps the API status projection schema-backed", () => {
    const check = TypeCompiler.Compile(SystemStatusSchema);

    expect(
      check.Check({
        health: "ok",
        build: { version: "2.0.0-dev.0", commit: "abc123", channel: "dev" },
      }),
    ).toBe(true);
    expect(check.Check({ health: "degraded" })).toBe(false);
  });
});

describe("CallMesh contract", () => {
  it("exposes only synchronized mappings and stable status metadata", () => {
    const check = TypeCompiler.Compile(CallMeshOverviewSchema);
    expect(
      check.Check({
        status: {
          state: "ready",
          updatedAt: "2026-07-18T00:00:00.000Z",
          activeMappingVersion: "mapping-1",
          activeMappingCount: 1,
        },
        mappings: [
          {
            version: "mapping-1",
            effectiveAt: "2026-07-18T00:00:00.000Z",
            meshNetworkId: "fixture",
            nodeNum: 42,
            callsign: "N0CALL-7",
          },
        ],
      }),
    ).toBe(true);
    expect(
      check.Check({
        status: {
          state: "ready",
          updatedAt: "2026-07-18T00:00:00.000Z",
          activeMappingCount: 0,
          apiKey: "must-not-be-public",
        },
        mappings: [],
      }),
    ).toBe(false);
  });
});

describe("domain event contract", () => {
  it("requires the versioned SSE envelope fields", () => {
    const check = TypeCompiler.Compile(DomainEventSchema);
    expect(
      check.Check({
        eventId: "event-1",
        schemaVersion: 1,
        type: "gateway.ready",
        occurredAt: "2026-07-18T00:00:00.000Z",
        source: "gateway",
        payload: { port: 4810 },
      }),
    ).toBe(true);
    expect(
      check.Check({
        eventId: "event-1",
        schemaVersion: 1,
        type: "invalid event",
        occurredAt: "2026-07-18T00:00:00.000Z",
        source: "gateway",
        payload: {},
      }),
    ).toBe(false);
  });
});

describe("job contract", () => {
  it("keeps API job state free of execution input", () => {
    const check = TypeCompiler.Compile(JobDetailSchema);
    expect(
      check.Check({
        id: "job-1",
        type: "diagnostics.integrity_check",
        status: "succeeded",
        createdAt: "2026-07-18T00:00:00.000Z",
        updatedAt: "2026-07-18T00:00:01.000Z",
        completedAt: "2026-07-18T00:00:01.000Z",
      }),
    ).toBe(true);
    expect(
      check.Check({
        id: "job-1",
        type: "diagnostics.integrity_check",
        status: "succeeded",
        createdAt: "2026-07-18T00:00:00.000Z",
        updatedAt: "2026-07-18T00:00:01.000Z",
        input: { secret: "must-not-leak" },
      }),
    ).toBe(false);
  });
});

describe("transport contract", () => {
  it("represents backoff with its retry attempt and stable reason", () => {
    const check = TypeCompiler.Compile(TransportConnectionStateSchema);
    expect(
      check.Check({
        transport: "tcp",
        status: "backoff",
        changedAt: "2026-07-18T00:00:00.000Z",
        attempt: 2,
        reasonCode: "TCP_CONNECT_FAILED",
      }),
    ).toBe(true);
  });

  it("keeps device receive time distinct in normalized packets", () => {
    const check = TypeCompiler.Compile(NormalizedFromRadioSchema);
    expect(
      check.Check({
        schemaVersion: 1,
        kind: "packet",
        fromRadioId: 13,
        packet: {
          sender: 4041641985,
          packetId: 1001,
          portNum: "POSITION_APP",
          deviceRxTimeSeconds: 1893456246,
        },
      }),
    ).toBe(true);
  });

  it("keeps session, API ingest, and server ingest times distinct", () => {
    const check = TypeCompiler.Compile(MeshObservationSchema);
    expect(
      check.Check({
        schemaVersion: 1,
        id: "observation-1",
        transport: "tcp",
        sessionConnectedAt: "2026-07-18T00:00:00.000Z",
        ingestedAt: "2026-07-18T00:00:01.000Z",
        serverIngestedAt: "2026-07-18T00:00:01.005Z",
        deviceRxTimeSeconds: 1784332800,
        backlogClassification: "backlog",
        normalizedFromRadio: {
          schemaVersion: 1,
          kind: "packet",
          packet: { portNum: "POSITION_APP" },
        },
      }),
    ).toBe(true);
  });
});

describe("proxy contract", () => {
  it("projects operational state without raw client or address identifiers", () => {
    const check = TypeCompiler.Compile(ProxyStatusSchema);
    const status = {
      state: "running",
      listener: { host: "127.0.0.1", port: 4403 },
      policy: {
        activeClients: 1,
        allowLan: false,
        allowedAddressCount: 0,
        maxClients: 16,
        maxWritesPerMinute: 120,
        mode: "monitor",
      },
      queue: {
        broadcastAccepted: 0,
        broadcastDropped: 0,
        broadcastFrames: 0,
        directAccepted: 0,
        directDropped: 0,
        pendingCorrelations: 0,
        queuedWrites: 0,
        writing: false,
      },
      recentAudit: [
        {
          action: "client_admitted",
          clientFingerprint: "0123456789abcdef",
          mode: "monitor",
          occurredAt: "2026-07-18T00:00:00.000Z",
        },
      ],
      upstream: {
        configFrameCount: 0,
        metrics: {
          bytesReceived: 0,
          bytesSent: 0,
          framesReceived: 0,
          framesSent: 0,
          malformedFrames: 0,
          reconnects: 0,
        },
        state: {
          transport: "tcp",
          status: "ready",
          changedAt: "2026-07-18T00:00:00.000Z",
        },
      },
    };
    expect(check.Check(status)).toBe(true);
    expect(
      check.Check({
        ...status,
        recentAudit: [
          {
            ...status.recentAudit[0],
            clientId: "must-not-be-public",
          },
        ],
      }),
    ).toBe(false);
  });
});

describe("mesh domain contracts", () => {
  it("keeps network-scoped node, message, and telemetry records versioned", () => {
    const node = TypeCompiler.Compile(MeshNodeSchema);
    const message = TypeCompiler.Compile(MeshMessageSchema);
    const telemetry = TypeCompiler.Compile(MeshTelemetrySchema);
    expect(
      node.Check({
        schemaVersion: 1,
        meshNetworkId: "fixture-network",
        nodeNum: 42,
        firstSeenAt: "2026-07-18T00:00:00.000Z",
        lastSeenAt: "2026-07-18T00:00:01.000Z",
        lastObservationId: "observation-1",
      }),
    ).toBe(true);
    expect(
      message.Check({
        schemaVersion: 1,
        id: "message-1",
        observationId: "observation-2",
        meshNetworkId: "fixture-network",
        sender: 42,
        text: "fixture message",
        observedAt: "2026-07-18T00:00:01.000Z",
      }),
    ).toBe(true);
    expect(
      telemetry.Check({
        schemaVersion: 1,
        id: "telemetry-1",
        observationId: "observation-3",
        meshNetworkId: "fixture-network",
        nodeNum: 42,
        metricKind: "deviceMetrics",
        metrics: { batteryLevel: 73, voltage: 3.9 },
        observedAt: "2026-07-18T00:00:02.000Z",
      }),
    ).toBe(true);
  });
});

describe("packet recording contract", () => {
  it("only permits the fixture-safe recording representation", () => {
    const check = TypeCompiler.Compile(SanitizedPacketFixtureSetSchema);
    expect(
      check.Check({
        schemaVersion: 1,
        dataset: "cmclient-sanitized-packet-recordings",
        sanitized: true,
        fixtures: [
          {
            id: "fixture-record-000001",
            sanitized: true,
            recording: {
              rawFrameEncoding: "synthetic-hex",
              rawFrameHex: "0123abcd",
              gatewayId: "fixture-gateway-a",
              meshNetworkId: "fixture-network-a",
              transport: "tcp",
              transportMetadata: { connectionStatus: "ready" },
              sessionConnectedAt: "2030-01-02T00:00:00.000Z",
              receivedAt: "2030-01-02T00:00:01.000Z",
              ingestedAt: "2030-01-02T00:00:02.000Z",
              serverIngestedAt: "2030-01-02T00:00:03.000Z",
            },
            normalizedFromRadio: { schemaVersion: 1, kind: "other" },
          },
        ],
      }),
    ).toBe(true);
  });
});

describe("position domain contracts", () => {
  it("keeps local observations, canonical events, decisions, and high-water state separate", () => {
    const observation = TypeCompiler.Compile(PositionObservationSchema);
    const event = TypeCompiler.Compile(PositionCanonicalEventSchema);
    const decision = TypeCompiler.Compile(PositionDecisionSchema);
    const state = TypeCompiler.Compile(NodePositionStateSchema);
    const position = {
      latitudeI: 250000000,
      longitudeI: 1215000000,
      precisionBits: 32,
      positionTimestampSeconds: 1784332800,
      sequenceNumber: 9,
    };
    expect(
      observation.Check({
        schemaVersion: 1,
        id: "position-observation-1",
        meshNetworkId: "fixture-network",
        nodeNum: 42,
        meshObservationId: "mesh-observation-1",
        gatewayId: "fixture-gateway-a",
        transport: "tcp",
        sessionConnectedAt: "2026-07-18T00:00:00.000Z",
        ingestedAt: "2026-07-18T00:00:01.000Z",
        serverIngestedAt: "2026-07-18T00:00:01.005Z",
        backlogClassification: "live",
        payloadHash: "a".repeat(64),
        position,
      }),
    ).toBe(true);
    expect(
      event.Check({
        schemaVersion: 1,
        id: "position-event-1",
        canonicalKey: "canonical-key-1",
        meshNetworkId: "fixture-network",
        nodeNum: 42,
        sourceObservationId: "position-observation-1",
        payloadHash: "a".repeat(64),
        eventTime: "2026-07-18T00:00:00.000Z",
        eventTimeSource: "position_timestamp",
        sequenceEpoch: 1,
        sequenceNumber: 9,
        position,
        createdAt: "2026-07-18T00:00:01.005Z",
      }),
    ).toBe(true);
    expect(
      decision.Check({
        schemaVersion: 1,
        id: "position-decision-1",
        observationId: "position-observation-1",
        canonicalEventId: "position-event-1",
        code: "POSITION_ACCEPTED",
        decidedAt: "2026-07-18T00:00:01.006Z",
        parameters: { source: "position_timestamp" },
      }),
    ).toBe(true);
    expect(
      state.Check({
        schemaVersion: 1,
        meshNetworkId: "fixture-network",
        nodeNum: 42,
        callsign: "N0CALL-7",
        mappingVersion: "mapping-v1",
        latestCanonicalEventId: "position-event-1",
        latestEventTime: "2026-07-18T00:00:00.000Z",
        latestSequenceEpoch: 1,
        latestSequenceNumber: 9,
        latestLatitudeI: 250000000,
        latestLongitudeI: 1215000000,
        updatedAt: "2026-07-18T00:00:01.006Z",
      }),
    ).toBe(true);
  });
});
