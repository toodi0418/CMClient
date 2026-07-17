import { describe, expect, it } from "vitest";

import { TypeCompiler } from "@sinclair/typebox/compiler";

import {
  DomainEventSchema,
  JobDetailSchema,
  JOB_STATUSES,
  MeshMessageSchema,
  MeshNodeSchema,
  MeshObservationSchema,
  MeshTelemetrySchema,
  NormalizedFromRadioSchema,
  SystemCapabilitiesSchema,
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
