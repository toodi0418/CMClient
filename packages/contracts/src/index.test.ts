import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";

import { TypeCompiler } from "@sinclair/typebox/compiler";

import {
  DomainEventSchema,
  AgentEventSchema,
  AgentLifecycleStatusSchema,
  AprsIgateSubmissionSchema,
  AprsOutboxEntrySchema,
  ComponentIdentityReportSchema,
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
  SetupAcceptTermsRequestSchema,
  SetupResetRequestSchema,
  SetupStatusSchema,
  ProductIdentitySchema,
  ProductTargetSchema,
  ProxyStatusSchema,
  RemoteDispatchTaskSchema,
  SystemCapabilitiesSchema,
  SystemStatusSchema,
  SanitizedPacketFixtureSetSchema,
  TransportConnectionStateSchema,
  SignedUpdateManifestSchema,
  UpdateControlStatusSchema,
  UpdateManifestSchema,
} from "./index";

const releaseIdentity = {
  schemaVersion: 1 as const,
  product: "CMClient" as const,
  version: "2.0.0-dev.1",
  sourceCommit: "a".repeat(40),
  sourceTree: "b".repeat(40),
  channel: "dev" as const,
};

const gatewayIdentity = {
  schemaVersion: 1 as const,
  component: "gateway" as const,
  identity: {
    ...releaseIdentity,
    target: {
      os: "linux" as const,
      architecture: "x86_64" as const,
      profile: "native" as const,
      packageProfile: "workspace" as const,
    },
  },
};

describe("remote dispatch contract", () => {
  it("defines the later feature without a removed compatibility shape", () => {
    const check = TypeCompiler.Compile(RemoteDispatchTaskSchema);
    expect(
      check.Check({
        schemaVersion: 1,
        jobId: "dispatch-1",
        gatewayTarget: "gateway-a",
        meshNetworkId: "mesh-a",
        nodeTarget: 42,
        channel: 0,
        message: "fixture message",
        expiresAt: "2026-07-18T01:00:00.000Z",
        dedupKey: "dedup-1",
        status: "queued",
      }),
    ).toBe(true);
    expect(
      check.Check({
        schemaVersion: 1,
        jobId: "dispatch-1",
        gatewayTarget: "gateway-a",
        meshNetworkId: "mesh-a",
        nodeTarget: 42,
        channel: 0,
        message: "fixture message",
        expiresAt: "2026-07-18T01:00:00.000Z",
        dedupKey: "dedup-1",
        status: "removed_compatibility",
      }),
    ).toBe(false);
  });
});

describe("signed update manifest contract", () => {
  const manifest = {
    schemaVersion: 2,
    release: releaseIdentity,
    publishedAt: "2026-07-18T02:40:00.000Z",
    minimumAgentVersion: "2.0.0-dev.0",
    bundles: [
      {
        target: {
          os: "macos",
          architecture: "universal",
          profile: "native",
          packageProfile: "dmg",
        },
        archive: "tar.zst",
        url: "https://releases.example.invalid/cmclient/2.0.0-dev.1/darwin-aarch64.tar.zst",
        sha256:
          "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        sizeBytes: 4096,
      },
    ],
  };

  it("accepts the exact signed manifest wire shape", () => {
    const check = TypeCompiler.Compile(SignedUpdateManifestSchema);
    expect(
      check.Check({
        manifest,
        signingKeyId: "release-2026",
        signatureAlgorithm: "ed25519",
        signature:
          "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      }),
    ).toBe(true);
  });

  it("rejects unsigned target, digest, and transport deviations", () => {
    const check = TypeCompiler.Compile(UpdateManifestSchema);
    expect(
      check.Check({
        ...manifest,
        bundles: [
          {
            ...manifest.bundles[0],
            target: {
              os: "linux",
              architecture: "riscv64",
              profile: "native",
              packageProfile: "appimage",
            },
          },
        ],
      }),
    ).toBe(false);
    expect(
      check.Check({
        ...manifest,
        bundles: [{ ...manifest.bundles[0], sha256: "A".repeat(64) }],
      }),
    ).toBe(false);
    expect(
      check.Check({
        ...manifest,
        bundles: [
          {
            ...manifest.bundles[0],
            url: "http://releases.example.invalid/archive",
          },
        ],
      }),
    ).toBe(false);
  });

  it("rejects every removed public component selector", () => {
    const check = TypeCompiler.Compile(UpdateManifestSchema);
    for (const component of ["desktop", "headless", "cli", "service"]) {
      expect(
        check.Check({
          ...manifest,
          bundles: [{ ...manifest.bundles[0], component }],
        }),
      ).toBe(false);
    }
    expect(check.Check({ ...manifest, channel: "beta" })).toBe(false);
  });
});

describe("unified product identity contract", () => {
  it("accepts the shared TypeScript and Rust wire fixture", () => {
    const fixture = JSON.parse(
      readFileSync(
        new URL(
          "../../../test/fixtures/unified-product-identity.json",
          import.meta.url,
        ),
        "utf8",
      ),
    ) as unknown;
    expect(
      TypeCompiler.Compile(ComponentIdentityReportSchema).Check(fixture),
    ).toBe(true);
  });

  it("accepts only CMClient identities on supported target tuples", () => {
    const identity = TypeCompiler.Compile(ProductIdentitySchema);
    const target = TypeCompiler.Compile(ProductTargetSchema);

    expect(identity.Check(gatewayIdentity.identity)).toBe(true);
    expect(
      identity.Check({
        ...gatewayIdentity.identity,
        sourceTree: `sha256:${"c".repeat(64)}`,
      }),
    ).toBe(true);
    expect(
      identity.Check({ ...gatewayIdentity.identity, product: "CMClient CLI" }),
    ).toBe(false);
    expect(
      identity.Check({ ...gatewayIdentity.identity, sourceCommit: "unknown" }),
    ).toBe(false);
    expect(
      target.Check({
        os: "windows",
        architecture: "aarch64",
        profile: "native",
        packageProfile: "setup",
      }),
    ).toBe(false);
    expect(
      target.Check({
        os: "windows",
        architecture: "x86_64",
        profile: "docker",
        packageProfile: "oci",
      }),
    ).toBe(false);
  });
});

describe("Agent update control contract", () => {
  it("projects a persistent update phase without raw diagnostics", () => {
    const check = TypeCompiler.Compile(UpdateControlStatusSchema);
    expect(
      check.Check({
        schemaVersion: 1,
        job: {
          id: "update-1",
          phase: "downloading",
          updatedAt: "2026-07-18T03:00:00.000Z",
          errorCode: null,
          bytesDownloaded: 1024,
          bytesTotal: 4096,
          bytesPerSecond: 512,
          recentLogCodes: ["UPDATE_DOWNLOAD_STARTED"],
        },
      }),
    ).toBe(true);
    expect(
      check.Check({
        schemaVersion: 1,
        job: {
          id: "update-1",
          phase: "downloading",
          updatedAt: "2026-07-18T03:00:00.000Z",
          errorCode: "https://must-not-leak.invalid",
          bytesDownloaded: 1024,
          bytesTotal: 4096,
          bytesPerSecond: 512,
          recentLogCodes: [],
        },
      }),
    ).toBe(false);
  });
});

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
        schemaVersion: 2,
        identity: gatewayIdentity,
        capabilities: {
          managementWeb: { available: false, reasonCode: "owned_by_agent" },
          commandMode: { available: false, reasonCode: "owned_by_agent" },
          graphicalMode: {
            available: false,
            reasonCode: "owned_by_graphical_mode",
          },
          loginAutostart: { available: false, reasonCode: "owned_by_agent" },
          serial: { available: true },
          nativeUpdate: { available: false, reasonCode: "owned_by_agent" },
          dockerPullRecreateUpdate: {
            available: false,
            reasonCode: "unavailable_in_native",
          },
          localControl: { available: false, reasonCode: "owned_by_agent" },
          remoteDispatch: {
            available: false,
            reasonCode: "not_enabled",
          },
        },
      }),
    ).toBe(true);
    expect(
      check.Check({
        schemaVersion: 2,
        identity: gatewayIdentity,
        capabilities: {
          managementWeb: { available: false, reasonCode: "owned_by_agent" },
          commandMode: { available: false, reasonCode: "owned_by_agent" },
          graphicalMode: { available: false },
          loginAutostart: { available: false, reasonCode: "owned_by_agent" },
          serial: { available: true },
          nativeUpdate: { available: false, reasonCode: "owned_by_agent" },
          dockerPullRecreateUpdate: {
            available: false,
            reasonCode: "unavailable_in_native",
          },
          localControl: { available: false, reasonCode: "owned_by_agent" },
          remoteDispatch: {
            available: false,
            reasonCode: "not_enabled",
          },
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
        schemaVersion: 2,
        health: "ok",
        identity: gatewayIdentity,
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
          activeMappingHash: "hash-1",
          activeMappingCount: 1,
          provisionState: "valid",
          lastServerTime: "2026-07-18T00:00:00.000Z",
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
          provisionState: "unavailable",
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

describe("Agent setup and lifecycle contracts", () => {
  it("makes recovery_required a canonical redacted setup phase", () => {
    const check = TypeCompiler.Compile(SetupStatusSchema);
    const recovery = {
      schemaVersion: 1,
      phase: "recovery_required",
      setupRequired: true,
      termsRequired: false,
      credentialsRequired: false,
      validating: false,
      ready: false,
      recoveryRequired: true,
      reasonCode: "SETUP_RECOVERY_REQUIRED",
    };

    expect(check.Check(recovery)).toBe(true);
    expect(check.Check({ ...recovery, setupGeneration: 42 })).toBe(false);
    expect(check.Check({ ...recovery, apiKey: "must-not-leak" })).toBe(false);
    expect(check.Check({ ...recovery, phase: "degraded" })).toBe(false);
  });

  it("binds setup commands without accepting credentials or hidden state", () => {
    const terms = TypeCompiler.Compile(SetupAcceptTermsRequestSchema);
    const reset = TypeCompiler.Compile(SetupResetRequestSchema);

    expect(terms.Check({ termsVersion: "cmclient-2.0-terms-v1" })).toBe(true);
    expect(
      terms.Check({
        termsVersion: "cmclient-2.0-terms-v1",
        apiKey: "must-not-cross-this-contract",
      }),
    ).toBe(false);
    expect(reset.Check({ confirmation: "operational_reset" })).toBe(true);
    expect(reset.Check({ confirmation: true })).toBe(false);
  });

  it("keeps Agent setup, lifecycle, and update events in an Agent namespace", () => {
    const lifecycle = {
      schemaVersion: 1,
      agent: "running",
      gateway: "backoff",
      managementWeb: "running",
      managementWebUrl: "http://127.0.0.1:7080",
      uptimeSeconds: 12,
      latestErrorCode: "GATEWAY_START_FAILED",
    };
    expect(
      TypeCompiler.Compile(AgentLifecycleStatusSchema).Check(lifecycle),
    ).toBe(true);

    const check = TypeCompiler.Compile(AgentEventSchema);
    expect(
      check.Check({
        eventId: "agent:lifecycle:17",
        schemaVersion: 1,
        stream: "lifecycle",
        type: "lifecycle.status",
        occurredAt: "2026-07-25T00:00:00.000Z",
        source: "agent",
        payload: lifecycle,
      }),
    ).toBe(true);
    expect(
      check.Check({
        eventId: "gateway-17",
        schemaVersion: 1,
        stream: "lifecycle",
        type: "lifecycle.status",
        occurredAt: "2026-07-25T00:00:00.000Z",
        source: "agent",
        payload: lifecycle,
      }),
    ).toBe(false);
    expect(
      check.Check({
        eventId: "agent:setup:17",
        schemaVersion: 1,
        stream: "lifecycle",
        type: "lifecycle.status",
        occurredAt: "2026-07-25T00:00:00.000Z",
        source: "agent",
        payload: lifecycle,
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
    expect(
      check.Check({
        id: "job-1",
        type: "diagnostics.integrity_check",
        status: "failed",
        createdAt: "2026-07-18T00:00:00.000Z",
        updatedAt: "2026-07-18T00:00:01.000Z",
        error: { code: "INVALID CODE", params: {} },
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
  it.each([
    "sending",
    "transmission_uncertain",
    "submitted",
    "observer_confirmed",
    "observation_expired",
  ] as const)(
    "accepts sanitized %s station delivery state",
    (deliveryStatus) => {
      const submission = TypeCompiler.Compile(AprsIgateSubmissionSchema);
      const value = {
        id: "aprs-igate-00000000-0000-4000-8000-000000000001",
        packetKind: "beacon",
        deliveryStatus,
        attemptedAt: "2026-07-18T00:00:00.000Z",
        updatedAt: "2026-07-18T00:00:01.000Z",
        observationExpiresAt: "2026-07-18T03:00:00.000Z",
        ...(deliveryStatus === "submitted" ||
        deliveryStatus === "observer_confirmed"
          ? { submittedAt: "2026-07-18T00:00:01.000Z" }
          : {}),
        ...(deliveryStatus === "observer_confirmed"
          ? { observerConfirmedAt: "2026-07-18T00:00:02.000Z" }
          : {}),
      };

      expect(submission.Check(value)).toBe(true);
      expect(submission.Check({ ...value, callsign: "N0CALL-7" })).toBe(false);
      expect(submission.Check({ ...value, info: "private-packet" })).toBe(
        false,
      );
      expect(
        submission.Check({ ...value, provisionFingerprint: "a".repeat(64) }),
      ).toBe(false);
    },
  );

  it.each([
    "queued",
    "sending",
    "failed",
    "submitted",
    "observer_confirmed",
    "observation_expired",
  ] as const)("accepts the %s APRS delivery state", (deliveryStatus) => {
    const outboxEntry = TypeCompiler.Compile(AprsOutboxEntrySchema);
    const entry = {
      id: "outbox-1",
      callsign: "N0CALL-7",
      canonicalEventId: "position-event-1",
      status:
        deliveryStatus === "queued"
          ? "queued"
          : deliveryStatus === "sending"
            ? "sending"
            : deliveryStatus === "failed"
              ? "failed"
              : "sent",
      deliveryStatus,
      attempts: 0,
      nextAttemptAt: "2026-07-18T00:00:00.000Z",
      createdAt: "2026-07-18T00:00:00.000Z",
      updatedAt: "2026-07-18T00:00:00.000Z",
    };

    expect(outboxEntry.Check(entry)).toBe(true);
    expect(outboxEntry.Check({ ...entry, deliveryStatus: "sent" })).toBe(false);
  });

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
