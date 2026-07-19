import { describe, expect, it } from "vitest";

import type {
  TransportConnectionState,
  TransportMetrics,
} from "@cmclient/contracts";

import { DomainEventBus } from "./events";
import { MeshGatewayRuntime } from "./mesh-runtime";
import { GatewayDatabase } from "./persistence/database";
import { MeshtasticApplicationDecoder } from "./protobuf/application";
import { MeshtasticProtobufCodec } from "./protobuf/protobuf";
import { loadMeshtasticSchema, type MeshtasticSchema } from "./protobuf/schema";
import type {
  MeshtasticTransport,
  TransportEvent,
  TransportEventListener,
} from "./transport/types";

describe("MeshGatewayRuntime", () => {
  it("routes a transport frame through persistence, SSE events, ordering, and APRS outbox", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      {
        version: "mapping-v1",
        effectiveAt: "2026-07-18T00:00:00.000Z",
        meshNetworkId: "fixture-network",
        nodeNum: 42,
        callsign: "N0CALL-7",
      },
    ]);
    const schema = await loadMeshtasticSchema();
    const transport = new FixtureTransport();
    const events = new DomainEventBus({
      clock: () => new Date("2026-07-18T00:00:10.000Z"),
      eventIdFactory: sequentialFactory("event"),
    });
    const observedTypes: string[] = [];
    events.subscribe((event) => observedTypes.push(event.type));
    const runtime = createRuntime(database, schema, transport, events);

    runtime.start();
    transport.emit({
      kind: "frame",
      frame: positionFrame(schema, 32),
      receivedAt: "2026-07-18T00:00:05.000Z",
      sessionConnectedAt: "2026-07-18T00:00:01.000Z",
    });

    expect(database.meshNodes.find("fixture-network", 42)).toMatchObject({
      nodeNum: 42,
      lastObservationId: "mesh-observation-observation-1",
    });
    expect(database.positions.listCanonicalEvents(10)).toEqual([
      expect.objectContaining({
        meshNetworkId: "fixture-network",
        nodeNum: 42,
        eventTime: "2026-07-18T00:00:04.250Z",
        eventTimeSource: "position_timestamp",
        sequenceNumber: 7,
        position: expect.objectContaining({
          latitudeI: 250_475_000,
          longitudeI: 1_215_175_000,
          precisionBits: 32,
        }),
      }),
    ]);
    expect(database.aprsOutbox.list(10)).toEqual([
      expect.objectContaining({
        callsign: "N0CALL-7",
        status: "queued",
        attempts: 0,
      }),
    ]);
    expect(
      database.connection
        .prepare(
          "SELECT mapping_version, sequence_epoch, sequence_number FROM aprs_outbox",
        )
        .get(),
    ).toEqual({
      mapping_version: "mapping-v1",
      sequence_epoch: 0,
      sequence_number: 7,
    });
    const queued = database.aprsOutbox.find(
      database.aprsOutbox.list(10)[0]!.id,
    );
    expect(queued?.data).toMatch(
      /^N0CALL-7>APCM20:\/180000z2502\.85N\/12131\.05E>/,
    );
    expect(queued?.data).toMatch(/ CM2\/[a-f0-9]{12}$/);
    expect(queued?.data).not.toMatch(/gateway|rssi|snr|received/i);
    expect(observedTypes).toEqual(
      expect.arrayContaining([
        "mesh.transport.state",
        "mesh.observation.persisted",
        "node.updated",
        "position.observed",
        "position.decision",
        "aprs.outbox.queued",
      ]),
    );
    expect(events.replayBufferSize).toBeGreaterThanOrEqual(6);

    await runtime.stop();
    database.close();
  });

  it("retains an insufficient-precision position without advancing or enqueueing", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      {
        version: "mapping-v1",
        effectiveAt: "2026-07-18T00:00:00.000Z",
        meshNetworkId: "fixture-network",
        nodeNum: 42,
        callsign: "N0CALL-7",
      },
    ]);
    const schema = await loadMeshtasticSchema();
    const transport = new FixtureTransport();
    const events = new DomainEventBus({
      eventIdFactory: sequentialFactory("event"),
    });
    const decisions: string[] = [];
    events.subscribe((event) => {
      if (event.type === "position.decision") {
        decisions.push(String(event.payload.code));
      }
    });
    const runtime = createRuntime(database, schema, transport, events);

    runtime.start();
    const result = runtime.ingestFrame({
      kind: "frame",
      frame: positionFrame(schema, 24),
      receivedAt: "2026-07-18T00:00:05.000Z",
      sessionConnectedAt: "2026-07-18T00:00:01.000Z",
    });
    expect(result.payload).toMatchObject({ kind: "position" });

    expect(database.positions.listCanonicalEvents(10)).toHaveLength(1);
    expect(database.aprsOutbox.list(10)).toEqual([]);
    expect(decisions).toContain("POSITION_PRECISION_INSUFFICIENT");
    expect(
      database.connection.prepare("SELECT * FROM node_position_state").all(),
    ).toEqual([]);

    await runtime.stop();
    database.close();
  });

  it("retries an orphan canonical event after an atomic outbox failure", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      {
        version: "mapping-v1",
        effectiveAt: "2026-07-18T00:00:00.000Z",
        meshNetworkId: "fixture-network",
        nodeNum: 42,
        callsign: "N0CALL-7",
      },
    ]);
    const schema = await loadMeshtasticSchema();
    const transport = new FixtureTransport();
    const events = new DomainEventBus({
      clock: () => new Date("2026-07-18T00:00:10.000Z"),
      eventIdFactory: sequentialFactory("event"),
    });
    const runtime = createRuntime(database, schema, transport, events);
    database.connection.exec(
      "CREATE TRIGGER fixture_reject_aprs_outbox BEFORE INSERT ON aprs_outbox BEGIN SELECT RAISE(ABORT, 'fixture outbox failure'); END",
    );

    expect(() =>
      runtime.ingestFrame({
        kind: "frame",
        frame: positionFrame(schema, 32),
        receivedAt: "2026-07-18T00:00:05.000Z",
        sessionConnectedAt: "2026-07-18T00:00:01.000Z",
      }),
    ).toThrow("POSITION_PERSISTENCE_FAILED");
    expect(database.aprsOutbox.list(10)).toEqual([]);
    expect(
      database.connection.prepare("SELECT * FROM node_position_state").all(),
    ).toEqual([]);

    database.connection.exec("DROP TRIGGER fixture_reject_aprs_outbox");
    const recovered = runtime.ingestFrame({
      kind: "frame",
      frame: positionFrame(schema, 32),
      receivedAt: "2026-07-18T00:00:06.000Z",
      sessionConnectedAt: "2026-07-18T00:00:01.000Z",
    });

    expect(recovered.position?.decision?.code).toBe("POSITION_ACCEPTED");
    expect(database.aprsOutbox.list(10)).toHaveLength(1);
    expect(
      database.connection.prepare("SELECT * FROM node_position_state").all(),
    ).toHaveLength(1);
    expect(database.positions.listCanonicalEvents(10)).toHaveLength(1);

    database.close();
  });

  it("records fail-closed decisions and a durable event for mapping-order conflicts", async () => {
    const database = new GatewayDatabase(":memory:");
    const schema = await loadMeshtasticSchema();
    const transport = new FixtureTransport();
    const events = new DomainEventBus({
      clock: () => new Date("2026-07-18T00:00:10.000Z"),
      eventIdFactory: sequentialFactory("conflict-event"),
    });
    const decisionCodes: string[] = [];
    const outboxFailures: Array<Record<string, unknown>> = [];
    events.subscribe((event) => {
      if (event.type === "position.decision") {
        decisionCodes.push(String(event.payload.code));
      }
      if (event.type === "aprs.outbox.failed") {
        outboxFailures.push(event.payload);
      }
    });
    const runtime = createRuntime(database, schema, transport, events);
    const ingest = (
      mappingVersion: string,
      sequence: number,
      packetId: number,
    ) => {
      database.callmeshMappings.replace([
        {
          version: mappingVersion,
          effectiveAt: "2026-07-18T00:00:00.000Z",
          meshNetworkId: "fixture-network",
          nodeNum: 42,
          callsign: "N0CALL-7",
        },
      ]);
      return runtime.ingestFrame({
        kind: "frame",
        frame: positionFrame(schema, 32, {
          sequenceNumber: sequence,
          packetId,
        }),
        receivedAt: "2026-07-18T00:00:05.000Z",
        sessionConnectedAt: "2026-07-18T00:00:01.000Z",
      });
    };

    runtime.start();
    const first = ingest("mapping-v1", 7, 100);
    const second = ingest("mapping-v2", 8, 101);
    const third = ingest("mapping-v3", 9, 102);

    expect(first.position).toMatchObject({
      decision: { code: "POSITION_ACCEPTED" },
      outboxCreated: true,
    });
    expect(second.position).toMatchObject({
      decision: { code: "APRS_SKIPPED_OUT_OF_ORDER" },
      outboxCreated: false,
    });
    expect(third.position).toMatchObject({
      decision: { code: "APRS_SKIPPED_OUT_OF_ORDER" },
      outboxCreated: false,
    });
    expect(decisionCodes).toEqual([
      "POSITION_ACCEPTED",
      "APRS_SKIPPED_OUT_OF_ORDER",
      "APRS_SKIPPED_OUT_OF_ORDER",
    ]);
    expect(outboxFailures).toEqual([
      expect.objectContaining({
        status: "failed",
        code: "APRS_ORDER_UNPROVEN",
      }),
    ]);
    expect(database.aprsOutbox.list(10)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          status: "failed",
          lastErrorCode: "APRS_ORDER_UNPROVEN",
        }),
        expect.objectContaining({ status: "queued" }),
      ]),
    );
    expect(database.aprsOutbox.list(10)).toHaveLength(2);
    expect(
      database.connection
        .prepare(
          "SELECT code, COUNT(*) AS count FROM position_decisions GROUP BY code ORDER BY code",
        )
        .all(),
    ).toEqual([
      { code: "APRS_SKIPPED_OUT_OF_ORDER", count: 2 },
      { code: "POSITION_ACCEPTED", count: 1 },
    ]);
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM node_position_state")
        .get()?.count,
    ).toBe(3);

    await runtime.stop();
    database.close();
  });

  it("waits for and disconnects a transport that connects after stop begins", async () => {
    const database = new GatewayDatabase(":memory:");
    const schema = await loadMeshtasticSchema();
    const transport = new FixtureTransport(true);
    const events = new DomainEventBus({
      eventIdFactory: sequentialFactory("lifecycle-event"),
    });
    const observed: string[] = [];
    events.subscribe((event) => observed.push(event.type));
    const runtime = createRuntime(database, schema, transport, events);

    runtime.start();
    expect(transport.connects).toBe(1);
    const stopping = runtime.stop();
    expect(transport.disconnects).toBe(1);
    transport.emit({ kind: "error", code: "LATE_TRANSPORT_ERROR" });
    transport.releaseConnect();
    await stopping;

    expect(transport.disconnects).toBe(2);
    expect(observed).not.toContain("mesh.transport.error");
    expect(runtime.status()).toMatchObject({
      connection: { status: "disconnected" },
    });
    database.close();
  });

  it("retries the same bounded teardown failure without allowing restart", async () => {
    const database = new GatewayDatabase(":memory:");
    const schema = await loadMeshtasticSchema();
    const transport = new FailingStopTransport();
    const runtime = new MeshGatewayRuntime({
      applicationDecoder: new MeshtasticApplicationDecoder(schema),
      codec: new MeshtasticProtobufCodec(schema),
      database,
      eventBus: new DomainEventBus(),
      gatewayId: "fixture-gateway",
      meshNetworkId: "fixture-network",
      transport,
      stopTimeoutMs: 25,
    });

    runtime.start();
    const firstStop = runtime.stop();
    expect(runtime.stop()).toBe(firstStop);
    await expect(settlesWithin(firstStop, 1_000)).rejects.toMatchObject({
      code: "MESH_DISCONNECT_FAILED",
    });
    expect(transport.disconnects).toBe(2);
    expect(() => runtime.start()).toThrowError(
      expect.objectContaining({ code: "MESH_RUNTIME_STOPPING" }),
    );

    const retry = runtime.stop();
    expect(runtime.stop()).toBe(retry);
    await expect(settlesWithin(retry, 1_000)).rejects.toMatchObject({
      code: "MESH_DISCONNECT_FAILED",
    });
    expect(transport.disconnects).toBe(4);
    expect(() => runtime.start()).toThrowError(
      expect.objectContaining({ code: "MESH_RUNTIME_STOPPING" }),
    );
    database.close();
  });

  it("accepts a confirmed retry after a transient disconnect failure", async () => {
    const database = new GatewayDatabase(":memory:");
    const schema = await loadMeshtasticSchema();
    const transport = new TransientStopTransport();
    const runtime = createRuntime(
      database,
      schema,
      transport,
      new DomainEventBus(),
    );

    runtime.start();
    await Promise.resolve();
    await Promise.resolve();
    await expect(runtime.stop()).resolves.toBeUndefined();

    expect(transport.disconnects).toBe(2);
    expect(runtime.status()).toMatchObject({
      connection: { status: "disconnected" },
    });
    expect(() => runtime.start()).not.toThrow();
    await Promise.resolve();
    await Promise.resolve();
    await runtime.stop();
    database.close();
  });

  it("keeps a timeout latched until a late connection is actually closed", async () => {
    const database = new GatewayDatabase(":memory:");
    const schema = await loadMeshtasticSchema();
    const transport = new LateConnectTransport();
    const events = new DomainEventBus({
      eventIdFactory: sequentialFactory("late-connect-event"),
    });
    const observed: string[] = [];
    events.subscribe((event) => observed.push(event.type));
    const runtime = new MeshGatewayRuntime({
      applicationDecoder: new MeshtasticApplicationDecoder(schema),
      codec: new MeshtasticProtobufCodec(schema),
      database,
      eventBus: events,
      gatewayId: "fixture-gateway",
      meshNetworkId: "fixture-network",
      transport,
      stopTimeoutMs: 25,
    });

    runtime.start();
    const firstStop = runtime.stop();
    await expect(settlesWithin(firstStop, 1_000)).rejects.toMatchObject({
      code: "MESH_STOP_TIMEOUT",
    });
    expect(transport.disconnects).toBe(2);
    expect(() => runtime.start()).toThrowError(
      expect.objectContaining({ code: "MESH_RUNTIME_STOPPING" }),
    );

    await transport.releaseConnect();
    expect(runtime.status()).toMatchObject({
      connection: { status: "ready" },
    });
    expect(observed).not.toContain("mesh.transport.state");
    expect(() => runtime.start()).toThrowError(
      expect.objectContaining({ code: "MESH_RUNTIME_STOPPING" }),
    );

    await expect(runtime.stop()).resolves.toBeUndefined();
    expect(transport.disconnects).toBe(4);
    expect(runtime.status()).toMatchObject({
      connection: { status: "disconnected" },
    });

    expect(() => runtime.start()).not.toThrow();
    await Promise.resolve();
    expect(transport.connects).toBe(2);
    await runtime.stop();
    database.close();
  });

  it("does not clear teardown when disconnect resolves without closing", async () => {
    const database = new GatewayDatabase(":memory:");
    const schema = await loadMeshtasticSchema();
    const transport = new UnconfirmedStopTransport();
    const runtime = createRuntime(
      database,
      schema,
      transport,
      new DomainEventBus(),
    );

    runtime.start();
    await expect(runtime.stop()).rejects.toMatchObject({
      code: "MESH_TRANSPORT_DISCONNECT_UNCONFIRMED",
    });
    expect(transport.disconnects).toBe(2);
    expect(() => runtime.start()).toThrowError(
      expect.objectContaining({ code: "MESH_RUNTIME_STOPPING" }),
    );

    transport.confirmDisconnects = true;
    await expect(runtime.stop()).resolves.toBeUndefined();
    expect(transport.disconnects).toBe(3);
    expect(runtime.status()).toMatchObject({
      connection: { status: "disconnected" },
    });
    database.close();
  });
});

async function settlesWithin<T>(
  operation: Promise<T>,
  timeoutMs: number,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("fixture operation hung")),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

function createRuntime(
  database: GatewayDatabase,
  schema: MeshtasticSchema,
  transport: MeshtasticTransport,
  events: DomainEventBus,
): MeshGatewayRuntime {
  return new MeshGatewayRuntime({
    applicationDecoder: new MeshtasticApplicationDecoder(schema),
    codec: new MeshtasticProtobufCodec(schema),
    database,
    eventBus: events,
    gatewayId: "fixture-gateway",
    meshNetworkId: "fixture-network",
    transport,
    clock: () => new Date("2026-07-18T00:00:10.000Z"),
    idFactory: sequentialFactory("observation"),
  });
}

function positionFrame(
  schema: MeshtasticSchema,
  precisionBits: number,
  options: { packetId?: number; sequenceNumber?: number } = {},
): Uint8Array {
  const payload = schema.position
    .encode({
      latitudeI: 250_475_000,
      longitudeI: 1_215_175_000,
      altitude: 12,
      timestamp: Math.floor(Date.parse("2026-07-18T00:00:04.000Z") / 1_000),
      timestampMillisAdjust: 250,
      groundSpeed: 3,
      groundTrack: 9_000,
      seqNumber: options.sequenceNumber ?? 7,
      precisionBits,
    })
    .finish();
  return schema.fromRadio
    .encode({
      packet: {
        from: 42,
        id: options.packetId ?? 100,
        rxTime: Math.floor(Date.parse("2026-07-18T00:00:05.000Z") / 1_000),
        rxSnr: 7.5,
        rxRssi: -80,
        hopLimit: 3,
        hopStart: 4,
        decoded: { portnum: schema.portNum.values.POSITION_APP, payload },
      },
    })
    .finish();
}

function sequentialFactory(prefix: string): () => string {
  let index = 0;
  return () => `${prefix}-${++index}`;
}

class FixtureTransport implements MeshtasticTransport {
  readonly kind = "simulator" as const;
  readonly metrics: TransportMetrics = {
    bytesReceived: 0,
    bytesSent: 0,
    framesReceived: 0,
    framesSent: 0,
    malformedFrames: 0,
    reconnects: 0,
  };
  state: TransportConnectionState = {
    transport: "simulator",
    status: "disconnected",
    changedAt: "2026-07-18T00:00:00.000Z",
  };
  private readonly listeners = new Set<TransportEventListener>();
  private releasePendingConnect: (() => void) | undefined;
  connects = 0;
  disconnects = 0;

  constructor(private readonly deferConnect = false) {}

  async connect(): Promise<void> {
    this.connects += 1;
    if (this.deferConnect) {
      await new Promise<void>((resolve) => {
        this.releasePendingConnect = resolve;
      });
    }
    this.state = {
      transport: "simulator",
      status: "ready",
      changedAt: "2026-07-18T00:00:01.000Z",
    };
    this.emit({ kind: "state", state: this.state });
  }

  async disconnect(): Promise<void> {
    this.disconnects += 1;
    this.state = {
      transport: "simulator",
      status: "disconnected",
      changedAt: "2026-07-18T00:00:11.000Z",
    };
  }

  async writeFrame(): Promise<void> {}

  releaseConnect(): void {
    this.releasePendingConnect?.();
  }

  subscribe(listener: TransportEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emit(event: TransportEvent): void {
    if (event.kind === "frame") {
      this.metrics.framesReceived += 1;
      this.metrics.bytesReceived += event.frame.length;
    }
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

class FailingStopTransport implements MeshtasticTransport {
  readonly kind = "simulator" as const;
  readonly metrics: TransportMetrics = {
    bytesReceived: 0,
    bytesSent: 0,
    framesReceived: 0,
    framesSent: 0,
    malformedFrames: 0,
    reconnects: 0,
  };
  readonly state: TransportConnectionState = {
    transport: "simulator",
    status: "connecting",
    changedAt: "2026-07-18T00:00:00.000Z",
  };
  disconnects = 0;

  connect(): Promise<void> {
    return new Promise(() => undefined);
  }

  disconnect(): Promise<void> {
    this.disconnects += 1;
    return Promise.reject(
      Object.assign(new Error("disconnect failed"), {
        code: "MESH_DISCONNECT_FAILED",
      }),
    );
  }

  writeFrame(): Promise<void> {
    return Promise.resolve();
  }

  subscribe(): () => void {
    return () => undefined;
  }
}

class TransientStopTransport implements MeshtasticTransport {
  readonly kind = "simulator" as const;
  readonly metrics: TransportMetrics = {
    bytesReceived: 0,
    bytesSent: 0,
    framesReceived: 0,
    framesSent: 0,
    malformedFrames: 0,
    reconnects: 0,
  };
  state: TransportConnectionState = {
    transport: "simulator",
    status: "disconnected",
    changedAt: "2026-07-18T00:00:00.000Z",
  };
  disconnects = 0;

  connect(): Promise<void> {
    this.state = {
      transport: "simulator",
      status: "ready",
      changedAt: "2026-07-18T00:00:01.000Z",
    };
    return Promise.resolve();
  }

  disconnect(): Promise<void> {
    this.disconnects += 1;
    if (this.disconnects === 1) {
      return Promise.reject(
        Object.assign(new Error("transient disconnect failure"), {
          code: "SERIAL_DISCONNECT_PENDING_OPEN",
        }),
      );
    }
    this.state = {
      transport: "simulator",
      status: "disconnected",
      changedAt: "2026-07-18T00:00:11.000Z",
    };
    return Promise.resolve();
  }

  writeFrame(): Promise<void> {
    return Promise.resolve();
  }

  subscribe(): () => void {
    return () => undefined;
  }
}

class LateConnectTransport implements MeshtasticTransport {
  readonly kind = "simulator" as const;
  readonly metrics: TransportMetrics = {
    bytesReceived: 0,
    bytesSent: 0,
    framesReceived: 0,
    framesSent: 0,
    malformedFrames: 0,
    reconnects: 0,
  };
  state: TransportConnectionState = {
    transport: "simulator",
    status: "connecting",
    changedAt: "2026-07-18T00:00:00.000Z",
  };
  private releasePendingConnect = (): void => undefined;
  private readonly firstConnectGate = new Promise<void>((resolve) => {
    this.releasePendingConnect = resolve;
  });
  private completeFirstConnect = (): void => undefined;
  private readonly firstConnectCompleted = new Promise<void>((resolve) => {
    this.completeFirstConnect = resolve;
  });
  private readonly listeners = new Set<TransportEventListener>();
  connects = 0;
  disconnects = 0;

  async connect(): Promise<void> {
    this.connects += 1;
    if (this.connects === 1) {
      await this.firstConnectGate;
    }
    this.state = {
      transport: "simulator",
      status: "ready",
      changedAt: "2026-07-18T00:00:01.000Z",
    };
    for (const listener of this.listeners) {
      listener({ kind: "state", state: this.state });
    }
    if (this.connects === 1) {
      this.completeFirstConnect();
    }
  }

  async disconnect(): Promise<void> {
    this.disconnects += 1;
    this.state = {
      transport: "simulator",
      status: "disconnected",
      changedAt: "2026-07-18T00:00:11.000Z",
    };
  }

  writeFrame(): Promise<void> {
    return Promise.resolve();
  }

  subscribe(listener: TransportEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async releaseConnect(): Promise<void> {
    this.releasePendingConnect();
    await this.firstConnectCompleted;
  }
}

class UnconfirmedStopTransport implements MeshtasticTransport {
  readonly kind = "simulator" as const;
  readonly metrics: TransportMetrics = {
    bytesReceived: 0,
    bytesSent: 0,
    framesReceived: 0,
    framesSent: 0,
    malformedFrames: 0,
    reconnects: 0,
  };
  state: TransportConnectionState = {
    transport: "simulator",
    status: "connecting",
    changedAt: "2026-07-18T00:00:00.000Z",
  };
  confirmDisconnects = false;
  disconnects = 0;

  connect(): Promise<void> {
    this.state = {
      transport: "simulator",
      status: "ready",
      changedAt: "2026-07-18T00:00:01.000Z",
    };
    return Promise.resolve();
  }

  disconnect(): Promise<void> {
    this.disconnects += 1;
    if (this.confirmDisconnects) {
      this.state = {
        transport: "simulator",
        status: "disconnected",
        changedAt: "2026-07-18T00:00:11.000Z",
      };
    }
    return Promise.resolve();
  }

  writeFrame(): Promise<void> {
    return Promise.resolve();
  }

  subscribe(): () => void {
    return () => undefined;
  }
}
