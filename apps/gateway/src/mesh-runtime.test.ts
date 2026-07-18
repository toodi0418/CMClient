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
        sequenceEpoch: 0,
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
});

function createRuntime(
  database: GatewayDatabase,
  schema: MeshtasticSchema,
  transport: FixtureTransport,
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
      seqNumber: 7,
      precisionBits,
    })
    .finish();
  return schema.fromRadio
    .encode({
      packet: {
        from: 42,
        id: 100,
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

  async connect(): Promise<void> {
    this.state = {
      transport: "simulator",
      status: "ready",
      changedAt: "2026-07-18T00:00:01.000Z",
    };
    this.emit({ kind: "state", state: this.state });
  }

  async disconnect(): Promise<void> {
    this.state = {
      transport: "simulator",
      status: "disconnected",
      changedAt: "2026-07-18T00:00:11.000Z",
    };
  }

  async writeFrame(): Promise<void> {}

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
