import net, { type Socket } from "node:net";

import { describe, expect, it } from "vitest";

import type {
  TransportConnectionState,
  TransportMetrics,
} from "@cmclient/contracts";

import { DomainEventBus } from "../events";
import { loadMeshtasticSchema } from "../protobuf/schema";
import type {
  MeshtasticTransport,
  TransportEvent,
  TransportEventListener,
} from "../transport/types";
import { ProxyAccessController } from "./policy";
import { ProxyRuntime } from "./runtime";
import {
  ProxyConfigCache,
  ProxyFrameCodec,
  ProxyUpstreamManager,
} from "./upstream";

const timestamp = "2026-07-18T00:00:00.000Z";

describe("proxy runtime", () => {
  it("routes framed multi-client traffic through policy, writer, reply router, and broadcast hub", async () => {
    const schema = await loadMeshtasticSchema();
    const transport = new FakeTransport();
    let eventSequence = 0;
    const events = new DomainEventBus({
      eventIdFactory: () => `event-${++eventSequence}`,
    });
    const runtime = new ProxyRuntime({
      eventBus: events,
      listenPort: 0,
      policy: new ProxyAccessController(schema, { mode: "full" }),
      schema,
      upstream: new ProxyUpstreamManager(
        transport,
        new ProxyConfigCache(schema),
      ),
    });

    await runtime.start();
    const port = runtime.status().listener.port;
    const clientA = await connect(port);
    const clientB = await connect(port);
    const readerA = new FrameReader(clientA);
    const readerB = new FrameReader(clientB);
    const codec = new ProxyFrameCodec();
    try {
      const request = new Uint8Array(
        schema.toRadio
          .encode({
            packet: { id: 77, decoded: { wantResponse: true } },
          })
          .finish(),
      );
      clientA.write(codec.encode(request));
      await waitFor(() => transport.writes.length === 1);
      expect(transport.writes).toEqual([request]);

      const reply = new Uint8Array(
        schema.fromRadio
          .encode({ packet: { decoded: { replyId: 77 } } })
          .finish(),
      );
      transport.emitFrame(reply);
      await readerA.waitForFrames(1);
      expect(readerA.frames).toEqual([reply]);
      await new Promise((resolve) => setTimeout(resolve, 5));
      expect(readerB.frames).toEqual([]);

      const broadcast = new Uint8Array(
        schema.fromRadio.encode({ packet: { from: 2, to: 1 } }).finish(),
      );
      transport.emitFrame(broadcast);
      await Promise.all([readerA.waitForFrames(2), readerB.waitForFrames(1)]);
      expect(readerA.frames).toEqual([reply, broadcast]);
      expect(readerB.frames).toEqual([broadcast]);
      expect(runtime.status()).toMatchObject({
        state: "running",
        policy: { activeClients: 2, mode: "full" },
        queue: {
          broadcastFrames: 1,
          directAccepted: 1,
          pendingCorrelations: 0,
        },
      });
      expect(events.replayAfter("missing").map((event) => event.type)).toEqual(
        expect.arrayContaining([
          "proxy.started",
          "proxy.client",
          "proxy.queue",
        ]),
      );
      transport.emitState("backoff");
      expect(runtime.status().state).toBe("degraded");
      transport.emitState("ready");
      expect(runtime.status().state).toBe("running");
    } finally {
      clientA.destroy();
      clientB.destroy();
      await runtime.stop();
    }
    expect(transport.disconnects).toBe(1);
  });
});

class FakeTransport implements MeshtasticTransport {
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
    changedAt: timestamp,
    status: "disconnected",
    transport: "simulator",
  };
  readonly writes: Uint8Array[] = [];
  connects = 0;
  disconnects = 0;
  private readonly listeners = new Set<TransportEventListener>();

  async connect(): Promise<void> {
    this.connects += 1;
    this.state = { ...this.state, status: "ready" };
    this.emit({ kind: "state", state: this.state });
  }

  async disconnect(): Promise<void> {
    this.disconnects += 1;
    this.state = { ...this.state, status: "disconnected" };
  }

  async writeFrame(frame: Uint8Array): Promise<void> {
    this.writes.push(new Uint8Array(frame));
  }

  subscribe(listener: TransportEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emitFrame(frame: Uint8Array): void {
    this.emit({ kind: "frame", frame, receivedAt: timestamp });
  }

  emitState(status: TransportConnectionState["status"]): void {
    this.state = { ...this.state, status };
    this.emit({ kind: "state", state: this.state });
  }

  private emit(event: TransportEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

class FrameReader {
  readonly frames: Uint8Array[] = [];
  private readonly codec = new ProxyFrameCodec();

  constructor(socket: Socket) {
    socket.on("data", (chunk: Buffer) => {
      this.frames.push(...this.codec.decode(chunk));
    });
  }

  async waitForFrames(expected: number): Promise<void> {
    await waitFor(() => this.frames.length >= expected);
  }
}

function connect(port: number): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: "127.0.0.1", port });
    socket.once("connect", () => resolve(socket));
    socket.once("error", reject);
  });
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("proxy runtime fixture did not settle");
}
