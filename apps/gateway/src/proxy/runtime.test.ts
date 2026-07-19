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

describe("proxy runtime", { timeout: 20_000 }, () => {
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
    await expect(runtime.stop()).resolves.toBeUndefined();
    expect(transport.disconnects).toBe(1);
  });

  it("pauses one client while a coalesced burst is serialized upstream", async () => {
    const schema = await loadMeshtasticSchema();
    const transport = new FakeTransport(true);
    const events = new DomainEventBus();
    const runtime = new ProxyRuntime({
      eventBus: events,
      listenPort: 0,
      policy: new ProxyAccessController(schema, {
        maxWritesPerMinute: 3_600,
        mode: "full",
      }),
      schema,
      upstream: new ProxyUpstreamManager(
        transport,
        new ProxyConfigCache(schema),
      ),
    });

    await runtime.start();
    const client = await connect(runtime.status().listener.port);
    const codec = new ProxyFrameCodec();
    const payload = new Uint8Array(
      schema.toRadio.encode({ heartbeat: {} }).finish(),
    );
    const burst = Buffer.concat(
      Array.from({ length: 256 }, () => Buffer.from(codec.encode(payload))),
    );
    try {
      client.write(burst);
      await waitFor(() => transport.writes.length === 1);
      expect(runtime.outbound.snapshot).toEqual({
        pendingCorrelations: 0,
        queuedWrites: 0,
        writing: true,
      });

      transport.releaseFirstWrite();
      await waitFor(() => transport.writes.length === 256);
      expect(runtime.status().policy.activeClients).toBe(1);
      expect(
        events
          .replayAfter("missing")
          .some((event) => event.type === "proxy.backpressure"),
      ).toBe(false);
    } finally {
      transport.releaseFirstWrite();
      client.destroy();
      await runtime.stop();
    }
  });

  it("closes and reports only clients that exceed the global outbound queue", async () => {
    const schema = await loadMeshtasticSchema();
    const transport = new FakeTransport(true);
    const events = new DomainEventBus();
    const runtime = new ProxyRuntime({
      eventBus: events,
      listenPort: 0,
      policy: new ProxyAccessController(schema, {
        maxClients: 160,
        mode: "full",
      }),
      schema,
      upstream: new ProxyUpstreamManager(
        transport,
        new ProxyConfigCache(schema),
      ),
    });

    await runtime.start();
    const clients = await Promise.all(
      Array.from({ length: 130 }, () =>
        connect(runtime.status().listener.port),
      ),
    );
    for (const client of clients) {
      client.on("error", () => undefined);
    }
    const payload = new Uint8Array(
      schema.toRadio.encode({ heartbeat: {} }).finish(),
    );
    const frame = new ProxyFrameCodec().encode(payload);
    try {
      await waitFor(() => runtime.status().policy.activeClients === 130);
      for (const client of clients) {
        client.write(frame);
      }

      await waitFor(() => runtime.status().policy.activeClients === 128);
      expect(runtime.outbound.snapshot).toEqual({
        pendingCorrelations: 0,
        queuedWrites: 127,
        writing: true,
      });
      expect(runtime.status().lastErrorCode).toBe("PROXY_OUTBOUND_QUEUE_FULL");
      expect(events.replayAfter("missing")).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            type: "proxy.backpressure",
            payload: { code: "PROXY_OUTBOUND_QUEUE_FULL" },
          }),
        ]),
      );

      transport.releaseFirstWrite();
      await waitFor(() => transport.writes.length === 128);
    } finally {
      transport.releaseFirstWrite();
      for (const client of clients) {
        client.destroy();
      }
      await runtime.stop();
    }
  }, 20_000);

  it("invalidates and cleans up a start that resolves after stop begins", async () => {
    const schema = await loadMeshtasticSchema();
    const transport = new FakeTransport(false, true);
    const runtime = new ProxyRuntime({
      listenPort: 0,
      policy: new ProxyAccessController(schema, { mode: "full" }),
      schema,
      upstream: new ProxyUpstreamManager(
        transport,
        new ProxyConfigCache(schema),
      ),
    });

    const starting = runtime.start();
    await waitFor(() => transport.connects === 1);
    const startResult = expect(starting).rejects.toMatchObject({
      code: "PROXY_RUNTIME_STOPPED",
    });
    const stopping = runtime.stop();
    await waitFor(() => transport.disconnects >= 1);
    transport.releaseConnect();

    await startResult;
    await stopping;
    expect(runtime.status()).toMatchObject({
      state: "stopped",
      listener: { port: 0 },
      policy: { activeClients: 0 },
    });
    expect(transport.disconnects).toBeGreaterThanOrEqual(2);
    await expect(runtime.start()).rejects.toMatchObject({
      code: "PROXY_RUNTIME_STOPPED",
    });
  });

  it("bounds stop when upstream disconnect fails and start never settles", async () => {
    const schema = await loadMeshtasticSchema();
    const transport = new FailingStopTransport();
    const runtime = new ProxyRuntime({
      listenPort: 0,
      policy: new ProxyAccessController(schema, { mode: "full" }),
      schema,
      stopTimeoutMs: 25,
      upstream: new ProxyUpstreamManager(
        transport,
        new ProxyConfigCache(schema),
      ),
    });

    void runtime.start().catch(() => undefined);
    await waitFor(() => transport.connects === 1);
    await expect(
      Promise.race([
        runtime.stop(),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("fixture stop hung")), 1_000),
        ),
      ]),
    ).rejects.toMatchObject({ code: "PROXY_DISCONNECT_FAILED" });
    expect(runtime.status().state).toBe("stopped");
    expect(transport.disconnects).toBe(2);
    await expect(runtime.stop()).resolves.toBeUndefined();
    expect(transport.disconnects).toBe(2);
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
  private releaseWrite: (() => void) | undefined;
  private releasePendingConnect: (() => void) | undefined;

  constructor(
    private readonly blockFirstWrite = false,
    private readonly deferConnect = false,
  ) {}

  async connect(): Promise<void> {
    this.connects += 1;
    if (this.deferConnect) {
      await new Promise<void>((resolve) => {
        this.releasePendingConnect = resolve;
      });
    }
    this.state = { ...this.state, status: "ready" };
    this.emit({ kind: "state", state: this.state });
  }

  async disconnect(): Promise<void> {
    this.disconnects += 1;
    this.state = { ...this.state, status: "disconnected" };
  }

  async writeFrame(frame: Uint8Array): Promise<void> {
    this.writes.push(new Uint8Array(frame));
    if (this.blockFirstWrite && this.writes.length === 1) {
      await new Promise<void>((resolve) => {
        this.releaseWrite = resolve;
      });
    }
  }

  releaseFirstWrite(): void {
    this.releaseWrite?.();
  }

  releaseConnect(): void {
    this.releasePendingConnect?.();
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
    changedAt: timestamp,
    status: "connecting",
    transport: "simulator",
  };
  connects = 0;
  disconnects = 0;

  connect(): Promise<void> {
    this.connects += 1;
    return new Promise(() => undefined);
  }

  disconnect(): Promise<void> {
    this.disconnects += 1;
    return Promise.reject(
      Object.assign(new Error("disconnect failed"), {
        code: "PROXY_DISCONNECT_FAILED",
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
  const deadline = Date.now() + 8_000;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error("proxy runtime fixture did not settle");
}
