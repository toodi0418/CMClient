import { once } from "node:events";
import net, { type Server, type Socket } from "node:net";

import { describe, expect, it } from "vitest";

import type {
  TransportConnectionState,
  TransportMetrics,
} from "@cmclient/contracts";

import { MeshtasticProtobufCodec } from "../protobuf/protobuf";
import { loadMeshtasticSchema } from "../protobuf/schema";
import {
  MeshtasticFrameDecoder,
  encodeMeshtasticFrame,
} from "../transport/framing";
import { TcpMeshtasticTransport } from "../transport/tcp";
import type {
  MeshtasticTransport,
  TransportEvent,
  TransportEventListener,
} from "../transport/types";
import {
  ProxyConfigCache,
  ProxyConfigCacheError,
  ProxyFrameCodec,
  ProxyUpstreamManager,
} from "./upstream";

const timestamp = "2026-07-18T00:00:00.000Z";

describe("proxy frame codec", () => {
  it("owns bounded Meshtastic framing for fragmented client and upstream chunks", () => {
    const codec = new ProxyFrameCodec({ maxPayloadBytes: 8 });
    const first = codec.encode(new Uint8Array([1, 2]));
    const second = codec.encode(new Uint8Array([3]));

    expect(codec.decode(first.slice(0, 3))).toEqual([]);
    expect(codec.decode(join(first.slice(3), second))).toEqual([
      new Uint8Array([1, 2]),
      new Uint8Array([3]),
    ]);
    expect(() => codec.encode(new Uint8Array(9))).toThrow();
  });
});

describe("proxy config cache", () => {
  it("retains only cacheable FromRadio configuration fragments with bounded identity", async () => {
    const schema = await loadMeshtasticSchema();
    const cache = new ProxyConfigCache(schema, { maxEntries: 2 });
    const myInfo = schema.fromRadio.encode({ myInfo: {} }).finish();
    const config = schema.fromRadio.encode({ config: {} }).finish();
    const packet = schema.fromRadio
      .encode({ packet: { from: 42, to: 1 } })
      .finish();

    expect(cache.observe(myInfo, timestamp)).toBe(true);
    expect(cache.observe(config, timestamp)).toBe(true);
    expect(cache.observe(packet, timestamp)).toBe(false);
    expect(cache.snapshot().map((entry) => entry.kind)).toEqual([
      "config",
      "my_info",
    ]);
    expect(() =>
      cache.observe(
        schema.fromRadio.encode({ metadata: {} }).finish(),
        timestamp,
      ),
    ).toThrow(ProxyConfigCacheError);
    cache.clear();
    expect(cache.snapshot()).toEqual([]);
  });
});

describe("proxy upstream manager", () => {
  it("caches config fragments from a real loopback upstream session", async () => {
    const schema = await loadMeshtasticSchema();
    const codec = new MeshtasticProtobufCodec(schema);
    const server = net.createServer((socket) =>
      serveConfigSession(socket, schema),
    );
    const port = await listen(server);
    const manager = new ProxyUpstreamManager(
      new TcpMeshtasticTransport({
        host: "127.0.0.1",
        port,
        configSession: codec,
        random: () => 0,
      }),
      new ProxyConfigCache(schema),
    );

    await manager.start();

    expect(manager.snapshot.state.status).toBe("ready");
    expect(manager.configCache.snapshot()).toMatchObject([
      { kind: "my_info", receivedAt: expect.any(String) },
    ]);
    await manager.stop();
    await close(server);
  });

  it("owns exactly one upstream transport and clears stale config on a new session", async () => {
    const schema = await loadMeshtasticSchema();
    const transport = new FakeTransport();
    const manager = new ProxyUpstreamManager(
      transport,
      new ProxyConfigCache(schema),
    );
    const events: string[] = [];
    manager.subscribe((event) => events.push(event.kind));

    await manager.start();
    await manager.start();
    expect(transport.connects).toBe(1);
    transport.emitState("configuring");
    transport.emitFrame(schema.fromRadio.encode({ myInfo: {} }).finish());
    expect(manager.snapshot.configFrameCount).toBe(1);

    transport.emitState("configuring");
    expect(manager.snapshot.configFrameCount).toBe(0);
    transport.emitError("TCP_SOCKET_ERROR");
    expect(manager.snapshot.lastErrorCode).toBe("TCP_SOCKET_ERROR");
    expect(events).toEqual(["state", "frame", "state", "error"]);

    await manager.stop();
    expect(transport.disconnects).toBe(1);
    expect(manager.snapshot.configFrameCount).toBe(0);
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
    transport: "simulator",
    status: "disconnected",
    changedAt: timestamp,
  };
  connects = 0;
  disconnects = 0;
  private readonly listeners = new Set<TransportEventListener>();

  async connect(): Promise<void> {
    this.connects += 1;
  }

  async disconnect(): Promise<void> {
    this.disconnects += 1;
  }

  async writeFrame(): Promise<void> {
    return undefined;
  }

  subscribe(listener: TransportEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emitState(status: TransportConnectionState["status"]): void {
    this.state = { transport: "simulator", status, changedAt: timestamp };
    this.emit({ kind: "state", state: this.state });
  }

  emitFrame(frame: Uint8Array): void {
    this.emit({ kind: "frame", frame, receivedAt: timestamp });
  }

  emitError(code: string): void {
    this.emit({ kind: "error", code });
  }

  private emit(event: TransportEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

function join(left: Uint8Array, right: Uint8Array): Uint8Array {
  const result = new Uint8Array(left.length + right.length);
  result.set(left);
  result.set(right, left.length);
  return result;
}

function serveConfigSession(
  socket: Socket,
  schema: Awaited<ReturnType<typeof loadMeshtasticSchema>>,
): void {
  const decoder = new MeshtasticFrameDecoder();
  socket.on("data", (chunk: Buffer) => {
    for (const frame of decoder.push(chunk)) {
      const request = schema.toRadio.toObject(schema.toRadio.decode(frame));
      const nonce = Number(request.wantConfigId);
      if (!Number.isInteger(nonce) || nonce < 1) {
        continue;
      }
      socket.write(
        encodeMeshtasticFrame(schema.fromRadio.encode({ myInfo: {} }).finish()),
      );
      socket.write(
        encodeMeshtasticFrame(
          schema.fromRadio.encode({ configCompleteId: nonce }).finish(),
        ),
      );
    }
  });
}

async function listen(server: Server): Promise<number> {
  server.listen({ host: "127.0.0.1", port: 0 });
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("proxy upstream fixture did not bind");
  }
  return address.port;
}

async function close(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}
