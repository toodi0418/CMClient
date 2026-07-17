import { once } from "node:events";
import net, { type Server, type Socket } from "node:net";

import { describe, expect, it } from "vitest";

import { ReconnectBackoff } from "./backoff";
import { ConfigSession } from "./config-session";
import { MeshtasticFrameDecoder, encodeMeshtasticFrame } from "./framing";
import { TcpMeshtasticTransport } from "./tcp";

const codec = {
  encodeWantConfig(nonce: number): Uint8Array {
    return new Uint8Array([1, ...uint32(nonce)]);
  },
  isConfigComplete(payload: Uint8Array, nonce: number): boolean {
    return payload[0] === 2 && equals(payload.slice(1), uint32(nonce));
  },
};

describe("Meshtastic TCP framing", () => {
  it("decodes fragmented and coalesced frames while resynchronizing malformed input", () => {
    const decoder = new MeshtasticFrameDecoder({ maxPayloadBytes: 8 });
    const first = encodeMeshtasticFrame(new Uint8Array([1, 2]));
    const second = encodeMeshtasticFrame(new Uint8Array([3]));

    expect(decoder.push(first.slice(0, 3))).toEqual([]);
    expect(decoder.push(concat(first.slice(3), second))).toEqual([
      new Uint8Array([1, 2]),
      new Uint8Array([3]),
    ]);
    expect(decoder.push(new Uint8Array([0, 0x94, 0xc3, 0, 9, 0xff]))).toEqual(
      [],
    );
    expect(decoder.metrics.malformedFrames).toBeGreaterThan(0);
    expect(() => encodeMeshtasticFrame(new Uint8Array())).toThrow();
  });
});

describe("ConfigSession and ReconnectBackoff", () => {
  it("only accepts a config completion that matches the active nonce", () => {
    const session = new ConfigSession(codec, () => 0x0102_0304);
    expect(session.begin()).toEqual(new Uint8Array([1, 1, 2, 3, 4]));
    expect(session.observe(new Uint8Array([2, 0, 0, 0, 1]))).toBe(false);
    expect(session.observe(new Uint8Array([2, 1, 2, 3, 4]))).toBe(true);
    session.reset();
    expect(session.observe(new Uint8Array([2, 1, 2, 3, 4]))).toBe(false);
  });

  it("bounds exponential reconnect delay and applies deterministic jitter", () => {
    const backoff = new ReconnectBackoff({
      initialDelayMs: 100,
      maximumDelayMs: 800,
      jitterRatio: 0.2,
    });
    expect(backoff.delayForAttempt(1, () => 0)).toBe(80);
    expect(backoff.delayForAttempt(1, () => 1)).toBe(120);
    expect(backoff.delayForAttempt(10, () => 1)).toBe(800);
  });
});

describe("TcpMeshtasticTransport", () => {
  it("runs config session over fragmented TCP frames before accepting writes", async () => {
    let observedPayload: Uint8Array | undefined;
    let observePayload: (() => void) | undefined;
    const payloadObserved = new Promise<void>((resolve) => {
      observePayload = resolve;
    });
    const server = net.createServer((socket) => {
      wireConfigServer(socket, (payload) => {
        observedPayload = payload;
        observePayload?.();
      });
    });
    const port = await listen(server);
    const transport = new TcpMeshtasticTransport({
      host: "127.0.0.1",
      port,
      configSession: codec,
      random: () => 0,
    });
    const events: string[] = [];
    const sessionConnectedAt: string[] = [];
    transport.subscribe((event) => {
      events.push(event.kind);
      if (event.kind === "frame" && event.sessionConnectedAt) {
        sessionConnectedAt.push(event.sessionConnectedAt);
      }
    });

    await transport.connect();
    expect(transport.state.status).toBe("ready");
    await transport.writeFrame(new Uint8Array([99]));
    await payloadObserved;
    expect(observedPayload).toEqual(new Uint8Array([99]));
    expect(events).toContain("frame");
    expect(sessionConnectedAt).toHaveLength(1);
    expect(sessionConnectedAt[0]).toMatch(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
    );
    expect(transport.metrics.framesSent).toBe(2);

    await transport.disconnect();
    await close(server);
  });

  it("backs off after a refused connection and becomes ready when TCP returns", async () => {
    const reservation = net.createServer();
    const port = await listen(reservation);
    await close(reservation);
    const transport = new TcpMeshtasticTransport({
      host: "127.0.0.1",
      port,
      configSession: codec,
      reconnect: { initialDelayMs: 100, maximumDelayMs: 100, jitterRatio: 0 },
      random: () => 0,
    });
    const backedOff = new Promise<void>((resolve) => {
      const unsubscribe = transport.subscribe((event) => {
        if (event.kind === "state" && event.state.status === "backoff") {
          unsubscribe();
          resolve();
        }
      });
    });
    const connecting = transport.connect();
    await backedOff;
    const server = net.createServer((socket) => wireConfigServer(socket));
    await listen(server, port);

    await connecting;
    expect(transport.state.status).toBe("ready");
    expect(transport.metrics.reconnects).toBe(1);

    await transport.disconnect();
    await close(server);
  });

  it("fails a stalled config session into bounded backoff", async () => {
    const sockets = new Set<Socket>();
    const server = net.createServer((socket) => {
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
    });
    const port = await listen(server);
    const transport = new TcpMeshtasticTransport({
      host: "127.0.0.1",
      port,
      configSession: codec,
      configTimeoutMs: 10,
      reconnect: { initialDelayMs: 100, maximumDelayMs: 100, jitterRatio: 0 },
      random: () => 0,
    });
    const errors: string[] = [];
    const backedOff = new Promise<void>((resolve) => {
      transport.subscribe((event) => {
        if (event.kind === "error") {
          errors.push(event.code);
        }
        if (event.kind === "state" && event.state.status === "backoff") {
          resolve();
        }
      });
    });
    const connecting = transport.connect().catch((error: unknown) => error);

    await backedOff;
    expect(transport.state).toMatchObject({
      status: "backoff",
      attempt: 1,
      reasonCode: "TCP_CONFIG_TIMEOUT",
    });
    expect(errors).toContain("TCP_CONFIG_TIMEOUT");
    await transport.disconnect();
    await expect(connecting).resolves.toMatchObject({
      code: "TRANSPORT_DISCONNECTED",
    });
    for (const socket of sockets) {
      socket.destroy();
    }
    await close(server);
  });
});

function wireConfigServer(
  socket: Socket,
  onPayload?: (payload: Uint8Array) => void,
): void {
  const decoder = new MeshtasticFrameDecoder();
  socket.on("data", (chunk: Buffer) => {
    for (const payload of decoder.push(chunk)) {
      if (payload[0] === 1) {
        const completion = encodeMeshtasticFrame(
          new Uint8Array([2, ...payload.slice(1)]),
        );
        socket.write(completion.slice(0, 2));
        socket.write(completion.slice(2));
      } else {
        onPayload?.(payload);
      }
    }
  });
}

async function listen(server: Server, port = 0): Promise<number> {
  server.listen({ host: "127.0.0.1", port });
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("TCP fixture did not bind");
  }
  return address.port;
}

async function close(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

function uint32(value: number): Uint8Array {
  return new Uint8Array([
    (value >>> 24) & 0xff,
    (value >>> 16) & 0xff,
    (value >>> 8) & 0xff,
    value & 0xff,
  ]);
}

function concat(left: Uint8Array, right: Uint8Array): Uint8Array {
  const value = new Uint8Array(left.length + right.length);
  value.set(left);
  value.set(right, left.length);
  return value;
}

function equals(left: Uint8Array, right: Uint8Array): boolean {
  return (
    left.length === right.length &&
    left.every((value, index) => value === right[index])
  );
}
