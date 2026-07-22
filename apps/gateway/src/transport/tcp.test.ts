import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import net, { type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { MeshtasticProtobufCodec } from "../protobuf/protobuf";
import { loadMeshtasticSchema } from "../protobuf/schema";
import { ReconnectBackoff } from "./backoff";
import { ConfigSession } from "./config-session";
import { MeshtasticFrameDecoder, encodeMeshtasticFrame } from "./framing";
import { PhysicalWriteGuard } from "./physical-guard";
import { TcpMeshtasticTransport } from "./tcp";

const codec = {
  encodeWantConfig(nonce: number): Uint8Array {
    return new Uint8Array([1, ...uint32(nonce)]);
  },
  isConfigComplete(payload: Uint8Array, nonce: number): boolean {
    return payload[0] === 2 && equals(payload.slice(1), uint32(nonce));
  },
};

const physicalCodec = {
  encodeWantConfig(nonce: number): Uint8Array {
    return wantConfig(nonce);
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

  it("resynchronizes a large malformed chunk in linear time with a bounded tail", () => {
    const decoder = new MeshtasticFrameDecoder({ maxPayloadBytes: 512 });
    const garbage = new Uint8Array(512 * 1_024).fill(0x55);
    const expected = new Uint8Array([1, 2, 3]);
    const input = concat(garbage, encodeMeshtasticFrame(expected));

    expect(decoder.push(input)).toEqual([expected]);

    expect(decoder.metrics).toEqual({
      bufferedBytes: 0,
      copiedBytes: input.length,
      copyOperations: 2,
      discardedBytes: garbage.length,
      malformedFrames: garbage.length,
      scanSteps: garbage.length + 1,
    });

    expect(decoder.push(garbage)).toEqual([]);
    expect(decoder.metrics.bufferedBytes).toBeLessThanOrEqual(515);
    expect(decoder.push(encodeMeshtasticFrame(expected))).toEqual([expected]);
    expect(decoder.metrics.bufferedBytes).toBe(0);
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

  it("locks the physical reconnect schedule to 5, 30, then 120 seconds", () => {
    const backoff = new ReconnectBackoff({
      fixedDelaysMs: [5_000, 30_000, 120_000],
    });
    expect(
      [1, 2, 3, 4].map((attempt) => backoff.delayForAttempt(attempt)),
    ).toEqual([5_000, 30_000, 120_000, 120_000]);
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

  it("does not carry a truncated decoder tail into the next TCP session", async () => {
    let connections = 0;
    const server = net.createServer((socket) => {
      connections += 1;
      const generation = connections;
      const decoder = new MeshtasticFrameDecoder();
      socket.on("data", (chunk: Buffer) => {
        for (const payload of decoder.push(chunk)) {
          if (payload[0] !== 1) {
            continue;
          }
          const completion = encodeMeshtasticFrame(
            new Uint8Array([2, ...payload.slice(1)]),
          );
          if (generation === 1) {
            socket.end(completion.slice(0, 3));
          } else {
            socket.write(completion);
          }
        }
      });
    });
    const port = await listen(server);
    const transport = new TcpMeshtasticTransport({
      host: "127.0.0.1",
      port,
      configSession: codec,
      reconnect: { initialDelayMs: 1, maximumDelayMs: 1, jitterRatio: 0 },
      random: () => 0,
    });

    await transport.connect();
    expect(connections).toBe(2);
    expect(transport.state.status).toBe("ready");

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

  it("times out a blackholed connect and can stop without a late backoff", async () => {
    const socket = new net.Socket();
    const transport = new TcpMeshtasticTransport(
      {
        host: "192.0.2.1",
        port: 4403,
        configSession: codec,
        connectTimeoutMs: 10,
        reconnect: {
          initialDelayMs: 1_000,
          maximumDelayMs: 1_000,
          jitterRatio: 0,
        },
        random: () => 0,
      },
      () => socket,
    );
    const errors: string[] = [];
    transport.subscribe((event) => {
      if (event.kind === "error") {
        errors.push(event.code);
      }
    });
    const connecting = transport.connect().catch((error: unknown) => error);

    await waitFor(() => transport.state.status === "backoff");
    expect(transport.state).toMatchObject({
      status: "backoff",
      attempt: 1,
      reasonCode: "TCP_CONNECT_TIMEOUT",
    });
    expect(errors).toEqual(["TCP_CONNECT_TIMEOUT"]);
    expect(socket.destroyed).toBe(true);

    await transport.disconnect();
    await expect(connecting).resolves.toMatchObject({
      code: "TRANSPORT_DISCONNECTED",
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(transport.state.status).toBe("disconnected");
    expect(transport.metrics.reconnects).toBe(1);
  });

  it("plateaus socket resources across repeated connect timeouts", async () => {
    let activeSockets = 0;
    let maximumActiveSockets = 0;
    let openAttempts = 0;
    const transport = new TcpMeshtasticTransport(
      {
        host: "192.0.2.1",
        port: 4403,
        configSession: codec,
        connectTimeoutMs: 5,
        reconnect: {
          initialDelayMs: 1,
          maximumDelayMs: 1,
          jitterRatio: 0,
        },
        random: () => 0,
      },
      () => {
        openAttempts += 1;
        activeSockets += 1;
        maximumActiveSockets = Math.max(maximumActiveSockets, activeSockets);
        const socket = new net.Socket();
        socket.once("close", () => {
          activeSockets -= 1;
        });
        return socket;
      },
    );
    const connecting = transport.connect().catch((error: unknown) => error);

    await waitFor(() => openAttempts >= 5 && activeSockets === 1);
    expect(maximumActiveSockets).toBe(1);
    await transport.disconnect();
    await waitFor(() => activeSockets === 0);
    const stoppedAttempts = openAttempts;
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(openAttempts).toBe(stoppedAttempts);
    expect(transport.state.status).toBe("disconnected");
    await expect(connecting).resolves.toMatchObject({
      code: "TRANSPORT_DISCONNECTED",
    });
  });

  it("rejects transport deadlines above the bounded maximum", () => {
    expect(
      () =>
        new TcpMeshtasticTransport({
          host: "127.0.0.1",
          port: 4403,
          configSession: codec,
          connectTimeoutMs: 120_001,
        }),
    ).toThrowError(/TCP_CONFIGURATION_INVALID/);
  });

  it("uses the product TCP path for one physical config write and fences every later ToRadio frame", async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-physical-tcp-"));
    const schema = await loadMeshtasticSchema();
    const observed: Uint8Array[] = [];
    const server = net.createServer((socket) => {
      const decoder = new MeshtasticFrameDecoder();
      socket.on("data", (chunk: Buffer) => {
        for (const payload of decoder.push(chunk)) {
          observed.push(payload);
          const request = schema.toRadio.toObject(
            schema.toRadio.decode(payload),
            { oneofs: true },
          ) as { wantConfigId?: number };
          if (request.wantConfigId !== undefined) {
            socket.write(
              encodeMeshtasticFrame(
                schema.fromRadio
                  .encode({ configCompleteId: request.wantConfigId })
                  .finish(),
              ),
            );
          }
        }
      });
    });
    try {
      const port = await listen(server);
      const transport = new TcpMeshtasticTransport({
        host: "127.0.0.1",
        port,
        configSession: new MeshtasticProtobufCodec(schema),
        random: () => 0,
        physicalGuard: physicalGuard(directory, "tcp-session"),
      });

      await transport.connect();
      expect(transport.state.status).toBe("ready");
      expect(observed).toEqual([wantConfig(1)]);
      await expect(
        transport.writeFrame(new Uint8Array([99])),
      ).rejects.toMatchObject({ code: "PHYSICAL_GUARD_WRITER_DISABLED" });
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(observed).toHaveLength(1);
      expect(transport.metrics.framesSent).toBe(1);

      await transport.disconnect();
    } finally {
      await close(server);
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("writes zero bytes when a physical codec returns a non-allowlisted ToRadio variant", async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-physical-tcp-"));
    let receivedBytes = 0;
    let connections = 0;
    const server = net.createServer((socket) => {
      connections += 1;
      socket.on("data", (chunk: Buffer) => {
        receivedBytes += chunk.length;
      });
    });
    try {
      const port = await listen(server);
      const transport = new TcpMeshtasticTransport({
        host: "127.0.0.1",
        port,
        configSession: {
          encodeWantConfig: () => new Uint8Array([0x22, 0x00]),
          isConfigComplete: () => false,
        },
        random: () => 0,
        physicalGuard: physicalGuard(directory, "malicious-codec"),
      });

      await expect(transport.connect()).rejects.toMatchObject({
        code: "PHYSICAL_GUARD_CONFIG_PAYLOAD_REJECTED",
      });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(connections).toBe(1);
      expect(receivedBytes).toBe(0);
      expect(transport.metrics.framesSent).toBe(0);
      expect(transport.state.status).toBe("disconnected");
      await transport.disconnect();
    } finally {
      await close(server);
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("does not schedule another physical socket after the third config failure opens the fuse", async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-physical-tcp-"));
    let connections = 0;
    const sockets = new Set<Socket>();
    const server = net.createServer((socket) => {
      connections += 1;
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
    });
    try {
      for (let attempt = 1; attempt <= 2; attempt += 1) {
        const guard = physicalGuard(directory, `failed-${attempt}`);
        guard.acquireSession(attempt);
        guard.authorizeConfigRequest(attempt, wantConfig(attempt));
        guard.recordConfigFailure("timeout");
        guard.releaseSession();
      }
      const port = await listen(server);
      const transport = new TcpMeshtasticTransport({
        host: "127.0.0.1",
        port,
        configSession: physicalCodec,
        configTimeoutMs: 5,
        random: () => 0,
        physicalGuard: physicalGuard(directory, "third-failure"),
      });
      const trace: string[] = [];
      transport.subscribe((event) => {
        trace.push(
          event.kind === "state"
            ? `state:${event.state.status}`
            : `${event.kind}:${"code" in event ? event.code : "frame"}`,
        );
      });
      const outcome = await Promise.race([
        transport.connect().catch((error: unknown) => error),
        new Promise((resolve) =>
          setTimeout(
            () =>
              resolve({
                code: "FIXTURE_CONNECT_DID_NOT_SETTLE",
                state: transport.state.status,
                trace,
              }),
            1_000,
          ),
        ),
      ]);
      await transport.disconnect();
      expect(outcome).toMatchObject({
        code: "PHYSICAL_GUARD_FUSE_OPEN",
      });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(connections).toBe(1);
      expect(transport.state.status).toBe("disconnected");
    } finally {
      for (const socket of sockets) {
        socket.destroy();
      }
      await close(server);
      await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);

  it("opens no socket when the aggregate physical attempt fuse rejects a fifth cycle", async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-physical-tcp-"));
    let socketFactoryCalls = 0;
    try {
      for (let attempt = 0; attempt < 4; attempt += 1) {
        const guard = physicalGuard(directory, `reserve-${attempt}`);
        guard.acquireSession(attempt + 1);
        guard.releaseSession("connect");
      }
      const transport = new TcpMeshtasticTransport(
        {
          host: "127.0.0.1",
          port: 4403,
          configSession: codec,
          random: () => 0,
          physicalGuard: physicalGuard(directory, "blocked-fifth"),
        },
        () => {
          socketFactoryCalls += 1;
          return new net.Socket();
        },
      );

      await expect(transport.connect()).rejects.toMatchObject({
        code: "PHYSICAL_GUARD_ATTEMPT_WINDOW_EXCEEDED",
      });
      expect(socketFactoryCalls).toBe(0);
      await transport.disconnect();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
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

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("fixture condition was not reached");
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

function wantConfig(nonce: number): Uint8Array {
  const bytes = [0x18];
  let remaining = nonce;
  while (remaining >= 0x80) {
    bytes.push((remaining % 0x80) | 0x80);
    remaining = Math.floor(remaining / 0x80);
  }
  bytes.push(remaining);
  return Uint8Array.from(bytes);
}

function physicalGuard(directory: string, token: string): PhysicalWriteGuard {
  return new PhysicalWriteGuard({
    physicalProfile: true,
    allowedRoot: directory,
    ledgerPath: join(directory, "physical-write-ledger.sqlite"),
    candidateId: "fixture-candidate",
    qualificationStage: "fixture-stage",
    sessionTokenFactory: () => `fixture-${token}-00000000`,
  });
}
