import { once } from "node:events";
import net, { type Server, type Socket } from "node:net";

import { describe, expect, it } from "vitest";

import type { PositionCanonicalEvent } from "@cmclient/contracts";

import { encodeAprsPosition } from "./aprs-position";
import {
  AprsIsMonitor,
  AprsIsRxClient,
  AprsRemoteHighWaterStore,
  parseCmClientAprsLine,
} from "./aprs-monitor";
import { GatewayDatabase } from "./persistence/database";

const target = {
  callsign: "N0CALL-7",
  mappingVersion: "mapping-v1",
  meshNetworkId: "fixture-network",
  nodeNum: 42,
};
const PROVISION_FINGERPRINT = "a".repeat(64);
const ROTATED_PROVISION_FINGERPRINT = "b".repeat(64);

function authorization(
  loginLine: string,
  provisionFingerprint = PROVISION_FINGERPRINT,
) {
  return () => ({ loginLine, provisionFingerprint });
}

describe("APRS-IS monitor", () => {
  it("filters mapped callsigns and advances an isolated remote high-water", () => {
    const database = new GatewayDatabase(":memory:");
    const highWater = new AprsRemoteHighWaterStore(database.connection);
    const monitor = new AprsIsMonitor([target], highWater);
    const encoded = encode(event("2026-07-18T00:00:35.000Z"));

    const advanced = monitor.observeLine(encoded, "2026-07-18T00:01:00.000Z");
    const repeated = monitor.observeLine(encoded, "2026-07-18T00:01:01.000Z");
    const unknown = monitor.observeLine(
      encoded.replace("N0CALL-7", "N1CALL-7"),
      "2026-07-18T00:01:02.000Z",
    );

    expect(monitor.filterExpression()).toBe("b/N0CALL-7");
    expect(advanced).toMatchObject({
      kind: "advanced",
      remote: { eventMarker: `CM2/${"a".repeat(12)}` },
      state: {
        meshNetworkId: target.meshNetworkId,
        nodeNum: target.nodeNum,
        latestEventTime: "2026-07-18T00:00:00.000Z",
      },
    });
    expect(repeated).toMatchObject({ kind: "not_new" });
    expect(unknown).toEqual({ kind: "ignored", reason: "unmapped_callsign" });
    database.close();
  });

  it("fails closed for malformed remote lines and local events not proven newer", () => {
    const database = new GatewayDatabase(":memory:");
    const highWater = new AprsRemoteHighWaterStore(database.connection);
    const monitor = new AprsIsMonitor([target], highWater);
    expect(highWater.canUpload(event(undefined), target)).toBe(false);
    monitor.observeLine(
      encode(event("2026-07-18T00:00:35.000Z")),
      "2026-07-18T00:01:00.000Z",
    );

    expect(
      monitor.observeLine(
        `N0CALL-7>APCM20:/180000z2500.00Q/12130.00E> CM2/${"a".repeat(12)}`,
        "2026-07-18T00:01:00.000Z",
      ),
    ).toEqual({ kind: "ignored", reason: "malformed" });
    expect(
      parseCmClientAprsLine(
        `N0CALL-7>APCM20:/010000z2500.00N/12130.00E> CM2/${"b".repeat(12)}`,
        "2026-09-18T00:01:00.000Z",
      ),
    ).toBeUndefined();
    expect(highWater.canUpload(event("2026-07-18T00:00:35.000Z"), target)).toBe(
      true,
    );
    expect(
      highWater.canUpload(event("2026-07-18T00:00:59.000Z", "b"), target),
    ).toBe(false);
    expect(
      highWater.canUpload(event("2026-07-18T00:01:00.000Z", "c"), target),
    ).toBe(true);
    expect(highWater.canUpload(event(undefined, "d"), target)).toBe(false);
    expect(highWater.canUpload(event("not-a-time", "e"), target)).toBe(false);
    database.close();
  });
});

describe("AprsIsRxClient", () => {
  it("subscribes with the monitor filter and frames received APRS lines", async () => {
    const received: string[] = [];
    const loginLines: string[] = [];
    const server = net.createServer((socket) => {
      let buffer = "";
      socket.on("data", (chunk: Buffer) => {
        buffer += chunk.toString("utf8");
        const parts = buffer.split("\r\n");
        buffer = parts.pop() ?? "";
        loginLines.push(...parts.filter(Boolean));
        if (loginLines.length === 1) {
          socket.write(`${encode(event("2026-07-18T00:00:35.000Z"))}\r\n`);
        }
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    const client = new AprsIsRxClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      provisionFingerprint: PROVISION_FINGERPRINT,
      filterExpression: "b/TEST01-7",
    });

    const session = await client.connect((line) => received.push(line));
    await waitFor(() => loginLines.length === 1 && received.length === 1);

    expect(loginLines).toEqual([
      "user TEST01 pass 11111 vers CMClient 2.0 filter b/TEST01-7",
    ]);
    expect(received).toEqual([encode(event("2026-07-18T00:00:35.000Z"))]);
    await session.close();
    await close(server);
  });

  it("resolves a rotated login provider before every monitor connection", async () => {
    const sessions: string[][] = [];
    const server = net.createServer((socket) => {
      const lines: string[] = [];
      sessions.push(lines);
      let buffer = "";
      socket.on("data", (chunk: Buffer) => {
        buffer += chunk.toString("utf8");
        const parts = buffer.split("\r\n");
        buffer = parts.pop() ?? "";
        lines.push(...parts.filter(Boolean));
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    const firstClient = new AprsIsRxClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      provisionFingerprint: PROVISION_FINGERPRINT,
      filterExpression: "b/TEST01-7",
    });

    const first = await firstClient.connect(() => undefined);
    await waitFor(() => sessions.length === 1 && sessions[0]!.length === 1);
    await first.close();
    const secondClient = new AprsIsRxClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: authorization(
        "user AB12CD-7 pass 22222 vers CMClient 2.0",
        ROTATED_PROVISION_FINGERPRINT,
      ),
      provisionFingerprint: ROTATED_PROVISION_FINGERPRINT,
      filterExpression: "b/TEST01-7",
    });
    const second = await secondClient.connect(() => undefined);
    await waitFor(() => sessions.length === 2 && sessions[1]!.length === 1);
    await second.close();

    expect(sessions.map((lines) => lines[0])).toEqual([
      "user TEST01 pass 11111 vers CMClient 2.0 filter b/TEST01-7",
      "user AB12CD-7 pass 22222 vers CMClient 2.0 filter b/TEST01-7",
    ]);
    await close(server);
  });

  it.each([
    ["revoked", undefined],
    [
      "rotated",
      {
        loginLine: "user AB12CD-7 pass 22222 vers CMClient 2.0",
        provisionFingerprint: ROTATED_PROVISION_FINGERPRINT,
      },
    ],
  ] as const)(
    "revalidates a provision that is %s while the monitor connection opens",
    async (_case, changedAuthorization) => {
      let connections = 0;
      const lines: string[] = [];
      const server = net.createServer((socket) => {
        connections += 1;
        socket.on("data", (chunk: Buffer) => {
          lines.push(chunk.toString("utf8"));
        });
      });
      server.listen({ host: "127.0.0.1", port: 0 });
      await once(server, "listening");
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("fixture APRS server did not bind");
      }
      let providerCalls = 0;
      const client = new AprsIsRxClient({
        host: "127.0.0.1",
        port: address.port,
        authorizationProvider: () => {
          providerCalls += 1;
          return providerCalls === 1
            ? {
                loginLine: "user TEST01 pass 11111 vers CMClient 2.0",
                provisionFingerprint: PROVISION_FINGERPRINT,
              }
            : changedAuthorization;
        },
        provisionFingerprint: PROVISION_FINGERPRINT,
        filterExpression: "b/TEST01-7",
      });

      await expect(client.connect(() => undefined)).rejects.toMatchObject({
        code: "APRS_PROVISION_UNAVAILABLE",
      });
      await new Promise((resolve) => setImmediate(resolve));

      expect(connections).toBe(1);
      expect(providerCalls).toBe(2);
      expect(lines).toEqual([]);
      await close(server);
    },
  );

  it("rejects invalid login providers before opening a monitor socket", async () => {
    let connections = 0;
    const server = net.createServer(() => {
      connections += 1;
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    const providers = [
      () => undefined,
      () => {
        throw new Error("fixture provider failure");
      },
      authorization("user TEST01 pass 11111\r\nuser injected"),
      authorization("x".repeat(513)),
      authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
        ROTATED_PROVISION_FINGERPRINT,
      ),
    ];

    for (const authorizationProvider of providers) {
      const client = new AprsIsRxClient({
        host: "127.0.0.1",
        port: address.port,
        authorizationProvider,
        provisionFingerprint: PROVISION_FINGERPRINT,
        filterExpression: "b/TEST01-7",
      });
      await expect(client.connect(() => undefined)).rejects.toMatchObject({
        code: "APRS_PROVISION_UNAVAILABLE",
      });
    }
    await new Promise((resolve) => setImmediate(resolve));

    expect(connections).toBe(0);
    await close(server);
  });

  it("isolates a throwing line callback and continues with later lines", async () => {
    let sent = false;
    const server = net.createServer((socket) => {
      socket.on("data", () => {
        if (sent) {
          return;
        }
        sent = true;
        socket.write(
          [
            encode(event("2026-07-18T00:00:35.000Z")),
            encode(event("2026-07-18T00:01:35.000Z", "b")),
            "",
          ].join("\r\n"),
        );
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    const client = new AprsIsRxClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      provisionFingerprint: PROVISION_FINGERPRINT,
      filterExpression: "b/N0CALL-7",
    });
    const processed: string[] = [];
    const callbackErrors: unknown[] = [];
    let callbacks = 0;

    const session = await client.connect(
      (line) => {
        callbacks += 1;
        if (callbacks === 1) {
          throw new Error("fixture callback failure");
        }
        processed.push(line);
      },
      (error) => {
        callbackErrors.push(error);
        throw new Error("fixture error callback failure");
      },
    );
    await waitFor(() => callbacks === 2);

    expect(callbackErrors).toHaveLength(1);
    expect(processed).toEqual([encode(event("2026-07-18T00:01:35.000Z", "b"))]);
    await session.close();
    await close(server);
  });

  it("drains a coalesced burst before bounding the unterminated remainder", async () => {
    const received: string[] = [];
    let peerClosed = false;
    let sent = false;
    const line = encode(event("2026-07-18T00:00:35.000Z"));
    const burst = Array.from({ length: 20 }, () => line).join("\r\n") + "\r\n";
    expect(Buffer.byteLength(burst)).toBeGreaterThan(1_024);
    const server = net.createServer((socket) => {
      socket.once("close", () => {
        peerClosed = true;
      });
      socket.on("data", () => {
        if (!sent) {
          sent = true;
          socket.write(burst);
        }
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    const client = new AprsIsRxClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      provisionFingerprint: PROVISION_FINGERPRINT,
      filterExpression: "b/N0CALL-7",
    });

    const session = await client.connect((value) => received.push(value));
    await waitFor(() => received.length === 20);

    expect(received).toEqual(Array.from({ length: 20 }, () => line));
    expect(peerClosed).toBe(false);
    await session.close();
    await close(server);
  });

  it("forces a bounded socket close when the peer remains half-open", async () => {
    const sockets = new Set<Socket>();
    const server = net.createServer({ allowHalfOpen: true }, (socket) => {
      sockets.add(socket);
      socket.on("error", () => undefined);
      socket.once("close", () => sockets.delete(socket));
      socket.resume();
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    const client = new AprsIsRxClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      provisionFingerprint: PROVISION_FINGERPRINT,
      filterExpression: "b/N0CALL-7",
      closeTimeoutMs: 25,
    });

    try {
      const session = await client.connect(() => undefined);
      await settlesWithin(session.close(), 1_000);
    } finally {
      for (const socket of sockets) {
        socket.destroy();
      }
      await close(server);
    }
  });
});

function encode(positionEvent: PositionCanonicalEvent): string {
  return encodeAprsPosition(positionEvent, {
    source: target.callsign,
    destination: "APCM20",
    symbolTable: "/",
    symbolCode: ">",
  }).data;
}

function event(
  eventTime: string | undefined,
  canonicalKeyCharacter = "a",
): PositionCanonicalEvent {
  return {
    schemaVersion: 1,
    id: `position-event-${canonicalKeyCharacter}`,
    canonicalKey: canonicalKeyCharacter.repeat(64),
    meshNetworkId: target.meshNetworkId,
    nodeNum: target.nodeNum,
    sourceObservationId: "position-observation-fixture",
    payloadHash: "f".repeat(64),
    ...(eventTime
      ? { eventTime, eventTimeSource: "position_timestamp" as const }
      : {}),
    position: {
      latitudeI: 250000000,
      longitudeI: 1215000000,
      precisionBits: 32,
    },
    createdAt: "2026-07-18T00:00:00.000Z",
  };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("fixture condition was not reached");
}

async function close(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

async function settlesWithin(
  operation: Promise<void>,
  timeoutMs: number,
): Promise<void> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<void>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error("fixture operation did not settle")),
      timeoutMs,
    );
  });
  try {
    await Promise.race([operation, timeout]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}
