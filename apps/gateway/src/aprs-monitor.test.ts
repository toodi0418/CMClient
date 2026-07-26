import { once } from "node:events";
import net, { type Server, type Socket } from "node:net";

import { describe, expect, it } from "vitest";

import type { PositionCanonicalEvent } from "@cmclient/contracts";

import { encodeAprsPosition } from "./aprs-position";
import {
  APRS_RX_FILTER_EXPRESSION,
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

function boundaryLoginLine(totalBytes: number): string {
  const prefix = "user TEST01-C7 pass 17602 vers ";
  const suffix = ` filter ${APRS_RX_FILTER_EXPRESSION}\r\n`;
  const padding = totalBytes - Buffer.byteLength(`${prefix}${suffix}`, "ascii");
  if (padding < 1) {
    throw new Error("fixture login boundary is too short");
  }
  return `${prefix}${"x".repeat(padding)}`;
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

    expect(monitor.filterExpression()).toBe(APRS_RX_FILTER_EXPRESSION);
    expect(advanced).toMatchObject({
      kind: "advanced",
      remote: { infoDigest: expect.stringMatching(/^[a-f0-9]{64}$/) },
      state: {
        meshNetworkId: target.meshNetworkId,
        nodeNum: target.nodeNum,
        latestInfoDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
        receivedAt: "2026-07-18T00:01:00.000Z",
      },
    });
    expect(repeated).toMatchObject({ kind: "not_new" });
    expect(unknown).toEqual({ kind: "ignored", reason: "unmapped_callsign" });
    database.close();
  });

  it("replaces exact local targets without changing the fixed server filter", () => {
    const database = new GatewayDatabase(":memory:");
    const monitor = new AprsIsMonitor(
      [target],
      new AprsRemoteHighWaterStore(database.connection),
    );
    const replacement = {
      callsign: "N1CALL-7",
      mappingVersion: "mapping-v2",
      meshNetworkId: "fixture-network-b",
      nodeNum: 7,
    };

    monitor.replaceTargets([replacement]);

    expect(monitor.filterExpression()).toBe(APRS_RX_FILTER_EXPRESSION);
    expect(
      monitor.observeLine(
        "N0CALL-7>APTMAG,MESHD*,qAO,TEST01-7:!2502.85N/12131.05E>old",
        "2026-07-18T00:00:10.000Z",
      ),
    ).toEqual({ kind: "ignored", reason: "unmapped_callsign" });
    expect(
      monitor.observeLine(
        "N1CALL-7>APTMAG,MESHD*,qAO,TEST01-7:!2502.85N/12131.05E>new",
        "2026-07-18T00:00:11.000Z",
      ),
    ).toMatchObject({ kind: "advanced", state: replacement });
    database.close();
  });

  it("keeps a 200-mapping login line below the APRS-IS limit", () => {
    const database = new GatewayDatabase(":memory:");
    const targets = Array.from({ length: 200 }, (_, index) => ({
      callsign: `B${index.toString(36).toUpperCase().padStart(4, "0")}`,
      mappingVersion: "mapping-v1",
      meshNetworkId: `fixture-network-${index}`,
      nodeNum: index,
    }));
    const monitor = new AprsIsMonitor(
      targets,
      new AprsRemoteHighWaterStore(database.connection),
    );
    const loginLine = `user TEST01-CF pass 12345 vers CMClient 2.0 filter ${monitor.filterExpression()}\r\n`;

    expect(Buffer.byteLength(loginLine, "ascii")).toBeLessThanOrEqual(512);
    expect(monitor.filterExpression()).not.toContain(targets[0]!.callsign);
    expect(monitor.filterExpression()).not.toContain(targets.at(-1)!.callsign);
    database.close();
  });

  it("uses path-insensitive exact Info TTLs and fails closed for malformed data", () => {
    const database = new GatewayDatabase(":memory:");
    const highWater = new AprsRemoteHighWaterStore(database.connection);
    const monitor = new AprsIsMonitor([target], highWater);
    const encoded = encode(event("2026-07-18T00:00:35.000Z"));
    const packet = parseCmClientAprsLine(encoded);
    if (!packet) {
      throw new Error("fixture APRS packet did not parse");
    }
    const alternateDestination = encoded.replace(
      `>${packet.destination}`,
      ">APCM20",
    );
    const qArObserved = encoded.replace(",qAO,", ",qAR,");
    monitor.observeLine(qArObserved, "2026-07-18T00:01:00.000Z");

    expect(
      monitor.observeLine("not-an-aprs-line", "2026-07-18T00:01:00.000Z"),
    ).toEqual({ kind: "ignored", reason: "malformed" });
    expect(parseCmClientAprsLine(`${encoded}\r\n`)).toBeUndefined();
    expect(
      highWater.canUploadData(encoded, target, "2026-07-18T00:01:30.000Z"),
    ).toBe(false);
    expect(
      highWater.canUploadData(
        alternateDestination,
        target,
        "2026-07-18T00:01:30.000Z",
      ),
    ).toBe(true);
    database.connection
      .prepare(
        "INSERT INTO aprs_observed_packets (callsign, destination, info, first_observed_at, last_observed_at) VALUES (?, '', ?, ?, ?)",
      )
      .run(
        packet.callsign,
        packet.info,
        "2026-07-18T00:01:00.000Z",
        "2026-07-18T00:01:00.000Z",
      );
    expect(
      highWater.canUploadData(
        alternateDestination,
        target,
        "2026-07-18T00:01:30.000Z",
      ),
    ).toBe(false);
    expect(
      highWater.canUploadData(
        encoded.replace(/:!.*/, ":!2500.01N/12130.00E>"),
        target,
        "2026-07-18T00:01:30.000Z",
      ),
    ).toBe(true);
    expect(
      highWater.canUploadData(encoded, target, "2026-07-18T03:01:01.000Z"),
    ).toBe(true);
    expect(highWater.canUploadData(encoded, target, "not-a-time")).toBe(false);
    database.close();
  });

  it("keys new local-transmission suppression by exact destination and Info", () => {
    const database = new GatewayDatabase(":memory:");
    const highWater = new AprsRemoteHighWaterStore(database.connection);
    const encoded = encode(event("2026-07-18T00:00:35.000Z"));
    const packet = parseCmClientAprsLine(encoded);
    if (!packet) {
      throw new Error("fixture APRS packet did not parse");
    }
    const alternateDestination = encoded.replace(
      `>${packet.destination}`,
      ">APCM20",
    );

    highWater.recordLocalTransmission(encoded, "2026-07-18T00:01:00.000Z");

    expect(
      highWater.canUploadData(encoded, target, "2026-07-18T00:01:01.000Z"),
    ).toBe(false);
    expect(
      highWater.canUploadData(
        alternateDestination,
        target,
        "2026-07-18T00:01:01.000Z",
      ),
    ).toBe(true);
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
          socket.write("# logresp TEST01-");
          socket.write(
            `CM verified, fixture\r\n${encode(event("2026-07-18T00:00:35.000Z"))}\r\n`,
          );
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
        "user TEST01-CM pass 17602 vers CMClient 2.0",
      ),
      provisionFingerprint: PROVISION_FINGERPRINT,
      filterExpression: APRS_RX_FILTER_EXPRESSION,
    });

    const session = await client.connect((line) => received.push(line));
    await waitFor(() => loginLines.length === 1 && received.length === 1);

    expect(loginLines).toEqual([
      `user TEST01-CM pass 17602 vers CMClient 2.0 filter ${APRS_RX_FILTER_EXPRESSION}`,
    ]);
    expect(received).toEqual([encode(event("2026-07-18T00:00:35.000Z"))]);
    await session.close();
    await close(server);
  });

  it.each(["end", "reset", "destroy"] as const)(
    "signals one post-login termination when the peer uses %s",
    async (terminationMode) => {
      let peer: Socket | undefined;
      const server = net.createServer((socket) => {
        peer = socket;
        socket.on("error", () => undefined);
        socket.once("data", () => {
          socket.write("# logresp TEST01-CM verified, fixture\r\n");
        });
      });
      server.listen({ host: "127.0.0.1", port: 0 });
      await once(server, "listening");
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("fixture APRS server did not bind");
      }
      const errors: unknown[] = [];
      const client = new AprsIsRxClient({
        host: "127.0.0.1",
        port: address.port,
        authorizationProvider: authorization(
          "user TEST01-CM pass 17602 vers CMClient 2.0",
        ),
        provisionFingerprint: PROVISION_FINGERPRINT,
        filterExpression: APRS_RX_FILTER_EXPRESSION,
      });

      const session = await client.connect(
        () => undefined,
        (error) => errors.push(error),
      );
      const terminated = session.terminated;
      if (!terminated || !peer) {
        throw new Error("fixture monitor session did not expose termination");
      }
      let terminationSignals = 0;
      void terminated.then(() => {
        terminationSignals += 1;
      });

      if (terminationMode === "end") {
        peer.end();
      } else if (terminationMode === "reset") {
        peer.resetAndDestroy();
      } else {
        peer.destroy();
      }
      await settlesWithin(terminated, 1_000);
      await new Promise((resolve) => setImmediate(resolve));

      expect(terminationSignals).toBe(1);
      expect(errors).toHaveLength(1);
      expect(errors[0]).toMatchObject({ code: "APRS_MONITOR_INVALID" });
      await session.close();
      expect(terminationSignals).toBe(1);
      await close(server);
    },
  );

  it.each([
    ["observer", "user TEST01-CM pass 17602 vers CMClient 2.0"],
    ["transmitter", "user TEST01 pass 11111 vers CMClient 2.0"],
  ] as const)(
    "rejects an unverified %s logresp status",
    async (_mode, loginLine) => {
      const server = net.createServer((socket) => {
        socket.once("data", () => {
          const callsign = /^user\s+(\S+)/.exec(loginLine)?.[1];
          socket.write(`# logresp ${callsign} unverified, fixture\r\n`);
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
        authorizationProvider: authorization(loginLine),
        provisionFingerprint: PROVISION_FINGERPRINT,
        filterExpression: APRS_RX_FILTER_EXPRESSION,
      });

      await expect(client.connect(() => undefined)).rejects.toMatchObject({
        code: "APRS_MONITOR_INVALID",
      });
      await close(server);
    },
  );

  it("keeps termination signals isolated between rotated sessions", async () => {
    const peers: Socket[] = [];
    const server = net.createServer((socket) => {
      peers.push(socket);
      socket.on("error", () => undefined);
      socket.once("data", (chunk: Buffer) => {
        const callsign = /^user\s+(\S+)/.exec(chunk.toString("utf8"))?.[1];
        if (callsign) {
          socket.write(`# logresp ${callsign} verified, fixture\r\n`);
        }
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    const createClient = (loginLine: string, provisionFingerprint: string) =>
      new AprsIsRxClient({
        host: "127.0.0.1",
        port: address.port,
        authorizationProvider: authorization(loginLine, provisionFingerprint),
        provisionFingerprint,
        filterExpression: APRS_RX_FILTER_EXPRESSION,
      });
    const first = await createClient(
      "user TEST01-CM pass 17602 vers CMClient 2.0",
      PROVISION_FINGERPRINT,
    ).connect(() => undefined);
    const received: string[] = [];
    const second = await createClient(
      "user AB12CD-CM pass 16598 vers CMClient 2.0",
      ROTATED_PROVISION_FINGERPRINT,
    ).connect((line) => received.push(line));
    if (!first.terminated || !second.terminated || peers.length !== 2) {
      throw new Error("fixture monitor sessions were not established");
    }
    let secondTerminated = false;
    void second.terminated.then(() => {
      secondTerminated = true;
    });

    peers[0]!.destroy();
    await settlesWithin(first.terminated, 1_000);
    await new Promise((resolve) => setImmediate(resolve));
    expect(secondTerminated).toBe(false);

    const line = encode(event("2026-07-18T00:00:35.000Z"));
    peers[1]!.write(`${line}\r\n`);
    await waitFor(() => received.length === 1);
    expect(received).toEqual([line]);

    await second.close();
    await settlesWithin(second.terminated, 1_000);
    expect(secondTerminated).toBe(true);
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
        const callsign = /^user\s+([^\s]+)/.exec(lines[0] ?? "")?.[1];
        if (lines.length === 1 && callsign) {
          socket.write(`# logresp ${callsign} verified, fixture\r\n`);
        }
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
        "user ABC-CM pass 13026 vers CMClient 2.0",
      ),
      provisionFingerprint: PROVISION_FINGERPRINT,
      filterExpression: APRS_RX_FILTER_EXPRESSION,
    });

    const first = await firstClient.connect(() => undefined);
    await waitFor(() => sessions.length === 1 && sessions[0]!.length === 1);
    await first.close();
    const secondClient = new AprsIsRxClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: authorization(
        "user ABCD-CM pass 12960 vers CMClient 2.0",
        ROTATED_PROVISION_FINGERPRINT,
      ),
      provisionFingerprint: ROTATED_PROVISION_FINGERPRINT,
      filterExpression: APRS_RX_FILTER_EXPRESSION,
    });
    const second = await secondClient.connect(() => undefined);
    await waitFor(() => sessions.length === 2 && sessions[1]!.length === 1);
    await second.close();

    expect(sessions.map((lines) => lines[0])).toEqual([
      `user ABC-CM pass 13026 vers CMClient 2.0 filter ${APRS_RX_FILTER_EXPRESSION}`,
      `user ABCD-CM pass 12960 vers CMClient 2.0 filter ${APRS_RX_FILTER_EXPRESSION}`,
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
        filterExpression: APRS_RX_FILTER_EXPRESSION,
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
      authorization(boundaryLoginLine(513)),
      authorization("user ABC-CM pass -1 vers CMClient 2.0"),
      authorization("user TEST01-CM pass -1 vers CMClient 2.0"),
      authorization("user TEST01-CM pass invalid vers CMClient 2.0"),
      authorization("user TEST01-CM pass 32768 vers CMClient 2.0"),
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
        filterExpression: APRS_RX_FILTER_EXPRESSION,
      });
      await expect(client.connect(() => undefined)).rejects.toMatchObject({
        code: "APRS_PROVISION_UNAVAILABLE",
      });
    }
    await new Promise((resolve) => setImmediate(resolve));

    expect(connections).toBe(0);
    await close(server);
  });

  it("accepts a login line that is exactly 512 bytes including CRLF", async () => {
    const loginLines: string[] = [];
    const server = net.createServer((socket) => {
      let buffer = "";
      socket.on("data", (chunk: Buffer) => {
        buffer += chunk.toString("utf8");
        const lineEnd = buffer.indexOf("\r\n");
        if (lineEnd >= 0 && loginLines.length === 0) {
          loginLines.push(buffer.slice(0, lineEnd + 2));
          socket.write("# logresp TEST01-C7 verified, fixture\r\n");
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
      authorizationProvider: authorization(boundaryLoginLine(512)),
      provisionFingerprint: PROVISION_FINGERPRINT,
      filterExpression: APRS_RX_FILTER_EXPRESSION,
    });

    const session = await client.connect(() => undefined);

    expect(loginLines).toHaveLength(1);
    expect(Buffer.byteLength(loginLines[0]!, "ascii")).toBe(512);
    await session.close();
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
            "# logresp TEST01 verified, fixture",
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
      filterExpression: APRS_RX_FILTER_EXPRESSION,
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
          socket.write(`# logresp TEST01 verified, fixture\r\n${burst}`);
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
      filterExpression: APRS_RX_FILTER_EXPRESSION,
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
      socket.on("data", () => {
        socket.write("# logresp TEST01 verified, fixture\r\n");
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
      filterExpression: APRS_RX_FILTER_EXPRESSION,
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
    mappingCallsign: target.callsign,
    mappingSymbolTable: "/",
    mappingSymbolCode: ">",
    provisionIgateCallsign: "N1GATE-10",
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
