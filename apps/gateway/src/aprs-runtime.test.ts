import { once } from "node:events";
import net from "node:net";

import { describe, expect, it, vi } from "vitest";

import {
  AprsIsRxClient,
  parseCmClientAprsLine,
  type AprsIsRxSession,
} from "./aprs-monitor";
import {
  AprsTransmissionFencedError,
  type AprsOutboxEntry,
} from "./aprs-outbox";
import {
  deriveAprsRuntimeIdentity,
  observerConnectionAuthorization,
  type AprsRuntimeState,
} from "./aprs-identity";
import { AprsGatewayRuntime } from "./aprs-runtime";
import { DomainEventBus } from "./events";
import { GatewayDatabase } from "./persistence/database";

describe("AprsGatewayRuntime", () => {
  it("submits and independently confirms the complete initial station family", async () => {
    const database = new GatewayDatabase(":memory:");
    const state = aprsState("a", "N0CALL-7", 42);
    const transmitted: string[] = [];
    const eventTypes: string[] = [];
    const events = new DomainEventBus({ eventIdFactory: sequentialFactory() });
    events.subscribe((event) => eventTypes.push(event.type));
    let now = new Date("2026-07-18T00:00:10.000Z");
    let filter = "";
    let onLine: ((line: string) => void) | undefined;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: events,
      stateProvider: () => state,
      clock: () => now,
      version: "2.0.0-test",
      outbox: { flush: async () => [] },
      stationTransport: {
        send: async (data, provisionFingerprint) => {
          expect(provisionFingerprint).toBe(state.provisionFingerprint);
          transmitted.push(data);
        },
      },
      monitorClientFactory: (expression) => {
        filter = expression;
        return {
          connect: async (listener) => {
            onLine = listener;
            return { close: async () => undefined };
          },
        };
      },
    });

    runtime.recordDecodedSummary(
      "position",
      Date.parse("2026-07-18T00:00:09.000Z"),
    );
    await runtime.refreshMonitor();
    await runtime.igateNow();

    const infos = transmitted.map((line) => parseCmClientAprsLine(line)?.info);
    expect(infos).toHaveLength(6);
    expect(infos[0]).toMatch(/^!/u);
    expect(infos[1]).toContain(":PARM.ALL_PKTS_10M");
    expect(infos[2]).toContain(":UNIT.cnt,cnt,cnt,cnt,cnt");
    expect(infos[3]).toContain(":EQNS.0,1,0");
    expect(infos[4]).toBe("T#001,1,0,1,0,0,00000000");
    expect(infos[5]).toBe(">TMAG Client v2.0.0-test");
    expect(
      database.connection
        .prepare(
          "SELECT delivery_status, COUNT(*) AS count FROM aprs_igate_submissions GROUP BY delivery_status",
        )
        .all(),
    ).toEqual([{ delivery_status: "submitted", count: 6 }]);

    expect(filter).toBe("b/N0CALL-7/TEST01-7");
    for (const line of transmitted) {
      onLine?.(line.replace("TCPIP*:", "TCPIP*,qAC,T2TEST:"));
    }

    expect(
      database.connection
        .prepare(
          "SELECT delivery_status, COUNT(*) AS count FROM aprs_igate_submissions GROUP BY delivery_status",
        )
        .all(),
    ).toEqual([{ delivery_status: "observer_confirmed", count: 6 }]);
    expect(
      database.connection
        .prepare(
          "SELECT last_successful_telemetry_sequence AS sequence FROM aprs_igate_state",
        )
        .get(),
    ).toEqual({ sequence: 1 });
    expect(
      eventTypes.filter((type) => type === "aprs.igate.submitted"),
    ).toHaveLength(6);
    expect(
      eventTypes.filter((type) => type === "aprs.igate.observer_confirmed"),
    ).toHaveLength(6);

    now = new Date("2026-07-18T00:11:10.000Z");
    await runtime.igateNow();
    expect(transmitted).toHaveLength(8);
    expect(parseCmClientAprsLine(transmitted[6]!)?.info).toBe(infos[0]);
    expect(parseCmClientAprsLine(transmitted[7]!)?.info).toBe(
      "T#002,0,0,0,0,0,00000000",
    );

    await runtime.stop();
    database.close();
  });

  it("reanchors station cadence after the verified TX session changes", async () => {
    const database = new GatewayDatabase(":memory:");
    const state = aprsState("a", "N0CALL-7", 42);
    const transmitted: string[] = [];
    let now = new Date("2026-07-18T00:00:10.000Z");
    let sessionGeneration = 1;
    let onLine: ((line: string) => void) | undefined;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      stateProvider: () => state,
      clock: () => now,
      version: "2.0.0-test",
      outbox: { flush: async () => [] },
      stationTransport: {
        prepareVerifiedSession: async () => ({
          generation: sessionGeneration,
        }),
        send: async (data, provisionFingerprint, expectedSession) => {
          expect(provisionFingerprint).toBe(state.provisionFingerprint);
          expect(expectedSession?.generation).toBe(sessionGeneration);
          transmitted.push(data);
        },
      },
      monitorClientFactory: () => ({
        connect: async (listener) => {
          onLine = listener;
          return { close: async () => undefined };
        },
      }),
    });

    await runtime.refreshMonitor();
    await runtime.igateNow();
    expect(transmitted).toHaveLength(6);
    for (const line of transmitted) {
      onLine?.(line.replace("TCPIP*:", "TCPIP*,qAC,T2TEST:"));
    }

    sessionGeneration = 2;
    now = new Date("2026-07-18T00:11:10.000Z");
    await runtime.igateNow();
    expect(transmitted).toHaveLength(7);
    expect(parseCmClientAprsLine(transmitted[6]!)?.info).toBe(
      "T#002,0,0,0,0,0,00000000",
    );

    now = new Date("2026-07-18T00:21:10.000Z");
    await runtime.igateNow();
    expect(transmitted).toHaveLength(9);
    expect(parseCmClientAprsLine(transmitted[7]!)?.info).toMatch(/^!/u);
    expect(parseCmClientAprsLine(transmitted[8]!)?.info).toBe(
      "T#003,0,0,0,0,0,00000000",
    );

    await runtime.stop();
    database.close();
  });

  it("does not start outbound traffic before the observer session is verified", async () => {
    const database = new GatewayDatabase(":memory:");
    const state = aprsState("a", "N0CALL-7", 42);
    const session = deferred<AprsIsRxSession>();
    const connectStarted = deferred<void>();
    let flushes = 0;
    let sends = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      stateProvider: () => state,
      flushIntervalMs: 60_000,
      igateTickIntervalMs: 60_000,
      monitorRefreshIntervalMs: 60_000,
      outbox: {
        flush: async () => {
          flushes += 1;
          return [];
        },
      },
      stationTransport: {
        send: async () => {
          sends += 1;
        },
      },
      monitorClientFactory: () => ({
        connect: () => {
          connectStarted.resolve();
          return session.promise;
        },
      }),
    });

    runtime.start();
    await connectStarted.promise;
    await Promise.resolve();
    expect(flushes).toBe(0);
    expect(sends).toBe(0);

    session.resolve({ close: async () => undefined });
    await waitFor(() => flushes === 1 && sends === 6);

    await runtime.stop();
    database.close();
  });

  it("keeps every producer fenced when the real observer client is unverified", async () => {
    const database = new GatewayDatabase(":memory:");
    const state = aprsState("a", "N0CALL-7", 42);
    const logins: string[] = [];
    const server = net.createServer((socket) => {
      socket.once("data", (chunk: Buffer) => {
        const login = chunk.toString("utf8").split("\r\n", 1)[0] ?? "";
        logins.push(login);
        const callsign = /^user\s+(\S+)/u.exec(login)?.[1] ?? "UNKNOWN";
        socket.write(`# logresp ${callsign} unverified, fixture\r\n`);
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    let flushes = 0;
    let sends = 0;
    const authorizationProvider = observerConnectionAuthorization(() => state);
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      stateProvider: () => state,
      outbox: {
        flush: async () => {
          flushes += 1;
          return [];
        },
      },
      stationTransport: {
        send: async () => {
          sends += 1;
        },
      },
      monitorClientFactory: (filterExpression, provisionFingerprint) =>
        new AprsIsRxClient({
          host: "127.0.0.1",
          port: address.port,
          authorizationProvider,
          provisionFingerprint: provisionFingerprint!,
          filterExpression,
          timeoutMs: 100,
        }),
    });

    await runtime.refreshMonitor();

    expect(runtime.status().monitorStatus).toBe("error");
    expect(flushes).toBe(0);
    expect(sends).toBe(0);
    expect(logins).toEqual([
      `user TEST01-CM pass ${state.identity.passcode} vers CMClient 2.0 filter b/N0CALL-7/TEST01-7`,
    ]);
    await runtime.stop();
    await closeServer(server);
    database.close();
  });

  it("waits for an active station writer before closing the shared transport", async () => {
    const database = new GatewayDatabase(":memory:");
    const state = aprsState("a", "N0CALL-7", 42);
    const sendStarted = deferred<void>();
    const releaseSend = deferred<void>();
    const order: string[] = [];
    let sends = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      stateProvider: () => state,
      outbox: {
        flush: async () => [],
        close: async () => {
          order.push("close");
        },
      },
      stationTransport: {
        send: async () => {
          sends += 1;
          sendStarted.resolve();
          await releaseSend.promise;
          order.push("send");
        },
      },
      monitorClientFactory: () => ({
        connect: async () => ({ close: async () => undefined }),
      }),
    });

    await runtime.refreshMonitor();
    await sendStarted.promise;
    const stop = runtime.stop();
    await Promise.resolve();
    expect(order).toEqual([]);

    releaseSend.resolve();
    await stop;

    expect(sends).toBe(1);
    expect(order).toEqual(["send", "close"]);
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM aprs_igate_submissions")
        .get(),
    ).toEqual({ count: 1 });
    await runtime.igateNow();
    expect(sends).toBe(1);
    database.close();
  });

  it("stops a failed station batch and applies bounded exponential retry", async () => {
    const database = new GatewayDatabase(":memory:");
    const state = aprsState("a", "N0CALL-7", 42);
    const base = Date.parse("2026-07-18T00:00:00.000Z");
    let nowMs = base;
    let sends = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      stateProvider: () => state,
      clock: () => new Date(nowMs),
      igateRetryInitialMs: 100,
      igateRetryMaximumMs: 400,
      outbox: { flush: async () => [] },
      stationTransport: {
        send: async () => {
          sends += 1;
          throw new Error("fixture APRS-IS outage");
        },
      },
      monitorClientFactory: () => ({
        connect: async () => ({ close: async () => undefined }),
      }),
    });

    await runtime.refreshMonitor();
    await runtime.igateNow();
    expect(sends).toBe(1);

    nowMs = base + 99;
    await runtime.igateNow();
    expect(sends).toBe(1);
    nowMs = base + 100;
    await runtime.igateNow();
    expect(sends).toBe(2);

    nowMs = base + 299;
    await runtime.igateNow();
    expect(sends).toBe(2);
    nowMs = base + 300;
    await runtime.igateNow();
    expect(sends).toBe(3);

    nowMs = base + 699;
    await runtime.igateNow();
    expect(sends).toBe(3);
    nowMs = base + 700;
    await runtime.igateNow();
    expect(sends).toBe(4);
    nowMs = base + 1_099;
    await runtime.igateNow();
    expect(sends).toBe(4);
    nowMs = base + 1_100;
    await runtime.igateNow();
    expect(sends).toBe(5);

    await runtime.stop();
    database.close();
  });

  it("fences new outbound traffic when the observer session terminates", async () => {
    const database = new GatewayDatabase(":memory:");
    const state = aprsState("a", "N0CALL-7", 42);
    const terminated = deferred<void>();
    let now = new Date("2026-07-18T00:00:00.000Z");
    let flushes = 0;
    let sends = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      stateProvider: () => state,
      clock: () => now,
      monitorRefreshIntervalMs: 60_000,
      outbox: {
        flush: async () => {
          flushes += 1;
          return [];
        },
      },
      stationTransport: {
        send: async () => {
          sends += 1;
        },
      },
      monitorClientFactory: () => ({
        connect: async () => ({
          close: async () => undefined,
          terminated: terminated.promise,
        }),
      }),
    });

    await runtime.refreshMonitor();
    await runtime.igateNow();
    expect(sends).toBe(6);
    expect(flushes).toBe(1);

    terminated.resolve();
    await waitFor(() => runtime.status().monitorStatus === "error");
    now = new Date("2026-07-18T00:11:00.000Z");
    await runtime.igateNow();
    await runtime.flushNow();
    expect(sends).toBe(6);
    expect(flushes).toBe(1);

    await runtime.stop();
    database.close();
  });

  it("cancels an unwritten station intent when the observer closes at the socket gate", async () => {
    const database = new GatewayDatabase(":memory:");
    const state = aprsState("a", "N0CALL-7", 42);
    const terminated = deferred<void>();
    let sends = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      stateProvider: () => state,
      monitorRefreshIntervalMs: 60_000,
      outbox: { flush: async () => [] },
      stationTransport: {
        send: async (_data, _fingerprint, _session, transmissionGate) => {
          sends += 1;
          terminated.resolve();
          await waitFor(() => runtime.status().monitorStatus === "error");
          if (!transmissionGate?.()) {
            throw new AprsTransmissionFencedError();
          }
        },
      },
      monitorClientFactory: () => ({
        connect: async () => ({
          close: async () => undefined,
          terminated: terminated.promise,
        }),
      }),
    });

    await runtime.refreshMonitor();
    await runtime.igateNow();

    expect(sends).toBe(1);
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM aprs_igate_submissions")
        .get(),
    ).toEqual({ count: 0 });
    await runtime.stop();
    database.close();
  });

  it("flushes durable entries and monitors active mapped callsigns", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      mapping("fixture-network-a", 42, "N0CALL-7"),
      mapping("fixture-network-b", 7, "N1CALL-7"),
    ]);
    const events = new DomainEventBus({
      clock: () => new Date("2026-07-18T00:00:10.000Z"),
      eventIdFactory: sequentialFactory(),
    });
    const types: string[] = [];
    events.subscribe((event) => types.push(event.type));
    let filter = "";
    let onLine: ((line: string) => void) | undefined;
    let closes = 0;
    const session: AprsIsRxSession = {
      close: async () => {
        closes += 1;
      },
    };
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: events,
      clock: () => new Date("2026-07-18T00:00:10.000Z"),
      outbox: {
        flush: async () => [outboxEntry("sent"), outboxEntry("failed")],
      },
      monitorClientFactory: (expression) => {
        filter = expression;
        return {
          connect: async (listener) => {
            onLine = listener;
            return session;
          },
        };
      },
    });

    await runtime.refreshMonitor();
    await runtime.flushNow();
    onLine?.(
      "N0CALL-7>APTMAG,MESHD*,qAO,TEST01-7:!2502.85N/12131.05E>fixture-one",
    );

    expect(filter).toBe("b/N0CALL-7/N1CALL-7");
    expect(runtime.status()).toMatchObject({
      configured: true,
      running: false,
      monitorStatus: "connected",
      mappedCallsigns: 2,
      pendingOutbox: 0,
      failedOutbox: 0,
    });
    expect(types).toEqual(
      expect.arrayContaining([
        "aprs.outbox.submitted",
        "aprs.outbox.failed",
        "aprs.monitor.connected",
        "aprs.monitor.observed",
      ]),
    );
    expect(
      database.connection
        .prepare("SELECT callsign FROM aprs_observed_packets")
        .all(),
    ).toEqual([{ callsign: "N0CALL-7" }]);

    await runtime.refreshMonitor();
    expect(closes).toBe(0);
    await runtime.stop();
    expect(closes).toBe(1);
    database.close();
  });

  it("contains a high-water callback failure and recovers on the next valid line", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      mapping("fixture-network-a", 42, "N0CALL-7"),
    ]);
    const events = new DomainEventBus({
      clock: () => new Date("2026-07-18T00:02:00.000Z"),
      eventIdFactory: sequentialFactory(),
    });
    const errorCodes: string[] = [];
    const types: string[] = [];
    events.subscribe((event) => {
      types.push(event.type);
      if (event.type === "aprs.monitor.error") {
        errorCodes.push(String(event.payload.code));
      }
    });
    let onLine: ((line: string) => void) | undefined;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: events,
      clock: () => new Date("2026-07-18T00:02:00.000Z"),
      outbox: { flush: async () => [] },
      monitorClientFactory: () => ({
        connect: async (listener) => {
          onLine = listener;
          return { close: async () => undefined };
        },
      }),
    });
    await runtime.refreshMonitor();
    database.connection.exec(
      "CREATE TRIGGER fixture_reject_observed_packet BEFORE INSERT ON aprs_observed_packets BEGIN SELECT RAISE(ABORT, 'fixture persistence failure'); END",
    );

    expect(() =>
      onLine?.(
        "N0CALL-7>APTMAG,MESHD*,qAO,TEST01-7:!2502.85N/12131.05E>fixture-one",
      ),
    ).not.toThrow();
    expect(runtime.status()).toMatchObject({
      monitorStatus: "error",
      lastErrorCode: "APRS_MONITOR_PERSISTENCE_FAILED",
      mappedCallsigns: 1,
    });
    expect(errorCodes).toEqual(["APRS_MONITOR_PERSISTENCE_FAILED"]);

    database.connection.exec("DROP TRIGGER fixture_reject_observed_packet");
    expect(() =>
      onLine?.(
        "N0CALL-7>APTMAG,MESHD*,qAO,TEST01-7:!2502.85N/12131.05E>fixture-two",
      ),
    ).not.toThrow();

    expect(runtime.status()).toMatchObject({
      monitorStatus: "connected",
      mappedCallsigns: 1,
    });
    expect(runtime.status()).not.toHaveProperty("lastErrorCode");
    expect(
      database.connection
        .prepare("SELECT info FROM aprs_observed_packets WHERE callsign = ?")
        .get("N0CALL-7"),
    ).toEqual({ info: "!2502.85N/12131.05E>fixture-two" });
    expect(
      types.filter((type) => type === "aprs.monitor.connected"),
    ).toHaveLength(2);
    expect(
      types.filter((type) => type === "aprs.monitor.observed"),
    ).toHaveLength(1);

    await runtime.stop();
    database.close();
  });

  it("drops a terminated monitor session and reconnects with the same filter", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      mapping("fixture-network-a", 42, "N0CALL-7"),
    ]);
    const terminations = [deferred<void>(), deferred<void>()];
    const filters: string[] = [];
    let connections = 0;
    let closes = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      outbox: { flush: async () => [] },
      monitorRefreshIntervalMs: 100,
      monitorClientFactory: (filter) => ({
        connect: async () => {
          const index = connections;
          connections += 1;
          filters.push(filter);
          return {
            close: async () => {
              closes += 1;
            },
            terminated: terminations[index]!.promise,
          } as AprsIsRxSession;
        },
      }),
    });

    await runtime.refreshMonitor();
    expect(connections).toBe(1);
    terminations[0]!.resolve();
    await Promise.resolve();
    expect(connections).toBe(1);
    await new Promise((resolve) => setTimeout(resolve, 110));
    await waitFor(() => connections === 2);

    expect(filters).toEqual(["b/N0CALL-7", "b/N0CALL-7"]);
    expect(runtime.status()).toMatchObject({
      monitorStatus: "connected",
      mappedCallsigns: 1,
    });

    await runtime.stop();
    expect(closes).toBe(1);
    database.close();
  });

  it("bounds repeated monitor termination retries and cancels them on stop", async () => {
    vi.useFakeTimers();
    const database = new GatewayDatabase(":memory:");
    try {
      database.callmeshMappings.replace([
        mapping("fixture-network-a", 42, "N0CALL-7"),
      ]);
      let connections = 0;
      const runtime = new AprsGatewayRuntime({
        database,
        eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
        outbox: { flush: async () => [] },
        monitorRefreshIntervalMs: 1_000,
        monitorClientFactory: () => ({
          connect: async () => {
            connections += 1;
            return {
              close: async () => undefined,
              terminated: Promise.resolve(),
            };
          },
        }),
      });

      await runtime.refreshMonitor();
      await Promise.resolve();
      expect(connections).toBe(1);
      await vi.advanceTimersByTimeAsync(999);
      expect(connections).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(connections).toBe(2);

      await runtime.stop();
      await vi.advanceTimersByTimeAsync(5_000);
      expect(connections).toBe(2);
    } finally {
      database.close();
      vi.useRealTimers();
    }
  });

  it("fails closed when one callsign maps to more than one node", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      mapping("fixture-network-a", 42, "N0CALL-7"),
      mapping("fixture-network-b", 7, "N0CALL-7"),
    ]);
    const events = new DomainEventBus({ eventIdFactory: sequentialFactory() });
    const codes: string[] = [];
    events.subscribe((event) => {
      if (event.type === "aprs.monitor.error") {
        codes.push(String(event.payload.code));
      }
    });
    let connections = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: events,
      outbox: { flush: async () => [] },
      monitorClientFactory: () => ({
        connect: async () => {
          connections += 1;
          return { close: async () => undefined };
        },
      }),
    });

    await runtime.refreshMonitor();

    expect(connections).toBe(0);
    expect(codes).toEqual(["CALLMESH_MAPPING_CONFLICT"]);
    expect(runtime.status()).toMatchObject({
      monitorStatus: "error",
      mappedCallsigns: 2,
      lastErrorCode: "CALLMESH_MAPPING_CONFLICT",
    });
    database.close();
  });

  it("waits for an active flush and suppresses its late events", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      mapping("fixture-network-a", 42, "N0CALL-7"),
    ]);
    const events = new DomainEventBus({ eventIdFactory: sequentialFactory() });
    const types: string[] = [];
    events.subscribe((event) => types.push(event.type));
    const pendingFlush = deferred<AprsOutboxEntry[]>();
    let flushes = 0;
    let closes = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: events,
      outbox: {
        flush: () => {
          flushes += 1;
          return pendingFlush.promise;
        },
        close: async () => {
          closes += 1;
        },
      },
      monitorClientFactory: () => ({
        connect: async () => ({ close: async () => undefined }),
      }),
    });

    await runtime.refreshMonitor();
    const flush = runtime.flushNow();
    const stop = runtime.stop();
    let stopped = false;
    void stop.then(() => {
      stopped = true;
    });
    await Promise.resolve();

    expect(stopped).toBe(false);
    expect(closes).toBe(0);
    pendingFlush.resolve([outboxEntry("sent")]);
    await Promise.all([flush, stop]);

    expect(stopped).toBe(true);
    expect(closes).toBe(1);
    expect(types).not.toContain("aprs.outbox.submitted");
    await runtime.flushNow();
    expect(flushes).toBe(1);
    database.close();
  });

  it("reruns a queued monitor refresh with one atomic rotated state", async () => {
    const database = new GatewayDatabase(":memory:");
    let state = aprsState("a", "N0CALL-7", 42);
    const firstSession = deferred<AprsIsRxSession>();
    const firstConnectStarted = deferred<void>();
    const filters: string[] = [];
    const fingerprints: string[] = [];
    let connections = 0;
    let closes = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      stateProvider: () => state,
      outbox: { flush: async () => [] },
      monitorClientFactory: (filter, provisionFingerprint) => {
        filters.push(filter);
        fingerprints.push(provisionFingerprint ?? "");
        return {
          connect: () => {
            connections += 1;
            if (connections === 1) {
              firstConnectStarted.resolve();
              return firstSession.promise;
            }
            return Promise.resolve({
              close: async () => {
                closes += 1;
              },
            });
          },
        };
      },
    });

    const firstRefresh = runtime.refreshMonitor();
    await firstConnectStarted.promise;
    state = aprsState("b", "N1CALL-7", 7);
    const queuedRefresh = runtime.refreshMonitor();
    expect(queuedRefresh).toBe(firstRefresh);
    firstSession.resolve({
      close: async () => {
        closes += 1;
      },
    });

    await firstRefresh;
    expect(connections).toBe(2);
    expect(closes).toBe(1);
    expect(filters).toEqual(["b/N0CALL-7/TEST01-7", "b/AB12CD-7/N1CALL-7"]);
    expect(fingerprints).toEqual(["a".repeat(64), "b".repeat(64)]);
    expect(runtime.status()).toMatchObject({
      monitorStatus: "connected",
      mappedCallsigns: 1,
    });

    await runtime.stop();
    expect(closes).toBe(2);
    database.close();
  });

  it("revalidates a revoked state before publishing monitor connected", async () => {
    const database = new GatewayDatabase(":memory:");
    let state: AprsRuntimeState | undefined = aprsState("a", "N0CALL-7", 42);
    const eventTypes: string[] = [];
    const events = new DomainEventBus({ eventIdFactory: sequentialFactory() });
    events.subscribe((event) => eventTypes.push(event.type));
    let closes = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: events,
      stateProvider: () => state,
      outbox: { flush: async () => [] },
      monitorClientFactory: () => ({
        connect: async () => {
          state = undefined;
          return {
            close: async () => {
              closes += 1;
            },
          };
        },
      }),
    });

    await runtime.refreshMonitor();

    expect(closes).toBe(1);
    expect(eventTypes).not.toContain("aprs.monitor.connected");
    expect(runtime.status()).toMatchObject({
      monitorStatus: "idle",
      mappedCallsigns: 0,
      lastErrorCode: "APRS_PROVISION_UNAVAILABLE",
    });
    database.close();
  });

  it.each(["revoked", "rotated", "expired"] as const)(
    "invalidates an established monitor before a post-connect %s line can persist",
    async (change) => {
      const database = new GatewayDatabase(":memory:");
      let state: AprsRuntimeState | undefined = aprsState("a", "N0CALL-7", 42);
      let expired = false;
      let onLine: ((line: string) => void) | undefined;
      let connections = 0;
      let closes = 0;
      const filters: string[] = [];
      const eventTypes: string[] = [];
      const events = new DomainEventBus({
        eventIdFactory: sequentialFactory(),
      });
      events.subscribe((event) => eventTypes.push(event.type));
      const runtime = new AprsGatewayRuntime({
        database,
        eventBus: events,
        stateProvider: () => (expired ? undefined : state),
        outbox: { flush: async () => [] },
        monitorClientFactory: (filter) => {
          filters.push(filter);
          return {
            connect: async (listener) => {
              connections += 1;
              onLine = listener;
              return {
                close: async () => {
                  closes += 1;
                },
              };
            },
          };
        },
      });

      await runtime.refreshMonitor();
      const staleListener = onLine;
      if (!staleListener) {
        throw new Error("fixture monitor listener was not connected");
      }
      if (change === "rotated") {
        state = aprsState("b", "N1CALL-7", 7);
      } else if (change === "expired") {
        expired = true;
      } else {
        state = undefined;
      }

      expect(() =>
        staleListener(
          "N0CALL-7>APCM20:/180000z2502.85N/12131.05E> CM2/abcdef123456",
        ),
      ).not.toThrow();
      await waitFor(() =>
        change === "rotated"
          ? connections === 2 &&
            closes === 1 &&
            runtime.status().monitorStatus === "connected"
          : closes === 1 && runtime.status().monitorStatus === "idle",
      );

      expect(
        database.connection
          .prepare("SELECT * FROM aprs_remote_high_water")
          .all(),
      ).toEqual([]);
      expect(eventTypes).not.toContain("aprs.monitor.observed");
      expect(filters).toEqual(
        change === "rotated"
          ? ["b/N0CALL-7/TEST01-7", "b/AB12CD-7/N1CALL-7"]
          : ["b/N0CALL-7/TEST01-7"],
      );
      expect(runtime.status()).toMatchObject(
        change === "rotated"
          ? { monitorStatus: "connected", mappedCallsigns: 1 }
          : {
              monitorStatus: "idle",
              mappedCallsigns: 0,
              lastErrorCode: "APRS_PROVISION_UNAVAILABLE",
            },
      );

      await runtime.stop();
      expect(closes).toBe(change === "rotated" ? 2 : 1);
      database.close();
    },
  );

  it("closes a monitor session that connects after stop begins", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      mapping("fixture-network-a", 42, "N0CALL-7"),
    ]);
    const events = new DomainEventBus({ eventIdFactory: sequentialFactory() });
    const types: string[] = [];
    events.subscribe((event) => types.push(event.type));
    const pendingSession = deferred<AprsIsRxSession>();
    const connectStarted = deferred<void>();
    let onLine: ((line: string) => void) | undefined;
    let closes = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: events,
      outbox: { flush: async () => [] },
      monitorClientFactory: () => ({
        connect: (listener) => {
          onLine = listener;
          connectStarted.resolve();
          return pendingSession.promise;
        },
      }),
    });

    const refresh = runtime.refreshMonitor();
    await connectStarted.promise;
    const stop = runtime.stop();
    let stopped = false;
    void stop.then(() => {
      stopped = true;
    });
    await Promise.resolve();

    expect(stopped).toBe(false);
    pendingSession.resolve({
      close: async () => {
        closes += 1;
      },
    });
    await Promise.all([refresh, stop]);

    expect(closes).toBe(1);
    expect(types).not.toContain("aprs.monitor.connected");
    expect(types).not.toContain("aprs.monitor.error");
    expect(runtime.status()).toMatchObject({
      running: false,
      monitorStatus: "stopped",
      mappedCallsigns: 0,
    });
    database.close();
    expect(() =>
      onLine?.("N0CALL-7>APCM20:/180000z2502.85N/12131.05E> CM2/abcdef123456"),
    ).not.toThrow();
    expect(types).not.toContain("aprs.monitor.observed");
  });

  it("surfaces a late monitor session close failure through stop", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      mapping("fixture-network-a", 42, "N0CALL-7"),
    ]);
    const pendingSession = deferred<AprsIsRxSession>();
    const connectStarted = deferred<void>();
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
      outbox: { flush: async () => [] },
      monitorClientFactory: () => ({
        connect: () => {
          connectStarted.resolve();
          return pendingSession.promise;
        },
      }),
    });

    const refresh = runtime.refreshMonitor();
    await connectStarted.promise;
    const stop = runtime.stop();
    const refreshFailure = expect(refresh).rejects.toMatchObject({
      code: "APRS_MONITOR_CLOSE_FAILED",
    });
    const stopFailure = expect(stop).rejects.toMatchObject({
      code: "APRS_MONITOR_CLOSE_FAILED",
    });
    pendingSession.resolve({
      close: async () => {
        throw new Error("fixture close failure");
      },
    });

    await Promise.all([refreshFailure, stopFailure]);
    expect(runtime.status()).toMatchObject({
      running: false,
      monitorStatus: "stopped",
      mappedCallsigns: 0,
    });
    database.close();
  });

  it("bounds a late monitor session close that never settles", async () => {
    vi.useFakeTimers();
    const database = new GatewayDatabase(":memory:");
    try {
      database.callmeshMappings.replace([
        mapping("fixture-network-a", 42, "N0CALL-7"),
      ]);
      const pendingSession = deferred<AprsIsRxSession>();
      const connectStarted = deferred<void>();
      const runtime = new AprsGatewayRuntime({
        database,
        eventBus: new DomainEventBus({ eventIdFactory: sequentialFactory() }),
        outbox: { flush: async () => [] },
        monitorClientFactory: () => ({
          connect: () => {
            connectStarted.resolve();
            return pendingSession.promise;
          },
        }),
      });

      const refresh = runtime.refreshMonitor();
      await connectStarted.promise;
      const stop = runtime.stop();
      const refreshFailure = expect(refresh).rejects.toMatchObject({
        code: "APRS_MONITOR_CLOSE_FAILED",
      });
      const stopFailure = expect(stop).rejects.toMatchObject({
        code: "APRS_MONITOR_CLOSE_FAILED",
      });
      pendingSession.resolve({
        close: () => new Promise<void>(() => undefined),
      });
      await Promise.resolve();
      await vi.advanceTimersByTimeAsync(10_000);

      await Promise.all([refreshFailure, stopFailure]);
      expect(runtime.status()).toMatchObject({
        running: false,
        monitorStatus: "stopped",
        mappedCallsigns: 0,
      });
    } finally {
      database.close();
      vi.useRealTimers();
    }
  });

  it("releases every queued monitor session across repeated start-stop cycles", async () => {
    const database = new GatewayDatabase(":memory:");
    database.callmeshMappings.replace([
      mapping("fixture-network-a", 42, "N0CALL-7"),
    ]);
    const events = new DomainEventBus({ eventIdFactory: sequentialFactory() });
    let connections = 0;
    let closes = 0;
    let flushes = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: events,
      flushIntervalMs: 100,
      monitorRefreshIntervalMs: 100,
      outbox: {
        flush: async () => {
          flushes += 1;
          return [];
        },
      },
      monitorClientFactory: () => ({
        connect: async () => {
          connections += 1;
          return {
            close: async () => {
              closes += 1;
            },
          };
        },
      }),
    });

    const cycles = 32;
    for (let index = 0; index < cycles; index += 1) {
      runtime.start();
      await Promise.all([runtime.flushNow(), runtime.refreshMonitor()]);
      await runtime.stop();
      expect(runtime.status()).toMatchObject({
        running: false,
        monitorStatus: "stopped",
        mappedCallsigns: 0,
      });
    }

    expect(connections).toBe(cycles);
    expect(closes).toBe(connections);
    expect(flushes).toBe(cycles);
    await Promise.all([runtime.flushNow(), runtime.refreshMonitor()]);
    expect(connections).toBe(cycles);
    expect(flushes).toBe(cycles);
    database.close();
  });
});

function mapping(meshNetworkId: string, nodeNum: number, callsign: string) {
  return {
    version: "mapping-v1",
    effectiveAt: "2026-07-18T00:00:00.000Z",
    meshNetworkId,
    nodeNum,
    callsign,
  };
}

function outboxEntry(status: "sent" | "failed"): AprsOutboxEntry {
  return {
    id: `outbox-${status}`,
    callsign: "N0CALL-7",
    canonicalEventId: `event-${status}`,
    data: "N0CALL-7>APCM20:test",
    status,
    deliveryStatus: status === "sent" ? "submitted" : "failed",
    attempts: status === "sent" ? 0 : 1,
    nextAttemptAt: "2026-07-18T00:00:00.000Z",
    createdAt: "2026-07-18T00:00:00.000Z",
    updatedAt: "2026-07-18T00:00:00.000Z",
    ...(status === "failed" ? { lastErrorCode: "APRS_TX_FAILED" } : {}),
  };
}

function aprsState(
  fingerprintCharacter: string,
  callsign: string,
  nodeNum: number,
): AprsRuntimeState {
  const fingerprint = fingerprintCharacter.repeat(64);
  const provision = {
    callsignBase: fingerprintCharacter === "a" ? "TEST01" : "AB12CD",
    ssid: -7,
    symbolTable: "/",
    symbolCode: ">",
    latitude: 25.0475,
    longitude: 121.5175,
  } as const;
  return {
    mappings: [mapping("fixture-network-a", nodeNum, callsign)],
    mappingsFingerprint: fingerprint,
    identity: deriveAprsRuntimeIdentity(provision),
    provision,
    provisionFingerprint: fingerprint,
  };
}

function sequentialFactory(): () => string {
  let index = 0;
  return () => `event-${++index}`;
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setImmediate(resolve));
  }
  throw new Error("fixture condition was not reached");
}

async function closeServer(server: net.Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}
