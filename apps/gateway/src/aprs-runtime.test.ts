import { describe, expect, it, vi } from "vitest";

import type { AprsIsRxSession } from "./aprs-monitor";
import type { AprsOutboxEntry } from "./aprs-outbox";
import {
  deriveAprsRuntimeIdentity,
  type AprsRuntimeState,
} from "./aprs-identity";
import { AprsGatewayRuntime } from "./aprs-runtime";
import { DomainEventBus } from "./events";
import { GatewayDatabase } from "./persistence/database";

describe("AprsGatewayRuntime", () => {
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

    await runtime.flushNow();
    await runtime.refreshMonitor();
    onLine?.("N0CALL-7>APCM20:/180000z2502.85N/12131.05E> CM2/abcdef123456");

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
        "aprs.outbox.sent",
        "aprs.outbox.failed",
        "aprs.monitor.connected",
        "aprs.monitor.observed",
      ]),
    );
    expect(
      database.connection
        .prepare("SELECT callsign FROM aprs_remote_high_water")
        .all(),
    ).toEqual([{ callsign: "N0CALL-7" }]);

    await runtime.refreshMonitor();
    expect(closes).toBe(1);
    await runtime.stop();
    expect(closes).toBe(2);
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
      "CREATE TRIGGER fixture_reject_remote_high_water BEFORE INSERT ON aprs_remote_high_water BEGIN SELECT RAISE(ABORT, 'fixture persistence failure'); END",
    );

    expect(() =>
      onLine?.("N0CALL-7>APCM20:/180000z2502.85N/12131.05E> CM2/abcdef123456"),
    ).not.toThrow();
    expect(runtime.status()).toMatchObject({
      monitorStatus: "error",
      lastErrorCode: "APRS_MONITOR_PERSISTENCE_FAILED",
      mappedCallsigns: 1,
    });
    expect(errorCodes).toEqual(["APRS_MONITOR_PERSISTENCE_FAILED"]);

    database.connection.exec("DROP TRIGGER fixture_reject_remote_high_water");
    expect(() =>
      onLine?.("N0CALL-7>APCM20:/180001z2502.85N/12131.05E> CM2/bcdefa123456"),
    ).not.toThrow();

    expect(runtime.status()).toMatchObject({
      monitorStatus: "connected",
      mappedCallsigns: 1,
    });
    expect(runtime.status()).not.toHaveProperty("lastErrorCode");
    expect(
      database.connection
        .prepare(
          "SELECT latest_event_marker FROM aprs_remote_high_water WHERE callsign = ?",
        )
        .get("N0CALL-7"),
    ).toEqual({ latest_event_marker: "CM2/bcdefa123456" });
    expect(
      types.filter((type) => type === "aprs.monitor.connected"),
    ).toHaveLength(2);
    expect(
      types.filter((type) => type === "aprs.monitor.observed"),
    ).toHaveLength(1);

    await runtime.stop();
    database.close();
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
    const events = new DomainEventBus({ eventIdFactory: sequentialFactory() });
    const types: string[] = [];
    events.subscribe((event) => types.push(event.type));
    const pendingFlush = deferred<AprsOutboxEntry[]>();
    let flushes = 0;
    const runtime = new AprsGatewayRuntime({
      database,
      eventBus: events,
      outbox: {
        flush: () => {
          flushes += 1;
          return pendingFlush.promise;
        },
      },
      monitorClientFactory: () => ({
        connect: async () => ({ close: async () => undefined }),
      }),
    });

    const flush = runtime.flushNow();
    const stop = runtime.stop();
    let stopped = false;
    void stop.then(() => {
      stopped = true;
    });
    await Promise.resolve();

    expect(stopped).toBe(false);
    pendingFlush.resolve([outboxEntry("sent")]);
    await Promise.all([flush, stop]);

    expect(stopped).toBe(true);
    expect(types).not.toContain("aprs.outbox.sent");
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
    expect(filters).toEqual(["b/N0CALL-7", "b/N1CALL-7"]);
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
        change === "rotated" ? ["b/N0CALL-7", "b/N1CALL-7"] : ["b/N0CALL-7"],
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

    expect(connections).toBe(cycles * 2);
    expect(closes).toBe(connections);
    expect(flushes).toBe(cycles);
    await Promise.all([runtime.flushNow(), runtime.refreshMonitor()]);
    expect(connections).toBe(cycles * 2);
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
  return {
    mappings: [mapping("fixture-network-a", nodeNum, callsign)],
    mappingsFingerprint: fingerprint,
    identity: deriveAprsRuntimeIdentity({
      callsignBase: fingerprintCharacter === "a" ? "TEST01" : "AB12CD",
      ssid: -7,
      symbolTable: "/",
      symbolCode: ">",
    }),
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
