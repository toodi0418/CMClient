import { describe, expect, it } from "vitest";

import type { AprsIsRxSession } from "./aprs-monitor";
import type { AprsOutboxEntry } from "./aprs-outbox";
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

function sequentialFactory(): () => string {
  let index = 0;
  return () => `event-${++index}`;
}
