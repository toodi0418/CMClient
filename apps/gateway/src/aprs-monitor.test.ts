import { once } from "node:events";
import net, { type Server } from "node:net";

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
      loginLine: "user N0CALL pass -1 vers CMClient 2.0",
      filterExpression: "b/N0CALL-7",
    });

    const session = await client.connect((line) => received.push(line));
    await waitFor(() => loginLines.length === 1 && received.length === 1);

    expect(loginLines).toEqual([
      "user N0CALL pass -1 vers CMClient 2.0 filter b/N0CALL-7",
    ]);
    expect(received).toEqual([encode(event("2026-07-18T00:00:35.000Z"))]);
    await session.close();
    await close(server);
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
