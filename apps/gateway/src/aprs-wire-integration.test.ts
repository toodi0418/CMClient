import { once } from "node:events";
import net, { type Server, type Socket } from "node:net";

import { describe, expect, it } from "vitest";

import type { PositionCanonicalEvent } from "@cmclient/contracts";

import {
  AprsIsMonitor,
  AprsIsRxClient,
  AprsRemoteHighWaterStore,
  type AprsMonitorResult,
} from "./aprs-monitor";
import { AprsIsTcpClient } from "./aprs-outbox";
import { encodeAprsPosition } from "./aprs-position";
import { GatewayDatabase } from "./persistence/database";

const PROVISION_FINGERPRINT = "a".repeat(64);

describe("APRS verified wire integration", () => {
  it("sends legacy qAO Data only after logresp and observes the same source plus Info", async () => {
    const sockets = new Set<Socket>();
    const verified = new WeakSet<Socket>();
    const activeLogins = new Set<string>();
    const loginBySocket = new WeakMap<Socket, string>();
    const loginLines: string[] = [];
    let observerSocket: Socket | undefined;
    let transmittedLine: string | undefined;
    let dataBeforeVerification = false;
    const server = net.createServer((socket) => {
      sockets.add(socket);
      socket.on("close", () => {
        sockets.delete(socket);
        const login = loginBySocket.get(socket);
        if (login) {
          activeLogins.delete(login);
        }
      });
      let buffer = "";
      socket.on("data", (chunk: Buffer) => {
        buffer += chunk.toString("utf8");
        const lines = buffer.split("\r\n");
        buffer = lines.pop() ?? "";
        for (const line of lines.filter(Boolean)) {
          if (line.startsWith("user ")) {
            const callsign = /^user\s+([^\s]+)/.exec(line)?.[1];
            if (!callsign || activeLogins.has(callsign)) {
              socket.end();
              continue;
            }
            activeLogins.add(callsign);
            loginBySocket.set(socket, callsign);
            loginLines.push(line);
            if (line.includes(" filter ")) {
              observerSocket = socket;
            }
            setTimeout(() => {
              verified.add(socket);
              socket.write(`# logresp ${callsign} verified, integration\r\n`);
            }, 20);
            continue;
          }
          if (!verified.has(socket)) {
            dataBeforeVerification = true;
          }
          transmittedLine = line;
          const separator = line.indexOf(":");
          const source = line.slice(0, line.indexOf(">"));
          if (observerSocket && observerSocket !== socket) {
            observerSocket.write(
              `${source}>APTMAG,MESHD*,qAO,TEST01:${line.slice(separator + 1)}\r\n`,
            );
          }
        }
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("APRS integration server did not bind");
    }

    const database = new GatewayDatabase(":memory:");
    const target = {
      callsign: "TRACK1-7",
      mappingVersion: "mapping-v1",
      meshNetworkId: "integration-network",
      nodeNum: 42,
    };
    const highWater = new AprsRemoteHighWaterStore(database.connection);
    const monitor = new AprsIsMonitor([target], highWater);
    const transmitterAuthorizationProvider = () => ({
      loginLine: "user TEST01 pass 11111 vers CMClient 2.0",
      provisionFingerprint: PROVISION_FINGERPRINT,
    });
    const observerAuthorizationProvider = () => ({
      loginLine: "user TEST01-C0 pass 11111 vers CMClient 2.0",
      provisionFingerprint: PROVISION_FINGERPRINT,
    });
    const receiver = new AprsIsRxClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: observerAuthorizationProvider,
      provisionFingerprint: PROVISION_FINGERPRINT,
      filterExpression: monitor.filterExpression(),
      timeoutMs: 2_000,
    });
    const transmitter = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: transmitterAuthorizationProvider,
      timeoutMs: 2_000,
    });
    let observed: AprsMonitorResult | undefined;

    try {
      const receiveSession = await receiver.connect((line) => {
        observed = monitor.observeLine(line, "2026-07-18T00:00:02.000Z");
      });
      const encoded = encodeAprsPosition(positionEvent(), {
        mappingCallsign: target.callsign,
        mappingSymbolTable: "/",
        mappingSymbolCode: ">",
        mappingComment: "Wire integration",
        provisionIgateCallsign: "TEST01",
      });

      await transmitter.send(encoded.data, PROVISION_FINGERPRINT);
      await waitFor(() => observed !== undefined);

      expect(dataBeforeVerification).toBe(false);
      expect(loginLines.map((line) => line.split(" ")[1])).toEqual([
        "TEST01-C0",
        "TEST01",
      ]);
      expect(transmittedLine).toBe(encoded.data);
      expect(transmittedLine).toMatch(/^TRACK1-7>APTMAG,MESHD\*,qAO,TEST01:!/);
      expect(observed).toMatchObject({ kind: "advanced" });
      expect(
        highWater.canUploadData(
          encoded.data,
          target,
          "2026-07-18T00:00:03.000Z",
        ),
      ).toBe(false);

      await transmitter.close();
      await receiveSession.close();
    } finally {
      await transmitter.close().catch(() => undefined);
      for (const socket of sockets) {
        socket.destroy();
      }
      await closeServer(server);
      database.close();
    }
  });
});

function positionEvent(): PositionCanonicalEvent {
  return {
    schemaVersion: 1,
    id: "position-wire-integration",
    canonicalKey: "b".repeat(64),
    meshNetworkId: "integration-network",
    nodeNum: 42,
    sourceObservationId: "observation-wire-integration",
    payloadHash: "c".repeat(64),
    eventTime: "2026-07-18T00:00:00.000Z",
    eventTimeSource: "position_timestamp",
    position: {
      latitudeI: 250_000_000,
      longitudeI: 1_215_000_000,
      precisionBits: 32,
      altitudeMslMeters: 10,
      groundSpeedMetersPerSecond: 4.5,
      groundTrackDegrees: 90,
    },
    createdAt: "2026-07-18T00:00:01.000Z",
  };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error("APRS integration observation timed out");
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
}
