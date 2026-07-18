import { once } from "node:events";
import net, { type Server } from "node:net";

import { describe, expect, it } from "vitest";

import { createMeshObservation } from "./observations";
import {
  AprsIsTcpClient,
  AprsOutboxRepository,
  AprsOutboxWorker,
} from "./aprs-outbox";
import { GatewayDatabase } from "./persistence/database";
import { PositionRepository, createCanonicalPositionEvent } from "./position";

describe("APRS outbox", () => {
  it("lists a bounded public projection without exposing APRS Data", () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database);
    const repository = new AprsOutboxRepository(database.connection);
    repository.enqueue({
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "N0CALL-7>APCM20:private deterministic data",
      now: "2026-07-18T00:00:00.000Z",
    });

    expect(repository.list(1)).toEqual([
      expect.objectContaining({
        callsign: "N0CALL-7",
        status: "queued",
      }),
    ]);
    expect(repository.list(1)[0]).not.toHaveProperty("data");
    expect(() => repository.list(201)).toThrow("APRS_OUTBOX_FAILED");
    database.close();
  });

  it("enforces callsign/event idempotency and retries a failed send", async () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database);
    const repository = new AprsOutboxRepository(database.connection);
    const first = repository.enqueue({
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "N0CALL-7>APCM20:fixture",
      now: "2026-07-18T00:00:00.000Z",
    });
    const repeated = repository.enqueue({
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "changed data must not replace first data",
      now: "2026-07-18T00:00:01.000Z",
    });
    let now = new Date("2026-07-18T00:00:00.000Z");
    let sends = 0;
    const worker = new AprsOutboxWorker(
      repository,
      {
        async send(): Promise<void> {
          sends += 1;
          if (sends === 1) {
            throw new Error("fixture send failure");
          }
        },
      },
      { clock: () => now, initialDelayMs: 1_000, maximumDelayMs: 1_000 },
    );

    expect(first.created).toBe(true);
    expect(repeated).toMatchObject({
      created: false,
      entry: { id: first.entry.id, data: first.entry.data },
    });
    await expect(worker.flush()).resolves.toMatchObject([
      { status: "failed", attempts: 1, lastErrorCode: "APRS_TX_FAILED" },
    ]);
    now = new Date("2026-07-18T00:00:01.000Z");
    await expect(worker.flush()).resolves.toMatchObject([
      { status: "sent", attempts: 1 },
    ]);
    expect(sends).toBe(2);
    database.close();
  });

  it("recovers an entry interrupted while sending", async () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database);
    const repository = new AprsOutboxRepository(database.connection);
    const entry = repository.enqueue({
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "N0CALL-7>APCM20:fixture",
      now: "2026-07-18T00:00:00.000Z",
    }).entry;
    repository.claimDue("2026-07-18T00:00:00.000Z");
    const worker = new AprsOutboxWorker(repository, {
      async send(): Promise<void> {
        return undefined;
      },
    });

    await worker.flush();

    const recovered = repository.find(entry.id);
    expect(recovered).toMatchObject({ status: "sent", attempts: 1 });
    expect(recovered?.lastErrorCode).toBeUndefined();
    database.close();
  });
});

describe("AprsIsTcpClient", () => {
  it("writes login and APRS Data lines to a loopback server", async () => {
    const lines: string[] = [];
    const server = net.createServer((socket) => {
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
    const client = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: address.port,
      loginLine: "user N0CALL pass -1 vers CMClient 2.0",
    });

    await client.send("N0CALL-7>APCM20:fixture");
    await waitFor(() => lines.length === 2);
    expect(lines).toEqual([
      "user N0CALL pass -1 vers CMClient 2.0",
      "N0CALL-7>APCM20:fixture",
    ]);
    await close(server);
  });

  it("rejects line injection in APRS-IS login and Data", async () => {
    expect(
      () =>
        new AprsIsTcpClient({
          host: "127.0.0.1",
          port: 14580,
          loginLine: "user N0CALL pass -1\r\nuser injected",
        }),
    ).toThrow("APRS_OUTBOX_FAILED");
    const client = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: 14580,
      loginLine: "user N0CALL pass -1",
    });
    await expect(client.send("line one\r\nline two")).rejects.toMatchObject({
      code: "APRS_OUTBOX_FAILED",
    });
  });
});

function persistedPositionEvent(database: GatewayDatabase) {
  const meshObservation = createMeshObservation({
    id: "mesh-observation-fixture",
    transport: "simulator",
    sessionConnectedAt: "2026-07-18T00:00:00.000Z",
    ingestedAt: "2026-07-18T00:00:01.000Z",
    serverIngestedAt: "2026-07-18T00:00:01.005Z",
    normalizedFromRadio: { schemaVersion: 1, kind: "other" },
  });
  database.meshObservations.insert(meshObservation);
  const repository = new PositionRepository(database.connection);
  const observation = repository.insertOrFindObservation({
    schemaVersion: 1,
    id: "position-observation-fixture",
    meshNetworkId: "fixture-network",
    nodeNum: 42,
    meshObservationId: meshObservation.id,
    gatewayId: "fixture-gateway",
    transport: "simulator",
    sessionConnectedAt: meshObservation.sessionConnectedAt,
    ingestedAt: meshObservation.ingestedAt,
    serverIngestedAt: meshObservation.serverIngestedAt,
    backlogClassification: "live",
    payloadHash: "a".repeat(64),
    position: {
      latitudeI: 250000000,
      longitudeI: 1215000000,
      precisionBits: 32,
      positionTimestampSeconds: 1784332800,
    },
  });
  const event = createCanonicalPositionEvent(observation).event;
  return repository.insertOrFindEvent(event).event;
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
