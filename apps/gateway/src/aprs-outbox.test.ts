import { once } from "node:events";
import net, { type Server, type Socket } from "node:net";

import { describe, expect, it } from "vitest";

import { createMeshObservation } from "./observations";
import { DomainEventBus } from "./events";
import { GatewayMaintenanceRuntime } from "./maintenance";
import {
  AprsRemoteHighWaterStore,
  parseCmClientAprsLine,
} from "./aprs-monitor";
import {
  AprsIsTcpClient,
  AprsOutboxRepository,
  AprsOutboxWorker,
  type AprsOutboxEntry,
  type EnqueueAprsResult,
  type AprsOutboxWorkerOptions,
  type AprsTransport,
} from "./aprs-outbox";
import { GatewayDatabase } from "./persistence/database";
import {
  PositionHighWaterStore,
  PositionRepository,
  createCanonicalPositionEvent,
} from "./position";

const PROVISION_FINGERPRINT = "a".repeat(64);
const ROTATED_PROVISION_FINGERPRINT = "b".repeat(64);

function authorization(
  loginLine: string,
  provisionFingerprint = PROVISION_FINGERPRINT,
) {
  return () => ({ loginLine, provisionFingerprint });
}

function enqueueProvisioned(
  repository: AprsOutboxRepository,
  input: Omit<
    Parameters<AprsOutboxRepository["enqueue"]>[0],
    "provisionFingerprint"
  >,
): EnqueueAprsResult {
  return repository.enqueue({
    ...input,
    provisionFingerprint: PROVISION_FINGERPRINT,
  });
}

function newWorker(
  repository: AprsOutboxRepository,
  transport: AprsTransport,
  options: Omit<AprsOutboxWorkerOptions, "authorizationProvider"> = {},
): AprsOutboxWorker {
  return new AprsOutboxWorker(repository, transport, {
    authorizationProvider: () => PROVISION_FINGERPRINT,
    ...options,
  });
}

function confirmEntry(
  repository: AprsOutboxRepository,
  entry: AprsOutboxEntry | undefined,
  observedAt: string,
): void {
  if (!entry) {
    throw new Error("fixture APRS outbox entry was not found");
  }
  const packet = parseCmClientAprsLine(entry.data);
  if (!packet) {
    throw new Error("fixture APRS packet did not parse");
  }
  expect(
    repository.confirmObserved(
      packet.callsign,
      packet.destination,
      packet.info,
      observedAt,
      PROVISION_FINGERPRINT,
    ),
  ).toHaveLength(1);
}

describe("APRS outbox", () => {
  it("enforces the 510-byte APRS Data boundary before persistence", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsOutboxRepository(database.connection);
    const prefix = "N0CALL-7>APTMAG:";
    const validData = `${prefix}${"x".repeat(510 - Buffer.byteLength(prefix))}`;
    const oversizedData = `${prefix}${"x".repeat(511 - Buffer.byteLength(prefix))}`;

    expect(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: persistedPositionEvent(database, "wire-510").id,
        data: validData,
        now: "2026-07-18T00:00:00.000Z",
      }).created,
    ).toBe(true);
    expect(() =>
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: persistedPositionEvent(database, "wire-511").id,
        data: oversizedData,
        now: "2026-07-18T00:00:00.000Z",
      }),
    ).toThrow("APRS_OUTBOX_FAILED");
    database.close();
  });

  it("discards an oversized row left by an older database before transport", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsOutboxRepository(database.connection);
    const queued = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: persistedPositionEvent(
          database,
          "legacy-oversized-wire",
        ).id,
        data: "N0CALL-7>APTMAG:valid",
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    const prefix = "N0CALL-7>APTMAG:";
    database.connection
      .prepare("UPDATE aprs_outbox SET data = ? WHERE id = ?")
      .run(
        `${prefix}${"x".repeat(511 - Buffer.byteLength(prefix))}`,
        queued.id,
      );
    let sends = 0;

    await expect(
      newWorker(repository, {
        send: async () => {
          sends += 1;
        },
      }).flush(),
    ).resolves.toEqual([]);
    expect(sends).toBe(0);
    expect(repository.find(queued.id)).toBeUndefined();
    database.close();
  });

  it("lists a bounded public projection without exposing APRS Data", () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database);
    const repository = new AprsOutboxRepository(database.connection);
    enqueueProvisioned(repository, {
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
    expect(repository.list(1)[0]).not.toHaveProperty("provisionFingerprint");
    expect(() => repository.list(201)).toThrow("APRS_OUTBOX_FAILED");
    database.close();
  });

  it("enforces callsign/event idempotency and retries a failed send", async () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database);
    const repository = new AprsOutboxRepository(database.connection);
    const first = enqueueProvisioned(repository, {
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "N0CALL-7>APCM20:fixture",
      now: "2026-07-18T00:00:00.000Z",
    });
    const repeated = enqueueProvisioned(repository, {
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "changed data must not replace first data",
      now: "2026-07-18T00:00:01.000Z",
    });
    let now = new Date("2026-07-18T00:00:00.000Z");
    let sends = 0;
    const worker = newWorker(
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
    expect(first.suppressed).toBe(false);
    expect(repeated).toMatchObject({
      created: false,
      suppressed: false,
      entry: {
        id: requiredOutbox(first).id,
        data: requiredOutbox(first).data,
      },
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
    const entry = enqueueProvisioned(repository, {
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "N0CALL-7>APCM20:fixture",
      now: "2026-07-18T00:00:00.000Z",
    });
    const storedEntry = requiredOutbox(entry);
    repository.claimDue("2026-07-18T00:00:00.000Z");
    const worker = newWorker(repository, {
      async send(): Promise<void> {
        return undefined;
      },
    });

    await worker.flush();

    const recovered = repository.find(storedEntry.id);
    expect(recovered).toMatchObject({ status: "sent", attempts: 1 });
    expect(recovered?.lastErrorCode).toBeUndefined();
    database.close();
  });

  it("advances delivery high-water only after an exact independent observation", () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database, "observer-confirmation");
    const repository = new AprsOutboxRepository(database.connection);
    const queued = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: event.id,
        data: "N0CALL-7>APTMAG,MESHD*,qAO,TEST01:!2500.00N/12130.00E>fixture",
        now: "2026-07-18T00:00:00.000Z",
      }),
    );

    repository.claimDue("2026-07-18T00:00:00.000Z", 1);
    const submitted = repository.markSubmitted(
      queued.id,
      "2026-07-18T00:00:01.000Z",
      PROVISION_FINGERPRINT,
    );

    expect(submitted.deliveryStatus).toBe("submitted");
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM aprs_delivery_high_water")
        .get(),
    ).toEqual({ count: 0 });
    expect(
      repository.confirmObserved(
        "N0CALL-7",
        "OTHER",
        "!2500.00N/12130.00E>fixture",
        "2026-07-18T00:00:02.000Z",
        PROVISION_FINGERPRINT,
      ),
    ).toEqual([]);

    const confirmed = repository.confirmObserved(
      "N0CALL-7",
      "APTMAG",
      "!2500.00N/12130.00E>fixture",
      "2026-07-18T00:00:03.000Z",
      PROVISION_FINGERPRINT,
    );
    expect(confirmed).toHaveLength(1);
    expect(confirmed[0]).toMatchObject({
      id: queued.id,
      deliveryStatus: "observer_confirmed",
    });
    expect(
      database.connection
        .prepare(
          "SELECT latest_canonical_event_id FROM aprs_delivery_high_water",
        )
        .get(),
    ).toEqual({ latest_canonical_event_id: event.id });
    database.close();
  });

  it("atomically preserves exact observer evidence received before socket completion", () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database, "observer-before-submit");
    const repository = new AprsOutboxRepository(database.connection);
    const data =
      "N0CALL-7>APTMAG,MESHD*,qAO,TEST01:!2500.00N/12130.00E>early-observer";
    const packet = parseCmClientAprsLine(data);
    if (!packet) {
      throw new Error("fixture APRS packet did not parse");
    }
    const queued = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: packet.callsign,
        canonicalEventId: event.id,
        data,
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    repository.claimDue("2026-07-18T00:00:01.000Z", 1);
    new AprsRemoteHighWaterStore(database.connection).apply(
      packet,
      {
        callsign: packet.callsign,
        mappingVersion: "mapping-v1",
        meshNetworkId: event.meshNetworkId,
        nodeNum: event.nodeNum,
      },
      "2026-07-18T00:00:02.000Z",
    );

    expect(
      repository.markSubmitted(
        queued.id,
        "2026-07-18T00:00:03.000Z",
        PROVISION_FINGERPRINT,
      ),
    ).toMatchObject({
      deliveryStatus: "observer_confirmed",
      observerConfirmedAt: "2026-07-18T00:00:02.000Z",
      submittedAt: "2026-07-18T00:00:03.000Z",
    });
    expect(
      database.connection
        .prepare(
          "SELECT latest_canonical_event_id FROM aprs_delivery_high_water",
        )
        .get(),
    ).toEqual({ latest_canonical_event_id: event.id });
    database.close();
  });

  it("isolates observer confirmation to the current provision fingerprint", () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database, "observer-provision");
    const repository = new AprsOutboxRepository(database.connection);
    const queued = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: event.id,
        data: "N0CALL-7>APTMAG,MESHD*,qAO,TEST01:!2500.00N/12130.00E>provision",
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    repository.claimDue("2026-07-18T00:00:00.000Z", 1);
    repository.markSubmitted(
      queued.id,
      "2026-07-18T00:00:01.000Z",
      PROVISION_FINGERPRINT,
    );

    expect(
      repository.confirmObserved(
        "N0CALL-7",
        "APTMAG",
        "!2500.00N/12130.00E>provision",
        "2026-07-18T00:00:02.000Z",
        ROTATED_PROVISION_FINGERPRINT,
      ),
    ).toEqual([]);
    expect(repository.find(queued.id)?.deliveryStatus).toBe("submitted");
    expect(
      repository.confirmObserved(
        "N0CALL-7",
        "APTMAG",
        "!2500.00N/12130.00E>provision",
        "2026-07-18T00:00:02.000Z",
        PROVISION_FINGERPRINT,
      ),
    ).toHaveLength(1);
    database.close();
  });

  it("reconciles exact persisted observer evidence but never a legacy wildcard", () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database, "observer-reconcile");
    const repository = new AprsOutboxRepository(database.connection);
    const data =
      "N0CALL-7>APTMAG,MESHD*,qAO,TEST01:!2500.00N/12130.00E>reconcile";
    const packet = parseCmClientAprsLine(data);
    if (!packet) {
      throw new Error("fixture APRS packet did not parse");
    }
    const queued = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: packet.callsign,
        canonicalEventId: event.id,
        data,
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    repository.claimDue("2026-07-18T00:00:00.000Z", 1);
    repository.markSubmitted(
      queued.id,
      "2026-07-18T00:00:01.000Z",
      PROVISION_FINGERPRINT,
    );
    database.connection
      .prepare(
        "INSERT INTO aprs_observed_packets (callsign, destination, info, first_observed_at, last_observed_at) VALUES (?, '', ?, ?, ?)",
      )
      .run(
        packet.callsign,
        packet.info,
        "2026-07-18T00:00:02.000Z",
        "2026-07-18T00:00:02.000Z",
      );

    expect(repository.reconcileObservedCache(PROVISION_FINGERPRINT)).toEqual(
      [],
    );
    new AprsRemoteHighWaterStore(database.connection).apply(
      packet,
      {
        callsign: packet.callsign,
        mappingVersion: "mapping-v1",
        meshNetworkId: "fixture-network",
        nodeNum: 42,
      },
      "2026-07-18T00:00:03.000Z",
    );
    expect(
      repository.reconcileObservedCache(ROTATED_PROVISION_FINGERPRINT),
    ).toEqual([]);
    expect(repository.reconcileObservedCache(PROVISION_FINGERPRINT)).toEqual([
      expect.objectContaining({
        id: queued.id,
        deliveryStatus: "observer_confirmed",
      }),
    ]);
    database.close();
  });

  it("fails closed when one observation matches multiple pending submissions", () => {
    const database = new GatewayDatabase(":memory:");
    const firstEvent = persistedPositionEvent(
      database,
      "observer-ambiguous-first",
      1_784_332_800,
      undefined,
      42,
    );
    const secondEvent = persistedPositionEvent(
      database,
      "observer-ambiguous-second",
      1_784_332_800,
      undefined,
      43,
    );
    const repository = new AprsOutboxRepository(database.connection);
    const data =
      "N0CALL-7>APTMAG,MESHD*,qAO,TEST01:!2500.00N/12130.00E>ambiguous";
    const first = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: firstEvent.id,
        data,
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    const second = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: secondEvent.id,
        data,
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    repository.claimDue("2026-07-18T00:00:00.000Z", 2);
    const packet = parseCmClientAprsLine(data);
    if (!packet) {
      throw new Error("fixture APRS packet did not parse");
    }
    new AprsRemoteHighWaterStore(database.connection).apply(
      packet,
      {
        callsign: packet.callsign,
        mappingVersion: "mapping-v1",
        meshNetworkId: firstEvent.meshNetworkId,
        nodeNum: firstEvent.nodeNum,
      },
      "2026-07-18T00:00:00.500Z",
    );
    repository.markSubmitted(
      first.id,
      "2026-07-18T00:00:01.000Z",
      PROVISION_FINGERPRINT,
    );
    repository.markSubmitted(
      second.id,
      "2026-07-18T00:00:02.000Z",
      PROVISION_FINGERPRINT,
    );

    expect(repository.reconcileObservedCache(PROVISION_FINGERPRINT)).toEqual(
      [],
    );
    expect(repository.find(first.id)?.deliveryStatus).toBe("submitted");
    expect(repository.find(second.id)?.deliveryStatus).toBe("submitted");
    database.close();
  });

  it("expires an unobserved submission without advancing delivery high-water", () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database, "observer-expiry");
    const repository = new AprsOutboxRepository(database.connection);
    const queued = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: event.id,
        data: "N0CALL-7>APTMAG,MESHD*,qAO,TEST01:!2500.00N/12130.00E>expiry",
        now: "2026-07-18T00:00:00.000Z",
      }),
    );

    repository.claimDue("2026-07-18T00:00:00.000Z", 1);
    repository.markSubmitted(
      queued.id,
      "2026-07-18T00:00:01.000Z",
      PROVISION_FINGERPRINT,
    );

    expect(repository.expireSubmitted("2026-07-18T03:00:00.999Z")).toBe(0);
    expect(repository.expireSubmitted("2026-07-18T03:00:01.000Z")).toBe(1);
    expect(repository.find(queued.id)?.deliveryStatus).toBe(
      "observation_expired",
    );
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM aprs_delivery_high_water")
        .get(),
    ).toEqual({ count: 0 });
    database.close();
  });

  it.each([
    ["revoked", undefined],
    ["rotated", ROTATED_PROVISION_FINGERPRINT],
  ] as const)(
    "permanently discards a queued row after its provision is %s",
    async (_case, invalidFingerprint) => {
      const database = new GatewayDatabase(":memory:");
      const repository = database.aprsOutbox;
      const event = persistedPositionEvent(database, `queued-${_case}`);
      const queued = enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: event.id,
        data: "N0CALL-7>APCM20:fixture",
        now: "2026-07-18T00:00:00.000Z",
      });
      let currentFingerprint: string | undefined = invalidFingerprint;
      let sends = 0;
      const worker = new AprsOutboxWorker(
        repository,
        {
          send: async () => {
            sends += 1;
          },
        },
        {
          authorizationProvider: () => currentFingerprint,
          clock: () => new Date("2026-07-18T00:00:00.000Z"),
        },
      );

      await expect(worker.flush()).resolves.toEqual([]);
      expect(repository.find(requiredOutbox(queued).id)).toBeUndefined();
      currentFingerprint = PROVISION_FINGERPRINT;
      await expect(worker.flush()).resolves.toEqual([]);
      expect(sends).toBe(0);
      database.close();
    },
  );

  it("revalidates immediately before the APRS Data write", async () => {
    const lines: string[] = [];
    const server = net.createServer((socket) => {
      let buffer = "";
      socket.on("data", (chunk: Buffer) => {
        buffer += chunk.toString("utf8");
        const parts = buffer.split("\r\n");
        buffer = parts.pop() ?? "";
        for (const line of parts.filter(Boolean)) {
          lines.push(line);
          if (line.startsWith("user ")) {
            socket.write("# logresp TEST01 verified, server fixture\r\n");
          }
        }
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    let providerCalls = 0;
    const client = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: () => {
        providerCalls += 1;
        return providerCalls < 3
          ? {
              loginLine: "user TEST01 pass 11111 vers CMClient 2.0",
              provisionFingerprint: PROVISION_FINGERPRINT,
            }
          : undefined;
      },
    });

    await expect(
      client.send("N0CALL-7>APCM20:fixture", PROVISION_FINGERPRINT),
    ).rejects.toMatchObject({ code: "APRS_PROVISION_UNAVAILABLE" });
    await waitFor(() => lines.length === 1);

    expect(lines).toEqual(["user TEST01 pass 11111 vers CMClient 2.0"]);
    await close(server);
  });

  it("coalesces concurrent flush calls without reclaiming an active send", async () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database);
    const repository = new AprsOutboxRepository(database.connection);
    const entry = enqueueProvisioned(repository, {
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "N0CALL-7>APCM20:fixture",
      now: "2026-07-18T00:00:00.000Z",
    });
    const storedEntry = requiredOutbox(entry);
    let release = (): void => undefined;
    let sends = 0;
    const worker = newWorker(repository, {
      send: async () => {
        sends += 1;
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      },
    });

    const first = worker.flush();
    const concurrent = worker.flush();
    expect(concurrent).toBe(first);
    expect(repository.find(storedEntry.id)?.status).toBe("sending");
    expect(sends).toBe(1);

    release();
    await expect(Promise.all([first, concurrent])).resolves.toEqual([
      [expect.objectContaining({ id: storedEntry.id, status: "sent" })],
      [expect.objectContaining({ id: storedEntry.id, status: "sent" })],
    ]);
    expect(sends).toBe(1);
    database.close();
  });

  it("stops an active batch and releases remaining work when its gate closes", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsOutboxRepository(database.connection);
    const first = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: persistedPositionEvent(
          database,
          "batch-fence-first",
          1_784_332_800,
          undefined,
          42,
        ).id,
        data: "N0CALL-7>APCM20:first",
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    const second = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N1CALL-7",
        canonicalEventId: persistedPositionEvent(
          database,
          "batch-fence-second",
          1_784_332_801,
          undefined,
          43,
        ).id,
        data: "N1CALL-7>APCM20:second",
        now: "2026-07-18T00:00:00.001Z",
      }),
    );
    let allowed = true;
    const transmitted: string[] = [];
    const worker = newWorker(repository, {
      send: async (data, _fingerprint, _session, transmissionGate) => {
        expect(transmissionGate?.()).toBe(true);
        transmitted.push(data);
        allowed = false;
      },
    });

    await expect(worker.flush(undefined, () => allowed)).resolves.toEqual([
      expect.objectContaining({ id: first.id, deliveryStatus: "submitted" }),
    ]);
    expect(transmitted).toEqual([first.data]);
    expect(repository.find(first.id)?.deliveryStatus).toBe("submitted");
    expect(repository.find(second.id)).toMatchObject({
      status: "queued",
      deliveryStatus: "queued",
      attempts: 0,
    });
    database.close();
  });

  it("deletes only sent outbox rows older than the retention cutoff", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsOutboxRepository(database.connection);
    const oldEvent = persistedPositionEvent(database, "old");
    const old = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: oldEvent.id,
        data: "N0CALL-7>APCM20:old",
        now: "2026-01-01T00:00:00.000Z",
      }),
    );
    new PositionHighWaterStore(database.connection).apply(
      oldEvent,
      { callsign: "N0CALL-7", mappingVersion: "mapping-v1" },
      "2026-01-01T00:00:00.000Z",
    );
    repository.claimDue("2026-01-01T00:00:00.000Z", 1);
    repository.markSent(
      old.id,
      "2026-01-01T00:00:01.000Z",
      PROVISION_FINGERPRINT,
    );
    confirmEntry(repository, old, "2026-01-01T00:00:02.000Z");

    const recent = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: persistedPositionEvent(
          database,
          "recent",
          1_784_332_801,
        ).id,
        data: "N0CALL-7>APCM20:recent",
        now: "2026-07-17T00:00:00.000Z",
      }),
    );
    repository.claimDue("2026-07-17T00:00:00.000Z", 1);
    repository.markSent(
      recent.id,
      "2026-07-17T00:00:01.000Z",
      PROVISION_FINGERPRINT,
    );
    confirmEntry(repository, recent, "2026-07-17T00:00:02.000Z");
    const queued = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N1CALL-7",
        canonicalEventId: persistedPositionEvent(database, "queued").id,
        data: "N1CALL-7>APCM20:queued",
        now: "2026-01-01T00:00:00.000Z",
      }),
    );

    expect(repository.deleteSentBefore("2026-06-01T00:00:00.000Z", 1)).toBe(1);
    expect(repository.find(old.id)).toBeUndefined();
    expect(repository.find(recent.id)?.status).toBe("sent");
    expect(repository.find(queued.id)?.status).toBe("queued");
    expect(() => repository.deleteSentBefore("invalid", 1)).toThrow(
      "APRS_OUTBOX_FAILED",
    );
    database.close();
  });

  it("keeps exact delivery idempotency after sent retention and mapping rotation", async () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database, "rotation");
    const repository = new AprsOutboxRepository(database.connection);
    const highWater = new PositionHighWaterStore(database.connection);
    const target = { callsign: "N0CALL-7", mappingVersion: "mapping-v1" };
    let firstOutboxId = "";
    const accepted = highWater.apply(
      event,
      target,
      "2026-07-18T00:00:02.000Z",
      {
        onAccepted: (acceptedEvent) => {
          firstOutboxId = requiredOutbox(
            enqueueProvisioned(repository, {
              callsign: target.callsign,
              canonicalEventId: acceptedEvent.id,
              data: "N0CALL-7>APCM20:rotation",
              now: "2026-07-18T00:00:02.000Z",
            }),
          ).id;
        },
      },
    );
    expect(accepted.decision.code).toBe("POSITION_ACCEPTED");
    repository.claimDue("2026-07-18T00:00:02.000Z", 1);
    repository.markSent(
      firstOutboxId,
      "2026-07-18T00:00:03.000Z",
      PROVISION_FINGERPRINT,
    );
    confirmEntry(
      repository,
      repository.find(firstOutboxId),
      "2026-07-18T00:00:04.000Z",
    );
    expect(repository.deleteSentBefore("2027-01-01T00:00:00.000Z", 1)).toBe(1);
    expect(repository.find(firstOutboxId)).toBeUndefined();

    let reenqueues = 0;
    const rotated = highWater.apply(
      event,
      { ...target, mappingVersion: "mapping-v2" },
      "2027-01-01T00:00:01.000Z",
      {
        onAccepted: () => {
          reenqueues += 1;
        },
      },
    );
    let sends = 0;
    const worker = newWorker(
      repository,
      {
        send: async () => {
          sends += 1;
        },
      },
      { clock: () => new Date("2026-07-18T00:00:02.000Z") },
    );
    await worker.flush();

    expect(rotated.decision.code).not.toBe("POSITION_ACCEPTED");
    expect(reenqueues).toBe(0);
    expect(sends).toBe(0);
    expect(repository.list(10)).toEqual([]);
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM node_position_state")
        .get()?.count,
    ).toBe(1);
    database.close();
  });

  it("removes an older failed retry when a newer event is queued and delivered", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const firstEvent = persistedPositionEvent(
      database,
      "retry-first",
      1_784_332_800,
      10,
    );
    const secondEvent = persistedPositionEvent(
      database,
      "retry-second",
      1_784_332_801,
      11,
    );
    const first = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: firstEvent.id,
        data: "N0CALL-7>APCM20:first",
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    let now = new Date("2026-07-18T00:00:00.000Z");
    const transmitted: string[] = [];
    const worker = newWorker(
      repository,
      {
        async send(data): Promise<void> {
          transmitted.push(data);
          if (data.endsWith(":first")) {
            throw new Error("fixture outage");
          }
        },
      },
      { clock: () => now, initialDelayMs: 1_000, maximumDelayMs: 1_000 },
    );

    await expect(worker.flush()).resolves.toMatchObject([
      { id: first.id, status: "failed" },
    ]);
    now = new Date("2026-07-18T00:00:00.500Z");
    const second = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: secondEvent.id,
        data: "N0CALL-7>APCM20:second",
        now: now.toISOString(),
      }),
    );
    expect(repository.find(first.id)).toBeUndefined();
    await expect(worker.flush()).resolves.toMatchObject([
      { id: second.id, status: "sent" },
    ]);
    now = new Date("2026-07-18T00:00:01.000Z");
    await expect(worker.flush()).resolves.toEqual([]);

    expect(transmitted).toEqual([
      "N0CALL-7>APCM20:first",
      "N0CALL-7>APCM20:second",
    ]);
    database.close();
  });

  it("plateaus failed outbox and position history across a sustained outage", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const cycles = 33;
    const samplesPerCycle = 64;
    const maintenance = new GatewayMaintenanceRuntime({
      database,
      eventBus: new DomainEventBus(),
      positionBatchSize: 1_000,
      observationBatchSize: 4_000,
      clock: () => new Date("2027-07-18T00:00:00.000Z"),
    });
    for (let cycle = 0; cycle < cycles; cycle += 1) {
      for (let offset = 0; offset < samplesPerCycle; offset += 1) {
        const index = cycle * samplesPerCycle + offset;
        const now = new Date(
          Date.parse("2026-07-18T00:00:00.000Z") + index * 1_000,
        ).toISOString();
        const event = persistedPositionEvent(
          database,
          `outage-${index}`,
          1_784_332_800 + index,
          index,
        );
        const entry = requiredOutbox(
          enqueueProvisioned(repository, {
            callsign: "N0CALL-7",
            canonicalEventId: event.id,
            data: `N0CALL-7>APCM20:outage-${index}`,
            now,
          }),
        );
        repository.claimDue(now, 1);
        repository.markFailed(entry.id, now, 60_000, "APRS_TX_FAILED");
      }
      maintenance.runCycle();
      for (const table of [
        "aprs_outbox",
        "position_events",
        "position_observations",
        "mesh_observations",
      ]) {
        expect(
          database.connection
            .prepare(`SELECT COUNT(*) AS count FROM ${table}`)
            .get()?.count,
        ).toBe(1);
      }
    }

    expect(database.integrityCheck()).toBe("ok");
    database.close();
  }, 20_000);

  it("rechecks a claimed row against a newer durable pending watermark before I/O", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const first = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: persistedPositionEvent(
          database,
          "claimed-first",
          1_784_332_800,
          20,
        ).id,
        data: "N0CALL-7>APCM20:claimed-first",
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    repository.claimDue("2026-07-18T00:00:00.000Z", 1);
    expect(repository.find(first.id)?.status).toBe("sending");
    const second = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: persistedPositionEvent(
          database,
          "claimed-second",
          1_784_332_801,
          21,
        ).id,
        data: "N0CALL-7>APCM20:claimed-second",
        now: "2026-07-18T00:00:01.000Z",
      }),
    );
    expect(
      repository.prepareSend(
        first.id,
        "2026-07-18T00:00:01.000Z",
        1_000,
        PROVISION_FINGERPRINT,
      ),
    ).toEqual({ authorized: false });
    expect(repository.find(first.id)).toBeUndefined();
    const transmitted: string[] = [];
    const worker = newWorker(
      repository,
      { send: async (data) => void transmitted.push(data) },
      { clock: () => new Date("2026-07-18T00:00:01.000Z") },
    );

    await expect(worker.flush()).resolves.toMatchObject([
      { id: second.id, status: "sent" },
    ]);
    expect(transmitted).toEqual(["N0CALL-7>APCM20:claimed-second"]);
    database.close();
  });

  it("does not derive temporal order from a different observed Info payload", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const event = persistedPositionEvent(
      database,
      "remote-newer",
      1_784_332_800,
      20,
    );
    const entry = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: event.id,
        data: "N0CALL-7>APCM20:remote-newer",
        now: "2026-07-18T00:00:00.000Z",
        order: {
          meshNetworkId: event.meshNetworkId,
          nodeNum: event.nodeNum,
          mappingVersion: "mapping-v1",
          eventTime: event.eventTime!,
          sequenceNumber: event.sequenceNumber!,
        },
      }),
    );
    const remote = parseCmClientAprsLine(
      "N0CALL-7>APTMAG,MESHD*,qAR,N1GATE-10:remote-different",
    );
    if (!remote) {
      throw new Error("invalid observer fixture");
    }
    new AprsRemoteHighWaterStore(database.connection).apply(
      remote,
      {
        callsign: "N0CALL-7",
        mappingVersion: "mapping-v2",
        meshNetworkId: event.meshNetworkId,
        nodeNum: event.nodeNum,
      },
      "2026-07-18T00:00:01.000Z",
    );
    let sends = 0;
    const worker = newWorker(
      repository,
      {
        send: async () => {
          sends += 1;
        },
      },
      { clock: () => new Date("2026-07-18T00:00:02.000Z") },
    );

    await expect(worker.flush()).resolves.toMatchObject([
      { id: entry.id, status: "sent" },
    ]);
    expect(sends).toBe(1);
    database.close();
  });

  it("suppresses a byte-identical Info observed through a different q path", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const event = persistedPositionEvent(
      database,
      "remote-exact",
      1_784_332_800,
      20,
    );
    const eventTime = event.eventTime!;
    const entry = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: event.id,
        data: "N0CALL-7>APCM20:remote-exact",
        now: "2026-07-18T00:00:00.000Z",
        order: {
          meshNetworkId: event.meshNetworkId,
          nodeNum: event.nodeNum,
          mappingVersion: "mapping-v1",
          eventTime,
          sequenceNumber: event.sequenceNumber!,
        },
      }),
    );
    const remote = parseCmClientAprsLine(
      "N0CALL-7>APCM20,MESHD*,qAR,N1GATE-10:remote-exact",
    );
    if (!remote) {
      throw new Error("invalid observer fixture");
    }
    new AprsRemoteHighWaterStore(database.connection).apply(
      remote,
      {
        callsign: "N0CALL-7",
        mappingVersion: "mapping-v1",
        meshNetworkId: event.meshNetworkId,
        nodeNum: event.nodeNum,
      },
      "2026-07-18T00:00:01.000Z",
    );
    let sends = 0;

    await newWorker(
      repository,
      {
        send: async () => {
          sends += 1;
        },
      },
      { clock: () => new Date("2026-07-18T00:00:02.000Z") },
    ).flush();

    expect(sends).toBe(0);
    expect(repository.find(entry.id)).toBeUndefined();
    database.close();
  });

  it("checks the permanent legacy barrier at enqueue and final pre-send", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const older = persistedPositionEvent(
      database,
      "legacy-barrier-older",
      1_784_332_800,
      10,
    );
    const newer = persistedPositionEvent(
      database,
      "legacy-barrier-newer",
      1_784_332_801,
      11,
    );
    const first = requiredOutbox(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: older.id,
        data: "N0CALL-7>APCM20:legacy-barrier",
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    expect(repository.claimDue("2026-07-18T00:00:00.000Z", 1)).toHaveLength(1);
    database.connection
      .prepare(
        "INSERT INTO aprs_legacy_submission_barriers (mesh_network_id, node_num, callsign, latest_canonical_event_id, latest_event_time, latest_sequence_epoch, latest_sequence_number, latest_mapping_version, submitted_at) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, ?)",
      )
      .run(
        newer.meshNetworkId,
        newer.nodeNum,
        "N0CALL-7",
        newer.id,
        newer.eventTime!,
        newer.sequenceNumber!,
        "2026-07-18T00:00:01.000Z",
      );

    expect(
      repository.prepareSend(
        first.id,
        "2026-07-18T00:00:01.000Z",
        1_000,
        PROVISION_FINGERPRINT,
      ),
    ).toEqual({ authorized: false });
    expect(repository.find(first.id)).toBeUndefined();
    expect(
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: older.id,
        data: "N0CALL-7>APCM20:legacy-barrier",
        now: "2026-07-18T00:00:02.000Z",
      }),
    ).toEqual({ created: false, suppressed: true });
    database.close();
  });

  it("persists a bounded cross-mapping conflict and performs no ambiguous upload", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const highWater = new PositionHighWaterStore(database.connection);
    const firstEvent = persistedPositionEvent(
      database,
      "mapping-conflict-a",
      1_784_332_800,
      30,
    );
    const secondEvent = persistedPositionEvent(
      database,
      "mapping-conflict-b",
      1_784_332_800,
      31,
    );
    const thirdEvent = persistedPositionEvent(
      database,
      "mapping-conflict-c",
      1_784_332_800,
      32,
    );
    const enqueueFromAccepted = (
      event: typeof firstEvent,
      mappingVersion: string,
      now: string,
    ) =>
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: event.id,
        data: `N0CALL-7>APCM20:${mappingVersion}`,
        now,
        order: {
          meshNetworkId: event.meshNetworkId,
          nodeNum: event.nodeNum,
          mappingVersion,
          ...(event.eventTime ? { eventTime: event.eventTime } : {}),
          ...(event.sequenceEpoch === undefined
            ? {}
            : { sequenceEpoch: event.sequenceEpoch }),
          ...(event.sequenceNumber === undefined
            ? {}
            : { sequenceNumber: event.sequenceNumber }),
        },
      });
    let firstEnqueue: EnqueueAprsResult | undefined;
    let secondEnqueue: EnqueueAprsResult | undefined;
    const first = highWater.apply(
      firstEvent,
      { callsign: "N0CALL-7", mappingVersion: "mapping-v1" },
      "2026-07-18T00:00:01.000Z",
      {
        onAccepted: (event) => {
          firstEnqueue = enqueueFromAccepted(
            event,
            "mapping-v1",
            "2026-07-18T00:00:01.000Z",
          );
        },
      },
    );
    const second = highWater.apply(
      secondEvent,
      { callsign: "N0CALL-7", mappingVersion: "mapping-v2" },
      "2026-07-18T00:00:02.000Z",
      {
        onAccepted: (event) => {
          secondEnqueue = enqueueFromAccepted(
            event,
            "mapping-v2",
            "2026-07-18T00:00:02.000Z",
          );
        },
      },
    );
    const thirdEnqueue = enqueueFromAccepted(
      { ...thirdEvent, sequenceEpoch: 0 },
      "mapping-v3",
      "2026-07-18T00:00:03.000Z",
    );
    expect(repository.claimDue("2026-07-18T00:00:03.000Z", 10)).toHaveLength(1);
    expect(
      database.connection
        .prepare(
          "SELECT COUNT(*) AS count FROM aprs_outbox WHERE status = 'sending'",
        )
        .get()?.count,
    ).toBe(1);
    repository.resumeInterrupted("2026-07-18T00:00:03.000Z");
    const transmitted: string[] = [];
    const worker = newWorker(
      repository,
      { send: async (data) => void transmitted.push(data) },
      { clock: () => new Date("2026-07-18T00:00:03.000Z") },
    );

    await expect(worker.flush()).resolves.toMatchObject([
      { status: "failed", lastErrorCode: "APRS_ORDER_UNPROVEN" },
    ]);
    expect(first.decision.code).toBe("POSITION_ACCEPTED");
    expect(second.decision.code).toBe("POSITION_ACCEPTED");
    expect(firstEnqueue).toMatchObject({ created: true, suppressed: false });
    expect(secondEnqueue).toMatchObject({ created: true, suppressed: true });
    expect(secondEnqueue?.entry).toMatchObject({
      status: "failed",
      deliveryStatus: "failed",
    });
    expect(thirdEnqueue).toEqual({ created: false, suppressed: true });
    expect(transmitted).toEqual([]);
    expect(repository.list(10)).toHaveLength(2);
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM node_position_state")
        .get()?.count,
    ).toBe(2);
    expect(
      database.connection
        .prepare("SELECT COUNT(*) AS count FROM position_decisions")
        .get()?.count,
    ).toBe(2);
    database.close();
  });

  it("keeps mapping-scoped reboot epochs in the outbox snapshot only", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const target = { callsign: "N0CALL-7", mappingVersion: "mapping-v1" };
    const firstEvent = persistedPositionEvent(
      database,
      "epoch-before-reboot",
      1_784_332_800,
      100,
    );
    const rebootEvent = persistedPositionEvent(
      database,
      "epoch-after-reboot",
      1_784_332_801,
      1,
    );
    const enqueue = (
      event: typeof firstEvent,
      now: string,
    ): EnqueueAprsResult =>
      enqueueProvisioned(repository, {
        callsign: target.callsign,
        canonicalEventId: event.id,
        data: `N0CALL-7>APCM20:${event.sequenceNumber}`,
        now,
        order: {
          meshNetworkId: event.meshNetworkId,
          nodeNum: event.nodeNum,
          mappingVersion: target.mappingVersion,
          ...(event.eventTime ? { eventTime: event.eventTime } : {}),
          ...(event.sequenceEpoch === undefined
            ? {}
            : { sequenceEpoch: event.sequenceEpoch }),
          ...(event.sequenceNumber === undefined
            ? {}
            : { sequenceNumber: event.sequenceNumber }),
        },
      });
    new PositionHighWaterStore(database.connection).apply(
      firstEvent,
      target,
      "2026-07-18T00:00:01.000Z",
      {
        onAccepted: (event) => void enqueue(event, "2026-07-18T00:00:01.000Z"),
      },
    );
    let rebootEnqueue: EnqueueAprsResult | undefined;
    const reboot = new PositionHighWaterStore(database.connection).apply(
      rebootEvent,
      target,
      "2026-07-18T00:00:02.000Z",
      {
        onAccepted: (event) => {
          rebootEnqueue = enqueue(event, "2026-07-18T00:00:02.000Z");
        },
      },
    );
    const rebootOutbox = requiredOutbox(rebootEnqueue!);

    expect(reboot.event.sequenceEpoch).toBe(1);
    expect(rebootOutbox).toMatchObject({
      mappingVersion: "mapping-v1",
      sequenceEpoch: 1,
      sequenceNumber: 1,
    });
    expect(
      database.connection
        .prepare(
          "SELECT sequence_epoch FROM position_events WHERE id IN (?, ?) ORDER BY id",
        )
        .all(firstEvent.id, rebootEvent.id),
    ).toEqual([{ sequence_epoch: null }, { sequence_epoch: null }]);
    await newWorker(
      repository,
      { send: async () => undefined },
      {
        clock: () => new Date("2026-07-18T00:00:02.000Z"),
      },
    ).flush();
    confirmEntry(
      repository,
      repository.find(rebootOutbox.id),
      "2026-07-18T00:00:03.000Z",
    );
    expect(
      database.connection
        .prepare(
          "SELECT latest_mapping_version, latest_sequence_epoch, latest_sequence_number FROM aprs_delivery_high_water",
        )
        .get(),
    ).toEqual({
      latest_mapping_version: "mapping-v1",
      latest_sequence_epoch: 1,
      latest_sequence_number: 1,
    });
    database.close();
  });

  it("does not rewrite a queued snapshot when the mapping rotates before send", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const highWater = new PositionHighWaterStore(database.connection);
    const priorEvent = persistedPositionEvent(
      database,
      "rotation-prior",
      1_784_332_800,
      100,
    );
    const rebootEvent = persistedPositionEvent(
      database,
      "rotation-reboot",
      1_784_332_801,
      1,
    );
    const enqueue = (mappingVersion: string, accepted: typeof priorEvent) =>
      enqueueProvisioned(repository, {
        callsign: "N0CALL-7",
        canonicalEventId: accepted.id,
        data: "N0CALL-7>APCM20:stable",
        now: "2026-07-18T00:00:01.000Z",
        order: {
          meshNetworkId: accepted.meshNetworkId,
          nodeNum: accepted.nodeNum,
          mappingVersion,
          ...(accepted.eventTime ? { eventTime: accepted.eventTime } : {}),
          ...(accepted.sequenceEpoch === undefined
            ? {}
            : { sequenceEpoch: accepted.sequenceEpoch }),
          ...(accepted.sequenceNumber === undefined
            ? {}
            : { sequenceNumber: accepted.sequenceNumber }),
        },
      });
    let original: EnqueueAprsResult | undefined;
    let rotated: EnqueueAprsResult | undefined;
    highWater.apply(
      priorEvent,
      { callsign: "N0CALL-7", mappingVersion: "mapping-v1" },
      "2026-07-18T00:00:01.000Z",
      {
        onAccepted: (accepted) => {
          enqueue("mapping-v1", accepted);
        },
      },
    );
    const reboot = highWater.apply(
      rebootEvent,
      { callsign: "N0CALL-7", mappingVersion: "mapping-v1" },
      "2026-07-18T00:00:02.000Z",
      {
        onAccepted: (accepted) => {
          original = enqueue("mapping-v1", accepted);
        },
      },
    );
    const remapped = highWater.apply(
      rebootEvent,
      { callsign: "N0CALL-7", mappingVersion: "mapping-v2" },
      "2026-07-18T00:00:03.000Z",
      {
        onAccepted: (accepted) => {
          rotated = enqueue("mapping-v2", accepted);
        },
      },
    );
    const queued = requiredOutbox(original!);

    expect(reboot.event.sequenceEpoch).toBe(1);
    expect(remapped.event.sequenceEpoch).toBe(0);
    expect(rotated).toMatchObject({
      created: false,
      suppressed: false,
      entry: { id: queued.id, mappingVersion: "mapping-v1" },
    });
    expect(repository.find(queued.id)).toMatchObject({
      mappingVersion: "mapping-v1",
      sequenceEpoch: 1,
      sequenceNumber: 1,
    });
    expect(
      database.connection
        .prepare("SELECT sequence_epoch FROM position_events WHERE id = ?")
        .get(rebootEvent.id),
    ).toEqual({ sequence_epoch: null });
    let sends = 0;
    await newWorker(
      repository,
      {
        send: async () => {
          sends += 1;
        },
      },
      { clock: () => new Date("2026-07-18T00:00:03.000Z") },
    ).flush();
    confirmEntry(
      repository,
      repository.find(queued.id),
      "2026-07-18T00:00:04.000Z",
    );
    expect(sends).toBe(1);
    expect(
      database.connection
        .prepare(
          "SELECT latest_mapping_version, latest_sequence_epoch, latest_sequence_number FROM aprs_delivery_high_water",
        )
        .get(),
    ).toEqual({
      latest_mapping_version: "mapping-v1",
      latest_sequence_epoch: 1,
      latest_sequence_number: 1,
    });
    database.close();
  });
});

describe("AprsIsTcpClient", () => {
  it("waits for a fragmented verified logresp and reuses one provision session", async () => {
    const sessions: string[][] = [];
    const server = net.createServer((socket) => {
      const lines: string[] = [];
      sessions.push(lines);
      collectFixtureLines(socket, lines, (line) => {
        if (line.startsWith("user ")) {
          socket.write("# fixture banner\r\n# log");
          setImmediate(() => {
            socket.write("resp TEST01 verified, server fixture\r\n");
          });
        }
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
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
    });

    await client.send("N0CALL-7>APCM20:fixture-one", PROVISION_FINGERPRINT);
    await client.send("N0CALL-7>APCM20:fixture-two", PROVISION_FINGERPRINT);
    await waitFor(() => sessions[0]?.length === 3);
    expect(sessions).toEqual([
      [
        "user TEST01 pass 11111 vers CMClient 2.0",
        "N0CALL-7>APCM20:fixture-one",
        "N0CALL-7>APCM20:fixture-two",
      ],
    ]);
    await client.close();
    await close(server);
  });

  it("requires the prepared verified session instead of silently reopening", async () => {
    const sessions: string[][] = [];
    const server = net.createServer((socket) => {
      const lines: string[] = [];
      sessions.push(lines);
      collectFixtureLines(socket, lines, (line) => {
        if (line.startsWith("user ")) {
          socket.write("# logresp TEST01 verified, server fixture\r\n");
        }
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
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
    });

    const first = await client.prepareVerifiedSession(PROVISION_FINGERPRINT);
    expect(first).toEqual({ generation: 1 });
    await client.close();
    await expect(
      client.send(
        "N0CALL-7>APCM20:must-not-send",
        PROVISION_FINGERPRINT,
        first,
      ),
    ).rejects.toMatchObject({ code: "APRS_OUTBOX_FAILED" });
    expect(sessions).toEqual([["user TEST01 pass 11111 vers CMClient 2.0"]]);

    const second = await client.prepareVerifiedSession(PROVISION_FINGERPRINT);
    expect(second).toEqual({ generation: 2 });
    await client.send("N0CALL-7>APCM20:fixture", PROVISION_FINGERPRINT, second);
    await waitFor(() => sessions[1]?.length === 2);
    expect(sessions[1]).toEqual([
      "user TEST01 pass 11111 vers CMClient 2.0",
      "N0CALL-7>APCM20:fixture",
    ]);

    await client.close();
    await close(server);
  });

  it("checks the transmission gate at the socket write and preserves the verified session", async () => {
    const sessions: string[][] = [];
    const server = net.createServer((socket) => {
      const lines: string[] = [];
      sessions.push(lines);
      collectFixtureLines(socket, lines, (line) => {
        if (line.startsWith("user ")) {
          socket.write("# logresp TEST01 verified, server fixture\r\n");
        }
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
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
    });
    const session = await client.prepareVerifiedSession(PROVISION_FINGERPRINT);

    await expect(
      client.send(
        "N0CALL-7>APCM20:must-not-send",
        PROVISION_FINGERPRINT,
        session,
        () => false,
      ),
    ).rejects.toMatchObject({ code: "APRS_TX_FENCED" });
    await client.send(
      "N0CALL-7>APCM20:fixture",
      PROVISION_FINGERPRINT,
      session,
      () => true,
    );
    await waitFor(() => sessions[0]?.length === 2);
    expect(sessions).toEqual([
      ["user TEST01 pass 11111 vers CMClient 2.0", "N0CALL-7>APCM20:fixture"],
    ]);
    await client.close();
    await close(server);
  });

  it("closes a reused session when its provision is revoked", async () => {
    const lines: string[] = [];
    let closedResolve: (() => void) | undefined;
    const closed = new Promise<void>((resolve) => {
      closedResolve = resolve;
    });
    const server = net.createServer((socket) => {
      socket.once("close", () => closedResolve?.());
      collectFixtureLines(socket, lines, (line) => {
        if (line.startsWith("user ")) {
          socket.write("# logresp TEST01 verified, server fixture\r\n");
        }
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    let revoked = false;
    const client = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: () =>
        revoked
          ? undefined
          : {
              loginLine: "user TEST01 pass 11111 vers CMClient 2.0",
              provisionFingerprint: PROVISION_FINGERPRINT,
            },
    });

    await client.send("N0CALL-7>APCM20:fixture-one", PROVISION_FINGERPRINT);
    revoked = true;
    await expect(
      client.send("N0CALL-7>APCM20:must-not-send", PROVISION_FINGERPRINT),
    ).rejects.toMatchObject({ code: "APRS_PROVISION_UNAVAILABLE" });
    await closed;
    expect(lines).toEqual([
      "user TEST01 pass 11111 vers CMClient 2.0",
      "N0CALL-7>APCM20:fixture-one",
    ]);
    await client.close();
    await close(server);
  });

  it.each([
    ["mismatched callsign", "# logresp OTHER1 verified, server fixture\r\n"],
    ["unverified identity", "# logresp TEST01 unverified, server fixture\r\n"],
  ])("fences Data after a %s logresp", async (_case, response) => {
    const lines: string[] = [];
    const server = net.createServer((socket) => {
      collectFixtureLines(socket, lines, (line) => {
        if (line.startsWith("user ")) {
          socket.write(response);
        }
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
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      timeoutMs: 100,
    });

    await expect(
      client.send("N0CALL-7>APCM20:must-not-send", PROVISION_FINGERPRINT),
    ).rejects.toMatchObject({ code: "APRS_OUTBOX_FAILED" });
    expect(lines).toEqual(["user TEST01 pass 11111 vers CMClient 2.0"]);
    await client.close();
    await close(server);
  });

  it("fences Data when verification times out", async () => {
    const lines: string[] = [];
    const server = net.createServer((socket) => {
      collectFixtureLines(socket, lines);
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
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      timeoutMs: 25,
    });

    await expect(
      client.send("N0CALL-7>APCM20:must-not-send", PROVISION_FINGERPRINT),
    ).rejects.toMatchObject({ code: "APRS_OUTBOX_FAILED" });
    expect(lines).toEqual(["user TEST01 pass 11111 vers CMClient 2.0"]);
    await client.close();
    await close(server);
  });

  it("fences Data when the server reaches EOF before verification", async () => {
    const lines: string[] = [];
    const server = net.createServer((socket) => {
      collectFixtureLines(socket, lines, (line) => {
        if (line.startsWith("user ")) {
          socket.end("# fixture banner\r\n");
        }
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
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      timeoutMs: 100,
    });

    await expect(
      client.send("N0CALL-7>APCM20:must-not-send", PROVISION_FINGERPRINT),
    ).rejects.toMatchObject({ code: "APRS_OUTBOX_FAILED" });
    expect(lines).toEqual(["user TEST01 pass 11111 vers CMClient 2.0"]);
    await client.close();
    await close(server);
  });

  it("fences a closed session and requires fresh verification", async () => {
    const sessions: string[][] = [];
    let firstClosedResolve: (() => void) | undefined;
    const firstClosed = new Promise<void>((resolve) => {
      firstClosedResolve = resolve;
    });
    const server = net.createServer((socket) => {
      const sessionIndex = sessions.length;
      const lines: string[] = [];
      sessions.push(lines);
      collectFixtureLines(socket, lines, (line) => {
        if (line.startsWith("user ") && sessionIndex === 0) {
          socket.write("# logresp TEST01 verified, server fixture\r\n");
        } else if (line.endsWith("fixture-one") && sessionIndex === 0) {
          socket.end();
        }
      });
      if (sessionIndex === 0) {
        socket.once("close", () => firstClosedResolve?.());
      }
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
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      timeoutMs: 30,
    });

    await client.send("N0CALL-7>APCM20:fixture-one", PROVISION_FINGERPRINT);
    await firstClosed;
    await expect(
      client.send("N0CALL-7>APCM20:fixture-two", PROVISION_FINGERPRINT),
    ).rejects.toMatchObject({ code: "APRS_OUTBOX_FAILED" });
    expect(sessions).toEqual([
      [
        "user TEST01 pass 11111 vers CMClient 2.0",
        "N0CALL-7>APCM20:fixture-one",
      ],
      ["user TEST01 pass 11111 vers CMClient 2.0"],
    ]);
    await client.close();
    await close(server);
  });

  it("fences Data on a TCP connection error", async () => {
    const server = net.createServer();
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    await close(server);
    const client = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: authorization(
        "user TEST01 pass 11111 vers CMClient 2.0",
      ),
      timeoutMs: 100,
    });

    await expect(
      client.send("N0CALL-7>APCM20:must-not-send", PROVISION_FINGERPRINT),
    ).rejects.toMatchObject({ code: "APRS_OUTBOX_FAILED" });
    await client.close();
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
    "revalidates a provision that is %s while the TCP connection opens",
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
      const client = new AprsIsTcpClient({
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
      });

      await expect(
        client.send("N0CALL-7>APCM20:fixture", PROVISION_FINGERPRINT),
      ).rejects.toMatchObject({ code: "APRS_PROVISION_UNAVAILABLE" });
      await new Promise((resolve) => setImmediate(resolve));

      expect(connections).toBe(1);
      expect(providerCalls).toBe(2);
      expect(lines).toEqual([]);
      await close(server);
    },
  );

  it("resolves a rotated login provider before every send", async () => {
    const sessions: string[][] = [];
    const server = net.createServer((socket) => {
      const lines: string[] = [];
      sessions.push(lines);
      collectFixtureLines(socket, lines, (line) => {
        const callsign = /^user ([^ ]+) /.exec(line)?.[1];
        if (callsign) {
          socket.write(`# logresp ${callsign} verified, server fixture\r\n`);
        }
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    let current = {
      loginLine: "user TEST01 pass 11111 vers CMClient 2.0",
      provisionFingerprint: PROVISION_FINGERPRINT,
    };
    const client = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: () => ({ ...current }),
    });

    await client.send("TEST01-7>APCM20:fixture-one", PROVISION_FINGERPRINT);
    current = {
      loginLine: "user AB12CD-7 pass 22222 vers CMClient 2.0",
      provisionFingerprint: ROTATED_PROVISION_FINGERPRINT,
    };
    await client.send(
      "TEST01-7>APCM20:fixture-two",
      ROTATED_PROVISION_FINGERPRINT,
    );
    await waitFor(
      () =>
        sessions.length === 2 && sessions.every((lines) => lines.length === 2),
    );

    expect(sessions.map((lines) => lines[0])).toEqual([
      "user TEST01 pass 11111 vers CMClient 2.0",
      "user AB12CD-7 pass 22222 vers CMClient 2.0",
    ]);
    expect(sessions.map((lines) => lines[1])).toEqual([
      "TEST01-7>APCM20:fixture-one",
      "TEST01-7>APCM20:fixture-two",
    ]);
    await client.close();
    await close(server);
  });

  it("does not let a stale provision request close the current verified session", async () => {
    const sessions: string[][] = [];
    const server = net.createServer((socket) => {
      const lines: string[] = [];
      sessions.push(lines);
      collectFixtureLines(socket, lines, (line) => {
        const callsign = /^user ([^ ]+) /.exec(line)?.[1];
        if (callsign) {
          socket.write(`# logresp ${callsign} verified, server fixture\r\n`);
        }
      });
    });
    server.listen({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture APRS server did not bind");
    }
    let current = {
      loginLine: "user TEST01 pass 11111 vers CMClient 2.0",
      provisionFingerprint: PROVISION_FINGERPRINT,
    };
    const client = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: address.port,
      authorizationProvider: () => ({ ...current }),
    });
    const staleSession = await client.prepareVerifiedSession(
      PROVISION_FINGERPRINT,
    );
    current = {
      loginLine: "user AB12CD-7 pass 22222 vers CMClient 2.0",
      provisionFingerprint: ROTATED_PROVISION_FINGERPRINT,
    };
    const currentSession = await client.prepareVerifiedSession(
      ROTATED_PROVISION_FINGERPRINT,
    );

    await expect(
      client.send("TEST01-7>APCM20:stale", PROVISION_FINGERPRINT, staleSession),
    ).rejects.toMatchObject({ code: "APRS_PROVISION_UNAVAILABLE" });
    await client.send(
      "AB12CD-7>APCM20:current",
      ROTATED_PROVISION_FINGERPRINT,
      currentSession,
    );
    await waitFor(() => sessions[1]?.length === 2);
    expect(sessions).toHaveLength(2);
    expect(sessions[1]).toEqual([
      "user AB12CD-7 pass 22222 vers CMClient 2.0",
      "AB12CD-7>APCM20:current",
    ]);
    await client.close();
    await close(server);
  });

  it("rejects invalid login providers before opening a socket", async () => {
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
      const client = new AprsIsTcpClient({
        host: "127.0.0.1",
        port: address.port,
        authorizationProvider,
      });
      await expect(
        client.send("TEST01-7>APCM20:fixture", PROVISION_FINGERPRINT),
      ).rejects.toMatchObject({ code: "APRS_PROVISION_UNAVAILABLE" });
    }
    await new Promise((resolve) => setImmediate(resolve));

    expect(connections).toBe(0);
    await close(server);
  });

  it("rejects line injection and oversized APRS-IS login and Data", async () => {
    const injectedLoginClient = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: 14580,
      authorizationProvider: authorization(
        "user TEST01 pass 11111\r\nuser injected",
      ),
    });
    await expect(
      injectedLoginClient.send(
        "TEST01-7>APCM20:fixture",
        PROVISION_FINGERPRINT,
      ),
    ).rejects.toMatchObject({ code: "APRS_PROVISION_UNAVAILABLE" });

    const oversizedLoginClient = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: 14580,
      authorizationProvider: authorization("x".repeat(513)),
    });
    await expect(
      oversizedLoginClient.send(
        "TEST01-7>APCM20:fixture",
        PROVISION_FINGERPRINT,
      ),
    ).rejects.toMatchObject({ code: "APRS_PROVISION_UNAVAILABLE" });

    const client = new AprsIsTcpClient({
      host: "127.0.0.1",
      port: 14580,
      authorizationProvider: authorization("user TEST01 pass 11111"),
    });
    await expect(
      client.send("line one\r\nline two", PROVISION_FINGERPRINT),
    ).rejects.toMatchObject({
      code: "APRS_OUTBOX_FAILED",
    });
    const prefix = "TEST01-7>APTMAG:";
    await expect(
      client.send(
        `${prefix}${"x".repeat(511 - Buffer.byteLength(prefix))}`,
        PROVISION_FINGERPRINT,
      ),
    ).rejects.toMatchObject({ code: "APRS_OUTBOX_FAILED" });
  });
});

function persistedPositionEvent(
  database: GatewayDatabase,
  suffix = "fixture",
  positionTimestampSeconds = 1_784_332_800,
  sequenceNumber?: number,
  nodeNum = 42,
) {
  const meshObservation = createMeshObservation({
    id: `mesh-observation-${suffix}`,
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
    id: `position-observation-${suffix}`,
    meshNetworkId: "fixture-network",
    nodeNum,
    meshObservationId: meshObservation.id,
    gatewayId: "fixture-gateway",
    transport: "simulator",
    sessionConnectedAt: meshObservation.sessionConnectedAt,
    ingestedAt: meshObservation.ingestedAt,
    serverIngestedAt: meshObservation.serverIngestedAt,
    backlogClassification: "live",
    payloadHash: (suffix.charCodeAt(0) % 16).toString(16).repeat(64),
    position: {
      latitudeI: 250000000,
      longitudeI: 1215000000,
      precisionBits: 32,
      positionTimestampSeconds,
      ...(sequenceNumber === undefined ? {} : { sequenceNumber }),
    },
  });
  const event = createCanonicalPositionEvent(observation).event;
  return repository.insertOrFindEvent(event).event;
}

function requiredOutbox(result: EnqueueAprsResult) {
  if (!result.entry) {
    throw new Error("fixture APRS outbox enqueue was suppressed");
  }
  return result.entry;
}

function collectFixtureLines(
  socket: Socket,
  lines: string[],
  onLine: (line: string) => void = () => undefined,
): void {
  let buffer = "";
  socket.on("data", (chunk: Buffer) => {
    buffer += chunk.toString("utf8");
    const parts = buffer.split("\r\n");
    buffer = parts.pop() ?? "";
    for (const line of parts.filter(Boolean)) {
      lines.push(line);
      onLine(line);
    }
  });
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
