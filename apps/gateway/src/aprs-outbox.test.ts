import { once } from "node:events";
import net, { type Server } from "node:net";

import { describe, expect, it } from "vitest";

import { createMeshObservation } from "./observations";
import { DomainEventBus } from "./events";
import { GatewayMaintenanceRuntime } from "./maintenance";
import { AprsRemoteHighWaterStore } from "./aprs-monitor";
import {
  AprsIsTcpClient,
  AprsOutboxRepository,
  AprsOutboxWorker,
  type EnqueueAprsResult,
} from "./aprs-outbox";
import { GatewayDatabase } from "./persistence/database";
import {
  PositionHighWaterStore,
  PositionRepository,
  createCanonicalPositionEvent,
} from "./position";

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
    const entry = repository.enqueue({
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "N0CALL-7>APCM20:fixture",
      now: "2026-07-18T00:00:00.000Z",
    });
    const storedEntry = requiredOutbox(entry);
    repository.claimDue("2026-07-18T00:00:00.000Z");
    const worker = new AprsOutboxWorker(repository, {
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

  it("coalesces concurrent flush calls without reclaiming an active send", async () => {
    const database = new GatewayDatabase(":memory:");
    const event = persistedPositionEvent(database);
    const repository = new AprsOutboxRepository(database.connection);
    const entry = repository.enqueue({
      callsign: "N0CALL-7",
      canonicalEventId: event.id,
      data: "N0CALL-7>APCM20:fixture",
      now: "2026-07-18T00:00:00.000Z",
    });
    const storedEntry = requiredOutbox(entry);
    let release = (): void => undefined;
    let sends = 0;
    const worker = new AprsOutboxWorker(repository, {
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

  it("deletes only sent outbox rows older than the retention cutoff", () => {
    const database = new GatewayDatabase(":memory:");
    const repository = new AprsOutboxRepository(database.connection);
    const oldEvent = persistedPositionEvent(database, "old");
    const old = requiredOutbox(
      repository.enqueue({
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
    repository.markSent(old.id, "2026-01-01T00:00:01.000Z");

    const recent = requiredOutbox(
      repository.enqueue({
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
    repository.markSent(recent.id, "2026-07-17T00:00:01.000Z");
    const queued = requiredOutbox(
      repository.enqueue({
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
            repository.enqueue({
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
    repository.markSent(firstOutboxId, "2026-07-18T00:00:03.000Z");
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
    const worker = new AprsOutboxWorker(repository, {
      send: async () => {
        sends += 1;
      },
    });
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
      repository.enqueue({
        callsign: "N0CALL-7",
        canonicalEventId: firstEvent.id,
        data: "N0CALL-7>APCM20:first",
        now: "2026-07-18T00:00:00.000Z",
      }),
    );
    let now = new Date("2026-07-18T00:00:00.000Z");
    const transmitted: string[] = [];
    const worker = new AprsOutboxWorker(
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
      repository.enqueue({
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
          repository.enqueue({
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
      repository.enqueue({
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
      repository.enqueue({
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
      repository.prepareSend(first.id, "2026-07-18T00:00:01.000Z", 1_000),
    ).toEqual({ authorized: false });
    expect(repository.find(first.id)).toBeUndefined();
    const transmitted: string[] = [];
    const worker = new AprsOutboxWorker(
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

  it("drops a pre-rotation retry when another iGate advances the remote watermark", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const event = persistedPositionEvent(
      database,
      "remote-newer",
      1_784_332_800,
      20,
    );
    const entry = requiredOutbox(
      repository.enqueue({
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
    const remoteMinute = new Date(Date.parse(event.eventTime!) + 60_000);
    remoteMinute.setUTCSeconds(0, 0);
    new AprsRemoteHighWaterStore(database.connection).apply(
      {
        callsign: "N0CALL-7",
        eventMarker: "CM2/ffffffffffff",
        eventTime: remoteMinute.toISOString(),
      },
      {
        callsign: "N0CALL-7",
        mappingVersion: "mapping-v2",
        meshNetworkId: event.meshNetworkId,
        nodeNum: event.nodeNum,
      },
      "2026-07-18T00:00:01.000Z",
    );
    let sends = 0;
    const worker = new AprsOutboxWorker(repository, {
      send: async () => {
        sends += 1;
      },
    });

    await expect(worker.flush()).resolves.toEqual([]);
    expect(sends).toBe(0);
    expect(repository.find(entry.id)).toBeUndefined();
    database.close();
  });

  it("allows a byte-identical event after observing its exact remote marker", async () => {
    const database = new GatewayDatabase(":memory:");
    const repository = database.aprsOutbox;
    const event = persistedPositionEvent(
      database,
      "remote-exact",
      1_784_332_800,
      20,
    );
    const eventTime = event.eventTime!;
    const remoteMinute = new Date(eventTime);
    remoteMinute.setUTCSeconds(0, 0);
    const entry = requiredOutbox(
      repository.enqueue({
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
    new AprsRemoteHighWaterStore(database.connection).apply(
      {
        callsign: "N0CALL-7",
        eventMarker: `CM2/${event.canonicalKey.slice(0, 12)}`,
        eventTime: remoteMinute.toISOString(),
      },
      {
        callsign: "N0CALL-7",
        mappingVersion: "mapping-v1",
        meshNetworkId: event.meshNetworkId,
        nodeNum: event.nodeNum,
      },
      "2026-07-18T00:00:01.000Z",
    );
    let sends = 0;

    await new AprsOutboxWorker(repository, {
      send: async () => {
        sends += 1;
      },
    }).flush();

    expect(sends).toBe(1);
    expect(repository.find(entry.id)?.status).toBe("sent");
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
      repository.enqueue({
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
    const worker = new AprsOutboxWorker(
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
      repository.enqueue({
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
    await new AprsOutboxWorker(
      repository,
      { send: async () => undefined },
      {
        clock: () => new Date("2026-07-18T00:00:02.000Z"),
      },
    ).flush();
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
      repository.enqueue({
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
    await new AprsOutboxWorker(repository, {
      send: async () => {
        sends += 1;
      },
    }).flush();
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

function persistedPositionEvent(
  database: GatewayDatabase,
  suffix = "fixture",
  positionTimestampSeconds = 1_784_332_800,
  sequenceNumber?: number,
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
    nodeNum: 42,
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
