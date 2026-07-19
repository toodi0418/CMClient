import { describe, expect, it } from "vitest";

import {
  DEFAULT_EVENT_PAYLOAD_MAX_BYTES,
  DEFAULT_EVENT_SUBSCRIBER_MAX,
  DEFAULT_SSE_FRAME_MAX_BYTES,
  DomainEventBus,
  formatSseEvent,
} from "./events";

describe("DomainEventBus", () => {
  it("keeps a bounded replay journal and resumes after an event ID", () => {
    let sequence = 0;
    const bus = new DomainEventBus({
      bufferSize: 2,
      clock: () => new Date("2026-07-18T00:00:00.000Z"),
      eventIdFactory: () => `event-${++sequence}`,
    });
    const first = bus.publish({
      type: "gateway.started",
      source: "gateway",
      payload: { port: 4810 },
    });
    const second = bus.publish({
      type: "gateway.ready",
      source: "gateway",
      payload: {},
      correlationId: "bootstrap-1",
    });
    const third = bus.publish({
      type: "system.health_changed",
      source: "gateway",
      payload: { status: "ok" },
    });

    expect(bus.replayAfter(second.eventId)).toEqual([third]);
    expect(bus.replayAfter(first.eventId)).toEqual([second, third]);
    expect(bus.replayAfter()).toEqual([]);
    expect(bus.replayBufferSize).toBe(2);
  });

  it("isolates event payloads and sends each published event to subscribers", () => {
    const bus = new DomainEventBus({ eventIdFactory: () => "event-1" });
    const received: string[] = [];
    const unsubscribe = bus.subscribe((event) => received.push(event.eventId));
    const payload = { nested: { state: "ready" } };
    const event = bus.publish({
      type: "gateway.ready",
      source: "gateway",
      payload,
    });
    payload.nested.state = "changed";
    unsubscribe();

    expect(event.payload).toEqual({ nested: { state: "ready" } });
    expect(received).toEqual(["event-1"]);
    expect(formatSseEvent(event)).toBe(
      `id: event-1\nevent: gateway.ready\ndata: ${JSON.stringify(event)}\n\n`,
    );
  });

  it("rejects invalid event names and non-serializable payloads", () => {
    const bus = new DomainEventBus();
    expect(() =>
      bus.publish({ type: "invalid event", source: "gateway", payload: {} }),
    ).toThrow("Event type is invalid");
    expect(() =>
      bus.publish({
        type: "gateway.ready",
        source: "gateway",
        payload: { value: BigInt(1) },
      }),
    ).toThrow("Event payload must be JSON serializable");
  });

  it("enforces UTF-8 payload and SSE frame byte limits", () => {
    expect(DEFAULT_EVENT_PAYLOAD_MAX_BYTES).toBe(56 * 1024);
    expect(DEFAULT_SSE_FRAME_MAX_BYTES).toBe(60 * 1024);
    const payload = { text: "測試-event-payload" };
    const payloadBytes = Buffer.byteLength(JSON.stringify(payload), "utf8");
    const bus = new DomainEventBus({
      maxPayloadBytes: payloadBytes,
      eventIdFactory: () => "event-sized",
    });
    const event = bus.publish({
      type: "gateway.ready",
      source: "gateway",
      payload,
    });
    const frame = formatSseEvent(event);

    expect(() =>
      new DomainEventBus({ maxPayloadBytes: payloadBytes - 1 }).publish({
        type: "gateway.ready",
        source: "gateway",
        payload,
      }),
    ).toThrow("Event payload exceeds byte limit");
    expect(formatSseEvent(event, Buffer.byteLength(frame, "utf8"))).toBe(frame);
    expect(() =>
      formatSseEvent(event, Buffer.byteLength(frame, "utf8") - 1),
    ).toThrow("SSE frame exceeds byte limit");
    expect(
      () =>
        new DomainEventBus({
          maxPayloadBytes: DEFAULT_EVENT_PAYLOAD_MAX_BYTES + 1,
        }),
    ).toThrow("Event payload byte limit is invalid");
    expect(() =>
      formatSseEvent(event, DEFAULT_SSE_FRAME_MAX_BYTES + 1),
    ).toThrow("SSE frame byte limit is invalid");

    const maximumPayload = {
      text: "x".repeat(
        DEFAULT_EVENT_PAYLOAD_MAX_BYTES -
          Buffer.byteLength('{"text":""}', "utf8"),
      ),
    };
    const maximumEvent = new DomainEventBus({
      eventIdFactory: () => "e".repeat(128),
    }).publish({
      type: "t".repeat(128),
      source: "s".repeat(128),
      correlationId: "c".repeat(128),
      payload: maximumPayload,
    });
    expect(
      Buffer.byteLength(JSON.stringify(maximumEvent.payload), "utf8"),
    ).toBe(DEFAULT_EVENT_PAYLOAD_MAX_BYTES);
    expect(
      Buffer.byteLength(formatSseEvent(maximumEvent), "utf8"),
    ).toBeLessThan(DEFAULT_SSE_FRAME_MAX_BYTES);
  });

  it("uses a listener snapshot for each frozen event fan-out", () => {
    const bus = new DomainEventBus();
    let deliveries = 0;
    let unsubscribeSecond = (): void => undefined;
    bus.subscribe(() => unsubscribeSecond());
    unsubscribeSecond = bus.subscribe((event) => {
      expect(Object.isFrozen(event)).toBe(true);
      deliveries += 1;
    });

    bus.publish({ type: "gateway.ready", source: "gateway", payload: {} });
    bus.publish({ type: "gateway.ready", source: "gateway", payload: {} });

    expect(deliveries).toBe(1);
  });

  it("isolates listener failures and exposes bounded diagnostics", () => {
    const failures: string[] = [];
    const received: string[] = [];
    const bus = new DomainEventBus({
      eventIdFactory: () => "event-isolated",
      onListenerError: (failure) => failures.push(failure.code),
    });
    bus.subscribe((event) => {
      (event.payload.nested as { state: string }).state = "mutated";
    });
    bus.subscribe((event) => {
      received.push((event.payload.nested as { state: string }).state);
    });

    bus.publish({
      type: "gateway.ready",
      source: "gateway",
      payload: { nested: { state: "original" } },
    });

    expect(received).toEqual(["original"]);
    expect(failures).toEqual(["EVENT_LISTENER_FAILED"]);
    expect(bus.recent(1)[0]?.payload).toEqual({
      nested: { state: "original" },
    });
    expect(bus.metricsSnapshot).toEqual({
      listenerDeliveries: 1,
      listenerErrors: 1,
      payloadRejections: 0,
      published: 1,
      replayBufferSize: 1,
      subscriberCount: 2,
      subscriberRejections: 0,
    });
  });

  it("keeps the replay journal bounded through a sustained event burst", () => {
    let sequence = 0;
    const bus = new DomainEventBus({
      bufferSize: 256,
      eventIdFactory: () => `load-${++sequence}`,
    });
    let deliveries = 0;
    bus.subscribe(() => {
      deliveries += 1;
    });

    const startedAt = performance.now();
    for (let index = 0; index < 50_000; index += 1) {
      bus.publish({
        type: "gateway.load_sample",
        source: "gateway",
        payload: { index },
      });
    }
    const elapsedMs = performance.now() - startedAt;

    expect(deliveries).toBe(50_000);
    expect(bus.metricsSnapshot).toMatchObject({
      listenerDeliveries: 50_000,
      listenerErrors: 0,
      published: 50_000,
      replayBufferSize: 256,
    });
    expect(bus.recent(200)[0]?.eventId).toBe("load-50000");
    expect(elapsedMs).toBeLessThan(10_000);
  }, 15_000);

  it("bounds subscribers and releases capacity after unsubscribe", () => {
    const bus = new DomainEventBus({ maxSubscribers: 2 });
    const first = bus.subscribe(() => undefined);
    const second = bus.subscribe(() => undefined);

    expect(DEFAULT_EVENT_SUBSCRIBER_MAX).toBe(128);
    expect(() => bus.subscribe(() => undefined)).toThrowError(
      expect.objectContaining({ code: "SSE_SUBSCRIBER_LIMIT_REACHED" }),
    );
    expect(bus.metricsSnapshot).toMatchObject({
      subscriberCount: 2,
      subscriberRejections: 1,
    });

    first();
    const replacement = bus.subscribe(() => undefined);
    expect(bus.metricsSnapshot.subscriberCount).toBe(2);
    second();
    replacement();
    expect(bus.metricsSnapshot.subscriberCount).toBe(0);
  });
});
