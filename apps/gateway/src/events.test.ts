import { describe, expect, it } from "vitest";

import { DomainEventBus, formatSseEvent } from "./events";

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
});
