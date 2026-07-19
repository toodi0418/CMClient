import { describe, expect, it } from "vitest";

import {
  DEFAULT_SSE_FRAME_MAX_BYTES,
  GatewayEventClient,
  SseFrameParser,
} from "./index";

const encoder = new TextEncoder();

describe("SSE frame parser", () => {
  it("preserves a frame that arrives across arbitrary stream chunks", () => {
    const parser = new SseFrameParser();

    expect(parser.push("id: event-1\nevent: gateway.ready\ndata: {")).toEqual(
      [],
    );
    expect(parser.push('"eventId":"event-1"}\n\n')).toEqual([
      {
        id: "event-1",
        event: "gateway.ready",
        data: '{"eventId":"event-1"}',
      },
    ]);
  });

  it("rejects an unterminated frame before its buffer can grow without bound", () => {
    const parser = new SseFrameParser();

    for (let index = 0; index < 7; index += 1) {
      expect(parser.push("x".repeat(8 * 1_024))).toEqual([]);
    }
    expect(parser.bufferedBytes).toBe(56 * 1_024);
    expect(() => parser.push("x".repeat(8 * 1_024))).toThrowError(
      expect.objectContaining({ code: "SSE_EVENT_TOO_LARGE" }),
    );
    expect(parser.bufferedBytes).toBe(0);
  });

  it("parses a large transport chunk when each contained frame is bounded", () => {
    const parser = new SseFrameParser({ maxFrameBytes: 256 });
    const input = Array.from(
      { length: 10_000 },
      (_, index) => `id: ${index}\ndata: ${index}\n\n`,
    ).join("");

    const startedAt = performance.now();
    const frames = parser.push(input);
    const elapsedMs = performance.now() - startedAt;

    expect(frames).toHaveLength(10_000);
    expect(frames[0]).toEqual({ id: "0", data: "0" });
    expect(frames.at(-1)).toEqual({ id: "9999", data: "9999" });
    expect(parser.bufferedBytes).toBe(0);
    expect(elapsedMs).toBeLessThan(5_000);
    expect(DEFAULT_SSE_FRAME_MAX_BYTES).toBe(60 * 1_024);
  });

  it("does not permit a parser limit above the Gateway protocol cap", () => {
    expect(
      () =>
        new SseFrameParser({
          maxFrameBytes: DEFAULT_SSE_FRAME_MAX_BYTES + 1,
        }),
    ).toThrow("SSE frame byte limit is invalid");
  });
});

describe("gateway event client", () => {
  it("reconnects with Last-Event-ID after a closed stream", async () => {
    const requests: Headers[] = [];
    let call = 0;
    const client = new GatewayEventClient({
      reconnect: { initialDelayMs: 0, maximumDelayMs: 0 },
      sleep: async () => undefined,
      fetch: async (_input, init) => {
        requests.push(new Headers(init?.headers));
        call += 1;
        return sseResponse(
          call === 1 ? [eventFrame("event-1")] : [eventFrame("event-2")],
        );
      },
    });
    const received: string[] = [];

    const completed = new Promise<void>((resolve) => {
      client.onEvent((event) => {
        received.push(event.eventId);
        if (event.eventId === "event-2") {
          client.stop();
          resolve();
        }
      });
    });

    client.start();
    await completed;

    expect(received).toEqual(["event-1", "event-2"]);
    expect(requests[0]?.get("last-event-id")).toBeNull();
    expect(requests[1]?.get("last-event-id")).toBe("event-1");
  });

  it("fails closed when the SSE metadata disagrees with the payload", async () => {
    const client = new GatewayEventClient({
      reconnect: { initialDelayMs: 0, maximumDelayMs: 0 },
      sleep: async () => new Promise<void>(() => undefined),
      fetch: async () => sseResponse([eventFrame("event-1", "different-id")]),
    });
    const error = new Promise<string>((resolve) => {
      client.onError((received) => {
        client.stop();
        resolve(received.code);
      });
    });

    client.start();

    await expect(error).resolves.toBe("SSE_EVENT_ID_MISMATCH");
  });

  it("freezes events and isolates listener failures during fan-out", async () => {
    const client = new GatewayEventClient({
      fetch: async () =>
        sseResponse([
          eventFrame("event-frozen", "event-frozen", {
            nested: { state: "original" },
          }),
        ]),
    });
    const received = new Promise<string>((resolve) => {
      client.onEvent((event) => {
        expect(Object.isFrozen(event)).toBe(true);
        expect(Object.isFrozen(event.payload)).toBe(true);
        expect(Object.isFrozen(event.payload.nested)).toBe(true);
        (event.payload.nested as { state: string }).state = "mutated";
      });
      client.onEvent((event) => {
        client.stop();
        resolve((event.payload.nested as { state: string }).state);
      });
    });

    client.start();

    await expect(received).resolves.toBe("original");
    expect(client.lastReceivedEventId).toBe("event-frozen");
  });

  it("isolates state and error observers without rejecting the run loop", async () => {
    const client = new GatewayEventClient({
      reconnect: { initialDelayMs: 0, maximumDelayMs: 0 },
      sleep: async () => new Promise<void>(() => undefined),
      fetch: async () => sseResponse([eventFrame("event-1", "different-id")]),
    });
    const states: string[] = [];
    client.onStateChange(() => {
      throw new Error("fixture state observer failure");
    });
    client.onStateChange((state) => states.push(state));
    client.onError(() => {
      throw new Error("fixture error observer failure");
    });
    const error = new Promise<string>((resolve) => {
      client.onError((received) => {
        client.stop();
        resolve(received.code);
      });
    });

    client.start();

    await expect(error).resolves.toBe("SSE_EVENT_ID_MISMATCH");
    expect(states).toEqual(["connecting", "open", "stopped"]);
    expect(client.connectionState).toBe("stopped");
  });
});

function sseResponse(chunks: string[]): Response {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
  return new Response(body, {
    headers: { "content-type": "text/event-stream" },
  });
}

function eventFrame(
  eventId: string,
  payloadId = eventId,
  payload: Record<string, unknown> = {},
): string {
  return `id: ${eventId}\nevent: gateway.ready\ndata: ${JSON.stringify({
    eventId: payloadId,
    schemaVersion: 1,
    type: "gateway.ready",
    occurredAt: "2026-07-18T00:00:00.000Z",
    source: "gateway",
    payload,
  })}\n\n`;
}
