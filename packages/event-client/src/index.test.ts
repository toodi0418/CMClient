import { describe, expect, it } from "vitest";

import { GatewayEventClient, SseFrameParser } from "./index";

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

function eventFrame(eventId: string, payloadId = eventId): string {
  return `id: ${eventId}\nevent: gateway.ready\ndata: ${JSON.stringify({
    eventId: payloadId,
    schemaVersion: 1,
    type: "gateway.ready",
    occurredAt: "2026-07-18T00:00:00.000Z",
    source: "gateway",
    payload: {},
  })}\n\n`;
}
