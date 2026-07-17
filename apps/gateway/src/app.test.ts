import { request, type IncomingMessage } from "node:http";

import { describe, expect, it } from "vitest";

import {
  GatewayConfigurationError,
  GatewayRuntime,
  createGatewayApp,
  parseGatewayListenOptions,
} from "./app";
import { MemoryLogger, redact } from "./observability";
import { DomainEventBus } from "./events";

describe("GatewayRuntime", () => {
  it("fails closed for a non-loopback bind", () => {
    expect(() =>
      parseGatewayListenOptions({ CMCLIENT_GATEWAY_HOST: "0.0.0.0" }),
    ).toThrow(GatewayConfigurationError);
  });

  it("propagates request IDs and redacts structured fields", async () => {
    const logger = new MemoryLogger();
    const app = createGatewayApp(logger);
    const response = await app.inject({
      method: "GET",
      url: "/missing",
      headers: {
        "x-correlation-id": "sync-42",
      },
    });
    expect(response.headers["x-trace-id"]).toMatch(/^[a-f0-9-]{36}$/);
    expect(logger.entries).toHaveLength(1);
    expect(logger.entries[0]).toMatchObject({ correlationId: "sync-42" });
    expect(
      redact({ nested: { apiKey: "secret", passcode: 1234 }, plain: "safe" }),
    ).toEqual({
      nested: { apiKey: "[REDACTED]", passcode: "[REDACTED]" },
      plain: "safe",
    });
    await app.close();
  });

  it("serves schema-backed system endpoints", async () => {
    const app = createGatewayApp(new MemoryLogger());
    const [health, version, capabilities] = await Promise.all([
      app.inject("/api/v1/system/health"),
      app.inject("/api/v1/system/version"),
      app.inject("/api/v1/system/capabilities"),
    ]);
    expect(health.json()).toEqual({ status: "ok" });
    expect(version.json()).toMatchObject({
      version: "2.0.0-dev.0",
      channel: "dev",
    });
    expect(capabilities.json()).toMatchObject({
      schemaVersion: 1,
      capabilities: {
        serial: { available: false, reasonCode: "CAPABILITY_NOT_CONFIGURED" },
      },
    });
    await app.close();
  });

  it("replays SSE events after Last-Event-ID and starts a heartbeat stream", async () => {
    let sequence = 0;
    const events = new DomainEventBus({
      eventIdFactory: () => `event-${++sequence}`,
    });
    const first = events.publish({
      type: "gateway.started",
      source: "gateway",
      payload: {},
    });
    const second = events.publish({
      type: "gateway.ready",
      source: "gateway",
      payload: { port: 4810 },
    });
    const app = createGatewayApp(new MemoryLogger(), undefined, events);
    await app.listen({ host: "127.0.0.1", port: 0 });
    const address = app.server.address();
    if (!address || typeof address === "string") {
      throw new Error("Gateway did not bind a TCP address");
    }

    const stream = await openSse(address.port, first.eventId);
    try {
      const body = await readUntil(stream.response, `id: ${second.eventId}`);
      expect(body).toContain(": heartbeat\n\n");
      expect(body).not.toContain(`id: ${first.eventId}`);
      expect(body).toContain(`id: ${second.eventId}`);
      expect(body).toContain("event: gateway.ready");
    } finally {
      stream.request.destroy();
      stream.response.destroy();
      await app.close();
    }
  });

  it("listens and closes gracefully", async () => {
    const runtime = new GatewayRuntime({ host: "127.0.0.1", port: 0 });
    let closed = false;
    runtime.app.addHook("onClose", () => {
      closed = true;
    });

    const address = await runtime.start();
    expect(address.port).toBeGreaterThan(0);
    expect(runtime.app.server.listening).toBe(true);
    await runtime.close();
    expect(closed).toBe(true);
    expect(runtime.app.server.listening).toBe(false);
  });
});

function openSse(
  port: number,
  lastEventId: string,
): Promise<{ request: ReturnType<typeof request>; response: IncomingMessage }> {
  return new Promise((resolve, reject) => {
    const client = request({
      host: "127.0.0.1",
      port,
      path: "/api/v1/events",
      headers: { "last-event-id": lastEventId },
    });
    client.once("response", (response) =>
      resolve({ request: client, response }),
    );
    client.once("error", reject);
    client.end();
  });
}

function readUntil(response: IncomingMessage, needle: string): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = "";
    response.setEncoding("utf8");
    response.on("data", (chunk: string) => {
      body += chunk;
      if (body.includes(needle)) {
        resolve(body);
      }
    });
    response.once("error", reject);
  });
}
