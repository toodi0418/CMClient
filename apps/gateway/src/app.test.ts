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
import { JobEngine } from "./jobs";
import { GatewayDatabase } from "./persistence/database";

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

  it("serves bounded domain list projections and fails closed without persistence", async () => {
    const limits: number[] = [];
    const app = createGatewayApp(
      new MemoryLogger(),
      undefined,
      undefined,
      {},
      undefined,
      {
        listNodes: (limit) => {
          limits.push(limit);
          return [];
        },
        listMessages: () => [],
        listTelemetry: () => [],
        listPositions: () => [],
        listAprsOutbox: () => [],
      },
    );

    const nodes = await app.inject("/api/v1/nodes?limit=2");
    const positions = await app.inject("/api/v1/positions");
    expect(nodes.statusCode).toBe(200);
    expect(nodes.json()).toEqual({ items: [] });
    expect(positions.json()).toEqual({ items: [] });
    expect(limits).toEqual([2]);
    await app.close();

    const unavailable = createGatewayApp(new MemoryLogger());
    const response = await unavailable.inject("/api/v1/messages");
    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({
      code: "GATEWAY_DOMAIN_DATA_UNAVAILABLE",
    });
    await unavailable.close();
  });

  it("projects CallMesh state without exposing upstream credentials", async () => {
    const app = createGatewayApp(
      new MemoryLogger(),
      undefined,
      undefined,
      {},
      undefined,
      undefined,
      {
        getOverview: () => ({
          status: {
            state: "degraded",
            updatedAt: "2026-07-18T00:00:00.000Z",
            reasonCode: "CALLMESH_AUTH_INVALID",
            activeMappingCount: 0,
          },
          mappings: [],
        }),
      },
    );
    const response = await app.inject("/api/v1/callmesh");
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      status: expect.objectContaining({ reasonCode: "CALLMESH_AUTH_INVALID" }),
      mappings: [],
    });
    await app.close();

    const unavailable = createGatewayApp(new MemoryLogger());
    const unavailableResponse = await unavailable.inject("/api/v1/callmesh");
    expect(unavailableResponse.statusCode).toBe(503);
    expect(unavailableResponse.json()).toMatchObject({
      code: "CALLMESH_CLIENT_UNAVAILABLE",
    });
    await unavailable.close();
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

    const stream = await openSse(address.port, "/api/v1/events", first.eventId);
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

  it("serves persisted Job state and replays job-only SSE events", async () => {
    const database = new GatewayDatabase(":memory:");
    let eventSequence = 0;
    const events = new DomainEventBus({
      eventIdFactory: () => `event-${++eventSequence}`,
    });
    const engine = new JobEngine(database.jobs, events, {
      idFactory: () => "job-1",
      handlers: [
        {
          type: "diagnostics.integrity_check",
          handler: async () => ({ integrity: "ok" }),
        },
      ],
    });
    const checkpoint = events.publish({
      type: "gateway.ready",
      source: "gateway",
      payload: {},
    });
    const accepted = engine.submit({
      type: "diagnostics.integrity_check",
      input: {},
    });
    await engine.waitFor(accepted.job.id);
    const app = createGatewayApp(
      new MemoryLogger(),
      undefined,
      events,
      {},
      engine,
    );
    const details = await app.inject(`/api/v1/jobs/${accepted.job.id}`);
    expect(details.statusCode).toBe(200);
    expect(details.json()).toMatchObject({
      id: accepted.job.id,
      status: "succeeded",
    });
    await app.listen({ host: "127.0.0.1", port: 0 });
    const address = app.server.address();
    if (!address || typeof address === "string") {
      throw new Error("Gateway did not bind a TCP address");
    }

    const stream = await openSse(
      address.port,
      `/api/v1/jobs/${accepted.job.id}/events`,
      checkpoint.eventId,
    );
    try {
      const body = await readUntil(
        stream.response,
        "event: job.status_changed",
      );
      expect(body).toContain("event: job.created");
      expect(body).toContain(`"jobId":"${accepted.job.id}"`);
      expect(body).not.toContain("event: gateway.ready");
    } finally {
      stream.request.destroy();
      stream.response.destroy();
      await app.close();
      database.close();
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
  path: string,
  lastEventId: string,
): Promise<{ request: ReturnType<typeof request>; response: IncomingMessage }> {
  return new Promise((resolve, reject) => {
    const client = request({
      host: "127.0.0.1",
      port,
      path,
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
