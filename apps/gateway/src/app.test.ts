import { request, type IncomingMessage } from "node:http";

import { describe, expect, it } from "vitest";

import {
  GatewayConfigurationError,
  GatewayRuntime,
  createGatewayApp,
  parseGatewayListenOptions,
} from "./app";
import { MemoryLogger, redact } from "./observability";
import {
  DEFAULT_EVENT_PAYLOAD_MAX_BYTES,
  DEFAULT_SSE_FRAME_MAX_BYTES,
  DomainEventBus,
} from "./events";
import { JobEngine, JobQueueFullError } from "./jobs";
import { GatewayDatabase } from "./persistence/database";

describe("GatewayRuntime", () => {
  it("fails closed for a non-loopback bind", () => {
    expect(() =>
      parseGatewayListenOptions({ CMCLIENT_GATEWAY_HOST: "0.0.0.0" }),
    ).toThrow(GatewayConfigurationError);
  });

  it("allows a wildcard bind only for the constrained Docker runtime", () => {
    expect(
      parseGatewayListenOptions({
        CMCLIENT_RUNTIME_PROFILE: "docker",
        CMCLIENT_GATEWAY_HOST: "0.0.0.0",
        CMCLIENT_GATEWAY_PORT: "8081",
      }),
    ).toEqual({ host: "0.0.0.0", port: 8081 });
    expect(() =>
      parseGatewayListenOptions({
        CMCLIENT_RUNTIME_PROFILE: "native",
        CMCLIENT_GATEWAY_HOST: "0.0.0.0",
      }),
    ).toThrow(GatewayConfigurationError);
  });

  it("propagates request IDs and redacts structured fields", async () => {
    const logger = new MemoryLogger();
    const app = createGatewayApp(logger);
    const response = await app.inject({
      method: "GET",
      url: "/missing?token=audit-fixture-secret",
      headers: {
        "x-correlation-id": "sync-42",
      },
    });
    expect(response.headers["x-trace-id"]).toMatch(/^[a-f0-9-]{36}$/);
    expect(logger.entries).toHaveLength(1);
    expect(logger.entries[0]).toMatchObject({ correlationId: "sync-42" });
    expect(logger.entries[0]?.fields?.path).toBe("unmatched");
    expect(JSON.stringify(logger.entries)).not.toContain(
      "audit-fixture-secret",
    );
    expect(
      redact({
        nested: {
          apiKey: "secret",
          credential: "credential-value",
          sessionCookie: "session-value",
          passcode: 1234,
        },
        plain: "safe",
      }),
    ).toEqual({
      nested: {
        apiKey: "[REDACTED]",
        credential: "[REDACTED]",
        sessionCookie: "[REDACTED]",
        passcode: "[REDACTED]",
      },
      plain: "safe",
    });
    await app.close();
  });

  it("serves schema-backed system endpoints", async () => {
    const app = createGatewayApp(new MemoryLogger());
    const [health, version, capabilities, status, aprs] = await Promise.all([
      app.inject("/api/v1/system/health"),
      app.inject("/api/v1/system/version"),
      app.inject("/api/v1/system/capabilities"),
      app.inject("/api/v1/system/status"),
      app.inject("/api/v1/aprs"),
    ]);
    expect(health.json()).toEqual({ status: "ok" });
    expect(version.json()).toMatchObject({
      component: "gateway",
      identity: {
        product: "CMClient",
        version: "2.0.0-rc.1",
        channel: "dev",
        target: { profile: "native", packageProfile: "workspace" },
      },
    });
    expect(capabilities.json()).toMatchObject({
      schemaVersion: 2,
      identity: version.json(),
      capabilities: {
        serial: { available: false, reasonCode: "not_configured" },
      },
    });
    expect(status.json()).toEqual({
      schemaVersion: 2,
      health: "ok",
      identity: version.json(),
    });
    expect(aprs.json()).toEqual({
      configured: false,
      running: false,
      monitorStatus: "stopped",
      mappedCallsigns: 0,
      pendingOutbox: 0,
      failedOutbox: 0,
    });
    await app.close();
  });

  it("serves bounded domain list projections and fails closed without persistence", async () => {
    const limits: number[] = [];
    const telemetryQueries: unknown[] = [];
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
        queryTelemetry: (query) => {
          telemetryQueries.push(query);
          return [];
        },
        listPositions: () => [],
        listAprsOutbox: () => [],
      },
    );

    const nodes = await app.inject("/api/v1/nodes?limit=2");
    const positions = await app.inject("/api/v1/positions");
    const telemetry = await app.inject(
      "/api/v1/telemetry?meshNetworkId=mesh-a&nodeNum=42&metricKind=deviceMetrics&from=2026-07-18T00%3A00%3A00.000Z&to=2026-07-18T01%3A00%3A00.000Z&limit=25",
    );
    const invalidTelemetry = await app.inject("/api/v1/telemetry?nodeNum=42");
    const mixedPrecisionTelemetry = await app.inject(
      "/api/v1/telemetry?from=2026-07-18T00%3A00%3A00Z&to=2026-07-18T00%3A00%3A00.500Z",
    );
    expect(nodes.statusCode).toBe(200);
    expect(nodes.json()).toEqual({ items: [] });
    expect(positions.json()).toEqual({ items: [] });
    expect(telemetry.json()).toEqual({ items: [] });
    expect(mixedPrecisionTelemetry.statusCode).toBe(200);
    expect(telemetryQueries).toEqual([
      {
        limit: 25,
        meshNetworkId: "mesh-a",
        nodeNum: 42,
        metricKind: "deviceMetrics",
        from: "2026-07-18T00:00:00.000Z",
        to: "2026-07-18T01:00:00.000Z",
      },
      {
        limit: 100,
        from: "2026-07-18T00:00:00.000Z",
        to: "2026-07-18T00:00:00.500Z",
      },
    ]);
    expect(invalidTelemetry.statusCode).toBe(400);
    expect(invalidTelemetry.json()).toMatchObject({
      code: "TELEMETRY_RANGE_INVALID",
    });
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
            provisionState: "invalid",
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

  it("projects proxy status and fails closed when no proxy runtime exists", async () => {
    const app = createGatewayApp(
      new MemoryLogger(),
      undefined,
      undefined,
      {},
      undefined,
      undefined,
      undefined,
      {
        status: () => proxyStatusFixture(),
      },
    );
    const response = await app.inject("/api/v1/proxy");
    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      state: "running",
      policy: { activeClients: 1, mode: "monitor" },
    });
    await app.close();

    const unavailable = createGatewayApp(new MemoryLogger());
    const unavailableResponse = await unavailable.inject("/api/v1/proxy");
    expect(unavailableResponse.statusCode).toBe(503);
    expect(unavailableResponse.json()).toMatchObject({
      code: "PROXY_RUNTIME_UNAVAILABLE",
    });
    await unavailable.close();
  });

  it("submits an idempotent diagnostics integrity-check job", async () => {
    const submissions: Array<{
      correlationId?: string;
      idempotencyKey?: string;
    }> = [];
    const app = createGatewayApp(
      new MemoryLogger(),
      undefined,
      undefined,
      {},
      {
        get: () => undefined,
        cancel: () => undefined,
        submitIntegrityCheck(correlationId, idempotencyKey) {
          submissions.push({
            ...(correlationId ? { correlationId } : {}),
            ...(idempotencyKey ? { idempotencyKey } : {}),
          });
          return {
            created: true,
            job: {
              id: "diagnostics-1",
              type: "diagnostics.integrity_check",
              status: "queued",
              createdAt: "2026-07-18T00:00:00.000Z",
              updatedAt: "2026-07-18T00:00:00.000Z",
            },
          };
        },
      },
    );

    const response = await app.inject({
      method: "POST",
      url: "/api/v1/diagnostics/integrity-check",
      headers: {
        "x-correlation-id": "diagnostics-42",
        "idempotency-key": "fixture-fixture-fixture",
      },
    });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toEqual({ jobId: "diagnostics-1", reused: false });
    expect(submissions).toEqual([
      {
        correlationId: "diagnostics-42",
        idempotencyKey: "fixture-fixture-fixture",
      },
    ]);
    const invalid = await app.inject({
      method: "POST",
      url: "/api/v1/diagnostics/integrity-check",
      headers: { "idempotency-key": "invalid key" },
    });
    expect(invalid.statusCode).toBe(400);
    expect(invalid.json()).toMatchObject({ code: "JOB_INPUT_INVALID" });
    await app.close();
  });

  it("returns a stable 503 when the durable Job queue is full", async () => {
    const queueFull = (): never => {
      throw new JobQueueFullError();
    };
    const app = createGatewayApp(
      new MemoryLogger(),
      undefined,
      undefined,
      {},
      {
        get: () => undefined,
        cancel: () => undefined,
        submitIntegrityCheck: queueFull,
        submitBackup: queueFull,
      },
    );

    for (const url of [
      "/api/v1/diagnostics/integrity-check",
      "/api/v1/backups",
    ]) {
      const response = await app.inject({ method: "POST", url });
      expect(response.statusCode).toBe(503);
      expect(response.json()).toMatchObject({
        code: "JOB_QUEUE_FULL",
        params: {},
        traceId: expect.any(String),
      });
    }
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

    const stream = await openSse(address.port, "/api/v1/events", first.eventId);
    try {
      const body = await readUntil(stream.response, ": heartbeat\n\n");
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

  it("replays the maximum accepted payload within the SSE frame cap", async () => {
    const logger = new MemoryLogger();
    const events = new DomainEventBus({
      eventIdFactory: () => "oversized-event",
    });
    events.publish({
      type: "gateway.load_sample",
      source: "gateway",
      payload: {
        sample: "x".repeat(
          DEFAULT_EVENT_PAYLOAD_MAX_BYTES -
            Buffer.byteLength('{"sample":""}', "utf8"),
        ),
      },
    });
    const app = createGatewayApp(logger, undefined, events);
    await app.listen({ host: "127.0.0.1", port: 0 });
    const address = app.server.address();
    if (!address || typeof address === "string") {
      throw new Error("Gateway did not bind a TCP address");
    }

    let stream: Awaited<ReturnType<typeof openSse>> | undefined;
    try {
      stream = await openSse(address.port, "/api/v1/events", "missing");
      const body = await readUntil(stream.response, ": heartbeat\n\n");
      const frame = body.slice(0, body.indexOf(": heartbeat\n\n"));

      expect(Buffer.byteLength(frame, "utf8")).toBeLessThanOrEqual(
        DEFAULT_SSE_FRAME_MAX_BYTES,
      );
      expect(frame).toContain("id: oversized-event");
      expect(events.metricsSnapshot.subscriberCount).toBe(1);
      expect(logger.entries).not.toContainEqual(
        expect.objectContaining({ fields: { reason: "SSE_FRAME_TOO_LARGE" } }),
      );
      expect(logger.entries).not.toContainEqual(
        expect.objectContaining({ fields: { reason: "SSE_SLOW_CONSUMER" } }),
      );
    } finally {
      stream?.request.destroy();
      stream?.response.destroy();
      await app.close();
    }
  });

  it("closes a replay client when bounded SSE buffering is exhausted", async () => {
    let sequence = 0;
    const logger = new MemoryLogger();
    const events = new DomainEventBus({
      eventIdFactory: () => `burst-${++sequence}`,
    });
    const sample = "x".repeat(
      DEFAULT_EVENT_PAYLOAD_MAX_BYTES -
        Buffer.byteLength('{"sample":""}', "utf8"),
    );
    for (let index = 0; index < 32; index += 1) {
      events.publish({
        type: "gateway.load_sample",
        source: "gateway",
        payload: { sample },
      });
    }
    const app = createGatewayApp(logger, undefined, events);
    await app.listen({ host: "127.0.0.1", port: 0 });
    const address = app.server.address();
    if (!address || typeof address === "string") {
      throw new Error("Gateway did not bind a TCP address");
    }

    let stream: Awaited<ReturnType<typeof openSse>> | undefined;
    try {
      stream = await openSse(address.port, "/api/v1/events", "missing");
      const responseClosed = stream.response.destroyed
        ? Promise.resolve()
        : new Promise<void>((resolve) => {
            stream?.response.once("error", () => resolve());
            stream?.response.once("close", () => resolve());
          });
      await settlesWithin(responseClosed, 1_000);

      expect(events.metricsSnapshot.subscriberCount).toBe(0);
      expect(logger.entries).toContainEqual(
        expect.objectContaining({
          level: "warn",
          message: "gateway.sse_client_closed",
          fields: { reason: "SSE_SLOW_CONSUMER" },
        }),
      );
    } finally {
      stream?.request.destroy();
      stream?.response.destroy();
      await app.close();
    }
  });

  it("rejects excess SSE subscribers with a stable 503 before streaming", async () => {
    const logger = new MemoryLogger();
    const events = new DomainEventBus({ maxSubscribers: 1 });
    const unsubscribe = events.subscribe(() => undefined);
    const app = createGatewayApp(logger, undefined, events);

    const response = await app.inject("/api/v1/events");

    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({
      code: "SSE_SUBSCRIBER_LIMIT_REACHED",
      params: {},
      traceId: expect.any(String),
    });
    expect(events.metricsSnapshot).toMatchObject({
      subscriberCount: 1,
      subscriberRejections: 1,
    });
    expect(logger.entries).toContainEqual(
      expect.objectContaining({
        level: "warn",
        message: "gateway.sse_client_rejected",
        fields: { reason: "SSE_SUBSCRIBER_LIMIT_REACHED" },
      }),
    );
    unsubscribe();
    await app.close();
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

  it("closes a healthy SSE client before HTTP shutdown completes", async () => {
    const logger = new MemoryLogger();
    const events = new DomainEventBus();
    const runtime = new GatewayRuntime(
      { host: "127.0.0.1", port: 0 },
      logger,
      undefined,
      events,
    );
    const address = await runtime.start();
    const stream = await openSse(address.port, "/api/v1/events", "missing");
    await readUntil(stream.response, ": heartbeat\n\n");
    expect(events.metricsSnapshot.subscriberCount).toBe(1);
    const responseClosed = new Promise<void>((resolve) => {
      stream.response.once("close", resolve);
    });

    try {
      await settlesWithin(runtime.close(), 1_000);
      await settlesWithin(responseClosed, 1_000);
      expect(events.metricsSnapshot.subscriberCount).toBe(0);
      expect(logger.entries).toContainEqual(
        expect.objectContaining({
          level: "info",
          message: "gateway.sse_client_closed",
          fields: { reason: "SSE_SERVER_SHUTDOWN" },
        }),
      );
      expect(runtime.app.server.listening).toBe(false);
    } finally {
      stream.request.destroy();
      stream.response.destroy();
      if (runtime.app.server.listening) {
        await runtime.close();
      }
    }
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

function settlesWithin<T>(
  operation: Promise<T>,
  timeoutMs: number,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("fixture operation timed out")),
      timeoutMs,
    );
    void operation.then(
      (value) => {
        clearTimeout(timeout);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timeout);
        reject(error);
      },
    );
  });
}

function proxyStatusFixture() {
  return {
    state: "running" as const,
    listener: { host: "127.0.0.1", port: 4403 },
    policy: {
      activeClients: 1,
      allowLan: false,
      allowedAddressCount: 0,
      maxClients: 16,
      maxWritesPerMinute: 120,
      mode: "monitor" as const,
    },
    queue: {
      broadcastAccepted: 0,
      broadcastDropped: 0,
      broadcastFrames: 0,
      directAccepted: 0,
      directDropped: 0,
      pendingCorrelations: 0,
      queuedWrites: 0,
      writing: false,
    },
    recentAudit: [],
    upstream: {
      configFrameCount: 0,
      metrics: {
        bytesReceived: 0,
        bytesSent: 0,
        framesReceived: 0,
        framesSent: 0,
        malformedFrames: 0,
        reconnects: 0,
      },
      state: {
        changedAt: "2026-07-18T00:00:00.000Z",
        status: "ready" as const,
        transport: "tcp" as const,
      },
    },
  };
}
