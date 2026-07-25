import { request, type IncomingMessage } from "node:http";

import type {
  FastifyInstance,
  InjectOptions,
  LightMyRequestCallback,
} from "fastify";
import { describe, expect, it } from "vitest";

import {
  GatewayAccessConfigurationError,
  GatewayConfigurationError,
  GatewayRuntime,
  createGatewayApp as createCapabilityProtectedGatewayApp,
} from "./app";
import { MemoryLogger, redact } from "./observability";
import {
  DEFAULT_EVENT_PAYLOAD_MAX_BYTES,
  DEFAULT_SSE_FRAME_MAX_BYTES,
  DomainEventBus,
  formatSseEvent,
} from "./events";
import { JobEngine, JobQueueFullError } from "./jobs";
import { GatewayDatabase } from "./persistence/database";
import { GATEWAY_CAPABILITY_HEADER } from "./bootstrap";

describe("GatewayRuntime", () => {
  it("allows only an OS-assigned IPv4 loopback bind", () => {
    for (const options of [
      { host: "0.0.0.0", port: 0 },
      { host: "localhost", port: 0 },
      { host: "::1", port: 0 },
      { host: "127.0.0.1", port: 8081 },
    ]) {
      expect(() => new GatewayRuntime(options)).toThrow(
        GatewayConfigurationError,
      );
    }
  });

  it("requires a valid private capability in the production runtime", () => {
    for (const capability of [undefined, "", "g".repeat(64), "c".repeat(63)]) {
      expect(
        () =>
          new GatewayRuntime(
            { host: "127.0.0.1", port: 0 },
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            capability === undefined ? undefined : { capability },
          ),
      ).toThrow(GatewayAccessConfigurationError);
    }
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
      pendingStationSubmissions: 0,
      failedStationSubmissions: 0,
    });
    await app.close();
  });

  it("normalizes Fastify validation, routing, and SSE negotiation errors", async () => {
    const app = createGatewayApp(new MemoryLogger());
    const responses = await Promise.all([
      app.inject("/api/v1/nodes?limit=0"),
      app.inject("/api/v1/not-a-route"),
      app.inject({ method: "POST", url: "/api/v1/system/health" }),
      app.inject({
        method: "GET",
        url: "/api/v1/events",
        headers: { accept: "application/json" },
      }),
    ]);
    const expected = [
      [400, "GATEWAY_REQUEST_SCHEMA_INVALID"],
      [404, "GATEWAY_ROUTE_NOT_FOUND"],
      [405, "GATEWAY_METHOD_NOT_ALLOWED"],
      [406, "GATEWAY_SSE_NOT_ACCEPTABLE"],
    ] as const;

    for (const [index, response] of responses.entries()) {
      const [statusCode, code] = expected[index] ?? [];
      const traceId = response.headers["x-trace-id"];
      expect(response.statusCode).toBe(statusCode);
      expect(response.json()).toEqual({ code, params: {}, traceId });
      expect(Object.keys(response.json()).sort()).toEqual([
        "code",
        "params",
        "traceId",
      ]);
      expect(response.body).not.toContain("message");
      expect(response.body).not.toContain("Not Acceptable");
    }
    expect(responses[2]?.headers.allow).toContain("GET");
    await app.close();
  });

  it("rejects direct and spoofed access on health, HTTP, SSE, and unmatched routes", async () => {
    const capability = "c".repeat(64);
    const app = createCapabilityProtectedGatewayApp(
      { capability },
      new MemoryLogger(),
    );
    for (const url of [
      "/api/v1/system/health",
      "/api/v1/nodes",
      "/api/v1/events",
      "/missing",
    ]) {
      const direct = await app.inject({ method: "GET", url });
      expect(direct.statusCode, url).toBe(403);
      expect(direct.json()).toEqual({
        code: "GATEWAY_CAPABILITY_REJECTED",
        params: {},
        traceId: direct.headers["x-trace-id"],
      });

      const spoofed = await app.inject({
        method: "GET",
        url,
        headers: { [GATEWAY_CAPABILITY_HEADER]: "d".repeat(64) },
      });
      expect(spoofed.statusCode, url).toBe(403);

      const nonAscii = await app.inject({
        method: "GET",
        url,
        headers: { [GATEWAY_CAPABILITY_HEADER]: "\u00e9".repeat(64) },
      });
      expect(nonAscii.statusCode, url).toBe(403);
    }

    const accepted = await app.inject({
      method: "GET",
      url: "/api/v1/system/health",
      headers: { [GATEWAY_CAPABILITY_HEADER]: capability },
    });
    expect(accepted.statusCode).toBe(200);
    expect(accepted.json()).toEqual({ status: "ok" });
    expect(JSON.stringify(accepted.headers)).not.toContain(capability);
    expect(accepted.body).not.toContain(capability);
    await app.close();
  });

  it("rejects malformed private capability configuration instead of disabling the gate", () => {
    for (const capability of [undefined, "", "g".repeat(64), "c".repeat(63)]) {
      expect(() =>
        createCapabilityProtectedGatewayApp(
          capability === undefined ? undefined : { capability },
          new MemoryLogger(),
        ),
      ).toThrow(GatewayAccessConfigurationError);
    }
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

  it("serves a sanitized bounded station-delivery projection", async () => {
    const limits: number[] = [];
    const app = createGatewayApp(
      new MemoryLogger(),
      undefined,
      undefined,
      {},
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      {
        status: () => ({
          configured: true,
          running: true,
          monitorStatus: "connected",
          mappedCallsigns: 1,
          pendingOutbox: 0,
          failedOutbox: 0,
          pendingStationSubmissions: 1,
          failedStationSubmissions: 0,
        }),
        listStationSubmissions: (limit) => {
          limits.push(limit);
          return [
            {
              id: "aprs-igate-00000000-0000-4000-8000-000000000001",
              packetKind: "beacon",
              deliveryStatus: "submitted",
              attemptedAt: "2026-07-18T00:00:00.000Z",
              submittedAt: "2026-07-18T00:00:01.000Z",
              updatedAt: "2026-07-18T00:00:01.000Z",
              observationExpiresAt: "2026-07-18T03:00:00.000Z",
            },
          ];
        },
      },
    );

    const response = await app.inject(
      "/api/v1/aprs/station-submissions?limit=7",
    );
    expect(response.statusCode).toBe(200);
    expect(limits).toEqual([7]);
    expect(response.json()).toEqual({
      items: [
        {
          id: "aprs-igate-00000000-0000-4000-8000-000000000001",
          packetKind: "beacon",
          deliveryStatus: "submitted",
          attemptedAt: "2026-07-18T00:00:00.000Z",
          submittedAt: "2026-07-18T00:00:01.000Z",
          updatedAt: "2026-07-18T00:00:01.000Z",
          observationExpiresAt: "2026-07-18T03:00:00.000Z",
        },
      ],
    });
    expect(JSON.stringify(response.json())).not.toMatch(
      /callsign|destination|info|fingerprint|latitude|longitude/iu,
    );
    await app.close();
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
    const app = createGatewayApp(new MemoryLogger(), undefined, events, {
      heartbeatIntervalMs: 1_000,
    });
    await app.listen({ host: "127.0.0.1", port: 0 });
    const address = app.server.address();
    if (!address || typeof address === "string") {
      throw new Error("Gateway did not bind a TCP address");
    }

    const stream = await openSse(
      address.port,
      "/api/v1/events",
      first.eventId,
      TEST_GATEWAY_CAPABILITY,
    );
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

  it("rejects an invalid Last-Event-ID before committing SSE headers", async () => {
    const app = createGatewayApp(new MemoryLogger());
    const response = await app.inject({
      method: "GET",
      url: "/api/v1/events",
      headers: {
        accept: "text/event-stream",
        "last-event-id": "invalid cursor",
      },
    });

    expect(response.statusCode).toBe(400);
    expect(response.headers["content-type"]).toContain("application/json");
    expect(response.json()).toMatchObject({
      code: "SSE_CURSOR_INVALID",
      params: {},
      traceId: expect.any(String),
    });
    await app.close();
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
    const app = createGatewayApp(logger, undefined, events, {
      heartbeatIntervalMs: 1_000,
    });
    await app.listen({ host: "127.0.0.1", port: 0 });
    const address = app.server.address();
    if (!address || typeof address === "string") {
      throw new Error("Gateway did not bind a TCP address");
    }

    let stream: Awaited<ReturnType<typeof openSse>> | undefined;
    try {
      stream = await openSse(
        address.port,
        "/api/v1/events",
        "missing",
        TEST_GATEWAY_CAPABILITY,
      );
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

  it("replays multiple near-limit events before live events without sharing the live buffer", async () => {
    let sequence = 0;
    const logger = new MemoryLogger();
    const events = new DomainEventBus({
      eventIdFactory: () => `burst-${++sequence}`,
    });
    const checkpoint = events.publish({
      type: "gateway.checkpoint",
      source: "gateway",
      payload: {},
    });
    const sample = "x".repeat(
      DEFAULT_EVENT_PAYLOAD_MAX_BYTES -
        Buffer.byteLength('{"sample":""}', "utf8"),
    );
    const replay = [
      events.publish({
        type: "gateway.load_sample",
        source: "gateway",
        payload: { sample },
      }),
      events.publish({
        type: "gateway.load_sample",
        source: "gateway",
        payload: { sample },
      }),
    ];
    expect(
      replay.reduce(
        (bytes, event) =>
          bytes + Buffer.byteLength(formatSseEvent(event), "utf8"),
        0,
      ),
    ).toBeGreaterThan(DEFAULT_SSE_FRAME_MAX_BYTES);
    const app = createGatewayApp(logger, undefined, events);
    await app.listen({ host: "127.0.0.1", port: 0 });
    const address = app.server.address();
    if (!address || typeof address === "string") {
      throw new Error("Gateway did not bind a TCP address");
    }

    let stream: Awaited<ReturnType<typeof openSse>> | undefined;
    try {
      stream = await openSse(
        address.port,
        "/api/v1/events",
        checkpoint.eventId,
        TEST_GATEWAY_CAPABILITY,
      );
      const live = events.publish({
        type: "gateway.live",
        source: "gateway",
        payload: {},
      });
      const body = await settlesWithin(
        readUntil(stream.response, `id: ${live.eventId}`),
        2_000,
      );
      const firstReplay = body.indexOf(`id: ${replay[0]?.eventId}`);
      const secondReplay = body.indexOf(`id: ${replay[1]?.eventId}`);
      const liveEvent = body.indexOf(`id: ${live.eventId}`);

      expect(firstReplay).toBeGreaterThanOrEqual(0);
      expect(secondReplay).toBeGreaterThan(firstReplay);
      expect(liveEvent).toBeGreaterThan(secondReplay);
      expect(events.metricsSnapshot.subscriberCount).toBe(1);
      expect(logger.entries).not.toContainEqual(
        expect.objectContaining({
          fields: { reason: "SSE_SLOW_CONSUMER" },
        }),
      );
    } finally {
      stream?.request.destroy();
      stream?.response.destroy();
      await app.close();
    }
  });

  it("closes a live SSE client when its bounded pending queue is exhausted", async () => {
    let sequence = 0;
    const logger = new MemoryLogger();
    const events = new DomainEventBus({
      eventIdFactory: () => `live-burst-${++sequence}`,
    });
    const checkpoint = events.publish({
      type: "gateway.checkpoint",
      source: "gateway",
      payload: {},
    });
    const sample = "x".repeat(
      DEFAULT_EVENT_PAYLOAD_MAX_BYTES -
        Buffer.byteLength('{"sample":""}', "utf8"),
    );
    const app = createGatewayApp(logger, undefined, events);
    await app.listen({ host: "127.0.0.1", port: 0 });
    const address = app.server.address();
    if (!address || typeof address === "string") {
      throw new Error("Gateway did not bind a TCP address");
    }

    let stream: Awaited<ReturnType<typeof openSse>> | undefined;
    try {
      stream = await openSse(
        address.port,
        "/api/v1/events",
        checkpoint.eventId,
        TEST_GATEWAY_CAPABILITY,
      );
      const responseClosed = new Promise<void>((resolve) => {
        stream?.response.once("error", () => resolve());
        stream?.response.once("close", () => resolve());
      });
      stream.response.resume();
      for (let index = 0; index < 3; index += 1) {
        events.publish({
          type: "gateway.load_sample",
          source: "gateway",
          payload: { sample },
        });
      }
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
      TEST_GATEWAY_CAPABILITY,
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
    const runtime = new GatewayRuntime(
      { host: "127.0.0.1", port: 0 },
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      { capability: "c".repeat(64) },
    );
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
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      { capability: "c".repeat(64) },
    );
    const address = await runtime.start();
    const stream = await openSse(
      address.port,
      "/api/v1/events",
      "missing",
      "c".repeat(64),
    );
    stream.response.resume();
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

const TEST_GATEWAY_CAPABILITY = "f".repeat(64);
type GatewayTestDependencies =
  Parameters<typeof createCapabilityProtectedGatewayApp> extends [
    unknown,
    ...infer Dependencies,
  ]
    ? Dependencies
    : never;

function createGatewayApp(
  ...dependencies: GatewayTestDependencies
): FastifyInstance {
  const app = createCapabilityProtectedGatewayApp(
    { capability: TEST_GATEWAY_CAPABILITY },
    ...dependencies,
  );
  const inject = app.inject.bind(app);
  app.inject = ((
    options?: InjectOptions | string,
    callback?: LightMyRequestCallback,
  ) => {
    if (options === undefined) {
      return inject();
    }
    const securedOptions: InjectOptions =
      typeof options === "string"
        ? {
            method: "GET",
            url: options,
            headers: {
              [GATEWAY_CAPABILITY_HEADER]: TEST_GATEWAY_CAPABILITY,
            },
          }
        : {
            ...options,
            headers: {
              ...options.headers,
              [GATEWAY_CAPABILITY_HEADER]: TEST_GATEWAY_CAPABILITY,
            },
          };
    if (callback) {
      return inject(securedOptions, callback);
    }
    return inject(securedOptions);
  }) as typeof app.inject;
  return app;
}

function openSse(
  port: number,
  path: string,
  lastEventId: string,
  capability?: string,
): Promise<{ request: ReturnType<typeof request>; response: IncomingMessage }> {
  return new Promise((resolve, reject) => {
    const client = request({
      host: "127.0.0.1",
      port,
      path,
      headers: {
        "last-event-id": lastEventId,
        ...(capability ? { [GATEWAY_CAPABILITY_HEADER]: capability } : {}),
      },
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
