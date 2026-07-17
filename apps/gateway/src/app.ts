import Fastify, {
  type FastifyInstance,
  type FastifyReply,
  type FastifyRequest,
} from "fastify";
import { Type } from "@sinclair/typebox";
import {
  ApiErrorSchema,
  BuildMetadataSchema,
  JobDetailSchema,
  SystemCapabilitiesSchema,
  SystemHealthSchema,
  SystemStatusSchema,
  type DomainEvent,
  type JobDetail,
} from "@cmclient/contracts";

import {
  ConsoleStructuredLogger,
  type StructuredLogger,
  createTraceId,
  resolveCorrelationId,
  resolveTraceId,
} from "./observability.js";
import {
  defaultGatewaySystemState,
  type GatewaySystemState,
} from "./system.js";
import {
  DomainEventBus,
  formatSseEvent,
  formatSseHeartbeat,
} from "./events.js";

export interface GatewayJobApi {
  get(jobId: string): JobDetail | undefined;
  cancel(jobId: string, correlationId?: string): JobDetail | undefined;
}

declare module "fastify" {
  interface FastifyInstance {
    eventBus: DomainEventBus;
  }

  interface FastifyRequest {
    correlationId?: string;
    traceId: string;
  }
}

export interface GatewayListenOptions {
  host: string;
  port: number;
}

export interface GatewaySseOptions {
  heartbeatIntervalMs?: number;
}

interface JobIdParams {
  jobId: string;
}

export class GatewayConfigurationError extends Error {
  readonly code = "GATEWAY_LISTEN_CONFIGURATION_INVALID";
}

export function parseGatewayListenOptions(
  environment: Record<string, string | undefined>,
): GatewayListenOptions {
  const host = environment.CMCLIENT_GATEWAY_HOST?.trim() || "127.0.0.1";
  const portValue = environment.CMCLIENT_GATEWAY_PORT?.trim() || "0";
  const port = Number(portValue);
  if (
    !isLoopbackHost(host) ||
    !Number.isInteger(port) ||
    port < 0 ||
    port > 65_535
  ) {
    throw new GatewayConfigurationError();
  }
  return { host, port };
}

export class GatewayRuntime {
  readonly app: FastifyInstance;
  private readonly options: GatewayListenOptions;
  private started = false;

  constructor(
    options: GatewayListenOptions,
    logger?: StructuredLogger,
    system?: GatewaySystemState,
    eventBus?: DomainEventBus,
    jobs?: GatewayJobApi,
  ) {
    this.options = options;
    this.app = createGatewayApp(logger, system, eventBus, {}, jobs);
  }

  async start(): Promise<GatewayListenOptions> {
    if (this.started) {
      return this.address();
    }
    await this.app.listen(this.options);
    this.started = true;
    return this.address();
  }

  async close(): Promise<void> {
    if (!this.started) {
      return;
    }
    await this.app.close();
    this.started = false;
  }

  private address(): GatewayListenOptions {
    const address = this.app.server.address();
    if (!address || typeof address === "string") {
      throw new GatewayConfigurationError();
    }
    return { host: this.options.host, port: address.port };
  }
}

export function createGatewayApp(
  logger: StructuredLogger = new ConsoleStructuredLogger(),
  system: GatewaySystemState = defaultGatewaySystemState(),
  eventBus: DomainEventBus = new DomainEventBus(),
  sseOptions: GatewaySseOptions = {},
  jobs?: GatewayJobApi,
): FastifyInstance {
  const heartbeatIntervalMs = sseOptions.heartbeatIntervalMs ?? 15_000;
  if (!Number.isInteger(heartbeatIntervalMs) || heartbeatIntervalMs < 1_000) {
    throw new GatewayConfigurationError();
  }
  const app = Fastify({ logger: false });
  app.decorate("eventBus", eventBus);
  app.decorateRequest("traceId", "");
  app.addHook("onRequest", (request, reply, done) => {
    request.traceId = resolveTraceId(request.headers["x-trace-id"]);
    const correlationId = resolveCorrelationId(
      request.headers["x-correlation-id"],
    );
    if (correlationId) {
      request.correlationId = correlationId;
    }
    reply.header("x-trace-id", request.traceId);
    done();
  });
  app.addHook("onResponse", (request, reply, done) => {
    logger.log({
      level: "info",
      message: "gateway.request.complete",
      traceId: request.traceId || createTraceId(),
      ...(request.correlationId
        ? { correlationId: request.correlationId }
        : {}),
      fields: {
        method: request.method,
        path: request.routeOptions.url ?? request.url,
        statusCode: reply.statusCode,
      },
    });
    done();
  });
  app.get(
    "/api/v1/system/health",
    {
      schema: {
        response: {
          200: SystemHealthSchema,
        },
      },
    },
    async () => ({ status: "ok" }),
  );
  app.get(
    "/api/v1/system/version",
    {
      schema: { response: { 200: BuildMetadataSchema } },
    },
    async () => system.build,
  );
  app.get(
    "/api/v1/system/capabilities",
    {
      schema: { response: { 200: SystemCapabilitiesSchema } },
    },
    async () => system.capabilities,
  );
  app.get(
    "/api/v1/system/status",
    {
      schema: {
        response: { 200: SystemStatusSchema },
      },
    },
    async () => ({ health: "ok", build: system.build }),
  );
  app.get("/api/v1/events", (request, reply) => {
    openSseStream(request, reply, eventBus, logger, heartbeatIntervalMs);
  });
  app.get<{ Params: JobIdParams }>(
    "/api/v1/jobs/:jobId",
    {
      schema: {
        params: jobIdParamsSchema(),
        response: {
          200: JobDetailSchema,
          404: ApiErrorSchema,
          503: ApiErrorSchema,
        },
      },
    },
    async (request, reply) => {
      if (!jobs) {
        return sendJobEngineUnavailable(request, reply);
      }
      const job = jobs.get(request.params.jobId);
      return job ?? sendJobNotFound(request, reply);
    },
  );
  app.post<{ Params: JobIdParams }>(
    "/api/v1/jobs/:jobId/cancel",
    {
      schema: {
        params: jobIdParamsSchema(),
        response: {
          202: JobDetailSchema,
          404: ApiErrorSchema,
          503: ApiErrorSchema,
        },
      },
    },
    async (request, reply) => {
      if (!jobs) {
        return sendJobEngineUnavailable(request, reply);
      }
      const job = jobs.cancel(request.params.jobId, request.correlationId);
      if (!job) {
        return sendJobNotFound(request, reply);
      }
      return reply.code(202).send(job);
    },
  );
  app.get<{ Params: JobIdParams }>(
    "/api/v1/jobs/:jobId/events",
    { schema: { params: jobIdParamsSchema() } },
    (request, reply) => {
      if (!jobs) {
        return sendJobEngineUnavailable(request, reply);
      }
      const job = jobs.get(request.params.jobId);
      if (!job) {
        return sendJobNotFound(request, reply);
      }
      openSseStream(
        request,
        reply,
        eventBus,
        logger,
        heartbeatIntervalMs,
        (event) => isJobEvent(event, job.id),
      );
    },
  );
  return app;
}

function isLoopbackHost(host: string): boolean {
  return host === "localhost" || host === "127.0.0.1" || host === "::1";
}

function parseLastEventId(
  value: string | string[] | undefined,
): string | undefined {
  return typeof value === "string" && /^[a-zA-Z0-9-]{1,128}$/.test(value)
    ? value
    : undefined;
}

function jobIdParamsSchema() {
  return Type.Object({ jobId: Type.String({ minLength: 1, maxLength: 128 }) });
}

function sendJobNotFound(request: FastifyRequest, reply: FastifyReply) {
  return reply.code(404).send({
    code: "JOB_NOT_FOUND",
    params: {},
    traceId: request.traceId,
  });
}

function sendJobEngineUnavailable(
  request: FastifyRequest,
  reply: FastifyReply,
) {
  return reply.code(503).send({
    code: "GATEWAY_JOB_ENGINE_UNAVAILABLE",
    params: {},
    traceId: request.traceId,
  });
}

function isJobEvent(event: DomainEvent, jobId: string): boolean {
  return event.type.startsWith("job.") && event.payload.jobId === jobId;
}

function openSseStream(
  request: FastifyRequest,
  reply: FastifyReply,
  eventBus: DomainEventBus,
  logger: StructuredLogger,
  heartbeatIntervalMs: number,
  filter: (event: DomainEvent) => boolean = () => true,
): void {
  reply.hijack();
  const response = reply.raw;
  const replay = eventBus
    .replayAfter(parseLastEventId(request.headers["last-event-id"]))
    .filter(filter);
  let closed = false;
  const session: {
    heartbeat?: NodeJS.Timeout;
    unsubscribe?: () => void;
  } = {};

  const close = (reason: string): void => {
    if (closed) {
      return;
    }
    closed = true;
    if (session.heartbeat) {
      clearInterval(session.heartbeat);
    }
    session.unsubscribe?.();
    logger.log({
      level: reason === "SSE_SLOW_CONSUMER" ? "warn" : "info",
      message: "gateway.sse_client_closed",
      traceId: request.traceId || createTraceId(),
      ...(request.correlationId
        ? { correlationId: request.correlationId }
        : {}),
      fields: { reason },
    });
    if (!response.destroyed && !response.writableEnded) {
      response.end();
    }
  };
  const send = (message: string): boolean => {
    if (closed || response.destroyed || response.writableEnded) {
      return false;
    }
    if (!response.write(message)) {
      close("SSE_SLOW_CONSUMER");
      return false;
    }
    return true;
  };

  response.once("close", () => close("SSE_CLIENT_DISCONNECTED"));
  response.once("error", () => close("SSE_CLIENT_ERROR"));
  response.writeHead(200, {
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
    "content-type": "text/event-stream; charset=utf-8",
    "x-accel-buffering": "no",
  });
  response.flushHeaders();
  session.unsubscribe = eventBus.subscribe((event) => {
    if (filter(event)) {
      send(formatSseEvent(event));
    }
  });
  for (const event of replay) {
    if (!send(formatSseEvent(event))) {
      return;
    }
  }
  if (!send(formatSseHeartbeat())) {
    return;
  }
  session.heartbeat = setInterval(() => {
    send(formatSseHeartbeat());
  }, heartbeatIntervalMs);
  session.heartbeat.unref();
}
