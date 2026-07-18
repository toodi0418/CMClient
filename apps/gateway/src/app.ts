import Fastify, {
  type FastifyInstance,
  type FastifyReply,
  type FastifyRequest,
} from "fastify";
import { Type } from "@sinclair/typebox";
import {
  ApiErrorSchema,
  AprsOutboxEntryListSchema,
  AprsRuntimeStatusSchema,
  BuildMetadataSchema,
  CallMeshOverviewSchema,
  DomainEventListSchema,
  JobAcceptedSchema,
  JobDetailSchema,
  MeshMessageListSchema,
  MeshNodeListSchema,
  MeshTelemetryListSchema,
  MeshtasticRuntimeStatusSchema,
  PositionCanonicalEventListSchema,
  ProxyStatusSchema,
  SystemCapabilitiesSchema,
  SystemHealthSchema,
  SystemStatusSchema,
  type DomainEvent,
  type AprsOutboxEntry,
  type AprsRuntimeStatus,
  type CallMeshOverview,
  type JobAccepted,
  type JobDetail,
  type MeshMessage,
  type MeshNode,
  type MeshTelemetry,
  type MeshtasticRuntimeStatus,
  type PositionCanonicalEvent,
  type ProxyStatus,
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
  isDockerDeployment,
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
  submitIntegrityCheck?(
    correlationId?: string,
    idempotencyKey?: string,
  ): { created: boolean; job: JobDetail };
  submitBackup?(
    correlationId?: string,
    idempotencyKey?: string,
  ): { created: boolean; job: JobDetail };
}

export interface GatewayDomainReadApi {
  listNodes(limit: number): MeshNode[];
  listMessages(limit: number): MeshMessage[];
  listTelemetry(limit: number): MeshTelemetry[];
  queryTelemetry?(query: GatewayTelemetryRangeQuery): MeshTelemetry[];
  listPositions(limit: number): PositionCanonicalEvent[];
  listAprsOutbox(limit: number): AprsOutboxEntry[];
}

export interface GatewayTelemetryRangeQuery {
  limit: number;
  meshNetworkId?: string;
  nodeNum?: number;
  metricKind?: string;
  from?: string;
  to?: string;
}

export interface GatewayCallMeshReadApi {
  getOverview(): CallMeshOverview;
}

export interface GatewayProxyReadApi {
  status(): ProxyStatus;
}

export interface GatewayMeshtasticReadApi {
  status(): MeshtasticRuntimeStatus;
}

export interface GatewayAprsReadApi {
  status(): AprsRuntimeStatus;
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

interface ListQuery {
  limit?: number;
}

interface TelemetryQuery extends ListQuery {
  meshNetworkId?: string;
  nodeNum?: number;
  metricKind?: string;
  from?: string;
  to?: string;
}

interface IdempotencyHeaders {
  "idempotency-key"?: string;
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
    !isGatewayHostAllowed(host, environment) ||
    !Number.isInteger(port) ||
    port < 0 ||
    port > 65_535
  ) {
    throw new GatewayConfigurationError();
  }
  return { host, port };
}

function isGatewayHostAllowed(
  host: string,
  environment: Record<string, string | undefined>,
): boolean {
  return (
    isLoopbackHost(host) ||
    (isDockerDeployment(environment) && host === "0.0.0.0")
  );
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
    domain?: GatewayDomainReadApi,
    callmesh?: GatewayCallMeshReadApi,
    proxy?: GatewayProxyReadApi,
    meshtastic?: GatewayMeshtasticReadApi,
    aprs?: GatewayAprsReadApi,
  ) {
    this.options = options;
    this.app = createGatewayApp(
      logger,
      system,
      eventBus,
      {},
      jobs,
      domain,
      callmesh,
      proxy,
      meshtastic,
      aprs,
    );
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
  domain?: GatewayDomainReadApi,
  callmesh?: GatewayCallMeshReadApi,
  proxy?: GatewayProxyReadApi,
  meshtastic?: GatewayMeshtasticReadApi,
  aprs?: GatewayAprsReadApi,
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
    "/api/v1/aprs",
    {
      schema: {
        response: { 200: AprsRuntimeStatusSchema },
      },
    },
    () =>
      aprs?.status() ?? {
        configured: false,
        running: false,
        monitorStatus: "stopped",
        mappedCallsigns: 0,
        pendingOutbox: 0,
        failedOutbox: 0,
      },
  );
  app.get(
    "/api/v1/meshtastic",
    {
      schema: {
        response: { 200: MeshtasticRuntimeStatusSchema },
      },
    },
    () => meshtastic?.status() ?? { configured: false },
  );
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
  app.get<{ Querystring: ListQuery }>(
    "/api/v1/nodes",
    {
      schema: {
        querystring: listQuerySchema(),
        response: { 200: MeshNodeListSchema, 503: ApiErrorSchema },
      },
    },
    (request, reply) =>
      domain
        ? { items: domain.listNodes(resolveListLimit(request.query.limit)) }
        : sendDomainDataUnavailable(request, reply),
  );
  app.get(
    "/api/v1/callmesh",
    {
      schema: {
        response: { 200: CallMeshOverviewSchema, 503: ApiErrorSchema },
      },
    },
    (request, reply) =>
      callmesh
        ? callmesh.getOverview()
        : sendCallMeshUnavailable(request, reply),
  );
  app.get(
    "/api/v1/proxy",
    {
      schema: {
        response: { 200: ProxyStatusSchema, 503: ApiErrorSchema },
      },
    },
    (request, reply) =>
      proxy ? proxy.status() : sendProxyUnavailable(request, reply),
  );
  app.get<{ Querystring: ListQuery }>(
    "/api/v1/aprs/outbox",
    {
      schema: {
        querystring: listQuerySchema(),
        response: { 200: AprsOutboxEntryListSchema, 503: ApiErrorSchema },
      },
    },
    (request, reply) =>
      domain
        ? {
            items: domain.listAprsOutbox(resolveListLimit(request.query.limit)),
          }
        : sendDomainDataUnavailable(request, reply),
  );
  app.get<{ Querystring: ListQuery }>(
    "/api/v1/messages",
    {
      schema: {
        querystring: listQuerySchema(),
        response: { 200: MeshMessageListSchema, 503: ApiErrorSchema },
      },
    },
    (request, reply) =>
      domain
        ? { items: domain.listMessages(resolveListLimit(request.query.limit)) }
        : sendDomainDataUnavailable(request, reply),
  );
  app.get<{ Querystring: TelemetryQuery }>(
    "/api/v1/telemetry",
    {
      schema: {
        querystring: telemetryQuerySchema(),
        response: {
          200: MeshTelemetryListSchema,
          400: ApiErrorSchema,
          503: ApiErrorSchema,
        },
      },
    },
    (request, reply) => {
      if (!domain) {
        return sendDomainDataUnavailable(request, reply);
      }
      const query = resolveTelemetryQuery(request.query);
      if (!query) {
        return sendTelemetryRangeInvalid(request, reply);
      }
      if (domain.queryTelemetry) {
        return { items: domain.queryTelemetry(query) };
      }
      if (hasTelemetryRangeFilter(request.query)) {
        return sendDomainDataUnavailable(request, reply);
      }
      return { items: domain.listTelemetry(query.limit) };
    },
  );
  app.get<{ Querystring: ListQuery }>(
    "/api/v1/positions",
    {
      schema: {
        querystring: listQuerySchema(),
        response: {
          200: PositionCanonicalEventListSchema,
          503: ApiErrorSchema,
        },
      },
    },
    (request, reply) =>
      domain
        ? { items: domain.listPositions(resolveListLimit(request.query.limit)) }
        : sendDomainDataUnavailable(request, reply),
  );
  app.get("/api/v1/events", (request, reply) => {
    openSseStream(request, reply, eventBus, logger, heartbeatIntervalMs);
  });
  app.get<{ Querystring: ListQuery }>(
    "/api/v1/events/recent",
    {
      schema: {
        querystring: listQuerySchema(),
        response: { 200: DomainEventListSchema },
      },
    },
    (request) => ({
      items: eventBus.recent(resolveListLimit(request.query.limit)),
    }),
  );
  app.post<{ Headers: IdempotencyHeaders }>(
    "/api/v1/diagnostics/integrity-check",
    {
      schema: {
        response: { 202: JobAcceptedSchema, 503: ApiErrorSchema },
      },
    },
    (request, reply) => {
      if (!jobs?.submitIntegrityCheck) {
        return sendJobEngineUnavailable(request, reply);
      }
      const idempotencyKey = request.headers["idempotency-key"];
      if (
        idempotencyKey !== undefined &&
        !/^[a-zA-Z0-9._:-]{1,128}$/.test(idempotencyKey)
      ) {
        return sendJobInputInvalid(request, reply);
      }
      const submitted = jobs.submitIntegrityCheck(
        request.correlationId,
        idempotencyKey,
      );
      const accepted: JobAccepted = {
        jobId: submitted.job.id,
        reused: !submitted.created,
      };
      return reply.code(202).send(accepted);
    },
  );
  app.post<{ Headers: IdempotencyHeaders }>(
    "/api/v1/backups",
    {
      schema: {
        response: { 202: JobAcceptedSchema, 503: ApiErrorSchema },
      },
    },
    (request, reply) => {
      if (!jobs?.submitBackup) {
        return sendJobEngineUnavailable(request, reply);
      }
      const idempotencyKey = request.headers["idempotency-key"];
      if (
        idempotencyKey !== undefined &&
        !/^[a-zA-Z0-9._:-]{1,128}$/.test(idempotencyKey)
      ) {
        return sendJobInputInvalid(request, reply);
      }
      const submitted = jobs.submitBackup(
        request.correlationId,
        idempotencyKey,
      );
      return reply.code(202).send({
        jobId: submitted.job.id,
        reused: !submitted.created,
      } satisfies JobAccepted);
    },
  );
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

function listQuerySchema() {
  return Type.Object({
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 200 })),
  });
}

function telemetryQuerySchema() {
  const timestamp = Type.String({
    pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$",
  });
  return Type.Object(
    {
      limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 200 })),
      meshNetworkId: Type.Optional(
        Type.String({ minLength: 1, maxLength: 128 }),
      ),
      nodeNum: Type.Optional(
        Type.Integer({ minimum: 0, maximum: 4_294_967_295 }),
      ),
      metricKind: Type.Optional(Type.String({ minLength: 1, maxLength: 64 })),
      from: Type.Optional(timestamp),
      to: Type.Optional(timestamp),
    },
    { additionalProperties: false },
  );
}

function resolveTelemetryQuery(
  query: TelemetryQuery,
): GatewayTelemetryRangeQuery | undefined {
  const fromTime =
    query.from === undefined ? undefined : Date.parse(query.from);
  const toTime = query.to === undefined ? undefined : Date.parse(query.to);
  if (
    (query.nodeNum !== undefined && query.meshNetworkId === undefined) ||
    (fromTime !== undefined && !Number.isFinite(fromTime)) ||
    (toTime !== undefined && !Number.isFinite(toTime)) ||
    (fromTime !== undefined && toTime !== undefined && fromTime > toTime)
  ) {
    return undefined;
  }
  return {
    limit: resolveListLimit(query.limit),
    ...(query.meshNetworkId !== undefined
      ? { meshNetworkId: query.meshNetworkId }
      : {}),
    ...(query.nodeNum !== undefined ? { nodeNum: query.nodeNum } : {}),
    ...(query.metricKind !== undefined ? { metricKind: query.metricKind } : {}),
    ...(fromTime !== undefined
      ? { from: new Date(fromTime).toISOString() }
      : {}),
    ...(toTime !== undefined ? { to: new Date(toTime).toISOString() } : {}),
  };
}

function hasTelemetryRangeFilter(query: TelemetryQuery): boolean {
  return (
    query.meshNetworkId !== undefined ||
    query.nodeNum !== undefined ||
    query.metricKind !== undefined ||
    query.from !== undefined ||
    query.to !== undefined
  );
}

function resolveListLimit(limit: number | undefined): number {
  return limit ?? 100;
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

function sendJobInputInvalid(request: FastifyRequest, reply: FastifyReply) {
  return reply.code(400).send({
    code: "JOB_INPUT_INVALID",
    params: {},
    traceId: request.traceId,
  });
}

function sendDomainDataUnavailable(
  request: FastifyRequest,
  reply: FastifyReply,
) {
  return reply.code(503).send({
    code: "GATEWAY_DOMAIN_DATA_UNAVAILABLE",
    params: {},
    traceId: request.traceId,
  });
}

function sendTelemetryRangeInvalid(
  request: FastifyRequest,
  reply: FastifyReply,
) {
  return reply.code(400).send({
    code: "TELEMETRY_RANGE_INVALID",
    params: {},
    traceId: request.traceId,
  });
}

function sendCallMeshUnavailable(request: FastifyRequest, reply: FastifyReply) {
  return reply.code(503).send({
    code: "CALLMESH_CLIENT_UNAVAILABLE",
    params: {},
    traceId: request.traceId,
  });
}

function sendProxyUnavailable(request: FastifyRequest, reply: FastifyReply) {
  return reply.code(503).send({
    code: "PROXY_RUNTIME_UNAVAILABLE",
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
