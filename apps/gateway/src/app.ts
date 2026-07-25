import Fastify, {
  type FastifyInstance,
  type FastifyReply,
  type FastifyRequest,
} from "fastify";
import fastifySwagger from "@fastify/swagger";
import { fastifySSE, type SSEMessage } from "@fastify/sse";
import type { TypeBoxTypeProvider } from "@fastify/type-provider-typebox";
import { Type } from "@sinclair/typebox";
import {
  AGENT_CONTRACT_SCHEMAS,
  ApiErrorSchema,
  AprsOutboxEntryListSchema,
  AprsRuntimeStatusSchema,
  ComponentIdentityReportSchema,
  CallMeshOverviewSchema,
  DomainEventSchema,
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
  type GatewaySystemState,
} from "./system.js";
import {
  DEFAULT_SSE_FRAME_MAX_BYTES,
  DomainEventBus,
  DomainEventSubscriberLimitError,
} from "./events.js";
import {
  GATEWAY_CAPABILITY_HEADER,
  gatewayCapabilityMatches,
  isGatewayCapability,
} from "./bootstrap.js";

const SSE_HTTP_HIGH_WATER_MARK_BYTES = DEFAULT_SSE_FRAME_MAX_BYTES + 4 * 1024;
const SSE_PENDING_EVENT_MAX_BYTES = DEFAULT_SSE_FRAME_MAX_BYTES;
const MANAGEMENT_OPENAPI_SCHEMAS = Object.fromEntries(
  [...AGENT_CONTRACT_SCHEMAS, DomainEventSchema].map((schema) => {
    if (typeof schema.$id !== "string") {
      throw new Error("AGENT_CONTRACT_SCHEMA_ID_REQUIRED");
    }
    return [schema.$id, schema];
  }),
);
const GATEWAY_ROUTE_METHODS = [
  "DELETE",
  "GET",
  "HEAD",
  "OPTIONS",
  "PATCH",
  "POST",
  "PUT",
] as const;

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

export interface GatewayAccessOptions {
  capability: string;
}

type GatewaySseSessionCloser = (reason: string) => void;

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

export class GatewayAccessConfigurationError extends Error {
  readonly code = "GATEWAY_CAPABILITY_CONFIGURATION_INVALID";

  constructor() {
    super("GATEWAY_CAPABILITY_CONFIGURATION_INVALID");
  }
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
    access?: GatewayAccessOptions,
  ) {
    if (options.host !== "127.0.0.1" || options.port !== 0) {
      throw new GatewayConfigurationError();
    }
    if (!isGatewayCapability(access?.capability)) {
      throw new GatewayAccessConfigurationError();
    }
    this.options = options;
    this.app = createGatewayApp(
      access,
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
  access: GatewayAccessOptions | undefined,
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
  if (!isGatewayCapability(access?.capability)) {
    throw new GatewayAccessConfigurationError();
  }
  const app = Fastify({
    logger: false,
    http: { highWaterMark: SSE_HTTP_HIGH_WATER_MARK_BYTES },
  }).withTypeProvider<TypeBoxTypeProvider>();
  const sseSessions = new Set<GatewaySseSessionCloser>();
  app.register(fastifySwagger, {
    openapi: {
      openapi: "3.1.0",
      info: {
        title: "CMClient Gateway private API",
        version: "2.0.0",
      },
      components: {
        schemas: MANAGEMENT_OPENAPI_SCHEMAS,
        securitySchemes: {
          gatewayCapability: {
            type: "apiKey",
            in: "header",
            name: GATEWAY_CAPABILITY_HEADER,
          },
          agentBrowserSession: {
            type: "apiKey",
            in: "cookie",
            name: "cmclient.sid",
          },
        },
      },
      security: [{ gatewayCapability: [] }],
    },
    exposeHeadRoutes: false,
  });
  app.register(fastifySSE, { heartbeatInterval: heartbeatIntervalMs });
  app.decorate("eventBus", eventBus);
  app.decorateRequest("traceId", "");
  app.addHook("preClose", () => {
    for (const close of [...sseSessions]) {
      close("SSE_SERVER_SHUTDOWN");
    }
  });
  app.addHook("onRequest", (request, reply, done) => {
    request.traceId = resolveTraceId(request.headers["x-trace-id"]);
    reply.header("x-trace-id", request.traceId);
    if (
      !gatewayCapabilityMatches(
        request.headers[GATEWAY_CAPABILITY_HEADER],
        access.capability,
      )
    ) {
      reply
        .header("cache-control", "no-store")
        .code(403)
        .send(
          gatewayErrorEnvelope(request, reply, "GATEWAY_CAPABILITY_REJECTED"),
        );
      return;
    }
    delete request.headers[GATEWAY_CAPABILITY_HEADER];
    const correlationId = resolveCorrelationId(
      request.headers["x-correlation-id"],
    );
    if (correlationId) {
      request.correlationId = correlationId;
    }
    done();
  });
  app.setErrorHandler((error, request, reply) => {
    const statusCode = frameworkStatusCode(error);
    const code = isFrameworkValidationError(error)
      ? frameworkValidationErrorCode(error, request)
      : frameworkErrorCode(statusCode);
    return reply
      .code(statusCode)
      .send(gatewayErrorEnvelope(request, reply, code));
  });
  app.setNotFoundHandler((request, reply) => {
    const allowedMethods = allowedMethodsForPath(app, request.url);
    if (allowedMethods.length > 0) {
      reply.header("allow", allowedMethods.join(", "));
      return reply
        .code(405)
        .send(
          gatewayErrorEnvelope(request, reply, "GATEWAY_METHOD_NOT_ALLOWED"),
        );
    }
    return reply
      .code(404)
      .send(gatewayErrorEnvelope(request, reply, "GATEWAY_ROUTE_NOT_FOUND"));
  });
  app.addHook("onSend", (request, reply, payload, done) => {
    if (reply.statusCode < 400 || isGatewayErrorEnvelope(payload)) {
      done(null, payload);
      return;
    }
    reply.type("application/json; charset=utf-8");
    done(
      null,
      JSON.stringify(
        gatewayErrorEnvelope(
          request,
          reply,
          frameworkErrorCode(reply.statusCode),
        ),
      ),
    );
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
        path: request.routeOptions.url ?? "unmatched",
        statusCode: reply.statusCode,
      },
    });
    done();
  });
  app.register(async function gatewayRoutes(routeApp) {
    const app = routeApp.withTypeProvider<TypeBoxTypeProvider>();
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
          monitorStatus: "stopped" as const,
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
      async () => ({ status: "ok" as const }),
    );
    app.get(
      "/api/v1/system/version",
      {
        schema: { response: { 200: ComponentIdentityReportSchema } },
      },
      async () => system.identity,
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
      async () => ({
        schemaVersion: 2 as const,
        health: "ok" as const,
        identity: system.identity,
      }),
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
      (request, reply) => {
        if (callmesh) {
          return callmesh.getOverview();
        }
        sendCallMeshUnavailable(request, reply);
      },
    );
    app.get(
      "/api/v1/proxy",
      {
        schema: {
          response: { 200: ProxyStatusSchema, 503: ApiErrorSchema },
        },
      },
      (request, reply) => {
        if (proxy) {
          return proxy.status();
        }
        sendProxyUnavailable(request, reply);
      },
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
              items: domain.listAprsOutbox(
                resolveListLimit(request.query.limit),
              ),
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
          ? {
              items: domain.listMessages(resolveListLimit(request.query.limit)),
            }
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
          ? {
              items: domain.listPositions(
                resolveListLimit(request.query.limit),
              ),
            }
          : sendDomainDataUnavailable(request, reply),
    );
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
          headers: idempotencyHeadersSchema(),
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
        let submitted: ReturnType<
          NonNullable<GatewayJobApi["submitIntegrityCheck"]>
        >;
        try {
          submitted = jobs.submitIntegrityCheck(
            request.correlationId,
            idempotencyKey,
          );
        } catch (error) {
          if (isJobQueueFull(error)) {
            return sendJobQueueFull(request, reply);
          }
          throw error;
        }
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
          headers: idempotencyHeadersSchema(),
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
        let submitted: ReturnType<NonNullable<GatewayJobApi["submitBackup"]>>;
        try {
          submitted = jobs.submitBackup(request.correlationId, idempotencyKey);
        } catch (error) {
          if (isJobQueueFull(error)) {
            return sendJobQueueFull(request, reply);
          }
          throw error;
        }
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
    app.register(async function gatewaySseRoutes(app) {
      app.get(
        "/api/v1/events",
        {
          sse: "only",
          schema: {
            headers: sseHeadersSchema(),
            response: { 400: ApiErrorSchema, 503: ApiErrorSchema },
          },
        },
        async (request, reply) =>
          openSseStream(request, reply, eventBus, logger, sseSessions),
      );
      app.get<{ Params: JobIdParams }>(
        "/api/v1/jobs/:jobId/events",
        {
          sse: "only",
          schema: {
            headers: sseHeadersSchema(),
            params: jobIdParamsSchema(),
            response: {
              400: ApiErrorSchema,
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
          if (!job) {
            return sendJobNotFound(request, reply);
          }
          return openSseStream(
            request,
            reply,
            eventBus,
            logger,
            sseSessions,
            (event) => isJobEvent(event, job.id),
          );
        },
      );
    });
  });
  return app;
}

function frameworkStatusCode(error: unknown): number {
  const statusCode =
    error && typeof error === "object" && "statusCode" in error
      ? error.statusCode
      : undefined;
  return typeof statusCode === "number" &&
    statusCode >= 400 &&
    statusCode <= 599
    ? statusCode
    : 500;
}

function isFrameworkValidationError(error: unknown): boolean {
  return (
    error !== null &&
    typeof error === "object" &&
    "validation" in error &&
    Array.isArray(error.validation)
  );
}

function frameworkValidationErrorCode(
  error: unknown,
  request: FastifyRequest,
): string {
  const context =
    error && typeof error === "object" && "validationContext" in error
      ? error.validationContext
      : undefined;
  if (context !== "headers") {
    return "GATEWAY_REQUEST_SCHEMA_INVALID";
  }
  const path = request.routeOptions.url;
  if (
    path === "/api/v1/diagnostics/integrity-check" ||
    path === "/api/v1/backups"
  ) {
    return "JOB_INPUT_INVALID";
  }
  if (path === "/api/v1/events" || path === "/api/v1/jobs/:jobId/events") {
    return "SSE_CURSOR_INVALID";
  }
  return "GATEWAY_REQUEST_SCHEMA_INVALID";
}

function frameworkErrorCode(statusCode: number): string {
  switch (statusCode) {
    case 400:
      return "GATEWAY_REQUEST_INVALID";
    case 404:
      return "GATEWAY_ROUTE_NOT_FOUND";
    case 405:
      return "GATEWAY_METHOD_NOT_ALLOWED";
    case 406:
      return "GATEWAY_SSE_NOT_ACCEPTABLE";
    case 413:
      return "GATEWAY_REQUEST_TOO_LARGE";
    case 415:
      return "GATEWAY_MEDIA_TYPE_UNSUPPORTED";
    default:
      return statusCode >= 500
        ? "GATEWAY_INTERNAL_ERROR"
        : "GATEWAY_REQUEST_REJECTED";
  }
}

function gatewayErrorEnvelope(
  request: FastifyRequest,
  reply: FastifyReply,
  code: string,
) {
  const traceId = request.traceId || createTraceId();
  request.traceId = traceId;
  reply.header("x-trace-id", traceId);
  return { code, params: {}, traceId };
}

function isGatewayErrorEnvelope(payload: unknown): boolean {
  let value = payload;
  if (Buffer.isBuffer(value)) {
    value = value.toString("utf8");
  }
  if (typeof value === "string") {
    try {
      value = JSON.parse(value) as unknown;
    } catch {
      return false;
    }
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const envelope = value as Record<string, unknown>;
  const keys = Object.keys(envelope).sort();
  return (
    keys.length === 3 &&
    keys[0] === "code" &&
    keys[1] === "params" &&
    keys[2] === "traceId" &&
    typeof envelope.code === "string" &&
    envelope.params !== null &&
    typeof envelope.params === "object" &&
    !Array.isArray(envelope.params) &&
    typeof envelope.traceId === "string" &&
    envelope.traceId.length > 0
  );
}

function allowedMethodsForPath(app: FastifyInstance, url: string): string[] {
  const path = url.split("?", 1)[0] || "/";
  return GATEWAY_ROUTE_METHODS.filter(
    (method) => app.findRoute({ method, url: path }) !== null,
  );
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

function idempotencyHeadersSchema() {
  return Type.Object(
    {
      "idempotency-key": Type.Optional(
        Type.String({ pattern: "^[a-zA-Z0-9._:-]{1,128}$" }),
      ),
    },
    { additionalProperties: true },
  );
}

function sseHeadersSchema() {
  return Type.Object(
    {
      "last-event-id": Type.Optional(
        Type.String({ pattern: "^[a-zA-Z0-9-]{1,128}$" }),
      ),
    },
    { additionalProperties: true },
  );
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

function sendJobQueueFull(request: FastifyRequest, reply: FastifyReply) {
  return reply.code(503).send({
    code: "JOB_QUEUE_FULL",
    params: {},
    traceId: request.traceId,
  });
}

function isJobQueueFull(error: unknown): boolean {
  return (
    error instanceof Error && "code" in error && error.code === "JOB_QUEUE_FULL"
  );
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

async function openSseStream(
  request: FastifyRequest,
  reply: FastifyReply,
  eventBus: DomainEventBus,
  logger: StructuredLogger,
  sessions: Set<GatewaySseSessionCloser>,
  filter: (event: DomainEvent) => boolean = () => true,
): Promise<void> {
  const source = new GatewaySseEventSource();
  let unsubscribe = (): void => undefined;
  let closed = false;

  const close = (reason: string): void => {
    if (closed) {
      return;
    }
    closed = true;
    unsubscribe();
    sessions.delete(close);
    source.close();
    logger.log({
      level:
        reason.startsWith("SSE_") &&
        !["SSE_CLIENT_DISCONNECTED", "SSE_SERVER_SHUTDOWN"].includes(reason)
          ? "warn"
          : "info",
      message: "gateway.sse_client_closed",
      traceId: request.traceId || createTraceId(),
      ...(request.correlationId
        ? { correlationId: request.correlationId }
        : {}),
      fields: { reason },
    });
    if (reply.sse.isConnected) {
      reply.sse.close();
    }
  };
  const enqueue = (event: DomainEvent): void => {
    if (filter(event) && !source.push(event)) {
      close("SSE_SLOW_CONSUMER");
    }
  };

  let replay: DomainEvent[];
  try {
    const subscription = eventBus.subscribeWithReplay(
      parseLastEventId(reply.sse.lastEventId ?? undefined),
      enqueue,
    );
    replay = subscription.replay;
    unsubscribe = subscription.unsubscribe;
  } catch (error) {
    if (!(error instanceof DomainEventSubscriberLimitError)) {
      throw error;
    }
    logger.log({
      level: "warn",
      message: "gateway.sse_client_rejected",
      traceId: request.traceId || createTraceId(),
      ...(request.correlationId
        ? { correlationId: request.correlationId }
        : {}),
      fields: { reason: "SSE_SUBSCRIBER_LIMIT_REACHED" },
    });
    await reply.code(503).send({
      code: "SSE_SUBSCRIBER_LIMIT_REACHED",
      params: {},
      traceId: request.traceId,
    });
    return;
  }

  reply.sse.onClose(() => close("SSE_CLIENT_DISCONNECTED"));
  sessions.add(close);
  reply.sse.sendHeaders();
  reply.raw.flushHeaders();
  await reply.sse.send(replayThenLive(replay, source, filter));
}

async function* replayThenLive(
  replay: readonly DomainEvent[],
  live: GatewaySseEventSource,
  filter: (event: DomainEvent) => boolean,
): AsyncGenerator<SSEMessage> {
  for (const event of replay) {
    if (live.isClosed) {
      return;
    }
    if (filter(event)) {
      yield sseMessage(event);
    }
  }
  for await (const message of live) {
    yield message;
  }
}

function sseMessage(event: DomainEvent): SSEMessage {
  return {
    id: event.eventId,
    event: event.type,
    data: event,
  };
}

class GatewaySseEventSource implements AsyncIterable<SSEMessage> {
  private readonly pending: Array<{ bytes: number; message: SSEMessage }> = [];
  private pendingBytes = 0;
  private closed = false;
  private waiter: ((result: IteratorResult<SSEMessage>) => void) | undefined;

  get isClosed(): boolean {
    return this.closed;
  }

  push(event: DomainEvent): boolean {
    if (this.closed) {
      return false;
    }
    const message = sseMessage(event);
    const bytes = Buffer.byteLength(JSON.stringify(event), "utf8");
    if (this.waiter) {
      const resolve = this.waiter;
      this.waiter = undefined;
      resolve({ done: false, value: message });
      return true;
    }
    if (this.pendingBytes + bytes > SSE_PENDING_EVENT_MAX_BYTES) {
      return false;
    }
    this.pending.push({ bytes, message });
    this.pendingBytes += bytes;
    return true;
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.pending.length = 0;
    this.pendingBytes = 0;
    const resolve = this.waiter;
    this.waiter = undefined;
    resolve?.({ done: true, value: undefined });
  }

  [Symbol.asyncIterator](): AsyncIterator<SSEMessage> {
    return {
      next: () => {
        const entry = this.pending.shift();
        if (entry) {
          this.pendingBytes -= entry.bytes;
          return Promise.resolve({ done: false, value: entry.message });
        }
        if (this.closed) {
          return Promise.resolve({ done: true, value: undefined });
        }
        return new Promise<IteratorResult<SSEMessage>>((resolve) => {
          this.waiter = resolve;
        });
      },
      return: () => {
        this.close();
        return Promise.resolve({ done: true, value: undefined });
      },
    };
  }
}
