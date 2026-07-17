import Fastify, { type FastifyInstance } from "fastify";
import { Type } from "@sinclair/typebox";
import {
  BuildMetadataSchema,
  SystemCapabilitiesSchema,
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

declare module "fastify" {
  interface FastifyRequest {
    correlationId?: string;
    traceId: string;
  }
}

export interface GatewayListenOptions {
  host: string;
  port: number;
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
  ) {
    this.options = options;
    this.app = createGatewayApp(logger, system);
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
): FastifyInstance {
  const app = Fastify({ logger: false });
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
          200: Type.Object({ status: Type.Literal("ok") }),
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
        response: {
          200: Type.Object({
            health: Type.Literal("ok"),
            build: BuildMetadataSchema,
          }),
        },
      },
    },
    async () => ({ health: "ok", build: system.build }),
  );
  return app;
}

function isLoopbackHost(host: string): boolean {
  return host === "localhost" || host === "127.0.0.1" || host === "::1";
}
