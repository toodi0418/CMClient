import {
  ApiErrorSchema,
  AprsOutboxEntryListSchema,
  BuildMetadataSchema,
  CallMeshOverviewSchema,
  JobDetailSchema,
  MeshMessageListSchema,
  MeshNodeListSchema,
  MeshTelemetryListSchema,
  PositionCanonicalEventListSchema,
  SystemCapabilitiesSchema,
  SystemHealthSchema,
  SystemStatusSchema,
  type BuildMetadata,
  type CallMeshOverview,
  type ApiError,
  type AprsOutboxEntryList,
  type JobDetail,
  type MeshMessageList,
  type MeshNodeList,
  type MeshTelemetryList,
  type PositionCanonicalEventList,
  type SystemCapabilities,
  type SystemHealth,
  type SystemStatus,
} from "@cmclient/contracts";
import type { TSchema } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

export const DEFAULT_GATEWAY_API_BASE_URL = "/api/v1";

export interface GatewayApiClientOptions {
  baseUrl?: string;
  fetch?: typeof fetch;
  traceIdFactory?: () => string;
}

export interface GatewayApiErrorOptions {
  code: string;
  params?: Record<string, string | number | boolean | null>;
  traceId?: string;
  status?: number;
  retryable?: boolean;
  cause?: unknown;
}

export class GatewayApiError extends Error {
  readonly code: string;
  readonly params: Record<string, string | number | boolean | null>;
  readonly traceId: string | undefined;
  readonly status: number | undefined;
  readonly retryable: boolean;

  constructor(options: GatewayApiErrorOptions) {
    super(options.code, { cause: options.cause });
    this.name = "GatewayApiError";
    this.code = options.code;
    this.params = options.params ?? {};
    this.traceId = options.traceId;
    this.status = options.status;
    this.retryable = options.retryable ?? false;
  }
}

export function isGatewayApiError(error: unknown): error is GatewayApiError {
  return error instanceof GatewayApiError;
}

export function createNetworkError(cause?: unknown): GatewayApiError {
  return new GatewayApiError({
    code: "GATEWAY_NETWORK_UNAVAILABLE",
    retryable: true,
    ...(cause === undefined ? {} : { cause }),
  });
}

export async function mapGatewayResponseError(
  response: Response,
): Promise<GatewayApiError> {
  const payload = await readJson(response);
  const traceId = response.headers.get("x-trace-id") ?? undefined;

  if (checkSchema(ApiErrorSchema, payload)) {
    const apiError = payload as ApiError;
    return new GatewayApiError({
      code: apiError.code,
      params: apiError.params,
      traceId: apiError.traceId,
      status: response.status,
      retryable: isRetryable(response.status, apiError.code),
    });
  }

  if (isLooseStableError(payload)) {
    return new GatewayApiError({
      code: payload.code,
      ...(payload.params ? { params: payload.params } : {}),
      ...(traceId ? { traceId } : {}),
      status: response.status,
      retryable: isRetryable(response.status, payload.code),
    });
  }

  return new GatewayApiError({
    code: `GATEWAY_HTTP_${response.status}`,
    ...(traceId ? { traceId } : {}),
    status: response.status,
    retryable: isRetryable(response.status),
  });
}

export class GatewayApiClient {
  private readonly baseUrl: string;
  private readonly fetchImplementation: typeof fetch;
  private readonly traceIdFactory: () => string;

  constructor(options: GatewayApiClientOptions = {}) {
    this.baseUrl = normalizeBaseUrl(
      options.baseUrl ?? DEFAULT_GATEWAY_API_BASE_URL,
    );
    this.fetchImplementation = options.fetch ?? globalThis.fetch;
    this.traceIdFactory = options.traceIdFactory ?? defaultTraceId;
  }

  readonly system = {
    health: () =>
      this.request<SystemHealth>("/system/health", SystemHealthSchema),
    version: () =>
      this.request<BuildMetadata>("/system/version", BuildMetadataSchema),
    capabilities: () =>
      this.request<SystemCapabilities>(
        "/system/capabilities",
        SystemCapabilitiesSchema,
      ),
    status: () =>
      this.request<SystemStatus>("/system/status", SystemStatusSchema),
  };

  readonly jobs = {
    get: (jobId: string) =>
      this.requestJob<JobDetail>(jobId, "GET", JobDetailSchema),
    cancel: (jobId: string) =>
      this.requestJob<JobDetail>(jobId, "POST", JobDetailSchema),
  };

  readonly domain = {
    nodes: () => this.request<MeshNodeList>("/nodes", MeshNodeListSchema),
    messages: () =>
      this.request<MeshMessageList>("/messages", MeshMessageListSchema),
    telemetry: () =>
      this.request<MeshTelemetryList>("/telemetry", MeshTelemetryListSchema),
    positions: () =>
      this.request<PositionCanonicalEventList>(
        "/positions",
        PositionCanonicalEventListSchema,
      ),
  };

  readonly aprs = {
    outbox: () =>
      this.request<AprsOutboxEntryList>(
        "/aprs/outbox",
        AprsOutboxEntryListSchema,
      ),
  };

  readonly callmesh = {
    overview: () =>
      this.request<CallMeshOverview>("/callmesh", CallMeshOverviewSchema),
  };

  private async request<T>(
    path: string,
    schema: TSchema,
    method: "GET" | "POST" = "GET",
  ): Promise<T> {
    const traceId = this.traceIdFactory();
    let response: Response;

    try {
      response = await this.fetchImplementation(`${this.baseUrl}${path}`, {
        method,
        headers: {
          accept: "application/json",
          "x-trace-id": traceId,
        },
      });
    } catch (error) {
      throw createNetworkError(error);
    }

    if (!response.ok) {
      throw await mapGatewayResponseError(response);
    }

    const payload = await readJson(response);
    if (!checkSchema(schema, payload)) {
      throw new GatewayApiError({
        code: "GATEWAY_RESPONSE_INVALID",
        traceId: response.headers.get("x-trace-id") ?? traceId,
        status: response.status,
      });
    }

    return payload as T;
  }

  private requestJob<T>(
    jobId: string,
    method: "GET" | "POST",
    schema: TSchema,
  ) {
    if (!/^[a-zA-Z0-9._:-]{1,128}$/.test(jobId)) {
      return Promise.reject(
        new GatewayApiError({ code: "CLIENT_INPUT_INVALID" }),
      ) as Promise<T>;
    }

    const path = `/jobs/${encodeURIComponent(jobId)}${
      method === "POST" ? "/cancel" : ""
    }`;
    return this.request<T>(path, schema, method);
  }
}

export type GatewaySystemApi = {
  health: () => Promise<SystemHealth>;
  version: () => Promise<BuildMetadata>;
  capabilities: () => Promise<SystemCapabilities>;
  status: () => Promise<SystemStatus>;
};

async function readJson(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return undefined;
  }
}

function normalizeBaseUrl(baseUrl: string): string {
  const normalized = baseUrl.trim().replace(/\/+$/, "");
  if (!normalized || !normalized.startsWith("/")) {
    throw new TypeError("Gateway API base URL must be an absolute path");
  }
  return normalized;
}

function defaultTraceId(): string {
  return (
    globalThis.crypto?.randomUUID?.() ?? "00000000-0000-4000-8000-000000000000"
  );
}

function isRetryable(status: number, code?: string): boolean {
  return (
    status === 408 ||
    status === 429 ||
    status === 502 ||
    status === 503 ||
    status === 504 ||
    code === "GATEWAY_PROXY_UNAVAILABLE"
  );
}

function isLooseStableError(payload: unknown): payload is {
  code: string;
  params?: Record<string, string | number | boolean | null>;
} {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return false;
  }
  const error = payload as Record<string, unknown>;
  if (
    typeof error.code !== "string" ||
    !/^[A-Z0-9_]{1,128}$/.test(error.code)
  ) {
    return false;
  }
  if (error.params === undefined) {
    return true;
  }
  if (
    !error.params ||
    typeof error.params !== "object" ||
    Array.isArray(error.params)
  ) {
    return false;
  }
  return Object.values(error.params).every(
    (value) =>
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "boolean" ||
      value === null,
  );
}

function checkSchema(schema: TSchema, value: unknown): boolean {
  return Value.Check(schema, value);
}
