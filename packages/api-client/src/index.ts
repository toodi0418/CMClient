import {
  ApiErrorSchema,
  AprsIgateSubmissionListSchema,
  AprsOutboxEntryListSchema,
  AprsRuntimeStatusSchema,
  CMCloudEnrollmentRequestSchema,
  CMCloudEnrollmentStatusSchema,
  CMCloudAccountProjectionSchema,
  ComponentIdentityReportSchema,
  CallMeshOverviewSchema,
  JobAcceptedSchema,
  JobDetailSchema,
  MeshMessageListSchema,
  MeshNodeListSchema,
  MeshTelemetryListSchema,
  MeshtasticRuntimeStatusSchema,
  PositionCanonicalEventListSchema,
  ProxyStatusSchema,
  AgentLifecycleStatusSchema,
  SetupAcceptTermsRequestSchema,
  SetupConfigureRequestSchema,
  SetupDiscoveryResponseSchema,
  SetupResetRequestSchema,
  SetupStatusSchema,
  SystemCapabilitiesSchema,
  SystemHealthSchema,
  SystemStatusSchema,
  type ComponentIdentityReport,
  type CallMeshOverview,
  type ApiError,
  type AprsIgateSubmissionList,
  type AprsOutboxEntryList,
  type AprsRuntimeStatus,
  type CMCloudEnrollmentRequest,
  type CMCloudEnrollmentStatus,
  type CMCloudAccountProjection,
  type JobAccepted,
  type JobDetail,
  type MeshMessageList,
  type MeshNodeList,
  type MeshTelemetryList,
  type MeshtasticRuntimeStatus,
  type PositionCanonicalEventList,
  type ProxyStatus,
  type AgentLifecycleStatus,
  type SetupAcceptTermsRequest,
  type SetupConfigureRequest,
  type SetupDiscoveryResponse,
  type SetupResetRequest,
  type SetupStatus,
  type SystemCapabilities,
  type SystemHealth,
  type SystemStatus,
} from "@cmclient/contracts";
import type { TSchema } from "@sinclair/typebox";
import { FormatRegistry } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

FormatRegistry.Set("uuid", (value) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    value,
  ),
);
FormatRegistry.Set("date-time", (value) => {
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})$/.exec(
      value,
    );
  if (!match) {
    return false;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const timezone = match[7]!;
  const daysInMonth = [
    31,
    year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0) ? 29 : 28,
    31,
    30,
    31,
    30,
    31,
    31,
    30,
    31,
    30,
    31,
  ][month - 1];
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > (daysInMonth ?? 0) ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    return false;
  }
  if (timezone === "Z") {
    return true;
  }
  const offsetHours = Number(timezone.slice(1, 3));
  const offsetMinutes = Number(timezone.slice(4, 6));
  return offsetHours <= 23 && offsetMinutes <= 59;
});

export const DEFAULT_GATEWAY_API_BASE_URL = "/api/v1";

let managementCsrfToken: string | undefined;

/**
 * Keeps the LAN management CSRF token in process memory only. The management
 * shell calls this after login; no browser persistence is used for the token.
 */
export function setManagementCsrfToken(token: string | undefined): void {
  managementCsrfToken =
    token && /^[a-f0-9]{32}$/i.test(token) ? token : undefined;
}

export interface GatewayApiClientOptions {
  baseUrl?: string;
  fetch?: typeof fetch;
  traceIdFactory?: () => string;
}

export interface TelemetryRangeQuery {
  limit?: number;
  meshNetworkId?: string;
  nodeNum?: number;
  metricKind?: string;
  from?: string;
  to?: string;
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
    this.fetchImplementation =
      options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.traceIdFactory = options.traceIdFactory ?? defaultTraceId;
  }

  readonly system = {
    health: () =>
      this.request<SystemHealth>("/system/health", SystemHealthSchema),
    version: () =>
      this.request<ComponentIdentityReport>(
        "/system/version",
        ComponentIdentityReportSchema,
      ),
    capabilities: () =>
      this.request<SystemCapabilities>(
        "/system/capabilities",
        SystemCapabilitiesSchema,
      ),
    status: () =>
      this.request<SystemStatus>("/system/status", SystemStatusSchema),
  };

  readonly setup = {
    status: () => this.request<SetupStatus>("/setup/status", SetupStatusSchema),
    discovery: () =>
      this.request<SetupDiscoveryResponse>(
        "/setup/discovery",
        SetupDiscoveryResponseSchema,
      ),
    configure: (request: SetupConfigureRequest) =>
      this.requestBody<SetupStatus, SetupConfigureRequest>(
        "/setup/configure",
        SetupStatusSchema,
        SetupConfigureRequestSchema,
        request,
      ),
    acceptTerms: (termsVersion: string) =>
      this.requestBody<SetupStatus, SetupAcceptTermsRequest>(
        "/setup/terms",
        SetupStatusSchema,
        SetupAcceptTermsRequestSchema,
        { termsVersion },
      ),
    reset: (confirmation: SetupResetRequest["confirmation"]) =>
      this.requestBody<SetupStatus, SetupResetRequest>(
        "/setup/reset",
        SetupStatusSchema,
        SetupResetRequestSchema,
        { confirmation },
      ),
    operationalReset: (confirmation: SetupResetRequest["confirmation"]) =>
      this.requestBody<SetupStatus, SetupResetRequest>(
        "/reset/operational",
        SetupStatusSchema,
        SetupResetRequestSchema,
        { confirmation },
      ),
  };

  readonly lifecycle = {
    status: () =>
      this.request<AgentLifecycleStatus>(
        "/lifecycle/status",
        AgentLifecycleStatusSchema,
      ),
  };

  readonly jobs = {
    get: (jobId: string) =>
      this.requestJob<JobDetail>(jobId, "GET", JobDetailSchema),
    cancel: (jobId: string) =>
      this.requestJob<JobDetail>(jobId, "POST", JobDetailSchema),
  };

  readonly diagnostics = {
    integrityCheck: () =>
      this.request<JobAccepted>(
        "/diagnostics/integrity-check",
        JobAcceptedSchema,
        "POST",
        { "idempotency-key": this.traceIdFactory() },
      ),
  };

  readonly domain = {
    nodes: () => this.request<MeshNodeList>("/nodes", MeshNodeListSchema),
    messages: () =>
      this.request<MeshMessageList>("/messages", MeshMessageListSchema),
    telemetry: (query: TelemetryRangeQuery = {}) =>
      this.request<MeshTelemetryList>(
        telemetryPath(query),
        MeshTelemetryListSchema,
      ),
    positions: () =>
      this.request<PositionCanonicalEventList>(
        "/positions",
        PositionCanonicalEventListSchema,
      ),
  };

  readonly aprs = {
    status: () =>
      this.request<AprsRuntimeStatus>("/aprs", AprsRuntimeStatusSchema),
    outbox: () =>
      this.request<AprsOutboxEntryList>(
        "/aprs/outbox",
        AprsOutboxEntryListSchema,
      ),
    stationSubmissions: () =>
      this.request<AprsIgateSubmissionList>(
        "/aprs/station-submissions",
        AprsIgateSubmissionListSchema,
      ),
  };

  readonly meshtastic = {
    status: () =>
      this.request<MeshtasticRuntimeStatus>(
        "/meshtastic",
        MeshtasticRuntimeStatusSchema,
      ),
  };

  readonly callmesh = {
    overview: () =>
      this.request<CallMeshOverview>("/callmesh", CallMeshOverviewSchema),
  };

  readonly cmcloud = {
    status: () =>
      this.request<CMCloudEnrollmentStatus>(
        "/cmcloud/enrollment",
        CMCloudEnrollmentStatusSchema,
      ),
    enroll: (request: CMCloudEnrollmentRequest) =>
      this.requestBody<CMCloudEnrollmentStatus, CMCloudEnrollmentRequest>(
        "/cmcloud/enrollment",
        CMCloudEnrollmentStatusSchema,
        CMCloudEnrollmentRequestSchema,
        request,
      ),
    accountProjection: () =>
      this.request<CMCloudAccountProjection>(
        "/cmcloud/account-projection",
        CMCloudAccountProjectionSchema,
      ),
  };

  readonly proxy = {
    status: () => this.request<ProxyStatus>("/proxy", ProxyStatusSchema),
  };

  private async request<T>(
    path: string,
    schema: TSchema,
    method: "GET" | "POST" = "GET",
    additionalHeaders: Record<string, string> = {},
    body: unknown = {},
  ): Promise<T> {
    const traceId = this.traceIdFactory();
    let response: Response;

    try {
      response = await this.fetchImplementation(`${this.baseUrl}${path}`, {
        method,
        headers: {
          accept: "application/json",
          "x-trace-id": traceId,
          ...(method === "POST" ? { "content-type": "application/json" } : {}),
          ...(managementCsrfToken
            ? { "x-csrf-token": managementCsrfToken }
            : {}),
          ...additionalHeaders,
        },
        ...(method === "POST" ? { body: JSON.stringify(body) } : {}),
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

  private requestBody<TResponse, TRequest>(
    path: string,
    responseSchema: TSchema,
    requestSchema: TSchema,
    body: TRequest,
  ): Promise<TResponse> {
    if (!checkSchema(requestSchema, body)) {
      return Promise.reject(
        new GatewayApiError({ code: "CLIENT_INPUT_INVALID" }),
      );
    }
    return this.request<TResponse>(path, responseSchema, "POST", {}, body);
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

function telemetryPath(query: TelemetryRangeQuery): string {
  const fromTime =
    query.from === undefined ? undefined : Date.parse(query.from);
  const toTime = query.to === undefined ? undefined : Date.parse(query.to);
  if (
    (query.limit !== undefined &&
      (!Number.isInteger(query.limit) ||
        query.limit < 1 ||
        query.limit > 200)) ||
    (query.nodeNum !== undefined &&
      (query.meshNetworkId === undefined ||
        !Number.isInteger(query.nodeNum) ||
        query.nodeNum < 0 ||
        query.nodeNum > 4_294_967_295)) ||
    (fromTime !== undefined && !Number.isFinite(fromTime)) ||
    (toTime !== undefined && !Number.isFinite(toTime)) ||
    (fromTime !== undefined && toTime !== undefined && fromTime > toTime)
  ) {
    throw new GatewayApiError({ code: "CLIENT_INPUT_INVALID" });
  }
  const parameters = new URLSearchParams();
  for (const [name, value] of Object.entries(query)) {
    if (value !== undefined) {
      const normalized =
        name === "from" || name === "to"
          ? new Date(String(value)).toISOString()
          : String(value);
      parameters.set(name, normalized);
    }
  }
  const encoded = parameters.toString();
  return encoded ? `/telemetry?${encoded}` : "/telemetry";
}

export type GatewaySystemApi = {
  health: () => Promise<SystemHealth>;
  version: () => Promise<ComponentIdentityReport>;
  capabilities: () => Promise<SystemCapabilities>;
  status: () => Promise<SystemStatus>;
};

export type AgentSetupApi = {
  status: () => Promise<SetupStatus>;
  discovery: () => Promise<SetupDiscoveryResponse>;
  configure: (request: SetupConfigureRequest) => Promise<SetupStatus>;
  acceptTerms: (termsVersion: string) => Promise<SetupStatus>;
  reset: (
    confirmation: SetupResetRequest["confirmation"],
  ) => Promise<SetupStatus>;
  operationalReset: (
    confirmation: SetupResetRequest["confirmation"],
  ) => Promise<SetupStatus>;
};

export type AgentLifecycleApi = {
  status: () => Promise<AgentLifecycleStatus>;
};

export type GatewayProxyApi = {
  status: () => Promise<ProxyStatus>;
};

export type AgentCmCloudApi = {
  status: () => Promise<CMCloudEnrollmentStatus>;
  enroll: (
    request: CMCloudEnrollmentRequest,
  ) => Promise<CMCloudEnrollmentStatus>;
  accountProjection: () => Promise<CMCloudAccountProjection>;
};

export type GatewayMeshtasticApi = {
  status: () => Promise<MeshtasticRuntimeStatus>;
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
