import type { DatabaseSync } from "node:sqlite";

import { Value } from "@sinclair/typebox/value";
import {
  CallMeshMappingSchema,
  type CallMeshMapping,
  type CallMeshOverview,
  type CallMeshStatus,
} from "@cmclient/contracts";

export interface CallMeshMappingStore {
  replace(mappings: CallMeshMapping[]): void;
}

export interface CallMeshClientOptions {
  baseUrl: string;
  apiKey?: string;
  fetch?: typeof fetch;
  heartbeatPath?: string;
  mappingsPath?: string;
  provisionPath?: string;
  timeoutMs?: number;
  maximumRetries?: number;
  initialRetryDelayMs?: number;
  maximumRetryDelayMs?: number;
  clock?: () => Date;
  sleep?: (delayMs: number) => Promise<void>;
}

export class CallMeshClientError extends Error {
  constructor(
    readonly code: string,
    readonly retryable = false,
  ) {
    super(code);
    this.name = "CallMeshClientError";
  }
}

export class CallMeshMappingRepository implements CallMeshMappingStore {
  constructor(private readonly database: DatabaseSync) {}

  replace(mappings: CallMeshMapping[]): void {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      this.database.exec("DELETE FROM callmesh_mappings");
      const insert = this.database.prepare(
        "INSERT INTO callmesh_mappings (version, effective_at, mesh_network_id, node_num, callsign) VALUES (?, ?, ?, ?, ?)",
      );
      for (const mapping of mappings) {
        insert.run(
          mapping.version,
          mapping.effectiveAt,
          mapping.meshNetworkId,
          mapping.nodeNum,
          mapping.callsign,
        );
      }
      this.database.exec("COMMIT");
    } catch {
      this.database.exec("ROLLBACK");
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
  }

  list(): CallMeshMapping[] {
    return this.database
      .prepare(
        "SELECT version, effective_at, mesh_network_id, node_num, callsign FROM callmesh_mappings ORDER BY effective_at DESC, version ASC, mesh_network_id ASC, node_num ASC",
      )
      .all()
      .map((row) => ({
        version: String(row.version),
        effectiveAt: String(row.effective_at),
        meshNetworkId: String(row.mesh_network_id),
        nodeNum: Number(row.node_num),
        callsign: String(row.callsign),
      }));
  }
}

export class CallMeshClient {
  private readonly apiKey: string | undefined;
  private readonly fetchImplementation: typeof fetch;
  private readonly clock: () => Date;
  private readonly sleep: (delayMs: number) => Promise<void>;
  private readonly heartbeatPath: string;
  private readonly mappingsPath: string;
  private readonly provisionPath: string;
  private readonly timeoutMs: number;
  private readonly maximumRetries: number;
  private readonly initialRetryDelayMs: number;
  private readonly maximumRetryDelayMs: number;
  private readonly baseUrl: URL;
  private status: CallMeshStatus;
  private mappings: CallMeshMapping[] = [];
  private synchronizing: Promise<void> | undefined;

  constructor(
    options: CallMeshClientOptions,
    private readonly mappingStore?: CallMeshMappingStore,
  ) {
    this.baseUrl = parseBaseUrl(options.baseUrl);
    this.apiKey = options.apiKey?.trim() || undefined;
    this.fetchImplementation = options.fetch ?? globalThis.fetch;
    this.clock = options.clock ?? (() => new Date());
    this.sleep = options.sleep ?? ((delayMs) => delay(delayMs));
    this.heartbeatPath = parsePath(options.heartbeatPath ?? "/v1/heartbeat");
    this.mappingsPath = parsePath(options.mappingsPath ?? "/v1/mappings");
    this.provisionPath = parsePath(options.provisionPath ?? "/v1/provision");
    this.timeoutMs = positiveInteger(options.timeoutMs ?? 5_000);
    this.maximumRetries = nonNegativeInteger(options.maximumRetries ?? 2);
    this.initialRetryDelayMs = positiveInteger(
      options.initialRetryDelayMs ?? 250,
    );
    this.maximumRetryDelayMs = positiveInteger(
      options.maximumRetryDelayMs ?? 2_000,
    );
    if (this.maximumRetryDelayMs < this.initialRetryDelayMs) {
      throw new CallMeshClientError("CALLMESH_CONFIGURATION_INVALID");
    }
    this.status = this.apiKey
      ? this.makeStatus("checking")
      : this.makeStatus("unavailable", "CALLMESH_NOT_CONFIGURED");
  }

  getOverview(): CallMeshOverview {
    return {
      status: { ...this.status },
      mappings: this.mappings.map((mapping) => ({ ...mapping })),
    };
  }

  async synchronize(): Promise<CallMeshOverview> {
    if (!this.apiKey) {
      return this.getOverview();
    }
    if (!this.synchronizing) {
      this.synchronizing = this.synchronizeInner().finally(() => {
        this.synchronizing = undefined;
      });
    }
    await this.synchronizing;
    return this.getOverview();
  }

  private async synchronizeInner(): Promise<void> {
    this.status = this.makeStatus("checking");
    try {
      await this.request(this.heartbeatPath, "POST");
      await this.request(this.provisionPath, "POST");
      const payload = await this.request(this.mappingsPath, "GET");
      const mappings = parseMappings(payload);
      this.mappingStore?.replace(mappings);
      this.mappings = mappings;
      this.status = this.makeStatus("ready", undefined, mappings);
    } catch (error) {
      this.mappings = [];
      let code =
        error instanceof CallMeshClientError
          ? error.code
          : "CALLMESH_NETWORK_UNAVAILABLE";
      if (mustClearMappings(code)) {
        try {
          this.mappingStore?.replace([]);
        } catch {
          code = "CALLMESH_MAPPING_STORE_FAILED";
        }
      }
      this.status = this.makeStatus(
        code === "CALLMESH_AUTH_INVALID" ? "unavailable" : "degraded",
        code,
      );
    }
  }

  private async request(
    path: string,
    method: "GET" | "POST",
  ): Promise<unknown> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        const response = await this.fetchWithTimeout(path, method);
        if (!response.ok) {
          throw responseError(response.status);
        }
        if (response.status === 204) {
          return undefined;
        }
        try {
          return await response.json();
        } catch {
          throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
        }
      } catch (error) {
        const classified = classifyError(error);
        if (!classified.retryable || attempt >= this.maximumRetries) {
          throw classified;
        }
        await this.sleep(
          Math.min(
            this.initialRetryDelayMs * 2 ** attempt,
            this.maximumRetryDelayMs,
          ),
        );
      }
    }
  }

  private async fetchWithTimeout(
    path: string,
    method: "GET" | "POST",
  ): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      return await this.fetchImplementation(new URL(path, this.baseUrl), {
        method,
        headers: {
          accept: "application/json",
          authorization: `Bearer ${this.apiKey}`,
          "content-type": "application/json",
        },
        ...(method === "POST"
          ? { body: JSON.stringify({ client: "cmclient-gateway" }) }
          : {}),
        signal: controller.signal,
      });
    } catch {
      throw new CallMeshClientError("CALLMESH_NETWORK_UNAVAILABLE", true);
    } finally {
      clearTimeout(timeout);
    }
  }

  private makeStatus(
    state: CallMeshStatus["state"],
    reasonCode?: string,
    mappings: CallMeshMapping[] = [],
  ): CallMeshStatus {
    const versions = new Set(mappings.map((mapping) => mapping.version));
    return {
      state,
      updatedAt: this.clock().toISOString(),
      ...(reasonCode ? { reasonCode } : {}),
      ...(versions.size === 1
        ? { activeMappingVersion: [...versions][0] }
        : {}),
      activeMappingCount: mappings.length,
    };
  }
}

function parseMappings(payload: unknown): CallMeshMapping[] {
  if (
    !payload ||
    typeof payload !== "object" ||
    Array.isArray(payload) ||
    !Array.isArray((payload as { mappings?: unknown }).mappings)
  ) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  const mappings = (payload as { mappings: unknown[] }).mappings;
  if (
    mappings.length > 200 ||
    !mappings.every((mapping) => Value.Check(CallMeshMappingSchema, mapping))
  ) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }

  const seen = new Map<string, string>();
  const unique = new Map<string, CallMeshMapping>();
  for (const mapping of mappings as CallMeshMapping[]) {
    const target = [
      mapping.meshNetworkId,
      mapping.nodeNum,
      mapping.effectiveAt,
    ].join("\u0000");
    const previousCallsign = seen.get(target);
    if (previousCallsign && previousCallsign !== mapping.callsign) {
      throw new CallMeshClientError("CALLMESH_MAPPING_CONFLICT");
    }
    seen.set(target, mapping.callsign);
    unique.set(
      [
        mapping.version,
        mapping.effectiveAt,
        mapping.meshNetworkId,
        mapping.nodeNum,
        mapping.callsign,
      ].join("\u0000"),
      mapping,
    );
  }
  return [...unique.values()].sort(compareMappings);
}

function compareMappings(
  left: CallMeshMapping,
  right: CallMeshMapping,
): number {
  return (
    right.effectiveAt.localeCompare(left.effectiveAt) ||
    left.version.localeCompare(right.version) ||
    left.meshNetworkId.localeCompare(right.meshNetworkId) ||
    left.nodeNum - right.nodeNum ||
    left.callsign.localeCompare(right.callsign)
  );
}

function classifyError(error: unknown): CallMeshClientError {
  if (error instanceof CallMeshClientError) {
    return error;
  }
  return new CallMeshClientError("CALLMESH_NETWORK_UNAVAILABLE", true);
}

function responseError(status: number): CallMeshClientError {
  if (status === 401 || status === 403) {
    return new CallMeshClientError("CALLMESH_AUTH_INVALID");
  }
  return new CallMeshClientError(
    `CALLMESH_HTTP_${status}`,
    status === 408 || status === 429 || status >= 500,
  );
}

function parseBaseUrl(value: string): URL {
  try {
    const url = new URL(value);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new Error("unsupported protocol");
    }
    return url;
  } catch {
    throw new CallMeshClientError("CALLMESH_CONFIGURATION_INVALID");
  }
}

function parsePath(value: string): string {
  if (!value.startsWith("/") || value.startsWith("//")) {
    throw new CallMeshClientError("CALLMESH_CONFIGURATION_INVALID");
  }
  return value;
}

function positiveInteger(value: number): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new CallMeshClientError("CALLMESH_CONFIGURATION_INVALID");
  }
  return value;
}

function nonNegativeInteger(value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new CallMeshClientError("CALLMESH_CONFIGURATION_INVALID");
  }
  return value;
}

function delay(delayMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

function mustClearMappings(code: string): boolean {
  return (
    code === "CALLMESH_AUTH_INVALID" ||
    code === "CALLMESH_MAPPING_CONFLICT" ||
    code === "CALLMESH_SCHEMA_INVALID"
  );
}
