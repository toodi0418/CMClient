import { createHash } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";

import { Value } from "@sinclair/typebox/value";
import {
  CallMeshMappingSchema,
  CallMeshProvisionSchema,
  type CallMeshMapping,
  type CallMeshOverview,
  type CallMeshProvision,
  type CallMeshStatus,
} from "@cmclient/contracts";

const DEFAULT_BASE_URL = "https://callmesh.tmmarc.org";
const HEARTBEAT_PATH = "/api/v1/client/heartbeat";
const MAPPINGS_PATH = "/api/v1/client/mappings";
const PROVISION_LEASE_MS = 3 * 60_000;
const MAX_MAPPING_ITEMS = 200;
const MAX_RESPONSE_BYTES = 512 * 1024;
const FINGERPRINT_PATTERN = /^[a-f0-9]{64}$/;

export interface CallMeshSyncSnapshot {
  active: boolean;
  mappingHash: string;
  acceptedServerTime: string;
  mappingsFingerprint: string;
  lastHeartbeatAt: string;
  mappingSyncedAt: string;
  provision?: CallMeshProvision;
  provisionExpiresAt?: string;
  provisionFingerprint?: string;
  mappings: CallMeshMapping[];
}

/**
 * The only state shape that production APRS/Mesh wiring is allowed to consume.
 * Mappings and the lease are returned from one synchronous read so a refresh
 * cannot pair a mapping from one lease with credentials from another lease.
 */
export interface CallMeshAprsState {
  readonly mappings: CallMeshMapping[];
  readonly mappingsFingerprint: string;
  readonly provision: CallMeshProvision;
  readonly provisionFingerprint: string;
}

export interface CallMeshHistoryHighWater {
  mappingHash: string;
  lastServerTime: string;
  mappingsFingerprint: string;
}

export interface CallMeshSnapshotStore {
  list(): CallMeshMapping[];
  loadSnapshot(): CallMeshSyncSnapshot | undefined;
  applySnapshot(snapshot: CallMeshSyncSnapshot): void;
  deactivateSnapshot(updatedAt: string): void;
  loadHistoryHighWater(): CallMeshHistoryHighWater | undefined;
  hasHistoricalHash(hash: string): boolean;
}

export interface CallMeshClientOptions {
  baseUrl?: string;
  apiKey?: string;
  agent?: string;
  meshNetworkId?: string;
  fetch?: typeof fetch;
  timeoutMs?: number;
  maximumRetries?: number;
  initialRetryDelayMs?: number;
  maximumRetryDelayMs?: number;
  clock?: () => Date;
  sleep?: (delayMs: number) => Promise<void>;
}

export function callMeshOptionsFromRuntime(
  environment: NodeJS.ProcessEnv,
  version: string,
  callMeshApiKey?: string,
): CallMeshClientOptions {
  const baseUrl = environment.CMCLIENT_CALLMESH_URL?.trim();
  return {
    ...(baseUrl ? { baseUrl } : {}),
    ...(callMeshApiKey !== undefined ? { apiKey: callMeshApiKey } : {}),
    agent: buildCallMeshAgent(version),
    meshNetworkId: environment.CMCLIENT_MESH_NETWORK_ID?.trim() || "default",
  };
}

export function buildCallMeshAgent(
  version: string,
  platform = process.platform,
  architecture = process.arch,
): string {
  const normalizedVersion = boundedText(
    version,
    64,
    "CALLMESH_CONFIGURATION_INVALID",
  );
  const normalizedPlatform = boundedText(
    platform,
    64,
    "CALLMESH_CONFIGURATION_INVALID",
  );
  const normalizedArchitecture = boundedText(
    architecture,
    32,
    "CALLMESH_CONFIGURATION_INVALID",
  );
  const platformLabel =
    normalizedPlatform === "darwin"
      ? "macOS"
      : normalizedPlatform === "win32"
        ? "Windows NT"
        : normalizedPlatform === "linux"
          ? "Linux"
          : normalizedPlatform;
  return `callmesh-client/${normalizedVersion} (${platformLabel}; ${normalizedArchitecture})`;
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

export class CallMeshMappingRepository implements CallMeshSnapshotStore {
  constructor(private readonly database: DatabaseSync) {}

  replace(mappings: CallMeshMapping[]): void {
    validateMappings(mappings);
    let transactionStarted = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionStarted = true;
      this.replaceMappings(mappings);
      this.database.exec("COMMIT");
      transactionStarted = false;
    } catch {
      if (transactionStarted) {
        rollbackQuietly(this.database);
      }
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
  }

  list(): CallMeshMapping[] {
    return this.database
      .prepare(
        "SELECT version, effective_at, mesh_network_id, node_num, callsign, symbol_table_present, symbol_table, symbol_code_present, symbol_code, symbol_overlay_present, symbol_overlay, comment_present, comment, altitude_meters_present, altitude_meters FROM callmesh_mappings ORDER BY effective_at DESC, version ASC, mesh_network_id ASC, node_num ASC",
      )
      .all()
      .map((row) => ({
        version: String(row.version),
        effectiveAt: String(row.effective_at),
        meshNetworkId: String(row.mesh_network_id),
        nodeNum: Number(row.node_num),
        callsign: String(row.callsign),
        ...mappingMetadataFromRow(row),
      }));
  }

  loadSnapshot(): CallMeshSyncSnapshot | undefined {
    try {
      const row = this.database
        .prepare(
          "SELECT active, mapping_hash, accepted_server_time, mappings_fingerprint, last_heartbeat_at, mapping_synced_at, provision_json, provision_expires_at, provision_fingerprint FROM callmesh_sync_state WHERE id = 1",
        )
        .get();
      if (!row) {
        return undefined;
      }
      const provision = row.provision_json
        ? (JSON.parse(String(row.provision_json)) as unknown)
        : undefined;
      const snapshot: CallMeshSyncSnapshot = {
        active: Number(row.active) === 1,
        mappingHash: String(row.mapping_hash),
        acceptedServerTime: String(row.accepted_server_time),
        mappingsFingerprint: String(row.mappings_fingerprint),
        lastHeartbeatAt: String(row.last_heartbeat_at),
        mappingSyncedAt: String(row.mapping_synced_at),
        ...(provision ? { provision: provision as CallMeshProvision } : {}),
        ...(row.provision_expires_at
          ? { provisionExpiresAt: String(row.provision_expires_at) }
          : {}),
        ...(row.provision_fingerprint
          ? { provisionFingerprint: String(row.provision_fingerprint) }
          : {}),
        mappings: this.list(),
      };
      validateSnapshot(snapshot);
      return snapshot;
    } catch {
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
  }

  applySnapshot(snapshot: CallMeshSyncSnapshot): void {
    validateSnapshot(snapshot);
    let transactionStarted = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionStarted = true;
      this.replaceMappings(snapshot.mappings);
      this.database
        .prepare(
          "INSERT INTO callmesh_sync_state (id, active, mapping_hash, accepted_server_time, mappings_fingerprint, last_heartbeat_at, mapping_synced_at, provision_json, provision_expires_at, provision_fingerprint, updated_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET active = excluded.active, mapping_hash = excluded.mapping_hash, accepted_server_time = excluded.accepted_server_time, mappings_fingerprint = excluded.mappings_fingerprint, last_heartbeat_at = excluded.last_heartbeat_at, mapping_synced_at = excluded.mapping_synced_at, provision_json = excluded.provision_json, provision_expires_at = excluded.provision_expires_at, provision_fingerprint = excluded.provision_fingerprint, updated_at = excluded.updated_at",
        )
        .run(
          snapshot.active ? 1 : 0,
          snapshot.mappingHash,
          snapshot.acceptedServerTime,
          snapshot.mappingsFingerprint,
          snapshot.lastHeartbeatAt,
          snapshot.mappingSyncedAt,
          snapshot.provision ? JSON.stringify(snapshot.provision) : null,
          snapshot.provisionExpiresAt ?? null,
          snapshot.provisionFingerprint ?? null,
          snapshot.lastHeartbeatAt,
        );
      const historyResult = this.database
        .prepare(
          "INSERT INTO callmesh_sync_history (mapping_hash, first_server_time, last_server_time, mappings_fingerprint) VALUES (?, ?, ?, ?) ON CONFLICT(mapping_hash) DO UPDATE SET last_server_time = CASE WHEN excluded.last_server_time > callmesh_sync_history.last_server_time THEN excluded.last_server_time ELSE callmesh_sync_history.last_server_time END WHERE callmesh_sync_history.mappings_fingerprint = excluded.mappings_fingerprint",
        )
        .run(
          snapshot.mappingHash,
          snapshot.acceptedServerTime,
          snapshot.acceptedServerTime,
          snapshot.mappingsFingerprint,
        );
      if (historyResult.changes !== 1) {
        throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
      }
      this.database.exec("COMMIT");
      transactionStarted = false;
    } catch {
      if (transactionStarted) {
        rollbackQuietly(this.database);
      }
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
  }

  deactivateSnapshot(updatedAt: string): void {
    normalizeTimestamp(updatedAt);
    let transactionStarted = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionStarted = true;
      this.database
        .prepare(
          "UPDATE callmesh_sync_state SET active = 0, provision_json = NULL, provision_expires_at = NULL, provision_fingerprint = NULL, updated_at = ? WHERE id = 1",
        )
        .run(updatedAt);
      this.database.exec("COMMIT");
      transactionStarted = false;
    } catch {
      if (transactionStarted) {
        rollbackQuietly(this.database);
      }
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
  }

  loadHistoryHighWater(): CallMeshHistoryHighWater | undefined {
    try {
      const row = this.database
        .prepare(
          "SELECT mapping_hash, last_server_time, mappings_fingerprint FROM callmesh_sync_history ORDER BY last_server_time DESC, mapping_hash ASC LIMIT 1",
        )
        .get();
      if (!row) {
        return undefined;
      }
      const highWater: CallMeshHistoryHighWater = {
        mappingHash: parseHash(row.mapping_hash),
        lastServerTime: normalizeTimestamp(row.last_server_time),
        mappingsFingerprint: String(row.mappings_fingerprint),
      };
      if (!FINGERPRINT_PATTERN.test(highWater.mappingsFingerprint)) {
        throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
      }
      return highWater;
    } catch {
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
  }

  hasHistoricalHash(hash: string): boolean {
    try {
      return Boolean(
        this.database
          .prepare(
            "SELECT 1 AS present FROM callmesh_sync_history WHERE mapping_hash = ?",
          )
          .get(hash),
      );
    } catch {
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
  }

  private replaceMappings(mappings: CallMeshMapping[]): void {
    this.database.exec("DELETE FROM callmesh_mappings");
    const insert = this.database.prepare(
      "INSERT INTO callmesh_mappings (version, effective_at, mesh_network_id, node_num, callsign, symbol_table_present, symbol_table, symbol_code_present, symbol_code, symbol_overlay_present, symbol_overlay, comment_present, comment, altitude_meters_present, altitude_meters) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    for (const mapping of mappings) {
      insert.run(
        mapping.version,
        mapping.effectiveAt,
        mapping.meshNetworkId,
        mapping.nodeNum,
        mapping.callsign,
        hasOwn(mapping, "symbolTable") ? 1 : 0,
        mapping.symbolTable ?? null,
        hasOwn(mapping, "symbolCode") ? 1 : 0,
        mapping.symbolCode ?? null,
        hasOwn(mapping, "symbolOverlay") ? 1 : 0,
        mapping.symbolOverlay ?? null,
        hasOwn(mapping, "comment") ? 1 : 0,
        mapping.comment ?? null,
        hasOwn(mapping, "altitudeMeters") ? 1 : 0,
        mapping.altitudeMeters ?? null,
      );
    }
  }
}

export class CallMeshClient {
  private readonly apiKey: string | undefined;
  private readonly fetchImplementation: typeof fetch;
  private readonly clock: () => Date;
  private readonly sleep: (delayMs: number) => Promise<void>;
  private readonly timeoutMs: number;
  private readonly maximumRetries: number;
  private readonly initialRetryDelayMs: number;
  private readonly maximumRetryDelayMs: number;
  private readonly baseUrl: URL;
  private readonly agent: string;
  private readonly meshNetworkId: string;
  private readonly historicalHashes = new Set<string>();
  private status: CallMeshStatus;
  private mappings: CallMeshMapping[] = [];
  private mappingHash: string | undefined;
  private acceptedServerTime: string | undefined;
  private lastHeartbeatAt: string | undefined;
  private mappingsFingerprint: string | undefined;
  private mappingSyncedAt: string | undefined;
  private provision: CallMeshProvision | undefined;
  private provisionExpiresAt: string | undefined;
  private provisionFingerprint: string | undefined;
  private activeStateEligible = false;
  private historyHighWater: CallMeshHistoryHighWater | undefined;
  private historyTrusted = true;
  private synchronizing: Promise<void> | undefined;

  constructor(
    options: CallMeshClientOptions,
    private readonly snapshotStore?: CallMeshSnapshotStore,
  ) {
    this.baseUrl = parseBaseUrl(options.baseUrl ?? DEFAULT_BASE_URL);
    this.apiKey = validateApiKey(options.apiKey);
    this.fetchImplementation = options.fetch ?? globalThis.fetch;
    this.clock = options.clock ?? (() => new Date());
    this.sleep = options.sleep ?? ((delayMs) => delay(delayMs));
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
    this.agent = boundedText(
      options.agent ??
        `cmclient-gateway/unknown (${process.platform}; ${process.arch})`,
      256,
      "CALLMESH_CONFIGURATION_INVALID",
    );
    this.meshNetworkId = boundedText(
      options.meshNetworkId?.trim() || "default",
      128,
      "CALLMESH_CONFIGURATION_INVALID",
    );
    let restored: CallMeshSyncSnapshot | undefined;
    let restoreFailed = false;
    try {
      restored = snapshotStore?.loadSnapshot();
    } catch {
      restoreFailed = true;
    }
    try {
      this.historyHighWater = snapshotStore?.loadHistoryHighWater();
    } catch {
      restoreFailed = true;
      this.historyTrusted = false;
    }
    if (
      !restored &&
      this.historyTrusted &&
      this.historyHighWater &&
      snapshotStore
    ) {
      try {
        const recoveryMappings = snapshotStore.list();
        const recoveryFingerprint = fingerprint(recoveryMappings);
        if (recoveryFingerprint === this.historyHighWater.mappingsFingerprint) {
          this.mappingHash = this.historyHighWater.mappingHash;
          this.mappingsFingerprint = recoveryFingerprint;
          this.mappingSyncedAt = this.historyHighWater.lastServerTime;
          this.mappings = recoveryMappings.map((mapping) => ({ ...mapping }));
          this.historicalHashes.add(this.historyHighWater.mappingHash);
        }
      } catch {
        restoreFailed = true;
        this.historyTrusted = false;
      }
    }
    if (restored && this.historyTrusted && !this.historyHighWater) {
      restoreFailed = true;
      this.historyTrusted = false;
    }
    if (restored && this.historyTrusted) {
      if (!snapshotMatchesHighWater(restored, this.historyHighWater)) {
        restoreFailed = true;
        this.historyTrusted = false;
      } else {
        this.restoreSnapshot(restored);
      }
    }
    if (restoreFailed) {
      this.activeStateEligible = false;
    }
    if (!this.apiKey) {
      this.activeStateEligible = false;
    }
    this.status = restoreFailed
      ? this.makeStatus("degraded", "CALLMESH_MAPPING_STORE_FAILED", "invalid")
      : this.apiKey
        ? this.makeStatus("checking")
        : this.makeStatus(
            "unavailable",
            "CALLMESH_NOT_CONFIGURED",
            "unavailable",
          );
  }

  getOverview(): CallMeshOverview {
    return {
      status: {
        ...this.status,
        provisionState:
          this.status.provisionState === "invalid" ||
          this.status.provisionState === "unavailable"
            ? this.status.provisionState
            : this.currentProvisionState(),
      },
      mappings: this.activeStateEligible
        ? this.mappings.map((mapping) => ({ ...mapping }))
        : [],
    };
  }

  getProvision(): CallMeshProvision | undefined {
    return this.activeStateEligible &&
      this.currentProvisionState() === "valid" &&
      this.provision
      ? { ...this.provision }
      : undefined;
  }

  getAprsState(): CallMeshAprsState | undefined {
    if (
      !this.activeStateEligible ||
      this.currentProvisionState() !== "valid" ||
      !this.provision ||
      !this.provisionFingerprint ||
      !this.mappingsFingerprint ||
      !FINGERPRINT_PATTERN.test(this.provisionFingerprint) ||
      !FINGERPRINT_PATTERN.test(this.mappingsFingerprint)
    ) {
      return undefined;
    }
    return {
      mappings: this.mappings.map((mapping) => ({ ...mapping })),
      mappingsFingerprint: this.mappingsFingerprint,
      provision: { ...this.provision },
      provisionFingerprint: this.provisionFingerprint,
    };
  }

  getMappingsForUse(): CallMeshMapping[] {
    return this.activeStateEligible && this.currentProvisionState() === "valid"
      ? this.mappings.map((mapping) => ({ ...mapping }))
      : [];
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
      if (!this.historyTrusted) {
        throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
      }
      const heartbeat = parseHeartbeat(
        await this.request(HEARTBEAT_PATH, {
          local_hash: this.mappingHash ?? null,
          agent: this.agent,
        }),
      );
      const provision = parseProvision(heartbeat.provision);
      const provisionFingerprint = provision
        ? fingerprint(provision)
        : undefined;
      this.validateMonotonicHeartbeat(heartbeat, provisionFingerprint);

      if (heartbeat.serverTime === this.acceptedServerTime) {
        if (!this.activeStateEligible) {
          throw new CallMeshClientError("CALLMESH_STALE_RESPONSE");
        }
        this.status = this.makeStatus("ready");
        return;
      }

      if (
        this.mappingHash &&
        heartbeat.hash !== this.mappingHash &&
        !heartbeat.needsUpdate
      ) {
        throw new CallMeshClientError("CALLMESH_RESPONSE_CONFLICT");
      }

      let mappings = this.mappings;
      let mappingsFingerprint = this.mappingsFingerprint;
      let mappingSyncedAt = this.mappingSyncedAt;
      const acceptedAt = this.clock().toISOString();
      const requiresMappings = heartbeat.needsUpdate || !this.mappingHash;
      if (requiresMappings) {
        const response = parseMappingsResponse(
          await this.request(MAPPINGS_PATH, {
            known_hash: this.mappingHash ?? null,
          }),
        );
        if (response.hash !== heartbeat.hash) {
          throw new CallMeshClientError("CALLMESH_RESPONSE_CONFLICT");
        }
        mappings = parseMappings(
          response.items,
          response.hash,
          this.meshNetworkId,
          heartbeat.serverTime,
        );
        mappingsFingerprint = fingerprint(mappings);
        if (
          !this.mappingsFingerprint &&
          this.historyHighWater?.mappingHash === response.hash &&
          mappingsFingerprint !== this.historyHighWater.mappingsFingerprint
        ) {
          throw new CallMeshClientError("CALLMESH_RESPONSE_CONFLICT");
        }
        if (
          response.hash === this.mappingHash &&
          this.mappingsFingerprint &&
          mappingsFingerprint !== this.mappingsFingerprint
        ) {
          throw new CallMeshClientError("CALLMESH_RESPONSE_CONFLICT");
        }
        mappingSyncedAt = acceptedAt;
      }
      if (!mappingsFingerprint || !mappingSyncedAt) {
        throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
      }

      const provisionExpiresAt = provision
        ? new Date(Date.parse(acceptedAt) + PROVISION_LEASE_MS).toISOString()
        : undefined;
      const snapshot: CallMeshSyncSnapshot = {
        active: true,
        mappingHash: heartbeat.hash,
        acceptedServerTime: heartbeat.serverTime,
        mappingsFingerprint,
        lastHeartbeatAt: acceptedAt,
        mappingSyncedAt,
        ...(provision ? { provision } : {}),
        ...(provisionExpiresAt ? { provisionExpiresAt } : {}),
        ...(provisionFingerprint ? { provisionFingerprint } : {}),
        mappings,
      };
      this.snapshotStore?.applySnapshot(snapshot);
      this.historicalHashes.add(snapshot.mappingHash);
      this.restoreSnapshot(snapshot);
      this.status = this.makeStatus("ready");
    } catch (error) {
      let classified = classifyError(error);
      const forcedProvisionState = fatalProvisionState(classified.code);
      if (forcedProvisionState) {
        this.activeStateEligible = false;
        this.provision = undefined;
        this.provisionExpiresAt = undefined;
        this.provisionFingerprint = undefined;
        try {
          this.snapshotStore?.deactivateSnapshot(this.clock().toISOString());
        } catch {
          classified = new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
        }
      }
      this.status = this.makeStatus(
        classified.code === "CALLMESH_AUTH_INVALID"
          ? "unavailable"
          : "degraded",
        classified.code,
        forcedProvisionState ?? fatalProvisionState(classified.code),
      );
    }
  }

  private validateMonotonicHeartbeat(
    heartbeat: ParsedHeartbeat,
    provisionFingerprint: string | undefined,
  ): void {
    if (!this.acceptedServerTime) {
      if (this.historyHighWater) {
        const ordering =
          Date.parse(heartbeat.serverTime) -
          Date.parse(this.historyHighWater.lastServerTime);
        if (ordering <= 0) {
          throw new CallMeshClientError("CALLMESH_STALE_RESPONSE");
        }
        if (
          heartbeat.hash !== this.historyHighWater.mappingHash &&
          (this.historicalHashes.has(heartbeat.hash) ||
            this.snapshotStore?.hasHistoricalHash(heartbeat.hash))
        ) {
          throw new CallMeshClientError("CALLMESH_STALE_RESPONSE");
        }
      }
      return;
    }
    const ordering =
      Date.parse(heartbeat.serverTime) - Date.parse(this.acceptedServerTime);
    if (ordering < 0) {
      throw new CallMeshClientError("CALLMESH_STALE_RESPONSE");
    }
    if (
      ordering === 0 &&
      (heartbeat.hash !== this.mappingHash ||
        provisionFingerprint !== this.provisionFingerprint)
    ) {
      throw new CallMeshClientError("CALLMESH_RESPONSE_CONFLICT");
    }
    if (
      heartbeat.hash !== this.mappingHash &&
      (this.historicalHashes.has(heartbeat.hash) ||
        this.snapshotStore?.hasHistoricalHash(heartbeat.hash))
    ) {
      throw new CallMeshClientError("CALLMESH_STALE_RESPONSE");
    }
  }

  private async request(
    path: string,
    body: Record<string, unknown>,
  ): Promise<unknown> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await this.requestOnce(path, body);
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

  private async requestOnce(
    path: string,
    body: Record<string, unknown>,
  ): Promise<unknown> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      let response: Response;
      try {
        response = await this.fetchImplementation(new URL(path, this.baseUrl), {
          method: "POST",
          headers: {
            accept: "application/json",
            "content-type": "application/json",
            "x-api-key": this.apiKey!,
            "x-client-agent": this.agent,
          },
          body: JSON.stringify(body),
          signal: controller.signal,
          redirect: "error",
        });
      } catch {
        throw new CallMeshClientError("CALLMESH_NETWORK_UNAVAILABLE", true);
      }
      if (!response.ok) {
        throw responseError(response.status);
      }
      const contentLength = Number(response.headers.get("content-length"));
      if (
        Number.isFinite(contentLength) &&
        contentLength > MAX_RESPONSE_BYTES
      ) {
        throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
      }
      const text = await readBoundedResponse(response);
      try {
        return JSON.parse(text) as unknown;
      } catch {
        throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
      }
    } finally {
      clearTimeout(timeout);
    }
  }

  private restoreSnapshot(snapshot: CallMeshSyncSnapshot): void {
    this.activeStateEligible = snapshot.active && Boolean(this.apiKey);
    this.mappingHash = snapshot.mappingHash;
    this.acceptedServerTime = snapshot.acceptedServerTime;
    this.lastHeartbeatAt = snapshot.lastHeartbeatAt;
    this.mappingsFingerprint = snapshot.mappingsFingerprint;
    this.mappingSyncedAt = snapshot.mappingSyncedAt;
    this.provision = snapshot.provision ? { ...snapshot.provision } : undefined;
    this.provisionExpiresAt = snapshot.provisionExpiresAt;
    this.provisionFingerprint = snapshot.provisionFingerprint;
    this.mappings = snapshot.mappings.map((mapping) => ({ ...mapping }));
    this.historicalHashes.add(snapshot.mappingHash);
    this.historyHighWater = {
      mappingHash: snapshot.mappingHash,
      lastServerTime: snapshot.acceptedServerTime,
      mappingsFingerprint: snapshot.mappingsFingerprint,
    };
  }

  private makeStatus(
    state: CallMeshStatus["state"],
    reasonCode?: string,
    forcedProvisionState?: CallMeshStatus["provisionState"],
  ): CallMeshStatus {
    const activeMappings = this.activeStateEligible ? this.mappings : [];
    const versions = new Set(activeMappings.map((mapping) => mapping.version));
    return {
      state,
      updatedAt: this.clock().toISOString(),
      ...(reasonCode ? { reasonCode } : {}),
      ...(versions.size === 1
        ? { activeMappingVersion: [...versions][0] }
        : {}),
      ...(this.activeStateEligible && this.mappingHash
        ? { activeMappingHash: this.mappingHash }
        : {}),
      activeMappingCount: activeMappings.length,
      provisionState: forcedProvisionState ?? this.currentProvisionState(),
      ...(this.acceptedServerTime
        ? { lastServerTime: this.acceptedServerTime }
        : {}),
    };
  }

  private currentProvisionState(): CallMeshStatus["provisionState"] {
    if (!this.provision) {
      return this.acceptedServerTime ? "revoked" : "unavailable";
    }
    const now = this.clock().getTime();
    const lastHeartbeatAt = this.lastHeartbeatAt
      ? Date.parse(this.lastHeartbeatAt)
      : undefined;
    if (
      !this.provisionExpiresAt ||
      Date.parse(this.provisionExpiresAt) <= now ||
      (lastHeartbeatAt !== undefined && now < lastHeartbeatAt)
    ) {
      return "expired";
    }
    return "valid";
  }
}

interface ParsedHeartbeat {
  hash: string;
  needsUpdate: boolean;
  provision: unknown;
  serverTime: string;
}

function parseHeartbeat(payload: unknown): ParsedHeartbeat {
  const record = strictRecord(payload, [
    "hash",
    "needs_update",
    "provision",
    "server_time",
  ]);
  if (typeof record.needs_update !== "boolean") {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return {
    hash: parseHash(record.hash),
    needsUpdate: record.needs_update,
    provision: record.provision,
    serverTime: normalizeTimestamp(record.server_time),
  };
}

function parseMappingsResponse(payload: unknown): {
  hash: string;
  items: unknown[];
} {
  const record = strictRecord(payload, ["hash", "items"]);
  if (!Array.isArray(record.items) || record.items.length > MAX_MAPPING_ITEMS) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return { hash: parseHash(record.hash), items: record.items };
}

function parseMappings(
  items: unknown[],
  mappingHash: string,
  meshNetworkId: string,
  batchEffectiveAt: string,
): CallMeshMapping[] {
  const mappings = items
    .map((item) =>
      parseMappingItem(item, mappingHash, meshNetworkId, batchEffectiveAt),
    )
    .filter((mapping): mapping is CallMeshMapping => mapping !== undefined);
  validateMappings(mappings);
  const seen = new Map<string, string>();
  const callsignOwners = new Map<string, string>();
  const unique = new Map<string, CallMeshMapping>();
  for (const mapping of mappings) {
    const owner = `${mapping.meshNetworkId}\u0000${mapping.nodeNum}`;
    const previousOwner = callsignOwners.get(mapping.callsign);
    if (previousOwner && previousOwner !== owner) {
      throw new CallMeshClientError("CALLMESH_MAPPING_CONFLICT");
    }
    callsignOwners.set(mapping.callsign, owner);
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

function parseMappingItem(
  payload: unknown,
  mappingHash: string,
  meshNetworkId: string,
  batchEffectiveAt: string,
): CallMeshMapping | undefined {
  const record = strictRecord(payload, [
    "mesh_id",
    "meshId",
    "mesh_id_normalized",
    "meshIdNormalized",
    "mesh_network_id",
    "meshNetworkId",
    "callsign_base",
    "callsignBase",
    "callsign",
    "callsign_with_ssid",
    "callsignWithSsid",
    "aprs_callsign",
    "aprsCallsign",
    "aprs_callsign_base",
    "aprsCallsignBase",
    "aprs_callsign_with_ssid",
    "aprsCallsignWithSsid",
    "aprs_ssid",
    "aprsSsid",
    "ssid",
    "SSID",
    "enabled",
    "aprs_enabled",
    "aprsEnabled",
    "allow_aprs",
    "allowAprs",
    "effective_at",
    "effectiveAt",
    "version",
    "symbol_table",
    "symbolTable",
    "aprs_symbol_table",
    "aprsSymbolTable",
    "symbol_code",
    "symbolCode",
    "aprs_symbol_code",
    "aprsSymbolCode",
    "symbol_overlay",
    "symbolOverlay",
    "aprs_symbol_overlay",
    "aprsSymbolOverlay",
    "symbol",
    "comment",
    "aprs_comment",
    "aprsComment",
    "altitude_m",
    "altitudeMeters",
    "altitude",
  ]);
  if (!mappingEnabled(record)) {
    return undefined;
  }
  const metadata = parseMappingMetadata(record);
  const network = optionalText(
    record.mesh_network_id ?? record.meshNetworkId,
    128,
  );
  if (network && network !== meshNetworkId) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  const nodeNum = parseMeshId(
    record.mesh_id ??
      record.meshId ??
      record.mesh_id_normalized ??
      record.meshIdNormalized,
  );
  const callsign = parseMappingCallsign(record);
  const version =
    record.version !== undefined
      ? boundedText(record.version, 128)
      : mappingHash;
  const effectiveAt =
    record.effective_at !== undefined || record.effectiveAt !== undefined
      ? normalizeTimestamp(record.effective_at ?? record.effectiveAt)
      : normalizeTimestamp(batchEffectiveAt);
  return {
    version,
    effectiveAt,
    meshNetworkId,
    nodeNum,
    callsign,
    ...metadata,
  };
}

function mappingEnabled(record: Record<string, unknown>): boolean {
  const flags = [
    record.enabled,
    record.aprs_enabled,
    record.aprsEnabled,
    record.allow_aprs,
    record.allowAprs,
  ].filter((flag) => flag !== undefined && flag !== null);
  if (flags.some((flag) => typeof flag !== "boolean")) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  if (new Set(flags).size > 1) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return flags.length === 0 || flags[0] === true;
}

function parseMappingMetadata(
  record: Record<string, unknown>,
): Pick<
  CallMeshMapping,
  "symbolTable" | "symbolCode" | "symbolOverlay" | "comment" | "altitudeMeters"
> {
  const metadata: Pick<
    CallMeshMapping,
    | "symbolTable"
    | "symbolCode"
    | "symbolOverlay"
    | "comment"
    | "altitudeMeters"
  > = {};
  if (hasOwn(record, "symbol")) {
    const symbol = optionalAprsText(record.symbol, 2);
    metadata.symbolTable = symbol?.[0] ?? null;
    metadata.symbolCode = symbol?.[1] ?? null;
  } else {
    const table = firstPresent(record, [
      "symbol_table",
      "symbolTable",
      "aprs_symbol_table",
      "aprsSymbolTable",
    ]);
    if (table.present) {
      metadata.symbolTable = optionalAprsText(table.value, 1)?.[0] ?? null;
    }
    const code = firstPresent(record, [
      "symbol_code",
      "symbolCode",
      "aprs_symbol_code",
      "aprsSymbolCode",
    ]);
    if (code.present) {
      metadata.symbolCode = optionalAprsText(code.value, 1)?.[0] ?? null;
    }
  }
  const overlay = firstPresent(record, [
    "symbol_overlay",
    "symbolOverlay",
    "aprs_symbol_overlay",
    "aprsSymbolOverlay",
  ]);
  if (overlay.present) {
    metadata.symbolOverlay = optionalAprsText(overlay.value, 1)?.[0] ?? null;
  }
  const comment = firstPresent(record, [
    "aprs_comment",
    "aprsComment",
    "comment",
  ]);
  if (comment.present) {
    metadata.comment =
      comment.value === null ? null : boundedTextAllowEmpty(comment.value, 80);
  }
  const altitude = firstPresent(record, [
    "altitude_m",
    "altitudeMeters",
    "altitude",
  ]);
  if (altitude.present) {
    if (altitude.value === null) {
      metadata.altitudeMeters = null;
    } else if (
      typeof altitude.value === "number" &&
      Number.isFinite(altitude.value)
    ) {
      metadata.altitudeMeters = altitude.value;
    } else {
      throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
    }
  }
  return metadata;
}

function parseMappingCallsign(record: Record<string, unknown>): string {
  const full = optionalText(
    record.aprs_callsign ??
      record.aprsCallsign ??
      record.aprs_callsign_with_ssid ??
      record.aprsCallsignWithSsid ??
      record.callsign_with_ssid ??
      record.callsignWithSsid ??
      record.callsign,
    16,
  );
  if (full) {
    return validateCallsign(full);
  }
  const baseValue =
    record.aprs_callsign_base ??
    record.aprsCallsignBase ??
    record.callsign_base ??
    record.callsignBase;
  const base = boundedText(baseValue, 6).toUpperCase();
  if (!/^[A-Z0-9]{1,6}$/.test(base)) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  const rawSsid =
    record.aprs_ssid ?? record.aprsSsid ?? record.ssid ?? record.SSID ?? 0;
  const ssid = Number(rawSsid);
  if (!Number.isInteger(ssid) || ssid < -15 || ssid > 15) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return validateCallsign(ssid === 0 ? base : `${base}-${Math.abs(ssid)}`);
}

function parseProvision(payload: unknown): CallMeshProvision | undefined {
  if (payload === null) {
    return undefined;
  }
  const record = strictRecord(payload, [
    "callsign_base",
    "callsignBase",
    "base",
    "ssid",
    "aprs_ssid",
    "aprsSsid",
    "symbol_table",
    "symbolTable",
    "aprs_symbol_table",
    "aprsSymbolTable",
    "symbol_code",
    "symbolCode",
    "aprs_symbol_code",
    "aprsSymbolCode",
    "symbol_overlay",
    "symbolOverlay",
    "aprs_symbol_overlay",
    "aprsSymbolOverlay",
    "symbol",
    "comment",
    "latitude",
    "lat",
    "longitude",
    "lon",
    "tx_power_w",
    "txPowerW",
    "antenna_gain_dbi",
    "antennaGainDbi",
    "antenna_height_m",
    "antennaHeightM",
    "altitude_m",
    "altitudeMeters",
    "altitude",
    "phg",
  ]);
  const callsignBase = boundedText(
    record.callsign_base ?? record.callsignBase ?? record.base,
    6,
  ).toUpperCase();
  const symbol = optionalText(record.symbol, 2);
  if (symbol && symbol.length !== 2) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  const symbolTable =
    optionalText(
      record.symbol_table ??
        record.symbolTable ??
        record.aprs_symbol_table ??
        record.aprsSymbolTable,
      1,
    ) ??
    symbol?.[0] ??
    "/";
  const symbolCode =
    optionalText(
      record.symbol_code ??
        record.symbolCode ??
        record.aprs_symbol_code ??
        record.aprsSymbolCode,
      1,
    ) ??
    symbol?.[1] ??
    ">";
  const overlay = firstOwnValue(record, [
    "symbol_overlay",
    "symbolOverlay",
    "aprs_symbol_overlay",
    "aprsSymbolOverlay",
  ]);
  const altitude = firstPresent(record, [
    "altitude_m",
    "altitudeMeters",
    "altitude",
  ]);
  const phg = firstPresent(record, ["phg"]);
  const provision: CallMeshProvision = {
    callsignBase,
    ssid: numberField(record.ssid ?? record.aprs_ssid ?? record.aprsSsid),
    symbolTable,
    symbolCode,
    ...(overlay !== undefined
      ? {
          symbolOverlay: overlay === null ? null : boundedText(overlay, 1),
        }
      : {}),
    ...(record.comment !== undefined
      ? { comment: boundedText(record.comment, 80) }
      : {}),
    ...(record.latitude !== undefined || record.lat !== undefined
      ? { latitude: numberField(record.latitude ?? record.lat) }
      : {}),
    ...(record.longitude !== undefined || record.lon !== undefined
      ? { longitude: numberField(record.longitude ?? record.lon) }
      : {}),
    ...(record.tx_power_w !== undefined || record.txPowerW !== undefined
      ? { txPowerW: numberField(record.tx_power_w ?? record.txPowerW) }
      : {}),
    ...(record.antenna_gain_dbi !== undefined ||
    record.antennaGainDbi !== undefined
      ? {
          antennaGainDbi: numberField(
            record.antenna_gain_dbi ?? record.antennaGainDbi,
          ),
        }
      : {}),
    ...(record.antenna_height_m !== undefined ||
    record.antennaHeightM !== undefined
      ? {
          antennaHeightM: numberField(
            record.antenna_height_m ?? record.antennaHeightM,
          ),
        }
      : {}),
    ...(altitude.present
      ? {
          altitudeMeters:
            altitude.value === null ? null : numberField(altitude.value),
        }
      : {}),
    ...(phg.present
      ? {
          phg: phg.value === null ? null : parsePhg(phg.value),
        }
      : {}),
  };
  if (!Value.Check(CallMeshProvisionSchema, provision)) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return provision;
}

function parsePhg(value: unknown): string {
  const phg = boundedText(value, 4);
  if (!/^[0-9]{3,4}$/.test(phg)) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return phg;
}

function validateSnapshot(snapshot: CallMeshSyncSnapshot): void {
  if (typeof snapshot.active !== "boolean") {
    throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
  }
  parseHash(snapshot.mappingHash);
  normalizeTimestamp(snapshot.acceptedServerTime);
  normalizeTimestamp(snapshot.lastHeartbeatAt);
  normalizeTimestamp(snapshot.mappingSyncedAt);
  if (!FINGERPRINT_PATTERN.test(snapshot.mappingsFingerprint)) {
    throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
  }
  validateMappings(snapshot.mappings);
  if (snapshot.provision) {
    if (!snapshot.active) {
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
    if (
      !Value.Check(CallMeshProvisionSchema, snapshot.provision) ||
      !snapshot.provisionExpiresAt ||
      !snapshot.provisionFingerprint ||
      !FINGERPRINT_PATTERN.test(snapshot.provisionFingerprint)
    ) {
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
    normalizeTimestamp(snapshot.provisionExpiresAt);
    if (
      Date.parse(snapshot.provisionExpiresAt) !==
      Date.parse(snapshot.lastHeartbeatAt) + PROVISION_LEASE_MS
    ) {
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
  } else if (snapshot.provisionExpiresAt || snapshot.provisionFingerprint) {
    throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
  }
  if (fingerprint(snapshot.mappings) !== snapshot.mappingsFingerprint) {
    throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
  }
  if (
    snapshot.provision &&
    fingerprint(snapshot.provision) !== snapshot.provisionFingerprint
  ) {
    throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
  }
}

function snapshotMatchesHighWater(
  snapshot: CallMeshSyncSnapshot,
  highWater: CallMeshHistoryHighWater | undefined,
): boolean {
  return Boolean(
    highWater &&
    snapshot.mappingHash === highWater.mappingHash &&
    snapshot.acceptedServerTime === highWater.lastServerTime &&
    snapshot.mappingsFingerprint === highWater.mappingsFingerprint,
  );
}

function validateMappings(mappings: CallMeshMapping[]): void {
  if (
    mappings.length > MAX_MAPPING_ITEMS ||
    !mappings.every((mapping) => Value.Check(CallMeshMappingSchema, mapping))
  ) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
}

function strictRecord(
  value: unknown,
  allowedKeys: readonly string[],
): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  const record = value as Record<string, unknown>;
  const allowed = new Set(allowedKeys);
  if (Object.keys(record).some((key) => !allowed.has(key))) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return record;
}

function hasOwn(record: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(record, key);
}

function firstPresent(
  record: Record<string, unknown>,
  keys: readonly string[],
): { present: boolean; value?: unknown } {
  for (const key of keys) {
    if (hasOwn(record, key)) {
      return { present: true, value: record[key] };
    }
  }
  return { present: false };
}

function optionalAprsText(
  value: unknown,
  maximumLength: number,
): string | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  if (
    typeof value !== "string" ||
    value.length > maximumLength ||
    !/^[ -~]+$/.test(value)
  ) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return value;
}

function boundedTextAllowEmpty(value: unknown, maximumLength: number): string {
  if (typeof value !== "string") {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  const text = value
    .replace(/[\r\n]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (text.length > maximumLength || hasControlCharacter(text)) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return text;
}

function mappingMetadataFromRow(
  row: Record<string, unknown>,
): Pick<
  CallMeshMapping,
  "symbolTable" | "symbolCode" | "symbolOverlay" | "comment" | "altitudeMeters"
> {
  const metadata: Pick<
    CallMeshMapping,
    | "symbolTable"
    | "symbolCode"
    | "symbolOverlay"
    | "comment"
    | "altitudeMeters"
  > = {};
  const readOptional = (
    presentValue: unknown,
    value: unknown,
    key:
      | "symbolTable"
      | "symbolCode"
      | "symbolOverlay"
      | "comment"
      | "altitudeMeters",
  ) => {
    const present = Number(presentValue);
    if (present !== 0 && present !== 1) {
      throw new CallMeshClientError("CALLMESH_MAPPING_STORE_FAILED");
    }
    if (present === 1) {
      Object.assign(metadata, { [key]: value === null ? null : value });
    }
  };
  readOptional(row.symbol_table_present, row.symbol_table, "symbolTable");
  readOptional(row.symbol_code_present, row.symbol_code, "symbolCode");
  readOptional(row.symbol_overlay_present, row.symbol_overlay, "symbolOverlay");
  readOptional(row.comment_present, row.comment, "comment");
  readOptional(
    row.altitude_meters_present,
    row.altitude_meters === null ? null : Number(row.altitude_meters),
    "altitudeMeters",
  );
  return metadata;
}

function firstOwnValue(
  record: Record<string, unknown>,
  keys: readonly string[],
): unknown {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(record, key)) {
      return record[key];
    }
  }
  return undefined;
}

function parseHash(value: unknown): string {
  return boundedText(value, 128);
}

function parseMeshId(value: unknown): number {
  if (typeof value !== "string") {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  let normalized = value.trim();
  if (normalized.startsWith("!")) {
    normalized = normalized.slice(1);
  } else if (normalized.toLowerCase().startsWith("0x")) {
    normalized = normalized.slice(2);
  }
  const hex = normalized.replace(/[^a-fA-F0-9]/g, "");
  if (!hex) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  const uint32Hex = hex.length > 8 ? hex.slice(-8) : hex;
  return Number.parseInt(uint32Hex, 16) >>> 0;
}

function validateCallsign(value: string): string {
  const callsign = value.trim().toUpperCase();
  const match = /^(?<base>[A-Z0-9]{1,6})(?:-(?<ssid>[0-9]{1,2}))?$/.exec(
    callsign,
  );
  if (!match?.groups?.base) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  if (!match.groups.ssid) {
    return match.groups.base;
  }
  const ssid = Number(match.groups.ssid);
  if (!Number.isInteger(ssid) || ssid < 1 || ssid > 15) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return `${match.groups.base}-${ssid}`;
}

function normalizeTimestamp(value: unknown): string {
  if (typeof value !== "string") {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  const time = Date.parse(value);
  if (!Number.isFinite(time)) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return new Date(time).toISOString();
}

function boundedText(
  value: unknown,
  maximumLength: number,
  code = "CALLMESH_SCHEMA_INVALID",
): string {
  if (typeof value !== "string") {
    throw new CallMeshClientError(code);
  }
  const text = value.trim();
  if (!text || text.length > maximumLength || hasControlCharacter(text)) {
    throw new CallMeshClientError(code);
  }
  return text;
}

function hasControlCharacter(value: string): boolean {
  return [...value].some((character) => {
    const characterCode = character.codePointAt(0) ?? 0;
    return characterCode <= 0x1f || characterCode === 0x7f;
  });
}

function validateApiKey(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (
    value.length === 0 ||
    Buffer.byteLength(value, "utf8") > 4_096 ||
    hasControlCharacter(value)
  ) {
    throw new CallMeshClientError("CALLMESH_CONFIGURATION_INVALID");
  }
  return value;
}

async function readBoundedResponse(response: Response): Promise<string> {
  if (!response.body) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      size += value.byteLength;
      if (size > MAX_RESPONSE_BYTES) {
        await reader.cancel();
        throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
      }
      chunks.push(value);
    }
    if (size === 0) {
      throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
    }
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(
        Buffer.concat(chunks, size),
      );
    } catch {
      throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
    }
  } finally {
    reader.releaseLock();
  }
}

function rollbackQuietly(database: DatabaseSync): void {
  try {
    database.exec("ROLLBACK");
  } catch {
    // Preserve the stable repository error below.
  }
}

function optionalText(
  value: unknown,
  maximumLength: number,
): string | undefined {
  return value === undefined || value === null
    ? undefined
    : boundedText(value, maximumLength);
}

function numberField(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new CallMeshClientError("CALLMESH_SCHEMA_INVALID");
  }
  return value;
}

function fingerprint(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
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
    if (
      (url.protocol !== "http:" && url.protocol !== "https:") ||
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      (url.pathname !== "/" && url.pathname !== "")
    ) {
      throw new Error("invalid CallMesh origin");
    }
    return url;
  } catch {
    throw new CallMeshClientError("CALLMESH_CONFIGURATION_INVALID");
  }
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

function fatalProvisionState(
  code: string,
): CallMeshStatus["provisionState"] | undefined {
  return code === "CALLMESH_AUTH_INVALID" ||
    code === "CALLMESH_SCHEMA_INVALID" ||
    code === "CALLMESH_MAPPING_CONFLICT" ||
    code === "CALLMESH_RESPONSE_CONFLICT" ||
    code === "CALLMESH_MAPPING_STORE_FAILED" ||
    (/^CALLMESH_HTTP_4\d\d$/.test(code) &&
      code !== "CALLMESH_HTTP_408" &&
      code !== "CALLMESH_HTTP_429")
    ? "invalid"
    : undefined;
}

function delay(delayMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}
