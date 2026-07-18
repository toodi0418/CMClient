import type {
  AprsMonitorStatus,
  AprsRuntimeStatus,
  CallMeshMapping,
} from "@cmclient/contracts";

import {
  AprsIsMonitor,
  AprsRemoteHighWaterStore,
  type AprsIsRxSession,
} from "./aprs-monitor.js";
import type { AprsOutboxEntry } from "./aprs-outbox.js";
import { DomainEventBus } from "./events.js";
import { selectActiveMapping } from "./mesh-runtime.js";
import { GatewayDatabase } from "./persistence/database.js";

export interface AprsOutboxFlusher {
  flush(limit?: number): Promise<AprsOutboxEntry[]>;
}

export interface AprsMonitorClient {
  connect(onLine: (line: string) => void): Promise<AprsIsRxSession>;
}

export interface AprsGatewayRuntimeOptions {
  database: GatewayDatabase;
  eventBus: DomainEventBus;
  mappingProvider?: () => readonly CallMeshMapping[];
  monitorClientFactory: (filterExpression: string) => AprsMonitorClient;
  outbox: AprsOutboxFlusher;
  clock?: () => Date;
  flushIntervalMs?: number;
  monitorRefreshIntervalMs?: number;
}

export class AprsGatewayRuntimeError extends Error {
  readonly code = "APRS_RUNTIME_CONFIGURATION_INVALID";

  constructor() {
    super("APRS_RUNTIME_CONFIGURATION_INVALID");
    this.name = "AprsGatewayRuntimeError";
  }
}

export class AprsGatewayRuntime {
  private readonly clock: () => Date;
  private readonly mappingProvider: () => readonly CallMeshMapping[];
  private readonly flushIntervalMs: number;
  private readonly monitorRefreshIntervalMs: number;
  private flushTimer: NodeJS.Timeout | undefined;
  private monitorTimer: NodeJS.Timeout | undefined;
  private monitorSession: AprsIsRxSession | undefined;
  private flushInProgress = false;
  private monitorRefreshInProgress = false;
  private started = false;
  private monitorStatus: AprsMonitorStatus = "stopped";
  private mappedCallsigns = 0;
  private lastErrorCode: string | undefined;

  constructor(private readonly options: AprsGatewayRuntimeOptions) {
    this.clock = options.clock ?? (() => new Date());
    this.mappingProvider =
      options.mappingProvider ??
      (() => options.database.callmeshMappings.list());
    this.flushIntervalMs = positiveInterval(options.flushIntervalMs ?? 5_000);
    this.monitorRefreshIntervalMs = positiveInterval(
      options.monitorRefreshIntervalMs ?? 60_000,
    );
  }

  start(): void {
    if (this.started) {
      return;
    }
    this.started = true;
    void this.flushNow();
    void this.refreshMonitor();
    this.flushTimer = setInterval(
      () => void this.flushNow(),
      this.flushIntervalMs,
    );
    this.monitorTimer = setInterval(
      () => void this.refreshMonitor(),
      this.monitorRefreshIntervalMs,
    );
    this.flushTimer.unref();
    this.monitorTimer.unref();
  }

  async stop(): Promise<void> {
    if (!this.started) {
      return;
    }
    this.started = false;
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = undefined;
    }
    if (this.monitorTimer) {
      clearInterval(this.monitorTimer);
      this.monitorTimer = undefined;
    }
    const session = this.monitorSession;
    this.monitorSession = undefined;
    await session?.close();
    this.monitorStatus = "stopped";
    this.mappedCallsigns = 0;
  }

  status(): AprsRuntimeStatus {
    const counts = this.options.database.connection
      .prepare(
        "SELECT status, COUNT(*) AS count FROM aprs_outbox WHERE status IN ('queued', 'sending', 'failed') GROUP BY status",
      )
      .all() as Array<{
      status: "queued" | "sending" | "failed";
      count: number;
    }>;
    const count = (statuses: readonly string[]) =>
      counts
        .filter((entry) => statuses.includes(entry.status))
        .reduce((total, entry) => total + entry.count, 0);
    return {
      configured: true,
      running: this.started,
      monitorStatus: this.monitorStatus,
      mappedCallsigns: this.mappedCallsigns,
      pendingOutbox: count(["queued", "sending"]),
      failedOutbox: count(["failed"]),
      ...(this.lastErrorCode ? { lastErrorCode: this.lastErrorCode } : {}),
    };
  }

  async flushNow(): Promise<void> {
    if (this.flushInProgress) {
      return;
    }
    this.flushInProgress = true;
    try {
      const entries = await this.options.outbox.flush();
      for (const entry of entries) {
        this.publish(
          entry.status === "sent" ? "aprs.outbox.sent" : "aprs.outbox.failed",
          {
            outboxId: entry.id,
            canonicalEventId: entry.canonicalEventId,
            callsign: entry.callsign,
            status: entry.status,
            attempts: entry.attempts,
            ...(entry.lastErrorCode ? { code: entry.lastErrorCode } : {}),
          },
        );
      }
    } catch (error) {
      this.lastErrorCode = stableErrorCode(error, "APRS_OUTBOX_FLUSH_FAILED");
      this.publish("aprs.outbox.error", {
        code: this.lastErrorCode,
      });
    } finally {
      this.flushInProgress = false;
    }
  }

  async refreshMonitor(): Promise<void> {
    if (this.monitorRefreshInProgress) {
      return;
    }
    this.monitorRefreshInProgress = true;
    this.monitorStatus = "connecting";
    this.lastErrorCode = undefined;
    try {
      const previous = this.monitorSession;
      this.monitorSession = undefined;
      await previous?.close();
      const targets = activeTargets(this.mappingProvider(), this.clock());
      this.mappedCallsigns = targets.length;
      if (targets.length === 0) {
        this.monitorStatus = "idle";
        this.publish("aprs.monitor.idle", { mappedCallsigns: 0 });
        return;
      }
      if (
        new Set(targets.map((target) => target.callsign)).size !==
        targets.length
      ) {
        this.monitorStatus = "error";
        this.lastErrorCode = "CALLMESH_MAPPING_CONFLICT";
        this.publish("aprs.monitor.error", {
          code: this.lastErrorCode,
        });
        return;
      }
      const monitor = new AprsIsMonitor(
        targets,
        new AprsRemoteHighWaterStore(this.options.database.connection),
      );
      const client = this.options.monitorClientFactory(
        monitor.filterExpression(),
      );
      this.monitorSession = await client.connect((line) => {
        const receivedAt = this.clock().toISOString();
        const result = monitor.observeLine(line, receivedAt);
        this.publish("aprs.monitor.observed", {
          kind: result.kind,
          ...(result.reason ? { reason: result.reason } : {}),
          ...(result.remote
            ? {
                callsign: result.remote.callsign,
                eventMarker: result.remote.eventMarker,
                eventTime: result.remote.eventTime,
              }
            : {}),
        });
      });
      this.monitorStatus = "connected";
      this.publish("aprs.monitor.connected", {
        mappedCallsigns: targets.length,
      });
    } catch (error) {
      this.monitorStatus = "error";
      this.lastErrorCode = stableErrorCode(
        error,
        "APRS_MONITOR_CONNECT_FAILED",
      );
      this.publish("aprs.monitor.error", {
        code: this.lastErrorCode,
      });
    } finally {
      this.monitorRefreshInProgress = false;
    }
  }

  private publish(type: string, payload: Record<string, unknown>): void {
    this.options.eventBus.publish({ type, source: "gateway", payload });
  }
}

function activeTargets(mappings: readonly CallMeshMapping[], now: Date) {
  const identities = new Map<
    string,
    { meshNetworkId: string; nodeNum: number }
  >();
  for (const mapping of mappings) {
    identities.set(`${mapping.meshNetworkId}\u0000${mapping.nodeNum}`, {
      meshNetworkId: mapping.meshNetworkId,
      nodeNum: mapping.nodeNum,
    });
  }
  return [...identities.values()]
    .map((identity) => {
      const selected = selectActiveMapping(
        mappings,
        identity.meshNetworkId,
        identity.nodeNum,
        now.toISOString(),
      );
      return selected.kind === "mapping"
        ? {
            callsign: selected.mapping.callsign,
            mappingVersion: selected.mapping.version,
            ...identity,
          }
        : undefined;
    })
    .filter((target) => target !== undefined)
    .sort((left, right) => left.callsign.localeCompare(right.callsign));
}

function positiveInterval(value: number): number {
  if (!Number.isInteger(value) || value < 100) {
    throw new AprsGatewayRuntimeError();
  }
  return value;
}

function stableErrorCode(error: unknown, fallback: string): string {
  if (
    error instanceof Error &&
    "code" in error &&
    typeof error.code === "string" &&
    /^[A-Z][A-Z0-9_]{0,127}$/.test(error.code)
  ) {
    return error.code;
  }
  return fallback;
}
