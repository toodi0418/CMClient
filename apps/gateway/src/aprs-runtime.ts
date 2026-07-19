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

const MONITOR_SESSION_CLOSE_TIMEOUT_MS = 10_000;

export interface AprsOutboxFlusher {
  flush(limit?: number): Promise<AprsOutboxEntry[]>;
}

export interface AprsMonitorClient {
  connect(
    onLine: (line: string) => void,
    onLineError?: (error: unknown) => void,
  ): Promise<AprsIsRxSession>;
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

class AprsMonitorSessionCloseError extends Error {
  readonly code = "APRS_MONITOR_CLOSE_FAILED";

  constructor() {
    super("APRS_MONITOR_CLOSE_FAILED");
    this.name = "AprsMonitorSessionCloseError";
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
  private monitorToken: object | undefined;
  private flushOperation: Promise<void> | undefined;
  private monitorRefreshOperation: Promise<void> | undefined;
  private stopOperation: Promise<void> | undefined;
  private lifecycleGeneration = 0;
  private lifecycleStopped = false;
  private restartAfterStop = false;
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
    if (this.stopOperation) {
      this.restartAfterStop = true;
      return;
    }
    this.startTimers();
  }

  private startTimers(): void {
    this.lifecycleStopped = false;
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

  stop(): Promise<void> {
    if (this.stopOperation) {
      this.restartAfterStop = false;
      return this.stopOperation;
    }
    if (this.lifecycleStopped) {
      return Promise.resolve();
    }

    this.lifecycleStopped = true;
    this.lifecycleGeneration += 1;
    this.started = false;
    this.restartAfterStop = false;
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
    this.monitorToken = undefined;
    this.monitorStatus = "stopped";
    this.mappedCallsigns = 0;

    const closeSession = session ? closeMonitorSession(session) : undefined;
    const operations = [
      ...(this.flushOperation ? [this.flushOperation] : []),
      ...(this.monitorRefreshOperation ? [this.monitorRefreshOperation] : []),
      ...(closeSession ? [closeSession] : []),
    ];
    const stopOperation = this.finishStop(operations);
    this.stopOperation = stopOperation;
    void stopOperation.then(
      () => this.completeStop(stopOperation, true),
      () => this.completeStop(stopOperation, false),
    );
    return stopOperation;
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

  flushNow(): Promise<void> {
    if (this.lifecycleStopped) {
      return Promise.resolve();
    }
    if (this.flushOperation) {
      return this.flushOperation;
    }
    const generation = this.lifecycleGeneration;
    const operation = this.runFlush(generation);
    this.flushOperation = operation;
    void operation.then(
      () => this.completeFlush(operation),
      () => this.completeFlush(operation),
    );
    return operation;
  }

  private async runFlush(generation: number): Promise<void> {
    try {
      const entries = await this.options.outbox.flush();
      if (!this.isGenerationActive(generation)) {
        return;
      }
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
      if (!this.isGenerationActive(generation)) {
        return;
      }
      this.lastErrorCode = stableErrorCode(error, "APRS_OUTBOX_FLUSH_FAILED");
      this.publish("aprs.outbox.error", {
        code: this.lastErrorCode,
      });
    }
  }

  refreshMonitor(): Promise<void> {
    if (this.lifecycleStopped) {
      return Promise.resolve();
    }
    if (this.monitorRefreshOperation) {
      return this.monitorRefreshOperation;
    }
    const generation = this.lifecycleGeneration;
    const operation = this.runMonitorRefresh(generation);
    this.monitorRefreshOperation = operation;
    void operation.then(
      () => this.completeMonitorRefresh(operation),
      () => this.completeMonitorRefresh(operation),
    );
    return operation;
  }

  private async runMonitorRefresh(generation: number): Promise<void> {
    this.monitorStatus = "connecting";
    this.lastErrorCode = undefined;
    try {
      const previous = this.monitorSession;
      this.monitorSession = undefined;
      this.monitorToken = undefined;
      if (previous) {
        await closeMonitorSession(previous);
      }
      if (!this.isGenerationActive(generation)) {
        return;
      }
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
      const token = {};
      this.monitorToken = token;
      let callbackFailed = false;
      const onLineError = (error: unknown): void => {
        if (
          !this.isGenerationActive(generation) ||
          this.monitorToken !== token
        ) {
          return;
        }
        callbackFailed = true;
        this.monitorStatus = "error";
        this.lastErrorCode = stableErrorCode(
          error,
          "APRS_MONITOR_CALLBACK_FAILED",
        );
        try {
          this.publish("aprs.monitor.error", { code: this.lastErrorCode });
        } catch {
          // This callback may run inside a socket EventEmitter.
        }
      };
      const session = await client.connect((line) => {
        if (
          !this.isGenerationActive(generation) ||
          this.monitorToken !== token
        ) {
          return;
        }
        try {
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
          if (callbackFailed && result.kind !== "ignored") {
            callbackFailed = false;
            this.monitorStatus = "connected";
            this.lastErrorCode = undefined;
            this.publish("aprs.monitor.connected", {
              mappedCallsigns: targets.length,
            });
          }
        } catch (error) {
          onLineError(error);
        }
      }, onLineError);
      if (!this.isGenerationActive(generation) || this.monitorToken !== token) {
        await closeMonitorSession(session);
        return;
      }
      this.monitorSession = session;
      if (callbackFailed) {
        return;
      }
      this.monitorStatus = "connected";
      this.publish("aprs.monitor.connected", {
        mappedCallsigns: targets.length,
      });
    } catch (error) {
      if (!this.isGenerationActive(generation)) {
        if (error instanceof AprsMonitorSessionCloseError) {
          throw error;
        }
        return;
      }
      this.monitorToken = undefined;
      this.monitorStatus = "error";
      this.lastErrorCode = stableErrorCode(
        error,
        "APRS_MONITOR_CONNECT_FAILED",
      );
      this.publish("aprs.monitor.error", {
        code: this.lastErrorCode,
      });
    }
  }

  private async finishStop(operations: Promise<void>[]): Promise<void> {
    const results = await Promise.allSettled(operations);
    this.monitorStatus = "stopped";
    this.mappedCallsigns = 0;
    const failure = results.find(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );
    if (failure) {
      throw failure.reason;
    }
  }

  private completeStop(operation: Promise<void>, succeeded: boolean): void {
    if (this.stopOperation !== operation) {
      return;
    }
    this.stopOperation = undefined;
    const restart = succeeded && this.restartAfterStop;
    this.restartAfterStop = false;
    if (restart) {
      this.startTimers();
    }
  }

  private completeFlush(operation: Promise<void>): void {
    if (this.flushOperation === operation) {
      this.flushOperation = undefined;
    }
  }

  private completeMonitorRefresh(operation: Promise<void>): void {
    if (this.monitorRefreshOperation === operation) {
      this.monitorRefreshOperation = undefined;
    }
  }

  private isGenerationActive(generation: number): boolean {
    return !this.lifecycleStopped && this.lifecycleGeneration === generation;
  }

  private publish(type: string, payload: Record<string, unknown>): void {
    this.options.eventBus.publish({ type, source: "gateway", payload });
  }
}

function closeMonitorSession(session: AprsIsRxSession): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (error?: AprsMonitorSessionCloseError) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    };
    const timer = setTimeout(
      () => finish(new AprsMonitorSessionCloseError()),
      MONITOR_SESSION_CLOSE_TIMEOUT_MS,
    );
    timer.unref();
    void Promise.resolve()
      .then(() => session.close())
      .then(
        () => finish(),
        () => finish(new AprsMonitorSessionCloseError()),
      );
  });
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
