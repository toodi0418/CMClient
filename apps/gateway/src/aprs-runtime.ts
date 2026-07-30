import type {
  AprsMonitorStatus,
  AprsRuntimeStatus,
  CallMeshMapping,
} from "@cmclient/contracts";

import {
  APRS_RX_FILTER_EXPRESSION,
  AprsIsMonitor,
  AprsRemoteHighWaterStore,
  type AprsMonitorTarget,
  type AprsIsRxSession,
} from "./aprs-monitor.js";
import {
  AprsIgateFamily,
  AprsIgateRepository,
  type AprsIgatePacket,
  type AprsIgateSubmission,
} from "./aprs-igate.js";
import {
  AprsTransmissionFencedError,
  type AprsOutboxEntry,
  type AprsTransport,
  type AprsVerifiedTransportSession,
} from "./aprs-outbox.js";
import {
  deriveAprsObserverCallsign,
  type AprsRuntimeState,
} from "./aprs-identity.js";
import { DomainEventBus } from "./events.js";
import { selectActiveMapping } from "./mesh-runtime.js";
import { GatewayDatabase } from "./persistence/database.js";

const MONITOR_SESSION_CLOSE_TIMEOUT_MS = 10_000;
const DEFAULT_MONITOR_ACTIVITY_TIMEOUT_MS = 5 * 60_000;
const DEFAULT_IGATE_TICK_INTERVAL_MS = 1_000;
const DEFAULT_IGATE_RETRY_INITIAL_MS = 1_000;
const DEFAULT_IGATE_RETRY_MAXIMUM_MS = 60_000;

export interface AprsOutboxFlusher {
  flush(
    limit?: number,
    shouldContinue?: () => boolean,
  ): Promise<AprsOutboxEntry[]>;
  close?(): Promise<void>;
}

export interface AprsMonitorClient {
  connect(
    onLine: (line: string) => void,
    onLineError?: (error: unknown) => void,
    onActivity?: () => void,
  ): Promise<AprsIsRxSession>;
}

export interface AprsGatewayRuntimeOptions {
  database: GatewayDatabase;
  eventBus: DomainEventBus;
  stateProvider?: () => AprsRuntimeState | undefined;
  monitorClientFactory: (
    filterExpression: string,
    provisionFingerprint?: string,
  ) => AprsMonitorClient;
  outbox: AprsOutboxFlusher;
  stationTransport?: AprsTransport;
  version?: string;
  clock?: () => Date;
  flushIntervalMs?: number;
  igateTickIntervalMs?: number;
  igateRetryInitialMs?: number;
  igateRetryMaximumMs?: number;
  monitorRefreshIntervalMs?: number;
  monitorActivityTimeoutMs?: number;
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

class AprsMappingConflictError extends Error {
  readonly code = "CALLMESH_MAPPING_CONFLICT";

  constructor(readonly mappedCallsigns: number) {
    super("CALLMESH_MAPPING_CONFLICT");
    this.name = "AprsMappingConflictError";
  }
}

interface AprsMonitorPlan {
  readonly connectionKey: string;
  readonly mappedCallsigns: number;
  readonly mappingsFingerprint: string;
  readonly nextMappingEffectiveAt: number | undefined;
  readonly state: AprsRuntimeState | undefined;
  readonly targets: readonly AprsMonitorTarget[];
}

export class AprsGatewayRuntime {
  private readonly clock: () => Date;
  private readonly flushIntervalMs: number;
  private readonly igateTickIntervalMs: number;
  private readonly igateRetryInitialMs: number;
  private readonly igateRetryMaximumMs: number;
  private readonly monitorRefreshIntervalMs: number;
  private readonly monitorActivityTimeoutMs: number;
  private readonly igateRepository: AprsIgateRepository;
  private flushTimer: NodeJS.Timeout | undefined;
  private igateTimer: NodeJS.Timeout | undefined;
  private monitorTimer: NodeJS.Timeout | undefined;
  private monitorReconnectTimer: NodeJS.Timeout | undefined;
  private monitor: AprsIsMonitor | undefined;
  private monitorSession: AprsIsRxSession | undefined;
  private monitorToken: object | undefined;
  private monitorConfigurationKey: string | undefined;
  private monitorMappingsFingerprint: string | undefined;
  private monitorNextMappingEffectiveAt: number | undefined;
  private monitorLastActivityAt: number | undefined;
  private flushOperation: Promise<void> | undefined;
  private igateOperation: Promise<void> | undefined;
  private monitorRefreshOperation: Promise<void> | undefined;
  private monitorRefreshQueued = false;
  private txReady = false;
  private flushPendingUntilMonitor = false;
  private igatePendingUntilMonitor = false;
  private igateRetryFailures = 0;
  private igateRetryNotBefore = 0;
  private stopOperation: Promise<void> | undefined;
  private lifecycleGeneration = 0;
  private lifecycleStopped = false;
  private restartAfterStop = false;
  private started = false;
  private monitorStatus: AprsMonitorStatus = "stopped";
  private mappedCallsigns = 0;
  private lastErrorCode: string | undefined;
  private igateFamily: AprsIgateFamily | undefined;
  private igateProvisionFingerprint: string | undefined;
  private igateTransportSession: AprsVerifiedTransportSession | undefined;

  constructor(private readonly options: AprsGatewayRuntimeOptions) {
    this.clock = options.clock ?? (() => new Date());
    this.flushIntervalMs = positiveInterval(options.flushIntervalMs ?? 5_000);
    this.igateTickIntervalMs = positiveInterval(
      options.igateTickIntervalMs ?? DEFAULT_IGATE_TICK_INTERVAL_MS,
    );
    this.igateRetryInitialMs = positiveInterval(
      options.igateRetryInitialMs ?? DEFAULT_IGATE_RETRY_INITIAL_MS,
    );
    this.igateRetryMaximumMs = positiveInterval(
      options.igateRetryMaximumMs ?? DEFAULT_IGATE_RETRY_MAXIMUM_MS,
    );
    if (this.igateRetryMaximumMs < this.igateRetryInitialMs) {
      throw new AprsGatewayRuntimeError();
    }
    this.monitorRefreshIntervalMs = positiveInterval(
      options.monitorRefreshIntervalMs ?? 60_000,
    );
    this.monitorActivityTimeoutMs = positiveInterval(
      options.monitorActivityTimeoutMs ?? DEFAULT_MONITOR_ACTIVITY_TIMEOUT_MS,
    );
    if (this.monitorActivityTimeoutMs < this.monitorRefreshIntervalMs) {
      throw new AprsGatewayRuntimeError();
    }
    this.igateRepository = new AprsIgateRepository(options.database.connection);
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
    this.flushPendingUntilMonitor = true;
    this.igatePendingUntilMonitor = this.options.stationTransport !== undefined;
    void this.refreshMonitor();
    this.flushTimer = setInterval(
      () => void this.flushNow(),
      this.flushIntervalMs,
    );
    this.igateTimer = setInterval(
      () => void this.igateNow(),
      this.igateTickIntervalMs,
    );
    this.monitorTimer = setInterval(
      () => void this.refreshMonitor(),
      this.monitorRefreshIntervalMs,
    );
    this.flushTimer.unref();
    this.igateTimer.unref();
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
    this.fenceTransmitters(false);
    this.flushPendingUntilMonitor = false;
    this.igatePendingUntilMonitor = false;
    this.started = false;
    this.restartAfterStop = false;
    this.monitorRefreshQueued = false;
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = undefined;
    }
    if (this.igateTimer) {
      clearInterval(this.igateTimer);
      this.igateTimer = undefined;
    }
    if (this.monitorTimer) {
      clearInterval(this.monitorTimer);
      this.monitorTimer = undefined;
    }
    this.clearMonitorReconnectTimer();
    const session = this.monitorSession;
    this.monitor = undefined;
    this.monitorSession = undefined;
    this.monitorToken = undefined;
    this.monitorConfigurationKey = undefined;
    this.monitorMappingsFingerprint = undefined;
    this.monitorNextMappingEffectiveAt = undefined;
    this.monitorLastActivityAt = undefined;
    this.monitorStatus = "stopped";
    this.mappedCallsigns = 0;

    const closeSession = session ? closeMonitorSession(session) : undefined;
    const operations = [
      ...(this.flushOperation ? [this.flushOperation] : []),
      ...(this.igateOperation ? [this.igateOperation] : []),
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
        "SELECT delivery_status, COUNT(*) AS count FROM aprs_outbox WHERE delivery_status IN ('queued', 'sending', 'submitted', 'failed', 'observation_expired') GROUP BY delivery_status",
      )
      .all() as Array<{
      delivery_status:
        "queued" | "sending" | "submitted" | "failed" | "observation_expired";
      count: number;
    }>;
    const count = (statuses: readonly string[]) =>
      counts
        .filter((entry) => statuses.includes(entry.delivery_status))
        .reduce((total, entry) => total + entry.count, 0);
    const state = this.readState();
    const stationCounts = this.igateRepository.deliveryCounts(
      state?.provisionFingerprint,
    );
    return {
      configured: true,
      running: this.started,
      monitorStatus: this.monitorStatus,
      mappedCallsigns: this.mappedCallsigns,
      pendingOutbox: count(["queued", "sending", "submitted"]),
      failedOutbox: count(["failed"]),
      unconfirmedOutbox: count(["observation_expired"]),
      pendingStationSubmissions: stationCounts.pending,
      failedStationSubmissions: stationCounts.uncertain,
      unconfirmedStationSubmissions: stationCounts.unconfirmed,
      ...(this.monitorLastActivityAt !== undefined &&
      Number.isFinite(this.monitorLastActivityAt)
        ? {
            monitorLastActivityAt: new Date(
              this.monitorLastActivityAt,
            ).toISOString(),
          }
        : {}),
      ...(this.lastErrorCode ? { lastErrorCode: this.lastErrorCode } : {}),
    };
  }

  listStationSubmissions(limit = 200) {
    return this.igateRepository.listPublic(limit);
  }

  flushNow(): Promise<void> {
    if (this.lifecycleStopped) {
      return Promise.resolve();
    }
    if (!this.canTransmit()) {
      this.flushPendingUntilMonitor = true;
      const refresh = this.refreshStaleMonitorForTransmission();
      if (refresh) {
        return refresh.then(() => this.flushOperation ?? Promise.resolve());
      }
      return this.flushOperation ?? Promise.resolve();
    }
    if (this.flushOperation) {
      return this.flushOperation;
    }
    this.flushPendingUntilMonitor = false;
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
      if (this.options.stateProvider && !this.readState()) {
        this.lastErrorCode = "APRS_PROVISION_UNAVAILABLE";
      }
      const entries = await this.options.outbox.flush(undefined, () =>
        this.canTransmit(generation),
      );
      if (!this.isGenerationActive(generation)) {
        return;
      }
      for (const entry of entries) {
        const submitted = ["submitted", "observer_confirmed"].includes(
          entry.deliveryStatus,
        );
        if (submitted) {
          this.ensureIgateFamily()?.family.recordTrackerForward(
            Date.parse(entry.submittedAt ?? entry.updatedAt),
          );
        }
        this.publish(
          submitted ? "aprs.outbox.submitted" : "aprs.outbox.failed",
          {
            outboxId: entry.id,
            canonicalEventId: entry.canonicalEventId,
            callsign: entry.callsign,
            status: entry.status,
            attempts: entry.attempts,
            ...(entry.lastErrorCode ? { code: entry.lastErrorCode } : {}),
          },
        );
        if (entry.deliveryStatus === "observer_confirmed") {
          this.publish("aprs.outbox.observer_confirmed", {
            outboxId: entry.id,
            canonicalEventId: entry.canonicalEventId,
            callsign: entry.callsign,
            status: entry.deliveryStatus,
          });
        }
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

  recordDecodedSummary(
    type: string,
    timestampMs = this.clock().getTime(),
  ): void {
    try {
      this.ensureIgateFamily()?.family.recordDecodedSummary(type, timestampMs);
    } catch (error) {
      this.lastErrorCode = stableErrorCode(error, "APRS_IGATE_COUNTER_FAILED");
    }
  }

  igateNow(): Promise<void> {
    if (this.lifecycleStopped || !this.options.stationTransport) {
      return Promise.resolve();
    }
    if (!this.canTransmit()) {
      this.igatePendingUntilMonitor = true;
      const refresh = this.refreshStaleMonitorForTransmission();
      if (refresh) {
        return refresh.then(() => this.igateOperation ?? Promise.resolve());
      }
      return this.igateOperation ?? Promise.resolve();
    }
    if (this.clock().getTime() < this.igateRetryNotBefore) {
      return Promise.resolve();
    }
    if (this.igateOperation) {
      return this.igateOperation;
    }
    this.igatePendingUntilMonitor = false;
    const generation = this.lifecycleGeneration;
    const operation = this.runIgate(generation);
    this.igateOperation = operation;
    void operation.then(
      () => this.completeIgate(operation),
      () => this.completeIgate(operation),
    );
    return operation;
  }

  private async runIgate(generation: number): Promise<void> {
    const now = this.clock();
    const nowIso = now.toISOString();
    try {
      const active = this.ensureIgateFamily();
      if (
        !active ||
        !this.options.stationTransport ||
        !this.canTransmit(generation)
      ) {
        return;
      }
      const { family, state } = active;
      this.igateRepository.expireActive(nowIso, state.provisionFingerprint);
      this.igateRepository.recoverInterrupted(
        state.provisionFingerprint,
        nowIso,
      );
      const transportSession =
        await this.options.stationTransport.prepareVerifiedSession?.(
          state.provisionFingerprint,
        );
      if (
        transportSession &&
        this.igateTransportSession &&
        transportSession.generation !== this.igateTransportSession.generation
      ) {
        family.onDisconnected();
      }
      if (transportSession) {
        this.igateTransportSession = transportSession;
      }
      let writeFailed = false;
      let batchFenced = false;
      const outcomes = await family.onVerifiedLogin(
        now.getTime(),
        async (packet: AprsIgatePacket) => {
          if (writeFailed || batchFenced) {
            return false;
          }
          if (!this.canTransmit(generation)) {
            batchFenced = true;
            return false;
          }
          let submissionId: string | undefined;
          let completedSubmission: AprsIgateSubmission | undefined;
          let intent:
            ReturnType<AprsIgateRepository["beginTransmission"]> | undefined;
          try {
            const attemptedAt = this.clock().toISOString();
            intent = this.igateRepository.beginTransmission(
              packet,
              state.provisionFingerprint,
              attemptedAt,
            );
            if (!intent.writeRequired) {
              return true;
            }
            submissionId = intent.submission.id;
            await this.options.stationTransport!.send(
              packet.data,
              state.provisionFingerprint,
              transportSession,
              () => this.canTransmit(generation),
            );
            completedSubmission = intent.repeatReservationAt
              ? this.igateRepository.markRepeatedSubmitted(
                  submissionId,
                  intent.repeatReservationAt,
                  this.clock().toISOString(),
                )
              : this.igateRepository.markSubmitted(
                  submissionId,
                  this.clock().toISOString(),
                );
          } catch (error) {
            let failure = error;
            if (
              error instanceof AprsTransmissionFencedError &&
              submissionId &&
              intent?.created
            ) {
              try {
                this.igateRepository.cancelUnwritten(submissionId);
                batchFenced = true;
                return false;
              } catch (persistenceError) {
                failure = persistenceError;
              }
            }
            if (submissionId && intent?.created) {
              try {
                this.igateRepository.markTransmissionUncertain(
                  submissionId,
                  this.clock().toISOString(),
                );
              } catch (persistenceError) {
                failure = persistenceError;
              }
            }
            writeFailed = true;
            this.lastErrorCode = stableErrorCode(
              failure,
              "APRS_IGATE_TX_FAILED",
            );
            return false;
          }
          if (this.isGenerationActive(generation)) {
            this.publish("aprs.igate.submitted", {
              kind: packet.kind,
              ...(intent?.repeatReservationAt ? { periodic: true } : {}),
            });
            if (completedSubmission?.deliveryStatus === "observer_confirmed") {
              this.publish("aprs.igate.observer_confirmed", {
                submissionId: completedSubmission.id,
                kind: completedSubmission.packetKind,
              });
            }
          }
          return true;
        },
      );
      const unsuccessful = outcomes.some((outcome) => !outcome.successful);
      if (writeFailed || batchFenced || unsuccessful) {
        family.onDisconnected();
        if (writeFailed) {
          this.scheduleIgateRetry();
          if (this.isGenerationActive(generation)) {
            this.publish("aprs.igate.error", {
              code: this.lastErrorCode ?? "APRS_IGATE_TX_FAILED",
            });
          }
        }
      } else if (outcomes.some((outcome) => outcome.successful)) {
        this.resetIgateRetry();
      }
    } catch (error) {
      if (!this.isGenerationActive(generation)) {
        return;
      }
      this.igateFamily?.onDisconnected();
      this.scheduleIgateRetry();
      this.lastErrorCode = stableErrorCode(error, "APRS_IGATE_RUNTIME_FAILED");
      this.publish("aprs.igate.error", { code: this.lastErrorCode });
    }
  }

  refreshMonitor(): Promise<void> {
    if (this.lifecycleStopped) {
      return Promise.resolve();
    }
    if (this.monitorRefreshOperation) {
      this.monitorRefreshQueued = true;
      return this.monitorRefreshOperation;
    }
    const generation = this.lifecycleGeneration;
    const operation = this.runMonitorRefreshLoop(generation);
    this.monitorRefreshOperation = operation;
    void operation.then(
      () => this.completeMonitorRefresh(operation),
      () => this.completeMonitorRefresh(operation),
    );
    return operation;
  }

  private async runMonitorRefreshLoop(generation: number): Promise<void> {
    do {
      this.monitorRefreshQueued = false;
      await this.runMonitorRefresh(generation);
    } while (this.monitorRefreshQueued && this.isGenerationActive(generation));
  }

  private async runMonitorRefresh(generation: number): Promise<void> {
    let pendingSession: AprsIsRxSession | undefined;
    let preserveIgateCadence = false;
    try {
      const earlyPlan = this.createMonitorPlan();
      if (
        earlyPlan &&
        earlyPlan.targets.length > 0 &&
        this.monitorSession &&
        this.monitorToken &&
        this.monitor &&
        this.monitorConfigurationKey === earlyPlan.connectionKey
      ) {
        this.applyMonitorPlan(this.monitor, earlyPlan);
        if (this.monitorStatus !== "connected") {
          return;
        }
        if (this.hasFreshMonitorActivity()) {
          this.lastErrorCode = undefined;
          this.activateTransmitters(generation, this.monitorToken);
          return;
        }
        this.fenceTransmitters(true, false);
        preserveIgateCadence = true;
        this.monitorStatus = "error";
        this.lastErrorCode = "APRS_MONITOR_ACTIVITY_TIMEOUT";
        this.publish("aprs.monitor.error", { code: this.lastErrorCode });
      }
      this.fenceTransmitters(true, !preserveIgateCadence);
      this.monitorStatus = "connecting";
      const previous = this.monitorSession;
      this.monitor = undefined;
      this.monitorSession = undefined;
      this.monitorToken = undefined;
      this.monitorConfigurationKey = undefined;
      this.monitorMappingsFingerprint = undefined;
      this.monitorNextMappingEffectiveAt = undefined;
      this.monitorLastActivityAt = undefined;
      if (previous) {
        await closeMonitorSession(previous);
      }
      if (!this.isGenerationActive(generation)) {
        return;
      }
      const plan = this.createMonitorPlan();
      if (!plan) {
        this.setProvisionUnavailable();
        return;
      }
      this.mappedCallsigns = plan.mappedCallsigns;
      if (plan.targets.length === 0) {
        this.monitorStatus = "idle";
        this.lastErrorCode = undefined;
        this.publish("aprs.monitor.idle", { mappedCallsigns: 0 });
        return;
      }
      const monitor = new AprsIsMonitor(
        plan.targets,
        new AprsRemoteHighWaterStore(this.options.database.connection),
      );
      const client = this.options.monitorClientFactory(
        monitor.filterExpression(),
        plan.state?.provisionFingerprint,
      );
      const token = {};
      this.monitorToken = token;
      this.monitorMappingsFingerprint = plan.mappingsFingerprint;
      this.monitorNextMappingEffectiveAt = plan.nextMappingEffectiveAt;
      let callbackFailed = false;
      const onLineError = (error: unknown): void => {
        if (
          !this.isGenerationActive(generation) ||
          this.monitorToken !== token
        ) {
          return;
        }
        callbackFailed = true;
        this.fenceTransmitters(true);
        this.monitorStatus = "error";
        this.monitorLastActivityAt = undefined;
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
      const session = await client.connect(
        (line) => {
          if (
            !this.isGenerationActive(generation) ||
            this.monitorToken !== token
          ) {
            return;
          }
          this.recordMonitorActivity(generation, token);
          let observationState = this.readState();
          if (
            this.options.stateProvider &&
            (!observationState ||
              observationState.provisionFingerprint !==
                plan.state?.provisionFingerprint)
          ) {
            this.invalidateMonitorState(generation, token);
            return;
          }
          try {
            const receivedAtDate = this.clock();
            if (
              (observationState &&
                observationState.mappingsFingerprint !==
                  this.monitorMappingsFingerprint) ||
              (this.monitorNextMappingEffectiveAt !== undefined &&
                receivedAtDate.getTime() >= this.monitorNextMappingEffectiveAt)
            ) {
              const currentPlan = this.createMonitorPlan(
                observationState,
                receivedAtDate,
              );
              if (
                !currentPlan ||
                currentPlan.connectionKey !== plan.connectionKey
              ) {
                this.invalidateMonitorState(generation, token);
                return;
              }
              this.applyMonitorPlan(monitor, currentPlan);
              observationState = currentPlan.state;
            }
            const receivedAt = receivedAtDate.toISOString();
            const result = monitor.observeLine(line, receivedAt);
            const confirmed =
              result.remote && observationState
                ? this.options.database.aprsOutbox.confirmObserved(
                    result.remote.callsign,
                    result.remote.destination,
                    result.remote.info,
                    receivedAt,
                    observationState.provisionFingerprint,
                  )
                : [];
            const stationConfirmed =
              result.remote && observationState
                ? this.igateRepository.confirmObserved(
                    observationState.provisionFingerprint,
                    result.remote.callsign,
                    result.remote.destination,
                    result.remote.info,
                    receivedAt,
                  )
                : [];
            if (result.kind !== "ignored") {
              this.publish("aprs.monitor.observed", {
                kind: result.kind,
                ...(result.remote
                  ? {
                      packetDigest: result.remote.infoDigest,
                    }
                  : {}),
              });
            }
            for (const entry of confirmed) {
              this.publish("aprs.outbox.observer_confirmed", {
                outboxId: entry.id,
                canonicalEventId: entry.canonicalEventId,
                callsign: entry.callsign,
                status: entry.deliveryStatus,
              });
            }
            for (const entry of stationConfirmed) {
              this.publish("aprs.igate.observer_confirmed", {
                submissionId: entry.id,
                kind: entry.packetKind,
              });
            }
            if (
              callbackFailed &&
              result.kind !== "ignored" &&
              (!this.options.stateProvider ||
                observationState?.provisionFingerprint ===
                  plan.state?.provisionFingerprint)
            ) {
              callbackFailed = false;
              this.monitorStatus = "connected";
              this.lastErrorCode = undefined;
              this.publish("aprs.monitor.connected", {
                mappedCallsigns: this.mappedCallsigns,
              });
              this.activateTransmitters(generation, token);
            }
          } catch (error) {
            onLineError(error);
          }
        },
        onLineError,
        () => this.recordMonitorActivity(generation, token),
      );
      pendingSession = session;
      if (!this.isGenerationActive(generation) || this.monitorToken !== token) {
        await closeMonitorSession(session);
        pendingSession = undefined;
        return;
      }
      const currentPlan = this.createMonitorPlan();
      if (
        !currentPlan ||
        currentPlan.connectionKey !== plan.connectionKey ||
        currentPlan.targets.length === 0
      ) {
        this.monitorToken = undefined;
        this.monitorMappingsFingerprint = undefined;
        this.monitorNextMappingEffectiveAt = undefined;
        this.monitorLastActivityAt = undefined;
        await closeMonitorSession(session);
        pendingSession = undefined;
        if (this.isGenerationActive(generation)) {
          if (!currentPlan) {
            this.setProvisionUnavailable();
          } else if (currentPlan.targets.length === 0) {
            this.monitorStatus = "idle";
            this.mappedCallsigns = 0;
            this.lastErrorCode = undefined;
            this.publish("aprs.monitor.idle", { mappedCallsigns: 0 });
          } else {
            this.monitorRefreshQueued = true;
          }
        }
        return;
      }
      this.applyMonitorPlan(monitor, currentPlan);
      let reconciledOutbox: AprsOutboxEntry[] = [];
      let reconciledIgate: ReturnType<
        AprsIgateRepository["reconcileObserved"]
      > = [];
      if (currentPlan.state) {
        reconciledOutbox =
          this.options.database.aprsOutbox.reconcileObservedCache(
            currentPlan.state.provisionFingerprint,
          );
        reconciledIgate = this.igateRepository.reconcileObserved(
          currentPlan.state.provisionFingerprint,
          this.clock().toISOString(),
        );
      }
      this.monitor = monitor;
      this.monitorSession = session;
      pendingSession = undefined;
      this.monitorConfigurationKey = currentPlan.connectionKey;
      this.clearMonitorReconnectTimer();
      this.watchMonitorTermination(session, generation, token);
      if (callbackFailed) {
        return;
      }
      this.monitorStatus = "connected";
      this.recordMonitorActivity(generation, token);
      this.lastErrorCode = undefined;
      for (const entry of reconciledOutbox) {
        this.publish("aprs.outbox.observer_confirmed", {
          outboxId: entry.id,
          canonicalEventId: entry.canonicalEventId,
          callsign: entry.callsign,
          status: entry.deliveryStatus,
        });
      }
      for (const entry of reconciledIgate) {
        this.publish("aprs.igate.observer_confirmed", {
          submissionId: entry.id,
          kind: entry.packetKind,
        });
      }
      this.publish("aprs.monitor.connected", {
        mappedCallsigns: this.mappedCallsigns,
      });
      this.activateTransmitters(generation, token);
    } catch (error) {
      if (!this.isGenerationActive(generation)) {
        if (error instanceof AprsMonitorSessionCloseError) {
          throw error;
        }
        return;
      }
      this.fenceTransmitters(true);
      const activeSession = this.monitorSession ?? pendingSession;
      this.monitor = undefined;
      this.monitorSession = undefined;
      this.monitorToken = undefined;
      this.monitorConfigurationKey = undefined;
      this.monitorMappingsFingerprint = undefined;
      this.monitorNextMappingEffectiveAt = undefined;
      this.monitorLastActivityAt = undefined;
      let failure = error;
      if (activeSession) {
        try {
          await closeMonitorSession(activeSession);
        } catch (closeError) {
          failure = closeError;
        }
      }
      if (this.options.stateProvider && !this.readState()) {
        this.setProvisionUnavailable();
        return;
      }
      this.monitorStatus = "error";
      if (failure instanceof AprsMappingConflictError) {
        this.mappedCallsigns = failure.mappedCallsigns;
      }
      this.lastErrorCode = stableErrorCode(
        failure,
        "APRS_MONITOR_CONNECT_FAILED",
      );
      this.publish("aprs.monitor.error", {
        code: this.lastErrorCode,
      });
    }
  }

  private createMonitorPlan(
    state = this.readState(),
    now = this.clock(),
  ): AprsMonitorPlan | undefined {
    if (this.options.stateProvider && !state) {
      return undefined;
    }
    const mappings =
      state?.mappings ?? this.options.database.callmeshMappings.list();
    const mappingTargets = activeTargets(mappings, now);
    if (
      new Set(mappingTargets.map((target) => target.callsign)).size !==
      mappingTargets.length
    ) {
      throw new AprsMappingConflictError(mappingTargets.length);
    }
    const targets = appendStationTarget(mappingTargets, state);
    const observerIdentity = state
      ? deriveAprsObserverCallsign(state)
      : "unscoped";
    return {
      connectionKey: `${state?.provisionFingerprint ?? "unscoped"}:${observerIdentity}:${APRS_RX_FILTER_EXPRESSION}`,
      mappedCallsigns: mappingTargets.length,
      mappingsFingerprint:
        state?.mappingsFingerprint ?? JSON.stringify(mappingTargets),
      nextMappingEffectiveAt: nextMappingEffectiveAt(mappings, now),
      state,
      targets,
    };
  }

  private applyMonitorPlan(
    monitor: AprsIsMonitor,
    plan: AprsMonitorPlan,
  ): void {
    monitor.replaceTargets(plan.targets);
    this.mappedCallsigns = plan.mappedCallsigns;
    this.monitorMappingsFingerprint = plan.mappingsFingerprint;
    this.monitorNextMappingEffectiveAt = plan.nextMappingEffectiveAt;
  }

  private async finishStop(operations: Promise<void>[]): Promise<void> {
    const results = await Promise.allSettled(operations);
    let closeFailed = false;
    let closeFailure: unknown;
    try {
      await this.options.outbox.close?.();
    } catch (error) {
      closeFailed = true;
      closeFailure = error;
    }
    this.monitorStatus = "stopped";
    this.mappedCallsigns = 0;
    const failure = results.find(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );
    if (failure) {
      throw failure.reason;
    }
    if (closeFailed) {
      throw closeFailure;
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

  private completeIgate(operation: Promise<void>): void {
    if (this.igateOperation === operation) {
      this.igateOperation = undefined;
    }
  }

  private completeMonitorRefresh(operation: Promise<void>): void {
    if (this.monitorRefreshOperation === operation) {
      this.monitorRefreshOperation = undefined;
    }
  }

  private readState(): AprsRuntimeState | undefined {
    try {
      return this.options.stateProvider?.();
    } catch {
      return undefined;
    }
  }

  private ensureIgateFamily():
    { family: AprsIgateFamily; state: AprsRuntimeState } | undefined {
    const state = this.readState();
    if (!state) {
      this.igateFamily?.onDisconnected();
      this.igateFamily = undefined;
      this.igateProvisionFingerprint = undefined;
      this.igateTransportSession = undefined;
      this.resetIgateRetry();
      return undefined;
    }
    if (
      !this.igateFamily ||
      this.igateProvisionFingerprint !== state.provisionFingerprint
    ) {
      this.igateFamily?.onDisconnected();
      const restoredSequence = this.igateRepository.loadLastSuccessfulSequence(
        state.identity.callsign,
      );
      this.igateFamily = new AprsIgateFamily({
        provision: state.provision,
        version: this.options.version ?? "2.0.0",
        lastSuccessfulTelemetrySequence: restoredSequence,
      });
      this.igateProvisionFingerprint = state.provisionFingerprint;
      this.igateTransportSession = undefined;
      this.resetIgateRetry();
    }
    return { family: this.igateFamily, state };
  }

  private invalidateMonitorState(generation: number, token: object): void {
    if (!this.isGenerationActive(generation) || this.monitorToken !== token) {
      return;
    }
    this.fenceTransmitters(true);
    this.monitor = undefined;
    this.monitorToken = undefined;
    this.monitorConfigurationKey = undefined;
    this.monitorMappingsFingerprint = undefined;
    this.monitorNextMappingEffectiveAt = undefined;
    this.monitorLastActivityAt = undefined;
    this.monitorStatus = "idle";
    this.mappedCallsigns = 0;
    this.lastErrorCode = "APRS_PROVISION_UNAVAILABLE";
    try {
      this.publish("aprs.monitor.idle", {
        code: this.lastErrorCode,
        mappedCallsigns: 0,
      });
    } catch {
      // This method runs inside a socket EventEmitter callback.
    }
    try {
      void this.refreshMonitor().catch(() => undefined);
    } catch {
      // Keep authorization invalidation contained to the socket callback.
    }
  }

  private watchMonitorTermination(
    session: AprsIsRxSession,
    generation: number,
    token: object,
  ): void {
    if (!session.terminated) {
      return;
    }
    void session.terminated.then(
      () => this.handleMonitorTermination(session, generation, token),
      () => this.handleMonitorTermination(session, generation, token),
    );
  }

  private handleMonitorTermination(
    session: AprsIsRxSession,
    generation: number,
    token: object,
  ): void {
    if (
      !this.isGenerationActive(generation) ||
      this.monitorToken !== token ||
      this.monitorSession !== session
    ) {
      return;
    }
    this.fenceTransmitters(true);
    this.monitor = undefined;
    this.monitorSession = undefined;
    this.monitorToken = undefined;
    this.monitorConfigurationKey = undefined;
    this.monitorMappingsFingerprint = undefined;
    this.monitorNextMappingEffectiveAt = undefined;
    this.monitorLastActivityAt = undefined;
    this.monitorStatus = "error";
    this.lastErrorCode = "APRS_MONITOR_CONNECTION_LOST";
    try {
      this.publish("aprs.monitor.error", { code: this.lastErrorCode });
    } catch {
      // This callback runs from a socket lifecycle promise.
    }
    this.scheduleMonitorReconnect();
  }

  private scheduleMonitorReconnect(): void {
    if (this.lifecycleStopped || this.monitorReconnectTimer) {
      return;
    }
    this.monitorReconnectTimer = setTimeout(() => {
      this.monitorReconnectTimer = undefined;
      try {
        void this.refreshMonitor().catch(() => undefined);
      } catch {
        // Keep reconnect scheduling contained to the timer callback.
      }
    }, this.monitorRefreshIntervalMs);
    this.monitorReconnectTimer.unref();
  }

  private clearMonitorReconnectTimer(): void {
    if (!this.monitorReconnectTimer) {
      return;
    }
    clearTimeout(this.monitorReconnectTimer);
    this.monitorReconnectTimer = undefined;
  }

  private setProvisionUnavailable(): void {
    this.fenceTransmitters(true);
    this.monitor = undefined;
    this.monitorSession = undefined;
    this.monitorToken = undefined;
    this.monitorConfigurationKey = undefined;
    this.monitorMappingsFingerprint = undefined;
    this.monitorNextMappingEffectiveAt = undefined;
    this.monitorLastActivityAt = undefined;
    this.monitorStatus = "idle";
    this.mappedCallsigns = 0;
    this.lastErrorCode = "APRS_PROVISION_UNAVAILABLE";
    this.publish("aprs.monitor.idle", {
      code: this.lastErrorCode,
      mappedCallsigns: 0,
    });
  }

  private isGenerationActive(generation: number): boolean {
    return !this.lifecycleStopped && this.lifecycleGeneration === generation;
  }

  private canTransmit(generation = this.lifecycleGeneration): boolean {
    return (
      this.isGenerationActive(generation) &&
      this.txReady &&
      this.monitorStatus === "connected" &&
      this.monitorSession !== undefined &&
      this.monitorToken !== undefined &&
      this.hasFreshMonitorActivity()
    );
  }

  private fenceTransmitters(queueWork: boolean, disconnectIgate = true): void {
    this.txReady = false;
    if (disconnectIgate) {
      this.igateFamily?.onDisconnected();
    }
    if (queueWork) {
      this.flushPendingUntilMonitor = true;
      this.igatePendingUntilMonitor =
        this.options.stationTransport !== undefined;
    }
  }

  private activateTransmitters(generation: number, token: object): void {
    if (
      !this.isGenerationActive(generation) ||
      this.monitorToken !== token ||
      !this.monitorSession ||
      this.monitorStatus !== "connected" ||
      !this.hasFreshMonitorActivity()
    ) {
      return;
    }

    this.txReady = true;
    const flushPending = this.flushPendingUntilMonitor;
    const igatePending = this.igatePendingUntilMonitor;
    this.flushPendingUntilMonitor = false;
    this.igatePendingUntilMonitor = false;
    if (flushPending) {
      void this.flushNow();
    }
    if (igatePending) {
      void this.igateNow();
    }
  }

  private scheduleIgateRetry(): void {
    this.igateRetryFailures += 1;
    const exponent = Math.min(this.igateRetryFailures - 1, 30);
    const delay = Math.min(
      this.igateRetryMaximumMs,
      this.igateRetryInitialMs * 2 ** exponent,
    );
    this.igateRetryNotBefore = this.clock().getTime() + delay;
  }

  private resetIgateRetry(): void {
    this.igateRetryFailures = 0;
    this.igateRetryNotBefore = 0;
  }

  private refreshStaleMonitorForTransmission(): Promise<void> | undefined {
    if (this.monitorStatus === "connected" && !this.hasFreshMonitorActivity()) {
      return this.refreshMonitor();
    }
    return undefined;
  }

  private recordMonitorActivity(generation: number, token: object): void {
    if (!this.isGenerationActive(generation) || this.monitorToken !== token) {
      return;
    }
    const activityAt = this.clock().getTime();
    if (Number.isFinite(activityAt)) {
      this.monitorLastActivityAt = activityAt;
    }
  }

  private hasFreshMonitorActivity(now = this.clock().getTime()): boolean {
    return (
      this.monitorLastActivityAt !== undefined &&
      Number.isFinite(this.monitorLastActivityAt) &&
      Number.isFinite(now) &&
      now >= this.monitorLastActivityAt &&
      now - this.monitorLastActivityAt < this.monitorActivityTimeoutMs
    );
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

function nextMappingEffectiveAt(
  mappings: readonly CallMeshMapping[],
  now: Date,
): number | undefined {
  const nowMs = now.getTime();
  let next: number | undefined;
  for (const mapping of mappings) {
    const effectiveAt = Date.parse(mapping.effectiveAt);
    if (effectiveAt > nowMs && (next === undefined || effectiveAt < next)) {
      next = effectiveAt;
    }
  }
  return next;
}

function appendStationTarget(
  targets: readonly AprsMonitorTarget[],
  state: AprsRuntimeState | undefined,
): AprsMonitorTarget[] {
  if (!state) {
    return targets.map((target) => ({ ...target }));
  }
  if (targets.some((target) => target.callsign === state.identity.callsign)) {
    return targets.map((target) => ({ ...target }));
  }
  return [...targets, stationTarget(state)].sort((left, right) =>
    left.callsign.localeCompare(right.callsign),
  );
}

function stationTarget(state: AprsRuntimeState): AprsMonitorTarget {
  return {
    callsign: state.identity.callsign,
    mappingVersion: `provision:${state.provisionFingerprint}`,
    meshNetworkId: "__cmclient_station__",
    nodeNum: 0,
  };
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
