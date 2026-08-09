import { createHash } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";

import type {
  AprsRuntimeStatus,
  CallMeshProvision,
  CmCloudDirectAprsBeaconState,
  CmCloudDirectAprsCapabilityState,
  CmCloudDirectAprsProfileState,
} from "@cmclient/contracts";

import {
  AprsIgateFamily,
  AprsIgateRepository,
  type AprsIgatePacket,
} from "./aprs-igate.js";
import {
  parseCmCloudDirectAprsCapability,
  type CmCloudDirectAprsCapability,
  type CmCloudDirectAprsEgress,
} from "./cmcloud-aprs.js";
import type { DomainEventBus } from "./events.js";

const DEFAULT_TICK_INTERVAL_MS = 10_000;

export interface CmCloudDirectAprsIgateStatus {
  readonly configured: boolean;
  readonly running: boolean;
  readonly capabilityState: CmCloudDirectAprsCapabilityState;
  readonly profileState: CmCloudDirectAprsProfileState;
  readonly directAprsReady: boolean;
  readonly beaconState: CmCloudDirectAprsBeaconState;
  readonly lastErrorCode?: string;
}

export interface CmCloudDirectAprsIgateOptions {
  readonly database: DatabaseSync;
  readonly egress: CmCloudDirectAprsEgress;
  readonly version: string;
  readonly eventBus?: DomainEventBus;
  readonly clock?: () => Date;
  readonly beaconIntervalMs?: number;
  readonly tickIntervalMs?: number;
}

/**
 * Schedules CMCloud-authorized station self-identification over the existing
 * verified direct APRS-IS egress. It deliberately owns no APRS socket itself.
 */
export class CmCloudDirectAprsIgateRuntime {
  private readonly clock: () => Date;
  private readonly tickIntervalMs: number;
  private readonly repository: AprsIgateRepository;
  private family: AprsIgateFamily | undefined;
  private provisionFingerprint: string | undefined;
  private generation = 0;
  private running = false;
  private capabilityState: CmCloudDirectAprsCapabilityState = "not_granted";
  private profileState: CmCloudDirectAprsIgateStatus["profileState"] =
    "missing";
  private beaconActive = false;
  private lastErrorCode: string | undefined;
  private timer: NodeJS.Timeout | undefined;
  private tickOperation: Promise<void> | undefined;

  constructor(private readonly options: CmCloudDirectAprsIgateOptions) {
    if (
      typeof options.version !== "string" ||
      !options.version.trim() ||
      !Number.isInteger(options.tickIntervalMs ?? DEFAULT_TICK_INTERVAL_MS) ||
      (options.tickIntervalMs ?? DEFAULT_TICK_INTERVAL_MS) < 100
    ) {
      throw new CmCloudDirectAprsIgateError(
        "CMCLOUD_DIRECT_APRS_IGATE_CONFIGURATION_INVALID",
      );
    }
    this.clock = options.clock ?? (() => new Date());
    this.tickIntervalMs = options.tickIntervalMs ?? DEFAULT_TICK_INTERVAL_MS;
    this.repository = new AprsIgateRepository(options.database);
  }

  /**
   * Fence station telemetry while its CMCloud session or APRS-IS egress is
   * changing. The family stays in memory so a transient CMCloud reconnect can
   * resume the same rolling telemetry window without attributing it elsewhere.
   */
  suspend(): void {
    this.generation += 1;
    this.clearTimer();
    this.family?.onDisconnected();
    this.running = false;
    this.beaconActive = false;
  }

  async configure(capability?: CmCloudDirectAprsCapability): Promise<void> {
    let normalized: CmCloudDirectAprsCapability | undefined;
    try {
      normalized = capability
        ? parseCmCloudDirectAprsCapability(capability)
        : undefined;
    } catch {
      this.clearConfiguration(
        capability ? "granted" : "not_granted",
        capability ? "invalid" : "missing",
        "CMCLOUD_DIRECT_APRS_BEACON_PROVISION_INVALID",
      );
      return;
    }
    if (!normalized?.provision) {
      this.clearConfiguration(
        normalized ? "granted" : "not_granted",
        "missing",
      );
      return;
    }
    try {
      const provision = normalized.provision;
      const nextProvisionFingerprint = provisionFingerprint(
        normalized.callsign,
        provision,
      );
      const existingFamily = this.family;
      const reuseExistingFamily =
        existingFamily !== undefined &&
        this.provisionFingerprint === nextProvisionFingerprint;

      this.generation += 1;
      this.clearTimer();
      existingFamily?.onDisconnected();
      this.running = false;
      this.capabilityState = "granted";
      this.profileState = "configured";
      this.beaconActive = false;
      this.lastErrorCode = undefined;
      this.family = reuseExistingFamily
        ? existingFamily
        : new AprsIgateFamily({
            provision,
            version: this.options.version,
            ...(this.options.beaconIntervalMs === undefined
              ? {}
              : { beaconIntervalMs: this.options.beaconIntervalMs }),
            lastSuccessfulTelemetrySequence:
              this.repository.loadLastSuccessfulSequence(normalized.callsign),
          });
      this.provisionFingerprint = nextProvisionFingerprint;
      this.running = true;
      this.timer = setInterval(() => void this.tick(), this.tickIntervalMs);
      this.timer.unref();
      if (this.options.egress.ready()) {
        await this.tick();
      }
    } catch {
      this.clearConfiguration(
        "granted",
        "invalid",
        "CMCLOUD_DIRECT_APRS_BEACON_PROVISION_INVALID",
      );
    }
  }

  onEgressReadinessChanged(): void {
    if (!this.family) return;
    if (!this.options.egress.ready()) {
      this.family.onDisconnected();
      this.beaconActive = false;
      return;
    }
    void this.tick();
  }

  async stop(): Promise<void> {
    this.clearConfiguration("not_granted", "missing");
    await this.tickOperation?.catch(() => undefined);
  }

  status(): CmCloudDirectAprsIgateStatus {
    const directAprsReady = this.options.egress.ready();
    return {
      configured: this.profileState === "configured",
      running: this.running,
      capabilityState: this.capabilityState,
      profileState: this.profileState,
      directAprsReady,
      beaconState: this.beaconState(directAprsReady),
      ...(this.lastErrorCode ? { lastErrorCode: this.lastErrorCode } : {}),
    };
  }

  aprsRuntimeStatus(): AprsRuntimeStatus {
    const directStatus = this.status();
    const directAprs = {
      capabilityState: directStatus.capabilityState,
      profileState: directStatus.profileState,
      directAprsReady: directStatus.directAprsReady,
      beaconState: directStatus.beaconState,
      ...(directStatus.lastErrorCode
        ? { lastErrorCode: directStatus.lastErrorCode }
        : {}),
    };
    const stationCounts = this.provisionFingerprint
      ? this.repository.deliveryCounts(this.provisionFingerprint)
      : { pending: 0, uncertain: 0, unconfirmed: 0 };
    return {
      configured: directStatus.configured,
      running: directStatus.running,
      // CMCloud-only egress has no independent RX observer. A stopped monitor
      // is expected here and must be read with the direct APRS state below.
      monitorStatus: "stopped",
      mappedCallsigns: 0,
      pendingOutbox: 0,
      failedOutbox: 0,
      unconfirmedOutbox: 0,
      pendingStationSubmissions: stationCounts.pending,
      failedStationSubmissions: stationCounts.uncertain,
      unconfirmedStationSubmissions: stationCounts.unconfirmed,
      directAprs,
      ...(directStatus.lastErrorCode
        ? { lastErrorCode: directStatus.lastErrorCode }
        : {}),
    };
  }

  listStationSubmissions(limit = 200) {
    return this.provisionFingerprint
      ? this.repository.listPublicForProvision(this.provisionFingerprint, limit)
      : [];
  }

  recordDecodedSummary(
    type: string,
    timestampMs = this.clock().getTime(),
  ): void {
    try {
      this.family?.recordDecodedSummary(type, timestampMs);
    } catch (error) {
      this.lastErrorCode = stableErrorCode(
        error,
        "CMCLOUD_DIRECT_APRS_COUNTER_FAILED",
      );
    }
  }

  /**
   * A CMCloud `aprs_dispatch` is a Tracker forward, but it bypasses the
   * legacy APRS outbox. Record it here so the shared iGate telemetry window
   * reports the same successful-write boundary as the legacy path.
   */
  recordTrackerForward(timestampMs = this.clock().getTime()): void {
    const family = this.family;
    if (!family) return;
    this.recordTrackerForwardForFamily(family, timestampMs);
  }

  /**
   * Captures the exact station family that owns a dispatch before its APRS
   * socket write begins. A late successful write must not be counted against
   * a different station after CMCloud reconnects or changes profile.
   */
  captureTrackerForwardRecorder():
    ((timestampMs?: number) => void) | undefined {
    const family = this.family;
    if (!family || !this.running) return undefined;
    return (timestampMs = this.clock().getTime()) =>
      this.recordTrackerForwardForFamily(family, timestampMs);
  }

  private recordTrackerForwardForFamily(
    family: AprsIgateFamily,
    timestampMs: number,
  ): void {
    try {
      family.recordTrackerForward(timestampMs);
    } catch (error) {
      this.lastErrorCode = stableErrorCode(
        error,
        "CMCLOUD_DIRECT_APRS_COUNTER_FAILED",
      );
    }
  }

  private clearConfiguration(
    capabilityState: CmCloudDirectAprsCapabilityState,
    profileState: CmCloudDirectAprsIgateStatus["profileState"],
    lastErrorCode?: string,
  ): void {
    this.generation += 1;
    this.clearTimer();
    this.family?.onDisconnected();
    this.family = undefined;
    this.provisionFingerprint = undefined;
    this.running = false;
    this.capabilityState = capabilityState;
    this.profileState = profileState;
    this.beaconActive = false;
    this.lastErrorCode = lastErrorCode;
  }

  tick(): Promise<void> {
    if (!this.running || !this.family || !this.provisionFingerprint) {
      return Promise.resolve();
    }
    if (this.tickOperation) {
      return this.tickOperation;
    }
    const generation = this.generation;
    const operation = this.runTick(generation);
    this.tickOperation = operation;
    void operation.then(
      () => this.completeTick(operation),
      () => this.completeTick(operation),
    );
    return operation;
  }

  private async runTick(generation: number): Promise<void> {
    const family = this.family;
    const provisionFingerprintValue = this.provisionFingerprint;
    if (
      !family ||
      !provisionFingerprintValue ||
      !this.isActive(generation, family, provisionFingerprintValue) ||
      !this.options.egress.ready()
    ) {
      family?.onDisconnected();
      return;
    }
    const now = this.clock();
    if (!Number.isFinite(now.getTime())) {
      this.lastErrorCode = "CMCLOUD_DIRECT_APRS_CLOCK_INVALID";
      family.onDisconnected();
      this.beaconActive = false;
      return;
    }
    try {
      const nowIso = now.toISOString();
      this.repository.expireActive(nowIso, provisionFingerprintValue);
      this.repository.recoverInterrupted(provisionFingerprintValue, nowIso);
      const outcomes = await family.onVerifiedLogin(now.getTime(), (packet) =>
        this.writePacket(packet, generation, family, provisionFingerprintValue),
      );
      if (!this.isActive(generation, family, provisionFingerprintValue)) {
        return;
      }
      if (outcomes.some((outcome) => !outcome.successful)) {
        family.onDisconnected();
        this.beaconActive = false;
      } else if (outcomes.some((outcome) => outcome.successful)) {
        this.lastErrorCode = undefined;
        this.beaconActive = true;
      }
    } catch (error) {
      if (!this.isActive(generation, family, provisionFingerprintValue)) {
        return;
      }
      family.onDisconnected();
      this.beaconActive = false;
      this.lastErrorCode = stableErrorCode(
        error,
        "CMCLOUD_DIRECT_APRS_IGATE_RUNTIME_FAILED",
      );
      this.publish("aprs.igate.error", { code: this.lastErrorCode });
    }
  }

  private async writePacket(
    packet: AprsIgatePacket,
    generation: number,
    family: AprsIgateFamily,
    provisionFingerprintValue: string,
  ): Promise<boolean> {
    if (
      !this.isActive(generation, family, provisionFingerprintValue) ||
      !this.options.egress.ready()
    ) {
      this.lastErrorCode = "CMCLOUD_DIRECT_APRS_NOT_READY";
      return false;
    }
    let intent: ReturnType<AprsIgateRepository["beginTransmission"]>;
    try {
      intent = this.repository.beginTransmission(
        packet,
        provisionFingerprintValue,
        this.clock().toISOString(),
      );
    } catch (error) {
      this.lastErrorCode = stableErrorCode(
        error,
        "CMCLOUD_DIRECT_APRS_IGATE_PERSISTENCE_FAILED",
      );
      return false;
    }
    if (!intent.writeRequired) {
      return true;
    }

    let result;
    try {
      result = await this.options.egress.submit(packet.data);
    } catch {
      result = {
        outcome: "uncertain" as const,
        errorCode: "CMCLOUD_DIRECT_APRS_WRITE_UNCERTAIN",
      };
    }
    if (result.outcome === "submitted") {
      try {
        const submission = intent.repeatReservationAt
          ? this.repository.markRepeatedSubmitted(
              intent.submission.id,
              intent.repeatReservationAt,
              this.clock().toISOString(),
            )
          : this.repository.markSubmitted(
              intent.submission.id,
              this.clock().toISOString(),
            );
        if (this.isActive(generation, family, provisionFingerprintValue)) {
          this.publish("aprs.igate.submitted", {
            kind: packet.kind,
            ...(intent.repeatReservationAt ? { periodic: true } : {}),
          });
          if (submission.deliveryStatus === "observer_confirmed") {
            this.publish("aprs.igate.observer_confirmed", {
              submissionId: submission.id,
              kind: submission.packetKind,
            });
          }
        }
        return true;
      } catch (error) {
        this.lastErrorCode = stableErrorCode(
          error,
          "CMCLOUD_DIRECT_APRS_IGATE_PERSISTENCE_FAILED",
        );
        try {
          this.repository.markTransmissionUncertain(
            intent.submission.id,
            this.clock().toISOString(),
          );
        } catch {
          // The local socket write happened, so the original persistence error
          // remains the useful stable result even if the uncertainty marker races.
        }
        return false;
      }
    }

    this.lastErrorCode = result.errorCode ?? "CMCLOUD_DIRECT_APRS_WRITE_FAILED";
    try {
      if (result.outcome === "retryable_failure") {
        this.repository.cancelUnwritten(intent.submission.id);
      } else {
        this.repository.markTransmissionUncertain(
          intent.submission.id,
          this.clock().toISOString(),
        );
      }
    } catch (error) {
      this.lastErrorCode = stableErrorCode(
        error,
        "CMCLOUD_DIRECT_APRS_IGATE_PERSISTENCE_FAILED",
      );
    }
    return false;
  }

  private completeTick(operation: Promise<void>): void {
    if (this.tickOperation === operation) {
      this.tickOperation = undefined;
    }
  }

  private isActive(
    generation: number,
    family: AprsIgateFamily,
    provisionFingerprintValue: string,
  ): boolean {
    return (
      this.running &&
      this.generation === generation &&
      this.family === family &&
      this.provisionFingerprint === provisionFingerprintValue
    );
  }

  private clearTimer(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = undefined;
  }

  private beaconState(
    directAprsReady: boolean,
  ): CmCloudDirectAprsIgateStatus["beaconState"] {
    if (this.profileState === "missing") return "missing_profile";
    if (this.profileState === "invalid") return "error";
    if (!this.running) return "stopped";
    if (
      this.lastErrorCode &&
      this.lastErrorCode !== "CMCLOUD_DIRECT_APRS_NOT_READY"
    ) {
      return "error";
    }
    if (!directAprsReady) return "waiting_for_aprs_is";
    return this.beaconActive ? "active" : "waiting_for_aprs_is";
  }

  private publish(type: string, payload: Record<string, unknown>): void {
    this.options.eventBus?.publish({ type, source: "gateway", payload });
  }
}

export class CmCloudDirectAprsIgateError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "CmCloudDirectAprsIgateError";
  }
}

function provisionFingerprint(
  callsign: string,
  provision: CallMeshProvision,
): string {
  return createHash("sha256")
    .update(JSON.stringify({ callsign, provision }), "utf8")
    .digest("hex");
}

function stableErrorCode(error: unknown, fallback: string): string {
  if (
    error instanceof Error &&
    "code" in error &&
    typeof error.code === "string" &&
    /^[A-Z][A-Z0-9_]{0,127}$/u.test(error.code)
  ) {
    return error.code;
  }
  return fallback;
}
