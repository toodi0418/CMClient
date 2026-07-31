import { DomainEventBus } from "./events.js";
import { AprsIgateRepository } from "./aprs-igate.js";
import { GatewayDatabase } from "./persistence/database.js";

export const OBSERVATION_RETENTION_HEADROOM = 1_000;
export const MAX_OBSERVATION_RETENTION_BATCH_SIZE = 40_000;

export interface GatewayMaintenanceRuntimeOptions {
  database: GatewayDatabase;
  eventBus: DomainEventBus;
  aprsOutboxRetentionDays?: number;
  aprsOutboxBatchSize?: number;
  cmCloudRawOutboxRetentionDays?: number;
  cmCloudRawOutboxBatchSize?: number;
  jobRetentionDays?: number;
  jobBatchSize?: number;
  messageRetentionDays?: number;
  messageBatchSize?: number;
  positionRetentionDays?: number;
  positionBatchSize?: number;
  observationBatchSize?: number;
  retentionDays?: number;
  telemetryBatchSize?: number;
  intervalMs?: number;
  clock?: () => Date;
}

export class GatewayMaintenanceConfigurationError extends Error {
  readonly code = "GATEWAY_MAINTENANCE_CONFIGURATION_INVALID";

  constructor() {
    super("GATEWAY_MAINTENANCE_CONFIGURATION_INVALID");
    this.name = "GatewayMaintenanceConfigurationError";
  }
}

export class GatewayMaintenanceRuntime {
  private readonly clock: () => Date;
  private readonly aprsOutboxRetentionDays: number;
  private readonly aprsOutboxBatchSize: number;
  private readonly cmCloudRawOutboxRetentionDays: number;
  private readonly cmCloudRawOutboxBatchSize: number;
  private readonly jobRetentionDays: number;
  private readonly jobBatchSize: number;
  private readonly messageRetentionDays: number;
  private readonly messageBatchSize: number;
  private readonly positionRetentionDays: number;
  private readonly positionBatchSize: number;
  private readonly observationBatchSize: number;
  private readonly retentionDays: number;
  private readonly telemetryBatchSize: number;
  private readonly intervalMs: number;
  private readonly igateRepository: AprsIgateRepository;
  private timer: NodeJS.Timeout | undefined;
  private started = false;

  constructor(private readonly options: GatewayMaintenanceRuntimeOptions) {
    this.igateRepository = new AprsIgateRepository(options.database.connection);
    this.clock = options.clock ?? (() => new Date());
    this.aprsOutboxRetentionDays = positiveInteger(
      options.aprsOutboxRetentionDays ?? 90,
      3_650,
    );
    this.aprsOutboxBatchSize = positiveInteger(
      options.aprsOutboxBatchSize ?? 1_000,
      10_000,
    );
    this.cmCloudRawOutboxRetentionDays = positiveInteger(
      options.cmCloudRawOutboxRetentionDays ?? 7,
      3_650,
    );
    this.cmCloudRawOutboxBatchSize = positiveInteger(
      options.cmCloudRawOutboxBatchSize ?? 1_000,
      10_000,
    );
    this.jobRetentionDays = positiveInteger(
      options.jobRetentionDays ?? 90,
      3_650,
    );
    this.jobBatchSize = positiveInteger(options.jobBatchSize ?? 1_000, 10_000);
    this.messageRetentionDays = positiveInteger(
      options.messageRetentionDays ?? 30,
      3_650,
    );
    this.messageBatchSize = positiveInteger(
      options.messageBatchSize ?? 1_000,
      10_000,
    );
    this.positionRetentionDays = positiveInteger(
      options.positionRetentionDays ?? 30,
      3_650,
    );
    this.positionBatchSize = positiveInteger(
      options.positionBatchSize ?? 1_000,
      10_000,
    );
    this.retentionDays = positiveInteger(options.retentionDays ?? 30, 3_650);
    this.telemetryBatchSize = positiveInteger(
      options.telemetryBatchSize ?? 1_000,
      10_000,
    );
    const minimumObservationBatchSize =
      this.telemetryBatchSize +
      this.messageBatchSize +
      this.positionBatchSize +
      OBSERVATION_RETENTION_HEADROOM;
    this.observationBatchSize = positiveInteger(
      options.observationBatchSize ?? minimumObservationBatchSize,
      MAX_OBSERVATION_RETENTION_BATCH_SIZE,
    );
    if (this.observationBatchSize < minimumObservationBatchSize) {
      throw new GatewayMaintenanceConfigurationError();
    }
    this.intervalMs = positiveInteger(options.intervalMs ?? 60 * 60 * 1_000);
    if (this.intervalMs < 100) {
      throw new GatewayMaintenanceConfigurationError();
    }
  }

  start(): void {
    if (this.started) {
      return;
    }
    this.started = true;
    this.runCycle();
    this.timer = setInterval(() => this.runCycle(), this.intervalMs);
    this.timer.unref();
  }

  stop(): void {
    this.started = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  runCycle(): number {
    const now = this.clock();
    const cutoff = new Date(
      now.getTime() - this.retentionDays * 24 * 60 * 60 * 1_000,
    ).toISOString();
    const deleted = this.options.database.meshTelemetry.deleteBefore(
      cutoff,
      this.telemetryBatchSize,
    );
    const messageCutoff = new Date(
      now.getTime() - this.messageRetentionDays * 24 * 60 * 60 * 1_000,
    ).toISOString();
    const messagesDeleted = this.options.database.meshMessages.deleteBefore(
      messageCutoff,
      this.messageBatchSize,
    );
    const jobCutoff = new Date(
      now.getTime() - this.jobRetentionDays * 24 * 60 * 60 * 1_000,
    ).toISOString();
    const terminalJobsDeleted = this.options.database.jobs.deleteTerminalBefore(
      jobCutoff,
      this.jobBatchSize,
    );
    const aprsOutboxCutoff = new Date(
      now.getTime() - this.aprsOutboxRetentionDays * 24 * 60 * 60 * 1_000,
    ).toISOString();
    const sentAprsOutboxDeleted =
      this.options.database.aprsOutbox.deleteSentBefore(
        aprsOutboxCutoff,
        this.aprsOutboxBatchSize,
      );
    const supersededAprsOutboxDeleted =
      this.options.database.aprsOutbox.deleteSuperseded(
        this.aprsOutboxBatchSize,
      );
    const igateSubmissionsDeleted = this.igateRepository.deleteTerminalBefore(
      aprsOutboxCutoff,
      this.aprsOutboxBatchSize,
    );
    const cmCloudRawOutboxCutoff = new Date(
      now.getTime() - this.cmCloudRawOutboxRetentionDays * 24 * 60 * 60 * 1_000,
    ).toISOString();
    const acknowledgedCmCloudRawOutboxDeleted =
      this.options.database.cmcloudRawOutbox.deleteAcknowledgedBefore(
        cmCloudRawOutboxCutoff,
        this.cmCloudRawOutboxBatchSize,
      );
    const positionCutoff = new Date(
      now.getTime() - this.positionRetentionDays * 24 * 60 * 60 * 1_000,
    ).toISOString();
    const positionRetention =
      this.options.database.positions.deleteHistoryBefore(
        positionCutoff,
        this.positionBatchSize,
      );
    const observationCutoff = [cutoff, messageCutoff, positionCutoff]
      .sort()
      .at(-1);
    if (!observationCutoff) {
      throw new GatewayMaintenanceConfigurationError();
    }
    const observationsDeleted =
      this.options.database.meshObservations.deleteUnreferencedBefore(
        observationCutoff,
        this.observationBatchSize,
      );
    const walCheckpoint = this.options.database.checkpoint();
    this.options.eventBus.publish({
      type: "telemetry.retention.completed",
      source: "gateway",
      payload: {
        cutoff,
        deleted,
        batchSize: this.telemetryBatchSize,
        messageCutoff,
        messagesDeleted,
        messageBatchSize: this.messageBatchSize,
        observationsDeleted,
        observationBatchSize: this.observationBatchSize,
        jobCutoff,
        terminalJobsDeleted,
        jobBatchSize: this.jobBatchSize,
        aprsOutboxCutoff,
        sentAprsOutboxDeleted,
        supersededAprsOutboxDeleted,
        igateSubmissionsDeleted,
        aprsOutboxBatchSize: this.aprsOutboxBatchSize,
        cmCloudRawOutboxCutoff,
        acknowledgedCmCloudRawOutboxDeleted,
        cmCloudRawOutboxBatchSize: this.cmCloudRawOutboxBatchSize,
        positionCutoff,
        positionDecisionsDeleted: positionRetention.decisionsDeleted,
        positionEventsDeleted: positionRetention.eventsDeleted,
        positionObservationsDeleted: positionRetention.observationsDeleted,
        positionBatchSize: this.positionBatchSize,
        observationCutoff,
        walCheckpoint,
      },
      occurredAt: now,
    });
    return deleted;
  }
}

function positiveInteger(
  value: number,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  if (!Number.isInteger(value) || value < 1 || value > maximum) {
    throw new GatewayMaintenanceConfigurationError();
  }
  return value;
}
