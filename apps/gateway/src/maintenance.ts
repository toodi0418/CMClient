import { DomainEventBus } from "./events.js";
import { GatewayDatabase } from "./persistence/database.js";

export interface GatewayMaintenanceRuntimeOptions {
  database: GatewayDatabase;
  eventBus: DomainEventBus;
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
  private readonly retentionDays: number;
  private readonly telemetryBatchSize: number;
  private readonly intervalMs: number;
  private timer: NodeJS.Timeout | undefined;
  private started = false;

  constructor(private readonly options: GatewayMaintenanceRuntimeOptions) {
    this.clock = options.clock ?? (() => new Date());
    this.retentionDays = positiveInteger(options.retentionDays ?? 30, 3_650);
    this.telemetryBatchSize = positiveInteger(
      options.telemetryBatchSize ?? 1_000,
      10_000,
    );
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
    this.options.eventBus.publish({
      type: "telemetry.retention.completed",
      source: "gateway",
      payload: { cutoff, deleted, batchSize: this.telemetryBatchSize },
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
