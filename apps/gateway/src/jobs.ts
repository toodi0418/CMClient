import { randomUUID } from "node:crypto";

import type { JobDetail, JobError, JobStatus } from "@cmclient/contracts";

import { DomainEventBus } from "./events.js";
import { type JobRepository, type StoredJob } from "./persistence/database.js";

const JOB_TYPE = /^[a-z][a-z0-9_.-]{0,127}$/;
const TERMINAL_STATUSES = new Set<JobStatus>([
  "succeeded",
  "failed",
  "cancelled",
  "rolled_back",
]);

export interface JobSubmission {
  type: string;
  input: Record<string, unknown>;
  idempotencyKey?: string;
  correlationId?: string;
}

export interface JobSubmissionResult {
  created: boolean;
  job: JobDetail;
}

export interface JobExecutionContext {
  job: Readonly<StoredJob>;
  signal: AbortSignal;
  isCancellationRequested(): boolean;
  throwIfCancellationRequested(): void;
}

export type JobHandler = (
  context: JobExecutionContext,
) => Promise<Record<string, unknown> | void>;

export interface JobHandlerDefinition {
  type: string;
  handler: JobHandler;
}

export interface JobEngineOptions {
  clock?: () => Date;
  handlers?: readonly JobHandlerDefinition[];
  idFactory?: () => string;
}

export class JobEngine {
  private readonly clock: () => Date;
  private readonly handlers = new Map<string, JobHandler>();
  private readonly idFactory: () => string;
  private readonly controllers = new Map<string, AbortController>();
  private readonly executions = new Map<string, Promise<void>>();

  constructor(
    private readonly repository: JobRepository,
    private readonly events: DomainEventBus,
    options: JobEngineOptions = {},
  ) {
    this.clock = options.clock ?? (() => new Date());
    this.idFactory = options.idFactory ?? randomUUID;
    for (const definition of options.handlers ?? []) {
      if (
        !JOB_TYPE.test(definition.type) ||
        this.handlers.has(definition.type)
      ) {
        throw new JobConfigurationError();
      }
      this.handlers.set(definition.type, definition.handler);
    }
  }

  submit(submission: JobSubmission): JobSubmissionResult {
    if (!JOB_TYPE.test(submission.type)) {
      throw new JobInputError();
    }
    if (!this.handlers.has(submission.type)) {
      throw new JobTypeUnsupportedError();
    }
    if (
      submission.idempotencyKey !== undefined &&
      !/^[a-zA-Z0-9._:-]{1,128}$/.test(submission.idempotencyKey)
    ) {
      throw new JobInputError();
    }
    const jobId = this.idFactory();
    if (!/^[a-zA-Z0-9-]{1,128}$/.test(jobId)) {
      throw new JobConfigurationError();
    }
    const created = this.repository.create({
      id: jobId,
      type: submission.type,
      input: cloneRecord(submission.input),
      ...(submission.idempotencyKey
        ? { idempotencyKey: submission.idempotencyKey }
        : {}),
      now: this.now(),
    });
    if (created.created) {
      this.publish("job.created", created.job, submission.correlationId);
    }
    if (created.job.status === "queued") {
      this.schedule(created.job.id, submission.correlationId);
    }
    return { created: created.created, job: toJobDetail(created.job) };
  }

  get(jobId: string): JobDetail | undefined {
    const job = this.repository.find(jobId);
    return job ? toJobDetail(job) : undefined;
  }

  cancel(jobId: string, correlationId?: string): JobDetail | undefined {
    const current = this.repository.find(jobId);
    if (!current || TERMINAL_STATUSES.has(current.status)) {
      return current ? toJobDetail(current) : undefined;
    }
    const nextStatus: JobStatus =
      current.status === "queued" ? "cancelled" : "cancelling";
    const updated = this.repository.transition(
      jobId,
      [current.status],
      nextStatus,
      this.now(),
      {
        cancelRequested: true,
        ...(nextStatus === "cancelled" ? { completedAt: this.now() } : {}),
      },
    );
    if (!updated) {
      return undefined;
    }
    if (updated.status !== current.status) {
      this.publish("job.status_changed", updated, correlationId);
      this.controllers.get(jobId)?.abort();
    }
    return toJobDetail(updated);
  }

  recover(): void {
    for (const job of this.repository.findByStatuses([
      "running",
      "waiting",
      "cancelling",
      "rolling_back",
    ])) {
      const failed = this.repository.transition(
        job.id,
        [job.status],
        "failed",
        this.now(),
        {
          completedAt: this.now(),
          error: { code: "JOB_INTERRUPTED_BY_RESTART", params: {} },
        },
      );
      if (failed) {
        this.publish("job.status_changed", failed);
      }
    }
    for (const job of this.repository.findByStatuses(["queued"])) {
      this.schedule(job.id);
    }
  }

  async waitFor(jobId: string): Promise<JobDetail | undefined> {
    await this.executions.get(jobId);
    return this.get(jobId);
  }

  private schedule(jobId: string, correlationId?: string): void {
    if (this.executions.has(jobId)) {
      return;
    }
    const execution = this.execute(jobId, correlationId).finally(() => {
      this.executions.delete(jobId);
    });
    this.executions.set(jobId, execution);
  }

  private async execute(jobId: string, correlationId?: string): Promise<void> {
    const queued = this.repository.find(jobId);
    if (!queued || queued.status !== "queued") {
      return;
    }
    const handler = this.handlers.get(queued.type);
    if (!handler) {
      this.fail(
        queued,
        { code: "JOB_HANDLER_UNAVAILABLE", params: { type: queued.type } },
        correlationId,
      );
      return;
    }
    const running = this.repository.transition(
      queued.id,
      ["queued"],
      "running",
      this.now(),
      { startedAt: this.now() },
    );
    if (!running || running.status !== "running") {
      return;
    }
    this.publish("job.status_changed", running, correlationId);
    const controller = new AbortController();
    this.controllers.set(running.id, controller);
    try {
      const result = await handler({
        job: running,
        signal: controller.signal,
        isCancellationRequested: () =>
          this.repository.find(running.id)?.cancelRequested ?? true,
        throwIfCancellationRequested: () => {
          if (this.repository.find(running.id)?.cancelRequested) {
            throw new JobCancelledError();
          }
        },
      });
      const current = this.repository.find(running.id);
      if (!current) {
        return;
      }
      if (current.cancelRequested) {
        this.cancelled(current, correlationId);
        return;
      }
      const succeeded = this.repository.transition(
        running.id,
        ["running"],
        "succeeded",
        this.now(),
        {
          completedAt: this.now(),
          ...(result ? { result: cloneRecord(result) } : {}),
        },
      );
      if (succeeded) {
        this.publish("job.status_changed", succeeded, correlationId);
      }
    } catch (error) {
      const current = this.repository.find(running.id);
      if (!current) {
        return;
      }
      if (current.cancelRequested || error instanceof JobCancelledError) {
        this.cancelled(current, correlationId);
        return;
      }
      this.fail(current, toJobError(error), correlationId);
    } finally {
      this.controllers.delete(running.id);
    }
  }

  private cancelled(job: StoredJob, correlationId?: string): void {
    const cancelled = this.repository.transition(
      job.id,
      ["running", "waiting", "cancelling"],
      "cancelled",
      this.now(),
      { completedAt: this.now(), cancelRequested: true },
    );
    if (cancelled) {
      this.publish("job.status_changed", cancelled, correlationId);
    }
  }

  private fail(job: StoredJob, error: JobError, correlationId?: string): void {
    const failed = this.repository.transition(
      job.id,
      ["queued", "running", "waiting", "cancelling", "rolling_back"],
      "failed",
      this.now(),
      { completedAt: this.now(), error },
    );
    if (failed) {
      this.publish("job.status_changed", failed, correlationId);
    }
  }

  private publish(
    type: "job.created" | "job.status_changed",
    job: StoredJob,
    correlationId?: string,
  ): void {
    this.events.publish({
      type,
      source: "gateway",
      ...(correlationId ? { correlationId } : {}),
      payload: { jobId: job.id, job: toJobDetail(job) },
    });
  }

  private now(): string {
    return this.clock().toISOString();
  }
}

export class JobConfigurationError extends Error {
  readonly code = "JOB_CONFIGURATION_INVALID";
}

export class JobInputError extends Error {
  readonly code = "JOB_INPUT_INVALID";
}

export class JobTypeUnsupportedError extends Error {
  readonly code = "JOB_TYPE_UNSUPPORTED";
}

export class JobCancelledError extends Error {
  readonly code = "JOB_CANCELLED";
}

function cloneRecord(value: Record<string, unknown>): Record<string, unknown> {
  if (!value || Array.isArray(value)) {
    throw new JobInputError();
  }
  try {
    const serialized = JSON.stringify(value);
    if (!serialized) {
      throw new JobInputError();
    }
    return JSON.parse(serialized) as Record<string, unknown>;
  } catch (error) {
    if (error instanceof JobInputError) {
      throw error;
    }
    throw new JobInputError();
  }
}

function toJobError(error: unknown): JobError {
  return error instanceof Error && "code" in error
    ? { code: String(error.code), params: {} }
    : { code: "JOB_EXECUTION_FAILED", params: {} };
}

export function toJobDetail(job: StoredJob): JobDetail {
  return {
    id: job.id,
    type: job.type,
    status: job.status,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    ...(job.startedAt ? { startedAt: job.startedAt } : {}),
    ...(job.completedAt ? { completedAt: job.completedAt } : {}),
    ...(job.error ? { error: job.error } : {}),
  };
}
