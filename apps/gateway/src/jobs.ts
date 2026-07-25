import { randomUUID } from "node:crypto";

import type { JobDetail, JobError, JobStatus } from "@cmclient/contracts";
import PQueue from "p-queue";

import { DomainEventBus } from "./events.js";
import { type JobRepository, type StoredJob } from "./persistence/database.js";

const JOB_TYPE = /^[a-z][a-z0-9_.-]{0,127}$/;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;
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
  maximumConcurrency?: number;
  maximumQueuedJobs?: number;
  setupGeneration?: number | (() => number);
  shutdownTimeoutMs?: number;
}

export interface JobEngineScheduleSnapshot {
  active: number;
  maximumConcurrency: number;
  maximumQueuedJobs: number;
  queued: number;
}

interface ScheduledJob {
  completion: Promise<void>;
  dispatchController: AbortController;
  executionController: AbortController;
  started: boolean;
  correlationId?: string;
  resolve: () => void;
}

export class JobEngine {
  private readonly clock: () => Date;
  private readonly handlers = new Map<string, JobHandler>();
  private readonly idFactory: () => string;
  private readonly maximumConcurrency: number;
  private readonly maximumQueuedJobs: number;
  private readonly setupGeneration: () => number;
  private readonly shutdownTimeoutMs: number;
  private readonly dispatch: PQueue;
  private readonly scheduled = new Map<string, ScheduledJob>();
  private stopping = false;
  private stopPromise: Promise<void> | undefined;

  constructor(
    private readonly repository: JobRepository,
    private readonly events: DomainEventBus,
    options: JobEngineOptions = {},
  ) {
    this.clock = options.clock ?? (() => new Date());
    this.idFactory = options.idFactory ?? randomUUID;
    this.maximumConcurrency = options.maximumConcurrency ?? 2;
    this.maximumQueuedJobs = options.maximumQueuedJobs ?? 1_024;
    const configuredSetupGeneration = options.setupGeneration;
    this.setupGeneration =
      typeof configuredSetupGeneration === "function"
        ? configuredSetupGeneration
        : () => configuredSetupGeneration ?? 1;
    this.shutdownTimeoutMs =
      options.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
    if (
      !Number.isInteger(this.maximumConcurrency) ||
      this.maximumConcurrency < 1 ||
      this.maximumConcurrency > 32 ||
      !Number.isInteger(this.maximumQueuedJobs) ||
      this.maximumQueuedJobs < 1 ||
      this.maximumQueuedJobs > 10_000 ||
      !Number.isInteger(this.shutdownTimeoutMs) ||
      this.shutdownTimeoutMs < 1 ||
      this.shutdownTimeoutMs > 120_000
    ) {
      throw new JobConfigurationError();
    }
    this.currentSetupGeneration();
    this.dispatch = new PQueue({ concurrency: this.maximumConcurrency });
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
    if (this.stopping) {
      throw new JobEngineStoppedError();
    }
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
    const setupGeneration = this.currentSetupGeneration();
    if (submission.idempotencyKey) {
      const existing = this.repository.findByIdempotency(
        submission.type,
        submission.idempotencyKey,
        setupGeneration,
      );
      if (existing) {
        if (
          existing.status === "queued" &&
          !this.scheduled.has(existing.id) &&
          this.hasScheduleCapacity()
        ) {
          this.schedule(existing.id, submission.correlationId);
        }
        return { created: false, job: toJobDetail(existing) };
      }
    }
    if (!this.hasScheduleCapacity()) {
      throw new JobQueueFullError();
    }
    const jobId = this.idFactory();
    if (!/^[a-zA-Z0-9-]{1,128}$/.test(jobId)) {
      throw new JobConfigurationError();
    }
    const created = this.repository.create({
      id: jobId,
      type: submission.type,
      input: cloneRecord(submission.input),
      setupGeneration,
      ...(submission.idempotencyKey
        ? { idempotencyKey: submission.idempotencyKey }
        : {}),
      now: this.now(),
    });
    if (created.created) {
      this.publish("job.created", created.job, submission.correlationId);
    }
    if (created.job.status === "queued") {
      if (this.hasScheduleCapacity()) {
        this.schedule(created.job.id, submission.correlationId);
      }
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
    if (current.status === "cancelling") {
      this.scheduled.get(jobId)?.executionController.abort();
      return toJobDetail(current);
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
      current.setupGeneration,
    );
    if (!updated) {
      const latest = this.repository.find(jobId);
      return latest ? toJobDetail(latest) : undefined;
    }
    const scheduled = this.scheduled.get(jobId);
    try {
      this.publish("job.status_changed", updated, correlationId);
    } finally {
      if (updated.status === "cancelled") {
        scheduled?.dispatchController.abort();
      } else {
        scheduled?.executionController.abort();
      }
    }
    return toJobDetail(updated);
  }

  recover(): void {
    if (this.stopping) {
      throw new JobEngineStoppedError();
    }
    const setupGeneration = this.currentSetupGeneration();
    while (true) {
      const staleQueued = this.repository.findQueuedOutsideGeneration(
        setupGeneration,
        1_000,
      );
      if (staleQueued.length === 0) {
        break;
      }
      for (const job of staleQueued) {
        this.stale(job);
      }
    }
    const interruptedStatuses = [
      "running",
      "waiting",
      "cancelling",
      "rolling_back",
    ] as const;
    while (true) {
      const interrupted = this.repository.findByStatuses(
        interruptedStatuses,
        1_000,
      );
      if (interrupted.length === 0) {
        break;
      }
      for (const job of interrupted) {
        if (job.setupGeneration !== setupGeneration) {
          this.stale(job);
        } else {
          this.fail(job, {
            code: "JOB_INTERRUPTED_BY_RESTART",
            params: {},
          });
        }
      }
    }
    this.refillQueue();
  }

  async waitFor(jobId: string): Promise<JobDetail | undefined> {
    await this.scheduled.get(jobId)?.completion;
    return this.get(jobId);
  }

  async drain(): Promise<void> {
    while (this.scheduled.size > 0) {
      const pending = [...this.scheduled.values()].map((scheduled) =>
        scheduled.completion.then(() => undefined),
      );
      if (pending.length === 0) {
        return;
      }
      await Promise.all(pending);
    }
  }

  stop(): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }
    this.stopping = true;
    this.dispatch.pause();
    this.stopPromise = Promise.resolve().then(() => this.stopAndDrain());
    return this.stopPromise;
  }

  private async stopAndDrain(): Promise<void> {
    for (const [jobId, scheduled] of [...this.scheduled]) {
      if (!scheduled.started) {
        scheduled.dispatchController.abort();
        continue;
      }
      const job = this.repository.find(jobId);
      if (job && !TERMINAL_STATUSES.has(job.status)) {
        try {
          this.cancel(jobId);
        } catch {
          // Shutdown still aborts and drains when cancellation persistence fails.
        }
      }
      scheduled.executionController.abort();
    }
    await withDeadline(this.drain(), this.shutdownTimeoutMs);
  }

  get scheduleSnapshot(): JobEngineScheduleSnapshot {
    return {
      active: this.dispatch.pending,
      maximumConcurrency: this.maximumConcurrency,
      maximumQueuedJobs: this.maximumQueuedJobs,
      queued: this.dispatch.size,
    };
  }

  private schedule(jobId: string, correlationId?: string): void {
    if (this.stopping || this.scheduled.has(jobId)) {
      return;
    }
    let resolve = (): void => undefined;
    const completion = new Promise<void>((complete) => {
      resolve = complete;
    });
    const dispatchController = new AbortController();
    const executionController = new AbortController();
    const scheduled: ScheduledJob = {
      completion,
      dispatchController,
      executionController,
      started: false,
      ...(correlationId ? { correlationId } : {}),
      resolve,
    };
    this.scheduled.set(jobId, scheduled);
    const execution = this.dispatch.add(
      () => {
        scheduled.started = true;
        return this.runScheduled(jobId, scheduled);
      },
      { id: jobId, signal: dispatchController.signal },
    );
    void execution.then(
      () => this.completeScheduled(jobId, scheduled),
      () => this.completeScheduled(jobId, scheduled),
    );
  }

  private async runScheduled(
    jobId: string,
    scheduled: ScheduledJob,
  ): Promise<void> {
    try {
      await this.execute(
        jobId,
        scheduled.executionController.signal,
        scheduled.correlationId,
      );
    } catch (error) {
      this.handleUnexpectedExecutionFailure(
        jobId,
        error,
        scheduled.correlationId,
      );
    }
  }

  private completeScheduled(jobId: string, scheduled: ScheduledJob): void {
    if (this.scheduled.get(jobId) !== scheduled) {
      return;
    }
    this.scheduled.delete(jobId);
    scheduled.resolve();
    if (!this.stopping) {
      try {
        this.refillQueue();
      } catch {
        // A later submission or recovery pass can refill after persistence recovers.
      }
    }
  }

  private refillQueue(): void {
    if (this.stopping) {
      return;
    }
    const available =
      this.maximumConcurrency + this.maximumQueuedJobs - this.scheduled.size;
    if (available <= 0) {
      return;
    }
    const candidates = this.repository.findQueuedByTypes(
      [...this.handlers.keys()],
      Math.min(20_000, this.dispatch.size + available),
      this.currentSetupGeneration(),
    );
    for (const candidate of candidates) {
      if (!this.hasScheduleCapacity()) {
        return;
      }
      if (!this.scheduled.has(candidate.id)) {
        this.schedule(candidate.id);
      }
    }
  }

  private hasScheduleCapacity(): boolean {
    return (
      this.scheduled.size < this.maximumConcurrency + this.maximumQueuedJobs
    );
  }

  private handleUnexpectedExecutionFailure(
    jobId: string,
    error: unknown,
    correlationId?: string,
  ): void {
    try {
      const current = this.repository.find(jobId);
      if (!current || TERMINAL_STATUSES.has(current.status)) {
        return;
      }
      if (!this.isCurrentSetupGeneration(current)) {
        this.stale(current, correlationId);
        return;
      }
      if (current.cancelRequested) {
        this.cancelled(current, correlationId);
        return;
      }
      this.fail(current, toJobError(error), correlationId);
    } catch {
      // The execution promise remains terminal even when persistence is unavailable.
    }
  }

  private async execute(
    jobId: string,
    signal: AbortSignal,
    correlationId?: string,
  ): Promise<void> {
    const queued = this.repository.find(jobId);
    if (!queued || queued.status !== "queued") {
      return;
    }
    if (this.stopping) {
      return;
    }
    if (!this.isCurrentSetupGeneration(queued)) {
      this.stale(queued, correlationId);
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
      queued.setupGeneration,
    );
    if (!running || running.status !== "running") {
      return;
    }
    this.publish("job.status_changed", running, correlationId);
    try {
      const result = await handler({
        job: running,
        signal,
        isCancellationRequested: () => {
          const current = this.repository.find(running.id);
          return (
            !current ||
            this.stopping ||
            current.cancelRequested ||
            !this.isCurrentSetupGeneration(current)
          );
        },
        throwIfCancellationRequested: () => {
          const current = this.repository.find(running.id);
          if (!current || !this.isCurrentSetupGeneration(current)) {
            throw new JobGenerationStaleError();
          }
          if (this.stopping || current.cancelRequested) {
            throw new JobCancelledError();
          }
        },
      });
      const current = this.repository.find(running.id);
      if (!current) {
        return;
      }
      if (!this.isCurrentSetupGeneration(current)) {
        this.stale(current, correlationId);
        return;
      }
      if (this.stopping || current.cancelRequested) {
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
        running.setupGeneration,
      );
      if (succeeded) {
        this.publish("job.status_changed", succeeded, correlationId);
      }
    } catch (error) {
      const current = this.repository.find(running.id);
      if (!current) {
        return;
      }
      if (
        error instanceof JobGenerationStaleError ||
        !this.isCurrentSetupGeneration(current)
      ) {
        this.stale(current, correlationId);
        return;
      }
      if (
        this.stopping ||
        current.cancelRequested ||
        error instanceof JobCancelledError
      ) {
        this.cancelled(current, correlationId);
        return;
      }
      this.fail(current, toJobError(error), correlationId);
    }
  }

  private cancelled(job: StoredJob, correlationId?: string): void {
    const cancelled = this.repository.transition(
      job.id,
      ["running", "waiting", "cancelling"],
      "cancelled",
      this.now(),
      { completedAt: this.now(), cancelRequested: true },
      job.setupGeneration,
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
      job.setupGeneration,
    );
    if (failed) {
      this.publish("job.status_changed", failed, correlationId);
    }
  }

  private stale(job: StoredJob, correlationId?: string): void {
    this.fail(
      job,
      { code: "JOB_SETUP_GENERATION_STALE", params: {} },
      correlationId,
    );
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

  private currentSetupGeneration(): number {
    const setupGeneration = this.setupGeneration();
    if (!Number.isSafeInteger(setupGeneration) || setupGeneration < 1) {
      throw new JobConfigurationError();
    }
    return setupGeneration;
  }

  private isCurrentSetupGeneration(job: StoredJob): boolean {
    return job.setupGeneration === this.currentSetupGeneration();
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

export class JobQueueFullError extends Error {
  readonly code = "JOB_QUEUE_FULL";

  constructor() {
    super("JOB_QUEUE_FULL");
    this.name = "JobQueueFullError";
  }
}

export class JobEngineStoppedError extends Error {
  readonly code = "JOB_ENGINE_STOPPED";

  constructor() {
    super("JOB_ENGINE_STOPPED");
    this.name = "JobEngineStoppedError";
  }
}

export class JobShutdownTimeoutError extends Error {
  readonly code = "JOB_SHUTDOWN_TIMEOUT";

  constructor() {
    super("JOB_SHUTDOWN_TIMEOUT");
    this.name = "JobShutdownTimeoutError";
  }
}

function withDeadline(
  operation: Promise<void>,
  timeoutMs: number,
): Promise<void> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new JobShutdownTimeoutError()), timeoutMs);
  });
  return Promise.race([operation, timeout]).finally(() => {
    if (timer) {
      clearTimeout(timer);
    }
  });
}

export class JobCancelledError extends Error {
  readonly code = "JOB_CANCELLED";
}

export class JobGenerationStaleError extends Error {
  readonly code = "JOB_SETUP_GENERATION_STALE";
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
  const code =
    error instanceof Error && "code" in error ? String(error.code) : "";
  return {
    code: /^[A-Z][A-Z0-9_]{0,127}$/.test(code) ? code : "JOB_EXECUTION_FAILED",
    params: {},
  };
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
