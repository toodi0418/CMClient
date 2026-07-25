import { describe, expect, it } from "vitest";

import { DomainEventBus } from "./events";
import {
  JobEngine,
  JobEngineStoppedError,
  JobQueueFullError,
  JobShutdownTimeoutError,
  JobTypeUnsupportedError,
} from "./jobs";
import { GatewayDatabase } from "./persistence/database";

describe("JobEngine", () => {
  it("persists an idempotent job and publishes durable state transitions", async () => {
    const database = new GatewayDatabase(":memory:");
    let eventSequence = 0;
    let jobSequence = 0;
    let executions = 0;
    const events = new DomainEventBus({
      eventIdFactory: () => `event-${++eventSequence}`,
    });
    const engine = new JobEngine(database.jobs, events, {
      idFactory: () => `job-${++jobSequence}`,
      handlers: [
        {
          type: "diagnostics.integrity_check",
          handler: async () => {
            executions += 1;
            return { integrity: "ok" };
          },
        },
      ],
    });

    const accepted = engine.submit({
      type: "diagnostics.integrity_check",
      input: {},
      idempotencyKey: "integrity-1",
      correlationId: "request-1",
    });
    const completed = await engine.waitFor(accepted.job.id);
    const replayed = engine.submit({
      type: "diagnostics.integrity_check",
      input: {},
      idempotencyKey: "integrity-1",
    });

    expect(accepted.created).toBe(true);
    expect(completed).toMatchObject({ status: "succeeded" });
    expect(database.jobs.find(accepted.job.id)).toMatchObject({
      result: { integrity: "ok" },
      status: "succeeded",
    });
    expect(replayed).toEqual({ created: false, job: completed });
    expect(executions).toBe(1);
    expect(events.replayAfter("missing").map((event) => event.type)).toEqual([
      "job.created",
      "job.status_changed",
      "job.status_changed",
    ]);
    database.close();
  });

  it("normalizes untrusted handler error codes before persistence", async () => {
    const database = new GatewayDatabase(":memory:");
    let sequence = 0;
    const invalidCodes = ["INVALID CODE", "X".repeat(129)];
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      idFactory: () => `invalid-error-code-${++sequence}`,
      handlers: invalidCodes.map((code, index) => ({
        type: `diagnostics.invalid_error_${index}`,
        handler: async () => {
          const error = new Error("untrusted handler failure") as Error & {
            code: string;
          };
          error.code = code;
          throw error;
        },
      })),
    });

    for (const [index] of invalidCodes.entries()) {
      const accepted = engine.submit({
        type: `diagnostics.invalid_error_${index}`,
        input: {},
      });
      await expect(engine.waitFor(accepted.job.id)).resolves.toMatchObject({
        status: "failed",
        error: { code: "JOB_EXECUTION_FAILED", params: {} },
      });
      expect(database.jobs.find(accepted.job.id)?.error).toEqual({
        code: "JOB_EXECUTION_FAILED",
        params: {},
      });
    }
    database.close();
  });

  it("scopes idempotency to the active setup generation", async () => {
    const database = new GatewayDatabase(":memory:");
    let setupGeneration = 1;
    let jobSequence = 0;
    let executions = 0;
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      idFactory: () => `generation-job-${++jobSequence}`,
      setupGeneration: () => setupGeneration,
      handlers: [
        {
          type: "diagnostics.noop",
          handler: async () => {
            executions += 1;
          },
        },
      ],
    });

    const first = engine.submit({
      type: "diagnostics.noop",
      input: {},
      idempotencyKey: "same-operation",
    });
    await engine.waitFor(first.job.id);
    const replayed = engine.submit({
      type: "diagnostics.noop",
      input: {},
      idempotencyKey: "same-operation",
    });
    setupGeneration = 2;
    const nextGeneration = engine.submit({
      type: "diagnostics.noop",
      input: {},
      idempotencyKey: "same-operation",
    });
    await engine.waitFor(nextGeneration.job.id);

    expect(replayed).toMatchObject({
      created: false,
      job: { id: first.job.id },
    });
    expect(nextGeneration).toMatchObject({ created: true });
    expect(nextGeneration.job.id).not.toBe(first.job.id);
    expect(database.jobs.find(first.job.id)?.setupGeneration).toBe(1);
    expect(database.jobs.find(nextGeneration.job.id)?.setupGeneration).toBe(2);
    expect(executions).toBe(2);
    database.close();
  });

  it("fails stale queued work during recovery without invoking its handler", async () => {
    const database = new GatewayDatabase(":memory:");
    database.jobs.create({
      id: "stale-queued-job",
      type: "diagnostics.noop",
      input: {},
      setupGeneration: 1,
      now: "2026-07-18T00:00:00.000Z",
    });
    database.jobs.create({
      id: "current-queued-job",
      type: "diagnostics.noop",
      input: {},
      setupGeneration: 2,
      now: "2026-07-18T00:00:01.000Z",
    });
    let executions = 0;
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      setupGeneration: 2,
      handlers: [
        {
          type: "diagnostics.noop",
          handler: async () => {
            executions += 1;
          },
        },
      ],
    });

    engine.recover();

    expect(engine.get("stale-queued-job")).toMatchObject({
      status: "failed",
      error: { code: "JOB_SETUP_GENERATION_STALE" },
    });
    await expect(engine.waitFor("current-queued-job")).resolves.toMatchObject({
      status: "succeeded",
    });
    expect(executions).toBe(1);
    database.close();
  });

  it("fences a handler result after the setup generation changes", async () => {
    const database = new GatewayDatabase(":memory:");
    let setupGeneration = 1;
    let release = (): void => undefined;
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      idFactory: () => "generation-fenced-job",
      setupGeneration: () => setupGeneration,
      handlers: [
        {
          type: "diagnostics.wait",
          handler: async () =>
            new Promise<Record<string, unknown>>((resolve) => {
              release = () => resolve({ shouldNotCommit: true });
            }),
        },
      ],
    });
    const accepted = engine.submit({ type: "diagnostics.wait", input: {} });
    await waitFor(
      () => database.jobs.find(accepted.job.id)?.status === "running",
    );

    setupGeneration = 2;
    release();

    await expect(engine.waitFor(accepted.job.id)).resolves.toMatchObject({
      status: "failed",
      error: { code: "JOB_SETUP_GENERATION_STALE" },
    });
    expect(database.jobs.find(accepted.job.id)?.result).toBeUndefined();
    database.close();
  });

  it("cancels a running job through its abort signal", async () => {
    const database = new GatewayDatabase(":memory:");
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      idFactory: () => "job-1",
      handlers: [
        {
          type: "diagnostics.wait",
          handler: async ({ signal }) =>
            new Promise<void>((resolve) => {
              signal.addEventListener("abort", () => resolve(), { once: true });
            }),
        },
      ],
    });

    const accepted = engine.submit({ type: "diagnostics.wait", input: {} });
    expect(engine.cancel(accepted.job.id)).toMatchObject({
      status: "cancelling",
    });
    await expect(engine.waitFor(accepted.job.id)).resolves.toMatchObject({
      status: "cancelled",
    });
    database.close();
  });

  it("fails jobs interrupted by a process restart without replaying work", () => {
    const database = new GatewayDatabase(":memory:");
    database.jobs.create({
      id: "job-recover",
      type: "diagnostics.wait",
      input: {},
      now: "2026-07-18T00:00:00.000Z",
    });
    database.jobs.transition(
      "job-recover",
      ["queued"],
      "running",
      "2026-07-18T00:00:01.000Z",
      { startedAt: "2026-07-18T00:00:01.000Z" },
    );
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      handlers: [{ type: "diagnostics.wait", handler: async () => undefined }],
    });

    engine.recover();

    expect(engine.get("job-recover")).toMatchObject({
      status: "failed",
      error: { code: "JOB_INTERRUPTED_BY_RESTART" },
    });
    database.close();
  });

  it("fails an old-generation running job as stale during restart recovery", () => {
    const database = new GatewayDatabase(":memory:");
    database.jobs.create({
      id: "stale-running-job",
      type: "diagnostics.wait",
      input: {},
      setupGeneration: 1,
      now: "2026-07-18T00:00:00.000Z",
    });
    database.jobs.transition(
      "stale-running-job",
      ["queued"],
      "running",
      "2026-07-18T00:00:01.000Z",
      { startedAt: "2026-07-18T00:00:01.000Z" },
      1,
    );
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      setupGeneration: 2,
      handlers: [{ type: "diagnostics.wait", handler: async () => undefined }],
    });

    engine.recover();

    expect(engine.get("stale-running-job")).toMatchObject({
      status: "failed",
      error: { code: "JOB_SETUP_GENERATION_STALE" },
    });
    database.close();
  });

  it("caps concurrent executions and drains queued jobs in submission order", async () => {
    const database = new GatewayDatabase(":memory:");
    let jobSequence = 0;
    let active = 0;
    let maximumActive = 0;
    const started: string[] = [];
    const releases = new Map<string, () => void>();
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      maximumConcurrency: 2,
      maximumQueuedJobs: 6,
      idFactory: () => `load-job-${++jobSequence}`,
      handlers: [
        {
          type: "load.wait",
          handler: async ({ job }) => {
            active += 1;
            maximumActive = Math.max(maximumActive, active);
            started.push(job.id);
            await new Promise<void>((resolve) => {
              releases.set(job.id, resolve);
            });
            active -= 1;
            if (job.id === "load-job-4") {
              throw new Error("fixture load failure");
            }
          },
        },
      ],
    });
    const accepted = Array.from({ length: 8 }, () =>
      engine.submit({ type: "load.wait", input: {} }),
    );

    expect(started).toEqual(["load-job-1", "load-job-2"]);
    expect(engine.scheduleSnapshot).toEqual({
      active: 2,
      maximumConcurrency: 2,
      maximumQueuedJobs: 6,
      queued: 6,
    });

    for (const submission of accepted) {
      const release = releases.get(submission.job.id);
      if (!release) {
        throw new Error(
          `job did not start in FIFO order: ${submission.job.id}`,
        );
      }
      release();
      await expect(engine.waitFor(submission.job.id)).resolves.toMatchObject({
        status: submission.job.id === "load-job-4" ? "failed" : "succeeded",
      });
    }

    expect(started).toEqual(accepted.map((entry) => entry.job.id));
    expect(maximumActive).toBe(2);
    expect(engine.scheduleSnapshot).toEqual({
      active: 0,
      maximumConcurrency: 2,
      maximumQueuedJobs: 6,
      queued: 0,
    });
    database.close();
  });

  it("removes a cancelled queued job without waiting for an execution slot", async () => {
    const database = new GatewayDatabase(":memory:");
    let jobSequence = 0;
    let releaseRunning = (): void => undefined;
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      maximumConcurrency: 1,
      maximumQueuedJobs: 1,
      idFactory: () => `cancel-job-${++jobSequence}`,
      handlers: [
        {
          type: "load.wait",
          handler: async () =>
            new Promise<void>((resolve) => {
              releaseRunning = resolve;
            }),
        },
      ],
    });
    const running = engine.submit({ type: "load.wait", input: {} });
    const queued = engine.submit({ type: "load.wait", input: {} });

    expect(engine.cancel(queued.job.id)).toMatchObject({ status: "cancelled" });
    await expect(engine.waitFor(queued.job.id)).resolves.toMatchObject({
      status: "cancelled",
    });
    expect(engine.scheduleSnapshot).toMatchObject({ active: 1, queued: 0 });

    releaseRunning();
    await engine.waitFor(running.job.id);
    database.close();
  });

  it("rejects new work at queue capacity while reusing an idempotent Job", async () => {
    const database = new GatewayDatabase(":memory:");
    let jobSequence = 0;
    let releaseRunning = (): void => undefined;
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      maximumConcurrency: 1,
      maximumQueuedJobs: 2,
      idFactory: () => `capacity-job-${++jobSequence}`,
      handlers: [
        {
          type: "load.wait",
          handler: async () =>
            new Promise<void>((resolve) => {
              releaseRunning = resolve;
            }),
        },
      ],
    });
    const first = engine.submit({
      type: "load.wait",
      input: {},
      idempotencyKey: "capacity-reuse",
    });
    const second = engine.submit({ type: "load.wait", input: {} });
    const third = engine.submit({ type: "load.wait", input: {} });

    expect(
      engine.submit({
        type: "load.wait",
        input: { ignored: true },
        idempotencyKey: "capacity-reuse",
      }),
    ).toMatchObject({ created: false, job: { id: first.job.id } });
    expect(() => engine.submit({ type: "load.wait", input: {} })).toThrow(
      JobQueueFullError,
    );
    expect(
      database.connection.prepare("SELECT COUNT(*) AS count FROM jobs").get()
        ?.count,
    ).toBe(3);

    engine.cancel(second.job.id);
    engine.cancel(third.job.id);
    releaseRunning();
    await engine.waitFor(first.job.id);
    database.close();
  });

  it("recovers only a bounded SQLite queue window and refills it to completion", async () => {
    const database = new GatewayDatabase(":memory:");
    const jobIds = Array.from(
      { length: 25 },
      (_, index) => `recovered-${String(index).padStart(2, "0")}`,
    );
    for (const id of jobIds) {
      database.jobs.create({
        id,
        type: "load.recover",
        input: {},
        now: "2026-07-18T00:00:00.000Z",
      });
    }
    const releases = new Map<string, () => void>();
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      maximumConcurrency: 2,
      maximumQueuedJobs: 3,
      handlers: [
        {
          type: "load.recover",
          handler: async ({ job }) =>
            new Promise<void>((resolve) => {
              releases.set(job.id, resolve);
            }),
        },
      ],
    });

    engine.recover();

    expect(engine.scheduleSnapshot).toEqual({
      active: 2,
      maximumConcurrency: 2,
      maximumQueuedJobs: 3,
      queued: 3,
    });
    for (const id of jobIds) {
      await waitFor(() => releases.has(id));
      releases.get(id)?.();
      await waitFor(() => database.jobs.find(id)?.status === "succeeded");
    }
    expect(engine.scheduleSnapshot).toMatchObject({ active: 0, queued: 0 });
    expect(database.jobs.findByStatuses(["queued"])).toEqual([]);
    database.close();
  });

  it("refills older durable work before admitting a new submission", async () => {
    const database = new GatewayDatabase(":memory:");
    const jobIds = Array.from(
      { length: 6 },
      (_, index) => `durable-first-${index}`,
    );
    for (const id of jobIds) {
      database.jobs.create({
        id,
        type: "load.recover",
        input: {},
        now: "2026-07-18T00:00:00.000Z",
      });
    }
    const started: string[] = [];
    const releases = new Map<string, () => void>();
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      maximumConcurrency: 1,
      maximumQueuedJobs: 2,
      handlers: [
        {
          type: "load.recover",
          handler: async ({ job }) => {
            started.push(job.id);
            await new Promise<void>((resolve) => {
              releases.set(job.id, resolve);
            });
          },
        },
      ],
    });
    engine.recover();
    await waitFor(() => releases.has(jobIds[0]!));

    releases.get(jobIds[0]!)?.();
    await engine.waitFor(jobIds[0]!);

    expect(engine.scheduleSnapshot).toMatchObject({ active: 1, queued: 2 });
    expect(() => engine.submit({ type: "load.recover", input: {} })).toThrow(
      JobQueueFullError,
    );
    for (const id of jobIds.slice(1)) {
      await waitFor(() => releases.has(id));
      releases.get(id)?.();
      await engine.waitFor(id);
    }
    expect(started).toEqual(jobIds);
    database.close();
  });

  it("recovers a registered type behind more than 20k unsupported queued rows", async () => {
    const database = new GatewayDatabase(":memory:");
    const insert = database.connection.prepare(
      "INSERT INTO jobs (id, type, status, input, created_at, updated_at) VALUES (?, 'legacy.unknown', 'queued', '{}', ?, ?)",
    );
    database.connection.exec("BEGIN IMMEDIATE");
    try {
      for (let index = 0; index < 20_001; index += 1) {
        insert.run(
          `unsupported-${String(index).padStart(5, "0")}`,
          "2026-07-18T00:00:00.000Z",
          "2026-07-18T00:00:00.000Z",
        );
      }
      database.connection.exec("COMMIT");
    } catch (error) {
      database.connection.exec("ROLLBACK");
      throw error;
    }
    database.jobs.create({
      id: "b-supported",
      type: "diagnostics.noop",
      input: {},
      now: "2026-07-18T00:00:01.000Z",
    });
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      maximumConcurrency: 1,
      maximumQueuedJobs: 1,
      handlers: [
        {
          type: "diagnostics.noop",
          handler: async () => undefined,
        },
      ],
    });

    engine.recover();

    await expect(engine.waitFor("b-supported")).resolves.toMatchObject({
      status: "succeeded",
    });
    expect(engine.get("unsupported-00000")).toMatchObject({
      status: "queued",
    });
    await engine.stop();
    database.close();
  });

  it("stops dispatch, cancels active work, and drains before persistence closes", async () => {
    const database = new GatewayDatabase(":memory:");
    let jobSequence = 0;
    let markStarted = (): void => undefined;
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      maximumConcurrency: 1,
      maximumQueuedJobs: 1,
      idFactory: () => `shutdown-job-${++jobSequence}`,
      handlers: [
        {
          type: "load.wait",
          handler: async ({ signal }) => {
            markStarted();
            await new Promise<void>((resolve) => {
              if (signal.aborted) {
                resolve();
                return;
              }
              signal.addEventListener("abort", () => resolve(), { once: true });
            });
          },
        },
      ],
    });
    const running = engine.submit({ type: "load.wait", input: {} });
    const queued = engine.submit({ type: "load.wait", input: {} });
    await started;

    const stopping = engine.stop();

    expect(engine.stop()).toBe(stopping);
    await expect(stopping).resolves.toBeUndefined();
    expect(engine.get(running.job.id)).toMatchObject({ status: "cancelled" });
    expect(engine.get(queued.job.id)).toMatchObject({ status: "queued" });
    expect(engine.scheduleSnapshot).toMatchObject({ active: 0, queued: 0 });
    expect(() => engine.submit({ type: "load.wait", input: {} })).toThrow(
      JobEngineStoppedError,
    );

    database.close();
  });

  it("does not start queued work after stop begins while handlers settle", async () => {
    const database = new GatewayDatabase(":memory:");
    let jobSequence = 0;
    const started: string[] = [];
    const releases = new Map<string, () => void>();
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      maximumConcurrency: 2,
      maximumQueuedJobs: 2,
      idFactory: () => `stop-race-job-${++jobSequence}`,
      handlers: [
        {
          type: "load.wait",
          handler: async ({ job }) => {
            started.push(job.id);
            await new Promise<void>((resolve) => {
              releases.set(job.id, resolve);
            });
          },
        },
      ],
    });
    const submissions = Array.from({ length: 4 }, () =>
      engine.submit({ type: "load.wait", input: {} }),
    );
    const activeIds = submissions.slice(0, 2).map(({ job }) => job.id);
    const queuedIds = submissions.slice(2).map(({ job }) => job.id);
    expect(started).toEqual(activeIds);

    for (const id of activeIds) {
      releases.get(id)?.();
    }
    await expect(engine.stop()).resolves.toBeUndefined();

    expect(started).toEqual(activeIds);
    for (const id of activeIds) {
      expect(engine.get(id)).toMatchObject({ status: "cancelled" });
    }
    for (const id of queuedIds) {
      expect(engine.get(id)).toMatchObject({ status: "queued" });
    }
    database.close();
  });

  it("removes a terminalized queued task while the dispatcher is paused", async () => {
    const database = new GatewayDatabase(":memory:");
    let jobSequence = 0;
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      maximumConcurrency: 1,
      maximumQueuedJobs: 1,
      idFactory: () => `terminal-queue-job-${++jobSequence}`,
      handlers: [
        {
          type: "load.wait",
          handler: async ({ signal }) =>
            new Promise<void>((resolve) => {
              signal.addEventListener("abort", () => resolve(), { once: true });
            }),
        },
      ],
    });
    const running = engine.submit({ type: "load.wait", input: {} });
    const queued = engine.submit({ type: "load.wait", input: {} });
    database.jobs.transition(
      queued.job.id,
      ["queued"],
      "failed",
      "2026-07-18T00:00:00.000Z",
      {
        completedAt: "2026-07-18T00:00:00.000Z",
        error: { code: "FIXTURE_TERMINAL", params: {} },
      },
      1,
    );

    await expect(engine.stop()).resolves.toBeUndefined();

    expect(engine.get(running.job.id)).toMatchObject({ status: "cancelled" });
    expect(engine.get(queued.job.id)).toMatchObject({ status: "failed" });
    expect(engine.scheduleSnapshot).toMatchObject({ active: 0, queued: 0 });
    database.close();
  });

  it("fences an already-resolved handler result as soon as stop begins", async () => {
    const database = new GatewayDatabase(":memory:");
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      idFactory: () => "stop-fenced-result",
      handlers: [
        {
          type: "diagnostics.noop",
          handler: async () => ({ shouldNotCommit: true }),
        },
      ],
    });
    const accepted = engine.submit({ type: "diagnostics.noop", input: {} });

    await expect(engine.stop()).resolves.toBeUndefined();

    expect(engine.get(accepted.job.id)).toMatchObject({ status: "cancelled" });
    expect(database.jobs.find(accepted.job.id)?.result).toBeUndefined();
    database.close();
  });

  it("bounds shutdown when a handler ignores its abort signal", async () => {
    const database = new GatewayDatabase(":memory:");
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      idFactory: () => "shutdown-timeout-job",
      shutdownTimeoutMs: 25,
      handlers: [
        {
          type: "load.hung",
          handler: async () => new Promise<void>(() => undefined),
        },
      ],
    });
    const accepted = engine.submit({ type: "load.hung", input: {} });

    const stopping = engine.stop();

    expect(engine.stop()).toBe(stopping);
    await expect(stopping).rejects.toBeInstanceOf(JobShutdownTimeoutError);
    expect(engine.get(accepted.job.id)).toMatchObject({
      status: "cancelling",
    });
    expect(() => engine.submit({ type: "load.hung", input: {} })).toThrow(
      JobEngineStoppedError,
    );
    database.close();
  });

  it("aborts and drains when cancellation event publication fails", async () => {
    const database = new GatewayDatabase(":memory:");
    const engine = new JobEngine(
      database.jobs,
      new FailingCancellationEventBus(),
      {
        idFactory: () => "shutdown-publish-failure",
        handlers: [
          {
            type: "load.wait",
            handler: async ({ signal }) =>
              new Promise<void>((resolve) => {
                signal.addEventListener("abort", () => resolve(), {
                  once: true,
                });
              }),
          },
        ],
      },
    );
    const accepted = engine.submit({ type: "load.wait", input: {} });

    const stopping = engine.stop();

    expect(engine.stop()).toBe(stopping);
    await expect(stopping).resolves.toBeUndefined();
    expect(engine.get(accepted.job.id)).toMatchObject({ status: "cancelled" });
    expect(engine.scheduleSnapshot).toMatchObject({ active: 0, queued: 0 });
    database.close();
  });

  it("aborts and drains when cancellation persistence fails", async () => {
    const database = new GatewayDatabase(":memory:");
    const transition = database.jobs.transition.bind(database.jobs);
    let failCancellation = true;
    database.jobs.transition = (...args) => {
      if (failCancellation && args[2] === "cancelling") {
        failCancellation = false;
        throw new Error("fixture cancellation persistence failure");
      }
      return transition(...args);
    };
    const engine = new JobEngine(database.jobs, new DomainEventBus(), {
      idFactory: () => "shutdown-persistence-failure",
      handlers: [
        {
          type: "load.wait",
          handler: async ({ signal }) =>
            new Promise<void>((_resolve, reject) => {
              signal.addEventListener(
                "abort",
                () => reject(new Error("fixture handler aborted")),
                { once: true },
              );
            }),
        },
      ],
    });
    const accepted = engine.submit({ type: "load.wait", input: {} });
    await waitFor(
      () => database.jobs.find(accepted.job.id)?.status === "running",
    );

    await expect(engine.stop()).resolves.toBeUndefined();

    expect(failCancellation).toBe(false);
    expect(engine.get(accepted.job.id)).toMatchObject({ status: "cancelled" });
    expect(database.jobs.find(accepted.job.id)).toMatchObject({
      cancelRequested: true,
    });
    expect(engine.scheduleSnapshot).toMatchObject({ active: 0, queued: 0 });
    database.close();
  });

  it("terminally catches an unexpected execution rejection", async () => {
    const database = new GatewayDatabase(":memory:");
    const events = new FailingJobStatusEventBus();
    const engine = new JobEngine(database.jobs, events, {
      idFactory: () => "failed-publish-job",
      handlers: [
        {
          type: "diagnostics.noop",
          handler: async () => undefined,
        },
      ],
    });

    const accepted = engine.submit({ type: "diagnostics.noop", input: {} });

    await expect(engine.waitFor(accepted.job.id)).resolves.toMatchObject({
      status: "failed",
      error: { code: "EVENT_PUBLISH_FAILED" },
    });
    await expect(engine.stop()).resolves.toBeUndefined();
    database.close();
  });

  it("rejects submission for an unregistered job type", () => {
    const database = new GatewayDatabase(":memory:");
    const engine = new JobEngine(database.jobs, new DomainEventBus());

    expect(() =>
      engine.submit({ type: "diagnostics.unknown", input: {} }),
    ).toThrow(JobTypeUnsupportedError);
    database.close();
  });
});

class FailingJobStatusEventBus extends DomainEventBus {
  private failed = false;

  override publish(
    input: Parameters<DomainEventBus["publish"]>[0],
  ): ReturnType<DomainEventBus["publish"]> {
    if (input.type === "job.status_changed" && !this.failed) {
      this.failed = true;
      const error = new Error("EVENT_PUBLISH_FAILED") as Error & {
        code: string;
      };
      error.code = "EVENT_PUBLISH_FAILED";
      throw error;
    }
    return super.publish(input);
  }
}

class FailingCancellationEventBus extends DomainEventBus {
  override publish(
    input: Parameters<DomainEventBus["publish"]>[0],
  ): ReturnType<DomainEventBus["publish"]> {
    const job = input.payload.job;
    if (
      input.type === "job.status_changed" &&
      typeof job === "object" &&
      job !== null &&
      "status" in job &&
      job.status === "cancelling"
    ) {
      throw new Error("fixture cancellation publication failure");
    }
    return super.publish(input);
  }
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("fixture condition was not reached");
}
