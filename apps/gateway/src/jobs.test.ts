import { describe, expect, it } from "vitest";

import { DomainEventBus } from "./events";
import { JobEngine, JobTypeUnsupportedError } from "./jobs";
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

  it("rejects submission for an unregistered job type", () => {
    const database = new GatewayDatabase(":memory:");
    const engine = new JobEngine(database.jobs, new DomainEventBus());

    expect(() =>
      engine.submit({ type: "diagnostics.unknown", input: {} }),
    ).toThrow(JobTypeUnsupportedError);
    database.close();
  });
});
