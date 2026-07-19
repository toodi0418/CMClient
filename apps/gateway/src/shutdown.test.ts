import { describe, expect, it, vi } from "vitest";

import { GatewayConfigurationError, parseGatewayListenOptions } from "./app";
import {
  createGatewaySupervisorShutdownInput,
  createShutdownCoordinator,
  createStartupShutdownCoordinator,
  GatewayShutdownError,
  GatewayStartupCancelledError,
  GatewayTrackedOperation,
  runCleanupPhases,
  runCleanupWithDeadline,
} from "./shutdown";

describe("Gateway shutdown", () => {
  it("accepts a bounded fragmented supervisor command and parent EOF", () => {
    let requests = 0;
    const command = createGatewaySupervisorShutdownInput(() => {
      requests += 1;
    });
    command.push("CMCLIENT_");
    command.push(Buffer.from("SHUTDOWN\r\n"));
    command.push("CMCLIENT_SHUTDOWN\n");
    command.end();
    expect(requests).toBe(1);

    const parentExit = createGatewaySupervisorShutdownInput(() => {
      requests += 1;
    });
    parentExit.push("invalid\n");
    parentExit.end();
    expect(requests).toBe(2);

    const oversized = createGatewaySupervisorShutdownInput(() => {
      requests += 1;
    });
    oversized.push("x".repeat(257));
    oversized.push("CMCLIENT_SHUTDOWN\n");
    expect(requests).toBe(3);
  });

  it("settles every step and later phase before reporting a stable failure", async () => {
    const completed: string[] = [];

    await expect(
      runCleanupPhases([
        [
          () => {
            completed.push("http");
          },
          () => {
            completed.push("maintenance");
            throw new Error("fixture maintenance failure");
          },
        ],
        [
          async () => {
            await Promise.resolve();
            completed.push("mesh");
            throw new Error("fixture mesh failure");
          },
          () => {
            completed.push("aprs");
          },
          () => {
            completed.push("proxy");
          },
        ],
        [() => completed.push("jobs")],
        [() => completed.push("database")],
      ]),
    ).rejects.toEqual(new GatewayShutdownError());

    expect(completed).toEqual([
      "http",
      "maintenance",
      "aprs",
      "proxy",
      "mesh",
      "jobs",
      "database",
    ]);
  });

  it("resolves only after every successful cleanup phase", async () => {
    const completed: string[] = [];

    await runCleanupPhases([
      [() => void completed.push("producers")],
      [() => void completed.push("jobs")],
      [() => void completed.push("database")],
    ]);

    expect(completed).toEqual(["producers", "jobs", "database"]);
  });

  it("fails a stalled full cleanup at the outer shutdown deadline", async () => {
    vi.useFakeTimers();
    try {
      let release = (): void => undefined;
      const stopping = runCleanupWithDeadline(
        () =>
          new Promise<void>((resolve) => {
            release = resolve;
          }),
        30_000,
      );
      const rejected = expect(stopping).rejects.toEqual(
        new GatewayShutdownError(),
      );

      await vi.advanceTimersByTimeAsync(30_000);
      await rejected;
      release();
      await Promise.resolve();
    } finally {
      vi.useRealTimers();
    }
  });

  it("includes startup settlement in the outer shutdown deadline", async () => {
    vi.useFakeTimers();
    try {
      let markStartupEntered = (): void => undefined;
      let releaseStartup = (): void => undefined;
      const startupEntered = new Promise<void>((resolve) => {
        markStartupEntered = resolve;
      });
      const startupGate = new Promise<void>((resolve) => {
        releaseStartup = resolve;
      });
      const cleanup = vi.fn();
      const lifecycle = createStartupShutdownCoordinator(cleanup, 30_000);
      const startup = lifecycle.start(async (context) => {
        markStartupEntered();
        await startupGate;
        context.throwIfShutdownRequested();
      });
      await startupEntered;

      const stopping = lifecycle.shutdown();
      const rejected = expect(stopping).rejects.toEqual(
        new GatewayShutdownError(),
      );
      await vi.advanceTimersByTimeAsync(30_000);
      await rejected;
      expect(cleanup).not.toHaveBeenCalled();

      releaseStartup();
      await expect(startup).resolves.toMatchObject({
        ok: false,
        error: expect.any(GatewayStartupCancelledError),
      });
      expect(cleanup).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("shares one terminal cleanup promise across repeated shutdown requests", async () => {
    let release: (() => void) | undefined;
    const cleanup = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          release = resolve;
        }),
    );
    const shutdown = createShutdownCoordinator(cleanup);

    const first = shutdown();
    const second = shutdown();
    expect(second).toBe(first);
    expect(cleanup).toHaveBeenCalledTimes(0);

    await Promise.resolve();
    expect(cleanup).toHaveBeenCalledTimes(1);
    release?.();
    await first;

    expect(shutdown()).toBe(first);
    expect(cleanup).toHaveBeenCalledTimes(1);
  });

  it("cleans every partially acquired resource when listen configuration is invalid", async () => {
    const completed: string[] = [];
    let jobsAcquired = false;
    let databaseAcquired = false;

    const lifecycle = createStartupShutdownCoordinator(() =>
      runCleanupPhases([
        [
          () => {
            if (jobsAcquired) {
              completed.push("jobs");
            }
          },
        ],
        [
          () => {
            if (databaseAcquired) {
              completed.push("database");
            }
          },
        ],
      ]),
    );
    const result = await lifecycle.start(() => {
      databaseAcquired = true;
      jobsAcquired = true;
      parseGatewayListenOptions({ CMCLIENT_GATEWAY_HOST: "0.0.0.0" });
    });

    expect(result).toMatchObject({
      ok: false,
      error: expect.any(GatewayConfigurationError),
    });
    expect(completed).toEqual(["jobs", "database"]);
  });

  it("waits for deferred startup before cleaning a late resource", async () => {
    const completed: string[] = [];
    let releaseStartup = (): void => undefined;
    let markStartupEntered = (): void => undefined;
    let resourceRegistered = false;
    let runtimeStarted = false;
    const startupEntered = new Promise<void>((resolve) => {
      markStartupEntered = resolve;
    });
    const startupGate = new Promise<void>((resolve) => {
      releaseStartup = resolve;
    });
    const cleanup = vi.fn(() => {
      if (resourceRegistered) {
        resourceRegistered = false;
        completed.push("resource-cleaned");
      }
    });
    const lifecycle = createStartupShutdownCoordinator(cleanup);
    const startup = lifecycle.start(async (context) => {
      markStartupEntered();
      await startupGate;
      resourceRegistered = true;
      completed.push("resource-registered");
      context.throwIfShutdownRequested();
      runtimeStarted = true;
    });
    await startupEntered;

    const stopping = lifecycle.shutdown();
    expect(lifecycle.shutdown()).toBe(stopping);
    await Promise.resolve();
    expect(cleanup).not.toHaveBeenCalled();
    releaseStartup();

    await expect(startup).resolves.toMatchObject({
      ok: false,
      error: expect.any(GatewayStartupCancelledError),
    });
    await expect(stopping).resolves.toBeUndefined();
    expect(completed).toEqual(["resource-registered", "resource-cleaned"]);
    expect(resourceRegistered).toBe(false);
    expect(runtimeStarted).toBe(false);
    expect(cleanup).toHaveBeenCalledTimes(1);
  });

  it("drains one periodic operation and handles its rejection", async () => {
    let rejectOperation: ((error: Error) => void) | undefined;
    const reportFailure = vi.fn();
    const operation = new GatewayTrackedOperation(reportFailure);
    const active = operation.run(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectOperation = reject;
        }),
    );

    expect(operation.run(() => Promise.resolve())).toBe(active);
    expect(operation.drain()).toBe(active);
    await Promise.resolve();
    expect(rejectOperation).toBeDefined();
    rejectOperation?.(new Error("fixture refresh failure"));
    await expect(active).resolves.toBeUndefined();
    expect(reportFailure).toHaveBeenCalledWith(
      expect.objectContaining({ message: "fixture refresh failure" }),
    );

    const next = operation.run(() => Promise.resolve());
    expect(next).not.toBe(active);
    await expect(next).resolves.toBeUndefined();

    await operation.stopAndDrain();
    const ignored = vi.fn();
    await operation.run(ignored);
    expect(ignored).not.toHaveBeenCalled();
  });
});
