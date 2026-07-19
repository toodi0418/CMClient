export type GatewayCleanupStep = () => void | Promise<void>;

export type GatewayStartupResult<Value> =
  | { ok: true; value: Value }
  | { cleanupError?: unknown; error: unknown; ok: false };

export interface GatewayStartupContext {
  throwIfShutdownRequested(): void;
}

export interface GatewayStartupShutdownCoordinator<Value> {
  readonly shutdownRequested: boolean;
  shutdown(): Promise<void>;
  start(
    startup: (context: GatewayStartupContext) => Value | Promise<Value>,
  ): Promise<GatewayStartupResult<Value>>;
}

export class GatewayShutdownError extends Error {
  readonly code = "GATEWAY_SHUTDOWN_FAILED";

  constructor() {
    super("GATEWAY_SHUTDOWN_FAILED");
    this.name = "GatewayShutdownError";
  }
}

export class GatewayStartupCancelledError extends Error {
  readonly code = "GATEWAY_STARTUP_CANCELLED";

  constructor() {
    super("GATEWAY_STARTUP_CANCELLED");
    this.name = "GatewayStartupCancelledError";
  }
}

export async function runCleanupPhases(
  phases: readonly (readonly GatewayCleanupStep[])[],
): Promise<void> {
  let failed = false;
  for (const phase of phases) {
    const results = await Promise.allSettled(
      phase.map((step) => Promise.resolve().then(step)),
    );
    failed ||= results.some((result) => result.status === "rejected");
  }
  if (failed) {
    throw new GatewayShutdownError();
  }
}

export async function runCleanupWithDeadline(
  cleanup: GatewayCleanupStep,
  timeoutMs: number,
): Promise<void> {
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
    throw new GatewayShutdownError();
  }
  let timer: NodeJS.Timeout | undefined;
  try {
    await Promise.race([
      Promise.resolve().then(cleanup),
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new GatewayShutdownError()), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

export function createShutdownCoordinator(
  cleanup: GatewayCleanupStep,
): () => Promise<void> {
  let shutdownPromise: Promise<void> | undefined;
  return () => {
    shutdownPromise ??= Promise.resolve().then(cleanup);
    return shutdownPromise;
  };
}

export function createStartupShutdownCoordinator<Value>(
  cleanup: GatewayCleanupStep,
  shutdownTimeoutMs?: number,
): GatewayStartupShutdownCoordinator<Value> {
  let shutdownRequested = false;
  let shutdownPromise: Promise<void> | undefined;
  let startupPromise: Promise<GatewayStartupResult<Value>> | undefined;
  let startupSettledResolve = (): void => undefined;
  const startupSettled = new Promise<void>((resolve) => {
    startupSettledResolve = resolve;
  });
  const cleanupOnce = createShutdownCoordinator(() =>
    shutdownTimeoutMs === undefined
      ? Promise.resolve().then(cleanup)
      : runCleanupWithDeadline(cleanup, shutdownTimeoutMs),
  );
  const context: GatewayStartupContext = {
    throwIfShutdownRequested() {
      if (shutdownRequested) {
        throw new GatewayStartupCancelledError();
      }
    },
  };

  const start = (
    startup: (context: GatewayStartupContext) => Value | Promise<Value>,
  ): Promise<GatewayStartupResult<Value>> => {
    startupPromise ??= runStartup(
      startup,
      context,
      startupSettledResolve,
      cleanupOnce,
    );
    return startupPromise;
  };
  const shutdown = (): Promise<void> => {
    shutdownRequested = true;
    const settleAndCleanup = async (): Promise<void> => {
      if (startupPromise) {
        await startupSettled;
      }
      await cleanupOnce();
    };
    shutdownPromise ??=
      shutdownTimeoutMs === undefined
        ? Promise.resolve().then(settleAndCleanup)
        : runCleanupWithDeadline(settleAndCleanup, shutdownTimeoutMs);
    return shutdownPromise;
  };

  return {
    get shutdownRequested() {
      return shutdownRequested;
    },
    shutdown,
    start,
  };
}

async function runStartup<Value>(
  startup: (context: GatewayStartupContext) => Value | Promise<Value>,
  context: GatewayStartupContext,
  markSettled: () => void,
  cleanup: GatewayCleanupStep,
): Promise<GatewayStartupResult<Value>> {
  let result: GatewayStartupResult<Value>;
  try {
    context.throwIfShutdownRequested();
    result = {
      ok: true,
      value: await Promise.resolve().then(() => startup(context)),
    };
  } catch (error) {
    result = { ok: false, error };
  } finally {
    markSettled();
  }
  if (result.ok) {
    return result;
  }
  try {
    await cleanup();
    return result;
  } catch (cleanupError) {
    return { ...result, cleanupError };
  }
}

export class GatewayTrackedOperation {
  private active: Promise<void> | undefined;
  private stopped = false;

  constructor(private readonly reportFailure: (error: unknown) => void) {}

  run(operation: GatewayCleanupStep): Promise<void> {
    if (this.stopped) {
      return Promise.resolve();
    }
    if (this.active) {
      return this.active;
    }
    const active = Promise.resolve()
      .then(operation)
      .catch((error: unknown) => {
        try {
          this.reportFailure(error);
        } catch {
          // Failure reporting must not create an unhandled periodic rejection.
        }
      });
    this.active = active;
    void active.then(() => {
      if (this.active === active) {
        this.active = undefined;
      }
    });
    return active;
  }

  drain(): Promise<void> {
    return this.active ?? Promise.resolve();
  }

  stopAndDrain(): Promise<void> {
    this.stopped = true;
    return this.drain();
  }
}

export interface GatewaySupervisorShutdownInput {
  end(): void;
  push(chunk: string | Uint8Array): void;
}

export function createGatewaySupervisorShutdownInput(
  requestShutdown: () => void,
): GatewaySupervisorShutdownInput {
  const maximumBufferedBytes = 256;
  let buffer = "";
  let requested = false;
  const requestOnce = (): void => {
    if (!requested) {
      requested = true;
      requestShutdown();
    }
  };
  return {
    push(chunk) {
      if (requested) {
        return;
      }
      buffer +=
        typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
      if (Buffer.byteLength(buffer) > maximumBufferedBytes) {
        buffer = "";
        return;
      }
      let newline = buffer.indexOf("\n");
      while (newline >= 0) {
        const line = buffer.slice(0, newline).replace(/\r$/, "");
        buffer = buffer.slice(newline + 1);
        if (line === "CMCLIENT_SHUTDOWN") {
          requestOnce();
          return;
        }
        newline = buffer.indexOf("\n");
      }
    },
    end: requestOnce,
  };
}
