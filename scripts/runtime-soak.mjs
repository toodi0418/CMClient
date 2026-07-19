import { readdirSync } from "node:fs";
import http from "node:http";
import { dirname, resolve } from "node:path";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_CYCLES = 12;
const DEFAULT_CLIENTS = 16;
const DEFAULT_EVENTS_PER_CYCLE = 2_000;
const DEFAULT_REQUESTS_PER_CYCLE = 50;
const DEFAULT_RSS_GROWTH_BYTES = 64 * 1024 * 1024;
const DEFAULT_FD_GROWTH = 8;
const DEFAULT_ACTIVE_RESOURCE_GROWTH = 8;
const DEFAULT_EVENT_LOOP_P99_MS = 500;
const REPLAY_BUFFER_SIZE = 256;
const SSE_TARGET_TIMEOUT_MS = 15_000;

export function runtimeSoakConfiguration(environment = process.env) {
  return {
    activeResourceGrowthLimit: boundedInteger(
      environment.CMCLIENT_RUNTIME_SOAK_ACTIVE_RESOURCE_GROWTH,
      DEFAULT_ACTIVE_RESOURCE_GROWTH,
      0,
      1_024,
    ),
    clients: boundedInteger(
      environment.CMCLIENT_RUNTIME_SOAK_CLIENTS,
      DEFAULT_CLIENTS,
      1,
      64,
    ),
    cycles: boundedInteger(
      environment.CMCLIENT_RUNTIME_SOAK_CYCLES,
      DEFAULT_CYCLES,
      2,
      100,
    ),
    eventLoopP99LimitMs: boundedInteger(
      environment.CMCLIENT_RUNTIME_SOAK_EVENT_LOOP_P99_MS,
      DEFAULT_EVENT_LOOP_P99_MS,
      50,
      10_000,
    ),
    eventsPerCycle: boundedInteger(
      environment.CMCLIENT_RUNTIME_SOAK_EVENTS_PER_CYCLE,
      DEFAULT_EVENTS_PER_CYCLE,
      100,
      100_000,
    ),
    fdGrowthLimit: boundedInteger(
      environment.CMCLIENT_RUNTIME_SOAK_FD_GROWTH,
      DEFAULT_FD_GROWTH,
      0,
      1_024,
    ),
    iterations: boundedInteger(
      environment.CMCLIENT_RUNTIME_SOAK_ITERATIONS,
      1,
      1,
      100,
    ),
    requestsPerCycle: boundedInteger(
      environment.CMCLIENT_RUNTIME_SOAK_REQUESTS_PER_CYCLE,
      DEFAULT_REQUESTS_PER_CYCLE,
      1,
      10_000,
    ),
    rssGrowthLimitBytes:
      boundedInteger(
        environment.CMCLIENT_RUNTIME_SOAK_RSS_GROWTH_MIB,
        DEFAULT_RSS_GROWTH_BYTES / 1024 / 1024,
        1,
        4_096,
      ) *
      1024 *
      1024,
  };
}

export async function runRuntimeSoak(config = runtimeSoakConfiguration()) {
  if (typeof globalThis.gc !== "function") {
    throw new Error("runtime soak requires node --expose-gc");
  }
  const [{ createGatewayApp }, { DomainEventBus }] = await Promise.all([
    import(resolve(root, "apps/gateway/dist/app.js")),
    import(resolve(root, "apps/gateway/dist/events.js")),
  ]);
  const eventBus = new DomainEventBus({
    bufferSize: REPLAY_BUFFER_SIZE,
    maxSubscribers: 64,
  });
  const logger = { log() {} };
  const app = createGatewayApp(logger, undefined, eventBus, {
    heartbeatIntervalMs: 1_000,
  });
  const agent = new http.Agent({ keepAlive: true, maxSockets: 8 });
  const eventLoop = monitorEventLoopDelay({ resolution: 20 });
  eventLoop.enable();
  let address;

  try {
    await app.listen({ host: "127.0.0.1", port: 0 });
    const bound = app.server.address();
    if (!bound || typeof bound === "string") {
      throw new Error("runtime soak listener address unavailable");
    }
    address = `http://127.0.0.1:${bound.port}`;

    await runCycle({ address, agent, config, eventBus, sequence: 0 });
    await settle();
    globalThis.gc();
    await settle();
    const baselineFds = openFileDescriptorCount();
    const baselineActiveResources = activeResourceCount();
    const baselineRss = process.memoryUsage().rss;
    let maximumActiveResources = baselineActiveResources;
    let maximumFds = baselineFds;
    let maximumRss = baselineRss;

    const totalCycles = config.cycles * config.iterations;
    for (let cycle = 1; cycle <= totalCycles; cycle += 1) {
      await runCycle({ address, agent, config, eventBus, sequence: cycle });
      await settle();
      globalThis.gc();
      await settle();
      if (eventBus.metricsSnapshot.subscriberCount !== 0) {
        throw new Error("runtime soak SSE subscriber count did not plateau");
      }
      const activeResources = activeResourceCount();
      maximumActiveResources = Math.max(
        maximumActiveResources,
        activeResources,
      );
      if (
        activeResources >
        baselineActiveResources + config.activeResourceGrowthLimit
      ) {
        throw new Error(
          `runtime soak active-resource growth exceeded limit: ${activeResources - baselineActiveResources}`,
        );
      }
      const fds = openFileDescriptorCount();
      if (fds !== undefined) {
        maximumFds = Math.max(maximumFds ?? fds, fds);
        if (
          baselineFds !== undefined &&
          fds > baselineFds + config.fdGrowthLimit
        ) {
          throw new Error(
            `runtime soak FD growth exceeded limit: ${fds - baselineFds}`,
          );
        }
      }
      const rss = process.memoryUsage().rss;
      maximumRss = Math.max(maximumRss, rss);
      if (rss > baselineRss + config.rssGrowthLimitBytes) {
        throw new Error(
          `runtime soak RSS growth exceeded limit: ${rss - baselineRss}`,
        );
      }
    }

    const eventLoopP99Ms = eventLoop.percentile(99) / 1_000_000;
    if (eventLoopP99Ms > config.eventLoopP99LimitMs) {
      throw new Error(
        `runtime soak event-loop p99 exceeded limit: ${eventLoopP99Ms.toFixed(1)}ms`,
      );
    }
    const summary = {
      activeResourceGrowth: maximumActiveResources - baselineActiveResources,
      baselineActiveResources,
      baselineFds,
      cycles: totalCycles,
      eventLoopP99Ms: Number(eventLoopP99Ms.toFixed(1)),
      fileDescriptorGate:
        baselineFds === undefined ? "active-resource-fallback" : "native",
      maximumFds,
      iterations: config.iterations,
      rssGrowthBytes: maximumRss - baselineRss,
      status: "passed",
      subscriberCount: eventBus.metricsSnapshot.subscriberCount,
    };
    process.stdout.write(`[runtime-soak] ${JSON.stringify(summary)}\n`);
    return summary;
  } finally {
    eventLoop.disable();
    agent.destroy();
    await app.close();
  }
}

async function runCycle({ address, agent, config, eventBus, sequence }) {
  const finalEventMarker = `"cycle":${sequence},"index":${config.eventsPerCycle - 1}`;
  const sessions = Array.from({ length: config.clients }, () =>
    openSse(`${address}/api/v1/events`, "soak.tick", finalEventMarker),
  );
  try {
    await Promise.all(sessions.map((session) => session.connected));
    for (let index = 0; index < config.eventsPerCycle; index += 1) {
      eventBus.publish({
        type: "soak.tick",
        source: "load-gate",
        payload: { cycle: sequence, index },
      });
      if ((index + 1) % 16 === 0) {
        await new Promise((resolve) => globalThis.setImmediate(resolve));
      }
    }
    await Promise.all(sessions.map((session) => session.received));
    if (eventBus.metricsSnapshot.subscriberCount !== config.clients) {
      throw new Error(
        "runtime soak closed an SSE client before the final event",
      );
    }
  } finally {
    for (const session of sessions) {
      session.close();
    }
  }
  await Promise.all(
    Array.from({ length: config.requestsPerCycle }, () =>
      requestHealth(`${address}/api/v1/system/health`, agent),
    ),
  );
  await waitFor(() => eventBus.metricsSnapshot.subscriberCount === 0);
  const expectedReplayBufferSize = replayBufferTarget(
    config.eventsPerCycle,
    sequence,
  );
  if (eventBus.replayBufferSize !== expectedReplayBufferSize) {
    throw new Error(
      `runtime soak replay buffer expected ${expectedReplayBufferSize}, received ${eventBus.replayBufferSize}`,
    );
  }
}

export function replayBufferTarget(eventsPerCycle, sequence) {
  return Math.min(REPLAY_BUFFER_SIZE, eventsPerCycle * (sequence + 1));
}

function openSse(url, eventType, finalEventMarker) {
  let request;
  let response;
  let buffer = "";
  let connectedResolve;
  let connectedReject;
  let receivedResolve;
  let receivedReject;
  let receivedFinalEvent = false;
  const connected = new Promise((resolve, reject) => {
    connectedResolve = resolve;
    connectedReject = reject;
  });
  const received = new Promise((resolve, reject) => {
    receivedResolve = resolve;
    receivedReject = reject;
  });
  void received.catch(() => undefined);
  let targetTimer = globalThis.setTimeout(() => {
    receivedReject(new Error("runtime soak SSE final event timed out"));
  }, SSE_TARGET_TIMEOUT_MS);
  const clearTargetTimer = () => {
    if (targetTimer) {
      globalThis.clearTimeout(targetTimer);
      targetTimer = undefined;
    }
  };
  const rejectBeforeFinalEvent = (error) => {
    clearTargetTimer();
    if (!receivedFinalEvent) {
      receivedReject(error);
    }
  };
  request = http.get(
    url,
    { headers: { accept: "text/event-stream" } },
    (incoming) => {
      response = incoming;
      if (incoming.statusCode !== 200) {
        const error = new Error(
          `runtime soak SSE returned ${incoming.statusCode}`,
        );
        connectedReject(error);
        rejectBeforeFinalEvent(error);
        incoming.resume();
        return;
      }
      connectedResolve();
      incoming.setEncoding("utf8");
      incoming.on("data", (chunk) => {
        buffer = `${buffer}${chunk}`.slice(-64 * 1024);
        if (
          buffer.includes(`event: ${eventType}\n`) &&
          buffer.includes(finalEventMarker)
        ) {
          receivedFinalEvent = true;
          clearTargetTimer();
          receivedResolve();
        }
      });
      incoming.once("aborted", () =>
        rejectBeforeFinalEvent(new Error("runtime soak SSE aborted")),
      );
      incoming.once("end", () =>
        rejectBeforeFinalEvent(
          new Error("runtime soak SSE ended before the final event"),
        ),
      );
      incoming.once("error", (error) => rejectBeforeFinalEvent(error));
      incoming.once("close", () =>
        rejectBeforeFinalEvent(
          new Error("runtime soak SSE closed before the final event"),
        ),
      );
    },
  );
  request.setTimeout(10_000, () => {
    const error = new Error("runtime soak SSE timed out");
    connectedReject(error);
    rejectBeforeFinalEvent(error);
    request.destroy(error);
  });
  request.on("error", (error) => {
    if (!response) {
      connectedReject(error);
    }
    rejectBeforeFinalEvent(error);
  });
  return {
    close() {
      clearTargetTimer();
      response?.destroy();
      request.destroy();
    },
    connected,
    received,
  };
}

function requestHealth(url, agent) {
  return new Promise((resolve, reject) => {
    const request = http.get(url, { agent }, (response) => {
      if (response.statusCode !== 200) {
        reject(
          new Error(`runtime soak health returned ${response.statusCode}`),
        );
        response.resume();
        return;
      }
      response.on("error", reject);
      response.on("end", resolve);
      response.resume();
    });
    request.setTimeout(10_000, () =>
      request.destroy(new Error("runtime soak health timed out")),
    );
    request.on("error", reject);
  });
}

async function waitFor(predicate) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => globalThis.setImmediate(resolve));
  }
  throw new Error("runtime soak resource did not settle");
}

async function settle() {
  await new Promise((resolve) => globalThis.setTimeout(resolve, 25));
}

function openFileDescriptorCount() {
  for (const directory of ["/proc/self/fd", "/dev/fd"]) {
    try {
      return readdirSync(directory).length;
    } catch {
      // The next platform path may be available.
    }
  }
  return undefined;
}

function activeResourceCount() {
  return process.getActiveResourcesInfo().length;
}

function boundedInteger(value, fallback, minimum, maximum) {
  const source = value ?? String(fallback);
  if (!/^\d+$/.test(source)) {
    throw new Error("runtime soak configuration is invalid");
  }
  const parsed = Number(source);
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error("runtime soak configuration is invalid");
  }
  return parsed;
}

const isMain = process.argv[1]
  ? resolve(process.argv[1]) === fileURLToPath(import.meta.url)
  : false;
if (isMain) {
  try {
    if (process.argv.includes("--describe")) {
      process.stdout.write(`${JSON.stringify(runtimeSoakConfiguration({}))}\n`);
    } else {
      await runRuntimeSoak();
    }
  } catch (error) {
    process.stderr.write(
      `[runtime-soak] ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  }
}
