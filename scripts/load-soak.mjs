import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const LOAD_VITEST_FILES = [
  "packages/event-client/src/index.test.ts",
  "apps/gateway/src/events.test.ts",
  "apps/gateway/src/jobs.test.ts",
  "apps/gateway/src/maintenance.test.ts",
  "apps/gateway/src/shutdown.test.ts",
  "apps/gateway/src/persistence/database.test.ts",
  "apps/gateway/src/app.test.ts",
  "apps/gateway/src/position.test.ts",
  "apps/gateway/src/position-replay.test.ts",
  "apps/gateway/src/aprs-outbox.test.ts",
  "apps/gateway/src/aprs-monitor.test.ts",
  "apps/gateway/src/aprs-runtime.test.ts",
  "apps/gateway/src/mesh-runtime.test.ts",
  "apps/gateway/src/proxy/sessions.test.ts",
  "apps/gateway/src/proxy/outbound.test.ts",
  "apps/gateway/src/proxy/runtime.test.ts",
  "apps/gateway/src/transport/tcp.test.ts",
  "apps/gateway/src/transport/serial.test.ts",
];

export const LOAD_RUST_PACKAGES = [
  "cmclient-agent-core",
  "cmclient-control-api",
  "cmclient-supervisor",
  "cmclient-agent",
];
export const LOAD_COMMAND_TIMEOUT_MS = 10 * 60 * 1_000;
export const LOAD_TERMINATION_GRACE_MS = 5_000;
export const LOAD_FORCE_KILL_WAIT_MS = 5_000;
export const RUNTIME_SOAK_SCRIPT = "scripts/runtime-soak.mjs";

const PROCESS_GROUP_POLL_INTERVAL_MS = 25;

export function parseSoakIterations(value) {
  const iterations = Number.parseInt(value ?? "1", 10);
  if (!/^[1-9]\d*$/.test(value ?? "1") || iterations > 100) {
    throw new Error(
      "CMCLIENT_SOAK_ITERATIONS must be an integer from 1 to 100",
    );
  }
  return iterations;
}

export function parseCommandTimeout(value) {
  const source = value ?? String(LOAD_COMMAND_TIMEOUT_MS);
  if (!/^[1-9]\d*$/.test(source)) {
    throw new Error(
      "CMCLIENT_LOAD_COMMAND_TIMEOUT_MS must be an integer from 30000 to 3600000",
    );
  }
  const timeout = Number(source);
  if (
    !Number.isSafeInteger(timeout) ||
    timeout < 30_000 ||
    timeout > 3_600_000
  ) {
    throw new Error(
      "CMCLIENT_LOAD_COMMAND_TIMEOUT_MS must be an integer from 30000 to 3600000",
    );
  }
  return timeout;
}

export function packageManagerInvocation({
  nodeExecutable = process.execPath,
  packageManagerEntrypoint = process.env.npm_execpath,
  platform = process.platform,
} = {}) {
  if (platform !== "win32") {
    return { command: "pnpm", arguments: [] };
  }
  if (!packageManagerEntrypoint) {
    throw new Error("Windows load gate must be launched through pnpm");
  }
  return {
    command: nodeExecutable,
    arguments: [packageManagerEntrypoint],
  };
}

export async function runLoadSoak({
  iterations = parseSoakIterations(process.env.CMCLIENT_SOAK_ITERATIONS),
  root = resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  timeoutMs = parseCommandTimeout(process.env.CMCLIENT_LOAD_COMMAND_TIMEOUT_MS),
} = {}) {
  const packageManager = packageManagerInvocation();
  const cargo = process.platform === "win32" ? "cargo.exe" : "cargo";
  const startedAt = Date.now();

  await runCommand(
    packageManager.command,
    [
      ...packageManager.arguments,
      "--filter",
      "@cmclient/contracts",
      "run",
      "build",
    ],
    { cwd: root, timeoutMs },
  );
  await runCommand(
    packageManager.command,
    [
      ...packageManager.arguments,
      "--filter",
      "@cmclient/gateway",
      "run",
      "build",
    ],
    { cwd: root, timeoutMs },
  );
  process.stdout.write(
    `[load-soak] long-lived Gateway runtime: ${iterations} iteration(s) in one process\n`,
  );
  await runCommand(process.execPath, ["--expose-gc", RUNTIME_SOAK_SCRIPT], {
    cwd: root,
    timeoutMs,
    environment: {
      CMCLIENT_RUNTIME_SOAK_ITERATIONS: String(iterations),
    },
  });
  for (let iteration = 1; iteration <= iterations; iteration += 1) {
    process.stdout.write(
      `[load-soak] iteration ${iteration}/${iterations}: Node load surfaces\n`,
    );
    await runCommand(
      packageManager.command,
      [
        ...packageManager.arguments,
        "exec",
        "vitest",
        "run",
        ...LOAD_VITEST_FILES,
      ],
      { cwd: root, timeoutMs },
    );

    process.stdout.write(
      `[load-soak] iteration ${iteration}/${iterations}: Rust resource surfaces\n`,
    );
    await runCommand(
      cargo,
      [
        "test",
        "--locked",
        "--no-fail-fast",
        ...LOAD_RUST_PACKAGES.flatMap((name) => ["-p", name]),
      ],
      { cwd: root, timeoutMs },
    );
  }

  const summary = {
    elapsedSeconds: Math.ceil((Date.now() - startedAt) / 1_000),
    iterations,
    nodeFiles: LOAD_VITEST_FILES.length,
    rustPackages: LOAD_RUST_PACKAGES.length,
    runtimeSoak: true,
    status: "passed",
  };
  process.stdout.write(`${JSON.stringify(summary)}\n`);
  return summary;
}

export async function runCommand(
  command,
  arguments_,
  {
    cwd,
    environment = {},
    forceKillWaitMs = LOAD_FORCE_KILL_WAIT_MS,
    terminationGraceMs = LOAD_TERMINATION_GRACE_MS,
    timeoutMs,
  },
) {
  validateRunTiming(timeoutMs, terminationGraceMs, forceKillWaitMs);
  const child = spawn(command, arguments_, {
    cwd,
    detached: process.platform !== "win32",
    env: { ...process.env, ...environment },
    stdio: "inherit",
    windowsHide: true,
  });
  const completion = childCompletion(child);
  let timeoutTimer;
  const result = await Promise.race([
    completion,
    new Promise((resolveTimeout) => {
      timeoutTimer = globalThis.setTimeout(
        () => resolveTimeout({ kind: "timeout" }),
        timeoutMs,
      );
    }),
  ]);
  globalThis.clearTimeout(timeoutTimer);

  if (result.kind === "timeout") {
    await terminateProcessTree(child, completion, {
      forceKillWaitMs,
      terminationGraceMs,
    });
    if (child.exitCode === null && child.signalCode === null) {
      child.unref();
    }
    throw new Error(`${command} exceeded ${timeoutMs}ms timeout`);
  }
  if (result.kind === "error") {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(
      `${command} exited with status ${result.status ?? "unknown"}`,
    );
  }
}

function childCompletion(child) {
  return new Promise((resolveCompletion) => {
    child.once("error", (error) => resolveCompletion({ kind: "error", error }));
    child.once("close", (status, signal) =>
      resolveCompletion({ kind: "exit", signal, status }),
    );
  });
}

async function terminateProcessTree(
  child,
  completion,
  { forceKillWaitMs, terminationGraceMs },
) {
  const pid = child.pid;
  if (!pid) {
    return;
  }
  if (process.platform === "win32") {
    void runTaskkill(pid, false, terminationGraceMs);
    await delay(terminationGraceMs);
    await runTaskkill(pid, true, forceKillWaitMs);
    tryKillChild(child);
    await waitForCompletion(completion, forceKillWaitMs);
    return;
  }

  signalPosixProcessGroup(pid, "SIGTERM", child);
  if (await waitForPosixProcessGroupExit(pid, terminationGraceMs)) {
    await waitForCompletion(completion, forceKillWaitMs);
    return;
  }
  signalPosixProcessGroup(pid, "SIGKILL", child);
  await Promise.all([
    waitForPosixProcessGroupExit(pid, forceKillWaitMs),
    waitForCompletion(completion, forceKillWaitMs),
  ]);
}

function signalPosixProcessGroup(pid, signal, child) {
  try {
    process.kill(-pid, signal);
  } catch (error) {
    if (error?.code !== "ESRCH") {
      tryKillChild(child, signal);
    }
  }
}

async function waitForPosixProcessGroupExit(pid, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (isPosixProcessGroupAlive(pid) && Date.now() < deadline) {
    await delay(
      Math.min(PROCESS_GROUP_POLL_INTERVAL_MS, deadline - Date.now()),
    );
  }
  return !isPosixProcessGroupAlive(pid);
}

function isPosixProcessGroupAlive(pid) {
  try {
    process.kill(-pid, 0);
    return true;
  } catch (error) {
    return error?.code !== "ESRCH";
  }
}

function runTaskkill(pid, force, timeoutMs) {
  return new Promise((resolveTaskkill) => {
    const taskkill = spawn(
      "taskkill.exe",
      ["/PID", String(pid), "/T", ...(force ? ["/F"] : [])],
      { stdio: "ignore", windowsHide: true },
    );
    let settled = false;
    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      globalThis.clearTimeout(timer);
      resolveTaskkill();
    };
    const timer = globalThis.setTimeout(() => {
      tryKillChild(taskkill);
      finish();
    }, timeoutMs);
    taskkill.once("error", finish);
    taskkill.once("close", finish);
  });
}

function tryKillChild(child, signal = "SIGKILL") {
  try {
    child.kill(signal);
  } catch {
    // The process already exited or the platform tree-kill command owns it.
  }
}

async function waitForCompletion(completion, timeoutMs) {
  let timer;
  try {
    await Promise.race([
      completion,
      new Promise((resolveTimeout) => {
        timer = globalThis.setTimeout(resolveTimeout, timeoutMs);
      }),
    ]);
  } finally {
    globalThis.clearTimeout(timer);
  }
}

function delay(milliseconds) {
  return new Promise((resolveDelay) =>
    globalThis.setTimeout(resolveDelay, milliseconds),
  );
}

function validateRunTiming(timeoutMs, terminationGraceMs, forceKillWaitMs) {
  for (const value of [timeoutMs, terminationGraceMs, forceKillWaitMs]) {
    if (!Number.isSafeInteger(value) || value < 1) {
      throw new Error("load command timing is invalid");
    }
  }
}

const isMain = process.argv[1]
  ? resolve(process.argv[1]) === fileURLToPath(import.meta.url)
  : false;
if (isMain) {
  try {
    await runLoadSoak();
  } catch (error) {
    process.stderr.write(
      `[load-soak] ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  }
}
