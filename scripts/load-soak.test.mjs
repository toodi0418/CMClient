import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  LOAD_RUST_PACKAGES,
  LOAD_VITEST_FILES,
  RUNTIME_SOAK_SCRIPT,
  packageManagerInvocation,
  parseCommandTimeout,
  parseSoakIterations,
  runCommand,
} from "./load-soak.mjs";
import {
  replayBufferTarget,
  runtimeModuleUrl,
  runtimeSoakConfiguration,
} from "./runtime-soak.mjs";

test("load gate covers every bounded runtime surface", () => {
  for (const required of [
    "events.test.ts",
    "jobs.test.ts",
    "maintenance.test.ts",
    "shutdown.test.ts",
    "persistence/database.test.ts",
    "position-replay.test.ts",
    "aprs-outbox.test.ts",
    "aprs-monitor.test.ts",
    "aprs-runtime.test.ts",
    "mesh-runtime.test.ts",
    "proxy/runtime.test.ts",
    "transport/tcp.test.ts",
    "transport/serial.test.ts",
    "event-client/src/index.test.ts",
  ]) {
    assert.ok(
      LOAD_VITEST_FILES.some((file) => file.endsWith(required)),
      `missing load surface ${required}`,
    );
  }
  assert.deepEqual(LOAD_RUST_PACKAGES, [
    "cmclient-agent-core",
    "cmclient-control-api",
    "cmclient-supervisor",
    "cmclient-agent",
  ]);
});

test("soak iterations are explicit and bounded", () => {
  assert.equal(parseSoakIterations(undefined), 1);
  assert.equal(parseSoakIterations("10"), 10);
  for (const invalid of ["", "0", "01", "1.5", "101", "not-a-number"]) {
    assert.throws(() => parseSoakIterations(invalid));
  }
});

test("load commands have a bounded timeout", () => {
  expectEqual(parseCommandTimeout(undefined), 600_000);
  expectEqual(parseCommandTimeout("30000"), 30_000);
  expectEqual(parseCommandTimeout("3600000"), 3_600_000);
  for (const invalid of ["", "0", "29999", "3600001", "1.5", "timeout"]) {
    assert.throws(() => parseCommandTimeout(invalid));
  }
});

test("Windows load commands invoke pnpm without a command shell", () => {
  assert.deepEqual(
    packageManagerInvocation({
      nodeExecutable: "node.exe",
      packageManagerEntrypoint: "C:\\tools\\pnpm.cjs",
      platform: "win32",
    }),
    {
      command: "node.exe",
      arguments: ["C:\\tools\\pnpm.cjs"],
    },
  );
  assert.deepEqual(packageManagerInvocation({ platform: "linux" }), {
    command: "pnpm",
    arguments: [],
  });
  assert.throws(
    () =>
      packageManagerInvocation({
        packageManagerEntrypoint: "",
        platform: "win32",
      }),
    /must be launched through pnpm/,
  );
});

test("async command runner preserves success and exit-status semantics", async () => {
  const options = {
    cwd: process.cwd(),
    forceKillWaitMs: 500,
    terminationGraceMs: 150,
    timeoutMs: 2_000,
  };
  await runCommand(process.execPath, ["-e", "process.exit(0)"], options);
  await assert.rejects(
    runCommand(process.execPath, ["-e", "process.exit(7)"], options),
    /exited with status 7/,
  );
});

test("hard timeout removes a signal-resistant child process tree", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-load-timeout-"));
  const marker = join(directory, "grandchild-survived");
  t.after(() => rm(directory, { force: true, recursive: true }));
  const grandchildSource = `
    const { writeFileSync } = require("node:fs");
    process.on("SIGTERM", () => undefined);
    setTimeout(
      () => writeFileSync(process.env.CMCLIENT_TIMEOUT_MARKER, "survived"),
      1600,
    );
    setInterval(() => undefined, 1000);
  `;
  const parentSource = `
    const { spawn } = require("node:child_process");
    const child = spawn(process.execPath, ["-e", ${JSON.stringify(grandchildSource)}], {
      env: process.env,
      stdio: "ignore",
      windowsHide: true,
    });
    child.once("error", () => process.exit(2));
    process.on("SIGTERM", () => undefined);
    setInterval(() => undefined, 1000);
  `;
  const timeoutMs = 800;
  const terminationGraceMs = 150;
  const forceKillWaitMs = 500;
  const startedAt = Date.now();

  await assert.rejects(
    runCommand(process.execPath, ["-e", parentSource], {
      cwd: process.cwd(),
      environment: { CMCLIENT_TIMEOUT_MARKER: marker },
      forceKillWaitMs,
      terminationGraceMs,
      timeoutMs,
    }),
    /exceeded 800ms timeout/,
  );

  const elapsedMs = Date.now() - startedAt;
  assert.ok(
    elapsedMs < timeoutMs + terminationGraceMs + forceKillWaitMs + 1_000,
    `process-tree teardown exceeded its bounded margin: ${elapsedMs}ms`,
  );
  await delay(Math.max(0, 2_200 - elapsedMs));
  assert.equal(existsSync(marker), false, "timed-out grandchild survived");
});

test("runtime soak exposes explicit RSS, FD, subscriber, and event-loop gates", () => {
  assert.equal(existsSync(RUNTIME_SOAK_SCRIPT), true);
  const result = spawnSync(
    process.execPath,
    [RUNTIME_SOAK_SCRIPT, "--describe"],
    {
      encoding: "utf8",
    },
  );
  assert.equal(result.status, 0, result.stderr);
  const configuration = JSON.parse(result.stdout);
  assert.deepEqual(configuration, {
    activeResourceGrowthLimit: 8,
    clients: 16,
    cycles: 12,
    eventLoopP99LimitMs: 500,
    eventsPerCycle: 2_000,
    fdGrowthLimit: 8,
    iterations: 1,
    requestsPerCycle: 50,
    rssGrowthLimitBytes: 64 * 1024 * 1024,
  });
});

test("runtime soak converts platform paths to file URLs", () => {
  const modulePath = resolve("apps/gateway/dist/app.js");
  const moduleUrl = runtimeModuleUrl(modulePath);
  assert.equal(new URL(moduleUrl).protocol, "file:");
  assert.equal(fileURLToPath(moduleUrl), modulePath);
});

test("runtime iterations share one process and replay capacity fills incrementally", () => {
  const configuration = runtimeSoakConfiguration({
    CMCLIENT_RUNTIME_SOAK_ITERATIONS: "10",
  });
  assert.equal(configuration.iterations, 10);
  assert.equal(replayBufferTarget(100, 0), 100);
  assert.equal(replayBufferTarget(100, 1), 200);
  assert.equal(replayBufferTarget(100, 2), 256);
  assert.equal(replayBufferTarget(2_000, 0), 256);
  for (const invalid of ["", "0", "101", "1.5", "invalid"]) {
    assert.throws(() =>
      runtimeSoakConfiguration({
        CMCLIENT_RUNTIME_SOAK_ITERATIONS: invalid,
      }),
    );
  }
});

function expectEqual(actual, expected) {
  assert.equal(actual, expected);
}

function delay(milliseconds) {
  return new Promise((resolve) => globalThis.setTimeout(resolve, milliseconds));
}
