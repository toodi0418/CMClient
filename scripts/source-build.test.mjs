import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { parse } from "yaml";

import {
  JAVASCRIPT_SOURCE_BUILD_STEPS,
  WINDOWS_RUST_SOURCE_BUILD_STEP,
  runSourceBuild,
} from "./source-build.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

test("Desktop direct build establishes the contracts output first", () => {
  const desktopPackage = JSON.parse(
    readFileSync(join(root, "apps", "desktop", "package.json"), "utf8"),
  );
  assert.match(
    desktopPackage.scripts.build,
    /^pnpm --filter @cmclient\/contracts run build && /,
  );
});

test("workspace exposes the canonical Windows source-build entrypoint", () => {
  const rootPackage = JSON.parse(
    readFileSync(join(root, "package.json"), "utf8"),
  );
  assert.equal(
    rootPackage.scripts["build:source:windows"],
    "node scripts/source-build.mjs windows",
  );
  assert.equal(existsSync(join(root, "scripts", "source-build.mjs")), true);
});

test("canonical source build locks dependency and Windows target ordering", async () => {
  const calls = [];
  await runSourceBuild({
    mode: "windows",
    nodeExecutable: "node.exe",
    packageManagerEntrypoint: "C:\\tools\\pnpm.cjs",
    platform: "win32",
    root,
    runner: async (command, arguments_, options) => {
      calls.push({ command, arguments_, cwd: options.cwd });
    },
  });

  assert.deepEqual(
    calls,
    [...JAVASCRIPT_SOURCE_BUILD_STEPS, WINDOWS_RUST_SOURCE_BUILD_STEP].map(
      (step) => ({
        command: step.command === "pnpm" ? "node.exe" : "cargo.exe",
        arguments_:
          step.command === "pnpm"
            ? ["C:\\tools\\pnpm.cjs", ...step.arguments]
            : [...step.arguments],
        cwd: root,
      }),
    ),
  );

  const serialized = JSON.stringify(
    calls.map(({ command, arguments_ }) => ({ command, arguments_ })),
  ).toLowerCase();
  for (const forbidden of ["tauri", "nsis", "stage", "updater", "deploy"]) {
    assert.equal(serialized.includes(forbidden), false, forbidden);
  }
});

test("CI keeps the Windows source gate separate from package generation", () => {
  const workflow = parse(
    readFileSync(join(root, ".github", "workflows", "ci.yml"), "utf8"),
  );
  const job = workflow.jobs["windows-source-build"];
  assert.equal(job["runs-on"], "windows-latest");
  assert.equal(job["timeout-minutes"], 30);
  const build = job.steps.find(
    (step) => step.run === "pnpm build:source:windows",
  );
  assert.ok(build);
  assert.equal(
    build.env.CARGO_TARGET_DIR,
    "${{ runner.temp }}/cmclient-source-target",
  );
  const privateGateway = job.steps.find((step) =>
    step.run?.includes(
      "tests::supervised_real_gateway_uses_private_dynamic_session",
    ),
  );
  assert.ok(privateGateway);
  assert.match(privateGateway.run, /--ignored --exact$/);
  assert.equal(
    privateGateway.env.CARGO_TARGET_DIR,
    "${{ runner.temp }}/cmclient-source-target",
  );
  const serialized = JSON.stringify(job).toLowerCase();
  for (const forbidden of ["tauri build", "nsis", "stage", "updater payload"]) {
    assert.equal(serialized.includes(forbidden), false, forbidden);
  }
});
