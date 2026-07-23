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

test("workspace exact-pins the adopted runtime primitives", () => {
  const manifest = readFileSync(join(root, "Cargo.toml"), "utf8");
  const exactPins = new Map([
    ["atomic-write-file", "0.3.0"],
    ["fs4", "1.1.0"],
    ["same-file", "1.0.6"],
    ["time", "0.3.45"],
    ["tokio", "1.53.1"],
    ["tokio-util", "0.7.18"],
    ["tracing", "0.1.44"],
    ["tracing-appender", "0.2.5"],
  ]);

  for (const [dependency, version] of exactPins) {
    const escapedDependency = dependency.replaceAll("-", "\\-");
    const escapedVersion = version.replaceAll(".", "\\.");
    assert.match(
      manifest,
      new RegExp(
        `^${escapedDependency}\\s*=\\s*(?:"=${escapedVersion}"|\\{[^\\n}]*version\\s*=\\s*"=${escapedVersion}"[^\\n}]*\\})\\s*$`,
        "m",
      ),
      `${dependency} must use an exact workspace version`,
    );
  }
});

test("CI compiles all Rust targets at the MSRV and tests the pinned toolchain", () => {
  const workflow = parse(
    readFileSync(join(root, ".github", "workflows", "ci.yml"), "utf8"),
  );
  const msrv = workflow.jobs["rust-msrv"];
  assert.equal(msrv.name, "Rust MSRV 1.87");
  assert.equal(msrv["runs-on"], "ubuntu-22.04");
  assert.ok(
    msrv.steps.some(
      (step) =>
        step.uses ===
        "dtolnay/rust-toolchain@c4743642b206695ff6aa863032b1037759ee95ea",
    ),
  );
  assert.ok(
    msrv.steps.some(
      (step) => step.run === "cargo check --workspace --all-targets --locked",
    ),
  );

  const verify = workflow.jobs.verify;
  assert.ok(
    verify.steps.some(
      (step) =>
        step.uses ===
        "dtolnay/rust-toolchain@191af2e1955bbe165f9bbacff2d2438002dff4d4",
    ),
  );
  assert.ok(
    verify.steps.some((step) => step.run === "cargo test --workspace --locked"),
  );
});

test("production consumers use the shared lock and logging primitives", () => {
  const manifests = [
    "crates/agent-core/Cargo.toml",
    "crates/legacy-migration/Cargo.toml",
    "crates/runtime-primitives/Cargo.toml",
    "crates/updater/Cargo.toml",
  ].map((path) => readFileSync(join(root, path), "utf8"));
  const runtimeLogging = readFileSync(
    join(root, "crates/runtime-logging/src/lib.rs"),
    "utf8",
  );

  assert.equal(
    manifests.some((manifest) => /\bfs2\b/.test(manifest)),
    false,
  );
  assert.match(manifests[0], /cmclient-runtime-primitives/);
  assert.match(manifests[1], /cmclient-runtime-primitives/);
  assert.match(manifests[1], /same-file\.workspace = true/);
  assert.match(manifests[2], /atomic-write-file\.workspace = true/);
  assert.match(manifests[2], /fs4\.workspace = true/);
  assert.match(manifests[2], /same-file\.workspace = true/);
  assert.match(manifests[3], /cmclient-runtime-primitives/);
  assert.doesNotMatch(runtimeLogging, /fn rotate\s*\(/);
  assert.doesNotMatch(runtimeLogging, /fs::rename\s*\(/);
});
