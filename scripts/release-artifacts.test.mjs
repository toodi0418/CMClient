import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  DOCKER_COMPOSITION,
  RELEASE_TARGETS,
  releaseArtifactName,
  releaseArtifactPlan,
  releaseComposition,
  releasePlanDocument,
  stageBuild,
} from "./release-artifacts.mjs";

test("release artifact plan covers every planned component-target pair exactly once", () => {
  const plan = releaseArtifactPlan("2.0.0-dev.0");

  assert.equal(plan.length, 16);
  assert.deepEqual(
    new Set(plan.map(({ fileName }) => fileName)).size,
    plan.length,
  );
  assert.deepEqual(
    plan.find(
      ({ component, target }) =>
        component === "desktop" && target === "darwin-aarch64",
    ),
    {
      component: "desktop",
      target: "darwin-aarch64",
      archive: "tar.zst",
      fileName: "cmclient-desktop-darwin-aarch64-2.0.0-dev.0.tar.zst",
    },
  );
  assert.equal(
    releaseArtifactName({
      component: "cli",
      target: "windows-x86_64",
      version: "2.0.0",
    }),
    "cmclient-cli-windows-x86_64-2.0.0.zip",
  );
  assert.equal(
    releaseArtifactName({
      component: "service",
      target: "windows-x86_64",
      version: "2.0.0",
    }),
    "cmclient-service-windows-x86_64-2.0.0.zip",
  );
});

test("canonical compositions encode complete product surfaces and constrained Docker", () => {
  assert.deepEqual(releaseComposition("cli", "windows-x86_64"), [
    { role: "cli", path: "bin/cmclient.exe", kind: "file", executable: true },
  ]);
  assert.deepEqual(
    releaseComposition("headless", "linux-x86_64").map(({ role }) => role),
    [
      "agent",
      "cli",
      "migration",
      "gateway",
      "web",
      "proto",
      "systemdManager",
      "systemdUnit",
    ],
  );
  assert.deepEqual(
    releaseComposition("desktop", "darwin-aarch64").map(({ role }) => role),
    [
      "desktop",
      "agent",
      "cli",
      "migration",
      "gateway",
      "web",
      "proto",
      "launchdManager",
      "launchdPlist",
    ],
  );
  assert.deepEqual(
    releaseComposition("service", "windows-x86_64").map(({ role }) => role),
    [
      "serviceHost",
      "agent",
      "cli",
      "migration",
      "gateway",
      "web",
      "proto",
      "windowsServiceManager",
    ],
  );
  assert.deepEqual(DOCKER_COMPOSITION, {
    kind: "oci-image",
    updaterManaged: false,
    services: ["gateway", "web"],
    excluded: ["agent", "cli", "desktop", "serviceHost"],
  });

  const document = releasePlanDocument("2.0.0");
  assert.equal(document.schemaVersion, 2);
  assert.deepEqual(document.docker, DOCKER_COMPOSITION);
  assert.equal(document.artifacts[0].contents[0].role, "desktop");
});

test("release targets remain identical to the shared signed-update contract", async () => {
  const updateContract = await readFile(
    "packages/contracts/src/update.ts",
    "utf8",
  );
  const match = updateContract.match(
    /export const UPDATE_TARGETS = \[([\s\S]*?)\] as const;/,
  );
  assert.ok(match, "UPDATE_TARGETS must be declared in the shared contract");
  const contractTargets = Array.from(
    match[1].matchAll(/"([^"]+)"/g),
    ([, target]) => target,
  );

  assert.deepEqual(RELEASE_TARGETS, contractTargets);
});

test("release artifact plan rejects unsupported values and non-SemVer versions", () => {
  assert.throws(
    () =>
      releaseArtifactName({
        component: "gateway",
        target: "linux-x86_64",
        version: "2.0.0",
      }),
    /unknown release component/,
  );
  assert.throws(
    () =>
      releaseArtifactName({
        component: "cli",
        target: "linux-riscv64",
        version: "2.0.0",
      }),
    /unknown release target/,
  );
  assert.throws(() => releaseArtifactPlan("v2.0.0"), /version must be SemVer/);
  assert.throws(
    () =>
      releaseArtifactName({
        component: "service",
        target: "linux-x86_64",
        version: "2.0.0",
      }),
    /unsupported component target/,
  );
});

test("CLI staging contains only the CLI executable and canonical metadata", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-release-cli-"));
  const binary = join(directory, "cmclient-source");
  const output = join(directory, "output");
  t.after(() => rm(directory, { force: true, recursive: true }));
  await writeFile(binary, "fixture executable");

  const { manifest, stagedDirectory } = await stageBuild({
    component: "cli",
    target: "linux-x86_64",
    version: "2.0.0-dev.0",
    inputs: { cli: binary },
    output,
  });

  assert.equal(
    manifest.releaseAsset.fileName,
    "cmclient-cli-linux-x86_64-2.0.0-dev.0.tar.zst",
  );
  assert.equal(
    await readFile(join(stagedDirectory, "bin/cmclient"), "utf8"),
    "fixture executable",
  );
  assert.deepEqual(
    JSON.parse(
      await readFile(join(stagedDirectory, "build-manifest.json"), "utf8"),
    ),
    manifest,
  );
});

test("stage CLI accepts repeated role-path input arguments", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-release-command-"));
  const binary = join(directory, "cmclient-source");
  const output = join(directory, "output");
  t.after(() => rm(directory, { force: true, recursive: true }));
  await writeFile(binary, "fixture executable");

  const result = spawnSync(
    process.execPath,
    [
      "scripts/release-artifacts.mjs",
      "stage",
      "--component",
      "cli",
      "--target",
      "linux-x86_64",
      "--version",
      "2.0.0-dev.0",
      "--input",
      `cli=${binary}`,
      "--output",
      output,
    ],
    { encoding: "utf8" },
  );

  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).component, "cli");
  assert.equal(
    await readFile(join(output, "cli/linux-x86_64/bin/cmclient"), "utf8"),
    "fixture executable",
  );
});

test("Headless staging copies Agent, CLI, production Gateway, Web, and platform service support", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-release-headless-"));
  const output = join(directory, "output");
  t.after(() => rm(directory, { force: true, recursive: true }));
  const inputs = await createCompositionInputs(
    directory,
    "headless",
    "linux-x86_64",
  );

  const { manifest, stagedDirectory } = await stageBuild({
    component: "headless",
    target: "linux-x86_64",
    version: "2.0.0-dev.0",
    inputs,
    output,
  });

  assert.deepEqual(
    manifest.contents.map(({ role }) => role),
    [
      "agent",
      "cli",
      "migration",
      "gateway",
      "web",
      "proto",
      "systemdManager",
      "systemdUnit",
    ],
  );
  assert.ok(
    manifest.files.includes(
      "gateway/node_modules/runtime-package/package.json",
    ),
  );
  assert.equal(
    manifest.files.some((path) => path.includes("node_modules/.bin/")),
    false,
  );
  assert.equal(
    await readFile(join(stagedDirectory, "bin/cmclient-agent"), "utf8"),
    "agent",
  );
  assert.equal(
    await readFile(join(stagedDirectory, "bin/cmclient"), "utf8"),
    "cli",
  );
  assert.equal(
    await readFile(join(stagedDirectory, "bin/cmclient-migrate"), "utf8"),
    "migration",
  );
  assert.equal(
    await readFile(join(stagedDirectory, "gateway/dist/main.js"), "utf8"),
    "gateway-entrypoint",
  );
  assert.equal(
    await readFile(join(stagedDirectory, "web/index.html"), "utf8"),
    "web-index",
  );
  assert.equal(
    await readFile(
      join(stagedDirectory, "proto/meshtastic/mesh.proto"),
      "utf8",
    ),
    'syntax = "proto3";',
  );
  await assert.rejects(() =>
    readFile(join(stagedDirectory, "gateway/node_modules/.bin/tool")),
  );
});

test("staging fails closed on incomplete production inputs and non-canonical roles", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-release-invalid-"));
  t.after(() => rm(directory, { force: true, recursive: true }));
  const inputs = await createCompositionInputs(
    directory,
    "headless",
    "windows-x86_64",
  );

  await rm(join(inputs.gateway, "node_modules"), {
    force: true,
    recursive: true,
  });
  await assert.rejects(
    () =>
      stageBuild({
        component: "headless",
        target: "windows-x86_64",
        version: "2.0.0",
        inputs,
        output: join(directory, "output"),
      }),
    /release production input invalid: gateway missing node_modules/,
  );

  await assert.rejects(
    () =>
      stageBuild({
        component: "cli",
        target: "windows-x86_64",
        version: "2.0.0",
        inputs: { cli: inputs.cli, agent: inputs.agent },
        output: join(directory, "output"),
      }),
    /unexpected release input: agent/,
  );
  await assert.rejects(
    () =>
      stageBuild({
        component: "headless",
        target: "windows-x86_64",
        version: "2.0.0",
        inputs: { cli: inputs.cli },
        output: join(directory, "output"),
      }),
    /missing release input: agent/,
  );
});

test("release workflow builds each composition and gates the separate Docker surface", async () => {
  const [workflow, smoke] = await Promise.all([
    readFile(".github/workflows/release-build.yml", "utf8"),
    readFile("scripts/release-bundle-smoke.sh", "utf8"),
  ]);

  assert.match(workflow, /packages=\(--package cmclient-cli\)/);
  assert.match(
    workflow,
    /headless\) packages\+=\(--package cmclient-agent --package cmclient-legacy-migration\)/,
  );
  assert.match(
    workflow,
    /desktop\) packages\+=\(--package cmclient-agent --package cmclient-legacy-migration --package cmclient-desktop\)/,
  );
  assert.match(
    workflow,
    /service\) packages\+=\(--package cmclient-agent --package cmclient-legacy-migration --package cmclient-service-host\)/,
  );
  for (const role of [
    "cli",
    "agent",
    "migration",
    "gateway",
    "web",
    "proto",
    "desktop",
    "serviceHost",
  ]) {
    assert.match(workflow, new RegExp(`--input "${role}=`));
  }
  assert.match(workflow, /docker-composition:/);
  assert.match(workflow, /os: macos-15\n\s+rust_target: aarch64-apple-darwin/);
  assert.match(
    workflow,
    /os: macos-15-intel\n\s+rust_target: x86_64-apple-darwin/,
  );
  assert.doesNotMatch(workflow, /os: macos-(?:13|14)\b/);
  assert.match(
    workflow,
    /os: ubuntu-22\.04-arm\n\s+rust_target: aarch64-unknown-linux-gnu/,
  );
  assert.match(
    workflow,
    /os: ubuntu-22\.04\n\s+rust_target: x86_64-unknown-linux-gnu/,
  );
  assert.match(workflow, /node --test scripts\/docker\.test\.mjs/);
  assert.match(workflow, /bash scripts\/docker-smoke\.sh/);
  assert.match(workflow, /bash scripts\/release-bundle-smoke\.sh/);
  assert.match(
    workflow,
    /tar --zstd -xf \\\n\s+"release-dist\/cmclient-headless-linux-x86_64-\$version\.tar\.zst"/,
  );
  assert.doesNotMatch(workflow, /--binary|matrix\.package|matrix\.binary/);
  assert.match(smoke, /\[\[ -f "\$executable" \]\]/);
  assert.match(smoke, /"\$target" != windows-\*/);
});

async function createCompositionInputs(root, component, target) {
  const inputs = {};
  for (const content of releaseComposition(component, target)) {
    const source = join(root, "inputs", content.role);
    if (content.kind === "file") {
      await mkdir(join(root, "inputs"), { recursive: true });
      await writeFile(source, content.role);
    } else if (content.role === "gateway") {
      await mkdir(join(source, "dist"), { recursive: true });
      await mkdir(join(source, "node_modules", "runtime-package"), {
        recursive: true,
      });
      await mkdir(join(source, "node_modules", ".bin"), { recursive: true });
      await writeFile(join(source, "dist/main.js"), "gateway-entrypoint");
      await writeFile(
        join(source, "package.json"),
        '{"type":"module","dependencies":{"runtime-package":"1.0.0"}}\n',
      );
      await writeFile(
        join(source, "node_modules/runtime-package/index.js"),
        "runtime-package",
      );
      await writeFile(
        join(source, "node_modules/runtime-package/package.json"),
        '{"name":"runtime-package","version":"1.0.0"}\n',
      );
      await writeFile(
        join(source, "node_modules/.bin/tool"),
        "install-only-wrapper",
      );
    } else if (content.role === "web") {
      await mkdir(join(source, "assets"), { recursive: true });
      await writeFile(join(source, "index.html"), "web-index");
      await writeFile(join(source, "assets/app.js"), "web-asset");
    } else if (content.role === "proto") {
      await mkdir(join(source, "meshtastic"), { recursive: true });
      await writeFile(
        join(source, "meshtastic/mesh.proto"),
        'syntax = "proto3";',
      );
    }
    inputs[content.role] = source;
  }
  return inputs;
}
