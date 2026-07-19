import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

import {
  DOCKER_COMPOSITION,
  DOCKER_PLATFORMS,
  NATIVE_DESKTOP_BUNDLES,
  RELEASE_TARGETS,
  dockerArtifactPlan,
  dockerComposeArtifactPlan,
  nativeDesktopArtifactName,
  nativeDesktopArtifactPlan,
  releaseArtifactName,
  releaseArtifactPlan,
  releaseComposition,
  releasePlanDocument,
  stageBuild,
} from "./release-artifacts.mjs";
import {
  collectNativeDesktopBundles,
  tauriPackageVersion,
  tauriReleaseConfig,
  verifyBundledDesktopRuntime,
  verifyNativeDesktopStage,
} from "./desktop-native-bundles.mjs";

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
    services: ["gateway", "web", "ingress"],
    excluded: ["agent", "cli", "desktop", "serviceHost"],
  });

  const document = releasePlanDocument("2.0.0");
  assert.equal(document.schemaVersion, 4);
  const {
    artifacts: dockerArtifacts,
    compose: dockerCompose,
    ...dockerComposition
  } = document.docker;
  assert.deepEqual(dockerComposition, DOCKER_COMPOSITION);
  assert.deepEqual(dockerArtifacts, dockerArtifactPlan("2.0.0"));
  assert.deepEqual(dockerCompose, dockerComposeArtifactPlan("2.0.0"));
  assert.equal(document.artifacts[0].contents[0].role, "desktop");
  assert.deepEqual(document.nativeDesktop, nativeDesktopArtifactPlan("2.0.0"));
});

test("Docker artifact plan covers native x64 and ARM64 without Agent updater ownership", () => {
  const version = "2.0.0-rc.1";
  const plan = dockerArtifactPlan(version);
  assert.deepEqual(DOCKER_PLATFORMS, [
    {
      target: "linux-x86_64",
      platform: "linux/amd64",
      architecture: "amd64",
    },
    {
      target: "linux-aarch64",
      platform: "linux/arm64",
      architecture: "arm64",
    },
  ]);
  assert.equal(plan.length, 2);
  assert.equal(new Set(plan.map(({ fileName }) => fileName)).size, 2);
  assert.deepEqual(
    plan.map(({ target, fileName, updaterManaged }) => ({
      target,
      fileName,
      updaterManaged,
    })),
    [
      {
        target: "linux-x86_64",
        fileName: "cmclient-docker-linux-x86_64-2.0.0-rc.1.oci.tar",
        updaterManaged: false,
      },
      {
        target: "linux-aarch64",
        fileName: "cmclient-docker-linux-aarch64-2.0.0-rc.1.oci.tar",
        updaterManaged: false,
      },
    ],
  );
});

test("Docker Compose descriptor is versioned and excluded from Agent updates", () => {
  assert.deepEqual(dockerComposeArtifactPlan("2.0.0-rc.1"), {
    component: "docker",
    kind: "compose-descriptor",
    version: "2.0.0-rc.1",
    sourcePath: "docker-compose.yml",
    fileName: "cmclient-docker-compose-2.0.0-rc.1.yml",
    updaterManaged: false,
  });
});

test("native Desktop plan covers installer formats on every supported target", () => {
  const version = "2.0.0-rc.1";
  const plan = nativeDesktopArtifactPlan(version);
  assert.equal(plan.length, 8);
  assert.deepEqual(NATIVE_DESKTOP_BUNDLES, {
    "darwin-aarch64": ["dmg"],
    "darwin-x86_64": ["dmg"],
    "linux-aarch64": ["deb", "appimage"],
    "linux-x86_64": ["deb", "appimage"],
    "windows-x86_64": ["msi", "nsis"],
  });
  assert.equal(new Set(plan.map(({ fileName }) => fileName)).size, plan.length);
  assert.deepEqual(
    plan.find(
      ({ target, bundle }) =>
        target === "linux-aarch64" && bundle === "appimage",
    ),
    {
      component: "desktop",
      target: "linux-aarch64",
      bundle: "appimage",
      fileName: "cmclient-desktop-linux-aarch64-2.0.0-rc.1.AppImage",
      updaterManaged: false,
    },
  );
  assert.equal(
    nativeDesktopArtifactName({
      target: "windows-x86_64",
      bundle: "nsis",
      version,
    }),
    "cmclient-desktop-windows-x86_64-2.0.0-rc.1.setup.exe",
  );
  assert.throws(
    () =>
      nativeDesktopArtifactName({
        target: "darwin-aarch64",
        bundle: "msi",
        version,
      }),
    /unsupported native Desktop bundle/,
  );
});

test("Tauri release config embeds the complete portable Desktop composition", () => {
  const config = tauriReleaseConfig({
    target: "windows-x86_64",
    version: "2.0.0-rc.1",
    portable: "release-build/desktop/windows-x86_64",
    icons: "apps/desktop/src-tauri/icons/release",
  });
  assert.equal(config.version, "2.0.0-1");
  assert.deepEqual(config.bundle.targets, ["msi", "nsis"]);
  assert.equal(config.bundle.active, true);
  assert.equal(config.bundle.createUpdaterArtifacts, false);
  assert.equal(config.bundle.windows.allowDowngrades, false);
  assert.equal(Object.values(config.bundle.resources)[0], "cmclient-runtime/");
  assert.match(
    Object.keys(config.bundle.resources)[0],
    /release-build\/desktop\/windows-x86_64\/$/,
  );
  assert.ok(config.bundle.icon.some((path) => path.endsWith("icon.ico")));
  assert.ok(config.bundle.icon.some((path) => path.endsWith("icon.icns")));
});

test("Windows Tauri package versions stay MSI-compatible without changing RC asset identity", () => {
  assert.equal(tauriPackageVersion("windows-x86_64", "2.0.0-rc.1"), "2.0.0-1");
  assert.equal(tauriPackageVersion("windows-x86_64", "2.0.0"), "2.0.0");
  assert.equal(tauriPackageVersion("linux-x86_64", "2.0.0-rc.1"), "2.0.0-rc.1");
  assert.throws(
    () => tauriPackageVersion("windows-x86_64", "2.0.0-rc"),
    /numeric identifier/,
  );
  assert.throws(
    () => tauriPackageVersion("windows-x86_64", "2.0.0-rc.65536"),
    /MSI limit/,
  );
});

test("native Desktop collector renames and verifies exact Tauri outputs", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "cmclient-native-desktop-"));
  const bundleRoot = join(root, "bundle");
  const output = join(root, "output");
  t.after(() => rm(root, { force: true, recursive: true }));
  await mkdir(join(bundleRoot, "msi"), { recursive: true });
  await mkdir(join(bundleRoot, "nsis"), { recursive: true });
  await writeFile(join(bundleRoot, "msi/CMClient_fixture.msi"), "msi");
  await writeFile(join(bundleRoot, "nsis/CMClient_fixture.exe"), "nsis");

  const { directory, manifest } = await collectNativeDesktopBundles({
    target: "windows-x86_64",
    version: "2.0.0-rc.1",
    bundleRoot,
    output,
  });
  assert.equal(manifest.agentLaunch, "external-service-required");
  assert.equal(manifest.portableResource, "cmclient-runtime");
  await assert.doesNotReject(() =>
    verifyNativeDesktopStage({
      target: "windows-x86_64",
      version: "2.0.0-rc.1",
      input: directory,
    }),
  );
  await writeFile(join(directory, "unexpected.txt"), "unexpected");
  await assert.rejects(
    () =>
      verifyNativeDesktopStage({
        target: "windows-x86_64",
        version: "2.0.0-rc.1",
        input: directory,
      }),
    /staged files do not match canonical plan/,
  );
});

test("bundled Desktop runtime verification requires the complete Agent composition", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "cmclient-native-runtime-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  const inputs = await createCompositionInputs(root, "desktop", "linux-x86_64");
  const { stagedDirectory } = await stageBuild({
    component: "desktop",
    target: "linux-x86_64",
    version: "2.0.0-rc.1",
    inputs,
    output: join(root, "stage"),
  });
  await assert.doesNotReject(() =>
    verifyBundledDesktopRuntime({
      target: "linux-x86_64",
      version: "2.0.0-rc.1",
      input: stagedDirectory,
    }),
  );
  await rm(join(stagedDirectory, "bin/cmclient-agent"));
  await assert.rejects(
    () =>
      verifyBundledDesktopRuntime({
        target: "linux-x86_64",
        version: "2.0.0-rc.1",
        input: stagedDirectory,
      }),
    /ENOENT/,
  );
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

test("release entrypoints and Rust metadata keep the canonical CLI binary name", async () => {
  const [artifactScript, supplyChainScript, cliManifest, workflow] =
    await Promise.all([
      readFile("scripts/release-artifacts.mjs", "utf8"),
      readFile("scripts/release-supply-chain.mjs", "utf8"),
      readFile("apps/cli/Cargo.toml", "utf8"),
      readFile(".github/workflows/release-build.yml", "utf8"),
    ]);

  const platformSafeMainGuard =
    /import\.meta\.url === pathToFileURL\(resolve\(process\.argv\[1\]\)\)\.href/;
  for (const script of [artifactScript, supplyChainScript]) {
    assert.match(script, platformSafeMainGuard);
    assert.doesNotMatch(script, /`file:\/\/\$\{process\.argv\[1\]\}`/);
  }

  assert.match(
    cliManifest,
    /\[\[bin\]\]\s+name = "cmclient"\s+path = "src\/main\.rs"/,
  );
  assert.match(
    workflow,
    /inputs=\(--input "cli=\$rust_output\/cmclient\$executable_suffix"\)/,
  );
  assert.doesNotMatch(
    workflow,
    /--input "cli=\$rust_output\/cmclient-cli\$executable_suffix"/,
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

test("Gateway staging keeps only the serialport prebuild for its release target", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-native-target-"));
  t.after(() => rm(directory, { force: true, recursive: true }));
  const expectedByTarget = {
    "darwin-aarch64": "darwin-x64+arm64/@serialport+bindings-cpp.node",
    "darwin-x86_64": "darwin-x64+arm64/@serialport+bindings-cpp.node",
    "linux-aarch64": "linux-arm64/@serialport+bindings-cpp.armv8.glibc.node",
    "linux-x86_64": "linux-x64/@serialport+bindings-cpp.glibc.node",
    "windows-x86_64": "win32-x64/@serialport+bindings-cpp.node",
  };

  for (const [target, expected] of Object.entries(expectedByTarget)) {
    const inputs = await createCompositionInputs(
      join(directory, target),
      "headless",
      target,
    );
    await createSerialportPrebuildFixtures(inputs.gateway);
    const { manifest } = await stageBuild({
      component: "headless",
      target,
      version: "2.0.0-rc.1",
      inputs,
      output: join(directory, target, "stage"),
    });
    const prebuilds = manifest.files
      .filter((path) => path.includes("bindings-cpp/prebuilds/"))
      .map((path) => path.split("bindings-cpp/prebuilds/")[1]);
    assert.deepEqual(prebuilds, [expected], target);
  }
});

test("Gateway staging fails closed without a compatible serialport prebuild", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-native-missing-"));
  t.after(() => rm(directory, { force: true, recursive: true }));
  const inputs = await createCompositionInputs(
    directory,
    "headless",
    "linux-x86_64",
  );
  const prebuildRoot = join(
    inputs.gateway,
    "node_modules/@serialport/bindings-cpp/prebuilds/linux-x64",
  );
  await rm(prebuildRoot, { force: true, recursive: true });
  await mkdir(prebuildRoot, { recursive: true });
  await writeFile(
    join(prebuildRoot, "@serialport+bindings-cpp.musl.node"),
    "linux-x64-musl",
  );

  await assert.rejects(
    () =>
      stageBuild({
        component: "headless",
        target: "linux-x86_64",
        version: "2.0.0-rc.1",
        inputs,
        output: join(directory, "stage"),
      }),
    /gateway missing linux-x86_64 serialport prebuild/,
  );
});

test("Gateway staging requires a serialport prebuild root", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-native-root-"));
  t.after(() => rm(directory, { force: true, recursive: true }));
  const inputs = await createCompositionInputs(
    directory,
    "headless",
    "linux-x86_64",
  );
  await rm(
    join(inputs.gateway, "node_modules/@serialport/bindings-cpp/prebuilds"),
    { force: true, recursive: true },
  );

  await assert.rejects(
    () =>
      stageBuild({
        component: "headless",
        target: "linux-x86_64",
        version: "2.0.0-rc.1",
        inputs,
        output: join(directory, "stage"),
      }),
    /gateway missing linux-x86_64 serialport prebuild/,
  );
});

test("Gateway staging validates nested roots and preserves near-prefix directories", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-native-nested-"));
  t.after(() => rm(directory, { force: true, recursive: true }));
  const inputs = await createCompositionInputs(
    directory,
    "headless",
    "linux-x86_64",
  );
  const nestedPackage = join(
    inputs.gateway,
    "node_modules/wrapper/node_modules/@serialport/bindings-cpp",
  );
  const nestedTarget = join(nestedPackage, "prebuilds/linux-x64");
  await mkdir(nestedTarget, { recursive: true });
  await writeFile(
    join(nestedTarget, "@serialport+bindings-cpp.glibc.node"),
    "nested-linux-x64-glibc",
  );
  await writeFile(
    join(nestedTarget, "@serialport+bindings-cpp.musl.node"),
    "nested-linux-x64-musl",
  );
  const nearPrefix = join(nestedPackage, "prebuilds-backup/keep.txt");
  await mkdir(dirname(nearPrefix), { recursive: true });
  await writeFile(nearPrefix, "keep");

  const { manifest } = await stageBuild({
    component: "headless",
    target: "linux-x86_64",
    version: "2.0.0-rc.1",
    inputs,
    output: join(directory, "stage"),
  });
  assert.equal(
    manifest.files.filter((path) => path.endsWith(".glibc.node")).length,
    2,
  );
  assert.equal(
    manifest.files.some((path) => path.endsWith(".musl.node")),
    false,
  );
  assert.ok(
    manifest.files.includes(
      "gateway/node_modules/wrapper/node_modules/@serialport/bindings-cpp/prebuilds-backup/keep.txt",
    ),
  );

  await rm(join(nestedTarget, "@serialport+bindings-cpp.glibc.node"));
  await assert.rejects(
    () =>
      stageBuild({
        component: "headless",
        target: "linux-x86_64",
        version: "2.0.0-rc.1",
        inputs,
        output: join(directory, "invalid-stage"),
      }),
    /gateway missing linux-x86_64 serialport prebuild at .*wrapper/,
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
  const buildJob = workflow.slice(
    workflow.indexOf("\n  build:"),
    workflow.indexOf("\n  docker-composition:"),
  );

  assert.match(workflow, /packages=\(--package cmclient-cli\)/);
  assert.match(
    workflow,
    /pnpm --config\.node-linker=hoisted --filter @cmclient\/gateway deploy \\\n\s+--prod --frozen-lockfile release-build-input\/gateway/,
  );
  assert.doesNotMatch(workflow, /--legacy/);
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
  assert.match(workflow, /load-gate:[\s\S]*pnpm test:load/);
  assert.match(
    workflow,
    /build:[\s\S]*needs: \[artifact-plan, load-gate, security-gate\]/,
  );
  assert.match(
    workflow,
    /build:[\s\S]*CMCLIENT_BUILD_COMMIT: \$\{\{ github\.sha \}\}[\s\S]*cargo build --release --locked/,
  );
  assert.doesNotMatch(buildJob, /CMCLIENT_BUILD_CHANNEL:/);
  assert.match(
    workflow,
    /docker-composition:[\s\S]*needs: \[artifact-plan, load-gate, security-gate\]/,
  );
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
  assert.match(
    smoke,
    /expected_version="\$\{3:\?expected version is required\}"/,
  );
  assert.match(
    smoke,
    /expected_commit="\$\{4:\?expected source commit is required\}"/,
  );
  assert.match(smoke, /surface="\$\{5:-headless\}"/);
  assert.match(smoke, /xvfb-run -a "\$desktop"/);
  assert.match(
    workflow,
    /cmclient-desktop-linux-x86_64-\$version\.tar\.zst[\s\S]*release-bundle-smoke\.sh[\s\S]*desktop/,
  );
});

test("release workflow builds and inspects native Desktop packages from portable staging", async () => {
  const [workflow, shellSmoke, windowsSmoke, tauriConfig] = await Promise.all([
    readFile(".github/workflows/release-build.yml", "utf8"),
    readFile("scripts/desktop-native-smoke.sh", "utf8"),
    readFile("scripts/desktop-native-smoke.ps1", "utf8"),
    readFile("apps/desktop/src-tauri/tauri.conf.json", "utf8"),
  ]);
  assert.match(
    workflow,
    /name: Build native Desktop installers with the complete portable composition/,
  );
  assert.match(workflow, /pnpm --filter @cmclient\/desktop exec tauri icon/);
  assert.match(
    workflow,
    /bundle_args=\([\s\S]*--no-sign[\s\S]*--target "\$rust_target"[\s\S]*\)[\s\S]*pnpm --filter @cmclient\/desktop exec tauri bundle/,
  );
  assert.match(workflow, /export NO_STRIP=true/);
  assert.match(workflow, /bundle_args\+=\(--verbose\)/);
  assert.match(
    workflow,
    /--portable "\$GITHUB_WORKSPACE\/release-build\/desktop\/\$target"/,
  );
  assert.match(workflow, /desktop-native-bundles\.mjs collect/);
  assert.match(workflow, /if: matrix\.component != 'cli'/);
  assert.match(
    workflow,
    /libfuse2 \\\n\s+patchelf \\\n\s+xdg-utils \\\n\s+xvfb/,
  );

  assert.match(shellSmoke, /hdiutil attach/);
  assert.match(shellSmoke, /dpkg-deb --extract/);
  assert.match(shellSmoke, /--appimage-extract/);
  assert.match(shellSmoke, /desktop-native-bundles\.mjs verify-runtime/);
  assert.match(shellSmoke, /launch_native_app/);
  assert.match(windowsSmoke, /System32\\msiexec\.exe/);
  assert.match(windowsSmoke, /"\/a"/);
  assert.match(windowsSmoke, /Start-Process/);
  assert.match(windowsSmoke, /-Wait/);
  assert.match(windowsSmoke, /-PassThru/);
  assert.match(windowsSmoke, /\/L\*v/);
  assert.match(windowsSmoke, /7z\.exe x/);
  assert.match(windowsSmoke, /build-manifest\.json/);
  assert.match(windowsSmoke, /-Force -Filter/);
  assert.match(windowsSmoke, /desktop-native-bundles\.mjs verify-runtime/);
  assert.match(windowsSmoke, /Assert-NativeAppLaunch/);

  const config = JSON.parse(tauriConfig);
  assert.equal(config.bundle.active, false);
  assert.deepEqual(config.bundle.icon, ["icons/icon.png", "icons/icon.ico"]);
});

test("staged and final Windows Service archives share the real SCM launch gate", async () => {
  const [workflow, serviceSmoke] = await Promise.all([
    readFile(".github/workflows/release-build.yml", "utf8"),
    readFile("scripts/release-windows-service-smoke.ps1", "utf8"),
  ]);
  const buildJob = workflow.slice(
    workflow.indexOf("\n  build:"),
    workflow.indexOf("\n  docker-composition:"),
  );
  const finalJob = workflow.slice(
    workflow.indexOf("\n  final-windows-service-smoke:"),
    workflow.indexOf("\n  attest:"),
  );
  const attestJob = workflow.slice(
    workflow.indexOf("\n  attest:"),
    workflow.indexOf("\n  sign-update-manifest:"),
  );

  assert.match(
    buildJob,
    /Smoke staged Windows Service archive lifecycle[\s\S]*release-windows-service-smoke\.ps1[\s\S]*-Bundle "release-build\/service\/windows-x86_64"/,
  );
  assert.match(finalJob, /needs: supply-chain/);
  assert.match(finalJob, /runs-on: windows-latest/);
  assert.match(finalJob, /actions\/setup-node@[a-f0-9]{40}/);
  assert.match(finalJob, /pattern: cmclient-supply-chain-unsigned-\*/);
  assert.match(finalJob, /cmclient-service-windows-x86_64-\$version\.zip/);
  assert.match(finalJob, /Expand-Archive -LiteralPath \$archive/);
  assert.match(
    finalJob,
    /Get-FileHash -LiteralPath \$archive -Algorithm SHA256/,
  );
  assert.match(finalJob, /release-windows-service-smoke\.ps1/);
  assert.match(finalJob, /-Commit \$env:GITHUB_SHA/);
  assert.match(
    attestJob,
    /needs: \[supply-chain, final-windows-service-smoke\]/,
  );
  assert.equal(
    (workflow.match(/release-windows-service-smoke\.ps1/g) ?? []).length,
    2,
  );

  for (const parameter of ["Bundle", "Version", "Commit", "NodePath"]) {
    assert.match(
      serviceSmoke,
      new RegExp(
        `\\[Parameter\\(Mandatory = \\$true\\)\\]\\s+\\[string\\]\\$${parameter}`,
      ),
    );
  }
  assert.match(serviceSmoke, /RELEASE_SERVICE_NODE_MUST_BE_EXTERNAL/);
  assert.match(serviceSmoke, /RELEASE_SERVICE_BUNDLED_NODE_FORBIDDEN/);
  assert.match(serviceSmoke, /PropertyType MultiString/);
  assert.match(serviceSmoke, /@\("PATH=\$servicePath"\)/);
  assert.match(
    serviceSmoke,
    /Invoke-ServiceManager \$manager "install" \$hostPath -NoStart/,
  );
  assert.match(serviceSmoke, /Start-Service -Name \$ServiceName/);
  assert.match(serviceSmoke, /--json status/);
  assert.match(serviceSmoke, /--json start/);
  assert.match(serviceSmoke, /api\/v1\/system\/health/);
  assert.match(serviceSmoke, /api\/v1\/system\/version/);
  assert.match(serviceSmoke, /\$ReleaseVersion -match '-dev\\\.'/);
  assert.match(serviceSmoke, /\$ReleaseVersion\.Contains\("-"\)/);
  assert.match(serviceSmoke, /\[regex\]::Match/);
  assert.match(serviceSmoke, /\$nodeVersionMatch\.Groups\[1\]\.Value/);
  assert.doesNotMatch(serviceSmoke, /\$Matches\[/);
  assert.match(serviceSmoke, /\$candidate\.commit -eq \$Commit/);
  assert.match(serviceSmoke, /\$candidate\.channel -eq \$expectedChannel/);
  assert.match(
    serviceSmoke,
    /\$_.ParentProcessId -eq \$agent\.ProcessId -and \$_.ExecutablePath -eq \$NodePath/,
  );
  assert.match(serviceSmoke, /RELEASE_SERVICE_STATE_NOT_RETAINED/);
  assert.doesNotMatch(serviceSmoke, /Copy-Item[\s\S]*node\.exe/i);
  assert.doesNotMatch(
    serviceSmoke,
    /\?\?|\?\.|&&|\|\||ForEach-Object\s+-Parallel|IsPathFullyQualified/,
  );
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
        '{"type":"module","dependencies":{"runtime-package":"1.0.0","@serialport/bindings-cpp":"13.0.0"}}\n',
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
      await createTargetSerialportPrebuildFixture(source, target);
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

async function createSerialportPrebuildFixtures(gateway) {
  const files = [
    "android-arm/@serialport+bindings-cpp.armv7.node",
    "darwin-x64+arm64/@serialport+bindings-cpp.node",
    "linux-arm64/@serialport+bindings-cpp.armv8.glibc.node",
    "linux-arm64/@serialport+bindings-cpp.armv8.musl.node",
    "linux-x64/@serialport+bindings-cpp.glibc.node",
    "linux-x64/@serialport+bindings-cpp.musl.node",
    "win32-x64/@serialport+bindings-cpp.node",
  ];
  const root = join(gateway, "node_modules/@serialport/bindings-cpp/prebuilds");
  for (const file of files) {
    const destination = join(root, ...file.split("/"));
    await mkdir(dirname(destination), { recursive: true });
    await writeFile(destination, file);
  }
}

async function createTargetSerialportPrebuildFixture(gateway, target) {
  const fileByTarget = {
    "darwin-aarch64": "darwin-x64+arm64/@serialport+bindings-cpp.node",
    "darwin-x86_64": "darwin-x64+arm64/@serialport+bindings-cpp.node",
    "linux-aarch64": "linux-arm64/@serialport+bindings-cpp.armv8.glibc.node",
    "linux-x86_64": "linux-x64/@serialport+bindings-cpp.glibc.node",
    "windows-x86_64": "win32-x64/@serialport+bindings-cpp.node",
  };
  const packageRoot = join(gateway, "node_modules/@serialport/bindings-cpp");
  const destination = join(
    packageRoot,
    "prebuilds",
    ...fileByTarget[target].split("/"),
  );
  await mkdir(dirname(destination), { recursive: true });
  await writeFile(
    join(packageRoot, "package.json"),
    '{"name":"@serialport/bindings-cpp","version":"13.0.0"}\n',
  );
  await writeFile(destination, target);
}
