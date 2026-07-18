import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  RELEASE_COMPONENTS,
  RELEASE_TARGETS,
  releaseArtifactName,
  releaseArtifactPlan,
  stageBuild,
} from "./release-artifacts.mjs";

test("release artifact plan covers every component and updater target exactly once", () => {
  const plan = releaseArtifactPlan("2.0.0-dev.0");

  assert.equal(plan.length, RELEASE_COMPONENTS.length * RELEASE_TARGETS.length);
  assert.deepEqual(
    new Set(plan.map(({ fileName }) => fileName)).size,
    plan.length,
  );
  assert.deepEqual(
    plan.find(
      ({ component, target }) => component === "desktop" && target === "darwin-aarch64",
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
});

test("release targets remain identical to the shared signed-update contract", async () => {
  const updateContract = await readFile("packages/contracts/src/update.ts", "utf8");
  const match = updateContract.match(/export const UPDATE_TARGETS = \[([\s\S]*?)\] as const;/);
  assert.ok(match, "UPDATE_TARGETS must be declared in the shared contract");
  const contractTargets = Array.from(match[1].matchAll(/"([^"]+)"/g), ([, target]) => target);

  assert.deepEqual(RELEASE_TARGETS, contractTargets);
});

test("release artifact plan rejects unsupported values and non-SemVer versions", () => {
  assert.throws(
    () => releaseArtifactName({ component: "gateway", target: "linux-x86_64", version: "2.0.0" }),
    /unknown release component/,
  );
  assert.throws(
    () => releaseArtifactName({ component: "cli", target: "linux-riscv64", version: "2.0.0" }),
    /unknown release target/,
  );
  assert.throws(
    () => releaseArtifactPlan("v2.0.0"),
    /version must be SemVer/,
  );
});

test("staged build metadata records the future release asset without creating a release archive", async () => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-release-artifacts-"));
  const binary = join(directory, "cmclient");
  const output = join(directory, "output");
  await writeFile(binary, "fixture executable");

  const { manifest, stagedBinary } = await stageBuild({
    component: "cli",
    target: "linux-x86_64",
    version: "2.0.0-dev.0",
    binary,
    output,
  });

  assert.equal(manifest.releaseAsset.fileName, "cmclient-cli-linux-x86_64-2.0.0-dev.0.tar.zst");
  assert.equal(await readFile(stagedBinary, "utf8"), "fixture executable");
  assert.deepEqual(
    JSON.parse(await readFile(join(output, "cli/linux-x86_64/build-manifest.json"), "utf8")),
    manifest,
  );
});
