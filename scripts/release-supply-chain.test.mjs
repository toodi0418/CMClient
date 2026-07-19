import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, generateKeyPairSync, verify } from "node:crypto";
import {
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import {
  nativeDesktopArtifactPlan,
  releaseArtifactPlan,
  releaseComposition,
  stageBuild,
} from "./release-artifacts.mjs";
import { collectNativeDesktopBundles } from "./desktop-native-bundles.mjs";
import {
  assembleReleaseArtifacts,
  canonicalUpdateManifest,
  checksumFileContents,
  createSignedUpdateManifest,
  dockerArtifactPlans,
  finalizeChecksums,
  includeDockerArtifact,
  inspectOciArchive,
  stageDockerArtifact,
  verifyReleaseOutput,
} from "./release-supply-chain.mjs";

const runFile = promisify(execFile);
const sourceSha = "0123456789abcdef0123456789abcdef01234567";

test("assembler creates every updater-safe archive from canonical staged inputs", async (t) => {
  const version = "2.0.0-dev.0";
  const root = await mkdtemp(join(tmpdir(), "cmclient-release-assembly-"));
  const input = join(root, "input");
  const output = join(root, "output");
  const plan = releaseArtifactPlan(version);
  t.after(() => rm(root, { force: true, recursive: true }));

  await stageFixture(input, plan, version, "fixture");

  const index = await assembleReleaseArtifacts({ input, output, version });
  assert.equal(index.schemaVersion, 2);
  assert.equal(index.artifacts.length, plan.length);
  assert.equal(
    index.nativeDesktop.length,
    nativeDesktopArtifactPlan(version).length,
  );
  for (const artifact of index.artifacts) {
    assert.match(artifact.sha256, /^[a-f0-9]{64}$/);
    assert.ok(artifact.sizeBytes > 0);
  }
  const repeated = await assembleReleaseArtifacts({
    input,
    output: join(root, "output-repeat"),
    version,
  });
  assert.deepEqual(
    repeated.artifacts.map(({ fileName, sha256, sizeBytes }) => ({
      fileName,
      sha256,
      sizeBytes,
    })),
    index.artifacts.map(({ fileName, sha256, sizeBytes }) => ({
      fileName,
      sha256,
      sizeBytes,
    })),
  );
  assert.deepEqual(
    repeated.nativeDesktop.map(({ fileName, sha256, sizeBytes }) => ({
      fileName,
      sha256,
      sizeBytes,
    })),
    index.nativeDesktop.map(({ fileName, sha256, sizeBytes }) => ({
      fileName,
      sha256,
      sizeBytes,
    })),
  );
  const desktopArchive = plan.find(
    ({ component, target }) =>
      component === "desktop" && target === "darwin-aarch64",
  );
  assert.ok(desktopArchive);
  const { stdout: desktopListing } = await runFile("tar", [
    "-tf",
    join(output, desktopArchive.fileName),
  ]);
  const desktopEntries = new Set(desktopListing.trim().split("\n"));
  for (const expected of [
    "bin/cmclient-desktop",
    "bin/cmclient-agent",
    "bin/cmclient",
    "bin/cmclient-migrate",
    "gateway/dist/main.js",
    "gateway/node_modules/runtime-package/index.js",
    "proto/meshtastic/mesh.proto",
    "web/index.html",
    "scripts/cmclient-launchd.sh",
    "packaging/launchd/io.cmclient.agent.plist.in",
    "metadata/build-manifest.json",
  ]) {
    assert.ok(
      desktopEntries.has(expected),
      `desktop archive must contain ${expected}`,
    );
  }

  const portableArchive = plan.find(
    ({ component, target }) => component === "cli" && target === "linux-x86_64",
  );
  assert.ok(portableArchive);
  const portableIndex = plan.indexOf(portableArchive);
  const { stdout: portableListing } = await runFile("tar", [
    "-tf",
    join(output, portableArchive.fileName),
  ]);
  assert.deepEqual(portableListing.trim().split("\n").sort(), [
    "bin/cmclient",
    "metadata/build-manifest.json",
  ]);

  const portableV1 = join(root, "portable-v1");
  const portableV2 = join(root, "portable-v2");
  const retainedData = join(root, "user-data", "retained-state");
  await mkdir(portableV1, { recursive: true });
  await runFile("tar", [
    "-xf",
    join(output, portableArchive.fileName),
    "-C",
    portableV1,
  ]);
  assert.equal(
    await readFile(join(portableV1, "bin/cmclient"), "utf8"),
    `fixture-${portableIndex}-cli`,
  );
  await mkdir(join(root, "user-data"), { recursive: true });
  await writeFile(retainedData, "must survive portable refresh");

  const upgradeVersion = "2.0.0-dev.1";
  const upgradeInput = join(root, "input-v2");
  const upgradeOutput = join(root, "output-v2");
  const upgradePlan = releaseArtifactPlan(upgradeVersion);
  await stageFixture(upgradeInput, upgradePlan, upgradeVersion, "upgrade");
  await assembleReleaseArtifacts({
    input: upgradeInput,
    output: upgradeOutput,
    version: upgradeVersion,
  });
  const upgradeArchive = upgradePlan.find(
    (artifact) =>
      artifact.component === portableArchive.component &&
      artifact.target === portableArchive.target,
  );
  assert.ok(upgradeArchive);
  await mkdir(portableV2, { recursive: true });
  await runFile("tar", [
    "-xf",
    join(upgradeOutput, upgradeArchive.fileName),
    "-C",
    portableV2,
  ]);
  await rm(portableV1, { force: true, recursive: true });
  assert.equal(
    await readFile(join(portableV2, "bin/cmclient"), "utf8"),
    `upgrade-${portableIndex}-cli`,
  );
  assert.equal(
    await readFile(retainedData, "utf8"),
    "must survive portable refresh",
  );

  await writeFile(
    join(output, "cmclient-2.0.0-dev.0.spdx.json"),
    '{"spdxVersion":"SPDX-2.3"}\n',
  );
  await finalizeChecksums({ output });
  await assert.doesNotReject(() => verifyReleaseOutput({ output, version }));
});

async function stageFixture(input, plan, version, prefix) {
  for (const [index, artifact] of plan.entries()) {
    const sourceRoot = join(
      input,
      "..",
      "sources",
      `${artifact.component}-${artifact.target}`,
    );
    const inputs = {};
    for (const content of releaseComposition(
      artifact.component,
      artifact.target,
    )) {
      const source = join(sourceRoot, content.role);
      if (content.kind === "file") {
        await mkdir(sourceRoot, { recursive: true });
        await writeFile(source, `${prefix}-${index}-${content.role}`);
      } else if (content.role === "gateway") {
        await mkdir(join(source, "dist"), { recursive: true });
        await mkdir(join(source, "node_modules", "runtime-package"), {
          recursive: true,
        });
        await writeFile(
          join(source, "dist/main.js"),
          `${prefix}-${index}-gateway`,
        );
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
        await stageSerialportFixture(source, artifact.target);
      } else if (content.role === "web") {
        await mkdir(source, { recursive: true });
        await writeFile(join(source, "index.html"), `${prefix}-${index}-web`);
      } else if (content.role === "proto") {
        await mkdir(join(source, "meshtastic"), { recursive: true });
        await writeFile(
          join(source, "meshtastic/mesh.proto"),
          'syntax = "proto3";',
        );
      }
      inputs[content.role] = source;
    }
    await stageBuild({
      component: artifact.component,
      target: artifact.target,
      version,
      inputs,
      output: input,
    });
  }
  await stageNativeDesktopFixture(input, version, prefix);
}

async function stageSerialportFixture(gateway, target) {
  const fileByTarget = {
    "darwin-aarch64": "darwin-x64+arm64/@serialport+bindings-cpp.node",
    "darwin-x86_64": "darwin-x64+arm64/@serialport+bindings-cpp.node",
    "linux-aarch64": "linux-arm64/@serialport+bindings-cpp.armv8.glibc.node",
    "linux-x86_64": "linux-x64/@serialport+bindings-cpp.glibc.node",
    "windows-x86_64": "win32-x64/@serialport+bindings-cpp.node",
  };
  const packageRoot = join(gateway, "node_modules/@serialport/bindings-cpp");
  const prebuild = join(
    packageRoot,
    "prebuilds",
    ...fileByTarget[target].split("/"),
  );
  await mkdir(dirname(prebuild), { recursive: true });
  await writeFile(
    join(packageRoot, "package.json"),
    '{"name":"@serialport/bindings-cpp","version":"13.0.0"}\n',
  );
  await writeFile(prebuild, target);
}

async function stageNativeDesktopFixture(input, version, prefix) {
  const plan = nativeDesktopArtifactPlan(version);
  const extensions = {
    appimage: ".AppImage",
    deb: ".deb",
    dmg: ".dmg",
    msi: ".msi",
    nsis: ".exe",
  };
  for (const target of new Set(plan.map((artifact) => artifact.target))) {
    const bundleRoot = join(input, "..", "native-sources", target);
    await rm(bundleRoot, { force: true, recursive: true });
    for (const artifact of plan.filter(
      (candidate) => candidate.target === target,
    )) {
      const directory = join(bundleRoot, artifact.bundle);
      await mkdir(directory, { recursive: true });
      await writeFile(
        join(
          directory,
          `${prefix}-${artifact.bundle}${extensions[artifact.bundle]}`,
        ),
        `${prefix}-${target}-${artifact.bundle}`,
      );
    }
    await collectNativeDesktopBundles({
      target,
      version,
      bundleRoot,
      output: input,
    });
  }
}

test("assembler rejects files outside the canonical staged composition", async (t) => {
  const version = "2.0.0-dev.0";
  const root = await mkdtemp(join(tmpdir(), "cmclient-release-unexpected-"));
  const input = join(root, "input");
  t.after(() => rm(root, { force: true, recursive: true }));
  await stageFixture(input, releaseArtifactPlan(version), version, "fixture");
  await writeFile(
    join(input, "desktop/darwin-aarch64/gateway/dist/not-in-build-manifest.js"),
    "unexpected",
  );

  await assert.rejects(
    () =>
      assembleReleaseArtifacts({
        input,
        output: join(root, "output"),
        version,
      }),
    /RELEASE_BUILD_CONTENT_UNEXPECTED/,
  );
});

test("Docker OCI staging binds the image digest to version and source SHA", async (t) => {
  const version = "2.0.0-dev.0";
  const root = await mkdtemp(join(tmpdir(), "cmclient-docker-oci-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  for (const plan of dockerArtifactPlans(version)) {
    const archive = await createOciFixture(join(root, plan.target), {
      architecture: plan.architecture,
      sourceSha,
      version,
    });

    const inspected = await inspectOciArchive({
      archivePath: archive,
      sourceSha,
      target: plan.target,
      version,
    });
    assert.match(inspected.imageDigest, /^sha256:[a-f0-9]{64}$/);
    const staged = await stageDockerArtifact({
      input: archive,
      output: join(root, `staged-${plan.target}`),
      sourceSha,
      target: plan.target,
      version,
    });
    assert.equal(staged.imageDigest, inspected.imageDigest);
    assert.equal(staged.sourceSha, sourceSha);
    assert.equal(staged.version, version);
    assert.equal(staged.platform, plan.platform);
    assert.deepEqual(staged.composition.excluded, [
      "agent",
      "cli",
      "desktop",
      "serviceHost",
    ]);
  }

  const amd64Plan = dockerArtifactPlans(version)[0];
  const archive = join(root, amd64Plan.target, "cmclient.oci.tar");
  await assert.rejects(
    () =>
      inspectOciArchive({
        archivePath: archive,
        sourceSha: "f".repeat(40),
        target: amd64Plan.target,
        version,
      }),
    /RELEASE_DOCKER_OCI_IDENTITY_INVALID/,
  );
  await assert.rejects(
    () =>
      inspectOciArchive({
        archivePath: archive,
        sourceSha,
        target: amd64Plan.target,
        version: "2.0.0-dev.1",
      }),
    /RELEASE_DOCKER_OCI_IDENTITY_INVALID/,
  );

  const incompleteInput = join(root, "incomplete");
  await mkdir(incompleteInput, { recursive: true });
  for (const fileName of [amd64Plan.fileName, amd64Plan.metadataFileName]) {
    await copyFile(
      join(root, `staged-${amd64Plan.target}`, fileName),
      join(incompleteInput, fileName),
    );
  }
  await assert.rejects(
    () =>
      includeDockerArtifact({
        input: incompleteInput,
        output: join(root, "unused-output"),
        sourceSha,
        version,
      }),
    /RELEASE_DOCKER_INPUT_INVALID/,
  );
});

test("supply-chain checksums cover every archive and generated SBOM", async (t) => {
  const version = "2.0.0-dev.0";
  const root = await mkdtemp(join(tmpdir(), "cmclient-supply-chain-"));
  const output = join(root, "output");
  const plan = releaseArtifactPlan(version);
  const artifacts = [];
  const nativeDesktop = [];
  t.after(() => rm(root, { force: true, recursive: true }));
  await mkdir(output, { recursive: true });

  for (const [index, artifact] of plan.entries()) {
    const contents = Buffer.from(`fixture-${index}`);
    await writeFile(join(output, artifact.fileName), contents);
    artifacts.push({
      ...artifact,
      sha256: createHash("sha256").update(contents).digest("hex"),
      sizeBytes: contents.length,
    });
  }
  for (const [index, artifact] of nativeDesktopArtifactPlan(
    version,
  ).entries()) {
    const contents = Buffer.from(`native-fixture-${index}`);
    await writeFile(join(output, artifact.fileName), contents);
    nativeDesktop.push({
      ...artifact,
      sha256: createHash("sha256").update(contents).digest("hex"),
      sizeBytes: contents.length,
    });
  }
  await writeFile(
    join(output, "cmclient-2.0.0-dev.0.spdx.json"),
    '{"spdxVersion":"SPDX-2.3"}\n',
  );
  await writeFile(
    join(output, "release-index.json"),
    `${JSON.stringify({ schemaVersion: 2, version, artifacts, nativeDesktop }, null, 2)}\n`,
  );
  const dockerInput = join(root, "docker-input");
  await mkdir(dockerInput, { recursive: true });
  const dockerPlans = dockerArtifactPlans(version);
  for (const dockerPlan of dockerPlans) {
    const dockerArchive = await createOciFixture(
      join(root, `oci-${dockerPlan.target}`),
      {
        architecture: dockerPlan.architecture,
        sourceSha,
        version,
      },
    );
    const stagedDirectory = join(root, `docker-stage-${dockerPlan.target}`);
    await stageDockerArtifact({
      input: dockerArchive,
      output: stagedDirectory,
      sourceSha,
      target: dockerPlan.target,
      version,
    });
    for (const fileName of [dockerPlan.fileName, dockerPlan.metadataFileName]) {
      await copyFile(
        join(stagedDirectory, fileName),
        join(dockerInput, fileName),
      );
    }
    await writeFile(
      join(output, dockerPlan.sbomFileName),
      `{"spdxVersion":"SPDX-2.3","name":"docker-${dockerPlan.target}"}\n`,
    );
  }
  await includeDockerArtifact({
    input: dockerInput,
    output,
    sourceSha,
    version,
  });
  const checksums = await finalizeChecksums({ output });
  assert.equal(
    checksums.length,
    plan.length + nativeDesktopArtifactPlan(version).length + 8,
  );
  assert.match(
    checksumFileContents(checksums),
    /\*cmclient-2\.0\.0-dev\.0\.spdx\.json/,
  );
  assert.match(checksumFileContents(checksums), /\.oci\.tar/);
  assert.match(checksumFileContents(checksums), /\.metadata\.json/);
  assert.match(checksumFileContents(checksums), /\.AppImage/);
  assert.match(checksumFileContents(checksums), /\.setup\.exe/);
  assert.match(checksumFileContents(checksums), /\*release-index\.json/);
  assert.match(
    checksumFileContents(checksums),
    /\*cmclient-docker-linux-x86_64-2\.0\.0-dev\.0\.spdx\.json/,
  );
  assert.match(
    checksumFileContents(checksums),
    /\*cmclient-docker-linux-aarch64-2\.0\.0-dev\.0\.spdx\.json/,
  );
  await assert.doesNotReject(() =>
    verifyReleaseOutput({ output, sourceSha, version }),
  );
  await assert.rejects(
    () => verifyReleaseOutput({ output, sourceSha: "f".repeat(40), version }),
    /RELEASE_INDEX_SOURCE_SHA_INVALID/,
  );
  const indexPath = join(output, "release-index.json");
  const canonicalIndex = await readFile(indexPath, "utf8");
  const indexWithoutSource = JSON.parse(canonicalIndex);
  delete indexWithoutSource.sourceSha;
  await writeFile(
    indexPath,
    `${JSON.stringify(indexWithoutSource, null, 2)}\n`,
  );
  await assert.rejects(
    () => verifyReleaseOutput({ output, sourceSha, version }),
    /RELEASE_INDEX_SOURCE_SHA_INVALID/,
  );
  await writeFile(indexPath, canonicalIndex);

  const armSbomPath = join(output, dockerPlans[1].sbomFileName);
  const armSbom = await readFile(armSbomPath);
  await rm(armSbomPath);
  await assert.rejects(
    () => finalizeChecksums({ output }),
    /RELEASE_SBOM_SET_INVALID/,
  );
  await writeFile(armSbomPath, armSbom);
  const extraSbomPath = join(output, "unexpected.spdx.json");
  await writeFile(extraSbomPath, '{"spdxVersion":"SPDX-2.3"}\n');
  await assert.rejects(
    () => finalizeChecksums({ output }),
    /RELEASE_SBOM_SET_INVALID/,
  );
  await rm(extraSbomPath);
  await finalizeChecksums({ output });

  const unexpectedPath = join(output, "unchecked-release-payload.bin");
  await writeFile(unexpectedPath, "unexpected");
  await assert.rejects(
    () => verifyReleaseOutput({ output, sourceSha, version }),
    /RELEASE_OUTPUT_SET_INVALID/,
  );
  await rm(unexpectedPath);

  const sigstorePath = join(output, "SHA256SUMS.sigstore.json");
  await writeFile(
    sigstorePath,
    '{"mediaType":"application/vnd.dev.sigstore.bundle+json;version=0.3"}\n',
  );
  await assert.doesNotReject(() =>
    verifyReleaseOutput({ output, sourceSha, version }),
  );
  await rm(sigstorePath);

  await writeFile(join(output, plan[0].fileName), "tampered");
  await assert.rejects(
    () => verifyReleaseOutput({ output, version }),
    /RELEASE_ARCHIVE_DIGEST_INVALID/,
  );
});

test("supply-chain rejects a missing or tampered native Desktop package", async (t) => {
  const version = "2.0.0-dev.0";
  const root = await mkdtemp(join(tmpdir(), "cmclient-native-supply-chain-"));
  const input = join(root, "input");
  const output = join(root, "output");
  t.after(() => rm(root, { force: true, recursive: true }));
  await stageFixture(input, releaseArtifactPlan(version), version, "native");
  const nativePlan = nativeDesktopArtifactPlan(version);
  await rm(
    join(input, "native-desktop", nativePlan[0].target, nativePlan[0].fileName),
  );
  await assert.rejects(
    () => assembleReleaseArtifacts({ input, output, version }),
    /RELEASE_NATIVE_DESKTOP_INPUT_INVALID/,
  );

  await stageNativeDesktopFixture(input, version, "restaged");
  const index = await assembleReleaseArtifacts({ input, output, version });
  await writeFile(
    join(output, "cmclient-2.0.0-dev.0.spdx.json"),
    '{"spdxVersion":"SPDX-2.3"}\n',
  );
  await finalizeChecksums({ output });
  await writeFile(join(output, index.nativeDesktop[0].fileName), "tampered");
  await assert.rejects(
    () => verifyReleaseOutput({ output, version }),
    /RELEASE_NATIVE_DESKTOP_DIGEST_INVALID/,
  );
});

test("signed update manifest is exact Ed25519 canonical payload data", () => {
  const version = "2.0.0-dev.0";
  const plan = releaseArtifactPlan(version);
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  const signed = createSignedUpdateManifest({
    artifacts: plan.map((artifact, index) => ({
      ...artifact,
      sha256: `${index.toString(16).padStart(2, "0")}`.repeat(32),
      sizeBytes: index + 1,
    })),
    channel: "dev",
    minimumAgentVersion: "2.0.0-dev.0",
    privateKeyBase64: privateKey
      .export({ format: "der", type: "pkcs8" })
      .toString("base64"),
    publishedAt: "2026-07-18T08:00:00.000Z",
    releaseBaseUrl: "https://releases.example.invalid/cmclient/2.0.0-dev.0",
    signingKeyId: "release-2026",
    version,
  });

  assert.equal(signed.signatureAlgorithm, "ed25519");
  assert.equal(signed.signature.length, 86);
  assert.equal(
    verify(
      null,
      canonicalUpdateManifest(signed.manifest),
      publicKey,
      Buffer.from(`${signed.signature}==`, "base64"),
    ),
    true,
  );
  const canonical = canonicalUpdateManifest(signed.manifest).toString("utf8");
  assert.equal(canonical, JSON.stringify(signed.manifest));
  assert.match(
    canonical,
    /^\{"schemaVersion":1,"channel":"dev","version":"2\.0\.0-dev\.0","publishedAt":"2026-07-18T08:00:00\.000Z","minimumAgentVersion":"2\.0\.0-dev\.0","bundles":\[/,
  );
  assert.match(
    canonical,
    /\{"component":"desktop","target":"darwin-aarch64","archive":"tar\.zst","url":"https:\/\/releases\.example\.invalid\/cmclient\/2\.0\.0-dev\.0\/cmclient-desktop-darwin-aarch64-2\.0\.0-dev\.0\.tar\.zst","sha256":"(?:00){32}","sizeBytes":1\}/,
  );
});

test("manifest creation rejects invalid publication inputs before signing", () => {
  const { privateKey } = generateKeyPairSync("ed25519");
  const options = {
    artifacts: releaseArtifactPlan("2.0.0").map((artifact) => ({
      ...artifact,
      sha256: "a".repeat(64),
      sizeBytes: 1,
    })),
    channel: "stable",
    minimumAgentVersion: "2.0.0",
    privateKeyBase64: privateKey
      .export({ format: "der", type: "pkcs8" })
      .toString("base64"),
    publishedAt: "2026-07-18T08:00:00.000Z",
    releaseBaseUrl: "https://releases.example.invalid/cmclient/2.0.0",
    signingKeyId: "release-2026",
    version: "2.0.0",
  };

  assert.throws(
    () =>
      createSignedUpdateManifest({
        ...options,
        releaseBaseUrl: "http://example.invalid",
      }),
    /RELEASE_MANIFEST_BASE_URL_INVALID/,
  );
  assert.throws(
    () => createSignedUpdateManifest({ ...options, signingKeyId: "bad key" }),
    /RELEASE_MANIFEST_SIGNING_KEY_ID_INVALID/,
  );
  assert.throws(
    () => createSignedUpdateManifest({ ...options, publishedAt: "2026-07-18" }),
    /RELEASE_MANIFEST_PUBLISHED_AT_INVALID/,
  );
});

test("release workflow pins actions and keeps checkout credentials disabled", async () => {
  const workflow = await readFile(
    ".github/workflows/release-build.yml",
    "utf8",
  );
  assert.match(workflow, /deploy[\s\\]+--prod --frozen-lockfile/);
  assert.doesNotMatch(workflow, /--legacy/);

  const actionReferences = Array.from(
    workflow.matchAll(/^\s*-?\s*uses:\s+([^\s#]+)/gm),
    ([, reference]) => reference,
  );
  assert.ok(actionReferences.length > 0);
  for (const reference of actionReferences) {
    assert.match(reference, /^[^@\s]+@[a-f0-9]{40}$/);
  }

  for (const expected of [
    "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
    "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020",
    "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
    "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
    "pnpm/action-setup@0ebf47130e4866e96fce0953f49152a61190b271",
    "dtolnay/rust-toolchain@191af2e1955bbe165f9bbacff2d2438002dff4d4",
    "sigstore/cosign-installer@6f9f17788090df1f26f669e9d70d6ae9567deba6",
    "actions/attest-build-provenance@0f67c3f4856b2e3261c31976d6725780e5e4c373",
  ]) {
    assert.ok(
      actionReferences.includes(expected),
      `${expected} must be pinned`,
    );
  }

  const checkoutBlocks = workflowStepBlocks(workflow).filter((block) =>
    block.includes("uses: actions/checkout@"),
  );
  assert.ok(checkoutBlocks.length > 0);
  for (const block of checkoutBlocks) {
    assert.match(block, /persist-credentials: false/);
  }

  const buildUpload = workflowStepBlocks(workflowJob(workflow, "build")).find(
    (block) => block.includes("uses: actions/upload-artifact@"),
  );
  assert.ok(buildUpload, "build job must upload the staged composition");
  assert.match(
    buildUpload,
    /include-hidden-files: true/,
    "staged hidden paths listed by build-manifest.json must be uploaded",
  );
});

test("release workflow gates provenance and signing behind immutable release inputs", async () => {
  const workflow = await readFile(
    ".github/workflows/release-build.yml",
    "utf8",
  );
  const attestJob = workflowJob(workflow, "attest");
  const signingJob = workflowJob(workflow, "sign-update-manifest");
  const sbomStep = workflowStepBlocks(workflow).find((block) =>
    block.includes("name: Generate staged-composition SBOM"),
  );
  assert.ok(sbomStep);

  assert.doesNotMatch(workflow, /anchore\/sbom-action/);
  assert.match(workflow, /scripts\/install-sbom-tool\.sh/);
  assert.match(workflow, /syft" scan dir:release-build/);
  const sbomInstaller = await readFile("scripts/install-sbom-tool.sh", "utf8");
  assert.match(sbomInstaller, /SYFT_VERSION="1\.42\.3"/);
  assert.match(
    sbomInstaller,
    /0d6be741479eddd2c8644a288990c04f3df0d609bbc1599a005532a9dff63509/,
  );
  assert.match(
    workflow,
    /sigstore\/cosign-installer@6f9f17788090df1f26f669e9d70d6ae9567deba6/,
  );
  assert.match(
    workflow,
    /actions\/attest-build-provenance@0f67c3f4856b2e3261c31976d6725780e5e4c373/,
  );
  assert.match(sbomStep, /dir:release-build/);
  assert.match(sbomStep, /spdx-json=release-dist/);
  assert.match(workflow, /cosign sign-blob --yes/);
  assert.match(workflow, /subject-checksums: release-dist\/SHA256SUMS/);
  assert.match(workflow, /id-token: write/);
  assert.match(workflow, /attestations: write/);
  assert.match(workflow, /secrets\.CMCLIENT_UPDATE_SIGNING_KEY/);
  assert.match(
    attestJob,
    /github\.event_name == 'workflow_dispatch'[\s\S]*inputs\.attest[\s\S]*startsWith\(github\.ref, 'refs\/tags\/v'\)/,
  );
  assert.match(attestJob, /environment: production-release/);
  assert.match(
    attestJob,
    /\[\[ "\$GITHUB_REF" == "refs\/tags\/v\$version" \]\]/,
  );
  assert.ok(
    attestJob.indexOf("RELEASE_TAG_VERSION_MISMATCH") <
      attestJob.indexOf("cosign sign-blob"),
    "exact tag validation must precede attestation signing",
  );
  assert.match(signingJob, /needs: \[supply-chain, attest\]/);
  assert.match(
    signingJob,
    /github\.event_name == 'workflow_dispatch'[\s\S]*inputs\.attest[\s\S]*inputs\.release_base_url != ''[\s\S]*startsWith\(github\.ref, 'refs\/tags\/v'\)/,
  );
  assert.match(signingJob, /environment: production-release/);
  assert.match(signingJob, /pattern: cmclient-supply-chain-attested/);
  assert.match(
    workflow,
    /RELEASE_BASE_URL: \$\{\{ inputs\.release_base_url \}\}/,
  );
  assert.match(workflow, /--release-base-url "\$RELEASE_BASE_URL"/);
  assert.match(
    signingJob,
    /actions\/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0/,
  );
  assert.match(
    signingJob,
    /actions\/setup-node@820762786026740c76f36085b0efc47a31fe5020/,
  );
  assert.match(signingJob, /RELEASE_SIGNING_KEY_MISSING/);
  assert.match(signingJob, /release-supply-chain\.mjs verify/);
  assert.match(
    signingJob,
    /\[\[ "\$GITHUB_REF" == "refs\/tags\/v\$version" \]\]/,
  );
  assert.match(signingJob, /cosign verify-blob/);
  assert.match(
    signingJob,
    /Sign canonical Agent manifest[\s\S]*CMCLIENT_UPDATE_SIGNING_KEY: \$\{\{ secrets\.CMCLIENT_UPDATE_SIGNING_KEY \}\}/,
  );
  assert.doesNotMatch(signingJob, /if:.*secrets\.CMCLIENT_UPDATE_SIGNING_KEY/);
  assert.doesNotMatch(
    signingJob.slice(
      0,
      signingJob.indexOf("      - name: Sign canonical Agent manifest"),
    ),
    /CMCLIENT_UPDATE_SIGNING_KEY:/,
  );
  assert.ok(
    signingJob.indexOf("cosign verify-blob") <
      signingJob.indexOf("CMCLIENT_UPDATE_SIGNING_KEY:"),
    "attestation verification must finish before the signing key is exposed",
  );
  assert.doesNotMatch(
    workflow,
    /--release-base-url "\$\{\{ inputs\.release_base_url \}\}"/,
  );
  assert.doesNotMatch(
    workflow,
    /CMCLIENT_UPDATE_SIGNING_KEY:\s+['"][A-Za-z0-9+/=]+/,
  );
});

async function createOciFixture(
  root,
  { architecture, sourceSha: revision, version },
) {
  const layout = join(root, "layout");
  const blobs = join(layout, "blobs", "sha256");
  await mkdir(blobs, { recursive: true });
  const createdAt = "2026-07-19T00:00:00Z";
  const config = Buffer.from(
    JSON.stringify({
      architecture,
      os: "linux",
      config: {
        Env: [
          `CMCLIENT_BUILD_VERSION=${version}`,
          `CMCLIENT_BUILD_COMMIT=${revision}`,
          `CMCLIENT_BUILD_CHANNEL=${version.includes("-dev.") ? "dev" : version.includes("-") ? "beta" : "stable"}`,
        ],
        Labels: {
          "org.opencontainers.image.created": createdAt,
          "org.opencontainers.image.revision": revision,
          "org.opencontainers.image.title": "cmclient",
          "org.opencontainers.image.version": version,
        },
      },
    }),
  );
  const layer = Buffer.from("cmclient-docker-fixture");
  const configDescriptor = ociDescriptor(
    config,
    "application/vnd.oci.image.config.v1+json",
  );
  const layerDescriptor = ociDescriptor(
    layer,
    "application/vnd.oci.image.layer.v1.tar",
  );
  const manifest = Buffer.from(
    JSON.stringify({
      schemaVersion: 2,
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      config: configDescriptor,
      layers: [layerDescriptor],
    }),
  );
  const manifestDescriptor = ociDescriptor(
    manifest,
    "application/vnd.oci.image.manifest.v1+json",
  );
  manifestDescriptor.platform = { architecture, os: "linux" };
  const index = {
    schemaVersion: 2,
    mediaType: "application/vnd.oci.image.index.v1+json",
    manifests: [manifestDescriptor],
  };
  for (const [bytes, descriptor] of [
    [config, configDescriptor],
    [layer, layerDescriptor],
    [manifest, manifestDescriptor],
  ]) {
    await writeFile(
      join(blobs, descriptor.digest.replace("sha256:", "")),
      bytes,
    );
  }
  await writeFile(join(layout, "index.json"), JSON.stringify(index));
  await writeFile(join(layout, "oci-layout"), '{"imageLayoutVersion":"1.0.0"}');
  const archive = join(root, "cmclient.oci.tar");
  await runFile("tar", [
    "-cf",
    archive,
    "-C",
    layout,
    "oci-layout",
    "index.json",
    "blobs",
  ]);
  return archive;
}

function ociDescriptor(bytes, mediaType) {
  return {
    mediaType,
    digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
    size: bytes.length,
  };
}

function workflowJob(workflow, name) {
  const match = new RegExp(`^ {2}${name}:\\s*$`, "m").exec(workflow);
  assert.ok(match, `workflow job ${name} must exist`);
  const start = match.index;
  const rest = workflow.slice(start + match[0].length);
  const next = rest.search(/^ {2}[a-zA-Z0-9_-]+:\s*$/m);
  return next < 0
    ? workflow.slice(start)
    : workflow.slice(start, start + match[0].length + next);
}

function workflowStepBlocks(workflow) {
  const lines = workflow.split("\n");
  const starts = lines.flatMap((line, index) =>
    /^ {6}- (?:name|uses):/.test(line) ? [index] : [],
  );
  return starts.map((start, index) =>
    lines.slice(start, starts[index + 1] ?? lines.length).join("\n"),
  );
}
