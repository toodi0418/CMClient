import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, generateKeyPairSync, verify } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import {
  releaseArtifactPlan,
  releaseComposition,
  stageBuild,
} from "./release-artifacts.mjs";
import {
  assembleReleaseArtifacts,
  canonicalUpdateManifest,
  checksumFileContents,
  createSignedUpdateManifest,
  finalizeChecksums,
  verifyReleaseOutput,
} from "./release-supply-chain.mjs";

const runFile = promisify(execFile);

test("assembler creates every updater-safe archive from canonical staged inputs", async (t) => {
  const version = "2.0.0-dev.0";
  const root = await mkdtemp(join(tmpdir(), "cmclient-release-assembly-"));
  const input = join(root, "input");
  const output = join(root, "output");
  const plan = releaseArtifactPlan(version);
  t.after(() => rm(root, { force: true, recursive: true }));

  await stageFixture(input, plan, version, "fixture");

  const index = await assembleReleaseArtifacts({ input, output, version });
  assert.equal(index.artifacts.length, plan.length);
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

test("supply-chain checksums cover every archive and generated SBOM", async (t) => {
  const version = "2.0.0-dev.0";
  const output = await mkdtemp(join(tmpdir(), "cmclient-supply-chain-"));
  const plan = releaseArtifactPlan(version);
  const artifacts = [];
  t.after(() => rm(output, { force: true, recursive: true }));

  for (const [index, artifact] of plan.entries()) {
    const contents = Buffer.from(`fixture-${index}`);
    await writeFile(join(output, artifact.fileName), contents);
    artifacts.push({
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
    `${JSON.stringify({ schemaVersion: 1, version, artifacts }, null, 2)}\n`,
  );

  const checksums = await finalizeChecksums({ output });
  assert.equal(checksums.length, plan.length + 1);
  assert.match(
    checksumFileContents(checksums),
    /\*cmclient-2\.0\.0-dev\.0\.spdx\.json/,
  );
  await assert.doesNotReject(() => verifyReleaseOutput({ output, version }));

  await writeFile(join(output, plan[0].fileName), "tampered");
  await assert.rejects(
    () => verifyReleaseOutput({ output, version }),
    /RELEASE_ARCHIVE_DIGEST_INVALID/,
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

test("release workflow keeps provenance and signing outside ordinary CI permissions", async () => {
  const workflow = await readFile(
    ".github/workflows/release-build.yml",
    "utf8",
  );
  const signingJob = workflow.slice(
    workflow.indexOf("  sign-update-manifest:"),
  );

  assert.match(
    workflow,
    /anchore\/sbom-action@e22c389904149dbc22b58101806040fa8d37a610/,
  );
  assert.match(
    workflow,
    /sigstore\/cosign-installer@b4da77ecad80ff9afe572690e3ce4a55a58e629c/,
  );
  assert.match(
    workflow,
    /actions\/attest-build-provenance@96b4a1ef7235a096b17240c259729fdd70c83d45/,
  );
  assert.match(workflow, /cosign sign-blob --yes/);
  assert.match(workflow, /subject-checksums: release-dist\/SHA256SUMS/);
  assert.match(workflow, /id-token: write/);
  assert.match(workflow, /attestations: write/);
  assert.match(workflow, /secrets\.CMCLIENT_UPDATE_SIGNING_KEY/);
  assert.match(
    workflow,
    /github\.event_name == 'workflow_dispatch' && inputs\.attest/,
  );
  assert.match(
    workflow,
    /RELEASE_BASE_URL: \$\{\{ inputs\.release_base_url \}\}/,
  );
  assert.match(workflow, /--release-base-url "\$RELEASE_BASE_URL"/);
  assert.match(signingJob, /actions\/checkout@v4/);
  assert.match(signingJob, /actions\/setup-node@v4/);
  assert.match(signingJob, /RELEASE_SIGNING_KEY_MISSING/);
  assert.match(signingJob, /release-supply-chain\.mjs verify/);
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
  assert.doesNotMatch(
    workflow,
    /--release-base-url "\$\{\{ inputs\.release_base_url \}\}"/,
  );
  assert.doesNotMatch(
    workflow,
    /CMCLIENT_UPDATE_SIGNING_KEY:\s+['"][A-Za-z0-9+/=]+/,
  );
});
