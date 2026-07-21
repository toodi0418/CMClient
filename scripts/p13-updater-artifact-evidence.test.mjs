import assert from "node:assert/strict";
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  utimes,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";

import {
  inspectArtifacts,
  readFixtureDocuments,
  validateFixtureDocuments,
} from "./p13-updater-driver-fixture.mjs";

const TARGET = "x86_64-pc-windows-msvc";
const repositoryRoot = resolve(".");

function outerBase64(value) {
  return Buffer.from(`${value}\n`, "utf8").toString("base64");
}

function publicKey(keyId) {
  const encoded = Buffer.concat([
    Buffer.from("Ed", "ascii"),
    keyId,
    Buffer.alloc(32, 0x41),
  ]).toString("base64");
  return outerBase64(`untrusted comment: minisign public key\n${encoded}`);
}

function signature(keyId, fileName) {
  const encoded = Buffer.concat([
    Buffer.from("ED", "ascii"),
    keyId,
    Buffer.alloc(64, 0x42),
  ]).toString("base64");
  const global = Buffer.alloc(64, 0x43).toString("base64");
  return outerBase64(
    [
      "untrusted comment: signature from fixture key",
      encoded,
      `trusted comment: timestamp:1\tfile:${fileName}`,
      global,
    ].join("\n"),
  );
}

async function createFixture(t) {
  const temporary = await mkdtemp(join(tmpdir(), "cmclient-p13-artifacts-"));
  t.after(() => rm(temporary, { recursive: true, force: true }));
  const root = resolve(temporary, "p13-updater-driver");
  const paths = {
    root,
    target: resolve(root, "target"),
    evidence: resolve(root, "evidence"),
    signing: resolve(root, "signing"),
    source: resolve(root, "source"),
  };
  const artifactDirectory = resolve(
    paths.target,
    TARGET,
    "release/bundle/nsis",
  );
  const fixtureSource = resolve(paths.source, "test/p13-updater-driver");
  await Promise.all([
    mkdir(artifactDirectory, { recursive: true }),
    mkdir(paths.signing, { recursive: true }),
    mkdir(resolve(fixtureSource, "src-tauri"), { recursive: true }),
  ]);
  const keyId = Buffer.from("0102030405060708", "hex");
  await Promise.all([
    writeFile(resolve(paths.signing, "fixture.key.pub"), publicKey(keyId)),
    writeFile(resolve(fixtureSource, "src-tauri/Cargo.lock"), "lock-v1\n"),
    writeFile(resolve(fixtureSource, "fixture-lock.json"), '{"v":1}\n'),
  ]);
  return { artifactDirectory, keyId, paths };
}

async function writeNsisSet(
  artifactDirectory,
  keyId,
  { version, architecture = "x64", signedNameOverride, signatureKeyId },
) {
  const stem = `CMClient P13 Updater Fixture_${version}_${architecture}-setup`;
  const installerName = `${stem}.exe`;
  const payloadName = `${stem}.nsis.zip`;
  const effectiveKeyId = signatureKeyId ?? keyId;
  const files = {
    installer: resolve(artifactDirectory, installerName),
    installerSignature: resolve(artifactDirectory, `${installerName}.sig`),
    updaterPayload: resolve(artifactDirectory, payloadName),
    updaterSignature: resolve(artifactDirectory, `${payloadName}.sig`),
  };
  await Promise.all([
    writeFile(files.installer, `installer:${version}`),
    writeFile(
      files.installerSignature,
      signature(effectiveKeyId, installerName),
    ),
    writeFile(files.updaterPayload, `payload:${version}`),
    writeFile(
      files.updaterSignature,
      signature(effectiveKeyId, signedNameOverride ?? payloadName),
    ),
  ]);
  return files;
}

test("artifact evidence selects one fresh same-version NSIS set", async (t) => {
  const fixture = await createFixture(t);
  await writeNsisSet(fixture.artifactDirectory, fixture.keyId, {
    version: "0.1.0",
  });
  await writeNsisSet(fixture.artifactDirectory, fixture.keyId, {
    version: "0.2.0",
  });

  const evidence = await inspectArtifacts(fixture.paths, {
    bundle: "nsis",
    version: "0.2.0",
    targetTriple: TARGET,
    targetDirectoryTriple: TARGET,
    buildStartedAtMs: Date.now() - 5_000,
  });

  assert.equal(evidence.schemaVersion, 2);
  assert.equal(evidence.version, "0.2.0");
  assert.equal(evidence.targetTriple, TARGET);
  assert.equal(evidence.artifactArchitecture, "x64");
  assert.match(evidence.signer.publicKeyFingerprint, /^sha256:[a-f0-9]{64}$/);
  assert.equal(evidence.signer.keyMaterialRecorded, false);
  assert.equal(evidence.comparisonDriver.name, "cargo-packager-updater");
  assert.equal(evidence.comparisonDriver.selected, false);
  assert.equal(evidence.toolchain.tauriCli.license, "Apache-2.0 OR MIT");
  assert.equal(
    evidence.fixtureDependencies.cargo.reqwest.source,
    "registry+https://github.com/rust-lang/crates.io-index",
  );
  assert.equal(
    evidence.fixtureDependencies.cargo.reqwest.license,
    "MIT OR Apache-2.0",
  );
  for (const artifact of Object.values(evidence.artifacts)) {
    assert.match(artifact.relativePath, /0\.2\.0/);
    assert.doesNotMatch(artifact.relativePath, /^[A-Za-z]:/);
    assert.ok(artifact.size > 0);
    assert.match(artifact.sha256, /^[a-f0-9]{64}$/);
  }
  const persisted = JSON.parse(
    await readFile(
      resolve(fixture.paths.evidence, "artifacts-nsis-0.2.0.json"),
      "utf8",
    ),
  );
  assert.equal(persisted.version, "0.2.0");
});

test("artifact evidence binds unversioned official macOS names to the fresh build", async (t) => {
  const fixture = await createFixture(t);
  const target = "x86_64-apple-darwin";
  const directory = resolve(
    fixture.paths.target,
    target,
    "release/bundle/macos",
  );
  await mkdir(directory, { recursive: true });
  const payloadName = "CMClient P13 Updater Fixture.app.tar.gz";
  await Promise.all([
    writeFile(resolve(directory, payloadName), "macos-payload"),
    writeFile(
      resolve(directory, `${payloadName}.sig`),
      signature(fixture.keyId, payloadName),
    ),
  ]);

  const evidence = await inspectArtifacts(fixture.paths, {
    bundle: "app",
    version: "0.2.0",
    targetTriple: target,
    targetDirectoryTriple: target,
    buildStartedAtMs: Date.now() - 5_000,
  });
  assert.equal(evidence.versionBinding, "fresh-build-context");
  assert.equal(evidence.artifactArchitecture, "x64");
  assert.deepEqual(Object.keys(evidence.artifacts), [
    "updaterPayload",
    "updaterSignature",
  ]);
});

test("artifact evidence rejects stale matching artifacts", async (t) => {
  const fixture = await createFixture(t);
  const files = await writeNsisSet(fixture.artifactDirectory, fixture.keyId, {
    version: "0.2.0",
  });
  const buildStartedAtMs = Date.now();
  const stale = new Date(buildStartedAtMs - 60_000);
  await Promise.all(
    Object.values(files).map((path) => utimes(path, stale, stale)),
  );

  await assert.rejects(
    inspectArtifacts(fixture.paths, {
      bundle: "nsis",
      version: "0.2.0",
      targetTriple: TARGET,
      targetDirectoryTriple: TARGET,
      buildStartedAtMs,
    }),
    /P13_UPDATER_ARTIFACT_STALE_OR_EMPTY/,
  );
});

test("artifact evidence rejects a signature bound to another file", async (t) => {
  const fixture = await createFixture(t);
  await writeNsisSet(fixture.artifactDirectory, fixture.keyId, {
    version: "0.2.0",
    signedNameOverride: "different.nsis.zip",
  });

  await assert.rejects(
    inspectArtifacts(fixture.paths, {
      bundle: "nsis",
      version: "0.2.0",
      targetTriple: TARGET,
      targetDirectoryTriple: TARGET,
      buildStartedAtMs: Date.now() - 5_000,
    }),
    /P13_UPDATER_ARTIFACT_SIGNATURE_FILE_MISMATCH/,
  );
});

test("artifact evidence rejects signer and target mismatches", async (t) => {
  const signerFixture = await createFixture(t);
  await writeNsisSet(signerFixture.artifactDirectory, signerFixture.keyId, {
    version: "0.2.0",
    signatureKeyId: Buffer.from("1112131415161718", "hex"),
  });
  await assert.rejects(
    inspectArtifacts(signerFixture.paths, {
      bundle: "nsis",
      version: "0.2.0",
      targetTriple: TARGET,
      targetDirectoryTriple: TARGET,
      buildStartedAtMs: Date.now() - 5_000,
    }),
    /P13_UPDATER_ARTIFACT_SIGNER_MISMATCH/,
  );

  const targetFixture = await createFixture(t);
  await writeNsisSet(targetFixture.artifactDirectory, targetFixture.keyId, {
    version: "0.2.0",
    architecture: "arm64",
  });
  await assert.rejects(
    inspectArtifacts(targetFixture.paths, {
      bundle: "nsis",
      version: "0.2.0",
      targetTriple: TARGET,
      targetDirectoryTriple: TARGET,
      buildStartedAtMs: Date.now() - 5_000,
    }),
    /P13_UPDATER_ARTIFACT_TARGET_MISMATCH/,
  );
});

test("fixture provenance lock rejects license and preview-driver drift", async () => {
  const documents = await readFixtureDocuments(repositoryRoot);
  const lock = JSON.parse(documents.lock);
  lock.dependencies.cargo.tauri.license = "unknown";
  lock.driverComparison.selected = true;
  const errors = validateFixtureDocuments({
    ...documents,
    lock: JSON.stringify(lock),
  });
  assert.ok(
    errors.some((entry) =>
      entry.startsWith("P13_UPDATER_FIXTURE_CRATE_PROVENANCE_DRIFT: tauri"),
    ),
  );
  assert.ok(errors.includes("P13_UPDATER_FIXTURE_COMPARISON_PROVENANCE_DRIFT"));
});
