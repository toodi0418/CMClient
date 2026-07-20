import { createHash, createPrivateKey, sign } from "node:crypto";
import { spawn } from "node:child_process";
import { createReadStream } from "node:fs";
import {
  chmod,
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  utimes,
  writeFile,
} from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";

import {
  archiveForTarget,
  DOCKER_COMPOSITION,
  dockerArtifactPlan as canonicalDockerArtifactPlan,
  dockerComposeArtifactPlan as canonicalDockerComposeArtifactPlan,
  nativeDesktopArtifactPlan,
  releaseArtifactName,
  releaseArtifactPlan,
  releaseComposition,
} from "./release-artifacts.mjs";
import {
  nativeDesktopArtifactsForTarget,
  verifyNativeDesktopStage,
} from "./desktop-native-bundles.mjs";

const CHANNELS = new Set(["stable", "beta", "dev"]);
const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;
const SHA256 = /^[a-f0-9]{64}$/;
const OCI_DIGEST = /^sha256:([a-f0-9]{64})$/;
const SOURCE_SHA = /^[a-f0-9]{40}$/;
const SIGNING_KEY_ID = /^[A-Za-z0-9._-]{1,128}$/;
const UTC_MILLISECONDS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const EPOCH = new Date(0);
const OCI_IMAGE_MANIFEST = "application/vnd.oci.image.manifest.v1+json";
const OCI_IMAGE_CONFIG = "application/vnd.oci.image.config.v1+json";
const PLATFORM_SIGNING_SCHEMA_VERSION = 1;
const PLATFORM_TRUST = Object.freeze({
  apple: "apple-codesign-notarization",
  authenticode: "windows-authenticode",
  appImage: "linux-appimage-gpg",
  provenance: "checksum-provenance",
});

function tarArguments(argumentsList) {
  return process.platform === "win32"
    ? ["--force-local", ...argumentsList]
    : argumentsList;
}

export function dockerArtifactPlans(version) {
  return canonicalDockerArtifactPlan(version);
}

function dockerArtifactPlanForTarget(version, target) {
  const plan = dockerArtifactPlans(version).find(
    (artifact) => artifact.target === target,
  );
  if (!plan) {
    throw new Error("RELEASE_DOCKER_TARGET_INVALID");
  }
  return plan;
}

export async function stageDockerArtifact({
  input,
  output,
  sourceSha,
  target,
  version,
}) {
  assertSourceSha(sourceSha);
  const plan = dockerArtifactPlanForTarget(version, target);
  const sourcePath = resolve(input);
  const metadata = await lstat(sourcePath);
  if (!metadata.isFile() || metadata.size < 1) {
    throw new Error("RELEASE_DOCKER_ARCHIVE_INVALID");
  }
  const image = await inspectOciArchive({
    archivePath: sourcePath,
    sourceSha,
    target: plan.target,
    version,
  });
  const outputRoot = resolve(output);
  await rm(outputRoot, { force: true, recursive: true });
  await mkdir(outputRoot, { recursive: true });
  const archivePath = join(outputRoot, plan.fileName);
  await copyFile(sourcePath, archivePath);
  const staged = {
    schemaVersion: 1,
    ...plan,
    sourceSha,
    imageDigest: image.imageDigest,
    createdAt: image.createdAt,
    sha256: await sha256File(archivePath),
    sizeBytes: metadata.size,
    composition: DOCKER_COMPOSITION,
  };
  await writeJson(join(outputRoot, plan.metadataFileName), staged);
  return staged;
}

export async function includeDockerArtifact({
  compose,
  input,
  output,
  sourceSha,
  version,
}) {
  assertSourceSha(sourceSha);
  const plans = dockerArtifactPlans(version);
  const composePlan = canonicalDockerComposeArtifactPlan(version);
  if (typeof compose !== "string") {
    throw new Error("RELEASE_DOCKER_COMPOSE_INVALID");
  }
  const composeSource = resolve(compose);
  let composeSourceMetadata;
  try {
    composeSourceMetadata = await lstat(composeSource);
  } catch {
    throw new Error("RELEASE_DOCKER_COMPOSE_INVALID");
  }
  if (!composeSourceMetadata.isFile() || composeSourceMetadata.size < 1) {
    throw new Error("RELEASE_DOCKER_COMPOSE_INVALID");
  }
  const inputRoot = resolve(input);
  const entries = (await readdir(inputRoot, { withFileTypes: true }))
    .map((entry) => `${entry.isFile() ? "file" : "other"}:${entry.name}`)
    .sort(compareCanonicalText);
  const expectedEntries = plans
    .flatMap((plan) => [
      `file:${plan.fileName}`,
      `file:${plan.metadataFileName}`,
    ])
    .sort(compareCanonicalText);
  if (JSON.stringify(entries) !== JSON.stringify(expectedEntries)) {
    throw new Error("RELEASE_DOCKER_INPUT_INVALID");
  }

  const dockerImages = [];
  for (const plan of plans) {
    let docker;
    try {
      docker = JSON.parse(
        await readFile(join(inputRoot, plan.metadataFileName), "utf8"),
      );
    } catch {
      throw new Error("RELEASE_DOCKER_METADATA_INVALID");
    }
    await assertDockerMetadata({
      archivePath: join(inputRoot, plan.fileName),
      docker,
      plan,
      sourceSha,
      version,
    });
    dockerImages.push(docker);
  }

  const outputRoot = resolve(output);
  const index = await readReleaseIndex(outputRoot);
  if (
    index.version !== version ||
    index.sourceSha !== undefined ||
    index.dockerImages !== undefined ||
    index.dockerCompose !== undefined
  ) {
    throw new Error("RELEASE_DOCKER_INDEX_INVALID");
  }
  for (const docker of dockerImages) {
    await copyFile(
      join(inputRoot, docker.fileName),
      join(outputRoot, docker.fileName),
    );
    await writeJson(join(outputRoot, docker.metadataFileName), docker);
  }
  const composePath = join(outputRoot, composePlan.fileName);
  await copyFile(composeSource, composePath);
  await chmod(composePath, 0o644);
  const composeMetadata = await lstat(composePath);
  const dockerCompose = {
    schemaVersion: 1,
    ...composePlan,
    sourceSha,
    sha256: await sha256File(composePath),
    sizeBytes: composeMetadata.size,
  };
  index.sourceSha = sourceSha;
  index.dockerImages = dockerImages;
  index.dockerCompose = dockerCompose;
  await writeJson(join(outputRoot, "release-index.json"), index);
  return { dockerCompose, dockerImages };
}

async function assertDockerMetadata({
  archivePath,
  docker,
  plan,
  sourceSha,
  version,
}) {
  assertSourceSha(sourceSha);
  if (
    !docker ||
    docker.schemaVersion !== 1 ||
    Object.entries(plan).some(([key, value]) => docker[key] !== value) ||
    docker.sourceSha !== sourceSha ||
    !OCI_DIGEST.test(docker.imageDigest) ||
    typeof docker.createdAt !== "string" ||
    Number.isNaN(Date.parse(docker.createdAt)) ||
    !SHA256.test(docker.sha256) ||
    !Number.isSafeInteger(docker.sizeBytes) ||
    docker.sizeBytes < 1 ||
    JSON.stringify(docker.composition) !== JSON.stringify(DOCKER_COMPOSITION)
  ) {
    throw new Error("RELEASE_DOCKER_METADATA_INVALID");
  }
  let archiveMetadata;
  try {
    archiveMetadata = await lstat(archivePath);
  } catch {
    throw new Error("RELEASE_DOCKER_ARCHIVE_INVALID");
  }
  if (
    !archiveMetadata.isFile() ||
    archiveMetadata.size !== docker.sizeBytes ||
    (await sha256File(archivePath)) !== docker.sha256
  ) {
    throw new Error("RELEASE_DOCKER_ARCHIVE_INVALID");
  }
  const inspected = await inspectOciArchive({
    archivePath,
    sourceSha,
    target: plan.target,
    version,
  });
  if (
    inspected.imageDigest !== docker.imageDigest ||
    inspected.createdAt !== docker.createdAt
  ) {
    throw new Error("RELEASE_DOCKER_METADATA_INVALID");
  }
}

async function assertDockerComposeMetadata({
  composePath,
  dockerCompose,
  plan,
  sourceSha,
}) {
  const expectedKeys = [
    "schemaVersion",
    ...Object.keys(plan),
    "sourceSha",
    "sha256",
    "sizeBytes",
  ].sort(compareCanonicalText);
  if (
    !dockerCompose ||
    JSON.stringify(Object.keys(dockerCompose).sort(compareCanonicalText)) !==
      JSON.stringify(expectedKeys) ||
    dockerCompose.schemaVersion !== 1 ||
    Object.entries(plan).some(([key, value]) => dockerCompose[key] !== value) ||
    dockerCompose.sourceSha !== sourceSha ||
    !SHA256.test(dockerCompose.sha256) ||
    !Number.isSafeInteger(dockerCompose.sizeBytes) ||
    dockerCompose.sizeBytes < 1
  ) {
    throw new Error("RELEASE_DOCKER_COMPOSE_INVALID");
  }
  let metadata;
  try {
    metadata = await lstat(composePath);
  } catch {
    throw new Error("RELEASE_DOCKER_COMPOSE_INVALID");
  }
  if (
    !metadata.isFile() ||
    metadata.size !== dockerCompose.sizeBytes ||
    (await sha256File(composePath)) !== dockerCompose.sha256
  ) {
    throw new Error("RELEASE_DOCKER_COMPOSE_INVALID");
  }
}

export async function inspectOciArchive({
  archivePath,
  sourceSha,
  target,
  version,
}) {
  assertSourceSha(sourceSha);
  const plan = dockerArtifactPlanForTarget(version, target);
  const entries = new Set(
    (await readTarOutput(archivePath, ["-tf"]))
      .toString("utf8")
      .split("\n")
      .filter(Boolean)
      .map((entry) => entry.replace(/^\.\//u, "").replace(/\/$/u, "")),
  );
  if (
    !entries.has("index.json") ||
    !entries.has("oci-layout") ||
    [...entries].some(
      (entry) =>
        entry.startsWith("/") ||
        entry.split("/").some((segment) => segment === ".."),
    )
  ) {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }

  const layout = await readOciJson(archivePath, "oci-layout");
  if (layout?.imageLayoutVersion !== "1.0.0") {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }

  const index = await readOciJson(archivePath, "index.json");
  if (
    index?.schemaVersion !== 2 ||
    !Array.isArray(index.manifests) ||
    index.manifests.length !== 1
  ) {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }
  const manifestDescriptor = index.manifests[0];
  if (
    manifestDescriptor?.platform?.os !== "linux" ||
    manifestDescriptor?.platform?.architecture !== plan.architecture
  ) {
    throw new Error("RELEASE_DOCKER_OCI_IDENTITY_INVALID");
  }
  const manifestPath = descriptorPath(manifestDescriptor, OCI_IMAGE_MANIFEST);
  if (!entries.has(manifestPath)) {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }
  const manifestBytes = await readTarOutput(archivePath, [
    "-xOf",
    manifestPath,
  ]);
  assertDescriptorBytes(manifestDescriptor, manifestBytes);
  const manifest = parseOciJson(manifestBytes);
  if (
    manifest?.schemaVersion !== 2 ||
    manifest.mediaType !== OCI_IMAGE_MANIFEST ||
    !Array.isArray(manifest.layers) ||
    manifest.layers.length === 0
  ) {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }
  const configPath = descriptorPath(manifest.config, OCI_IMAGE_CONFIG);
  for (const descriptor of manifest.layers) {
    const layerPath = descriptorPath(descriptor);
    if (!entries.has(layerPath)) {
      throw new Error("RELEASE_DOCKER_OCI_INVALID");
    }
  }
  if (!entries.has(configPath)) {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }
  const configBytes = await readTarOutput(archivePath, ["-xOf", configPath]);
  assertDescriptorBytes(manifest.config, configBytes);
  const config = parseOciJson(configBytes);
  const labels = config?.config?.Labels;
  const environment = new Set(config?.config?.Env ?? []);
  if (
    config?.os !== "linux" ||
    config?.architecture !== plan.architecture ||
    labels?.["org.opencontainers.image.title"] !== "cmclient" ||
    labels?.["org.opencontainers.image.version"] !== version ||
    labels?.["org.opencontainers.image.revision"] !== sourceSha ||
    typeof labels?.["org.opencontainers.image.created"] !== "string" ||
    Number.isNaN(Date.parse(labels["org.opencontainers.image.created"])) ||
    !environment.has(`CMCLIENT_BUILD_VERSION=${version}`) ||
    !environment.has(`CMCLIENT_BUILD_COMMIT=${sourceSha}`) ||
    !environment.has(`CMCLIENT_BUILD_CHANNEL=${buildChannel(version)}`)
  ) {
    throw new Error("RELEASE_DOCKER_OCI_IDENTITY_INVALID");
  }
  return {
    imageDigest: manifestDescriptor.digest,
    createdAt: labels["org.opencontainers.image.created"],
  };
}

function buildChannel(version) {
  return version.includes("-dev.")
    ? "dev"
    : version.includes("-")
      ? "beta"
      : "stable";
}

function descriptorPath(descriptor, expectedMediaType) {
  const match = OCI_DIGEST.exec(descriptor?.digest);
  if (
    !match ||
    !Number.isSafeInteger(descriptor.size) ||
    descriptor.size < 1 ||
    (expectedMediaType !== undefined &&
      descriptor.mediaType !== expectedMediaType)
  ) {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }
  return `blobs/sha256/${match[1]}`;
}

function assertDescriptorBytes(descriptor, bytes) {
  const digest = createHash("sha256").update(bytes).digest("hex");
  if (
    descriptor.size !== bytes.length ||
    descriptor.digest !== `sha256:${digest}`
  ) {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }
}

async function readOciJson(archivePath, entry) {
  return parseOciJson(await readTarOutput(archivePath, ["-xOf", entry]));
}

function parseOciJson(bytes) {
  try {
    return JSON.parse(bytes.toString("utf8"));
  } catch {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }
}

export async function assembleReleaseArtifacts({ input, output, version }) {
  const plan = releaseArtifactPlan(version);
  const inputRoot = resolve(input);
  const outputRoot = resolve(output);
  const temporaryRoot = await mkdtemp(join(tmpdir(), "cmclient-release-"));
  await mkdir(outputRoot, { recursive: true });

  try {
    const artifacts = [];
    for (const planned of plan) {
      const buildDirectory = join(inputRoot, planned.component, planned.target);
      const buildManifest = await readBuildManifest(
        join(buildDirectory, "build-manifest.json"),
        planned,
        version,
      );
      await assertNoUnexpectedBuildEntries(buildDirectory, buildManifest);

      const packageRoot = join(
        temporaryRoot,
        `${planned.component}-${planned.target}`,
      );
      const metadataDirectory = join(packageRoot, "metadata");
      await mkdir(metadataDirectory, { recursive: true });

      for (const content of buildManifest.contents) {
        const source = manifestPath(buildDirectory, content.path);
        const destination = manifestPath(packageRoot, content.path);
        await copyReleaseContent(source, destination, content);
      }
      const stagedManifest = join(metadataDirectory, "build-manifest.json");
      await writeFile(
        stagedManifest,
        `${JSON.stringify(buildManifest, null, 2)}\n`,
      );
      await chmod(stagedManifest, 0o644);
      await utimes(stagedManifest, EPOCH, EPOCH);
      await normalizeReleaseDirectories(packageRoot);

      const archivePath = join(outputRoot, planned.fileName);
      await rm(archivePath, { force: true });
      await createArchive({
        archive: planned.archive,
        archivePath,
        entries: [
          ...buildManifest.contents.map(({ path }) => path),
          "metadata/build-manifest.json",
        ],
        cwd: packageRoot,
      });
      const metadata = await stat(archivePath);
      artifacts.push({
        ...planned,
        sha256: await sha256File(archivePath),
        sizeBytes: metadata.size,
      });
    }

    const nativeDesktop = await collectNativeDesktopArtifacts({
      input: join(inputRoot, "native-desktop"),
      output: outputRoot,
      version,
    });
    const index = {
      schemaVersion: 3,
      version,
      artifacts,
      nativeDesktop,
    };
    await writeJson(join(outputRoot, "release-index.json"), index);
    return index;
  } finally {
    await rm(temporaryRoot, { force: true, recursive: true });
  }
}

export async function collectNativeDesktopArtifacts({
  input,
  output,
  version,
}) {
  const plan = nativeDesktopArtifactPlan(version);
  const inputRoot = resolve(input);
  const outputRoot = resolve(output);
  const targets = [...new Set(plan.map(({ target }) => target))];
  let targetEntries;
  try {
    targetEntries = (await readdir(inputRoot, { withFileTypes: true }))
      .map(
        (entry) =>
          `${entry.isDirectory() ? "directory" : "other"}:${entry.name}`,
      )
      .sort(compareCanonicalText);
  } catch {
    throw new Error("RELEASE_NATIVE_DESKTOP_INPUT_INVALID");
  }
  const expectedEntries = targets
    .map((target) => `directory:${target}`)
    .sort(compareCanonicalText);
  if (JSON.stringify(targetEntries) !== JSON.stringify(expectedEntries)) {
    throw new Error("RELEASE_NATIVE_DESKTOP_INPUT_INVALID");
  }

  for (const target of targets) {
    try {
      await verifyNativeDesktopStage({
        target,
        version,
        input: join(inputRoot, target),
      });
    } catch {
      throw new Error("RELEASE_NATIVE_DESKTOP_INPUT_INVALID");
    }
  }

  const artifacts = [];
  for (const planned of plan) {
    const source = join(inputRoot, planned.target, planned.fileName);
    const destination = join(outputRoot, planned.fileName);
    await copyFile(source, destination);
    await chmod(destination, 0o644);
    const metadata = await stat(destination);
    artifacts.push({
      ...planned,
      sha256: await sha256File(destination),
      sizeBytes: metadata.size,
    });
  }
  return artifacts;
}

export async function createPlatformSigningReceipt({
  identityReference,
  input,
  output,
  sourceSha,
  target,
  version,
}) {
  assertSourceSha(sourceSha);
  assertPlatformIdentityReference(identityReference);
  const inputRoot = resolve(input);
  const plan = nativeDesktopArtifactsForTarget(target, version);
  await verifyNativeDesktopStage({ target, version, input: inputRoot });
  const artifacts = [];
  for (const planned of plan) {
    const artifactPath = join(inputRoot, planned.fileName);
    const metadata = await lstat(artifactPath);
    if (!metadata.isFile() || metadata.size < 1) {
      throw new Error("RELEASE_PLATFORM_SIGNING_ARTIFACT_INVALID");
    }
    artifacts.push({
      fileName: planned.fileName,
      sha256: await sha256File(artifactPath),
      sizeBytes: metadata.size,
      trust: platformTrustForArtifact(planned),
    });
  }
  const receipt = {
    schemaVersion: PLATFORM_SIGNING_SCHEMA_VERSION,
    target,
    version,
    sourceSha,
    identityReference,
    artifacts,
  };
  const outputPath = resolve(output);
  await mkdir(dirname(outputPath), { recursive: true });
  await writeJson(outputPath, receipt);
  return receipt;
}

export async function finalizePlatformSignedRelease({
  input,
  output,
  sbom,
  sourceSha,
  version,
}) {
  assertSourceSha(sourceSha);
  const outputRoot = resolve(output);
  await verifyReleaseOutput({ output: outputRoot, sourceSha, version });
  const index = await readReleaseIndex(outputRoot);
  if (index.platformSigning !== undefined) {
    throw new Error("RELEASE_PLATFORM_SIGNING_ALREADY_FINALIZED");
  }
  const receipts = await readPlatformSignedNativeInput({
    input,
    sourceSha,
    version,
  });
  const unsignedByName = new Map(
    index.nativeDesktop.map((artifact) => [artifact.fileName, { ...artifact }]),
  );
  const nativeByName = new Map(
    index.nativeDesktop.map((artifact) => [artifact.fileName, artifact]),
  );
  const receiptPlans = receipts.map((receipt) => {
    const finalizedArtifacts = [];
    for (const artifact of receipt.artifacts) {
      const unsigned = unsignedByName.get(artifact.fileName);
      const indexed = nativeByName.get(artifact.fileName);
      if (!unsigned || !indexed) {
        throw new Error("RELEASE_PLATFORM_SIGNING_ARTIFACT_INVALID");
      }
      if (
        artifact.trust !== PLATFORM_TRUST.provenance &&
        artifact.sha256 === unsigned.sha256
      ) {
        throw new Error("RELEASE_PLATFORM_SIGNING_ARTIFACT_UNCHANGED");
      }
      finalizedArtifacts.push({
        ...artifact,
        unsignedSha256: unsigned.sha256,
      });
    }
    return { finalizedArtifacts, receipt };
  });

  const sbomSource = resolve(sbom);
  let sbomValue;
  try {
    const metadata = await lstat(sbomSource);
    sbomValue = JSON.parse(await readFile(sbomSource, "utf8"));
    if (!metadata.isFile() || metadata.size < 1) throw new Error();
  } catch {
    throw new Error("RELEASE_PLATFORM_SIGNING_SBOM_INVALID");
  }
  if (sbomValue?.spdxVersion !== "SPDX-2.3") {
    throw new Error("RELEASE_PLATFORM_SIGNING_SBOM_INVALID");
  }

  const finalizedReceipts = [];
  for (const { finalizedArtifacts, receipt } of receiptPlans) {
    for (const artifact of receipt.artifacts) {
      const indexed = nativeByName.get(artifact.fileName);
      const source = join(
        resolve(input),
        "native-desktop",
        receipt.target,
        artifact.fileName,
      );
      const destination = join(outputRoot, artifact.fileName);
      await copyFile(source, destination);
      await chmod(destination, 0o644);
      indexed.sha256 = artifact.sha256;
      indexed.sizeBytes = artifact.sizeBytes;
    }
    const receiptFileName = platformSigningReceiptFileName(
      receipt.target,
      version,
    );
    const receiptSource = join(
      resolve(input),
      "platform-signing",
      receiptFileName,
    );
    const receiptDestination = join(outputRoot, receiptFileName);
    await copyFile(receiptSource, receiptDestination);
    await chmod(receiptDestination, 0o644);
    finalizedReceipts.push({
      target: receipt.target,
      identityReference: receipt.identityReference,
      receiptFileName,
      receiptSha256: await sha256File(receiptDestination),
      artifacts: finalizedArtifacts,
    });
  }
  await copyFile(sbomSource, join(outputRoot, `cmclient-${version}.spdx.json`));

  index.platformSigning = {
    schemaVersion: PLATFORM_SIGNING_SCHEMA_VERSION,
    sourceSha,
    receipts: finalizedReceipts,
  };
  await writeJson(join(outputRoot, "release-index.json"), index);
  await rm(join(outputRoot, "SHA256SUMS"), { force: true });
  await rm(join(outputRoot, "SHA256SUMS.sigstore.json"), { force: true });
  await finalizeChecksums({ output: outputRoot });
  await verifyReleaseOutput({
    output: outputRoot,
    requirePlatformSigning: true,
    sourceSha,
    version,
  });
  return index;
}

export async function finalizeChecksums({ output }) {
  const outputRoot = resolve(output);
  const index = await readReleaseIndex(outputRoot);
  const sbomNames = await exactSbomNames(outputRoot, index);
  const names = [
    ...index.artifacts.map((artifact) => artifact.fileName),
    ...index.nativeDesktop.map((artifact) => artifact.fileName),
    ...(index.dockerImages ?? []).flatMap((image) => [
      image.fileName,
      image.metadataFileName,
    ]),
    ...(index.dockerCompose ? [index.dockerCompose.fileName] : []),
    ...platformSigningReceiptNames(index),
    ...sbomNames,
    "release-index.json",
  ].sort();
  const entries = [];
  for (const fileName of names) {
    const filePath = join(outputRoot, fileName);
    const metadata = await lstat(filePath);
    if (!metadata.isFile()) {
      throw new Error("RELEASE_CHECKSUM_SUBJECT_INVALID");
    }
    entries.push({ fileName, sha256: await sha256File(filePath) });
  }
  const content = checksumFileContents(entries);
  await writeFile(join(outputRoot, "SHA256SUMS"), content);
  return entries;
}

export async function verifyReleaseOutput({
  output,
  requirePlatformSigning = false,
  sourceSha,
  version,
}) {
  const outputRoot = resolve(output);
  const index = await readReleaseIndex(outputRoot);
  if (index.version !== version) {
    throw new Error("RELEASE_INDEX_VERSION_INVALID");
  }
  const plan = releaseArtifactPlan(version);
  if (index.artifacts.length !== plan.length) {
    throw new Error("RELEASE_INDEX_ARTIFACTS_INVALID");
  }
  const expectedByName = new Map(
    plan.map((artifact) => [artifact.fileName, artifact]),
  );
  for (const artifact of index.artifacts) {
    const expected = expectedByName.get(artifact.fileName);
    if (
      !expected ||
      artifact.component !== expected.component ||
      artifact.target !== expected.target ||
      artifact.archive !== expected.archive ||
      artifact.sha256.length !== 64 ||
      !Number.isSafeInteger(artifact.sizeBytes) ||
      artifact.sizeBytes < 1
    ) {
      throw new Error("RELEASE_INDEX_ARTIFACTS_INVALID");
    }
    const path = join(outputRoot, artifact.fileName);
    const metadata = await stat(path);
    if (
      metadata.size !== artifact.sizeBytes ||
      (await sha256File(path)) !== artifact.sha256
    ) {
      throw new Error("RELEASE_ARCHIVE_DIGEST_INVALID");
    }
  }

  const nativePlan = nativeDesktopArtifactPlan(version);
  if (index.nativeDesktop.length !== nativePlan.length) {
    throw new Error("RELEASE_NATIVE_DESKTOP_INDEX_INVALID");
  }
  const nativeByName = new Map(
    nativePlan.map((artifact) => [artifact.fileName, artifact]),
  );
  const seenNative = new Set();
  for (const artifact of index.nativeDesktop) {
    const expected = nativeByName.get(artifact.fileName);
    if (
      !expected ||
      seenNative.has(artifact.fileName) ||
      artifact.component !== expected.component ||
      artifact.target !== expected.target ||
      artifact.bundle !== expected.bundle ||
      artifact.updaterManaged !== false ||
      !SHA256.test(artifact.sha256) ||
      !Number.isSafeInteger(artifact.sizeBytes) ||
      artifact.sizeBytes < 1
    ) {
      throw new Error("RELEASE_NATIVE_DESKTOP_INDEX_INVALID");
    }
    seenNative.add(artifact.fileName);
    const path = join(outputRoot, artifact.fileName);
    const metadata = await stat(path);
    if (
      metadata.size !== artifact.sizeBytes ||
      (await sha256File(path)) !== artifact.sha256
    ) {
      throw new Error("RELEASE_NATIVE_DESKTOP_DIGEST_INVALID");
    }
  }

  if (sourceSha !== undefined) {
    assertSourceSha(sourceSha);
    if (index.sourceSha !== sourceSha) {
      throw new Error("RELEASE_INDEX_SOURCE_SHA_INVALID");
    }
    if (
      !Array.isArray(index.dockerImages) ||
      index.dockerImages.length !== dockerArtifactPlans(version).length ||
      !index.dockerCompose
    ) {
      throw new Error("RELEASE_DOCKER_INDEX_INVALID");
    }
  }
  if (index.dockerImages !== undefined || index.dockerCompose !== undefined) {
    if (!SOURCE_SHA.test(index.sourceSha)) {
      throw new Error("RELEASE_INDEX_SOURCE_SHA_INVALID");
    }
    const dockerPlan = dockerArtifactPlans(version);
    if (
      !Array.isArray(index.dockerImages) ||
      index.dockerImages.length !== dockerPlan.length ||
      !index.dockerCompose
    ) {
      throw new Error("RELEASE_DOCKER_INDEX_INVALID");
    }
    const imagesByTarget = new Map(
      index.dockerImages.map((image) => [image?.target, image]),
    );
    if (imagesByTarget.size !== dockerPlan.length) {
      throw new Error("RELEASE_DOCKER_INDEX_INVALID");
    }
    for (const plan of dockerPlan) {
      const docker = imagesByTarget.get(plan.target);
      if (!docker) {
        throw new Error("RELEASE_DOCKER_INDEX_INVALID");
      }
      await assertDockerMetadata({
        archivePath: join(outputRoot, docker.fileName),
        docker,
        plan,
        sourceSha: sourceSha ?? index.sourceSha,
        version,
      });
      let metadata;
      try {
        metadata = JSON.parse(
          await readFile(join(outputRoot, docker.metadataFileName), "utf8"),
        );
      } catch {
        throw new Error("RELEASE_DOCKER_METADATA_INVALID");
      }
      if (JSON.stringify(metadata) !== JSON.stringify(docker)) {
        throw new Error("RELEASE_DOCKER_METADATA_INVALID");
      }
    }
    const composePlan = canonicalDockerComposeArtifactPlan(version);
    await assertDockerComposeMetadata({
      composePath: join(outputRoot, composePlan.fileName),
      dockerCompose: index.dockerCompose,
      plan: composePlan,
      sourceSha: sourceSha ?? index.sourceSha,
    });
  }

  await assertPlatformSigningMetadata({
    index,
    outputRoot,
    requirePlatformSigning,
    sourceSha: sourceSha ?? index.sourceSha,
    version,
  });

  const expectedChecksums = await expectedChecksumEntries(outputRoot, index);
  const checksumPath = join(outputRoot, "SHA256SUMS");
  const actualChecksums = parseChecksumFile(
    await readFile(checksumPath, "utf8"),
  );
  if (
    checksumFileContents(actualChecksums) !==
    checksumFileContents(expectedChecksums)
  ) {
    throw new Error("RELEASE_CHECKSUM_FILE_INVALID");
  }
  await assertExactReleaseOutputEntries(outputRoot, index);
  return index;
}

export function checksumFileContents(entries) {
  const names = new Set();
  const normalized = entries
    .map((entry) => {
      if (
        !isSafeArtifactFileName(entry.fileName) ||
        !SHA256.test(entry.sha256)
      ) {
        throw new Error("RELEASE_CHECKSUM_ENTRY_INVALID");
      }
      if (names.has(entry.fileName)) {
        throw new Error("RELEASE_CHECKSUM_ENTRY_DUPLICATE");
      }
      names.add(entry.fileName);
      return { fileName: entry.fileName, sha256: entry.sha256 };
    })
    .sort((left, right) => compareCanonicalText(left.fileName, right.fileName));
  return `${normalized.map((entry) => `${entry.sha256} *${entry.fileName}`).join("\n")}\n`;
}

export function canonicalUpdateManifest(manifest) {
  assertUpdateManifest(manifest);
  return Buffer.from(
    JSON.stringify({
      schemaVersion: manifest.schemaVersion,
      channel: manifest.channel,
      version: manifest.version,
      publishedAt: manifest.publishedAt,
      minimumAgentVersion: manifest.minimumAgentVersion,
      bundles: manifest.bundles.map((bundle) => ({
        component: bundle.component,
        target: bundle.target,
        archive: bundle.archive,
        url: bundle.url,
        sha256: bundle.sha256,
        sizeBytes: bundle.sizeBytes,
      })),
    }),
    "utf8",
  );
}

export function createSignedUpdateManifest({
  artifacts,
  channel,
  minimumAgentVersion,
  privateKeyBase64,
  publishedAt,
  releaseBaseUrl,
  signingKeyId,
  version,
}) {
  assertVersion(version);
  assertVersion(minimumAgentVersion);
  if (!CHANNELS.has(channel)) {
    throw new Error("RELEASE_MANIFEST_CHANNEL_INVALID");
  }
  if (
    !UTC_MILLISECONDS.test(publishedAt) ||
    new Date(publishedAt).toISOString() !== publishedAt
  ) {
    throw new Error("RELEASE_MANIFEST_PUBLISHED_AT_INVALID");
  }
  if (!SIGNING_KEY_ID.test(signingKeyId)) {
    throw new Error("RELEASE_MANIFEST_SIGNING_KEY_ID_INVALID");
  }
  const baseUrl = normalizeReleaseBaseUrl(releaseBaseUrl);
  const expectedPlan = releaseArtifactPlan(version);
  const artifactsByName = new Map(
    artifacts.map((artifact) => [artifact.fileName, artifact]),
  );
  if (artifactsByName.size !== expectedPlan.length) {
    throw new Error("RELEASE_MANIFEST_ARTIFACTS_INVALID");
  }

  const bundles = expectedPlan.map((planned) => {
    const artifact = artifactsByName.get(planned.fileName);
    if (
      !artifact ||
      artifact.component !== planned.component ||
      artifact.target !== planned.target ||
      artifact.archive !== planned.archive ||
      !SHA256.test(artifact.sha256) ||
      !Number.isSafeInteger(artifact.sizeBytes) ||
      artifact.sizeBytes < 1
    ) {
      throw new Error("RELEASE_MANIFEST_ARTIFACTS_INVALID");
    }
    return {
      component: planned.component,
      target: planned.target,
      archive: planned.archive,
      url: new URL(planned.fileName, baseUrl).toString(),
      sha256: artifact.sha256,
      sizeBytes: artifact.sizeBytes,
    };
  });
  const manifest = {
    schemaVersion: 1,
    channel,
    version,
    publishedAt,
    minimumAgentVersion,
    bundles,
  };
  const privateKey = privateKeyFromBase64(privateKeyBase64);
  const signature = sign(null, canonicalUpdateManifest(manifest), privateKey)
    .toString("base64")
    .replace(/=+$/u, "");
  return {
    manifest,
    signingKeyId,
    signatureAlgorithm: "ed25519",
    signature,
  };
}

async function createArchive({ archive, archivePath, entries, cwd }) {
  if (archive === "zip") {
    await run("zip", ["-X", "-q", "-r", archivePath, ...entries], cwd);
    return;
  }
  if (process.platform !== "linux") {
    const tarPath = join(cwd, ".cmclient-release.tar");
    try {
      await run("tar", tarArguments(["-cf", tarPath, ...entries]), cwd);
      await run(
        "zstd",
        ["--quiet", "--force", "-19", "-o", archivePath, tarPath],
        cwd,
      );
    } finally {
      await rm(tarPath, { force: true });
    }
    return;
  }
  await run(
    "tar",
    tarArguments([
      "--zstd",
      "--sort=name",
      "--mtime=@0",
      "--owner=0",
      "--group=0",
      "--numeric-owner",
      "-cf",
      archivePath,
      ...entries,
    ]),
    cwd,
  );
}

async function assertNoUnexpectedBuildEntries(buildDirectory, manifest) {
  const { contents } = manifest;
  const exactFiles = new Set([
    "build-manifest.json",
    ...contents.filter(({ kind }) => kind === "file").map(({ path }) => path),
  ]);
  const directoryRoots = contents
    .filter(({ kind }) => kind === "directory")
    .map(({ path }) => path);
  const permittedParents = new Set();
  for (const path of [...exactFiles, ...directoryRoots]) {
    const segments = path.split("/");
    for (let index = 1; index < segments.length; index += 1) {
      permittedParents.add(segments.slice(0, index).join("/"));
    }
  }

  const actualFiles = [];
  await walkBuildEntries(buildDirectory, "", ({ path, metadata }) => {
    if (
      metadata.isSymbolicLink() ||
      (!metadata.isFile() && !metadata.isDirectory())
    ) {
      throw new Error("RELEASE_BUILD_CONTENT_INVALID");
    }
    const insideDirectory = directoryRoots.some(
      (root) => path === root || path.startsWith(`${root}/`),
    );
    if (metadata.isDirectory()) {
      if (!insideDirectory && !permittedParents.has(path)) {
        throw new Error("RELEASE_BUILD_CONTENT_UNEXPECTED");
      }
    } else {
      if (!insideDirectory && !exactFiles.has(path)) {
        throw new Error("RELEASE_BUILD_CONTENT_UNEXPECTED");
      }
      if (path !== "build-manifest.json") {
        actualFiles.push(path);
      }
    }
  });
  actualFiles.sort(compareCanonicalText);
  if (JSON.stringify(actualFiles) !== JSON.stringify(manifest.files)) {
    throw new Error("RELEASE_BUILD_CONTENT_UNEXPECTED");
  }
}

async function walkBuildEntries(directory, prefix, visitor) {
  const entries = await readdir(directory, { withFileTypes: true });
  entries.sort((left, right) => compareCanonicalText(left.name, right.name));
  for (const entry of entries) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    const filesystemPath = join(directory, entry.name);
    const metadata = await lstat(filesystemPath);
    visitor({ path, metadata });
    if (metadata.isDirectory()) {
      await walkBuildEntries(filesystemPath, path, visitor);
    }
  }
}

function manifestPath(root, relativePath) {
  return join(root, ...relativePath.split("/"));
}

async function copyReleaseContent(source, destination, content) {
  const metadata = await lstat(source);
  if (content.kind === "file") {
    if (!metadata.isFile()) {
      throw new Error("RELEASE_BUILD_CONTENT_INVALID");
    }
    await mkdir(dirname(destination), { recursive: true });
    await copyFile(source, destination);
    await chmod(destination, content.executable ? 0o755 : 0o644);
    await utimes(destination, EPOCH, EPOCH);
    return;
  }
  if (!metadata.isDirectory()) {
    throw new Error("RELEASE_BUILD_CONTENT_INVALID");
  }
  await copyReleaseTree(source, destination);
}

async function copyReleaseTree(source, destination) {
  await mkdir(destination, { recursive: true });
  const entries = await readdir(source, { withFileTypes: true });
  entries.sort((left, right) => compareCanonicalText(left.name, right.name));
  if (entries.length === 0) {
    throw new Error("RELEASE_BUILD_CONTENT_EMPTY");
  }
  for (const entry of entries) {
    const sourcePath = join(source, entry.name);
    const destinationPath = join(destination, entry.name);
    const metadata = await lstat(sourcePath);
    if (
      metadata.isSymbolicLink() ||
      (!metadata.isFile() && !metadata.isDirectory())
    ) {
      throw new Error("RELEASE_BUILD_CONTENT_INVALID");
    }
    if (metadata.isDirectory()) {
      await copyReleaseTree(sourcePath, destinationPath);
    } else {
      await copyFile(sourcePath, destinationPath);
      await chmod(destinationPath, 0o644);
      await utimes(destinationPath, EPOCH, EPOCH);
    }
  }
  await chmod(destination, 0o755);
  await utimes(destination, EPOCH, EPOCH);
}

async function normalizeReleaseDirectories(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const path = join(directory, entry.name);
    await normalizeReleaseDirectories(path);
    await chmod(path, 0o755);
    await utimes(path, EPOCH, EPOCH);
  }
}

async function readBuildManifest(path, planned, version) {
  let manifest;
  try {
    manifest = JSON.parse(await readFile(path, "utf8"));
  } catch {
    throw new Error("RELEASE_BUILD_MANIFEST_INVALID");
  }
  const expectedContents = releaseComposition(
    planned.component,
    planned.target,
  );
  if (
    manifest?.schemaVersion !== 2 ||
    manifest.component !== planned.component ||
    manifest.target !== planned.target ||
    manifest.version !== version ||
    manifest.releaseAsset?.archive !== planned.archive ||
    manifest.releaseAsset?.fileName !== planned.fileName ||
    JSON.stringify(manifest.contents) !== JSON.stringify(expectedContents) ||
    !isCanonicalBuildFileList(manifest.files, expectedContents)
  ) {
    throw new Error("RELEASE_BUILD_MANIFEST_INVALID");
  }
  return manifest;
}

function isCanonicalBuildFileList(files, contents) {
  if (!Array.isArray(files) || files.length === 0) {
    return false;
  }
  const sorted = [...files].sort(compareCanonicalText);
  if (
    JSON.stringify(files) !== JSON.stringify(sorted) ||
    new Set(files).size !== files.length
  ) {
    return false;
  }
  const validPath = (path) =>
    typeof path === "string" &&
    path.length > 0 &&
    !path.includes("\\") &&
    !path.includes("\0") &&
    path
      .split("/")
      .every((segment) => segment && segment !== "." && segment !== "..");
  if (!files.every(validPath)) {
    return false;
  }
  return (
    contents.every((content) =>
      content.kind === "file"
        ? files.includes(content.path)
        : files.some((path) => path.startsWith(`${content.path}/`)),
    ) &&
    files.every((path) =>
      contents.some((content) =>
        content.kind === "file"
          ? path === content.path
          : path.startsWith(`${content.path}/`),
      ),
    )
  );
}

function platformTrustForArtifact(artifact) {
  if (artifact.target.startsWith("darwin-") && artifact.bundle === "dmg") {
    return PLATFORM_TRUST.apple;
  }
  if (artifact.target.startsWith("windows-")) {
    return PLATFORM_TRUST.authenticode;
  }
  if (artifact.target.startsWith("linux-") && artifact.bundle === "appimage") {
    return PLATFORM_TRUST.appImage;
  }
  if (artifact.target.startsWith("linux-") && artifact.bundle === "deb") {
    return PLATFORM_TRUST.provenance;
  }
  throw new Error("RELEASE_PLATFORM_SIGNING_TRUST_INVALID");
}

function platformSigningReceiptFileName(target, version) {
  nativeDesktopArtifactsForTarget(target, version);
  return `cmclient-platform-signing-${target}-${version}.json`;
}

function platformSigningReceiptNames(index) {
  return (
    index.platformSigning?.receipts?.map(
      ({ receiptFileName }) => receiptFileName,
    ) ?? []
  );
}

function assertPlatformIdentityReference(value) {
  if (
    typeof value !== "string" ||
    value.length < 1 ||
    value.length > 256 ||
    value.trim() !== value ||
    [...value].some((character) => {
      const code = character.codePointAt(0);
      return code < 0x20 || code === 0x7f;
    })
  ) {
    throw new Error("RELEASE_PLATFORM_SIGNING_IDENTITY_INVALID");
  }
}

function hasExactKeys(value, expected) {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    JSON.stringify(Object.keys(value).sort(compareCanonicalText)) ===
      JSON.stringify([...expected].sort(compareCanonicalText))
  );
}

async function readPlatformSigningReceipt({
  path,
  sourceSha,
  stage,
  target,
  version,
}) {
  let receipt;
  try {
    receipt = JSON.parse(await readFile(path, "utf8"));
  } catch {
    throw new Error("RELEASE_PLATFORM_SIGNING_RECEIPT_INVALID");
  }
  if (
    !hasExactKeys(receipt, [
      "schemaVersion",
      "target",
      "version",
      "sourceSha",
      "identityReference",
      "artifacts",
    ]) ||
    receipt.schemaVersion !== PLATFORM_SIGNING_SCHEMA_VERSION ||
    receipt.target !== target ||
    receipt.version !== version ||
    receipt.sourceSha !== sourceSha ||
    !Array.isArray(receipt.artifacts)
  ) {
    throw new Error("RELEASE_PLATFORM_SIGNING_RECEIPT_INVALID");
  }
  assertPlatformIdentityReference(receipt.identityReference);
  const plan = nativeDesktopArtifactsForTarget(target, version);
  if (receipt.artifacts.length !== plan.length) {
    throw new Error("RELEASE_PLATFORM_SIGNING_RECEIPT_INVALID");
  }
  for (const [index, artifact] of receipt.artifacts.entries()) {
    const planned = plan[index];
    if (
      !hasExactKeys(artifact, ["fileName", "sha256", "sizeBytes", "trust"]) ||
      artifact.fileName !== planned.fileName ||
      artifact.trust !== platformTrustForArtifact(planned) ||
      !SHA256.test(artifact.sha256) ||
      !Number.isSafeInteger(artifact.sizeBytes) ||
      artifact.sizeBytes < 1
    ) {
      throw new Error("RELEASE_PLATFORM_SIGNING_RECEIPT_INVALID");
    }
    const artifactPath = join(stage, artifact.fileName);
    let metadata;
    try {
      metadata = await lstat(artifactPath);
    } catch {
      throw new Error("RELEASE_PLATFORM_SIGNING_ARTIFACT_INVALID");
    }
    if (
      !metadata.isFile() ||
      metadata.size !== artifact.sizeBytes ||
      (await sha256File(artifactPath)) !== artifact.sha256
    ) {
      throw new Error("RELEASE_PLATFORM_SIGNING_ARTIFACT_INVALID");
    }
  }
  return receipt;
}

async function readPlatformSignedNativeInput({ input, sourceSha, version }) {
  const inputRoot = resolve(input);
  const plan = nativeDesktopArtifactPlan(version);
  const targets = [...new Set(plan.map(({ target }) => target))];
  const expectedRootEntries = ["native-desktop", "platform-signing"];
  let rootEntries;
  try {
    rootEntries = (await readdir(inputRoot, { withFileTypes: true }))
      .map(
        (entry) =>
          `${entry.isDirectory() ? "directory" : "other"}:${entry.name}`,
      )
      .sort(compareCanonicalText);
  } catch {
    throw new Error("RELEASE_PLATFORM_SIGNING_INPUT_INVALID");
  }
  if (
    JSON.stringify(rootEntries) !==
    JSON.stringify(
      expectedRootEntries
        .map((name) => `directory:${name}`)
        .sort(compareCanonicalText),
    )
  ) {
    throw new Error("RELEASE_PLATFORM_SIGNING_INPUT_INVALID");
  }

  const nativeRoot = join(inputRoot, "native-desktop");
  const receiptRoot = join(inputRoot, "platform-signing");
  const nativeEntries = (await readdir(nativeRoot, { withFileTypes: true }))
    .map(
      (entry) => `${entry.isDirectory() ? "directory" : "other"}:${entry.name}`,
    )
    .sort(compareCanonicalText);
  const receiptEntries = (await readdir(receiptRoot, { withFileTypes: true }))
    .map((entry) => `${entry.isFile() ? "file" : "other"}:${entry.name}`)
    .sort(compareCanonicalText);
  if (
    JSON.stringify(nativeEntries) !==
      JSON.stringify(
        targets
          .map((target) => `directory:${target}`)
          .sort(compareCanonicalText),
      ) ||
    JSON.stringify(receiptEntries) !==
      JSON.stringify(
        targets
          .map(
            (target) =>
              `file:${platformSigningReceiptFileName(target, version)}`,
          )
          .sort(compareCanonicalText),
      )
  ) {
    throw new Error("RELEASE_PLATFORM_SIGNING_INPUT_INVALID");
  }

  const receipts = [];
  for (const target of targets) {
    const stage = join(nativeRoot, target);
    await verifyNativeDesktopStage({ target, version, input: stage });
    receipts.push(
      await readPlatformSigningReceipt({
        path: join(
          receiptRoot,
          platformSigningReceiptFileName(target, version),
        ),
        sourceSha,
        stage,
        target,
        version,
      }),
    );
  }
  return receipts;
}

async function assertPlatformSigningMetadata({
  index,
  outputRoot,
  requirePlatformSigning,
  sourceSha,
  version,
}) {
  const signing = index.platformSigning;
  if (signing === undefined) {
    if (requirePlatformSigning) {
      throw new Error("RELEASE_PLATFORM_SIGNING_REQUIRED");
    }
    return;
  }
  if (
    !hasExactKeys(signing, ["schemaVersion", "sourceSha", "receipts"]) ||
    signing.schemaVersion !== PLATFORM_SIGNING_SCHEMA_VERSION ||
    signing.sourceSha !== sourceSha ||
    signing.sourceSha !== index.sourceSha ||
    !Array.isArray(signing.receipts)
  ) {
    throw new Error("RELEASE_PLATFORM_SIGNING_INDEX_INVALID");
  }
  const targets = [
    ...new Set(nativeDesktopArtifactPlan(version).map(({ target }) => target)),
  ];
  if (signing.receipts.length !== targets.length) {
    throw new Error("RELEASE_PLATFORM_SIGNING_INDEX_INVALID");
  }
  const byTarget = new Map(
    signing.receipts.map((receipt) => [receipt?.target, receipt]),
  );
  if (byTarget.size !== targets.length) {
    throw new Error("RELEASE_PLATFORM_SIGNING_INDEX_INVALID");
  }
  const nativeByName = new Map(
    index.nativeDesktop.map((artifact) => [artifact.fileName, artifact]),
  );
  for (const target of targets) {
    const metadata = byTarget.get(target);
    if (
      !hasExactKeys(metadata, [
        "target",
        "identityReference",
        "receiptFileName",
        "receiptSha256",
        "artifacts",
      ]) ||
      metadata.target !== target ||
      metadata.receiptFileName !==
        platformSigningReceiptFileName(target, version) ||
      !SHA256.test(metadata.receiptSha256) ||
      !Array.isArray(metadata.artifacts)
    ) {
      throw new Error("RELEASE_PLATFORM_SIGNING_INDEX_INVALID");
    }
    assertPlatformIdentityReference(metadata.identityReference);
    const receiptPath = join(outputRoot, metadata.receiptFileName);
    if ((await sha256File(receiptPath)) !== metadata.receiptSha256) {
      throw new Error("RELEASE_PLATFORM_SIGNING_RECEIPT_INVALID");
    }
    const receipt = await readPlatformSigningReceipt({
      path: receiptPath,
      sourceSha,
      stage: outputRoot,
      target,
      version,
    });
    if (
      receipt.identityReference !== metadata.identityReference ||
      metadata.artifacts.length !== receipt.artifacts.length
    ) {
      throw new Error("RELEASE_PLATFORM_SIGNING_INDEX_INVALID");
    }
    for (const [artifactIndex, artifact] of metadata.artifacts.entries()) {
      const recorded = receipt.artifacts[artifactIndex];
      const indexed = nativeByName.get(recorded.fileName);
      if (
        !hasExactKeys(artifact, [
          "fileName",
          "sha256",
          "sizeBytes",
          "trust",
          "unsignedSha256",
        ]) ||
        artifact.fileName !== recorded.fileName ||
        artifact.sha256 !== recorded.sha256 ||
        artifact.sizeBytes !== recorded.sizeBytes ||
        artifact.trust !== recorded.trust ||
        !SHA256.test(artifact.unsignedSha256) ||
        (artifact.trust !== PLATFORM_TRUST.provenance &&
          artifact.unsignedSha256 === artifact.sha256) ||
        indexed?.sha256 !== artifact.sha256 ||
        indexed?.sizeBytes !== artifact.sizeBytes
      ) {
        throw new Error("RELEASE_PLATFORM_SIGNING_INDEX_INVALID");
      }
    }
  }
}

async function readReleaseIndex(outputRoot) {
  let index;
  try {
    index = JSON.parse(
      await readFile(join(outputRoot, "release-index.json"), "utf8"),
    );
  } catch {
    throw new Error("RELEASE_INDEX_INVALID");
  }
  if (
    index?.schemaVersion !== 3 ||
    !Array.isArray(index.artifacts) ||
    !Array.isArray(index.nativeDesktop) ||
    (index.sourceSha !== undefined && !SOURCE_SHA.test(index.sourceSha)) ||
    !SEMVER.test(index.version)
  ) {
    throw new Error("RELEASE_INDEX_INVALID");
  }
  return index;
}

async function expectedChecksumEntries(outputRoot, index) {
  const sboms = await exactSbomNames(outputRoot, index);
  return Promise.all(
    [
      ...index.artifacts.map((artifact) => artifact.fileName),
      ...index.nativeDesktop.map((artifact) => artifact.fileName),
      ...(index.dockerImages ?? []).flatMap((image) => [
        image.fileName,
        image.metadataFileName,
      ]),
      ...(index.dockerCompose ? [index.dockerCompose.fileName] : []),
      ...platformSigningReceiptNames(index),
      ...sboms,
      "release-index.json",
    ]
      .sort()
      .map(async (fileName) => ({
        fileName,
        sha256: await sha256File(join(outputRoot, fileName)),
      })),
  );
}

async function exactSbomNames(outputRoot, index) {
  const expected = [
    `cmclient-${index.version}.spdx.json`,
    ...(index.dockerImages ?? []).map((image) => image.sbomFileName),
  ].sort(compareCanonicalText);
  if (
    expected.some((fileName) => !isSafeArtifactFileName(fileName)) ||
    new Set(expected).size !== expected.length
  ) {
    throw new Error("RELEASE_SBOM_SET_INVALID");
  }
  const actual = (await readdir(outputRoot, { withFileTypes: true }))
    .filter((entry) => entry.isFile() && entry.name.endsWith(".spdx.json"))
    .map((entry) => entry.name)
    .sort(compareCanonicalText);
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error("RELEASE_SBOM_SET_INVALID");
  }
  return actual;
}

async function assertExactReleaseOutputEntries(outputRoot, index) {
  const expected = new Set([
    ...index.artifacts.map((artifact) => artifact.fileName),
    ...index.nativeDesktop.map((artifact) => artifact.fileName),
    ...(index.dockerImages ?? []).flatMap((image) => [
      image.fileName,
      image.metadataFileName,
    ]),
    ...(index.dockerCompose ? [index.dockerCompose.fileName] : []),
    ...platformSigningReceiptNames(index),
    ...(await exactSbomNames(outputRoot, index)),
    "release-index.json",
    "SHA256SUMS",
  ]);
  const optional = new Set(["SHA256SUMS.sigstore.json"]);
  const actual = await readdir(outputRoot, { withFileTypes: true });
  if (
    actual.some(
      (entry) =>
        !entry.isFile() ||
        (!expected.has(entry.name) && !optional.has(entry.name)),
    ) ||
    [...expected].some(
      (fileName) => !actual.some(({ name }) => name === fileName),
    )
  ) {
    throw new Error("RELEASE_OUTPUT_SET_INVALID");
  }
}

function parseChecksumFile(content) {
  const lines = content.split("\n").filter(Boolean);
  if (lines.length === 0) {
    throw new Error("RELEASE_CHECKSUM_FILE_INVALID");
  }
  return lines.map((line) => {
    const match = line.match(/^([a-f0-9]{64}) \*([^\s/]+)$/u);
    if (!match) {
      throw new Error("RELEASE_CHECKSUM_FILE_INVALID");
    }
    return { sha256: match[1], fileName: match[2] };
  });
}

function assertUpdateManifest(manifest) {
  if (
    manifest?.schemaVersion !== 1 ||
    !CHANNELS.has(manifest.channel) ||
    !SEMVER.test(manifest.version) ||
    !SEMVER.test(manifest.minimumAgentVersion) ||
    !UTC_MILLISECONDS.test(manifest.publishedAt) ||
    new Date(manifest.publishedAt).toISOString() !== manifest.publishedAt ||
    !Array.isArray(manifest.bundles) ||
    manifest.bundles.length === 0
  ) {
    throw new Error("RELEASE_MANIFEST_INVALID");
  }
  const expectedPlan = releaseArtifactPlan(manifest.version);
  if (manifest.bundles.length !== expectedPlan.length) {
    throw new Error("RELEASE_MANIFEST_INVALID");
  }
  const seen = new Set();
  for (const bundle of manifest.bundles) {
    let name;
    try {
      name = releaseArtifactName({
        component: bundle.component,
        target: bundle.target,
        version: manifest.version,
      });
    } catch {
      throw new Error("RELEASE_MANIFEST_INVALID");
    }
    if (
      archiveForTarget(bundle.target) !== bundle.archive ||
      !isHttpsUrl(bundle.url) ||
      !SHA256.test(bundle.sha256) ||
      !Number.isSafeInteger(bundle.sizeBytes) ||
      bundle.sizeBytes < 1 ||
      seen.has(name)
    ) {
      throw new Error("RELEASE_MANIFEST_INVALID");
    }
    seen.add(name);
  }
}

function privateKeyFromBase64(value) {
  if (typeof value !== "string" || !/^[A-Za-z0-9+/]+={0,2}$/.test(value)) {
    throw new Error("RELEASE_SIGNING_KEY_INVALID");
  }
  try {
    const privateKey = createPrivateKey({
      key: Buffer.from(value, "base64"),
      format: "der",
      type: "pkcs8",
    });
    if (privateKey.asymmetricKeyType !== "ed25519") {
      throw new Error("RELEASE_SIGNING_KEY_INVALID");
    }
    return privateKey;
  } catch {
    throw new Error("RELEASE_SIGNING_KEY_INVALID");
  }
}

function normalizeReleaseBaseUrl(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error("RELEASE_MANIFEST_BASE_URL_INVALID");
  }
  if (
    url.protocol !== "https:" ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error("RELEASE_MANIFEST_BASE_URL_INVALID");
  }
  return new URL(`${url.pathname.replace(/\/+$/u, "")}/`, url);
}

function isHttpsUrl(value) {
  try {
    const url = new URL(value);
    return (
      url.protocol === "https:" &&
      Boolean(url.hostname) &&
      !url.username &&
      !url.password
    );
  } catch {
    return false;
  }
}

function assertVersion(value) {
  if (!SEMVER.test(value)) {
    throw new Error("RELEASE_MANIFEST_VERSION_INVALID");
  }
}

function assertSourceSha(value) {
  if (!SOURCE_SHA.test(value)) {
    throw new Error("RELEASE_DOCKER_SOURCE_SHA_INVALID");
  }
}

function compareCanonicalText(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function isSafeArtifactFileName(value) {
  return (
    typeof value === "string" &&
    value === basename(value) &&
    !value.includes("\0")
  );
}

async function sha256File(path) {
  const hash = createHash("sha256");
  const stream = createReadStream(path);
  for await (const chunk of stream) {
    hash.update(chunk);
  }
  return hash.digest("hex");
}

async function readTarOutput(archivePath, argumentsList) {
  try {
    return await runCapture(
      "tar",
      tarArguments([argumentsList[0], archivePath, ...argumentsList.slice(1)]),
    );
  } catch {
    throw new Error("RELEASE_DOCKER_OCI_INVALID");
  }
}

async function runCapture(program, argumentsList) {
  return new Promise((resolvePromise, reject) => {
    const chunks = [];
    let length = 0;
    const child = spawn(program, argumentsList, {
      stdio: ["ignore", "pipe", "ignore"],
    });
    child.stdout.on("data", (chunk) => {
      length += chunk.length;
      if (length > 16 * 1024 * 1024) {
        child.kill();
        reject(new Error("RELEASE_ARCHIVE_OUTPUT_TOO_LARGE"));
        return;
      }
      chunks.push(chunk);
    });
    child.once("error", reject);
    child.once("exit", (code) => {
      if (code === 0) {
        resolvePromise(Buffer.concat(chunks));
      } else {
        reject(new Error("RELEASE_ARCHIVE_READ_FAILED"));
      }
    });
  });
}

async function run(program, argumentsList, cwd) {
  await new Promise((resolvePromise, reject) => {
    const child = spawn(program, argumentsList, { cwd, stdio: "inherit" });
    child.once("error", () =>
      reject(new Error("RELEASE_ARCHIVE_TOOL_UNAVAILABLE")),
    );
    child.once("exit", (code) => {
      if (code === 0) {
        resolvePromise();
      } else {
        reject(new Error("RELEASE_ARCHIVE_CREATION_FAILED"));
      }
    });
  });
}

async function writeJson(path, value) {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

function argumentValue(argumentsList, name) {
  const index = argumentsList.indexOf(name);
  if (index < 0 || index === argumentsList.length - 1) {
    throw new Error(`missing ${name}`);
  }
  return argumentsList[index + 1];
}

function optionalArgumentValue(argumentsList, name) {
  return argumentsList.includes(name)
    ? argumentValue(argumentsList, name)
    : undefined;
}

async function main(argumentsList) {
  const [command] = argumentsList;
  if (command === "stage-docker") {
    await stageDockerArtifact({
      input: argumentValue(argumentsList, "--input"),
      output: argumentValue(argumentsList, "--output"),
      sourceSha: argumentValue(argumentsList, "--source-sha"),
      target: argumentValue(argumentsList, "--target"),
      version: argumentValue(argumentsList, "--version"),
    });
    return;
  }
  if (command === "include-docker") {
    await includeDockerArtifact({
      compose: argumentValue(argumentsList, "--compose"),
      input: argumentValue(argumentsList, "--input"),
      output: argumentValue(argumentsList, "--output"),
      sourceSha: argumentValue(argumentsList, "--source-sha"),
      version: argumentValue(argumentsList, "--version"),
    });
    return;
  }
  if (command === "assemble") {
    await assembleReleaseArtifacts({
      input: argumentValue(argumentsList, "--input"),
      output: argumentValue(argumentsList, "--output"),
      version: argumentValue(argumentsList, "--version"),
    });
    return;
  }
  if (command === "finalize") {
    await finalizeChecksums({
      output: argumentValue(argumentsList, "--output"),
    });
    return;
  }
  if (command === "platform-receipt") {
    await createPlatformSigningReceipt({
      identityReference: argumentValue(argumentsList, "--identity-reference"),
      input: argumentValue(argumentsList, "--input"),
      output: argumentValue(argumentsList, "--output"),
      sourceSha: argumentValue(argumentsList, "--source-sha"),
      target: argumentValue(argumentsList, "--target"),
      version: argumentValue(argumentsList, "--version"),
    });
    return;
  }
  if (command === "finalize-platform-signed") {
    await finalizePlatformSignedRelease({
      input: argumentValue(argumentsList, "--input"),
      output: argumentValue(argumentsList, "--output"),
      sbom: argumentValue(argumentsList, "--sbom"),
      sourceSha: argumentValue(argumentsList, "--source-sha"),
      version: argumentValue(argumentsList, "--version"),
    });
    return;
  }
  if (command === "verify") {
    await verifyReleaseOutput({
      output: argumentValue(argumentsList, "--output"),
      requirePlatformSigning: argumentsList.includes(
        "--require-platform-signing",
      ),
      sourceSha: optionalArgumentValue(argumentsList, "--source-sha"),
      version: argumentValue(argumentsList, "--version"),
    });
    return;
  }
  if (command === "sign-update-manifest") {
    const privateKeyEnvironment = argumentValue(
      argumentsList,
      "--private-key-env",
    );
    if (!/^[A-Z][A-Z0-9_]{0,127}$/.test(privateKeyEnvironment)) {
      throw new Error("RELEASE_SIGNING_KEY_ENVIRONMENT_INVALID");
    }
    const indexPath = resolve(argumentValue(argumentsList, "--index"));
    const unverifiedIndex = await readReleaseIndex(dirname(indexPath));
    const index = await verifyReleaseOutput({
      output: dirname(indexPath),
      requirePlatformSigning: true,
      sourceSha: unverifiedIndex.sourceSha,
      version: unverifiedIndex.version,
    });
    const privateKeyBase64 = process.env[privateKeyEnvironment];
    if (!privateKeyBase64) {
      throw new Error("RELEASE_SIGNING_KEY_UNAVAILABLE");
    }
    const signed = createSignedUpdateManifest({
      artifacts: index.artifacts,
      channel: argumentValue(argumentsList, "--channel"),
      minimumAgentVersion: argumentValue(
        argumentsList,
        "--minimum-agent-version",
      ),
      privateKeyBase64,
      publishedAt: argumentValue(argumentsList, "--published-at"),
      releaseBaseUrl: argumentValue(argumentsList, "--release-base-url"),
      signingKeyId: argumentValue(argumentsList, "--signing-key-id"),
      version: index.version,
    });
    await writeJson(argumentValue(argumentsList, "--output"), signed);
    return;
  }
  throw new Error(
    "usage: release-supply-chain.mjs <assemble|stage-docker|include-docker|finalize|platform-receipt|finalize-platform-signed|verify|sign-update-manifest> ...",
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
