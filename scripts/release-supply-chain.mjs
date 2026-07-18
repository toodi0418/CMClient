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

import {
  archiveForTarget,
  releaseArtifactName,
  releaseArtifactPlan,
  releaseComposition,
} from "./release-artifacts.mjs";

const CHANNELS = new Set(["stable", "beta", "dev"]);
const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;
const SHA256 = /^[a-f0-9]{64}$/;
const SIGNING_KEY_ID = /^[A-Za-z0-9._-]{1,128}$/;
const UTC_MILLISECONDS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const EPOCH = new Date(0);

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

      const packageRoot = join(temporaryRoot, `${planned.component}-${planned.target}`);
      const metadataDirectory = join(packageRoot, "metadata");
      await mkdir(metadataDirectory, { recursive: true });

      for (const content of buildManifest.contents) {
        const source = manifestPath(buildDirectory, content.path);
        const destination = manifestPath(packageRoot, content.path);
        await copyReleaseContent(source, destination, content);
      }
      const stagedManifest = join(metadataDirectory, "build-manifest.json");
      await writeFile(stagedManifest, `${JSON.stringify(buildManifest, null, 2)}\n`);
      await chmod(stagedManifest, 0o644);
      await utimes(stagedManifest, EPOCH, EPOCH);
      await normalizeReleaseDirectories(packageRoot);

      const archivePath = join(outputRoot, planned.fileName);
      await rm(archivePath, { force: true });
      await createArchive({
        archive: planned.archive,
        archivePath,
        entries: [...buildManifest.contents.map(({ path }) => path), "metadata/build-manifest.json"],
        cwd: packageRoot,
      });
      const metadata = await stat(archivePath);
      artifacts.push({
        ...planned,
        sha256: await sha256File(archivePath),
        sizeBytes: metadata.size,
      });
    }

    const index = { schemaVersion: 1, version, artifacts };
    await writeJson(join(outputRoot, "release-index.json"), index);
    return index;
  } finally {
    await rm(temporaryRoot, { force: true, recursive: true });
  }
}

export async function finalizeChecksums({ output }) {
  const outputRoot = resolve(output);
  const index = await readReleaseIndex(outputRoot);
  const sbomNames = (await readdir(outputRoot, { withFileTypes: true }))
    .filter((entry) => entry.isFile() && entry.name.endsWith(".spdx.json"))
    .map((entry) => entry.name)
    .sort();
  if (sbomNames.length === 0) {
    throw new Error("RELEASE_SBOM_MISSING");
  }
  const names = [...index.artifacts.map((artifact) => artifact.fileName), ...sbomNames].sort();
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

export async function verifyReleaseOutput({ output, version }) {
  const outputRoot = resolve(output);
  const index = await readReleaseIndex(outputRoot);
  if (index.version !== version) {
    throw new Error("RELEASE_INDEX_VERSION_INVALID");
  }
  const plan = releaseArtifactPlan(version);
  if (index.artifacts.length !== plan.length) {
    throw new Error("RELEASE_INDEX_ARTIFACTS_INVALID");
  }
  const expectedByName = new Map(plan.map((artifact) => [artifact.fileName, artifact]));
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
    if (metadata.size !== artifact.sizeBytes || (await sha256File(path)) !== artifact.sha256) {
      throw new Error("RELEASE_ARCHIVE_DIGEST_INVALID");
    }
  }

  const expectedChecksums = await expectedChecksumEntries(outputRoot, index);
  const checksumPath = join(outputRoot, "SHA256SUMS");
  const actualChecksums = parseChecksumFile(await readFile(checksumPath, "utf8"));
  if (checksumFileContents(actualChecksums) !== checksumFileContents(expectedChecksums)) {
    throw new Error("RELEASE_CHECKSUM_FILE_INVALID");
  }
  return index;
}

export function checksumFileContents(entries) {
  const names = new Set();
  const normalized = entries
    .map((entry) => {
      if (!isSafeArtifactFileName(entry.fileName) || !SHA256.test(entry.sha256)) {
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
  if (!UTC_MILLISECONDS.test(publishedAt) || new Date(publishedAt).toISOString() !== publishedAt) {
    throw new Error("RELEASE_MANIFEST_PUBLISHED_AT_INVALID");
  }
  if (!SIGNING_KEY_ID.test(signingKeyId)) {
    throw new Error("RELEASE_MANIFEST_SIGNING_KEY_ID_INVALID");
  }
  const baseUrl = normalizeReleaseBaseUrl(releaseBaseUrl);
  const expectedPlan = releaseArtifactPlan(version);
  const artifactsByName = new Map(artifacts.map((artifact) => [artifact.fileName, artifact]));
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
      await run("tar", ["-cf", tarPath, ...entries], cwd);
      await run("zstd", ["--quiet", "--force", "-19", "-o", archivePath, tarPath], cwd);
    } finally {
      await rm(tarPath, { force: true });
    }
    return;
  }
  await run(
    "tar",
    [
      "--zstd",
      "--sort=name",
      "--mtime=@0",
      "--owner=0",
      "--group=0",
      "--numeric-owner",
      "-cf",
      archivePath,
      ...entries,
    ],
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
    if (metadata.isSymbolicLink() || (!metadata.isFile() && !metadata.isDirectory())) {
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
    if (metadata.isSymbolicLink() || (!metadata.isFile() && !metadata.isDirectory())) {
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
  const expectedContents = releaseComposition(planned.component, planned.target);
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
  if (JSON.stringify(files) !== JSON.stringify(sorted) || new Set(files).size !== files.length) {
    return false;
  }
  const validPath = (path) =>
    typeof path === "string" &&
    path.length > 0 &&
    !path.includes("\\") &&
    !path.includes("\0") &&
    path.split("/").every((segment) => segment && segment !== "." && segment !== "..");
  if (!files.every(validPath)) {
    return false;
  }
  return contents.every((content) =>
    content.kind === "file"
      ? files.includes(content.path)
      : files.some((path) => path.startsWith(`${content.path}/`)),
  ) && files.every((path) =>
    contents.some((content) =>
      content.kind === "file" ? path === content.path : path.startsWith(`${content.path}/`),
    ),
  );
}

async function readReleaseIndex(outputRoot) {
  let index;
  try {
    index = JSON.parse(await readFile(join(outputRoot, "release-index.json"), "utf8"));
  } catch {
    throw new Error("RELEASE_INDEX_INVALID");
  }
  if (index?.schemaVersion !== 1 || !Array.isArray(index.artifacts) || !SEMVER.test(index.version)) {
    throw new Error("RELEASE_INDEX_INVALID");
  }
  return index;
}

async function expectedChecksumEntries(outputRoot, index) {
  const sboms = (await readdir(outputRoot, { withFileTypes: true }))
    .filter((entry) => entry.isFile() && entry.name.endsWith(".spdx.json"))
    .map((entry) => entry.name)
    .sort();
  if (sboms.length === 0) {
    throw new Error("RELEASE_SBOM_MISSING");
  }
  return Promise.all(
    [...index.artifacts.map((artifact) => artifact.fileName), ...sboms]
      .sort()
      .map(async (fileName) => ({
        fileName,
        sha256: await sha256File(join(outputRoot, fileName)),
      })),
  );
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
    return url.protocol === "https:" && Boolean(url.hostname) && !url.username && !url.password;
  } catch {
    return false;
  }
}

function assertVersion(value) {
  if (!SEMVER.test(value)) {
    throw new Error("RELEASE_MANIFEST_VERSION_INVALID");
  }
}

function compareCanonicalText(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function isSafeArtifactFileName(value) {
  return typeof value === "string" && value === basename(value) && !value.includes("\0");
}

async function sha256File(path) {
  const hash = createHash("sha256");
  const stream = createReadStream(path);
  for await (const chunk of stream) {
    hash.update(chunk);
  }
  return hash.digest("hex");
}

async function run(program, argumentsList, cwd) {
  await new Promise((resolvePromise, reject) => {
    const child = spawn(program, argumentsList, { cwd, stdio: "inherit" });
    child.once("error", () => reject(new Error("RELEASE_ARCHIVE_TOOL_UNAVAILABLE")));
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

async function main(argumentsList) {
  const [command] = argumentsList;
  if (command === "assemble") {
    await assembleReleaseArtifacts({
      input: argumentValue(argumentsList, "--input"),
      output: argumentValue(argumentsList, "--output"),
      version: argumentValue(argumentsList, "--version"),
    });
    return;
  }
  if (command === "finalize") {
    await finalizeChecksums({ output: argumentValue(argumentsList, "--output") });
    return;
  }
  if (command === "verify") {
    await verifyReleaseOutput({
      output: argumentValue(argumentsList, "--output"),
      version: argumentValue(argumentsList, "--version"),
    });
    return;
  }
  if (command === "sign-update-manifest") {
    const privateKeyEnvironment = argumentValue(argumentsList, "--private-key-env");
    if (!/^[A-Z][A-Z0-9_]{0,127}$/.test(privateKeyEnvironment)) {
      throw new Error("RELEASE_SIGNING_KEY_ENVIRONMENT_INVALID");
    }
    const indexPath = resolve(argumentValue(argumentsList, "--index"));
    const index = await readReleaseIndex(dirname(indexPath));
    const privateKeyBase64 = process.env[privateKeyEnvironment];
    if (!privateKeyBase64) {
      throw new Error("RELEASE_SIGNING_KEY_UNAVAILABLE");
    }
    const signed = createSignedUpdateManifest({
      artifacts: index.artifacts,
      channel: argumentValue(argumentsList, "--channel"),
      minimumAgentVersion: argumentValue(argumentsList, "--minimum-agent-version"),
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
    "usage: release-supply-chain.mjs <assemble|finalize|verify|sign-update-manifest> ...",
  );
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
