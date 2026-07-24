import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { createReadStream, readFileSync } from "node:fs";
import { lstat, mkdir, readdir, rename, rm } from "node:fs/promises";
import {
  basename,
  dirname,
  isAbsolute,
  join,
  relative,
  resolve,
  sep,
} from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  PrivateNodeRuntimeError,
  failPrivateNode as fail,
} from "./private-node-errors.mjs";
import {
  executableRelativeToStage,
  isSafeRelativePath,
  processWindowsNodeZip,
} from "./private-node-zip.mjs";

export { PrivateNodeRuntimeError } from "./private-node-errors.mjs";

export const PRIVATE_NODE_MANIFEST_RELATIVE_PATH =
  "packaging/runtime/private-node-runtime.json";

const REPOSITORY_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_MANIFEST_PATH = resolve(
  REPOSITORY_ROOT,
  ...PRIVATE_NODE_MANIFEST_RELATIVE_PATH.split("/"),
);
const EXPECTED_RUNTIME = Object.freeze({
  name: "node",
  version: "24.18.0",
  versionOutput: "v24.18.0",
  checksumDocument: Object.freeze({
    url: "https://nodejs.org/dist/v24.18.0/SHASUMS256.txt",
    sha256: "3927bab574a00ca0560c9583fe19655ba19603a1c5851414e4325d34ac50e469",
    sizeBytes: 2_967,
  }),
});
const EXPECTED_LIMITS = Object.freeze({
  maxArchiveBytes: 128 * 1_024 * 1_024,
  maxEntries: 32_768,
  maxEntryUncompressedBytes: 128 * 1_024 * 1_024,
  maxTotalUncompressedBytes: 512 * 1_024 * 1_024,
  maxPathBytes: 512,
});
const TARGET_CONTRACTS = Object.freeze({
  "windows-x86_64": [
    "zip",
    "win-x64",
    "runtime/node/node.exe",
    "current-host-contract",
  ],
  "darwin-x86_64": [
    "tar.xz",
    "darwin-x64",
    "runtime/node/bin/node",
    "P15-target-native",
  ],
  "darwin-aarch64": [
    "tar.xz",
    "darwin-arm64",
    "runtime/node/bin/node",
    "P15-target-native",
  ],
  "linux-x86_64": [
    "tar.xz",
    "linux-x64",
    "runtime/node/bin/node",
    "P15-target-native",
  ],
  "linux-aarch64": [
    "tar.xz",
    "linux-arm64",
    "runtime/node/bin/node",
    "P15-target-native",
  ],
});

const MANIFEST_KEYS = [
  "schemaVersion",
  "runtime",
  "limits",
  "targets",
  "exclusions",
];
const TARGET_KEYS = [
  "target",
  "archiveFormat",
  "archiveFileName",
  "archiveUrl",
  "archiveSha256",
  "archiveSizeBytes",
  "stageLayout",
  "qualification",
];
const LIMIT_KEYS = Object.keys(EXPECTED_LIMITS);
const MAX_PROCESS_OUTPUT_BYTES = 16 * 1_024;
const PROCESS_TIMEOUT_MILLISECONDS = 15_000;

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hasExactKeys(value, keys) {
  return (
    isObject(value) &&
    JSON.stringify(Object.keys(value).sort()) ===
      JSON.stringify([...keys].sort())
  );
}

function exactObject(value, expected) {
  return (
    hasExactKeys(value, Object.keys(expected)) &&
    Object.entries(expected).every(([key, expectedValue]) => {
      const actualValue = value[key];
      return isObject(expectedValue)
        ? exactObject(actualValue, expectedValue)
        : actualValue === expectedValue;
    })
  );
}

function isSha256(value) {
  return typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
}

export function validatePrivateNodeManifest(manifest) {
  if (!hasExactKeys(manifest, MANIFEST_KEYS) || manifest.schemaVersion !== 1) {
    fail("PRIVATE_NODE_MANIFEST_SCHEMA_INVALID");
  }
  if (!exactObject(manifest.runtime, EXPECTED_RUNTIME)) {
    fail("PRIVATE_NODE_RUNTIME_PIN_INVALID");
  }
  if (!exactObject(manifest.limits, EXPECTED_LIMITS)) {
    fail("PRIVATE_NODE_ARCHIVE_LIMITS_INVALID");
  }
  if (
    !Array.isArray(manifest.targets) ||
    manifest.targets.length !== Object.keys(TARGET_CONTRACTS).length
  ) {
    fail("PRIVATE_NODE_TARGET_SET_INVALID");
  }

  const targets = new Map();
  for (const target of manifest.targets) {
    if (
      !hasExactKeys(target, TARGET_KEYS) ||
      typeof target.target !== "string" ||
      targets.has(target.target) ||
      !isSha256(target.archiveSha256) ||
      !Number.isSafeInteger(target.archiveSizeBytes) ||
      target.archiveSizeBytes < 1 ||
      target.archiveSizeBytes > manifest.limits.maxArchiveBytes
    ) {
      fail("PRIVATE_NODE_TARGET_ENTRY_INVALID");
    }
    executableRelativeToStage(target, "PRIVATE_NODE_TARGET_ENTRY_INVALID");
    targets.set(target.target, target);
  }

  for (const [name, contract] of Object.entries(TARGET_CONTRACTS)) {
    const target = targets.get(name);
    const [archiveFormat, distributionName, executable, qualification] =
      contract;
    const archiveRoot = `node-v${EXPECTED_RUNTIME.version}-${distributionName}`;
    const archiveFileName = `${archiveRoot}.${archiveFormat}`;
    if (
      !target ||
      target.archiveFormat !== archiveFormat ||
      target.archiveFileName !== archiveFileName ||
      target.archiveUrl !==
        `https://nodejs.org/dist/v${EXPECTED_RUNTIME.version}/${archiveFileName}` ||
      target.qualification !== qualification ||
      !exactObject(target.stageLayout, {
        archiveRoot,
        stageRelativePath: "runtime/node",
        runtimeExecutableRelativePath: executable,
      })
    ) {
      fail("PRIVATE_NODE_TARGET_PIN_INVALID");
    }
  }
  if (
    !exactObject(manifest.exclusions, {
      windowsArm64: {
        supported: false,
        reason: "WINDOWS_ARM64_OUT_OF_SCOPE",
      },
      docker: {
        consumesNativeArchive: false,
        reason: "DOCKER_RUNTIME_IS_BUILT_IN_TARGET_IMAGE",
        qualification: "P16",
      },
    })
  ) {
    fail("PRIVATE_NODE_EXCLUSIONS_INVALID");
  }
  return manifest;
}

export function loadPrivateNodeManifest(path = DEFAULT_MANIFEST_PATH) {
  try {
    return validatePrivateNodeManifest(JSON.parse(readFileSync(path, "utf8")));
  } catch (error) {
    if (error instanceof PrivateNodeRuntimeError) throw error;
    fail("PRIVATE_NODE_MANIFEST_READ_FAILED");
  }
}

function validateArchiveLimits(limits) {
  return (
    hasExactKeys(limits, LIMIT_KEYS) &&
    Object.values(limits).every(
      (value) => Number.isSafeInteger(value) && value > 0,
    ) &&
    limits.maxEntries <= 0xffff
  );
}

function pathIsInside(root, candidate, allowRoot = false) {
  const path = relative(resolve(root), resolve(candidate));
  return (
    (allowRoot || path.length > 0) &&
    !isAbsolute(path) &&
    path !== ".." &&
    !path.startsWith(`..${sep}`)
  );
}

async function rejectLinkedExistingComponents(root, candidate) {
  const absoluteRoot = resolve(root);
  const absoluteCandidate = resolve(candidate);
  if (!pathIsInside(absoluteRoot, absoluteCandidate, true)) {
    fail("PRIVATE_NODE_CAMPAIGN_PATH_INVALID");
  }
  const components = relative(absoluteRoot, absoluteCandidate)
    .split(sep)
    .filter(Boolean);
  let current = absoluteRoot;
  for (const component of ["", ...components]) {
    if (component) current = join(current, component);
    try {
      const metadata = await lstat(current);
      if (metadata.isSymbolicLink()) {
        fail("PRIVATE_NODE_CAMPAIGN_LINK_REJECTED");
      }
    } catch (error) {
      if (error instanceof PrivateNodeRuntimeError) throw error;
      if (error?.code === "ENOENT") return;
      fail("PRIVATE_NODE_CAMPAIGN_PATH_INVALID");
    }
  }
}

async function sha256File(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

async function listRegularFiles(root, prefix = "") {
  const files = [];
  const entries = await readdir(root, { withFileTypes: true });
  entries.sort((left, right) => left.name.localeCompare(right.name, "en"));
  for (const entry of entries) {
    const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
    const path = join(root, entry.name);
    const metadata = await lstat(path);
    if (metadata.isSymbolicLink()) fail("PRIVATE_NODE_STAGE_LINK_REJECTED");
    if (metadata.isDirectory()) {
      files.push(...(await listRegularFiles(path, relativePath)));
    } else if (metadata.isFile()) {
      files.push(relativePath);
    } else {
      fail("PRIVATE_NODE_STAGE_SPECIAL_ENTRY_REJECTED");
    }
  }
  return files;
}

async function verifyExtractedInventory(root, inspection) {
  const expected = inspection.entries
    .filter((entry) => !entry.isDirectory && entry.relativePath)
    .map((entry) => entry.relativePath)
    .sort((left, right) => left.localeCompare(right, "en"));
  const actual = (await listRegularFiles(root)).sort((left, right) =>
    left.localeCompare(right, "en"),
  );
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    fail("PRIVATE_NODE_STAGE_INVENTORY_MISMATCH");
  }
}

function runProcess(command, arguments_, options = {}) {
  return new Promise((resolveProcess, rejectProcess) => {
    let settled = false;
    let stdout = Buffer.alloc(0);
    let stderr = Buffer.alloc(0);
    const child = spawn(command, arguments_, {
      cwd: options.cwd,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    const timer = setTimeout(() => {
      child.kill();
      if (!settled) {
        settled = true;
        rejectProcess(new Error("timeout"));
      }
    }, PROCESS_TIMEOUT_MILLISECONDS);
    const append = (current, chunk) => {
      const combined = Buffer.concat([current, chunk]);
      if (combined.length > MAX_PROCESS_OUTPUT_BYTES && !settled) {
        settled = true;
        clearTimeout(timer);
        child.kill();
        rejectProcess(new Error("output"));
      }
      return combined;
    };
    child.stdout.on("data", (chunk) => {
      stdout = append(stdout, chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr = append(stderr, chunk);
    });
    child.once("error", (error) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        rejectProcess(error);
      }
    });
    child.once("close", (status) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        resolveProcess({
          status,
          stdout: stdout.toString("utf8"),
          stderr: stderr.toString("utf8"),
        });
      }
    });
  });
}

export async function verifyPrivateNodeRuntime(
  executable,
  versionOutput,
  runner = runProcess,
) {
  let version;
  let sqlite;
  try {
    version = await runner(executable, ["--version"], {
      cwd: dirname(executable),
    });
    sqlite = await runner(
      executable,
      [
        "--input-type=module",
        "--eval",
        "import { DatabaseSync } from 'node:sqlite'; const db = new DatabaseSync(':memory:'); db.exec('CREATE TABLE smoke(value INTEGER); INSERT INTO smoke VALUES (1)'); if (db.prepare('SELECT value FROM smoke').get().value !== 1) process.exit(2); db.close(); process.stdout.write('NODE_SQLITE_OK');",
      ],
      { cwd: dirname(executable) },
    );
  } catch {
    fail("PRIVATE_NODE_RUNTIME_SMOKE_FAILED");
  }
  if (version.status !== 0 || version.stdout.trim() !== versionOutput) {
    fail("PRIVATE_NODE_RUNTIME_VERSION_INVALID");
  }
  if (sqlite.status !== 0 || sqlite.stdout !== "NODE_SQLITE_OK") {
    fail("PRIVATE_NODE_RUNTIME_SQLITE_INVALID");
  }
}

export async function inspectWindowsNodeZip(archivePath, target, limits) {
  if (!validateArchiveLimits(limits)) {
    fail("PRIVATE_NODE_WINDOWS_CONTRACT_INVALID");
  }
  executableRelativeToStage(target, "PRIVATE_NODE_WINDOWS_CONTRACT_INVALID");
  let metadata;
  try {
    metadata = await lstat(archivePath);
  } catch {
    fail("PRIVATE_NODE_ARCHIVE_READ_FAILED");
  }
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.size < 22 ||
    metadata.size > limits.maxArchiveBytes
  ) {
    fail("PRIVATE_NODE_ARCHIVE_SIZE_INVALID");
  }
  return processWindowsNodeZip({ archivePath, target, limits });
}

function validateWindowsArchiveContract(target, limits, versionOutput) {
  if (
    !isObject(target) ||
    target.target !== "windows-x86_64" ||
    target.archiveFormat !== "zip" ||
    !isSafeRelativePath(target.archiveFileName) ||
    target.archiveFileName.includes("/") ||
    !isSha256(target.archiveSha256) ||
    !Number.isSafeInteger(target.archiveSizeBytes) ||
    target.archiveSizeBytes < 22 ||
    !validateArchiveLimits(limits) ||
    target.archiveSizeBytes > limits.maxArchiveBytes ||
    typeof versionOutput !== "string" ||
    versionOutput.length === 0
  ) {
    fail("PRIVATE_NODE_WINDOWS_CONTRACT_INVALID");
  }
  return executableRelativeToStage(
    target,
    "PRIVATE_NODE_WINDOWS_CONTRACT_INVALID",
  );
}

async function pathExists(path) {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    fail("PRIVATE_NODE_CAMPAIGN_PATH_INVALID");
  }
}

export async function stageWindowsNodeArchive({
  target,
  limits,
  versionOutput,
  archivePath,
  campaignRoot,
  stageRoot,
  runtimeRunner = runProcess,
}) {
  const executableRelativePath = validateWindowsArchiveContract(
    target,
    limits,
    versionOutput,
  );
  const absoluteCampaignRoot = resolve(campaignRoot);
  const absoluteArchive = resolve(archivePath);
  const absoluteStageRoot = resolve(stageRoot);
  if (
    !pathIsInside(absoluteCampaignRoot, absoluteArchive) ||
    !pathIsInside(absoluteCampaignRoot, absoluteStageRoot, true) ||
    basename(absoluteArchive) !== target.archiveFileName
  ) {
    fail("PRIVATE_NODE_CAMPAIGN_PATH_INVALID");
  }
  await rejectLinkedExistingComponents(absoluteCampaignRoot, absoluteArchive);
  await rejectLinkedExistingComponents(absoluteCampaignRoot, absoluteStageRoot);

  let archiveMetadata;
  try {
    archiveMetadata = await lstat(absoluteArchive);
  } catch {
    fail("PRIVATE_NODE_ARCHIVE_READ_FAILED");
  }
  if (
    !archiveMetadata.isFile() ||
    archiveMetadata.isSymbolicLink() ||
    archiveMetadata.size !== target.archiveSizeBytes ||
    archiveMetadata.size > limits.maxArchiveBytes
  ) {
    fail("PRIVATE_NODE_ARCHIVE_SIZE_INVALID");
  }
  if ((await sha256File(absoluteArchive)) !== target.archiveSha256) {
    fail("PRIVATE_NODE_ARCHIVE_CHECKSUM_INVALID");
  }
  await mkdir(absoluteStageRoot, { recursive: true });
  await rejectLinkedExistingComponents(absoluteCampaignRoot, absoluteStageRoot);
  const finalRoot = resolve(
    absoluteStageRoot,
    ...target.stageLayout.stageRelativePath.split("/"),
  );
  if (
    !pathIsInside(absoluteStageRoot, finalRoot) ||
    (await pathExists(finalRoot))
  ) {
    fail("PRIVATE_NODE_STAGE_TARGET_CONFLICT");
  }
  await mkdir(dirname(finalRoot), { recursive: true });
  await rejectLinkedExistingComponents(
    absoluteCampaignRoot,
    dirname(finalRoot),
  );
  const temporaryRoot = join(
    dirname(finalRoot),
    `.private-node-${process.pid}-${randomBytes(8).toString("hex")}`,
  );
  try {
    await mkdir(temporaryRoot, { recursive: false });
    const inspection = await processWindowsNodeZip({
      archivePath: absoluteArchive,
      target,
      limits,
      destinationRoot: temporaryRoot,
    });
    const finalArchiveMetadata = await lstat(absoluteArchive).catch(
      () => undefined,
    );
    if (
      !finalArchiveMetadata?.isFile() ||
      finalArchiveMetadata.isSymbolicLink() ||
      finalArchiveMetadata.size !== archiveMetadata.size ||
      (await sha256File(absoluteArchive)) !== target.archiveSha256
    ) {
      fail("PRIVATE_NODE_ARCHIVE_CHANGED");
    }
    await verifyExtractedInventory(temporaryRoot, inspection);
    const executable = resolve(
      temporaryRoot,
      ...executableRelativePath.split("/"),
    );
    const metadata = await lstat(executable).catch(() => undefined);
    if (
      !pathIsInside(temporaryRoot, executable) ||
      !metadata?.isFile() ||
      metadata.isSymbolicLink()
    ) {
      fail("PRIVATE_NODE_RUNTIME_EXECUTABLE_MISSING");
    }
    await verifyPrivateNodeRuntime(executable, versionOutput, runtimeRunner);
    await rename(temporaryRoot, finalRoot);
  } catch (error) {
    await rm(temporaryRoot, { recursive: true, force: true }).catch(
      () => undefined,
    );
    throw error;
  }
  return Object.freeze({
    target: target.target,
    stageRoot: absoluteStageRoot,
    runtimeRoot: finalRoot,
    executable: resolve(
      absoluteStageRoot,
      ...target.stageLayout.runtimeExecutableRelativePath.split("/"),
    ),
    archiveSha256: target.archiveSha256,
  });
}

export async function stageWindowsPrivateNode(options) {
  validatePrivateNodeManifest(options.manifest);
  const target = options.manifest.targets.find(
    (candidate) => candidate.target === "windows-x86_64",
  );
  if (!target) fail("PRIVATE_NODE_WINDOWS_TARGET_MISSING");
  return stageWindowsNodeArchive({
    target,
    limits: options.manifest.limits,
    versionOutput: options.manifest.runtime.versionOutput,
    archivePath: options.archivePath,
    campaignRoot: options.campaignRoot,
    stageRoot: options.stageRoot,
    runtimeRunner: options.runtimeRunner,
  });
}

function parseArguments(arguments_) {
  const values = new Map();
  for (let index = 0; index < arguments_.length; index += 2) {
    const key = arguments_[index];
    const value = arguments_[index + 1];
    if (!key?.startsWith("--") || !value || values.has(key)) {
      fail("PRIVATE_NODE_USAGE_INVALID");
    }
    values.set(key, value);
  }
  return values;
}

export async function main(arguments_ = process.argv.slice(2)) {
  const [command, ...rest] = arguments_;
  if (command === "validate-manifest" && rest.length === 0) {
    loadPrivateNodeManifest();
    process.stdout.write("PRIVATE_NODE_MANIFEST_OK\n");
    return 0;
  }
  if (command !== "stage-windows") fail("PRIVATE_NODE_USAGE_INVALID");
  if (process.platform !== "win32" || process.arch !== "x64") {
    fail("PRIVATE_NODE_CURRENT_HOST_UNSUPPORTED");
  }
  const values = parseArguments(rest);
  if (
    values.size !== 3 ||
    !values.has("--archive") ||
    !values.has("--campaign-root") ||
    !values.has("--stage-root")
  ) {
    fail("PRIVATE_NODE_USAGE_INVALID");
  }
  const result = await stageWindowsPrivateNode({
    manifest: loadPrivateNodeManifest(),
    archivePath: values.get("--archive"),
    campaignRoot: values.get("--campaign-root"),
    stageRoot: values.get("--stage-root"),
  });
  process.stdout.write(
    `${JSON.stringify({
      schemaVersion: 1,
      target: result.target,
      stageRoot: result.stageRoot,
      runtimeRoot: result.runtimeRoot,
      executable: result.executable,
      archiveSha256: result.archiveSha256,
    })}\n`,
  );
  return 0;
}

const invokedPath = process.argv[1];
if (
  invokedPath &&
  import.meta.url === pathToFileURL(resolve(invokedPath)).href
) {
  main().catch((error) => {
    const code =
      error instanceof PrivateNodeRuntimeError
        ? error.code
        : "PRIVATE_NODE_INTERNAL_ERROR";
    process.stderr.write(`${code}\n`);
    process.exitCode = 1;
  });
}
