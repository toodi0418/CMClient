import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import test from "node:test";
import { deflateRawSync } from "node:zlib";

import {
  PRIVATE_NODE_MANIFEST_RELATIVE_PATH,
  PrivateNodeRuntimeError,
  inspectWindowsNodeZip,
  loadPrivateNodeManifest,
  stageWindowsNodeArchive,
  validatePrivateNodeManifest,
  verifyPrivateNodeRuntime,
} from "./private-node-runtime.mjs";

const REPOSITORY_ROOT = resolve(import.meta.dirname, "..");
const MANIFEST_PATH = resolve(
  REPOSITORY_ROOT,
  ...PRIVATE_NODE_MANIFEST_RELATIVE_PATH.split("/"),
);
const ZIP_UTF8_FLAG = 1 << 11;
const FILE_MODE = 0o100644;
const DIRECTORY_MODE = 0o040755;
const SYMLINK_MODE = 0o120777;

function crc32(bytes) {
  let value = 0xffffffff;
  for (const byte of bytes) {
    value ^= byte;
    for (let bit = 0; bit < 8; bit += 1) {
      value = (value >>> 1) ^ (0xedb88320 & -(value & 1));
    }
  }
  return (value ^ 0xffffffff) >>> 0;
}

function buildZip(entries) {
  const localRecords = [];
  const centralRecords = [];
  let localOffset = 0;

  for (const entry of entries) {
    const name = Buffer.from(entry.name, "utf8");
    const localName = Buffer.from(entry.localName ?? entry.name, "utf8");
    const contents = Buffer.from(entry.contents ?? "");
    const method = entry.method ?? 0;
    const compressed = method === 8 ? deflateRawSync(contents) : contents;
    const checksum = crc32(contents);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(ZIP_UTF8_FLAG, 6);
    local.writeUInt16LE(method, 8);
    local.writeUInt32LE(checksum, 14);
    local.writeUInt32LE(compressed.length, 18);
    local.writeUInt32LE(contents.length, 22);
    local.writeUInt16LE(localName.length, 26);
    localRecords.push(local, localName, compressed);

    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE((3 << 8) | 20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt16LE(ZIP_UTF8_FLAG, 8);
    central.writeUInt16LE(method, 10);
    central.writeUInt32LE(checksum, 16);
    central.writeUInt32LE(entry.compressedSize ?? compressed.length, 20);
    central.writeUInt32LE(entry.uncompressedSize ?? contents.length, 24);
    central.writeUInt16LE(name.length, 28);
    const mode =
      entry.mode ?? (entry.name.endsWith("/") ? DIRECTORY_MODE : FILE_MODE);
    const windowsAttributes =
      entry.windowsAttributes ?? (entry.name.endsWith("/") ? 0x10 : 0x20);
    central.writeUInt32LE((((mode << 16) >>> 0) | windowsAttributes) >>> 0, 38);
    central.writeUInt32LE(localOffset, 42);
    centralRecords.push(central, name);
    localOffset += local.length + localName.length + compressed.length;
  }

  const centralOffset = localOffset;
  const centralDirectory = Buffer.concat(centralRecords);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(centralDirectory.length, 12);
  end.writeUInt32LE(centralOffset, 16);
  return Buffer.concat([...localRecords, centralDirectory, end]);
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function syntheticTarget(archive, overrides = {}) {
  return {
    target: "windows-x86_64",
    archiveFormat: "zip",
    archiveFileName: "node-fixture.zip",
    archiveSha256: sha256(archive),
    archiveSizeBytes: archive.length,
    stageLayout: {
      archiveRoot: "node-fixture",
      stageRelativePath: "runtime/node",
      runtimeExecutableRelativePath: "runtime/node/node.exe",
    },
    ...overrides,
  };
}

function fixtureZip(entries = []) {
  return buildZip([
    { name: "node-fixture/" },
    {
      name: "node-fixture/node.exe",
      contents: "synthetic executable",
      method: 8,
    },
    { name: "node-fixture/LICENSE", contents: "synthetic license" },
    ...entries,
  ]);
}

function expectCode(code) {
  return (error) => {
    assert.ok(error instanceof PrivateNodeRuntimeError);
    assert.equal(error.code, code);
    return true;
  };
}

async function makeCampaign(t) {
  const campaign = await mkdtemp(join(tmpdir(), "cmclient-private-node-"));
  t.after(() => rm(campaign, { recursive: true, force: true }));
  return campaign;
}

async function writeArchive(campaign, archive) {
  const archiveDirectory = join(campaign, "inputs");
  await mkdir(archiveDirectory, { recursive: true });
  const archivePath = join(archiveDirectory, "node-fixture.zip");
  await writeFile(archivePath, archive);
  return archivePath;
}

test("private Node manifest exact-pins official Node 24 targets", () => {
  const manifest = loadPrivateNodeManifest(MANIFEST_PATH);
  const packageJson = JSON.parse(
    readFileSync(resolve(REPOSITORY_ROOT, "package.json"), "utf8"),
  );
  assert.equal(packageJson.devDependencies.yauzl, "3.4.0");
  assert.equal(manifest.runtime.version, "24.18.0");
  assert.deepEqual(
    manifest.targets.map((target) => target.target),
    [
      "windows-x86_64",
      "darwin-x86_64",
      "darwin-aarch64",
      "linux-x86_64",
      "linux-aarch64",
    ],
  );
  assert.equal(
    manifest.runtime.checksumDocument.url,
    "https://nodejs.org/dist/v24.18.0/SHASUMS256.txt",
  );
  assert.deepEqual(manifest.runtime.checksumDocument, {
    url: "https://nodejs.org/dist/v24.18.0/SHASUMS256.txt",
    sha256: "3927bab574a00ca0560c9583fe19655ba19603a1c5851414e4325d34ac50e469",
    sizeBytes: 2967,
  });
  assert.deepEqual(
    manifest.targets.map((target) => ({
      target: target.target,
      archiveSizeBytes: target.archiveSizeBytes,
      archiveSha256: target.archiveSha256,
    })),
    [
      {
        target: "windows-x86_64",
        archiveSizeBytes: 37176245,
        archiveSha256:
          "0ae68406b42d7725661da979b1403ec9926da205c6770827f33aac9d8f26e821",
      },
      {
        target: "darwin-x86_64",
        archiveSizeBytes: 28702492,
        archiveSha256:
          "4a3b6bc81542154430825128d9a279e8b364e8d90581544e506ef7579fd1ab6f",
      },
      {
        target: "darwin-aarch64",
        archiveSizeBytes: 27052608,
        archiveSha256:
          "4477b9f78efb77744cf5eb57a0e9594dba66466b38b4e93fa9f35cb907a095a6",
      },
      {
        target: "linux-x86_64",
        archiveSizeBytes: 31511588,
        archiveSha256:
          "55aa7153f9d88f28d765fcdad5ae6945b5c0f98a36881703817e4c450fa76742",
      },
      {
        target: "linux-aarch64",
        archiveSizeBytes: 30473480,
        archiveSha256:
          "58c9520501f6ae2b52d5b210444e24b9d0c029a58c5011b797bc1fe7105886f6",
      },
    ],
  );
  assert.deepEqual(
    manifest.targets.map(
      (target) => target.stageLayout.runtimeExecutableRelativePath,
    ),
    [
      "runtime/node/node.exe",
      "runtime/node/bin/node",
      "runtime/node/bin/node",
      "runtime/node/bin/node",
      "runtime/node/bin/node",
    ],
  );
  assert.equal(manifest.exclusions.windowsArm64.supported, false);
  assert.equal(manifest.exclusions.docker.consumesNativeArchive, false);
});

test("manifest rejects duplicate targets and malformed SHA-256 pins", () => {
  const manifest = globalThis.structuredClone(
    loadPrivateNodeManifest(MANIFEST_PATH),
  );
  manifest.targets[1].target = manifest.targets[0].target;
  assert.throws(
    () => validatePrivateNodeManifest(manifest),
    expectCode("PRIVATE_NODE_TARGET_ENTRY_INVALID"),
  );

  const malformed = globalThis.structuredClone(
    loadPrivateNodeManifest(MANIFEST_PATH),
  );
  malformed.targets[0].archiveSha256 = "not-a-sha256";
  assert.throws(
    () => validatePrivateNodeManifest(malformed),
    expectCode("PRIVATE_NODE_TARGET_ENTRY_INVALID"),
  );
});

test("runtime verification requires exact version and node:sqlite smoke", async () => {
  const calls = [];
  await verifyPrivateNodeRuntime(
    "C:\\fixture\\node.exe",
    "v24.18.0",
    async (_executable, arguments_) => {
      calls.push(arguments_);
      return arguments_[0] === "--version"
        ? { status: 0, stdout: "v24.18.0\r\n", stderr: "" }
        : { status: 0, stdout: "NODE_SQLITE_OK", stderr: "" };
    },
  );
  assert.equal(calls.length, 2);
  assert.match(calls[1].join(" "), /node:sqlite/);

  await assert.rejects(
    verifyPrivateNodeRuntime(
      "C:\\fixture\\node.exe",
      "v24.18.0",
      async (_executable, arguments_) =>
        arguments_[0] === "--version"
          ? { status: 0, stdout: "v23.0.0", stderr: "" }
          : { status: 0, stdout: "NODE_SQLITE_OK", stderr: "" },
    ),
    expectCode("PRIVATE_NODE_RUNTIME_VERSION_INVALID"),
  );
});

test("ZIP inspection rejects traversal, collisions, links, and reparse entries", async (t) => {
  const manifest = loadPrivateNodeManifest(MANIFEST_PATH);
  const campaign = await makeCampaign(t);
  const cases = [
    {
      entry: { name: "node-fixture/../escape", contents: "x" },
      code: "PRIVATE_NODE_ZIP_INVALID",
    },
    {
      entry: { name: "node-fixture/NODE.EXE", contents: "x" },
      code: "PRIVATE_NODE_ZIP_DUPLICATE_ENTRY_REJECTED",
    },
    {
      entry: {
        name: "node-fixture/link",
        contents: "node.exe",
        mode: SYMLINK_MODE,
      },
      code: "PRIVATE_NODE_ZIP_LINK_OR_REPARSE_REJECTED",
    },
    {
      entry: {
        name: "node-fixture/reparse",
        contents: "x",
        windowsAttributes: 0x400,
      },
      code: "PRIVATE_NODE_ZIP_LINK_OR_REPARSE_REJECTED",
    },
    {
      entry: { name: "node-fixture/CON.txt", contents: "x" },
      code: "PRIVATE_NODE_ZIP_WINDOWS_PATH_INVALID",
    },
  ];

  for (const fixture of cases) {
    const archive = fixtureZip([fixture.entry]);
    const archivePath = await writeArchive(campaign, archive);
    await assert.rejects(
      inspectWindowsNodeZip(
        archivePath,
        syntheticTarget(archive),
        manifest.limits,
      ),
      expectCode(fixture.code),
    );
  }
});

test("ZIP inspection enforces entry and expansion bounds", async (t) => {
  const manifest = loadPrivateNodeManifest(MANIFEST_PATH);
  const campaign = await makeCampaign(t);
  const archive = fixtureZip([{ name: "node-fixture/extra", contents: "xx" }]);
  const archivePath = await writeArchive(campaign, archive);
  const target = syntheticTarget(archive);
  await assert.rejects(
    inspectWindowsNodeZip(archivePath, target, {
      ...manifest.limits,
      maxEntries: 1,
    }),
    expectCode("PRIVATE_NODE_ZIP_ENTRY_COUNT_INVALID"),
  );
  await assert.rejects(
    inspectWindowsNodeZip(archivePath, target, {
      ...manifest.limits,
      maxEntryUncompressedBytes: 1,
    }),
    expectCode("PRIVATE_NODE_ZIP_ENTRY_INVALID"),
  );
  await assert.rejects(
    inspectWindowsNodeZip(archivePath, target, {
      ...manifest.limits,
      maxTotalUncompressedBytes: 1,
    }),
    expectCode("PRIVATE_NODE_ZIP_TOTAL_SIZE_INVALID"),
  );
});

test("synthetic Windows archive stages only below the campaign", async (t) => {
  const manifest = loadPrivateNodeManifest(MANIFEST_PATH);
  const campaign = await makeCampaign(t);
  const archive = fixtureZip();
  const archivePath = await writeArchive(campaign, archive);
  const stageRoot = join(campaign, "package");
  const calls = [];
  const result = await stageWindowsNodeArchive({
    target: syntheticTarget(archive),
    limits: manifest.limits,
    versionOutput: "v24.18.0",
    archivePath,
    campaignRoot: campaign,
    stageRoot,
    runtimeRunner: async (executable, arguments_) => {
      calls.push({ executable, arguments_ });
      return arguments_[0] === "--version"
        ? { status: 0, stdout: "v24.18.0\n", stderr: "" }
        : { status: 0, stdout: "NODE_SQLITE_OK", stderr: "" };
    },
  });

  assert.equal(result.stageRoot, resolve(stageRoot));
  assert.equal(result.runtimeRoot, resolve(stageRoot, "runtime", "node"));
  assert.equal(
    result.executable,
    resolve(stageRoot, "runtime", "node", "node.exe"),
  );
  assert.equal(
    await readFile(result.executable, "utf8"),
    "synthetic executable",
  );
  assert.equal(calls.length, 2);
  assert.equal(basename(calls[0].executable), "node.exe");
  assert.match(calls[1].arguments_.join(" "), /node:sqlite/);
});

test("staged inventory handles a file beside a same-stem directory", async (t) => {
  const manifest = loadPrivateNodeManifest(MANIFEST_PATH);
  const campaign = await makeCampaign(t);
  const archive = fixtureZip([
    { name: "node-fixture/lib/cli.js", contents: "cli" },
    { name: "node-fixture/lib/cli/entry.js", contents: "entry" },
  ]);
  const archivePath = await writeArchive(campaign, archive);

  await stageWindowsNodeArchive({
    target: syntheticTarget(archive),
    limits: manifest.limits,
    versionOutput: "v24.18.0",
    archivePath,
    campaignRoot: campaign,
    stageRoot: join(campaign, "package"),
    runtimeRunner: async (_executable, arguments_) =>
      arguments_[0] === "--version"
        ? { status: 0, stdout: "v24.18.0\n", stderr: "" }
        : { status: 0, stdout: "NODE_SQLITE_OK", stderr: "" },
  });
});

test("staging rejects archives outside the campaign", async (t) => {
  const manifest = loadPrivateNodeManifest(MANIFEST_PATH);
  const campaign = await makeCampaign(t);
  const outside = await makeCampaign(t);
  const archive = fixtureZip();
  const archivePath = await writeArchive(outside, archive);
  await assert.rejects(
    stageWindowsNodeArchive({
      target: syntheticTarget(archive),
      limits: manifest.limits,
      versionOutput: "v24.18.0",
      archivePath,
      campaignRoot: campaign,
      stageRoot: join(campaign, "package"),
    }),
    expectCode("PRIVATE_NODE_CAMPAIGN_PATH_INVALID"),
  );
});

test("staging verifies exact size and SHA-256 before extraction", async (t) => {
  const manifest = loadPrivateNodeManifest(MANIFEST_PATH);
  const campaign = await makeCampaign(t);
  const archive = fixtureZip();
  const archivePath = await writeArchive(campaign, archive);
  const common = {
    limits: manifest.limits,
    versionOutput: "v24.18.0",
    archivePath,
    campaignRoot: campaign,
    stageRoot: join(campaign, "package"),
  };
  await assert.rejects(
    stageWindowsNodeArchive({
      ...common,
      target: syntheticTarget(archive, {
        archiveSizeBytes: archive.length + 1,
      }),
    }),
    expectCode("PRIVATE_NODE_ARCHIVE_SIZE_INVALID"),
  );
  await assert.rejects(
    stageWindowsNodeArchive({
      ...common,
      target: syntheticTarget(archive, {
        archiveSha256: "0".repeat(64),
      }),
    }),
    expectCode("PRIVATE_NODE_ARCHIVE_CHECKSUM_INVALID"),
  );
});

test("failed runtime qualification leaves no staged runtime", async (t) => {
  const manifest = loadPrivateNodeManifest(MANIFEST_PATH);
  const campaign = await makeCampaign(t);
  const archive = fixtureZip();
  const archivePath = await writeArchive(campaign, archive);
  const stageRoot = join(campaign, "package");
  await assert.rejects(
    stageWindowsNodeArchive({
      target: syntheticTarget(archive),
      limits: manifest.limits,
      versionOutput: "v24.18.0",
      archivePath,
      campaignRoot: campaign,
      stageRoot,
      runtimeRunner: async (_executable, arguments_) =>
        arguments_[0] === "--version"
          ? { status: 0, stdout: "v23.0.0", stderr: "" }
          : { status: 0, stdout: "NODE_SQLITE_OK", stderr: "" },
    }),
    expectCode("PRIVATE_NODE_RUNTIME_VERSION_INVALID"),
  );
  await assert.rejects(
    readFile(join(stageRoot, "runtime", "node", "node.exe")),
  );
});

test("staging implementation has no archive downloader", async () => {
  const source = await readFile(
    resolve(REPOSITORY_ROOT, "scripts/private-node-runtime.mjs"),
    "utf8",
  );
  const zipSource = await readFile(
    resolve(REPOSITORY_ROOT, "scripts/private-node-zip.mjs"),
    "utf8",
  );
  assert.doesNotMatch(source, /\bfetch\s*\(/);
  assert.doesNotMatch(source, /https\.request\s*\(/);
  assert.match(zipSource, /yauzl\.openPromise\(/);
  assert.doesNotMatch(zipSource, /fromBufferPromise\(/);
});
