import { createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";

import yauzl from "yauzl";

import {
  PrivateNodeRuntimeError,
  failPrivateNode,
} from "./private-node-errors.mjs";

const UNIX_FILE_TYPE_MASK = 0xf000;
const UNIX_REGULAR_FILE = 0x8000;
const UNIX_DIRECTORY = 0x4000;
const UNIX_SYMBOLIC_LINK = 0xa000;
const WINDOWS_DIRECTORY_ATTRIBUTE = 0x10;
const WINDOWS_REPARSE_POINT_ATTRIBUTE = 0x400;

export function isSafeRelativePath(value) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.includes("\\") ||
    value.includes("\0") ||
    value.startsWith("/") ||
    /^[a-zA-Z]:/.test(value)
  ) {
    return false;
  }
  return value
    .split("/")
    .every(
      (component) =>
        component.length > 0 &&
        component !== "." &&
        component !== ".." &&
        ![...component].some((character) => character < " "),
    );
}

export function executableRelativeToStage(target, errorCode) {
  const layout = target?.stageLayout;
  if (
    !layout ||
    JSON.stringify(Object.keys(layout).sort()) !==
      JSON.stringify(
        [
          "archiveRoot",
          "stageRelativePath",
          "runtimeExecutableRelativePath",
        ].sort(),
      ) ||
    !isSafeRelativePath(layout.archiveRoot) ||
    layout.archiveRoot.includes("/") ||
    !isSafeRelativePath(layout.stageRelativePath) ||
    !isSafeRelativePath(layout.runtimeExecutableRelativePath)
  ) {
    failPrivateNode(errorCode);
  }
  const prefix = `${layout.stageRelativePath}/`;
  if (!layout.runtimeExecutableRelativePath.startsWith(prefix)) {
    failPrivateNode(errorCode);
  }
  const executable = layout.runtimeExecutableRelativePath.slice(prefix.length);
  if (!isSafeRelativePath(executable)) failPrivateNode(errorCode);
  return executable;
}

function pathIsInside(root, candidate) {
  const path = relative(resolve(root), resolve(candidate));
  return (
    path.length > 0 &&
    !isAbsolute(path) &&
    path !== ".." &&
    !path.startsWith(`..${sep}`)
  );
}

function windowsPathCollisionKey(name) {
  const path = name.endsWith("/") ? name.slice(0, -1) : name;
  for (const component of path.split("/")) {
    const stem = component.split(".", 1)[0].toUpperCase();
    if (
      /[<>:"|?*]/.test(component) ||
      component.endsWith(".") ||
      component.endsWith(" ") ||
      ["CON", "PRN", "AUX", "NUL"].includes(stem) ||
      /^(?:COM|LPT)[1-9]$/.test(stem)
    ) {
      failPrivateNode("PRIVATE_NODE_ZIP_WINDOWS_PATH_INVALID");
    }
  }
  return path.normalize("NFC").toUpperCase();
}

function inspectPath(entry, archiveRoot, limits) {
  const isDirectory = entry.fileName.endsWith("/");
  const path = isDirectory ? entry.fileName.slice(0, -1) : entry.fileName;
  if (
    entry.fileNameRaw.length > limits.maxPathBytes ||
    !isSafeRelativePath(path)
  ) {
    failPrivateNode("PRIVATE_NODE_ZIP_PATH_INVALID");
  }
  const components = path.split("/");
  if (components[0] !== archiveRoot) {
    failPrivateNode("PRIVATE_NODE_ZIP_ARCHIVE_ROOT_INVALID");
  }
  const relativePath = components.slice(1).join("/");
  if (!relativePath && !isDirectory) {
    failPrivateNode("PRIVATE_NODE_ZIP_ARCHIVE_ROOT_INVALID");
  }
  return { isDirectory, relativePath };
}

function inspectType(entry, isDirectory) {
  const unixType = (entry.externalFileAttributes >>> 16) & UNIX_FILE_TYPE_MASK;
  const windowsAttributes = entry.externalFileAttributes & 0xffff;
  if (
    unixType === UNIX_SYMBOLIC_LINK ||
    (windowsAttributes & WINDOWS_REPARSE_POINT_ATTRIBUTE) !== 0
  ) {
    failPrivateNode("PRIVATE_NODE_ZIP_LINK_OR_REPARSE_REJECTED");
  }
  if (
    unixType !== 0 &&
    unixType !== UNIX_REGULAR_FILE &&
    unixType !== UNIX_DIRECTORY
  ) {
    failPrivateNode("PRIVATE_NODE_ZIP_SPECIAL_ENTRY_REJECTED");
  }
  const attributesSayDirectory =
    unixType === UNIX_DIRECTORY ||
    (windowsAttributes & WINDOWS_DIRECTORY_ATTRIBUTE) !== 0;
  if (
    (isDirectory && unixType === UNIX_REGULAR_FILE) ||
    (!isDirectory && attributesSayDirectory)
  ) {
    failPrivateNode("PRIVATE_NODE_ZIP_ENTRY_TYPE_INVALID");
  }
}

function rethrowZipError(error) {
  if (error instanceof PrivateNodeRuntimeError) throw error;
  failPrivateNode("PRIVATE_NODE_ZIP_INVALID");
}

async function extractEntry(zipFile, entry, destination, limits) {
  const source = await zipFile.openReadStreamPromise(entry);
  let written = 0;
  const limiter = new Transform({
    transform(chunk, _encoding, callback) {
      written += chunk.length;
      if (
        written > entry.uncompressedSize ||
        written > limits.maxEntryUncompressedBytes
      ) {
        callback(
          new PrivateNodeRuntimeError(
            "PRIVATE_NODE_ZIP_EXPANSION_LIMIT_EXCEEDED",
          ),
        );
        return;
      }
      callback(null, chunk);
    },
  });
  await pipeline(
    source,
    limiter,
    createWriteStream(destination, { flags: "wx", mode: 0o644 }),
  );
  if (written !== entry.uncompressedSize) {
    failPrivateNode("PRIVATE_NODE_ZIP_CONTENT_INVALID");
  }
}

export async function processWindowsNodeZip({
  archivePath,
  target,
  limits,
  destinationRoot,
}) {
  let zipFile;
  try {
    zipFile = await yauzl.openPromise(archivePath, {
      decodeStrings: true,
      strictFileNames: true,
      validateEntrySizes: true,
    });
    if (zipFile.entryCount < 1 || zipFile.entryCount > limits.maxEntries) {
      failPrivateNode("PRIVATE_NODE_ZIP_ENTRY_COUNT_INVALID");
    }

    const entries = [];
    const names = new Map();
    const parents = new Set();
    let totalUncompressedBytes = 0;
    for await (const entry of zipFile.eachEntry()) {
      if (
        entry.isEncrypted() ||
        !entry.canDecodeFileData() ||
        ![0, 8].includes(entry.compressionMethod) ||
        !Number.isSafeInteger(entry.compressedSize) ||
        !Number.isSafeInteger(entry.uncompressedSize) ||
        entry.compressedSize < 0 ||
        entry.uncompressedSize < 0 ||
        entry.uncompressedSize > limits.maxEntryUncompressedBytes
      ) {
        failPrivateNode("PRIVATE_NODE_ZIP_ENTRY_INVALID");
      }
      const path = inspectPath(entry, target.stageLayout.archiveRoot, limits);
      inspectType(entry, path.isDirectory);
      if (
        path.isDirectory &&
        (entry.compressedSize !== 0 || entry.uncompressedSize !== 0)
      ) {
        failPrivateNode("PRIVATE_NODE_ZIP_ENTRY_TYPE_INVALID");
      }
      const collisionKey = windowsPathCollisionKey(entry.fileName);
      if (names.has(collisionKey)) {
        failPrivateNode("PRIVATE_NODE_ZIP_DUPLICATE_ENTRY_REJECTED");
      }
      const components = collisionKey.split("/");
      for (let index = 1; index < components.length; index += 1) {
        const parent = components.slice(0, index).join("/");
        parents.add(parent);
        if (names.has(parent) && names.get(parent) === false) {
          failPrivateNode("PRIVATE_NODE_ZIP_ENTRY_TYPE_INVALID");
        }
      }
      if (!path.isDirectory && parents.has(collisionKey)) {
        failPrivateNode("PRIVATE_NODE_ZIP_ENTRY_TYPE_INVALID");
      }
      names.set(collisionKey, path.isDirectory);
      totalUncompressedBytes += entry.uncompressedSize;
      if (
        !Number.isSafeInteger(totalUncompressedBytes) ||
        totalUncompressedBytes > limits.maxTotalUncompressedBytes
      ) {
        failPrivateNode("PRIVATE_NODE_ZIP_TOTAL_SIZE_INVALID");
      }

      entries.push({
        relativePath: path.relativePath,
        isDirectory: path.isDirectory,
      });
      if (!destinationRoot || !path.relativePath) continue;
      const destination = resolve(
        destinationRoot,
        ...path.relativePath.split("/"),
      );
      if (!pathIsInside(destinationRoot, destination)) {
        failPrivateNode("PRIVATE_NODE_ZIP_PATH_INVALID");
      }
      if (path.isDirectory) {
        await mkdir(destination, { recursive: true });
      } else {
        await mkdir(dirname(destination), { recursive: true });
        await extractEntry(zipFile, entry, destination, limits);
      }
    }
    if (entries.length !== zipFile.entryCount) {
      failPrivateNode("PRIVATE_NODE_ZIP_ENTRY_COUNT_INVALID");
    }
    const executable = executableRelativeToStage(
      target,
      "PRIVATE_NODE_WINDOWS_CONTRACT_INVALID",
    )
      .normalize("NFC")
      .toUpperCase();
    if (
      !entries.some(
        (entry) =>
          !entry.isDirectory &&
          entry.relativePath.normalize("NFC").toUpperCase() === executable,
      )
    ) {
      failPrivateNode("PRIVATE_NODE_RUNTIME_EXECUTABLE_MISSING");
    }
    return Object.freeze({
      entries: Object.freeze(entries),
      totalUncompressedBytes,
    });
  } catch (error) {
    rethrowZipError(error);
  } finally {
    zipFile?.close();
  }
}
