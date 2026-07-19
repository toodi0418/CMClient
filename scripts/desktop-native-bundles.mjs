import {
  chmod,
  copyFile,
  lstat,
  mkdir,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  nativeDesktopArtifactPlan,
  releaseComposition,
} from "./release-artifacts.mjs";

const BUNDLE_DIRECTORIES = Object.freeze({
  appimage: "appimage",
  deb: "deb",
  dmg: "dmg",
  msi: "msi",
  nsis: "nsis",
});

const BUNDLE_EXTENSIONS = Object.freeze({
  appimage: ".AppImage",
  deb: ".deb",
  dmg: ".dmg",
  msi: ".msi",
  nsis: ".exe",
});

export function nativeDesktopArtifactsForTarget(target, version) {
  const artifacts = nativeDesktopArtifactPlan(version).filter(
    (artifact) => artifact.target === target,
  );
  if (artifacts.length === 0) {
    throw new Error(`unsupported native Desktop target: ${target}`);
  }
  return artifacts;
}

// Windows Installer stores a SemVer prerelease as a numeric MSI field. Keep
// the canonical release version in artifact metadata while giving Tauri/WiX a
// bounded numeric prerelease it can encode.
export function tauriPackageVersion(target, version) {
  if (!target.startsWith("windows-")) {
    return version;
  }
  const match = version.match(
    /^(\d+\.\d+\.\d+)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/,
  );
  if (!match) {
    throw new Error(`invalid Windows Tauri version: ${version}`);
  }
  const [, core, prerelease] = match;
  if (!prerelease) {
    return version;
  }
  const numericPart = prerelease
    .split(".")
    .findLast((part) => /^\d+$/.test(part));
  if (numericPart === undefined) {
    throw new Error(
      `Windows Tauri prerelease must include a numeric identifier: ${version}`,
    );
  }
  const numeric = Number(numericPart);
  if (!Number.isSafeInteger(numeric) || numeric > 65_535) {
    throw new Error(
      `Windows Tauri prerelease identifier exceeds MSI limit: ${version}`,
    );
  }
  return `${core}-${numeric}`;
}

export function tauriReleaseConfig({ target, version, portable, icons }) {
  const artifacts = nativeDesktopArtifactsForTarget(target, version);
  const portableRoot = resolve(portable);
  const iconRoot = resolve(icons);
  return {
    version: tauriPackageVersion(target, version),
    bundle: {
      active: true,
      targets: artifacts.map(({ bundle }) => bundle),
      createUpdaterArtifacts: false,
      publisher: "CMClient",
      category: "Utility",
      shortDescription: "CMClient 2.0 local Agent supervisor",
      longDescription:
        "CMClient Desktop supervises the separately running local CMClient Agent control plane.",
      icon: [
        join(iconRoot, "32x32.png"),
        join(iconRoot, "128x128.png"),
        join(iconRoot, "128x128@2x.png"),
        join(iconRoot, "icon.icns"),
        join(iconRoot, "icon.ico"),
      ],
      resources: {
        [`${portableRoot}/`]: "cmclient-runtime/",
      },
      windows: {
        allowDowngrades: false,
        webviewInstallMode: {
          type: "downloadBootstrapper",
          silent: true,
        },
      },
    },
  };
}

export async function collectNativeDesktopBundles({
  target,
  version,
  bundleRoot,
  output,
}) {
  const artifacts = nativeDesktopArtifactsForTarget(target, version);
  const destination = join(output, "native-desktop", target);
  await rm(destination, { force: true, recursive: true });
  await mkdir(destination, { recursive: true });

  for (const artifact of artifacts) {
    const sourceDirectory = join(
      bundleRoot,
      BUNDLE_DIRECTORIES[artifact.bundle],
    );
    const candidates = await listFiles(sourceDirectory);
    const matches = candidates.filter((path) =>
      path.endsWith(BUNDLE_EXTENSIONS[artifact.bundle]),
    );
    if (matches.length !== 1) {
      throw new Error(
        `native Desktop bundle output invalid: ${artifact.bundle} expected one file, found ${matches.length}`,
      );
    }
    const destinationPath = join(destination, artifact.fileName);
    await copyFile(matches[0], destinationPath);
    await chmod(destinationPath, 0o644);
  }

  const manifest = {
    schemaVersion: 1,
    target,
    version,
    portableResource: "cmclient-runtime",
    agentLaunch: "external-service-required",
    artifacts,
  };
  await writeFile(
    join(destination, "native-desktop-manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  return { directory: destination, manifest };
}

export async function verifyNativeDesktopStage({ target, version, input }) {
  const expected = nativeDesktopArtifactsForTarget(target, version);
  const manifestPath = join(input, "native-desktop-manifest.json");
  let manifest;
  try {
    manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  } catch {
    throw new Error("native Desktop manifest invalid");
  }
  const canonical = {
    schemaVersion: 1,
    target,
    version,
    portableResource: "cmclient-runtime",
    agentLaunch: "external-service-required",
    artifacts: expected,
  };
  if (JSON.stringify(manifest) !== JSON.stringify(canonical)) {
    throw new Error("native Desktop manifest does not match canonical plan");
  }
  const actualFiles = (await listFiles(input))
    .map((path) => path.slice(resolve(input).length + 1))
    .sort(compareCanonicalText);
  const expectedFiles = [
    ...expected.map(({ fileName }) => fileName),
    "native-desktop-manifest.json",
  ].sort(compareCanonicalText);
  if (JSON.stringify(actualFiles) !== JSON.stringify(expectedFiles)) {
    throw new Error("native Desktop staged files do not match canonical plan");
  }
  for (const artifact of expected) {
    const metadata = await lstat(join(input, artifact.fileName));
    if (!metadata.isFile() || metadata.size === 0) {
      throw new Error(`native Desktop artifact invalid: ${artifact.fileName}`);
    }
  }
  return manifest;
}

export async function verifyBundledDesktopRuntime({ target, version, input }) {
  const root = resolve(input);
  const manifest = JSON.parse(
    await readFile(join(root, "build-manifest.json"), "utf8"),
  );
  const expectedContents = releaseComposition("desktop", target);
  if (
    manifest.schemaVersion !== 2 ||
    manifest.component !== "desktop" ||
    manifest.target !== target ||
    manifest.version !== version ||
    JSON.stringify(manifest.contents) !== JSON.stringify(expectedContents)
  ) {
    throw new Error("native Desktop runtime manifest invalid");
  }
  for (const content of expectedContents) {
    const metadata = await lstat(join(root, ...content.path.split("/")));
    if (
      (content.kind === "file" && !metadata.isFile()) ||
      (content.kind === "directory" && !metadata.isDirectory()) ||
      (content.kind === "file" &&
        content.executable &&
        !target.startsWith("windows-") &&
        (metadata.mode & 0o111) === 0)
    ) {
      throw new Error(
        `native Desktop runtime content invalid: ${content.role}`,
      );
    }
  }
  for (const required of [
    "gateway/dist/main.js",
    "gateway/package.json",
    "gateway/node_modules",
    "web/index.html",
    "proto/meshtastic/mesh.proto",
  ]) {
    await lstat(join(root, ...required.split("/")));
  }
  return manifest;
}

async function listFiles(directory) {
  const root = resolve(directory);
  const files = [];
  await walkFiles(root, files);
  return files.sort(compareCanonicalText);
}

async function walkFiles(directory, files) {
  let entries;
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch {
    throw new Error(`native Desktop bundle directory missing: ${directory}`);
  }
  entries.sort((left, right) => compareCanonicalText(left.name, right.name));
  for (const entry of entries) {
    const path = join(directory, entry.name);
    const metadata = await lstat(path);
    if (metadata.isSymbolicLink()) {
      throw new Error("native Desktop bundle output contains a symlink");
    }
    if (metadata.isDirectory()) {
      await walkFiles(path, files);
    } else if (metadata.isFile()) {
      files.push(path);
    } else {
      throw new Error("native Desktop bundle output contains a special file");
    }
  }
}

function compareCanonicalText(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
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
  if (command === "config") {
    const output = argumentValue(argumentsList, "--output");
    const config = tauriReleaseConfig({
      target: argumentValue(argumentsList, "--target"),
      version: argumentValue(argumentsList, "--version"),
      portable: argumentValue(argumentsList, "--portable"),
      icons: argumentValue(argumentsList, "--icons"),
    });
    await mkdir(dirname(output), { recursive: true });
    await writeFile(output, `${JSON.stringify(config, null, 2)}\n`);
    return;
  }
  if (command === "collect") {
    await collectNativeDesktopBundles({
      target: argumentValue(argumentsList, "--target"),
      version: argumentValue(argumentsList, "--version"),
      bundleRoot: argumentValue(argumentsList, "--bundle-root"),
      output: argumentValue(argumentsList, "--output"),
    });
    return;
  }
  if (command === "verify-stage") {
    await verifyNativeDesktopStage({
      target: argumentValue(argumentsList, "--target"),
      version: argumentValue(argumentsList, "--version"),
      input: argumentValue(argumentsList, "--input"),
    });
    return;
  }
  if (command === "verify-runtime") {
    await verifyBundledDesktopRuntime({
      target: argumentValue(argumentsList, "--target"),
      version: argumentValue(argumentsList, "--version"),
      input: argumentValue(argumentsList, "--input"),
    });
    return;
  }
  throw new Error(
    "usage: desktop-native-bundles.mjs <config|collect|verify-stage|verify-runtime> ...",
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
