import {
  access,
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

export const RELEASE_COMPONENTS = ["desktop", "headless", "cli", "service"];
export const RELEASE_TARGETS = [
  "darwin-aarch64",
  "darwin-x86_64",
  "linux-aarch64",
  "linux-x86_64",
  "windows-x86_64",
];

export const NATIVE_DESKTOP_BUNDLES = Object.freeze({
  "darwin-aarch64": Object.freeze(["dmg"]),
  "darwin-x86_64": Object.freeze(["dmg"]),
  "linux-aarch64": Object.freeze(["deb", "appimage"]),
  "linux-x86_64": Object.freeze(["deb", "appimage"]),
  "windows-x86_64": Object.freeze(["msi", "nsis"]),
});

export const DOCKER_COMPOSITION = Object.freeze({
  kind: "oci-image",
  updaterManaged: false,
  services: ["gateway", "web", "ingress"],
  excluded: ["agent", "cli", "desktop", "serviceHost"],
});

export const DOCKER_PLATFORMS = Object.freeze([
  Object.freeze({
    target: "linux-x86_64",
    platform: "linux/amd64",
    architecture: "amd64",
  }),
  Object.freeze({
    target: "linux-aarch64",
    platform: "linux/arm64",
    architecture: "arm64",
  }),
]);

const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

const SERIALPORT_PREBUILD_ROOT =
  "node_modules/@serialport/bindings-cpp/prebuilds";
const SERIALPORT_PREBUILD_TARGETS = Object.freeze({
  "darwin-aarch64": Object.freeze({
    directory: "darwin-x64+arm64",
    suffix: ".node",
  }),
  "darwin-x86_64": Object.freeze({
    directory: "darwin-x64+arm64",
    suffix: ".node",
  }),
  "linux-aarch64": Object.freeze({
    directory: "linux-arm64",
    suffix: ".glibc.node",
  }),
  "linux-x86_64": Object.freeze({
    directory: "linux-x64",
    suffix: ".glibc.node",
  }),
  "windows-x86_64": Object.freeze({
    directory: "win32-x64",
    suffix: ".node",
  }),
});

export function archiveForTarget(target) {
  assertTarget(target);
  return target.startsWith("windows-") ? "zip" : "tar.zst";
}

export function releaseArtifactName({ component, target, version }) {
  assertComponent(component);
  assertTarget(target);
  if (!targetsForComponent(component).includes(target)) {
    throw new Error(`unsupported component target: ${component} ${target}`);
  }
  assertVersion(version);
  return `cmclient-${component}-${target}-${version}.${archiveForTarget(target)}`;
}

export function releaseArtifactPlan(version) {
  assertVersion(version);
  return RELEASE_COMPONENTS.flatMap((component) =>
    targetsForComponent(component).map((target) => ({
      component,
      target,
      archive: archiveForTarget(target),
      fileName: releaseArtifactName({ component, target, version }),
    })),
  );
}

export function nativeDesktopArtifactName({ target, bundle, version }) {
  assertTarget(target);
  assertNativeDesktopBundle(target, bundle);
  assertVersion(version);
  const suffix =
    bundle === "appimage"
      ? "AppImage"
      : bundle === "nsis"
        ? "setup.exe"
        : bundle;
  return `cmclient-desktop-${target}-${version}.${suffix}`;
}

export function nativeDesktopArtifactPlan(version) {
  assertVersion(version);
  return RELEASE_TARGETS.flatMap((target) =>
    NATIVE_DESKTOP_BUNDLES[target].map((bundle) => ({
      component: "desktop",
      target,
      bundle,
      fileName: nativeDesktopArtifactName({ target, bundle, version }),
      updaterManaged: false,
    })),
  );
}

export function dockerArtifactPlan(version) {
  assertVersion(version);
  return DOCKER_PLATFORMS.map(({ target, platform, architecture }) => {
    const stem = `cmclient-docker-${target}-${version}`;
    return {
      component: "docker",
      kind: DOCKER_COMPOSITION.kind,
      version,
      target,
      platform,
      architecture,
      archive: "oci.tar",
      fileName: `${stem}.oci.tar`,
      metadataFileName: `${stem}.metadata.json`,
      sbomFileName: `${stem}.spdx.json`,
      updaterManaged: false,
    };
  });
}

export function dockerComposeArtifactPlan(version) {
  assertVersion(version);
  return {
    component: "docker",
    kind: "compose-descriptor",
    version,
    sourcePath: "docker-compose.yml",
    fileName: `cmclient-docker-compose-${version}.yml`,
    updaterManaged: false,
  };
}

export function releaseComposition(component, target) {
  assertComponent(component);
  assertTarget(target);
  if (!targetsForComponent(component).includes(target)) {
    throw new Error(`unsupported component target: ${component} ${target}`);
  }

  const headless = [
    executable("agent", binaryPath("cmclient-agent", target)),
    executable("cli", binaryPath("cmclient", target)),
    executable("migration", binaryPath("cmclient-migrate", target)),
    directory("gateway", "gateway"),
    directory("web", "web"),
    directory("proto", "proto"),
    ...platformServiceSupport(target),
  ];

  switch (component) {
    case "cli":
      return [executable("cli", binaryPath("cmclient", target))];
    case "headless":
      return headless;
    case "desktop":
      return [
        executable("desktop", binaryPath("cmclient-desktop", target)),
        ...headless,
      ];
    case "service":
      return [
        executable("serviceHost", binaryPath("cmclient-service-host", target)),
        ...headless,
        file("windowsServiceManager", "scripts/cmclient-windows-service.ps1"),
      ];
    default:
      throw new Error(`unknown release component: ${component}`);
  }
}

export function releasePlanDocument(version) {
  return {
    schemaVersion: 4,
    version,
    artifacts: releaseArtifactPlan(version).map((artifact) => ({
      ...artifact,
      contents: releaseComposition(artifact.component, artifact.target),
    })),
    nativeDesktop: nativeDesktopArtifactPlan(version),
    docker: {
      ...DOCKER_COMPOSITION,
      compose: dockerComposeArtifactPlan(version),
      artifacts: dockerArtifactPlan(version),
    },
  };
}

export async function stageBuild({
  component,
  target,
  version,
  inputs,
  output,
}) {
  const fileName = releaseArtifactName({ component, target, version });
  const contents = releaseComposition(component, target);
  const inputMap = normalizeInputs(inputs);
  assertExactInputs(inputMap, contents);

  const directory = join(output, component, target);
  await rm(directory, { force: true, recursive: true });
  await mkdir(directory, { recursive: true });

  for (const content of contents) {
    const source = inputMap.get(content.role);
    await copyCanonicalInput(
      source,
      join(directory, ...content.path.split("/")),
      content,
      target,
    );
  }
  const files = (await listReleaseFiles(directory)).sort(compareCanonicalText);

  const manifest = {
    schemaVersion: 2,
    component,
    target,
    version,
    releaseAsset: {
      archive: archiveForTarget(target),
      fileName,
    },
    contents,
    files,
  };
  await writeFile(
    join(directory, "build-manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  return { manifest, stagedDirectory: directory };
}

async function listReleaseFiles(directory, prefix = "") {
  const files = [];
  const entries = await readdir(directory, { withFileTypes: true });
  entries.sort((left, right) => compareCanonicalText(left.name, right.name));
  for (const entry of entries) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    const filesystemPath = join(directory, entry.name);
    const metadata = await lstat(filesystemPath);
    if (metadata.isDirectory()) {
      files.push(...(await listReleaseFiles(filesystemPath, path)));
    } else if (metadata.isFile()) {
      files.push(path);
    } else {
      throw new Error("release staged content contains an unsupported entry");
    }
  }
  return files;
}

function executable(role, path) {
  return Object.freeze({ role, path, kind: "file", executable: true });
}

function file(role, path) {
  return Object.freeze({ role, path, kind: "file", executable: false });
}

function directory(role, path) {
  return Object.freeze({ role, path, kind: "directory", executable: false });
}

function binaryPath(name, target) {
  return `bin/${name}${target.startsWith("windows-") ? ".exe" : ""}`;
}

function platformServiceSupport(target) {
  if (target.startsWith("linux-")) {
    return [
      executable("systemdManager", "scripts/cmclient-systemd.sh"),
      file("systemdUnit", "packaging/systemd/cmclient-agent.service.in"),
    ];
  }
  if (target.startsWith("darwin-")) {
    return [
      executable("launchdManager", "scripts/cmclient-launchd.sh"),
      file("launchdPlist", "packaging/launchd/io.cmclient.agent.plist.in"),
    ];
  }
  return [];
}

function normalizeInputs(inputs) {
  const entries =
    inputs instanceof Map
      ? [...inputs]
      : Array.isArray(inputs)
        ? inputs
        : Object.entries(inputs ?? {});
  const result = new Map();
  for (const entry of entries) {
    if (!Array.isArray(entry) || entry.length !== 2) {
      throw new Error("invalid release input entry");
    }
    const [role, path] = entry;
    if (typeof role !== "string" || role.length === 0) {
      throw new Error("invalid release input role");
    }
    if (result.has(role)) {
      throw new Error(`duplicate release input: ${role}`);
    }
    if (typeof path !== "string" || path.length === 0) {
      throw new Error(`invalid release input: ${role}`);
    }
    result.set(role, path);
  }
  return result;
}

function assertExactInputs(inputs, contents) {
  const expected = new Set(contents.map(({ role }) => role));
  for (const role of expected) {
    if (!inputs.has(role)) {
      throw new Error(`missing release input: ${role}`);
    }
  }
  for (const role of inputs.keys()) {
    if (!expected.has(role)) {
      throw new Error(`unexpected release input: ${role}`);
    }
  }
}

async function copyCanonicalInput(source, destination, content, target) {
  await access(source);
  const metadata = await lstat(source);
  if (content.kind === "file") {
    if (!metadata.isFile()) {
      throw new Error(`release input must be a file: ${content.role}`);
    }
    await mkdir(dirname(destination), { recursive: true });
    await copyFile(source, destination);
    await chmod(destination, content.executable ? 0o755 : 0o644);
    return;
  }
  if (!metadata.isDirectory()) {
    throw new Error(`release input must be a directory: ${content.role}`);
  }
  await assertProductionDirectory(source, content.role);
  if (content.role === "gateway") {
    await assertGatewayTargetNativePrebuild(source, target);
  }
  const copiedFiles = await copyDirectory(source, destination, {
    omitPackageBins: content.role === "gateway",
    insideNodeModules: false,
    gatewayTarget: content.role === "gateway" ? target : undefined,
    relativePath: "",
  });
  if (copiedFiles === 0) {
    throw new Error(`release input directory is empty: ${content.role}`);
  }
}

async function assertProductionDirectory(source, role) {
  const required =
    role === "gateway"
      ? ["dist/main.js", "package.json", "node_modules"]
      : role === "web"
        ? ["index.html"]
        : role === "proto"
          ? ["meshtastic/mesh.proto"]
          : [];
  for (const relativePath of required) {
    try {
      const metadata = await lstat(join(source, ...relativePath.split("/")));
      if (
        relativePath === "node_modules"
          ? !metadata.isDirectory()
          : !metadata.isFile()
      ) {
        throw new Error();
      }
    } catch {
      throw new Error(
        `release production input invalid: ${role} missing ${relativePath}`,
      );
    }
  }
  if (role === "gateway") {
    await assertGatewayProductionDependencies(source);
  }
}

async function assertGatewayProductionDependencies(source) {
  let packageJson;
  try {
    packageJson = JSON.parse(
      await readFile(join(source, "package.json"), "utf8"),
    );
  } catch {
    throw new Error("release production input invalid: gateway package.json");
  }
  const dependencies = Object.keys(packageJson.dependencies ?? {});
  if (dependencies.length === 0) {
    throw new Error(
      "release production input invalid: gateway dependencies missing",
    );
  }
  for (const dependency of dependencies) {
    try {
      const dependencyRoot = join(
        source,
        "node_modules",
        ...dependency.split("/"),
      );
      const [metadata, packageMetadata] = await Promise.all([
        lstat(dependencyRoot),
        lstat(join(dependencyRoot, "package.json")),
      ]);
      if (!metadata.isDirectory() || !packageMetadata.isFile()) {
        throw new Error();
      }
    } catch {
      throw new Error(`release production dependency missing: ${dependency}`);
    }
  }
}

async function assertGatewayTargetNativePrebuild(source, target) {
  const prebuildRoots = await findGatewaySerialportPrebuildRoots(source);
  if (prebuildRoots.length === 0) {
    throw new Error(
      `release production input invalid: gateway missing ${target} serialport prebuild`,
    );
  }

  const specification = SERIALPORT_PREBUILD_TARGETS[target];
  for (const prebuildRoot of prebuildRoots) {
    const targetDirectory = join(prebuildRoot.path, specification.directory);
    let entries;
    try {
      entries = await readdir(targetDirectory, { withFileTypes: true });
    } catch (error) {
      if (error?.code !== "ENOENT") {
        throw error;
      }
      throw new Error(
        `release production input invalid: gateway missing ${target} serialport prebuild at ${prebuildRoot.relativePath}`,
        { cause: error },
      );
    }
    if (
      !entries.some(
        (entry) => entry.isFile() && entry.name.endsWith(specification.suffix),
      )
    ) {
      throw new Error(
        `release production input invalid: gateway missing ${target} serialport prebuild at ${prebuildRoot.relativePath}`,
      );
    }
  }
}

async function findGatewaySerialportPrebuildRoots(
  directory,
  relativePath = "",
) {
  const roots = [];
  const entries = await readdir(directory, { withFileTypes: true });
  entries.sort((left, right) => compareCanonicalText(left.name, right.name));
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const entryRelativePath = relativePath
      ? `${relativePath}/${entry.name}`
      : entry.name;
    const entryPath = join(directory, entry.name);
    if (serialportPrebuildRemainder(entryRelativePath) === "") {
      roots.push({ path: entryPath, relativePath: entryRelativePath });
      continue;
    }
    roots.push(
      ...(await findGatewaySerialportPrebuildRoots(
        entryPath,
        entryRelativePath,
      )),
    );
  }
  return roots;
}

async function copyDirectory(source, destination, options) {
  await mkdir(destination, { recursive: true });
  let copiedFiles = 0;
  const entries = await readdir(source, { withFileTypes: true });
  entries.sort((left, right) => compareCanonicalText(left.name, right.name));
  for (const entry of entries) {
    const relativePath = options.relativePath
      ? `${options.relativePath}/${entry.name}`
      : entry.name;
    if (
      options.omitPackageBins &&
      options.insideNodeModules &&
      entry.name === ".bin" &&
      entry.isDirectory()
    ) {
      continue;
    }
    if (
      options.gatewayTarget &&
      !isGatewayTargetPath(relativePath, options.gatewayTarget)
    ) {
      continue;
    }
    const sourcePath = join(source, entry.name);
    const destinationPath = join(destination, entry.name);
    const metadata = await lstat(sourcePath);
    if (metadata.isSymbolicLink()) {
      throw new Error("release input contains an unsupported symlink");
    }
    if (metadata.isDirectory()) {
      copiedFiles += await copyDirectory(sourcePath, destinationPath, {
        ...options,
        insideNodeModules:
          options.insideNodeModules || entry.name === "node_modules",
        relativePath,
      });
      continue;
    }
    if (!metadata.isFile()) {
      throw new Error("release input contains an unsupported special file");
    }
    await copyFile(sourcePath, destinationPath);
    await chmod(destinationPath, metadata.mode & 0o111 ? 0o755 : 0o644);
    copiedFiles += 1;
  }
  return copiedFiles;
}

function isGatewayTargetPath(relativePath, target) {
  const remainder = serialportPrebuildRemainder(relativePath);
  if (remainder === undefined) {
    return true;
  }
  if (remainder.length === 0) {
    return true;
  }

  const [directory, fileName, ...nested] = remainder.split("/");
  const specification = SERIALPORT_PREBUILD_TARGETS[target];
  if (directory !== specification.directory) {
    return false;
  }
  if (fileName === undefined) {
    return true;
  }
  return nested.length === 0 && fileName.endsWith(specification.suffix);
}

function serialportPrebuildRemainder(relativePath) {
  const segments = relativePath.split("/");
  const rootSegments = SERIALPORT_PREBUILD_ROOT.split("/");
  for (let index = 0; index <= segments.length - rootSegments.length; index++) {
    if (
      rootSegments.every(
        (segment, offset) => segments[index + offset] === segment,
      )
    ) {
      return segments.slice(index + rootSegments.length).join("/");
    }
  }
  return undefined;
}

function assertComponent(component) {
  if (!RELEASE_COMPONENTS.includes(component)) {
    throw new Error(`unknown release component: ${component}`);
  }
}

function compareCanonicalText(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function targetsForComponent(component) {
  return component === "service" ? ["windows-x86_64"] : RELEASE_TARGETS;
}

function assertTarget(target) {
  if (!RELEASE_TARGETS.includes(target)) {
    throw new Error(`unknown release target: ${target}`);
  }
}

function assertNativeDesktopBundle(target, bundle) {
  if (!NATIVE_DESKTOP_BUNDLES[target].includes(bundle)) {
    throw new Error(`unsupported native Desktop bundle: ${target} ${bundle}`);
  }
}

function assertVersion(version) {
  if (!SEMVER.test(version)) {
    throw new Error(`version must be SemVer: ${version}`);
  }
}

function argumentValue(argumentsList, name) {
  const index = argumentsList.indexOf(name);
  if (index < 0 || index === argumentsList.length - 1) {
    throw new Error(`missing ${name}`);
  }
  return argumentsList[index + 1];
}

function argumentValues(argumentsList, name) {
  return argumentsList.flatMap((value, index) =>
    value === name && index < argumentsList.length - 1
      ? [argumentsList[index + 1]]
      : [],
  );
}

function parseInputArguments(values) {
  return values.map((value) => {
    const separator = value.indexOf("=");
    if (separator < 1 || separator === value.length - 1) {
      throw new Error(`invalid --input value: ${value}`);
    }
    return [value.slice(0, separator), value.slice(separator + 1)];
  });
}

async function main(argumentsList) {
  const [command] = argumentsList;
  if (command === "plan") {
    const version = argumentValue(argumentsList, "--version");
    const plan = releasePlanDocument(version);
    const output = argumentsList.includes("--output")
      ? argumentValue(argumentsList, "--output")
      : undefined;
    const content = `${JSON.stringify(plan, null, 2)}\n`;
    if (output) {
      await mkdir(dirname(output), { recursive: true });
      await writeFile(output, content);
    } else {
      process.stdout.write(content);
    }
    return;
  }
  if (command === "stage") {
    const result = await stageBuild({
      component: argumentValue(argumentsList, "--component"),
      target: argumentValue(argumentsList, "--target"),
      version: argumentValue(argumentsList, "--version"),
      inputs: parseInputArguments(argumentValues(argumentsList, "--input")),
      output: argumentValue(argumentsList, "--output"),
    });
    process.stdout.write(`${JSON.stringify(result.manifest)}\n`);
    return;
  }
  if (command === "check-plan") {
    const content = await readFile(
      argumentValue(argumentsList, "--input"),
      "utf8",
    );
    const plan = JSON.parse(content);
    const expected = releasePlanDocument(plan.version);
    if (JSON.stringify(plan) !== JSON.stringify(expected)) {
      throw new Error(
        "release artifact plan does not match the canonical matrix",
      );
    }
    return;
  }
  throw new Error("usage: release-artifacts.mjs <plan|stage|check-plan> ...");
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
