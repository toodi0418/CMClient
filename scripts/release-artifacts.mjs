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
import { dirname, join } from "node:path";

export const RELEASE_COMPONENTS = ["desktop", "headless", "cli", "service"];
export const RELEASE_TARGETS = [
  "darwin-aarch64",
  "darwin-x86_64",
  "linux-aarch64",
  "linux-x86_64",
  "windows-x86_64",
];

export const DOCKER_COMPOSITION = Object.freeze({
  kind: "oci-image",
  updaterManaged: false,
  services: ["gateway", "web"],
  excluded: ["agent", "cli", "desktop", "serviceHost"],
});

const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

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
    schemaVersion: 2,
    version,
    artifacts: releaseArtifactPlan(version).map((artifact) => ({
      ...artifact,
      contents: releaseComposition(artifact.component, artifact.target),
    })),
    docker: DOCKER_COMPOSITION,
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

async function copyCanonicalInput(source, destination, content) {
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
  const copiedFiles = await copyDirectory(source, destination, {
    omitPackageBins: content.role === "gateway",
    insideNodeModules: false,
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

async function copyDirectory(source, destination, options) {
  await mkdir(destination, { recursive: true });
  let copiedFiles = 0;
  const entries = await readdir(source, { withFileTypes: true });
  entries.sort((left, right) => compareCanonicalText(left.name, right.name));
  for (const entry of entries) {
    if (
      options.omitPackageBins &&
      options.insideNodeModules &&
      entry.name === ".bin" &&
      entry.isDirectory()
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

if (import.meta.url === `file://${process.argv[1]}`) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
