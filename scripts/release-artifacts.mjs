import { access, copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

export const RELEASE_COMPONENTS = ["desktop", "headless", "cli"];
export const RELEASE_TARGETS = [
  "darwin-aarch64",
  "darwin-x86_64",
  "linux-aarch64",
  "linux-x86_64",
  "windows-x86_64",
];

const SEMVER = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

export function archiveForTarget(target) {
  assertTarget(target);
  return target.startsWith("windows-") ? "zip" : "tar.zst";
}

export function releaseArtifactName({ component, target, version }) {
  assertComponent(component);
  assertTarget(target);
  assertVersion(version);
  return `cmclient-${component}-${target}-${version}.${archiveForTarget(target)}`;
}

export function releaseArtifactPlan(version) {
  assertVersion(version);
  return RELEASE_COMPONENTS.flatMap((component) =>
    RELEASE_TARGETS.map((target) => ({
      component,
      target,
      archive: archiveForTarget(target),
      fileName: releaseArtifactName({ component, target, version }),
    })),
  );
}

export async function stageBuild({ component, target, version, binary, output }) {
  const fileName = releaseArtifactName({ component, target, version });
  await access(binary);

  const binaryName = basename(binary);
  const directory = join(output, component, target);
  const stagedBinary = join(directory, binaryName);
  await mkdir(directory, { recursive: true });
  await copyFile(binary, stagedBinary);

  const manifest = {
    schemaVersion: 1,
    component,
    target,
    version,
    releaseAsset: {
      archive: archiveForTarget(target),
      fileName,
    },
    binary: binaryName,
  };
  await writeFile(join(directory, "build-manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  return { manifest, stagedBinary };
}

function assertComponent(component) {
  if (!RELEASE_COMPONENTS.includes(component)) {
    throw new Error(`unknown release component: ${component}`);
  }
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

async function main(argumentsList) {
  const [command] = argumentsList;
  if (command === "plan") {
    const version = argumentValue(argumentsList, "--version");
    const plan = {
      schemaVersion: 1,
      version,
      artifacts: releaseArtifactPlan(version),
    };
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
      binary: argumentValue(argumentsList, "--binary"),
      output: argumentValue(argumentsList, "--output"),
    });
    process.stdout.write(`${JSON.stringify(result.manifest)}\n`);
    return;
  }
  if (command === "check-plan") {
    const content = await readFile(argumentValue(argumentsList, "--input"), "utf8");
    const plan = JSON.parse(content);
    const expected = releaseArtifactPlan(plan.version);
    if (
      plan.schemaVersion !== 1 ||
      JSON.stringify(plan.artifacts) !== JSON.stringify(expected)
    ) {
      throw new Error("release artifact plan does not match the canonical matrix");
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
