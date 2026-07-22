import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const JAVASCRIPT_SOURCE_BUILD_STEPS = Object.freeze([
  Object.freeze({
    command: "pnpm",
    arguments: ["--filter", "@cmclient/contracts", "run", "build"],
  }),
  Object.freeze({
    command: "pnpm",
    arguments: ["--filter", "@cmclient/gateway", "run", "build"],
  }),
  Object.freeze({
    command: "pnpm",
    arguments: ["--filter", "@cmclient/web", "run", "build"],
  }),
  Object.freeze({
    command: "pnpm",
    arguments: ["--filter", "@cmclient/desktop", "run", "build"],
  }),
]);

export const WINDOWS_RUST_SOURCE_BUILD_STEP = Object.freeze({
  command: "cargo",
  arguments: [
    "build",
    "--locked",
    "--target",
    "x86_64-pc-windows-msvc",
    "--workspace",
  ],
});

export function sourceBuildSteps(mode) {
  if (mode === "javascript") {
    return [...JAVASCRIPT_SOURCE_BUILD_STEPS];
  }
  if (mode === "windows") {
    return [...JAVASCRIPT_SOURCE_BUILD_STEPS, WINDOWS_RUST_SOURCE_BUILD_STEP];
  }
  throw new Error("source build mode must be javascript or windows");
}

export async function runSourceBuild({
  mode,
  nodeExecutable = process.execPath,
  packageManagerEntrypoint = process.env.npm_execpath,
  platform = process.platform,
  root = resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  runner = runStep,
} = {}) {
  const steps = sourceBuildSteps(mode);
  for (const step of steps) {
    const invocation = invocationForPlatform(step, {
      nodeExecutable,
      packageManagerEntrypoint,
      platform,
    });
    await runner(invocation.command, invocation.arguments, { cwd: root });
  }
}

function invocationForPlatform(
  step,
  { nodeExecutable, packageManagerEntrypoint, platform },
) {
  if (platform === "win32" && step.command === "pnpm") {
    if (!packageManagerEntrypoint) {
      throw new Error("Windows source build must be launched through pnpm");
    }
    return {
      command: nodeExecutable,
      arguments: [packageManagerEntrypoint, ...step.arguments],
    };
  }
  return {
    command:
      platform === "win32" && step.command === "cargo"
        ? "cargo.exe"
        : step.command,
    arguments: [...step.arguments],
  };
}

function runStep(command, arguments_, { cwd }) {
  return new Promise((resolveStep, rejectStep) => {
    const child = spawn(command, arguments_, {
      cwd,
      env: process.env,
      stdio: "inherit",
      windowsHide: true,
    });
    child.once("error", rejectStep);
    child.once("close", (status, signal) => {
      if (status === 0) {
        resolveStep();
        return;
      }
      rejectStep(
        new Error(
          `${command} exited with ${status ?? `signal ${signal ?? "unknown"}`}`,
        ),
      );
    });
  });
}

const entrypoint = process.argv[1]
  ? pathToFileURL(resolve(process.argv[1])).href
  : undefined;
if (entrypoint === import.meta.url) {
  runSourceBuild({ mode: process.argv[2] }).catch((error) => {
    process.stderr.write(`[source-build] ${error.message}\n`);
    process.exitCode = 1;
  });
}
