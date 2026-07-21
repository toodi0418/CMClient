import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { basename, isAbsolute, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const MAX_CAPTURE_BYTES = 64 * 1024;
const DEFAULT_WORKING_SET_CAP_BYTES = 192 * 1024 * 1024;
const RESOURCE_SAMPLE_INTERVAL_MS = 100;
const POST_TERMINATION_DEADLINE_MS = 5000;

const EXPECTED_CASES = Object.freeze([
  ["check-valid", "check", "valid", 0],
  ["download-valid", "download", "valid", 0],
  ["download-bit-flip", "download", "bit-flip", 22],
  ["check-wrong-target", "check", "wrong-target", 21],
  ["check-downgrade", "check", "downgrade", 10],
  ["check-timeout", "check", "timeout", 21],
]);

export async function runUpdaterLiveMatrix({
  executable,
  campaignRoot,
  baseUrl,
  publicKeyFile,
  caFile,
  wrongPublicKey,
  workingSetCapBytes = DEFAULT_WORKING_SET_CAP_BYTES,
}) {
  const paths = resolveUpdaterMatrixPaths({
    executable,
    publicKeyFile,
    caFile,
  });
  const root = resolve(campaignRoot);
  const origin = validateBaseUrl(baseUrl);
  assert.equal(isAbsolute(root), true);
  assert.equal(isAbsolute(resolve(executable)), true);
  for (const path of paths) assert.equal((await stat(path)).isFile(), true);
  for (const path of [publicKeyFile, caFile]) {
    assert.equal(isWithin(root, path), true);
  }
  assert.equal(
    Number.isInteger(workingSetCapBytes) &&
      workingSetCapBytes >= 64 * 1024 * 1024,
    true,
  );
  const publicKey = (await readFile(publicKeyFile, "utf8")).trim();
  assert.ok(publicKey.length >= 100 && publicKey.length <= 4096);
  assert.ok(wrongPublicKey.length >= 100 && wrongPublicKey.length <= 4096);
  assert.notEqual(wrongPublicKey, publicKey);

  const executableHashBefore = await fileSha256(executable);
  const results = [];
  for (const [name, mode, caseName, expectedExit] of EXPECTED_CASES) {
    const result = await runHelper({
      executable,
      environment: helperEnvironment({
        campaignRoot: root,
        endpoint: `${origin}/manifest/${caseName}`,
        publicKey,
        caFile,
        mode,
        timeoutMs: caseName === "timeout" ? 500 : 5000,
      }),
      timeoutMs: 30_000,
    });
    assert.equal(result.exitCode, expectedExit, `${name}: ${result.stderr}`);
    assertNoKeyLeak(result, [publicKey, wrongPublicKey]);
    results.push(evidenceResult(name, result));
  }

  const wrongKey = await runHelper({
    executable,
    environment: helperEnvironment({
      campaignRoot: root,
      endpoint: `${origin}/manifest/valid`,
      publicKey: wrongPublicKey,
      caFile,
      mode: "download",
      timeoutMs: 5000,
    }),
    timeoutMs: 30_000,
  });
  assert.equal(wrongKey.exitCode, 22, wrongKey.stderr);
  assertNoKeyLeak(wrongKey, [publicKey, wrongPublicKey]);
  results.push(evidenceResult("download-wrong-key", wrongKey));

  const oversize = await runHelper({
    executable,
    environment: helperEnvironment({
      campaignRoot: root,
      endpoint: `${origin}/manifest/oversize`,
      publicKey,
      caFile,
      mode: "download",
      timeoutMs: 30_000,
    }),
    timeoutMs: 30_000,
    maxWorkingSetBytes: workingSetCapBytes,
    expectedTermination: "resource-cap",
  });
  assert.equal(oversize.terminationConfirmed, true);
  assert.equal(oversize.peakWorkingSetBytes > workingSetCapBytes, true);
  assertNoKeyLeak(oversize, [publicKey, wrongPublicKey]);
  results.push(evidenceResult("oversize-resource-cap", oversize));

  const killed = await runHelper({
    executable,
    environment: helperEnvironment({
      campaignRoot: root,
      endpoint: `${origin}/manifest/timeout`,
      publicKey,
      caFile,
      mode: "check",
      timeoutMs: 30_000,
    }),
    timeoutMs: 100,
    expectedTermination: "deadline",
  });
  assert.equal(killed.terminationConfirmed, true);
  assertNoKeyLeak(killed, [publicKey, wrongPublicKey]);
  results.push(evidenceResult("helper-death-contained", killed));

  const executableHashAfter = await fileSha256(executable);
  assert.equal(executableHashAfter, executableHashBefore);
  return {
    schemaVersion: 1,
    status: "passed",
    generatedAt: new Date().toISOString(),
    platform: process.platform,
    architecture: process.arch,
    endpointOrigin: origin,
    executable: {
      name: basename(executable),
      sha256Before: executableHashBefore,
      sha256After: executableHashAfter,
      unchanged: true,
    },
    publicKeySha256: sha256(publicKey),
    workingSetCapBytes,
    results,
  };
}

function evidenceResult(name, result) {
  return {
    name,
    exitCode: result.exitCode,
    durationMs: result.durationMs,
    peakWorkingSetBytes: result.peakWorkingSetBytes,
    terminationReason: result.terminationReason,
    terminationConfirmed: result.terminationConfirmed,
  };
}

function assertNoKeyLeak(result, keys) {
  for (const key of keys) {
    assert.equal(result.stdout.includes(key), false);
    assert.equal(result.stderr.includes(key), false);
  }
}

export function resolveUpdaterMatrixPaths({
  executable,
  publicKeyFile,
  caFile,
}) {
  return [executable, publicKeyFile, caFile].map((value) => resolve(value));
}

export function validateBaseUrl(value) {
  const parsed = new URL(value);
  const loopback = ["127.0.0.1", "localhost", "[::1]"].includes(
    parsed.hostname,
  );
  assert.equal(parsed.protocol, "https:");
  assert.equal(loopback, true);
  assert.equal(parsed.username, "");
  assert.equal(parsed.password, "");
  assert.equal(parsed.pathname, "/");
  assert.equal(parsed.search, "");
  assert.equal(parsed.hash, "");
  return parsed.origin;
}

export function helperEnvironment({
  campaignRoot,
  endpoint,
  publicKey,
  caFile,
  mode,
  timeoutMs,
}) {
  const inherited = {};
  for (const name of ["PATH", "SystemRoot", "WINDIR", "ComSpec"]) {
    if (process.env[name]) inherited[name] = process.env[name];
  }
  const root = resolve(campaignRoot);
  return {
    ...inherited,
    TEMP: resolve(root, "tmp"),
    TMP: resolve(root, "tmp"),
    TMPDIR: resolve(root, "tmp"),
    HOME: resolve(root, "home"),
    USERPROFILE: resolve(root, "home"),
    APPDATA: resolve(root, "home", "AppData", "Roaming"),
    LOCALAPPDATA: resolve(root, "home", "AppData", "Local"),
    CMCLIENT_CAMPAIGN_ROOT: root,
    CMCLIENT_P13_UPDATER_MODE: mode,
    CMCLIENT_P13_UPDATER_ENDPOINT: endpoint,
    CMCLIENT_P13_UPDATER_PUBKEY: publicKey,
    CMCLIENT_P13_UPDATER_CA_FILE: resolve(caFile),
    CMCLIENT_P13_UPDATER_TIMEOUT_MS: String(timeoutMs),
  };
}

export async function runHelper({
  executable,
  environment,
  timeoutMs,
  maxWorkingSetBytes,
  expectedTermination,
}) {
  assert.equal(Number.isInteger(timeoutMs) && timeoutMs >= 50, true);
  const startedAt = Date.now();
  return await new Promise((resolveChild, rejectChild) => {
    const child = spawn(resolve(executable), [], {
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    const pid = child.pid;
    let stdout = "";
    let stderr = "";
    let outputOverflow = false;
    let settled = false;
    let terminationReason = null;
    let peakWorkingSetBytes = 0;
    let sampling = false;
    let postTerminationTimer;

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      ({ value: stdout, overflow: outputOverflow } = appendBounded(
        stdout,
        chunk,
        outputOverflow,
      ));
      if (outputOverflow) void requestTermination("output-cap");
    });
    child.stderr.on("data", (chunk) => {
      ({ value: stderr, overflow: outputOverflow } = appendBounded(
        stderr,
        chunk,
        outputOverflow,
      ));
      if (outputOverflow) void requestTermination("output-cap");
    });

    const timeout = setTimeout(
      () => void requestTermination("deadline"),
      timeoutMs,
    );
    const resourceMonitor = maxWorkingSetBytes
      ? setInterval(() => void sampleWorkingSet(), RESOURCE_SAMPLE_INTERVAL_MS)
      : null;

    async function sampleWorkingSet() {
      if (sampling || settled || terminationReason || !pid) return;
      sampling = true;
      try {
        const bytes = await readWorkingSetBytes(pid);
        if (bytes !== null)
          peakWorkingSetBytes = Math.max(peakWorkingSetBytes, bytes);
        if (bytes !== null && bytes > maxWorkingSetBytes) {
          await requestTermination("resource-cap");
        }
      } catch (error) {
        if (await isProcessAlive(pid)) finish(error);
      } finally {
        sampling = false;
      }
    }

    async function requestTermination(reason) {
      if (settled || terminationReason) return;
      terminationReason = reason;
      try {
        await terminateProcessTree(pid, child);
      } catch (error) {
        finish(error);
        return;
      }
      if (!settled) {
        postTerminationTimer = setTimeout(
          () => finish(new Error("P13_UPDATER_HELPER_POST_KILL_TIMEOUT")),
          POST_TERMINATION_DEADLINE_MS,
        );
      }
    }

    child.once("error", finish);
    child.once("close", (exitCode, signal) => {
      void (async () => {
        const terminationConfirmed = pid ? !(await isProcessAlive(pid)) : true;
        if (!terminationConfirmed) {
          finish(new Error("P13_UPDATER_HELPER_PROCESS_STILL_ALIVE"));
          return;
        }
        if (outputOverflow) {
          finish(new Error("P13_UPDATER_HELPER_OUTPUT_OVERSIZED"));
          return;
        }
        if (expectedTermination && terminationReason !== expectedTermination) {
          finish(
            new Error(
              `P13_UPDATER_HELPER_TERMINATION_MISMATCH:${terminationReason ?? "none"}`,
            ),
          );
          return;
        }
        if (!expectedTermination && terminationReason) {
          finish(
            new Error(`P13_UPDATER_HELPER_${terminationReason.toUpperCase()}`),
          );
          return;
        }
        finish(undefined, {
          exitCode,
          signal,
          stdout,
          stderr,
          durationMs: Date.now() - startedAt,
          peakWorkingSetBytes,
          terminationReason,
          terminationConfirmed,
        });
      })();
    });

    function finish(error, value) {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      if (resourceMonitor) clearInterval(resourceMonitor);
      if (postTerminationTimer) clearTimeout(postTerminationTimer);
      child.stdout.removeAllListeners();
      child.stderr.removeAllListeners();
      child.removeAllListeners();
      if (error) rejectChild(error);
      else resolveChild(value);
    }
  });
}

function appendBounded(current, chunk, overflow) {
  if (overflow) return { value: current, overflow };
  const combined = `${current}${chunk}`;
  if (Buffer.byteLength(combined) <= MAX_CAPTURE_BYTES) {
    return { value: combined, overflow: false };
  }
  return {
    value: Buffer.from(combined)
      .subarray(0, MAX_CAPTURE_BYTES)
      .toString("utf8"),
    overflow: true,
  };
}

async function terminateProcessTree(pid, child) {
  if (!pid || !(await isProcessAlive(pid))) return;
  if (process.platform === "win32") {
    try {
      await execFileResult("taskkill.exe", ["/PID", String(pid), "/T", "/F"]);
    } catch (error) {
      if (await isProcessAlive(pid)) throw error;
    }
  } else if (!child.kill("SIGKILL") && (await isProcessAlive(pid))) {
    throw new Error("P13_UPDATER_HELPER_KILL_REJECTED");
  }
}

async function isProcessAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error?.code === "ESRCH") return false;
    if (error?.code === "EPERM") return true;
    return false;
  }
}

async function readWorkingSetBytes(pid) {
  if (process.platform !== "win32") return null;
  const { stdout } = await execFileResult("tasklist.exe", [
    "/FI",
    `PID eq ${pid}`,
    "/FO",
    "CSV",
    "/NH",
  ]);
  return parseTasklistWorkingSet(stdout, pid);
}

export function parseTasklistWorkingSet(output, pid) {
  const match = /^"[^"]*","(\d+)","[^"]*","[^"]*","([^"]*)"/m.exec(output);
  if (!match || Number(match[1]) !== pid) return null;
  const kilobytes = Number(match[2].replace(/\D/g, ""));
  return Number.isFinite(kilobytes) ? kilobytes * 1024 : null;
}

function execFileResult(file, argumentsList) {
  return new Promise((resolveChild, rejectChild) => {
    execFile(
      file,
      argumentsList,
      {
        encoding: "utf8",
        windowsHide: true,
        timeout: 5000,
        maxBuffer: 64 * 1024,
      },
      (error, stdout, stderr) => {
        if (error) rejectChild(error);
        else resolveChild({ stdout, stderr });
      },
    );
  });
}

function isWithin(parent, child) {
  const value = relative(resolve(parent), resolve(child));
  return value === "" || (!value.startsWith("..") && !isAbsolute(value));
}

async function fileSha256(path) {
  return sha256(await readFile(path));
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

async function main(argv = process.argv.slice(2)) {
  const option = (name) => {
    const index = argv.indexOf(name);
    if (index < 0 || !argv[index + 1]) throw new Error(`missing ${name}`);
    return argv[index + 1];
  };
  const fixtureConfig = JSON.parse(
    await readFile("test/p13-updater-driver/src-tauri/tauri.conf.json", "utf8"),
  );
  const campaignRoot = resolve(option("--campaign-root"));
  const result = await runUpdaterLiveMatrix({
    executable: option("--executable"),
    campaignRoot,
    baseUrl: option("--base-url"),
    publicKeyFile: option("--public-key-file"),
    caFile: option("--ca-file"),
    wrongPublicKey: fixtureConfig.plugins.updater.pubkey,
  });
  const evidenceFile = resolve(
    campaignRoot,
    "evidence",
    "updater-live-matrix.json",
  );
  await mkdir(resolve(campaignRoot, "evidence"), {
    recursive: true,
    mode: 0o700,
  });
  await writeFile(evidenceFile, `${JSON.stringify(result, null, 2)}\n`, {
    mode: 0o600,
  });
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  await main();
}
