import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { access, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { isAbsolute, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  helperEnvironment,
  runHelper,
  runUpdaterLiveMatrix,
  validateBaseUrl,
} from "./p13-updater-live-matrix.mjs";

const MAX_CAPTURE_BYTES = 64 * 1024;
const PROCESS_DEADLINE_MS = 180_000;
const INSTALLER_DEATH_DELAY_MS = 750;

export function seedUserPath(baseline, installDirectory) {
  const entries = [
    baseline,
    `${installDirectory}-tools`,
    `${installDirectory}\\tools`,
  ].filter((value) => value !== "");
  return entries.join(";");
}

export function validateInstalledState(state, expectedVersion) {
  assert.equal(state.installExists, true);
  assert.equal(state.executableExists, true);
  assert.equal(state.uninstallerExists, true);
  assert.equal(state.exactPathEntries, 1);
  assert.equal(state.prefixSentinelEntries, 1);
  assert.equal(state.childSentinelEntries, 1);
  assert.equal(state.runMatches, true);
  assert.equal(state.productInstallDirMatches, true);
  assert.equal(state.displayVersion, expectedVersion);
  assert.equal(state.machineUninstallEntries, 0);
  assert.equal(state.webView2Registrations >= 1, true);
}

export function validateUninstalledState(state) {
  assert.equal(state.installExists, false);
  assert.equal(state.executableExists, false);
  assert.equal(state.uninstallerExists, false);
  assert.equal(state.exactPathEntries, 0);
  assert.equal(state.prefixSentinelEntries, 1);
  assert.equal(state.childSentinelEntries, 1);
  assert.equal(state.runPresent, false);
  assert.equal(state.productKeyPresent, false);
  assert.equal(state.userUninstallPresent, false);
  assert.equal(state.machineUninstallEntries, 0);
}

export async function runWindowsInstallLab({
  campaignRoot,
  setupV1,
  setupV2,
  expectedV2Executable,
  publicKeyFile,
  caFile,
  baseUrl,
  wrongPublicKey,
}) {
  assert.equal(process.platform, "win32");
  const root = resolve(campaignRoot);
  const endpointOrigin = validateBaseUrl(baseUrl);
  assert.equal(isAbsolute(root), true);
  const installDirectory = resolve(
    root,
    "installed",
    "CMClient P13 Updater Fixture",
  );
  for (const path of [
    setupV1,
    setupV2,
    expectedV2Executable,
    publicKeyFile,
    caFile,
  ]) {
    assert.equal((await stat(path)).isFile(), true, path);
    assert.equal(isWithin(root, path), true, path);
  }
  assert.equal(isWithin(root, installDirectory), true);
  const publicKey = (await readFile(publicKeyFile, "utf8")).trim();
  const installedExecutable = resolve(
    installDirectory,
    "cmclient-p13-updater-fixture.exe",
  );
  const marker = resolve(root, "updater-relaunch.pending");
  const baseline = await readWindowsState(installDirectory);
  assert.equal(baseline.exactPathEntries, 0);
  assert.equal(baseline.runPresent, false);
  assert.equal(baseline.productKeyPresent, false);
  assert.equal(baseline.userUninstallPresent, false);
  assert.equal(baseline.installExists, false);
  const integrity = await readIntegrity();
  assert.equal(integrity.elevated, false);
  assert.equal(
    ["S-1-16-4096", "S-1-16-8192"].includes(integrity.sid),
    true,
    JSON.stringify(integrity),
  );

  const results = [];
  let completed = false;
  try {
    await setUserPath(seedUserPath(baseline.userPath, installDirectory));
    await cleanFixtureRegistry();

    const installV1 = await runProcess(setupV1, [
      "/S",
      `/D=${installDirectory}`,
    ]);
    assert.equal(installV1.exitCode, 0);
    let state = await readWindowsState(installDirectory);
    validateInstalledState(state, "0.1.0");
    const v1Hash = await fileSha256(installedExecutable);
    results.push(processEvidence("install-0.1.0", installV1));

    const probe = await runHelper({
      executable: installedExecutable,
      environment: helperEnvironment({
        campaignRoot: root,
        endpoint: `${endpointOrigin}/manifest/valid`,
        publicKey,
        caFile,
        mode: "probe",
        timeoutMs: 5000,
      }),
      timeoutMs: 15_000,
    });
    assert.equal(probe.exitCode, 0);
    results.push(processEvidence("headless-apphandle-probe", probe));

    const repairV1 = await runProcess(setupV1, [
      "/S",
      `/D=${installDirectory}`,
    ]);
    assert.equal(repairV1.exitCode, 0);
    state = await readWindowsState(installDirectory);
    validateInstalledState(state, "0.1.0");
    assert.equal(await fileSha256(installedExecutable), v1Hash);
    results.push(processEvidence("repair-0.1.0", repairV1));

    const faultMatrix = await runUpdaterLiveMatrix({
      executable: installedExecutable,
      campaignRoot: root,
      baseUrl: endpointOrigin,
      publicKeyFile,
      caFile,
      wrongPublicKey,
    });

    const installerDeath = await runProcess(
      setupV2,
      ["/S", `/D=${installDirectory}`],
      { killAfterMs: INSTALLER_DEATH_DELAY_MS },
    );
    assert.equal(installerDeath.terminationReason, "injected-death");
    assert.equal(installerDeath.terminationConfirmed, true);
    await waitFor(
      async () => (await accessResult(installedExecutable)) === true,
      10_000,
    );
    const hashAfterInstallerDeath = await fileSha256(installedExecutable);
    const stateAfterInstallerDeath = await readWindowsState(installDirectory);
    results.push({
      ...processEvidence("installer-death-contained", installerDeath),
      damageObserved: hashAfterInstallerDeath !== v1Hash,
      executableSha256AfterDeath: hashAfterInstallerDeath,
      displayVersionAfterDeath: stateAfterInstallerDeath.displayVersion,
    });

    const recoverV1 = await runProcess(setupV1, [
      "/S",
      `/D=${installDirectory}`,
    ]);
    assert.equal(recoverV1.exitCode, 0);
    state = await readWindowsState(installDirectory);
    validateInstalledState(state, "0.1.0");
    assert.equal(await fileSha256(installedExecutable), v1Hash);
    results.push(processEvidence("installer-death-repair-0.1.0", recoverV1));

    await rm(marker, { force: true });
    let helperSettled = false;
    let markerObserved = false;
    const updatePromise = runHelper({
      executable: installedExecutable,
      environment: helperEnvironment({
        campaignRoot: root,
        endpoint: `${endpointOrigin}/manifest/valid`,
        publicKey,
        caFile,
        mode: "install",
        timeoutMs: 30_000,
      }),
      timeoutMs: 120_000,
    }).finally(() => {
      helperSettled = true;
    });
    while (!helperSettled) {
      markerObserved ||= await accessResult(marker);
      await delay(50);
    }
    const updateHelper = await updatePromise;
    assert.equal(updateHelper.exitCode, 0);
    markerObserved ||= await accessResult(marker);

    const expectedV2Hash = await fileSha256(expectedV2Executable);
    state = await waitFor(async () => {
      const candidate = await readWindowsState(installDirectory);
      if (
        candidate.displayVersion === "0.2.0" &&
        candidate.executableExists &&
        (await fileSha256(installedExecutable)) === expectedV2Hash &&
        !(await accessResult(marker))
      ) {
        return candidate;
      }
      return null;
    }, PROCESS_DEADLINE_MS);
    assert.equal(markerObserved, true);
    validateInstalledState(state, "0.2.0");
    assert.notEqual(expectedV2Hash, v1Hash);
    results.push(processEvidence("signed-update-0.1.0-to-0.2.0", updateHelper));

    const repairV2 = await runProcess(setupV2, [
      "/S",
      `/D=${installDirectory}`,
    ]);
    assert.equal(repairV2.exitCode, 0);
    state = await readWindowsState(installDirectory);
    validateInstalledState(state, "0.2.0");
    assert.equal((await stat(installedExecutable)).size > 1024 * 1024, true);
    const installedV2SetupHash = await fileSha256(installedExecutable);
    results.push({
      ...processEvidence("repair-0.2.0", repairV2),
      executableSha256: installedV2SetupHash,
      differsFromUpdaterBundle: installedV2SetupHash !== expectedV2Hash,
    });

    const uninstall = await runProcess(
      resolve(installDirectory, "uninstall.exe"),
      ["/S"],
    );
    assert.equal(uninstall.exitCode, 0);
    state = await waitFor(async () => {
      const candidate = await readWindowsState(installDirectory);
      return !candidate.installExists &&
        candidate.exactPathEntries === 0 &&
        candidate.prefixSentinelEntries === 1 &&
        candidate.childSentinelEntries === 1 &&
        !candidate.runPresent &&
        !candidate.productKeyPresent &&
        !candidate.userUninstallPresent
        ? candidate
        : null;
    }, 60_000);
    validateUninstalledState(state);
    results.push(processEvidence("uninstall-0.2.0", uninstall));
    completed = true;

    return {
      schemaVersion: 1,
      status: "passed",
      generatedAt: new Date().toISOString(),
      platform: process.platform,
      architecture: process.arch,
      integrity,
      installDirectoryWithinCampaign: true,
      baselinePathSha256: sha256(baseline.userPath),
      setupV1Sha256: await fileSha256(setupV1),
      setupV2Sha256: await fileSha256(setupV2),
      installedV1Sha256: v1Hash,
      installedV2UpdaterSha256: expectedV2Hash,
      installedV2SetupSha256: installedV2SetupHash,
      markerObserved,
      faultMatrix,
      results,
    };
  } finally {
    if (!completed) await recoverFixtureInstall(installDirectory);
    await setUserPath(baseline.userPath);
    await cleanFixtureRegistry();
  }
}

async function recoverFixtureInstall(installDirectory) {
  const uninstaller = resolve(installDirectory, "uninstall.exe");
  if (await accessResult(uninstaller)) {
    try {
      await runProcess(uninstaller, ["/S"], { timeoutMs: 60_000 });
    } catch {
      // The campaign is retained for diagnosis; registry/PATH restoration still runs.
    }
  }
}

function processEvidence(name, result) {
  return {
    name,
    exitCode: result.exitCode,
    durationMs: result.durationMs,
    terminationReason: result.terminationReason ?? null,
    terminationConfirmed: result.terminationConfirmed ?? true,
  };
}

async function runProcess(
  executable,
  argumentsList,
  { timeoutMs = PROCESS_DEADLINE_MS, killAfterMs } = {},
) {
  const startedAt = Date.now();
  return await new Promise((resolveChild, rejectChild) => {
    const child = spawn(resolve(executable), argumentsList, {
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
      windowsVerbatimArguments: true,
    });
    let stdout = "";
    let stderr = "";
    let settled = false;
    let terminationReason = null;
    let outputOverflow = false;
    let postTerminationTimer;
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      const appended = appendOutput(stdout, chunk);
      stdout = appended.value;
      outputOverflow ||= appended.overflow;
      if (outputOverflow) void terminate("output-cap");
    });
    child.stderr.on("data", (chunk) => {
      const appended = appendOutput(stderr, chunk);
      stderr = appended.value;
      outputOverflow ||= appended.overflow;
      if (outputOverflow) void terminate("output-cap");
    });
    const deadline = setTimeout(() => void terminate("deadline"), timeoutMs);
    const injected = killAfterMs
      ? setTimeout(() => void terminate("injected-death"), killAfterMs)
      : null;

    async function terminate(reason) {
      if (settled || terminationReason) return;
      terminationReason = reason;
      try {
        await taskkill(child.pid);
      } catch (error) {
        finish(error);
        return;
      }
      if (!settled) {
        postTerminationTimer = setTimeout(
          () => finish(new Error("P13_INSTALL_PROCESS_POST_KILL_TIMEOUT")),
          5000,
        );
      }
    }

    child.once("error", finish);
    child.once("close", (exitCode, signal) => {
      void (async () => {
        const terminationConfirmed = child.pid
          ? !(await isProcessAlive(child.pid))
          : true;
        if (!terminationConfirmed) {
          finish(new Error("P13_INSTALL_PROCESS_STILL_ALIVE"));
          return;
        }
        if (outputOverflow) {
          finish(new Error("P13_INSTALL_PROCESS_OUTPUT_OVERSIZED"));
          return;
        }
        if (!killAfterMs && terminationReason) {
          finish(
            new Error(`P13_INSTALL_PROCESS_${terminationReason.toUpperCase()}`),
          );
          return;
        }
        if (killAfterMs && terminationReason !== "injected-death") {
          finish(new Error("P13_INSTALLER_EXITED_BEFORE_INJECTED_DEATH"));
          return;
        }
        finish(undefined, {
          exitCode,
          signal,
          stdout,
          stderr,
          durationMs: Date.now() - startedAt,
          terminationReason,
          terminationConfirmed,
        });
      })();
    });

    function finish(error, value) {
      if (settled) return;
      settled = true;
      clearTimeout(deadline);
      if (injected) clearTimeout(injected);
      if (postTerminationTimer) clearTimeout(postTerminationTimer);
      if (error) rejectChild(error);
      else resolveChild(value);
    }
  });
}

function appendOutput(current, chunk) {
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

async function taskkill(pid) {
  if (!pid || !(await isProcessAlive(pid))) return;
  try {
    await execFileText("taskkill.exe", ["/PID", String(pid), "/T", "/F"]);
  } catch (error) {
    if (await isProcessAlive(pid)) throw error;
  }
}

async function isProcessAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === "EPERM";
  }
}

async function readIntegrity() {
  const environment = windowsProcessEnvironment();
  const { stdout } = await execFileText(
    resolve(environment.SystemRoot, "System32", "whoami.exe"),
    ["/groups", "/fo", "csv", "/nh"],
    environment,
  );
  const sid = /S-1-16-\d+/.exec(stdout)?.[0] ?? "";
  return {
    sid,
    elevated: ["S-1-16-12288", "S-1-16-16384"].includes(sid),
  };
}

async function readWindowsState(installDirectory) {
  return await powershellJson(
    `
$install = $env:CMCLIENT_P13_INSTALL_DIR
$exe = Join-Path $install 'cmclient-p13-updater-fixture.exe'
$uninstaller = Join-Path $install 'uninstall.exe'
$pathValue = [Environment]::GetEnvironmentVariable('Path', 'User')
$entries = @($pathValue -split ';')
$run = (Get-ItemProperty -Path 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' -Name 'CMClientP13UpdaterFixture' -ErrorAction SilentlyContinue).CMClientP13UpdaterFixture
$product = Get-ItemProperty -Path 'HKCU:\\Software\\CMClient\\P13UpdaterFixture' -ErrorAction SilentlyContinue
$uninstall = Get-ItemProperty -Path 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CMClient P13 Updater Fixture' -ErrorAction SilentlyContinue
$machine = @(
  'HKLM:\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CMClient P13 Updater Fixture',
  'HKLM:\\Software\\WOW6432Node\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CMClient P13 Updater Fixture'
) | Where-Object { Test-Path $_ }
$webview = @(
  Get-ChildItem 'HKCU:\\Software\\Microsoft\\EdgeUpdate\\Clients' -ErrorAction SilentlyContinue
  Get-ChildItem 'HKLM:\\Software\\Microsoft\\EdgeUpdate\\Clients' -ErrorAction SilentlyContinue
  Get-ChildItem 'HKLM:\\Software\\WOW6432Node\\Microsoft\\EdgeUpdate\\Clients' -ErrorAction SilentlyContinue
) | ForEach-Object { Get-ItemProperty $_.PSPath -ErrorAction SilentlyContinue } |
  Where-Object { $_.name -like '*WebView2*' }
[pscustomobject]@{
  userPath = $pathValue
  exactPathEntries = @($entries | Where-Object { $_ -eq $install }).Count
  prefixSentinelEntries = @($entries | Where-Object { $_ -eq ($install + '-tools') }).Count
  childSentinelEntries = @($entries | Where-Object { $_ -eq (Join-Path $install 'tools') }).Count
  runPresent = $null -ne $run
  runMatches = $run -eq ('"' + $exe + '"')
  productKeyPresent = $null -ne $product
  productInstallDirMatches = $product.InstallDir -eq $install
  userUninstallPresent = $null -ne $uninstall
  displayVersion = $uninstall.DisplayVersion
  machineUninstallEntries = @($machine).Count
  webView2Registrations = @($webview).Count
  installExists = Test-Path -LiteralPath $install
  executableExists = Test-Path -LiteralPath $exe
  uninstallerExists = Test-Path -LiteralPath $uninstaller
} | ConvertTo-Json -Compress
`,
    { CMCLIENT_P13_INSTALL_DIR: installDirectory },
  );
}

async function setUserPath(value) {
  await powershellText(
    "[Environment]::SetEnvironmentVariable('Path', $env:CMCLIENT_P13_PATH_VALUE, 'User')",
    { CMCLIENT_P13_PATH_VALUE: value },
  );
}

async function cleanFixtureRegistry() {
  await powershellText(`
Remove-ItemProperty -Path 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' -Name 'CMClientP13UpdaterFixture' -ErrorAction SilentlyContinue
if (Test-Path 'HKCU:\\Software\\CMClient\\P13UpdaterFixture') {
  Remove-Item -Path 'HKCU:\\Software\\CMClient\\P13UpdaterFixture' -Force
}
if (Test-Path 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CMClient P13 Updater Fixture') {
  Remove-Item -Path 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\CMClient P13 Updater Fixture' -Force
}
exit 0
`);
}

async function powershellJson(script, additions = {}) {
  return JSON.parse((await powershellText(script, additions)).trim());
}

async function powershellText(script, additions = {}) {
  const environment = windowsProcessEnvironment(additions);
  const { stdout } = await execFileText(
    "powershell.exe",
    ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script],
    environment,
  );
  return stdout;
}

function windowsProcessEnvironment(additions = {}) {
  const systemRoot =
    process.env.SystemRoot ?? process.env.SYSTEMROOT ?? "C:\\Windows";
  return {
    PATH:
      process.env.PATH ?? process.env.Path ?? resolve(systemRoot, "System32"),
    SystemRoot: systemRoot,
    WINDIR: process.env.WINDIR ?? systemRoot,
    ComSpec:
      process.env.ComSpec ??
      process.env.COMSPEC ??
      resolve(systemRoot, "System32", "cmd.exe"),
    ...additions,
  };
}

function execFileText(file, argumentsList, environment = process.env) {
  return new Promise((resolveChild, rejectChild) => {
    execFile(
      file,
      argumentsList,
      {
        encoding: "utf8",
        env: environment,
        windowsHide: true,
        timeout: 30_000,
        maxBuffer: MAX_CAPTURE_BYTES,
      },
      (error, stdout, stderr) => {
        if (error) rejectChild(error);
        else resolveChild({ stdout, stderr });
      },
    );
  });
}

async function waitFor(probe, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const value = await probe();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await delay(250);
  }
  throw lastError ?? new Error("P13_WINDOWS_INSTALL_LAB_TIMEOUT");
}

async function accessResult(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function delay(milliseconds) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}

async function fileSha256(path) {
  return sha256(await readFile(path));
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function isWithin(parent, child) {
  const value = relative(resolve(parent), resolve(child));
  return value === "" || (!value.startsWith("..") && !isAbsolute(value));
}

async function main(argv = process.argv.slice(2)) {
  const option = (name) => {
    const index = argv.indexOf(name);
    if (index < 0 || !argv[index + 1]) throw new Error(`missing ${name}`);
    return argv[index + 1];
  };
  const campaignRoot = resolve(option("--campaign-root"));
  const fixtureConfig = JSON.parse(
    await readFile("test/p13-updater-driver/src-tauri/tauri.conf.json", "utf8"),
  );
  const result = await runWindowsInstallLab({
    campaignRoot,
    setupV1: option("--setup-v1"),
    setupV2: option("--setup-v2"),
    expectedV2Executable: option("--expected-v2-executable"),
    publicKeyFile: option("--public-key-file"),
    caFile: option("--ca-file"),
    baseUrl: option("--base-url"),
    wrongPublicKey: fixtureConfig.plugins.updater.pubkey,
  });
  const evidenceDirectory = resolve(campaignRoot, "evidence");
  await mkdir(evidenceDirectory, { recursive: true, mode: 0o700 });
  await writeFile(
    resolve(evidenceDirectory, "updater-windows-install-lab.json"),
    `${JSON.stringify(result, null, 2)}\n`,
    { mode: 0o600 },
  );
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  await main();
}
