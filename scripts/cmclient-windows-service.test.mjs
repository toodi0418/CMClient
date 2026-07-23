import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("Windows service manager registers the service host without credential arguments", async () => {
  const [script, serviceHost] = await Promise.all([
    readFile("scripts/cmclient-windows-service.ps1", "utf8"),
    readFile("apps/service-host/src/main.rs", "utf8"),
  ]);

  assert.match(script, /cmclient-service-host\.exe/);
  assert.match(script, /--service/);
  assert.match(script, /NT AUTHORITY\\LocalService/);
  assert.match(
    script,
    /ValidateSet\("install", "uninstall", "start", "stop", "restart", "status", "logs", "render"\)/,
  );
  assert.match(script, /\$ServiceName = "CMClientAgent"/);
  assert.match(serviceHost, /const SERVICE_NAME: &str = "CMClientAgent";/);
  assert.doesNotMatch(script, /\[string\]\$ServiceName/);
  assert.doesNotMatch(script, /function Assert-SafeServiceName/);
  assert.doesNotMatch(script, /WINDOWS_SERVICE_NAME_INVALID/);
  assert.doesNotMatch(
    script,
    /CALLMESH_API_KEY|APRS_PASSCODE|MANAGEMENT_ADMIN_TOKEN|password=/i,
  );
  assert.match(script, /\[string\]\$Lines = "200"/);
  assert.match(
    script,
    /\[int\]::TryParse\(\$Lines, \[ref\]\$LineCount\)[\s\S]*\$LineCount -lt 1[\s\S]*\$LineCount -gt 10000[\s\S]*WINDOWS_SERVICE_LOG_LINES_INVALID/,
  );
  assert.match(script, /CommonApplicationData/);
  assert.match(script, /service-host\.jsonl/);
  assert.match(script, /agent\.jsonl/);
  assert.match(script, /gateway\.jsonl/);
  assert.match(script, /\\d\{\{4\}\}-\\d\{\{2\}\}-\\d\{\{2\}\}/);
  assert.match(script, /Sort-Object Name -Descending/);
  assert.match(script, /\$datedLogs\[0\]\.FullName/);
  assert.match(script, /\[DateTime\]::TryParseExact/);
  assert.match(script, /Get-Content -LiteralPath \$logFile -Tail \$LineCount/);
  assert.match(script, /WINDOWS_SERVICE_LOG_FILE_INVALID/);
  assert.match(script, /WINDOWS_SERVICE_LOGS_UNAVAILABLE/);
  assert.match(serviceHost, /LogPolicy::from_environment\(\)/);
  assert.match(serviceHost, /service-host\.jsonl/);
  assert.match(serviceHost, /\.stdout\(Stdio::piped\(\)\)/);
  assert.match(serviceHost, /\.stderr\(Stdio::piped\(\)\)/);
  assert.match(serviceHost, /\.capture\(\s*stdout,\s*stderr,/);
  assert.match(serviceHost, /sensitive_process_environment_values\(\)/);
  assert.match(serviceHost, /WINDOWS_SERVICE_AGENT_START_FAILED/);
  assert.match(
    serviceHost,
    /agent\.child\.wait\(\)\?;[\s\S]*agent\.finish_capture\(\);/,
  );
  assert.doesNotMatch(serviceHost, /capture_output/);
  assert.doesNotMatch(serviceHost, /\.stdout\(Stdio::null\(\)\)/);
  assert.doesNotMatch(serviceHost, /\.stderr\(Stdio::null\(\)\)/);
});

test("Windows service path validation remains compatible with Windows PowerShell 5.1", async () => {
  const script = await readFile("scripts/cmclient-windows-service.ps1", "utf8");

  assert.match(script, /function Test-IsWindowsAbsolutePath/);
  assert.match(script, /WINDOWS_SERVICE_PATH_INVALID/);
  assert.doesNotMatch(script, /IsPathFullyQualified/);

  const pathPatterns = [...script.matchAll(/\$Path -match '([^']+)'/g)].map(
    ([, pattern]) => new RegExp(pattern),
  );
  assert.equal(pathPatterns.length, 2);

  const rejectedPrefixes = [
    ...script.matchAll(/\$Path\.StartsWith\('([^']+)'\)/g),
  ].map(([, prefix]) => prefix);
  assert.deepEqual(rejectedPrefixes, ["\\\\?\\", "\\\\.\\"]);

  const isAcceptedAbsolutePath = (path) =>
    !rejectedPrefixes.some((prefix) => path.startsWith(prefix)) &&
    pathPatterns.some((pattern) => pattern.test(path));

  for (const path of [
    String.raw`C:\Program Files\CMClient\cmclient-service-host.exe`,
    String.raw`D:/CMClient/cmclient-service-host.exe`,
    String.raw`\\server\share\cmclient-service-host.exe`,
  ]) {
    assert.equal(isAcceptedAbsolutePath(path), true, path);
  }

  for (const path of [
    String.raw`C:relative\cmclient-service-host.exe`,
    String.raw`\rooted-without-drive\cmclient-service-host.exe`,
    String.raw`relative\cmclient-service-host.exe`,
    String.raw`\\server`,
    String.raw`\\?\C:\CMClient\cmclient-service-host.exe`,
    String.raw`\\.\pipe\cmclient-control`,
  ]) {
    assert.equal(isAcceptedAbsolutePath(path), false, path);
  }
});

test("Windows lifecycle smoke creates and rechecks its retained-state sentinel", async () => {
  const workflow = await readFile(".github/workflows/ci.yml", "utf8");
  const lifecycleStart = workflow.indexOf(
    "Exercise canonical Windows Service install, upgrade, start, and uninstall",
  );
  const lifecycleEnd = workflow.indexOf(
    "\n  linux-systemd-smoke:",
    lifecycleStart,
  );
  const lifecycle = workflow.slice(lifecycleStart, lifecycleEnd);

  assert.ok(lifecycleStart >= 0);
  assert.ok(lifecycleEnd > lifecycleStart);
  assert.match(lifecycle, /\[IO\.File\]::WriteAllText\(\$state, \$sentinel\)/);
  assert.equal(
    (
      lifecycle.match(
        /Test-Path -LiteralPath \$state -PathType Leaf\) -or \[IO\.File\]::ReadAllText\(\$state\) -ne \$sentinel/g,
      ) ?? []
    ).length,
    2,
  );
  const writeIndex = lifecycle.indexOf(
    "[IO.File]::WriteAllText($state, $sentinel)",
  );
  const initialCheckIndex = lifecycle.indexOf(
    "Windows lifecycle retained-state fixture was not created",
  );
  const installIndex = lifecycle.indexOf(
    "powershell -NoProfile -ExecutionPolicy Bypass -File $manager install -HostPath $hostV1",
  );
  const uninstallIndex = lifecycle.indexOf(
    "powershell -NoProfile -ExecutionPolicy Bypass -File $manager uninstall -HostPath $hostV2",
  );
  const finalCheckIndex = lifecycle.indexOf(
    "Windows service uninstall removed retained state",
  );
  assert.ok(writeIndex >= 0);
  assert.ok(writeIndex < initialCheckIndex);
  assert.ok(initialCheckIndex < installIndex);
  assert.ok(installIndex < uninstallIndex);
  assert.ok(uninstallIndex < finalCheckIndex);
  assert.doesNotMatch(lifecycle, /Set-Content\s+-NoNewline\s+\$state/);
  assert.match(lifecycle, /\$serviceName = "CMClientAgent"/);
  assert.match(lifecycle, /WINDOWS_CANONICAL_SERVICE_NAME_IN_USE/);
  assert.doesNotMatch(lifecycle, /CMClientAgentPackage/);
  assert.doesNotMatch(lifecycle, /-ServiceName/);
  assert.match(lifecycle, /\$programDataIsolated = \$false/);
  const isolateIndex = lifecycle.indexOf("$programDataIsolated = $true");
  const cleanupIndex = lifecycle.indexOf("if ($programDataIsolated) {");
  assert.ok(isolateIndex >= 0);
  assert.ok(cleanupIndex > isolateIndex);
  assert.match(
    lifecycle.slice(cleanupIndex),
    /if \(\$programDataIsolated\) \{[\s\S]*Remove-Item -Recurse -Force \$programDataRoot[\s\S]*if \(\$programDataMoved\) \{ Move-Item/,
  );
  assert.match(lifecycle, /Start-Service -Name \$serviceName/);
  assert.match(lifecycle, /\\\\\.\\pipe\\cmclient-control/);
  assert.match(lifecycle, /\$candidate\.managementWeb -eq "disabled"/);
  assert.doesNotMatch(lifecycle, /\$candidate\.management_web/);
  assert.match(
    workflow,
    /cargo build -p cmclient-agent -p cmclient-cli -p cmclient-service-host --locked/,
  );
  assert.match(lifecycle, /\$controlStatus\.agent -ne "running"/);
  assert.match(lifecycle, /\$agent\.ExecutablePath -ne \$agentV2/);
});
