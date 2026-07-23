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

test("Windows CI keeps the transitional service host contract non-elevated", async () => {
  const workflow = await readFile(".github/workflows/ci.yml", "utf8");
  const jobStart = workflow.indexOf("\n  windows-service-smoke:");
  const jobEnd = workflow.indexOf("\n  linux-systemd-smoke:", jobStart);
  const job = workflow.slice(jobStart, jobEnd);

  assert.ok(jobStart >= 0);
  assert.ok(jobEnd > jobStart);
  assert.match(job, /name: Windows transitional service-host contract/);
  assert.match(job, /cargo test -p cmclient-service-host --locked/);
  assert.match(
    job,
    /cargo build -p cmclient-agent -p cmclient-cli -p cmclient-service-host --locked/,
  );
  assert.match(job, /cmclient-windows-service\.ps1 render/);
  const managerCommands = [
    ...job.matchAll(/cmclient-windows-service\.ps1\s+([a-z]+)/gi),
  ].map(([, command]) => command.toLowerCase());
  assert.deepEqual(managerCommands, ["render"]);
  assert.doesNotMatch(
    job,
    /\b(?:sc(?:\.exe)?|Get-Service|New-Service|Set-Service|Start-Service|Stop-Service|Restart-Service|Remove-Service)\b/i,
  );
  assert.doesNotMatch(
    job,
    /\b(?:icacls(?:\.exe)?|takeown(?:\.exe)?|Set-Acl|Get-Acl)\b|S-1-5-|LocalService|NetworkService/i,
  );
  assert.doesNotMatch(
    job,
    /#requires\s+-RunAsAdministrator|-Verb\s+RunAs|\brunas(?:\.exe)?\b|\bgsudo\b/i,
  );
  assert.doesNotMatch(job, /\bProgramData\b/i);
  assert.doesNotMatch(
    job,
    /cmclient-windows-service\.ps1 (?:install|uninstall)/,
  );
  assert.doesNotMatch(job, /Exercise canonical Windows Service/);
});
