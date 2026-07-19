import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("Windows service manager registers the service host without credential arguments", async () => {
  const script = await readFile("scripts/cmclient-windows-service.ps1", "utf8");

  assert.match(script, /cmclient-service-host\.exe/);
  assert.match(script, /--service/);
  assert.match(script, /NT AUTHORITY\\LocalService/);
  assert.match(
    script,
    /ValidateSet\("install", "uninstall", "start", "stop", "restart", "status", "render"\)/,
  );
  assert.match(script, /\[string\]\$ServiceName = "CMClientAgent"/);
  assert.match(script, /function Assert-SafeServiceName/);
  assert.match(script, /WINDOWS_SERVICE_NAME_INVALID/);
  assert.doesNotMatch(
    script,
    /CALLMESH_API_KEY|APRS_PASSCODE|MANAGEMENT_ADMIN_TOKEN|password=/i,
  );
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
