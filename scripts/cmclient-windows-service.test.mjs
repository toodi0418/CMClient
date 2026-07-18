import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("Windows service manager registers the service host without credential arguments", async () => {
  const script = await readFile("scripts/cmclient-windows-service.ps1", "utf8");

  assert.match(script, /cmclient-service-host\.exe/);
  assert.match(script, /--service/);
  assert.match(script, /NT AUTHORITY\\LocalService/);
  assert.match(script, /ValidateSet\("install", "uninstall", "start", "stop", "restart", "status", "render"\)/);
  assert.match(script, /\[string\]\$ServiceName = "CMClientAgent"/);
  assert.match(script, /function Assert-SafeServiceName/);
  assert.match(script, /WINDOWS_SERVICE_NAME_INVALID/);
  assert.doesNotMatch(script, /CALLMESH_API_KEY|APRS_PASSCODE|MANAGEMENT_ADMIN_TOKEN|password=/i);
});
