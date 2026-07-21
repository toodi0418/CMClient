import assert from "node:assert/strict";
import test from "node:test";

import {
  seedUserPath,
  validateInstalledState,
  validateUninstalledState,
} from "./p13-updater-windows-install-lab.mjs";

test("Windows install lab seeds unrelated PATH sentinels without the exact install entry", () => {
  const install = "X:\\campaign\\installed\\fixture";
  const seeded = seedUserPath("C:\\tools", install).split(";");
  assert.deepEqual(seeded, [
    "C:\\tools",
    `${install}-tools`,
    `${install}\\tools`,
  ]);
  assert.equal(seeded.includes(install), false);
});

test("Windows install state validator binds per-user hooks and version", () => {
  assert.doesNotThrow(() =>
    validateInstalledState(
      {
        installExists: true,
        executableExists: true,
        uninstallerExists: true,
        exactPathEntries: 1,
        prefixSentinelEntries: 1,
        childSentinelEntries: 1,
        runMatches: true,
        productInstallDirMatches: true,
        displayVersion: "0.2.0",
        machineUninstallEntries: 0,
        webView2Registrations: 1,
      },
      "0.2.0",
    ),
  );
});

test("Windows uninstall state validator preserves sentinels and removes product state", () => {
  assert.doesNotThrow(() =>
    validateUninstalledState({
      installExists: false,
      executableExists: false,
      uninstallerExists: false,
      exactPathEntries: 0,
      prefixSentinelEntries: 1,
      childSentinelEntries: 1,
      runPresent: false,
      productKeyPresent: false,
      userUninstallPresent: false,
      machineUninstallEntries: 0,
    }),
  );
});
