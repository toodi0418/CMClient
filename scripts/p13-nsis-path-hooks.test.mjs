import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const hookPath = new URL(
  "../test/p13-updater-driver/src-tauri/windows/hooks.nsh",
  import.meta.url,
);

function reconcilePath(pathValue, installDirectory, mode) {
  const retained = pathValue
    .split(";")
    .filter((entry) => entry !== installDirectory);
  if (mode === "install") retained.push(installDirectory);
  return retained.join(";");
}

test("NSIS hooks replace only semicolon-delimited install directory entries", async () => {
  const hooks = await readFile(hookPath, "utf8");

  assert.match(hooks, /StrCpy \$R1 ";\$R0;"/);
  assert.match(hooks, /\$\{StrRep\} \$R1 "\$R1" ";\$INSTDIR;" ";"/);
  assert.match(hooks, /\$\{UnStrRep\} \$R1 "\$R1" ";\$INSTDIR;" ";"/);
  assert.match(
    hooks,
    /StrCmp \$R1 \$R2 cmclient_install_path_deduped cmclient_install_path_dedupe/,
  );
  assert.match(
    hooks,
    /StrCmp \$R1 \$R2 cmclient_uninstall_path_deduped cmclient_uninstall_path_dedupe/,
  );
  assert.match(hooks, /StrCpy \$R0 "\$R1" -1 1/g);
  assert.doesNotMatch(hooks, /";\$INSTDIR" ""/);
  assert.doesNotMatch(hooks, /"\$INSTDIR;" ""/);
});

test("install leaves one exact entry and preserves prefix and suffix sentinels", () => {
  const installDirectory = String.raw`C:\Users\fixture\CMClient`;
  const prefixSentinel = `${installDirectory}-prefix`;
  const suffixSentinel = `${installDirectory}\\bin`;
  const original = [
    prefixSentinel,
    installDirectory,
    String.raw`C:\Windows`,
    installDirectory,
    suffixSentinel,
  ].join(";");

  const installed = reconcilePath(original, installDirectory, "install");
  assert.deepEqual(installed.split(";"), [
    prefixSentinel,
    String.raw`C:\Windows`,
    suffixSentinel,
    installDirectory,
  ]);
});

test("uninstall removes only exact entries and preserves unrelated formatting", () => {
  const installDirectory = String.raw`C:\Users\fixture\CMClient`;
  const original = [
    `${installDirectory}-prefix`,
    installDirectory,
    "",
    `${installDirectory}2`,
    `${installDirectory}\\bin`,
    installDirectory,
    "%LOCALAPPDATA%\\Tools",
  ].join(";");

  assert.equal(
    reconcilePath(original, installDirectory, "uninstall"),
    [
      `${installDirectory}-prefix`,
      "",
      `${installDirectory}2`,
      `${installDirectory}\\bin`,
      "%LOCALAPPDATA%\\Tools",
    ].join(";"),
  );
});
