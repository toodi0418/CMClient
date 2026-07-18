import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  ALLOWED_REMOVAL_EVIDENCE,
  scanEntry,
  scanTrackedMode,
  scanTrackedRepository,
} from "./legacy-removal-scan.mjs";

const mapName = `TEN${"MAN"}`;
const mapProduct = `Ten${"ManMap"}`;
const mapEnv = `TM${"AG_RELAY_ENDPOINT"}`;
const botName = `@${"cm"}`;

test("scanner rejects forbidden code, environment, database, UI, and docs", () => {
  const fixtures = [
    ["src/bridge.js", `const ${mapName}_QUEUE_LIMIT = 64;`],
    ["config/example.txt", `${mapEnv}=wss://fixture.invalid`],
    ["schema.sql", `CREATE TABLE ten${"map"}_queue (id TEXT);`],
    ["ui/settings.html", `<label>Share with ${mapProduct}</label>`],
    ["docs/bridge.md", `Send a retired command with ${botName} status`],
  ];

  for (const [path, content] of fixtures) {
    assert.notEqual(scanEntry(path, Buffer.from(content)).length, 0, path);
  }
});

test("scanner rejects compound names, normalized text, and NUL-separated bytes", () => {
  const binaryName = Buffer.from(
    [...mapName].flatMap((char) => [char.charCodeAt(0), 0]),
  );
  const normalizedName = `${"ＴＥＮ"}${"ＭＡＮ"}`;

  assert.notDeepEqual(
    scanEntry(`archive/${mapProduct}.bin`, Buffer.alloc(0)),
    [],
  );
  assert.notDeepEqual(scanEntry("fixture.bin", binaryName), []);
  assert.notDeepEqual(
    scanEntry("fixture.txt", Buffer.from(normalizedName)),
    [],
  );
});

test("scanner rejects arbitrary environment, database, archive, and log artifacts", () => {
  const paths = [
    "config/runtime.env.production",
    "state/cache.sqlite3",
    "dist/release.tar.zst",
    "logs/runtime.log",
    "run/nohup.out",
  ];

  for (const path of paths) {
    assert.notDeepEqual(scanEntry(path, Buffer.from("fixture")), [], path);
  }
});

test("scanner rejects Legacy runtime paths and gitlinks", () => {
  const paths = [
    "src/runtime.js",
    `test_${"hardware"}.js`,
    `scripts/${["run", "electron"].join("-")}.js`,
    ["meshtastic", "device"].join("-"),
    [".git", "modules"].join(""),
  ];

  for (const path of paths) {
    assert.notDeepEqual(scanEntry(path, Buffer.from("fixture")), [], path);
  }
  assert.notDeepEqual(scanTrackedMode("vendor/runtime", "160000"), []);
  assert.deepEqual(scanTrackedMode("proto/mesh.proto", "100644"), []);
});

test("scanner rejects retired package dependencies and scripts", () => {
  const path = "packages/fixture/package.json";
  assert.notDeepEqual(
    scanEntry(
      path,
      Buffer.from(
        JSON.stringify({
          dependencies: { electron: "fixture" },
          scripts: { start: "node src/index.js" },
        }),
      ),
    ),
    [],
  );
  assert.deepEqual(
    scanEntry(
      path,
      Buffer.from(
        JSON.stringify({
          dependencies: { protobufjs: "fixture", serialport: "fixture" },
        }),
      ),
    ),
    [],
  );
});

test("scanner does not confuse workspace package names with the retired command", () => {
  assert.deepEqual(
    scanEntry(
      "packages/api-client/src/index.ts",
      Buffer.from('import type { Build } from "@cmclient/contracts";'),
    ),
    [],
  );
});

test("scanner allows only exact removal evidence and migration rejection inputs", async () => {
  const path = "crates/legacy-migration/src/lib.rs";
  const migration = await readFile(path);
  assert.equal(
    ALLOWED_REMOVAL_EVIDENCE.has("docs/legacy-feature-matrix.md"),
    true,
  );
  assert.deepEqual(scanEntry(path, migration), []);
  assert.notDeepEqual(
    scanEntry(path, Buffer.concat([Buffer.from("\n"), migration])),
    [],
  );
  assert.notDeepEqual(
    scanEntry(
      "docs/operator-guide.md",
      Buffer.from(`Enable ${mapName} compatibility`),
    ),
    [],
  );
});

test("tracked repository has no forbidden compatibility or Legacy runtime path", async () => {
  assert.deepEqual(await scanTrackedRepository(), []);
});
