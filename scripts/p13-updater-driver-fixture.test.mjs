import assert from "node:assert/strict";
import { resolve } from "node:path";
import test from "node:test";

import {
  childEnvironment,
  campaignPaths,
  cargoExecutablePath,
  checkFixture,
  FORBIDDEN_REPOSITORY_OUTPUTS,
  readFixtureDocuments,
  validateFixtureDocuments,
} from "./p13-updater-driver-fixture.mjs";
import {
  buildManifest,
  isCampaignPath,
  validateLabEnvironment,
} from "../test/p13-updater-driver/lab/server.mjs";

const repositoryRoot = resolve(".");

function expectCode(errors, code) {
  assert.ok(
    errors.some((entry) => entry.startsWith(code)),
    `expected ${code}, received ${errors.join(", ")}`,
  );
}

test("committed hidden Tauri updater fixture is internally pinned", async () => {
  assert.deepEqual(await checkFixture(repositoryRoot), []);
});

test("fixture validator rejects preview or custom updater drivers", async () => {
  const documents = await readFixtureDocuments(repositoryRoot);
  const errors = validateFixtureDocuments({
    ...documents,
    cargoToml: `${documents.cargoToml}\ncargo-packager-updater = "=0.2.3"\n`,
  });
  expectCode(errors, "P13_UPDATER_FIXTURE_CUSTOM_OR_PREVIEW_DRIVER");
});

test("fixture validator rejects visible windows and artifact-mode drift", async () => {
  const documents = await readFixtureDocuments(repositoryRoot);
  const config = JSON.parse(documents.config);
  config.app.windows = [{ label: "main" }];
  config.bundle.createUpdaterArtifacts = true;
  const errors = validateFixtureDocuments({
    ...documents,
    config: JSON.stringify(config),
  });
  expectCode(errors, "P13_UPDATER_FIXTURE_WINDOW_CONFIG_PRESENT");
  expectCode(errors, "P13_UPDATER_FIXTURE_ARTIFACT_MODE_INVALID");
});

test("fixture validator rejects elevated NSIS hooks", async () => {
  const documents = await readFixtureDocuments(repositoryRoot);
  const errors = validateFixtureDocuments({
    ...documents,
    hooks: `${documents.hooks}\nWriteRegStr HKLM "Software\\CMClient" "x" "y"\n`,
  });
  expectCode(errors, "P13_UPDATER_FIXTURE_HOOK_ELEVATION_FORBIDDEN");
});

test("fixture validator detects locked source drift", async () => {
  const documents = await readFixtureDocuments(repositoryRoot);
  const lock = JSON.parse(documents.lock);
  lock.files.cargoToml = "0".repeat(64);
  const errors = validateFixtureDocuments({
    ...documents,
    lock: JSON.stringify(lock),
  });
  expectCode(errors, "P13_UPDATER_FIXTURE_FILE_DIGEST_DRIFT");
});

test("fault lab manifests preserve target, HTTPS, and downgrade semantics", () => {
  const configuration = {
    host: "127.0.0.1",
    port: 9443,
    target: "windows-x86_64",
    version: "0.2.0",
    signature: "fixture-signature",
  };
  const valid = buildManifest({ caseName: "valid", ...configuration });
  assert.deepEqual(Object.keys(valid.platforms), [configuration.target]);
  assert.equal(
    valid.platforms[configuration.target].url,
    "https://127.0.0.1:9443/payload/valid",
  );

  const wrongTarget = buildManifest({
    caseName: "wrong-target",
    ...configuration,
  });
  assert.equal(wrongTarget.platforms[configuration.target], undefined);
  assert.deepEqual(Object.keys(wrongTarget.platforms), [
    `unsupported-${configuration.target}`,
  ]);

  const downgrade = buildManifest({
    caseName: "downgrade",
    ...configuration,
  });
  assert.equal(downgrade.version, "0.0.1");
});

test("fault lab accepts only files beneath an absolute campaign root", () => {
  const campaignRoot = resolve(repositoryRoot, "..", "p13-fixture-campaign");
  const file = (name) => resolve(campaignRoot, name);
  const configuration = validateLabEnvironment({
    CMCLIENT_CAMPAIGN_ROOT: campaignRoot,
    CMCLIENT_P13_TLS_CERT_FILE: file("tls/certificate.pem"),
    CMCLIENT_P13_TLS_KEY_FILE: file("tls/private-key.pem"),
    CMCLIENT_P13_UPDATE_PAYLOAD: file("updates/payload.nsis.zip"),
    CMCLIENT_P13_UPDATE_SIGNATURE: file("updates/payload.nsis.zip.sig"),
  });
  assert.equal(configuration.campaignRoot, campaignRoot);
  assert.throws(
    () =>
      validateLabEnvironment({
        CMCLIENT_CAMPAIGN_ROOT: campaignRoot,
        CMCLIENT_P13_TLS_CERT_FILE: resolve(
          campaignRoot,
          "..",
          "certificate.pem",
        ),
        CMCLIENT_P13_TLS_KEY_FILE: file("tls/private-key.pem"),
        CMCLIENT_P13_UPDATE_PAYLOAD: file("updates/payload.nsis.zip"),
        CMCLIENT_P13_UPDATE_SIGNATURE: file("updates/payload.nsis.zip.sig"),
      }),
    /P13_UPDATER_LAB_CMCLIENT_P13_TLS_CERT_FILE_INVALID/,
  );
});

test("runner redirects every generated path below a disjoint campaign", () => {
  const campaignRoot = resolve(repositoryRoot, "..", "p13-fixture-campaign");
  const paths = campaignPaths(repositoryRoot, campaignRoot);
  for (const path of Object.values(paths)) {
    assert.equal(isCampaignPath(campaignRoot, path), true, path);
  }
  assert.equal(
    cargoExecutablePath(paths, "x86_64-pc-windows-msvc").includes(
      "x86_64-pc-windows-msvc",
    ),
    true,
  );
  assert.throws(
    () => campaignPaths(repositoryRoot, resolve(repositoryRoot, "test")),
    /P13_UPDATER_CAMPAIGN_OVERLAPS_REPOSITORY/,
  );
  assert.throws(
    () => campaignPaths(repositoryRoot, resolve(repositoryRoot, "..")),
    /P13_UPDATER_CAMPAIGN_OVERLAPS_REPOSITORY/,
  );
  assert.equal(FORBIDDEN_REPOSITORY_OUTPUTS.includes("src-tauri/gen"), true);
});

test("fixture child profile and cache paths stay below campaign without inherited credentials", () => {
  const campaignRoot = resolve(repositoryRoot, "..", "p13-fixture-campaign");
  const paths = campaignPaths(repositoryRoot, campaignRoot);
  const credentialName = "CMCLIENT_CALLMESH_API_KEY";
  const original = process.env[credentialName];
  process.env[credentialName] = "redaction-fixture-value";
  try {
    const environment = childEnvironment(paths);
    assert.equal(environment[credentialName], undefined);
    for (const name of [
      "CARGO_HOME",
      "CARGO_TARGET_DIR",
      "TEMP",
      "TMP",
      "TMPDIR",
      "HOME",
      "USERPROFILE",
      "APPDATA",
      "LOCALAPPDATA",
      "XDG_CACHE_HOME",
      "XDG_CONFIG_HOME",
    ]) {
      assert.equal(isCampaignPath(campaignRoot, environment[name]), true, name);
    }
  } finally {
    if (original === undefined) delete process.env[credentialName];
    else process.env[credentialName] = original;
  }
});
