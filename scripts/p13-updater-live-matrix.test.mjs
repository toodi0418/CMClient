import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { isAbsolute, relative, resolve } from "node:path";
import test from "node:test";

import {
  helperEnvironment,
  parseTasklistWorkingSet,
  resolveUpdaterMatrixPaths,
  validateBaseUrl,
} from "./p13-updater-live-matrix.mjs";
import { streamOversize } from "../test/p13-updater-driver/lab/server.mjs";

test("updater matrix resolves each input path independently", () => {
  const paths = resolveUpdaterMatrixPaths({
    executable: "fixture.exe",
    publicKeyFile: "fixture.key.pub",
    caFile: "certificate.pem",
  });
  assert.equal(paths.length, 3);
  assert.equal(
    paths.every((value) => typeof value === "string"),
    true,
  );
  assert.equal(
    paths.every((value) => isAbsolute(value)),
    true,
  );
});

test("updater helper environment carries no inherited credentials", () => {
  const campaignRoot = "X:/p13-updater-driver";
  const expectedRoot = resolve(campaignRoot);
  const credentialName = "CMCLIENT_CALLMESH_API_KEY";
  const original = process.env[credentialName];
  process.env[credentialName] = "must-not-cross-helper-boundary";
  try {
    const environment = helperEnvironment({
      campaignRoot,
      endpoint: "https://127.0.0.1:9443/manifest/valid",
      publicKey: "public-fixture-key",
      caFile: "X:/p13-updater-driver/tls/certificate.pem",
      mode: "check",
      timeoutMs: 5000,
    });
    assert.equal(environment[credentialName], undefined);
    assert.deepEqual(
      Object.keys(environment).filter((name) =>
        /password|secret|token/i.test(name),
      ),
      [],
    );
    for (const name of [
      "TEMP",
      "TMP",
      "TMPDIR",
      "HOME",
      "USERPROFILE",
      "APPDATA",
      "LOCALAPPDATA",
    ]) {
      const nested = relative(expectedRoot, environment[name]);
      assert.equal(
        nested !== "" && !nested.startsWith("..") && !isAbsolute(nested),
        true,
        name,
      );
    }
  } finally {
    if (original === undefined) delete process.env[credentialName];
    else process.env[credentialName] = original;
  }
});

test("updater matrix accepts only credential-free loopback HTTPS origins", () => {
  assert.equal(
    validateBaseUrl("https://127.0.0.1:9443"),
    "https://127.0.0.1:9443",
  );
  for (const value of [
    "http://127.0.0.1:9443",
    "https://example.com",
    "https://user:pass@127.0.0.1:9443",
    "https://127.0.0.1:9443/manifest",
  ]) {
    assert.throws(() => validateBaseUrl(value));
  }
});

test("tasklist working-set parser is PID-bound and locale tolerant", () => {
  const output = '"fixture.exe","4242","Console","1","123,456 K"\r\n';
  assert.equal(parseTasklistWorkingSet(output, 4242), 123456 * 1024);
  assert.equal(parseTasklistWorkingSet(output, 7), null);
});

test("oversize response declares its size and stops after response close", () => {
  const request = new EventEmitter();
  const response = new EventEmitter();
  let headers;
  let writes = 0;
  let ended = false;
  response.writeHead = (_status, value) => {
    headers = value;
  };
  response.write = () => {
    writes += 1;
    return false;
  };
  response.end = () => {
    ended = true;
  };
  streamOversize(request, response, 2 * 1024 * 1024);
  assert.equal(headers["content-length"], 2 * 1024 * 1024);
  assert.equal(writes, 1);
  response.emit("close");
  response.emit("drain");
  assert.equal(writes, 1);
  assert.equal(ended, false);
});
