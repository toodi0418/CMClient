import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  DECISION_RELATIVE_PATH,
  checkRepository,
  findLegacyPrebindClaims,
  validateDecisionDocument,
} from "./p13-feasibility-decision.mjs";

test("P13 decision fixture is exact and self-consistent", async () => {
  const decision = JSON.parse(await readFile(DECISION_RELATIVE_PATH, "utf8"));
  assert.deepEqual(validateDecisionDocument(decision), []);
  assert.equal(decision.node.version, "24.18.0");
  assert.equal(
    decision.node.windowsX64Archive.url,
    "https://nodejs.org/dist/v24.18.0/node-v24.18.0-win-x64.zip",
  );
  assert.equal(
    decision.node.windowsX64Archive.sha256,
    "0ae68406b42d7725661da979b1403ec9926da205c6770827f33aac9d8f26e821",
  );
});

test("the regression detector identifies inherited listener handoff", () => {
  const errors = findLegacyPrebindClaims(
    "Agent pre-binds the loopback listener and passes the inherited socket/handle to Gateway. Agent releases the probe and Gateway rebinds the same fixed port.",
    "synthetic-contract.md",
  );
  assert.deepEqual(errors, [
    "P13_T12_LEGACY_PREBIND_CONTRACT:AGENT_PREBINDS_LISTENER: synthetic-contract.md:1",
    "P13_T12_LEGACY_PREBIND_CONTRACT:PREBINDS_LOOPBACK_LISTENER: synthetic-contract.md:1",
    "P13_T12_LEGACY_PREBIND_CONTRACT:INHERITED_SOCKET_HANDOFF: synthetic-contract.md:1",
    "P13_T12_LEGACY_PREBIND_CONTRACT:RELEASE_REBIND_FALLBACK: synthetic-contract.md:1",
  ]);
});

test("authoritative implementation documents contain no legacy prebind contract", async () => {
  const errors = await checkRepository();
  assert.deepEqual(errors, [], errors.join("\n"));
});

test("decision validator rejects endpoint, bind, and resource-cap drift", async () => {
  const decision = JSON.parse(await readFile(DECISION_RELATIVE_PATH, "utf8"));
  decision.node.windowsX64Archive.sha256 = "f".repeat(64);
  decision.gatewayBootstrap.bind.requestedPort = 4810;
  decision.updaterDownloadBounds.formatCompatibility.cargoPackagerConsumesNsisZip = true;
  decision.updaterDownloadBounds.requiredMitigation.hardResourceCap = false;
  const errors = validateDecisionDocument(decision);
  assert.ok(
    errors.some((value) =>
      value.startsWith("P13_T12_NODE_FACT_DRIFT: windowsX64Archive.sha256"),
    ),
    errors.join("\n"),
  );
  assert.ok(errors.includes("P13_T12_GATEWAY_BIND_ENDPOINT_INVALID"));
  assert.ok(errors.includes("P13_T12_UPDATER_FORMAT_COMPATIBILITY_INVALID"));
  assert.ok(errors.includes("P13_T12_UPDATER_RESOURCE_CAP_REQUIRED"));
});
