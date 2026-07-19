import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import {
  EXPECTED_PACKAGE_IDENTITIES,
  EXPECTED_RUST_IDENTITIES,
  PRODUCTION_ARTIFACT_NAME,
  REQUIRED_CASE_IDS,
  REQUIRED_MODES,
  REQUIRED_TARGETS,
  createPendingEvidence,
  inspectReleaseCandidateSources,
  validateFieldValidationEvidence,
  validateFieldValidationPlan,
  validateFieldValidationSourceVersion,
  validateReleaseCandidateSources,
} from "./rc-readiness.mjs";

const planPath = "docs/testing/rc-field-validation-plan.json";
const readinessScriptPath = resolve("scripts/rc-readiness.mjs");
const clone = (value) => JSON.parse(JSON.stringify(value));
const runFile = promisify(execFile);
const rootVersion = JSON.parse(await readFile("package.json", "utf8")).version;

const identity = Object.freeze({
  releaseVersion: "2.0.0-rc.1",
  sourceCommit: "a".repeat(40),
  sourceTree: "b".repeat(40),
  ciRunUrl: "https://github.com/toodi0418/CMClient/actions/runs/1001",
  releaseRunUrl: "https://github.com/toodi0418/CMClient/actions/runs/1002",
  artifactName: "cmclient-supply-chain-unsigned-2.0.0-rc.1",
  artifactDigestSha256: "c".repeat(64),
});

const productionApproval = Object.freeze({
  taskId: "P12-T05",
  identity: "release-approver",
  approvedAt: "2026-07-20T08:00:00.000Z",
  reference: "approval://P12-T05/2026-07-20",
});

const productionIdentity = Object.freeze({
  releaseVersion: "2.0.0",
  tag: "v2.0.0",
  sourceCommit: "d".repeat(40),
  sourceTree: "e".repeat(40),
  ciRunUrl: "https://github.com/toodi0418/CMClient/actions/runs/2001",
  releaseRunUrl: "https://github.com/toodi0418/CMClient/actions/runs/2002",
  artifactName: PRODUCTION_ARTIFACT_NAME,
  artifactDigestSha256: "f".repeat(64),
});

test("release candidate sources expose one exact product manifest set", async () => {
  const result = await inspectReleaseCandidateSources();
  assert.deepEqual(result, {
    version: rootVersion,
    packageCount: EXPECTED_PACKAGE_IDENTITIES.length,
    rustPackageCount: EXPECTED_RUST_IDENTITIES.length,
  });
});

test("release candidate source validation rejects version, identity, and package drift", () => {
  const snapshot = sourceSnapshot();
  assert.equal(validateReleaseCandidateSources(snapshot).version, "2.0.0-rc.1");
  assert.equal(
    validateReleaseCandidateSources(sourceSnapshot("2.0.0")).version,
    "2.0.0",
  );

  assert.throws(
    () =>
      validateReleaseCandidateSources({
        ...snapshot,
        rootVersion: "2.0.0-dev.0",
      }),
    /RC_VERSION_INVALID/,
  );
  const driftedPackages = new Map(snapshot.packageVersions);
  driftedPackages.set("@cmclient/web:apps/web/package.json", "2.0.0-rc.2");
  assert.throws(
    () =>
      validateReleaseCandidateSources({
        ...snapshot,
        packageVersions: driftedPackages,
      }),
    /RC_PACKAGE_VERSION_DRIFT/,
  );
  const replacedPackages = new Map(snapshot.packageVersions);
  replacedPackages.delete(EXPECTED_PACKAGE_IDENTITIES[0]);
  replacedPackages.set(
    "@cmclient/replacement:apps/replacement/package.json",
    "2.0.0-rc.1",
  );
  assert.throws(
    () =>
      validateReleaseCandidateSources({
        ...snapshot,
        packageVersions: replacedPackages,
      }),
    /RC_PACKAGE_SET_INVALID/,
  );
  const driftedRust = new Map(snapshot.rustVersions);
  driftedRust.set("cmclient-agent:apps/agent/Cargo.toml", "2.0.0-dev.0");
  assert.throws(
    () =>
      validateReleaseCandidateSources({
        ...snapshot,
        rustVersions: driftedRust,
      }),
    /RC_RUST_VERSION_DRIFT/,
  );
  const replacedRust = new Map(snapshot.rustVersions);
  replacedRust.delete(EXPECTED_RUST_IDENTITIES[0]);
  replacedRust.set(
    "cmclient-replacement:apps/replacement/Cargo.toml",
    "2.0.0-rc.1",
  );
  assert.throws(
    () =>
      validateReleaseCandidateSources({
        ...snapshot,
        rustVersions: replacedRust,
      }),
    /RC_RUST_PACKAGE_SET_INVALID/,
  );
  assert.throws(
    () =>
      validateReleaseCandidateSources({
        ...snapshot,
        gatewayFallback: "2.0.0-dev.0",
      }),
    /RC_RUNTIME_VERSION_DRIFT/,
  );
});

test("field plan remains bound to its RC while allowing only the matching stable source", () => {
  assert.deepEqual(
    validateFieldValidationSourceVersion("2.0.0-rc.1", "2.0.0-rc.1"),
    {
      rcVersion: "2.0.0-rc.1",
      sourceVersion: "2.0.0-rc.1",
      stableVersion: "2.0.0",
    },
  );
  assert.equal(
    validateFieldValidationSourceVersion("2.0.0-rc.1", "2.0.0").stableVersion,
    "2.0.0",
  );
  assert.throws(
    () => validateFieldValidationSourceVersion("2.0.0-rc.1", "2.0.1"),
    /RC_FIELD_SOURCE_VERSION_INVALID/,
  );
});

test("field plan is pinned to the canonical target, mode, case, and execution matrix", async () => {
  const plan = await readPlan();
  const result = validateFieldValidationPlan(plan, "2.0.0-rc.1");
  assert.equal(result.caseCount, REQUIRED_CASE_IDS.length);
  assert.equal(result.executionCount, 129);
  assert.deepEqual(new Set(plan.requiredTargets), new Set(REQUIRED_TARGETS));
  assert.deepEqual(new Set(plan.requiredModes), new Set(REQUIRED_MODES));

  const missingTarget = clone(plan);
  missingTarget.requiredTargets = missingTarget.requiredTargets.filter(
    (target) => target !== "windows-x86_64",
  );
  assert.throws(
    () => validateFieldValidationPlan(missingTarget, "2.0.0-rc.1"),
    /RC_FIELD_TARGETS_INVALID/,
  );

  const missingMode = clone(plan);
  missingMode.requiredModes = missingMode.requiredModes.filter(
    (mode) => mode !== "windows-service",
  );
  assert.throws(
    () => validateFieldValidationPlan(missingMode, "2.0.0-rc.1"),
    /RC_FIELD_MODES_INVALID/,
  );

  const missingCase = clone(plan);
  missingCase.cases = missingCase.cases.filter(
    ({ id }) => id !== "RC-WINDOWS-SERVICE-HOST",
  );
  assert.throws(
    () => validateFieldValidationPlan(missingCase, "2.0.0-rc.1"),
    /RC_FIELD_CASE_SET_INVALID/,
  );

  const reducedCaseTargets = clone(plan);
  reducedCaseTargets.cases.find(
    ({ id }) => id === "RC-DESKTOP-NATIVE",
  ).targets = ["windows-x86_64"];
  assert.throws(
    () => validateFieldValidationPlan(reducedCaseTargets, "2.0.0-rc.1"),
    /RC_FIELD_CASE_TARGET_INVALID/,
  );

  const downgradedValidator = clone(plan);
  downgradedValidator.cases.find(
    ({ id }) => id === "RC-MESHTASTIC-SERIAL",
  ).validator = "machine";
  assert.throws(
    () => validateFieldValidationPlan(downgradedValidator, "2.0.0-rc.1"),
    /RC_FIELD_CASE_REQUIREMENT_INVALID/,
  );

  const duplicate = clone(plan);
  duplicate.cases[1].id = duplicate.cases[0].id;
  assert.throws(
    () => validateFieldValidationPlan(duplicate, "2.0.0-rc.1"),
    /RC_FIELD_CASE_INVALID/,
  );
});

test("field evidence requires the exact case-target-mode execution set", async () => {
  const plan = await readPlan();
  const pending = createPendingEvidence(plan, identity);
  assert.equal(pending.results.length, 129);
  assert.equal(
    validateFieldValidationEvidence(plan, pending).resultCount,
    pending.results.length,
  );

  const missing = clone(pending);
  missing.results.pop();
  assert.throws(
    () => validateFieldValidationEvidence(plan, missing),
    /RC_FIELD_RESULT_SET_INVALID/,
  );

  const duplicate = clone(pending);
  duplicate.results[1] = clone(duplicate.results[0]);
  assert.throws(
    () => validateFieldValidationEvidence(plan, duplicate),
    /RC_FIELD_RESULT_INVALID/,
  );

  const unknownTarget = clone(pending);
  unknownTarget.results[0].target = "linux-riscv64";
  assert.throws(
    () => validateFieldValidationEvidence(plan, unknownTarget),
    /RC_FIELD_RESULT_UNKNOWN/,
  );
});

test("field evidence rejects stored summaries and every unknown top-level field", async () => {
  const plan = await readPlan();
  const pending = createPendingEvidence(plan, identity);

  for (const [name, value] of [
    ["summary", false],
    ["artifactDownload", { downloadedAt: "2026-07-19T08:00:00.000Z" }],
    ["releaseMetadata", {}],
    ["targetEnvironments", []],
    ["unexpected", "field"],
  ]) {
    const unknownField = clone(pending);
    unknownField[name] = value;
    assert.throws(
      () => validateFieldValidationEvidence(plan, unknownField),
      /RC_FIELD_EVIDENCE_INVALID/,
      name,
    );
  }
});

test("pass and fail evidence accepts only safe absolute evidence references", async () => {
  const plan = await readPlan();
  const pending = createPendingEvidence(plan, identity);
  const humanResult = pending.results.find(
    ({ caseId }) => caseId === "RC-CLI-HOST",
  );

  for (const reference of [
    "fake://rc1/result",
    "relative/result",
    "https://operator:secret@example.com/result",
    "https://example.com/result?token=secret",
    "evidence://rc1/result#fragment",
    "evidence://rc1/result%0aforged",
    "evidence://rc1/result\nforged",
  ]) {
    const invalid = clone(pending);
    Object.assign(
      invalid.results.find(({ caseId }) => caseId === humanResult.caseId),
      passedResult([reference]),
    );
    assert.throws(
      () => validateFieldValidationEvidence(plan, invalid),
      /RC_FIELD_EVIDENCE_REFERENCE_INVALID/,
      reference,
    );
  }

  Object.assign(
    humanResult,
    passedResult(["evidence://rc1/windows-cli-status"]),
  );
  assert.doesNotThrow(() => validateFieldValidationEvidence(plan, pending));
});

test("machine evidence binds a canonical job URL to an identity run", async () => {
  const plan = await readPlan();
  const pending = createPendingEvidence(plan, identity);
  const machineResult = pending.results.find(
    ({ caseId }) => caseId === "RC-ARTIFACT-IDENTITY",
  );

  Object.assign(
    machineResult,
    passedResult([
      "https://github.com/toodi0418/CMClient/actions/runs/9999/job/3001",
    ]),
  );
  assert.throws(
    () => validateFieldValidationEvidence(plan, pending),
    /RC_FIELD_MACHINE_JOB_EVIDENCE_MISSING/,
  );

  for (const runUrl of [identity.ciRunUrl, identity.releaseRunUrl]) {
    Object.assign(machineResult, passedResult([`${runUrl}/job/3001`]));
    assert.doesNotThrow(() => validateFieldValidationEvidence(plan, pending));
  }
});

test("RC promotion binds trusted identity and every required RC execution", async () => {
  const plan = await readPlan();
  const pending = createPendingEvidence(plan, identity);
  assert.throws(
    () =>
      validateFieldValidationEvidence(plan, pending, {
        requirePromotionReady: true,
      }),
    /RC_FIELD_PROMOTION_BINDING_MISSING/,
  );
  assert.throws(
    () =>
      validateFieldValidationEvidence(plan, pending, {
        requirePromotionReady: true,
        expectedIdentity: identity,
      }),
    /RC_FIELD_VALIDATION_INCOMPLETE/,
  );

  const rcReady = clone(pending);
  markGatePassed(plan, rcReady, "rc");
  assert.doesNotThrow(() =>
    validateFieldValidationEvidence(plan, rcReady, {
      requirePromotionReady: true,
      expectedIdentity: identity,
    }),
  );

  assert.throws(
    () =>
      validateFieldValidationEvidence(plan, rcReady, {
        requirePromotionReady: true,
        expectedIdentity: {
          ...identity,
          artifactName: "cmclient-supply-chain-unsigned-2.0.0-rc.2",
        },
      }),
    /RC_FIELD_PROMOTION_IDENTITY_MISMATCH/,
  );

  const unrelatedRun = clone(rcReady);
  unrelatedRun.ciRunUrl = "https://example.invalid/actions/runs/1001";
  assert.throws(
    () => validateFieldValidationEvidence(plan, unrelatedRun),
    /RC_FIELD_EVIDENCE_INVALID/,
  );
});

test("production promotion requires all executions and the exact P12-T05 approval", async () => {
  const plan = await readPlan();
  const allReady = createPendingEvidence(plan, identity);
  markGatePassed(plan, allReady, "production");

  assert.throws(
    () =>
      validateFieldValidationEvidence(plan, allReady, {
        requirePromotionReady: true,
        gate: "production",
        expectedIdentity: identity,
      }),
    /RC_FIELD_PRODUCTION_IDENTITY_MISSING/,
  );

  allReady.productionIdentity = productionIdentity;
  assert.throws(
    () =>
      validateFieldValidationEvidence(plan, allReady, {
        requirePromotionReady: true,
        gate: "production",
        expectedIdentity: identity,
        expectedProductionIdentity: productionIdentity,
      }),
    /RC_FIELD_PRODUCTION_APPROVAL_MISSING/,
  );

  allReady.productionApproval = productionApproval;
  assert.doesNotThrow(() =>
    validateFieldValidationEvidence(plan, allReady, {
      requirePromotionReady: true,
      gate: "production",
      expectedIdentity: identity,
      expectedProductionIdentity: productionIdentity,
      expectedProductionApproval: productionApproval,
    }),
  );

  assert.throws(
    () =>
      validateFieldValidationEvidence(plan, allReady, {
        requirePromotionReady: true,
        gate: "production",
        expectedIdentity: identity,
        expectedProductionIdentity: {
          ...productionIdentity,
          sourceCommit: "0".repeat(40),
        },
        expectedProductionApproval: productionApproval,
      }),
    /RC_FIELD_PRODUCTION_IDENTITY_MISMATCH/,
  );

  assert.throws(
    () =>
      validateFieldValidationEvidence(plan, allReady, {
        requirePromotionReady: true,
        gate: "production",
        expectedIdentity: identity,
        expectedProductionIdentity: productionIdentity,
        expectedProductionApproval: {
          ...productionApproval,
          identity: "different-approver",
        },
      }),
    /RC_FIELD_PRODUCTION_APPROVAL_MISMATCH/,
  );

  const reusedRc = clone(allReady);
  reusedRc.productionIdentity.sourceCommit = identity.sourceCommit;
  assert.throws(
    () => validateFieldValidationEvidence(plan, reusedRc),
    /RC_FIELD_PRODUCTION_IDENTITY_NOT_PROMOTED/,
  );

  const wrongStableTag = clone(allReady);
  wrongStableTag.productionIdentity.tag = "v2.0.1";
  assert.throws(
    () => validateFieldValidationEvidence(plan, wrongStableTag),
    /RC_FIELD_PRODUCTION_IDENTITY_INVALID/,
  );
});

test("CLI promotion flags bind RC identity and production approval", async () => {
  const plan = await readPlan();
  const evidence = createPendingEvidence(plan, identity);
  markGatePassed(plan, evidence, "rc");
  const directory = await mkdtemp(join(tmpdir(), "cmclient-rc-readiness-"));
  const evidencePath = join(directory, "evidence.json");
  try {
    await writeFile(evidencePath, JSON.stringify(evidence), "utf8");
    const rcResult = await runReadinessCli([
      "--input",
      evidencePath,
      "--promotion-ready",
      ...identityCliArgs(),
    ]);
    assert.deepEqual(JSON.parse(rcResult.stdout), { resultCount: 129 });

    await assert.rejects(
      runReadinessCli(["--input", evidencePath, "--promotion-ready"]),
      (error) => error.stderr?.includes("missing --expected-version"),
    );

    markGatePassed(plan, evidence, "production");
    evidence.productionIdentity = productionIdentity;
    evidence.productionApproval = productionApproval;
    await writeFile(evidencePath, JSON.stringify(evidence), "utf8");
    await assert.rejects(
      runReadinessCli([
        "--input",
        evidencePath,
        "--promotion-ready",
        "--production",
        ...identityCliArgs(),
        "--approval-identity",
        productionApproval.identity,
        "--approval-at",
        productionApproval.approvedAt,
        "--approval-ref",
        productionApproval.reference,
      ]),
      (error) =>
        error.stderr?.includes("missing --expected-production-source-commit"),
    );
    const productionResult = await runReadinessCli([
      "--input",
      evidencePath,
      "--promotion-ready",
      "--production",
      ...identityCliArgs(),
      ...productionIdentityCliArgs(),
      "--approval-identity",
      productionApproval.identity,
      "--approval-at",
      productionApproval.approvedAt,
      "--approval-ref",
      productionApproval.reference,
    ]);
    assert.deepEqual(JSON.parse(productionResult.stdout), { resultCount: 129 });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("CLI evidence validation keeps the RC plan valid after stable source promotion", async () => {
  const plan = await readPlan();
  const evidence = createPendingEvidence(plan, identity);
  markGatePassed(plan, evidence, "production");
  evidence.productionIdentity = productionIdentity;
  evidence.productionApproval = productionApproval;
  const directory = await mkdtemp(join(tmpdir(), "cmclient-stable-readiness-"));
  const temporaryPlanPath = join(directory, "rc-plan.json");
  const evidencePath = join(directory, "evidence.json");
  const packagePath = join(directory, "package.json");
  const productionArgs = [
    "--input",
    evidencePath,
    "--promotion-ready",
    "--production",
    ...identityCliArgs(),
    ...productionIdentityCliArgs(),
    "--approval-identity",
    productionApproval.identity,
    "--approval-at",
    productionApproval.approvedAt,
    "--approval-ref",
    productionApproval.reference,
  ];

  try {
    await Promise.all([
      writeFile(temporaryPlanPath, JSON.stringify(plan), "utf8"),
      writeFile(evidencePath, JSON.stringify(evidence), "utf8"),
      writeFile(packagePath, JSON.stringify({ version: "2.0.0" }), "utf8"),
    ]);
    const result = await runReadinessCli(productionArgs, {
      cwd: directory,
      planPath: temporaryPlanPath,
    });
    assert.deepEqual(JSON.parse(result.stdout), { resultCount: 129 });

    await writeFile(packagePath, JSON.stringify({ version: "2.0.1" }), "utf8");
    await assert.rejects(
      runReadinessCli(productionArgs, {
        cwd: directory,
        planPath: temporaryPlanPath,
      }),
      (error) => error.stderr?.includes("RC_FIELD_SOURCE_VERSION_INVALID"),
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("malformed execution and approval timestamps return stable field codes", async () => {
  const plan = await readPlan();
  const evidence = createPendingEvidence(plan, identity);
  Object.assign(evidence.results[0], {
    status: "pass",
    operator: "lab-operator",
    executedAt: "2026-99-99T99:99:99.999Z",
    evidence: ["evidence://rc1/invalid-time"],
  });
  assert.throws(
    () => validateFieldValidationEvidence(plan, evidence),
    /RC_FIELD_TIMESTAMP_INVALID/,
  );

  const badApproval = createPendingEvidence(plan, identity);
  badApproval.productionApproval = {
    ...productionApproval,
    approvedAt: "2026-99-99T99:99:99.999Z",
  };
  assert.throws(
    () => validateFieldValidationEvidence(plan, badApproval),
    /RC_FIELD_TIMESTAMP_INVALID/,
  );
});

function sourceSnapshot(version = "2.0.0-rc.1") {
  return {
    rootVersion: version,
    packageVersions: new Map(
      EXPECTED_PACKAGE_IDENTITIES.map((identity) => [identity, version]),
    ),
    rustVersions: new Map(
      EXPECTED_RUST_IDENTITIES.map((identity) => [identity, version]),
    ),
    tauriVersion: version,
    gatewayFallback: version,
  };
}

function markGatePassed(plan, evidence, gate) {
  const byId = new Map(plan.cases.map((item) => [item.id, item]));
  for (const result of evidence.results) {
    const item = byId.get(result.caseId);
    if (gate === "rc" && item.gate !== "rc") continue;
    const evidenceReferences =
      item.validator === "machine"
        ? [`${evidence.ciRunUrl}/job/3001`]
        : [
            `evidence://rc1/${result.caseId.toLowerCase()}/${result.target}/${result.mode}`,
          ];
    Object.assign(result, passedResult(evidenceReferences));
  }
}

function passedResult(evidence) {
  return {
    status: "pass",
    operator: "lab-operator",
    executedAt: "2026-07-19T08:00:00.000Z",
    evidence,
  };
}

function identityCliArgs() {
  return [
    "--expected-version",
    identity.releaseVersion,
    "--expected-source-commit",
    identity.sourceCommit,
    "--expected-source-tree",
    identity.sourceTree,
    "--expected-ci-run-url",
    identity.ciRunUrl,
    "--expected-release-run-url",
    identity.releaseRunUrl,
    "--expected-artifact-name",
    identity.artifactName,
    "--expected-artifact-digest-sha256",
    identity.artifactDigestSha256,
  ];
}

function productionIdentityCliArgs() {
  return [
    "--expected-production-source-commit",
    productionIdentity.sourceCommit,
    "--expected-production-source-tree",
    productionIdentity.sourceTree,
    "--expected-production-ci-run-url",
    productionIdentity.ciRunUrl,
    "--expected-production-release-run-url",
    productionIdentity.releaseRunUrl,
    "--expected-production-artifact-digest-sha256",
    productionIdentity.artifactDigestSha256,
  ];
}

function runReadinessCli(
  args,
  { cwd = process.cwd(), planPath: selectedPlanPath = planPath } = {},
) {
  return runFile(
    process.execPath,
    [
      readinessScriptPath,
      "check-evidence",
      "--plan",
      selectedPlanPath,
      ...args,
    ],
    { cwd, encoding: "utf8" },
  );
}

async function readPlan() {
  return JSON.parse(await readFile(planPath, "utf8"));
}
