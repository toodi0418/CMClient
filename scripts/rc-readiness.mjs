import { execFile } from "node:child_process";
import { readFile, readdir } from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";

const runFile = promisify(execFile);

export const RC_VERSION = /^2\.0\.0-rc\.[1-9]\d*$/;
export const RESULT_STATUSES = new Set([
  "pending",
  "pass",
  "fail",
  "blocked",
  "notApplicable",
]);
export const VALIDATOR_KINDS = new Set(["machine", "human", "hardware"]);
export const VALIDATION_GATES = new Set(["rc", "production"]);

export const REQUIRED_TARGETS = Object.freeze([
  "darwin-aarch64",
  "darwin-x86_64",
  "linux-aarch64",
  "linux-x86_64",
  "windows-x86_64",
]);

export const REQUIRED_MODES = Object.freeze([
  "artifact",
  "desktop-portable",
  "desktop-native",
  "headless",
  "cli",
  "systemd",
  "launchd",
  "windows-service",
  "docker",
  "meshtastic-tcp",
  "meshtastic-serial",
  "aprs",
  "callmesh",
  "proxy",
  "management-web",
  "remote-control",
  "backup",
  "migration",
  "update-rollback",
  "security",
]);

export const EXPECTED_PACKAGE_IDENTITIES = Object.freeze([
  "@cmclient/desktop:apps/desktop/package.json",
  "@cmclient/gateway:apps/gateway/package.json",
  "@cmclient/web:apps/web/package.json",
  "@cmclient/api-client:packages/api-client/package.json",
  "@cmclient/config:packages/config/package.json",
  "@cmclient/contracts:packages/contracts/package.json",
  "@cmclient/event-client:packages/event-client/package.json",
  "@cmclient/testing:packages/testing/package.json",
  "@cmclient/validation:packages/validation/package.json",
]);

export const EXPECTED_RUST_IDENTITIES = Object.freeze([
  "cmclient-agent:apps/agent/Cargo.toml",
  "cmclient-cli:apps/cli/Cargo.toml",
  "cmclient-desktop:apps/desktop/src-tauri/Cargo.toml",
  "cmclient-service-host:apps/service-host/Cargo.toml",
  "cmclient-agent-core:crates/agent-core/Cargo.toml",
  "cmclient-cli-client:crates/cli-client/Cargo.toml",
  "cmclient-control-api:crates/control-api/Cargo.toml",
  "cmclient-legacy-migration:crates/legacy-migration/Cargo.toml",
  "cmclient-runtime-logging:crates/runtime-logging/Cargo.toml",
  "cmclient-supervisor:crates/supervisor/Cargo.toml",
  "cmclient-updater:crates/updater/Cargo.toml",
]);

const ALL = Object.freeze(["all"]);
const CASE_REQUIREMENTS = Object.freeze({
  "RC-ARTIFACT-IDENTITY": requirement("machine", ALL, ["artifact"]),
  "RC-SUPPLY-CHAIN": requirement("machine", ALL, ["artifact"]),
  "RC-DESKTOP-PORTABLE": requirement("human", ALL, ["desktop-portable"]),
  "RC-DESKTOP-NATIVE": requirement("human", ALL, ["desktop-native"]),
  "RC-HEADLESS-HOST": requirement("human", ALL, ["headless"]),
  "RC-CLI-HOST": requirement("human", ALL, ["cli"]),
  "RC-SYSTEMD-HOST": requirement(
    "human",
    ["linux-aarch64", "linux-x86_64"],
    ["systemd"],
  ),
  "RC-LAUNCHD-HOST": requirement(
    "human",
    ["darwin-aarch64", "darwin-x86_64"],
    ["launchd"],
  ),
  "RC-WINDOWS-SERVICE-HOST": requirement(
    "human",
    ["windows-x86_64"],
    ["windows-service"],
  ),
  "RC-DOCKER-OCI": requirement(
    "human",
    ["linux-aarch64", "linux-x86_64"],
    ["docker"],
  ),
  "RC-MESHTASTIC-TCP": requirement("human", ALL, ["meshtastic-tcp"]),
  "RC-MESHTASTIC-SERIAL": requirement("hardware", REQUIRED_TARGETS, [
    "meshtastic-serial",
  ]),
  "RC-APRS-IGATE": requirement("hardware", ALL, ["aprs"]),
  "RC-CALLMESH-TENANT": requirement("hardware", ALL, ["callmesh"]),
  "RC-PROXY-MULTICLIENT": requirement("hardware", ALL, ["proxy"]),
  "RC-MANAGEMENT-WEB": requirement("human", ALL, ["management-web"]),
  "RC-REMOTE-CONTROL": requirement("human", ALL, ["remote-control"]),
  "RC-BACKUP-RESTORE": requirement("human", ALL, ["backup"]),
  "RC-LEGACY-MIGRATION": requirement("human", ALL, ["migration"]),
  "RC-UPDATE-ROLLBACK": requirement("human", ALL, ["update-rollback"]),
  "RC-SECURITY-MACHINE": requirement("machine", ALL, ["security"]),
  "RC-RASPBERRY-PI": requirement(
    "hardware",
    ["linux-aarch64"],
    ["headless", "systemd"],
  ),
  "PROD-CHECKSUM-SIGNATURE": requirement(
    "human",
    ALL,
    ["artifact", "security"],
    "production",
  ),
  "PROD-PLATFORM-SIGNING": requirement(
    "human",
    ALL,
    ["desktop-native"],
    "production",
  ),
  "PROD-PROVENANCE": requirement(
    "human",
    ALL,
    ["artifact", "security"],
    "production",
  ),
  "PROD-UPDATE-MANIFEST": requirement(
    "human",
    ALL,
    ["artifact", "update-rollback"],
    "production",
  ),
});

export const REQUIRED_CASE_IDS = Object.freeze(Object.keys(CASE_REQUIREMENTS));

export async function inspectReleaseCandidateSources(root = process.cwd()) {
  const repositoryRoot = resolve(root);
  const rootPackage = await readJson(join(repositoryRoot, "package.json"));
  const packageVersions = new Map();
  for (const parent of ["apps", "packages"]) {
    const directory = join(repositoryRoot, parent);
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const path = join(directory, entry.name, "package.json");
      try {
        const manifest = await readJson(path);
        const manifestPath = repositoryPath(repositoryRoot, path);
        packageVersions.set(
          `${manifest.name}:${manifestPath}`,
          manifest.version,
        );
      } catch (error) {
        if (error?.code !== "ENOENT") throw error;
      }
    }
  }

  const { stdout } = await runFile(
    "cargo",
    ["metadata", "--format-version", "1", "--locked", "--no-deps"],
    { cwd: repositoryRoot, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 },
  );
  const metadata = JSON.parse(stdout);
  const rustVersions = new Map(
    metadata.packages
      .filter(({ manifest_path: manifestPath, source }) => {
        const path = relative(repositoryRoot, resolve(manifestPath));
        return source === null && isRepositoryRelativePath(path);
      })
      .map(({ manifest_path: manifestPath, name, version }) => [
        `${name}:${repositoryPath(repositoryRoot, manifestPath)}`,
        version,
      ]),
  );
  const tauri = await readJson(
    join(repositoryRoot, "apps/desktop/src-tauri/tauri.conf.json"),
  );
  const gatewaySystem = await readFile(
    join(repositoryRoot, "apps/gateway/src/system.ts"),
    "utf8",
  );
  const gatewayVersionDeclarations = [
    ...gatewaySystem.matchAll(/^const COMPILED_BUILD_VERSION = "([^"]+)";$/gm),
  ];
  const gatewayFallback =
    gatewayVersionDeclarations.length === 1
      ? gatewayVersionDeclarations[0][1]
      : undefined;

  return validateReleaseCandidateSources({
    rootVersion: rootPackage.version,
    packageVersions,
    rustVersions,
    tauriVersion: tauri.version,
    gatewayFallback,
  });
}

export function validateReleaseCandidateSources(snapshot) {
  const version = snapshot.rootVersion;
  if (!RC_VERSION.test(version)) throw new Error("RC_VERSION_INVALID");
  assertVersionMap(
    snapshot.packageVersions,
    EXPECTED_PACKAGE_IDENTITIES,
    version,
    "RC_PACKAGE_SET_INVALID",
    "RC_PACKAGE_VERSION_DRIFT",
  );
  assertVersionMap(
    snapshot.rustVersions,
    EXPECTED_RUST_IDENTITIES,
    version,
    "RC_RUST_PACKAGE_SET_INVALID",
    "RC_RUST_VERSION_DRIFT",
  );
  if (
    snapshot.tauriVersion !== version ||
    snapshot.gatewayFallback !== version
  ) {
    throw new Error("RC_RUNTIME_VERSION_DRIFT");
  }
  return {
    version,
    packageCount: snapshot.packageVersions.size,
    rustPackageCount: snapshot.rustVersions.size,
  };
}

export function validateFieldValidationPlan(plan, expectedVersion) {
  if (
    !plan ||
    plan.schemaVersion !== 1 ||
    plan.releaseVersion !== expectedVersion ||
    !RC_VERSION.test(plan.releaseVersion) ||
    !Array.isArray(plan.requiredTargets) ||
    !Array.isArray(plan.requiredModes) ||
    !Array.isArray(plan.cases) ||
    plan.cases.length === 0
  ) {
    throw new Error("RC_FIELD_PLAN_INVALID");
  }
  assertExactTextSet(
    plan.requiredTargets,
    REQUIRED_TARGETS,
    "RC_FIELD_TARGETS_INVALID",
  );
  assertExactTextSet(
    plan.requiredModes,
    REQUIRED_MODES,
    "RC_FIELD_MODES_INVALID",
  );
  const ids = new Set();
  for (const item of plan.cases) {
    if (
      !item ||
      typeof item.id !== "string" ||
      !/^[A-Z][A-Z0-9-]{2,63}$/.test(item.id) ||
      ids.has(item.id) ||
      item.required !== true ||
      !VALIDATOR_KINDS.has(item.validator) ||
      !VALIDATION_GATES.has(item.gate) ||
      !Array.isArray(item.targets) ||
      item.targets.length === 0 ||
      !Array.isArray(item.modes) ||
      item.modes.length === 0 ||
      !isNonEmptyText(item.procedure) ||
      !isNonEmptyText(item.expected) ||
      !Array.isArray(item.evidence) ||
      item.evidence.length === 0 ||
      item.evidence.some((value) => !isNonEmptyText(value))
    ) {
      throw new Error("RC_FIELD_CASE_INVALID");
    }
    ids.add(item.id);
    const expected = CASE_REQUIREMENTS[item.id];
    if (!expected) throw new Error("RC_FIELD_CASE_SET_INVALID");
    assertExactTextSet(
      item.targets,
      expected.targets,
      "RC_FIELD_CASE_TARGET_INVALID",
    );
    assertExactTextSet(
      item.modes,
      expected.modes,
      "RC_FIELD_CASE_MODE_INVALID",
    );
    if (item.gate !== expected.gate || item.validator !== expected.validator) {
      throw new Error("RC_FIELD_CASE_REQUIREMENT_INVALID");
    }
  }
  assertExactTextSet([...ids], REQUIRED_CASE_IDS, "RC_FIELD_CASE_SET_INVALID");
  const executions = buildExecutionMatrix(plan);
  return {
    caseCount: plan.cases.length,
    executionCount: executions.size,
    ids,
  };
}

export function validateFieldValidationEvidence(
  plan,
  evidence,
  {
    requirePromotionReady = false,
    gate = "rc",
    expectedIdentity,
    expectedProductionApproval,
  } = {},
) {
  validateFieldValidationPlan(
    plan,
    requirePromotionReady && expectedIdentity
      ? expectedIdentity.releaseVersion
      : plan.releaseVersion,
  );
  if (
    !evidence ||
    evidence.schemaVersion !== 1 ||
    evidence.releaseVersion !== plan.releaseVersion ||
    !isCommit(evidence.sourceCommit) ||
    !isCommit(evidence.sourceTree) ||
    !isCanonicalGithubActionsRunUrl(evidence.ciRunUrl) ||
    !isCanonicalGithubActionsRunUrl(evidence.releaseRunUrl) ||
    !isNonEmptyText(evidence.artifactName) ||
    !isDigest(evidence.artifactDigestSha256) ||
    !Array.isArray(evidence.results)
  ) {
    throw new Error("RC_FIELD_EVIDENCE_INVALID");
  }
  if (evidence.productionApproval !== undefined) {
    validateProductionApproval(evidence.productionApproval);
  }

  const expectedExecutions = buildExecutionMatrix(plan);
  const byKey = new Map();
  for (const result of evidence.results) {
    if (
      !result ||
      !RESULT_STATUSES.has(result.status) ||
      typeof result.caseId !== "string" ||
      typeof result.target !== "string" ||
      typeof result.mode !== "string"
    ) {
      throw new Error("RC_FIELD_RESULT_INVALID");
    }
    const key = executionKey(result.caseId, result.target, result.mode);
    if (byKey.has(key)) throw new Error("RC_FIELD_RESULT_INVALID");
    const execution = expectedExecutions.get(key);
    if (!execution) throw new Error("RC_FIELD_RESULT_UNKNOWN");
    if (result.status === "pass") {
      assertExecutionEvidence(result);
    } else if (result.status === "fail") {
      assertExecutionEvidence(result);
      if (!isNonEmptyText(result.defectId)) {
        throw new Error("RC_FIELD_FAILURE_DEFECT_MISSING");
      }
    } else if (result.status === "blocked") {
      if (
        !isNonEmptyText(result.owner) ||
        !isNonEmptyText(result.unblockCondition)
      ) {
        throw new Error("RC_FIELD_BLOCKER_INVALID");
      }
    } else if (result.status === "notApplicable") {
      if (execution.required || !isNonEmptyText(result.approvedBy)) {
        throw new Error("RC_FIELD_NOT_APPLICABLE_INVALID");
      }
    }
    byKey.set(key, result);
  }
  if (
    expectedExecutions.size !== byKey.size ||
    [...expectedExecutions.keys()].some((key) => !byKey.has(key))
  ) {
    throw new Error("RC_FIELD_RESULT_SET_INVALID");
  }

  if (requirePromotionReady) {
    if (!VALIDATION_GATES.has(gate)) throw new Error("RC_FIELD_GATE_INVALID");
    assertPromotionIdentity(evidence, expectedIdentity);
    const incomplete = [...expectedExecutions.entries()].some(
      ([key, execution]) =>
        execution.required &&
        (execution.gate === "rc" || gate === "production") &&
        byKey.get(key)?.status !== "pass",
    );
    if (incomplete) throw new Error("RC_FIELD_VALIDATION_INCOMPLETE");
    if (gate === "production") {
      assertProductionApproval(
        evidence.productionApproval,
        expectedProductionApproval,
      );
    }
  }
  return { resultCount: byKey.size };
}

export function createPendingEvidence(plan, identity) {
  validateFieldValidationPlan(plan, plan.releaseVersion);
  return {
    schemaVersion: 1,
    releaseVersion: plan.releaseVersion,
    ...identity,
    results: [...buildExecutionMatrix(plan).values()].map(
      ({ caseId, target, mode }) => ({
        caseId,
        target,
        mode,
        status: "pending",
      }),
    ),
  };
}

function requirement(validator, targets, modes, gate = "rc") {
  return Object.freeze({
    validator,
    targets: Object.freeze([...targets]),
    modes: Object.freeze([...modes]),
    gate,
  });
}

function buildExecutionMatrix(plan) {
  const executions = new Map();
  for (const item of plan.cases) {
    const targets = item.targets.includes("all")
      ? plan.requiredTargets
      : item.targets;
    for (const target of targets) {
      for (const mode of item.modes) {
        const execution = {
          caseId: item.id,
          target,
          mode,
          gate: item.gate,
          required: item.required,
        };
        executions.set(executionKey(item.id, target, mode), execution);
      }
    }
  }
  return executions;
}

function executionKey(caseId, target, mode) {
  return JSON.stringify([caseId, target, mode]);
}

function assertExecutionEvidence(result) {
  if (
    !isNonEmptyText(result.operator) ||
    !isNonEmptyText(result.executedAt) ||
    !Array.isArray(result.evidence) ||
    result.evidence.length === 0 ||
    result.evidence.some((value) => !isNonEmptyText(value))
  ) {
    throw new Error("RC_FIELD_PASS_EVIDENCE_MISSING");
  }
  if (!isUtcTimestamp(result.executedAt)) {
    throw new Error("RC_FIELD_TIMESTAMP_INVALID");
  }
}

function assertPromotionIdentity(evidence, expected) {
  if (
    !expected ||
    !RC_VERSION.test(expected.releaseVersion) ||
    !isCommit(expected.sourceCommit) ||
    !isCommit(expected.sourceTree) ||
    !isCanonicalGithubActionsRunUrl(expected.ciRunUrl) ||
    !isCanonicalGithubActionsRunUrl(expected.releaseRunUrl) ||
    !isNonEmptyText(expected.artifactName) ||
    !isDigest(expected.artifactDigestSha256)
  ) {
    throw new Error("RC_FIELD_PROMOTION_BINDING_MISSING");
  }
  for (const name of [
    "releaseVersion",
    "sourceCommit",
    "sourceTree",
    "ciRunUrl",
    "releaseRunUrl",
    "artifactName",
    "artifactDigestSha256",
  ]) {
    if (evidence[name] !== expected[name]) {
      throw new Error("RC_FIELD_PROMOTION_IDENTITY_MISMATCH");
    }
  }
}

function assertProductionApproval(actual, expected) {
  if (!expected) throw new Error("RC_FIELD_PRODUCTION_APPROVAL_MISSING");
  validateProductionApproval(expected);
  if (!actual) throw new Error("RC_FIELD_PRODUCTION_APPROVAL_MISSING");
  for (const name of ["taskId", "identity", "approvedAt", "reference"]) {
    if (actual[name] !== expected[name]) {
      throw new Error("RC_FIELD_PRODUCTION_APPROVAL_MISMATCH");
    }
  }
}

function validateProductionApproval(approval) {
  if (
    !approval ||
    approval.taskId !== "P12-T05" ||
    !isNonEmptyText(approval.identity) ||
    !isNonEmptyText(approval.approvedAt) ||
    !isNonEmptyText(approval.reference)
  ) {
    throw new Error("RC_FIELD_PRODUCTION_APPROVAL_INVALID");
  }
  if (!isUtcTimestamp(approval.approvedAt)) {
    throw new Error("RC_FIELD_TIMESTAMP_INVALID");
  }
}

function assertVersionMap(
  values,
  expectedIdentities,
  expectedVersion,
  setCode,
  versionCode,
) {
  if (!(values instanceof Map)) throw new Error(setCode);
  assertExactTextSet([...values.keys()], expectedIdentities, setCode);
  if ([...values.values()].some((value) => value !== expectedVersion)) {
    throw new Error(versionCode);
  }
}

function assertExactTextSet(values, expected, code) {
  if (
    !Array.isArray(values) ||
    values.length === 0 ||
    values.some((value) => !isNonEmptyText(value)) ||
    new Set(values).size !== values.length ||
    values.length !== expected.length ||
    expected.some((value) => !values.includes(value))
  ) {
    throw new Error(code);
  }
}

function isNonEmptyText(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function isCommit(value) {
  return typeof value === "string" && /^[a-f0-9]{40}$/.test(value);
}

function isDigest(value) {
  return typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
}

function isCanonicalGithubActionsRunUrl(value) {
  return (
    typeof value === "string" &&
    /^https:\/\/github\.com\/toodi0418\/CMClient\/actions\/runs\/[1-9]\d*$/.test(
      value,
    )
  );
}

function isUtcTimestamp(value) {
  if (
    typeof value !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)
  ) {
    return false;
  }
  const timestamp = Date.parse(value);
  return (
    Number.isFinite(timestamp) && new Date(timestamp).toISOString() === value
  );
}

function isRepositoryRelativePath(path) {
  return path !== ".." && !path.startsWith(`..${sep}`) && !isAbsolute(path);
}

function repositoryPath(repositoryRoot, path) {
  return relative(repositoryRoot, resolve(path)).split(sep).join("/");
}

async function readJson(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

function option(args, name) {
  const index = args.indexOf(name);
  if (index < 0 || !args[index + 1] || args[index + 1].startsWith("--")) {
    throw new Error(`missing ${name}`);
  }
  return args[index + 1];
}

function promotionIdentityFromArgs(args) {
  return {
    releaseVersion: option(args, "--expected-version"),
    sourceCommit: option(args, "--expected-source-commit"),
    sourceTree: option(args, "--expected-source-tree"),
    ciRunUrl: option(args, "--expected-ci-run-url"),
    releaseRunUrl: option(args, "--expected-release-run-url"),
    artifactName: option(args, "--expected-artifact-name"),
    artifactDigestSha256: option(args, "--expected-artifact-digest-sha256"),
  };
}

function productionApprovalFromArgs(args) {
  return {
    taskId: "P12-T05",
    identity: option(args, "--approval-identity"),
    approvedAt: option(args, "--approval-at"),
    reference: option(args, "--approval-ref"),
  };
}

async function main(args) {
  const [command] = args;
  if (command === "check-sources") {
    process.stdout.write(
      `${JSON.stringify(await inspectReleaseCandidateSources())}\n`,
    );
    return;
  }
  if (command === "check-plan") {
    const plan = await readJson(option(args, "--input"));
    const root = await readJson("package.json");
    const result = validateFieldValidationPlan(plan, root.version);
    process.stdout.write(
      `${JSON.stringify({ caseCount: result.caseCount, executionCount: result.executionCount })}\n`,
    );
    return;
  }
  if (command === "check-evidence") {
    const plan = await readJson(option(args, "--plan"));
    const root = await readJson("package.json");
    validateFieldValidationPlan(plan, root.version);
    const evidence = await readJson(option(args, "--input"));
    const requirePromotionReady = args.includes("--promotion-ready");
    const gate = args.includes("--production") ? "production" : "rc";
    const result = validateFieldValidationEvidence(plan, evidence, {
      requirePromotionReady,
      gate,
      expectedIdentity: requirePromotionReady
        ? promotionIdentityFromArgs(args)
        : undefined,
      expectedProductionApproval:
        requirePromotionReady && gate === "production"
          ? productionApprovalFromArgs(args)
          : undefined,
    });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    return;
  }
  throw new Error("unknown rc-readiness command");
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
