import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { parse as parseYaml } from "yaml";

const REQUIRED_TRACKED_FILES = new Set([
  ".github/dependabot.yml",
  ".github/workflows/ci.yml",
  ".github/workflows/release-build.yml",
  "Cargo.lock",
  "Cargo.toml",
  "Dockerfile",
  "package.json",
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "rust-toolchain.toml",
  "security/rustsec-waivers.json",
  "scripts/install-appimage-validator.sh",
  "scripts/install-sbom-tool.sh",
  "scripts/install-security-audit-tools.sh",
]);

const FORBIDDEN_SECRET_PATHS = [
  /(?:^|\/)(?:\.env(?:[._-][^/]*)?|\.envrc|[^/]+\.env(?:[._-][^/]*)?)$/i,
  /(?:^|\/)(?:credentials|secrets?)\.json$/i,
  /(?:^|\/)(?:id_(?:rsa|dsa|ecdsa|ed25519)|[^/]+\.(?:key|pem|p12|pfx))$/i,
  /(?:^|\/)\.npmrc$/i,
];

const FORBIDDEN_ARTIFACT_PATHS = [
  /\.(?:sqlite3?|db3?)(?:-(?:wal|shm))?$/i,
  /\.(?:tar|tgz|tar\.(?:gz|zst)|zip)$/i,
  /\.(?:log|out)$/i,
];

const FORBIDDEN_RESOLUTION_HOOK_PATHS = [/(?:^|\/)\.pnpmfile\.cjs$/i];

const RUSTSEC_WAIVER = Object.freeze({
  advisory: "RUSTSEC-2024-0429",
  expiresOn: "2026-10-19",
  package: "glib",
  transitiveRoot: "tauri",
  version: "0.18.5",
});
const RUSTSEC_WAIVER_PLATFORM = "x86_64-unknown-linux-gnu";

const SECRET_PATTERNS = [
  [
    "SECRET_PRIVATE_KEY",
    /-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY-----/g,
  ],
  ["SECRET_GITHUB_TOKEN", /\bgh[pousr]_[A-Za-z0-9]{36,255}\b/g],
  ["SECRET_GITHUB_PAT", /\bgithub_pat_[A-Za-z0-9_]{50,255}\b/g],
  ["SECRET_AWS_ACCESS_KEY", /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/g],
  ["SECRET_GOOGLE_API_KEY", /\bAIza[A-Za-z0-9_-]{35}\b/g],
  ["SECRET_NPM_TOKEN", /\bnpm_[A-Za-z0-9]{36}\b/g],
  ["SECRET_SLACK_TOKEN", /\bxox[baprs]-[A-Za-z0-9-]{20,255}\b/g],
];

const DEPENDENCY_SECTIONS = [
  "dependencies",
  "devDependencies",
  "optionalDependencies",
  "peerDependencies",
];

// Keep each transitive major compatible while forcing the GHSA-v2hh-gcrm-f6hx fixes.
const REQUIRED_PNPM_OVERRIDES = Object.freeze({
  "fast-uri@^3.0.0": "3.1.4",
  "fast-uri@^4.0.0": "4.1.1",
  "undici-types": "6.23.0",
});
const REQUIRED_FAST_URI_VERSIONS = Object.freeze(["3.1.4", "4.1.1"]);

const FORBIDDEN_LIFECYCLE_SCRIPTS = new Set([
  "preinstall",
  "install",
  "postinstall",
  "prepare",
]);

const SECURITY_TOOL_INSTALLER_PATTERNS = [
  /ACTIONLINT_VERSION="1\.7\.12"/u,
  /8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8/u,
  /GITLEAKS_VERSION="8\.30\.1"/u,
  /551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb/u,
  /CARGO_AUDIT_VERSION="0\.22\.2"/u,
  /cargo-audit-x86_64-unknown-linux-musl-v/u,
  /CARGO_AUDIT_DIRECTORY=/u,
  /7fb9497f8594b389e5fce5ef9b92db08432996895b2e0c5a0167a69ed445c428/u,
  /--strip-components=1/u,
  /sha256sum --check/u,
  /curl --proto '=https' --tlsv1\.2/u,
];

const SBOM_TOOL_INSTALLER_PATTERNS = [
  /SYFT_VERSION="1\.42\.3"/u,
  /0d6be741479eddd2c8644a288990c04f3df0d609bbc1599a005532a9dff63509/u,
  /sha256sum --check/u,
  /curl --proto '=https' --tlsv1\.2/u,
];

const APPIMAGE_VALIDATOR_INSTALLER_PATTERNS = [
  /set -euo pipefail/u,
  /VALIDATOR_VERSION="2\.0\.0-alpha-1-20251018"/u,
  /Linux:x86_64/u,
  /b10c8d39a0a917432af185afc92f1cd54b7f68aa70deda927acacf38ded84990/u,
  /Linux:aarch64/u,
  /79ca9d7b97ffbfb87838659cf7ffa35aa6956226c45bb038c323cda6843a49d4/u,
  /APPIMAGE_VALIDATOR_UNSUPPORTED_PLATFORM/u,
  /https:\/\/github\.com\/AppImageCommunity\/AppImageUpdate\/releases\/download/u,
  /curl --proto '=https' --tlsv1\.2/u,
  /--fail --silent --show-error --location/u,
  /sha256sum --check --strict/u,
  /install -m 0755/u,
];

const SECURITY_TOOL_INSTALLER_SHA256 =
  "b1c65ffc381687925d4e305ae9a3d5bbf581a599cb7f35a743a99d285d4e0e66";
const SBOM_TOOL_INSTALLER_SHA256 =
  "bb803e39839b54913c1bba18e9cf7303965357072efeac786ca3a251f6c6c53a";
const APPIMAGE_VALIDATOR_INSTALLER_SHA256 =
  "b74b5cdf2fb402383ea4e1b97c6511344017a6c1a7929e116a83511c5dfcac91";

const ALLOWED_DEPENDENCY_SPECIFIER =
  /^(?:workspace:\*|[~^]?\d+\.\d+\.\d+(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?(?:\+[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?)$/u;

const LOCKED_PRODUCTION_DEPLOYS = new Map([
  [
    "Dockerfile",
    "pnpm --filter @cmclient/gateway deploy --prod --frozen-lockfile /opt/cmclient/gateway",
  ],
  [
    ".github/workflows/release-build.yml",
    "pnpm --config.node-linker=hoisted --filter @cmclient/gateway deploy --prod --frozen-lockfile release-build-input/gateway",
  ],
]);

const RELEASE_SIGNING_SECRET_NAMES = [
  "CMCLIENT_APPLE_API_KEY_PRIVATE",
  "CMCLIENT_APPLE_CERTIFICATE",
  "CMCLIENT_APPLE_CERTIFICATE_PASSWORD",
  "CMCLIENT_LINUX_SIGNING_KEY",
  "CMCLIENT_LINUX_SIGNING_PASSPHRASE",
  "CMCLIENT_UPDATE_SIGNING_KEY",
  "CMCLIENT_WINDOWS_CERTIFICATE",
  "CMCLIENT_WINDOWS_CERTIFICATE_PASSWORD",
];

const RELEASE_SIGNING_SECRET_SET = new Set(RELEASE_SIGNING_SECRET_NAMES);

const PRODUCTION_RELEASE_CONDITION =
  "github.event_name == 'workflow_dispatch' && inputs.attest && startsWith(github.ref, 'refs/tags/v')";

const UPDATE_MANIFEST_CONDITION =
  "github.event_name == 'workflow_dispatch' && inputs.attest && inputs.release_base_url != '' && startsWith(github.ref, 'refs/tags/v')";

const PLATFORM_SECRET_SCOPES = [
  {
    condition: "startsWith(matrix.target, 'darwin-')",
    markers: ["security import", "APPLE_API_KEY_PATH"],
    names: [
      "CMCLIENT_APPLE_API_KEY_PRIVATE",
      "CMCLIENT_APPLE_CERTIFICATE",
      "CMCLIENT_APPLE_CERTIFICATE_PASSWORD",
    ],
  },
  {
    condition: "startsWith(matrix.target, 'windows-')",
    markers: ["Import-PfxCertificate", "CMCLIENT_IMPORTED_CERT_THUMBPRINT"],
    names: [
      "CMCLIENT_WINDOWS_CERTIFICATE",
      "CMCLIENT_WINDOWS_CERTIFICATE_PASSWORD",
    ],
  },
  {
    condition: "startsWith(matrix.target, 'linux-')",
    markers: ["gpg --batch --import", "GNUPGHOME"],
    names: ["CMCLIENT_LINUX_SIGNING_KEY"],
  },
  {
    condition: "startsWith(matrix.target, 'linux-')",
    envNames: {
      CMCLIENT_LINUX_SIGNING_PASSPHRASE: "APPIMAGETOOL_SIGN_PASSPHRASE",
    },
    markers: ["tauri bundle", "APPIMAGETOOL_SIGN_PASSPHRASE"],
    names: ["CMCLIENT_LINUX_SIGNING_PASSPHRASE"],
  },
];

export function scanSecretEntry(path, bytes) {
  const violations = [];
  if (FORBIDDEN_SECRET_PATHS.some((pattern) => pattern.test(path))) {
    violations.push({ code: "SECRET_BEARING_PATH", path });
  }
  if (FORBIDDEN_ARTIFACT_PATHS.some((pattern) => pattern.test(path))) {
    violations.push({ code: "GENERATED_OR_SENSITIVE_ARTIFACT_PATH", path });
  }
  if (FORBIDDEN_RESOLUTION_HOOK_PATHS.some((pattern) => pattern.test(path))) {
    violations.push({ code: "DEPENDENCY_RESOLUTION_HOOK_FORBIDDEN", path });
  }
  const text = bytes.toString("utf8").normalize("NFKC");
  for (const [code, pattern] of SECRET_PATTERNS) {
    for (const match of text.matchAll(pattern)) {
      violations.push({
        code,
        path,
        line: lineNumber(text, match.index ?? 0),
      });
    }
  }
  return violations;
}

export function auditWorkflow(path, workflow) {
  const violations = [];
  let document;
  try {
    document = parseYaml(workflow);
  } catch {
    return {
      actions: 0,
      violations: [{ code: "WORKFLOW_YAML_INVALID", path }],
    };
  }
  if (!isRecord(document)) {
    return {
      actions: 0,
      violations: [{ code: "WORKFLOW_DOCUMENT_INVALID", path }],
    };
  }

  if (workflowTriggerNames(document.on).includes("pull_request_target")) {
    violations.push({ code: "WORKFLOW_UNTRUSTED_PRIVILEGED_TRIGGER", path });
  }

  for (const expression of workflowSecretExpressions(document)) {
    const allowedReleaseSigningSecret =
      path === ".github/workflows/release-build.yml" &&
      isAllowedReleaseSigningSecretExpression(expression);
    if (!allowedReleaseSigningSecret) {
      violations.push({
        code: "WORKFLOW_SECRET_CONTEXT_UNEXPECTED",
        path,
      });
    }
  }
  if (
    !isRecord(document.permissions) ||
    document.permissions.contents !== "read"
  ) {
    violations.push({ code: "WORKFLOW_DEFAULT_PERMISSIONS_INVALID", path });
  }
  for (const [permission, access] of Object.entries(
    isRecord(document.permissions) ? document.permissions : {},
  )) {
    if (!["read", "none"].includes(access) || permission === "write-all") {
      violations.push({
        code: "WORKFLOW_EXCESS_PERMISSION",
        path,
        detail: `workflow:${permission}`,
      });
    }
  }

  let actions = 0;
  const jobs = isRecord(document.jobs) ? document.jobs : {};
  if (!isRecord(document.jobs)) {
    violations.push({ code: "WORKFLOW_JOBS_INVALID", path });
  }
  for (const [jobName, value] of Object.entries(jobs)) {
    if (!isRecord(value)) {
      violations.push({
        code: "WORKFLOW_JOB_INVALID",
        path,
        detail: jobName,
      });
      continue;
    }
    if (typeof value.uses === "string") {
      actions += 1;
      if (!isPinnedAction(value.uses)) {
        violations.push({
          code: "WORKFLOW_ACTION_NOT_SHA_PINNED",
          path,
          detail: `${jobName}:reusable:${value.uses}`,
        });
      }
      if (value.secrets === "inherit") {
        violations.push({
          code: "WORKFLOW_REUSABLE_SECRETS_INHERITED",
          path,
          detail: jobName,
        });
      }
    }
    if (Object.hasOwn(value, "permissions") && !isRecord(value.permissions)) {
      violations.push({
        code: "WORKFLOW_EXCESS_PERMISSION",
        path,
        detail: `${jobName}:permissions`,
      });
    }
    for (const [permission, access] of Object.entries(
      isRecord(value.permissions) ? value.permissions : {},
    )) {
      const allowedAttestationWrite =
        path === ".github/workflows/release-build.yml" &&
        jobName === "attest" &&
        ["id-token", "attestations"].includes(permission);
      if (
        !["read", "none"].includes(access) &&
        !(access === "write" && allowedAttestationWrite)
      ) {
        violations.push({
          code: "WORKFLOW_EXCESS_PERMISSION",
          path,
          detail: `${jobName}:${permission}`,
        });
      }
    }
    for (const [stepIndex, step] of workflowSteps(value).entries()) {
      if (typeof step.uses === "string") {
        actions += 1;
        if (!isPinnedAction(step.uses)) {
          violations.push({
            code: "WORKFLOW_ACTION_NOT_SHA_PINNED",
            path,
            detail: `${jobName}:${stepIndex}:${step.uses}`,
          });
        }
        if (
          step.uses.startsWith("actions/checkout@") &&
          (!isRecord(step.with) || step.with["persist-credentials"] !== false)
        ) {
          violations.push({
            code: "WORKFLOW_CHECKOUT_PERSISTS_CREDENTIALS",
            path,
            detail: `${jobName}:${stepIndex}`,
          });
        }
        if (
          step.uses.startsWith("actions/setup-node@") &&
          (!isRecord(step.with) || step.with["node-version"] !== "22.23.1")
        ) {
          violations.push({
            code: "WORKFLOW_NODE_TOOLCHAIN_UNPINNED",
            path,
            detail: `${jobName}:${stepIndex}`,
          });
        }
      }
      if (
        typeof step.run === "string" &&
        workflowSecretExpressions(step.run).length > 0
      ) {
        violations.push({
          code: "WORKFLOW_SECRET_INTERPOLATED_IN_SCRIPT",
          path,
          detail: `${jobName}:${stepIndex}`,
        });
      }
    }
  }

  if (path === ".github/workflows/ci.yml") {
    violations.push(...auditCiDocument(path, document));
  }
  if (path === ".github/workflows/release-build.yml") {
    violations.push(...auditReleaseDocument(path, document));
    violations.push(...auditProductionDeploy(path, workflow));
  }
  return { actions, violations };
}

function auditCiDocument(path, document) {
  const violations = [];
  auditSecurityJob(
    violations,
    path,
    document,
    "security-audit",
    "CI_SECURITY_GATE_MISSING",
  );
  return violations;
}

function auditReleaseDocument(path, document) {
  const violations = [];
  auditSecurityJob(
    violations,
    path,
    document,
    "security-gate",
    "RELEASE_SECURITY_GATE_MISSING",
  );

  const jobs = isRecord(document.jobs) ? document.jobs : {};
  for (const jobName of ["build", "docker-composition"]) {
    const job = isRecord(jobs[jobName]) ? jobs[jobName] : undefined;
    if (
      !job ||
      !sameStringSet(job.needs, ["artifact-plan", "load-gate", "security-gate"])
    ) {
      violations.push({
        code: "RELEASE_SECURITY_GATE_MISSING",
        path,
        detail: `${jobName}:needs`,
      });
    }
  }

  const finalWindowsService = isRecord(jobs["final-windows-service-smoke"])
    ? jobs["final-windows-service-smoke"]
    : undefined;
  if (
    !finalWindowsService ||
    !sameStringSet(finalWindowsService.needs, ["supply-chain"]) ||
    finalWindowsService["continue-on-error"] === true
  ) {
    violations.push({
      code: "RELEASE_PLATFORM_SIGNING_GATE_INVALID",
      path,
      detail: "final-windows-service-smoke:policy",
    });
  }

  const releaseSecretExpressions = workflowSecretExpressions(document);
  const releaseSecretNames = releaseSecretExpressions
    .map(releaseSigningSecretName)
    .filter((name) => name !== undefined);
  if (
    releaseSecretExpressions.length !== RELEASE_SIGNING_SECRET_NAMES.length ||
    !sameStringSet(releaseSecretNames, RELEASE_SIGNING_SECRET_NAMES)
  ) {
    violations.push({
      code: "RELEASE_SIGNING_SECRET_SCOPE_INVALID",
      path,
      detail: "release:secret-set",
    });
  }

  const platformSigning = isRecord(jobs["platform-sign-native"])
    ? jobs["platform-sign-native"]
    : undefined;
  if (!platformSigning) {
    violations.push({
      code: "RELEASE_PLATFORM_SIGNING_GATE_INVALID",
      path,
      detail: "platform-sign-native:missing",
    });
  } else {
    const steps = workflowSteps(platformSigning);
    const exactTagIndex = steps.findIndex((step) =>
      hasRunMarkers(step, [
        '[[ "$GITHUB_REF" == "refs/tags/v$version" ]]',
        "RELEASE_TAG_VERSION_MISMATCH",
      ]),
    );
    if (
      !sameStringSet(platformSigning.needs, [
        "build",
        "supply-chain",
        "final-windows-service-smoke",
      ]) ||
      !isProductionReleaseJob(platformSigning, PRODUCTION_RELEASE_CONDITION) ||
      exactTagIndex < 0 ||
      !isFailClosedStep(steps[exactTagIndex]) ||
      !platformSecretScopesAreValid(platformSigning, steps, exactTagIndex)
    ) {
      violations.push({
        code: "RELEASE_PLATFORM_SIGNING_GATE_INVALID",
        path,
        detail: "platform-sign-native:policy",
      });
    }
    const receiptIndex = steps.findIndex((step) =>
      hasRunMarkers(step, [
        "release-supply-chain.mjs platform-receipt",
        "--identity-reference",
      ]),
    );
    const platformVerificationScopes = [
      {
        condition: "startsWith(matrix.target, 'darwin-')",
        markers: ["codesign --verify", "xcrun stapler validate"],
      },
      {
        condition: "startsWith(matrix.target, 'windows-')",
        markers: ["Get-AuthenticodeSignature", "TimeStamperCertificate"],
      },
      {
        condition: "startsWith(matrix.target, 'linux-')",
        markers: [
          "install-appimage-validator.sh",
          "cmclient-appimage-tools/validate",
        ],
      },
    ];
    const platformVerificationIndexes = platformVerificationScopes.map(
      ({ markers }) => steps.findIndex((step) => hasRunMarkers(step, markers)),
    );
    if (
      receiptIndex < 0 ||
      platformVerificationIndexes.some(
        (index) => index <= exactTagIndex || index >= receiptIndex,
      ) ||
      !isFailClosedStep(steps[receiptIndex]) ||
      !platformVerificationIndexes.every(
        (index, scopeIndex) =>
          steps[index]["continue-on-error"] !== true &&
          normalizeWorkflowCondition(steps[index].if) ===
            platformVerificationScopes[scopeIndex].condition,
      )
    ) {
      violations.push({
        code: "RELEASE_PLATFORM_SIGNING_GATE_INVALID",
        path,
        detail: "platform-sign-native:trust-receipt",
      });
    }
  }

  const finalizer = isRecord(jobs["finalize-platform-signed"])
    ? jobs["finalize-platform-signed"]
    : undefined;
  if (!finalizer) {
    violations.push({
      code: "RELEASE_PLATFORM_FINALIZATION_GATE_INVALID",
      path,
      detail: "finalize-platform-signed:missing",
    });
  } else {
    const steps = workflowSteps(finalizer);
    const exactTagIndex = steps.findIndex((step) =>
      hasRunMarkers(step, [
        '[[ "$GITHUB_REF" == "refs/tags/v$version" ]]',
        "RELEASE_TAG_VERSION_MISMATCH",
      ]),
    );
    const unsignedDownloadIndex = findArtifactDownloadIndex(
      steps,
      "cmclient-supply-chain-unsigned-*",
    );
    const signedDownloadIndex = findArtifactDownloadIndex(
      steps,
      "cmclient-platform-signed-*",
    );
    const finalizationIndex = steps.findIndex((step) =>
      hasRunMarkers(step, [
        "release-supply-chain.mjs finalize-platform-signed",
        "release-supply-chain.mjs verify",
        "--require-platform-signing",
      ]),
    );
    const uploadIndex = findArtifactUploadIndex(
      steps,
      "cmclient-supply-chain-platform-signed-${{ steps.identity.outputs.version }}",
    );
    if (
      !sameStringSet(finalizer.needs, [
        "supply-chain",
        "final-windows-service-smoke",
        "platform-sign-native",
      ]) ||
      !isProductionReleaseJob(finalizer, PRODUCTION_RELEASE_CONDITION) ||
      exactTagIndex < 0 ||
      unsignedDownloadIndex <= exactTagIndex ||
      signedDownloadIndex <= exactTagIndex ||
      finalizationIndex <=
        Math.max(unsignedDownloadIndex, signedDownloadIndex) ||
      uploadIndex <= finalizationIndex ||
      ![
        exactTagIndex,
        unsignedDownloadIndex,
        signedDownloadIndex,
        finalizationIndex,
        uploadIndex,
      ].every((index) => index >= 0 && isFailClosedStep(steps[index]))
    ) {
      violations.push({
        code: "RELEASE_PLATFORM_FINALIZATION_GATE_INVALID",
        path,
        detail: "finalize-platform-signed:policy",
      });
    }
  }

  const attest = isRecord(jobs.attest) ? jobs.attest : undefined;
  if (!attest) {
    violations.push({
      code: "RELEASE_ATTESTATION_GATE_INVALID",
      path,
      detail: "attest:missing",
    });
  } else {
    const permissions = isRecord(attest.permissions) ? attest.permissions : {};
    const steps = workflowSteps(attest);
    const downloadIndex = findArtifactDownloadIndex(
      steps,
      "cmclient-supply-chain-platform-signed-*",
    );
    const verificationIndex = steps.findIndex((step) =>
      hasRunMarkers(step, [
        '[[ "$GITHUB_REF" == "refs/tags/v$version" ]]',
        "RELEASE_TAG_VERSION_MISMATCH",
        "release-supply-chain.mjs verify",
        "--require-platform-signing",
      ]),
    );
    const signingIndexes = steps
      .map((step, index) => ({ index, step }))
      .filter(
        ({ step }) =>
          hasRunMarkers(step, ["cosign sign-blob"]) ||
          (typeof step.uses === "string" &&
            step.uses.startsWith("actions/attest-build-provenance@")),
      )
      .map(({ index }) => index);
    if (
      !sameStringSet(attest.needs, ["finalize-platform-signed"]) ||
      !isProductionReleaseJob(attest, PRODUCTION_RELEASE_CONDITION) ||
      permissions.contents !== "read" ||
      permissions["id-token"] !== "write" ||
      permissions.attestations !== "write" ||
      downloadIndex < 0 ||
      verificationIndex <= downloadIndex ||
      signingIndexes.length !== 2 ||
      signingIndexes.some((index) => index <= verificationIndex) ||
      ![downloadIndex, verificationIndex, ...signingIndexes].every((index) =>
        isFailClosedStep(steps[index]),
      )
    ) {
      violations.push({
        code: "RELEASE_ATTESTATION_GATE_INVALID",
        path,
        detail: "attest:policy",
      });
    }
  }

  const supplyChain = isRecord(jobs["supply-chain"])
    ? jobs["supply-chain"]
    : undefined;
  const supplyChainSteps = supplyChain ? workflowSteps(supplyChain) : [];
  const sbomInstallerIndex = supplyChainSteps.findIndex(
    (step) =>
      typeof step.run === "string" &&
      step.run.trim() ===
        'bash scripts/install-sbom-tool.sh "$RUNNER_TEMP/cmclient-sbom-tools"',
  );
  const sbomIndex = supplyChainSteps.findIndex(
    (step) =>
      typeof step.run === "string" &&
      normalizeShellCommand(step.run) ===
        '"$RUNNER_TEMP/cmclient-sbom-tools/syft" scan dir:release-build --output "spdx-json=release-dist/cmclient-${{ steps.release.outputs.version }}.spdx.json"',
  );
  if (
    sbomInstallerIndex < 0 ||
    sbomIndex < 0 ||
    sbomInstallerIndex >= sbomIndex ||
    !isFailClosedStep(supplyChainSteps[sbomInstallerIndex]) ||
    !isFailClosedStep(supplyChainSteps[sbomIndex])
  ) {
    violations.push({
      code: "RELEASE_SBOM_INPUT_INVALID",
      path,
      detail: "supply-chain:sbom",
    });
  }

  const signing = isRecord(jobs["sign-update-manifest"])
    ? jobs["sign-update-manifest"]
    : undefined;
  if (!signing) {
    violations.push({
      code: "RELEASE_MANIFEST_SIGNING_GATE_INVALID",
      path,
      detail: "sign-update-manifest:missing",
    });
  } else {
    const steps = workflowSteps(signing);
    const downloadIndex = findArtifactDownloadIndex(
      steps,
      "cmclient-supply-chain-attested",
    );
    const verificationIndex = steps.findIndex((step) =>
      hasRunMarkers(step, [
        '[[ "$GITHUB_REF" == "refs/tags/v$version" ]]',
        "RELEASE_TAG_VERSION_MISMATCH",
        "release-supply-chain.mjs verify",
        "--require-platform-signing",
        "cosign verify-blob",
      ]),
    );
    const secretIndexes = steps
      .map((step, index) => ({ index, names: workflowSecretNames(step), step }))
      .filter(({ names }) => names.includes("CMCLIENT_UPDATE_SIGNING_KEY"));
    const secretEntry = secretIndexes[0];
    const secretStep = secretEntry?.step;
    if (
      !sameStringSet(signing.needs, ["attest"]) ||
      !isProductionReleaseJob(signing, UPDATE_MANIFEST_CONDITION) ||
      downloadIndex < 0 ||
      verificationIndex <= downloadIndex ||
      secretIndexes.length !== 1 ||
      secretEntry.index <= verificationIndex ||
      !isFailClosedStep(steps[downloadIndex]) ||
      !isFailClosedStep(steps[verificationIndex]) ||
      !isFailClosedStep(secretStep) ||
      !sameStringSet(workflowSecretNames(secretStep), [
        "CMCLIENT_UPDATE_SIGNING_KEY",
      ]) ||
      !hasExactSecretEnvBinding(secretStep, "CMCLIENT_UPDATE_SIGNING_KEY") ||
      typeof secretStep.run !== "string" ||
      !secretStep.run.includes(
        "release-supply-chain.mjs sign-update-manifest",
      ) ||
      typeof secretStep.uses === "string"
    ) {
      violations.push({
        code: "RELEASE_MANIFEST_SIGNING_GATE_INVALID",
        path,
        detail: "sign-update-manifest:policy",
      });
    }
  }

  return violations;
}

function auditSecurityJob(violations, path, document, jobName, code) {
  const jobs = isRecord(document.jobs) ? document.jobs : {};
  const job = isRecord(jobs[jobName]) ? jobs[jobName] : undefined;
  if (!job) {
    violations.push({ code, path, detail: `${jobName}:missing` });
    return;
  }
  const steps = workflowSteps(job);
  const requiredCommands = [
    "pnpm install --frozen-lockfile --ignore-scripts",
    "pnpm audit:policy",
    "pnpm audit --prod --audit-level high",
    "pnpm audit --audit-level high",
    "pnpm audit signatures",
    'bash scripts/install-security-audit-tools.sh "$RUNNER_TEMP/cmclient-security-tools"',
    '"$RUNNER_TEMP/cmclient-security-tools/actionlint" .github/workflows/*.yml',
    '"$RUNNER_TEMP/cmclient-security-tools/gitleaks" dir --redact --no-banner --no-color .',
    '"$RUNNER_TEMP/cmclient-security-tools/cargo-audit" audit --deny unsound --ignore RUSTSEC-2024-0429',
  ];
  if (
    job["runs-on"] !== "ubuntu-22.04" ||
    job["continue-on-error"] === true ||
    Object.hasOwn(job, "if")
  ) {
    violations.push({ code, path, detail: `${jobName}:job-policy` });
  }
  for (const command of requiredCommands) {
    const step = steps.find(
      (candidate) =>
        typeof candidate.run === "string" && candidate.run.trim() === command,
    );
    if (
      !step ||
      step["continue-on-error"] === true ||
      Object.hasOwn(step, "if")
    ) {
      violations.push({
        code,
        path,
        detail: `${jobName}:required-step:${command}`,
      });
    }
  }
  if (
    jobName === "security-gate" &&
    !sameStringSet(job.needs, ["artifact-plan"])
  ) {
    violations.push({ code, path, detail: `${jobName}:needs` });
  }
}

export function auditPackageManifest(path, bytes) {
  let manifest;
  try {
    manifest = JSON.parse(bytes.toString("utf8"));
  } catch {
    return [{ code: "DEPENDENCY_MANIFEST_INVALID", path }];
  }
  const violations = [];
  for (const section of DEPENDENCY_SECTIONS) {
    for (const [name, specifier] of Object.entries(manifest[section] ?? {})) {
      if (
        typeof specifier !== "string" ||
        !ALLOWED_DEPENDENCY_SPECIFIER.test(specifier)
      ) {
        violations.push({
          code: "DEPENDENCY_SPECIFIER_UNSAFE",
          path,
          detail: `${section}:${name}`,
        });
      }
    }
  }
  for (const name of Object.keys(manifest.scripts ?? {})) {
    if (FORBIDDEN_LIFECYCLE_SCRIPTS.has(name)) {
      violations.push({
        code: "DEPENDENCY_LIFECYCLE_SCRIPT_FORBIDDEN",
        path,
        detail: name,
      });
    }
  }
  if (path === "package.json") {
    if (
      manifest.private !== true ||
      manifest.packageManager !== "pnpm@11.9.0"
    ) {
      violations.push({ code: "DEPENDENCY_ROOT_IDENTITY_INVALID", path });
    }
    if (manifest.engines?.node !== "^22.18.0 || >=24.11.0") {
      violations.push({ code: "DEPENDENCY_NODE_BASELINE_INVALID", path });
    }
  }
  return violations;
}

export function auditSecurityToolInstaller(path, installer) {
  const violations = [];
  for (const pattern of SECURITY_TOOL_INSTALLER_PATTERNS) {
    requirePattern(
      violations,
      path,
      installer,
      pattern,
      "SECURITY_TOOL_INSTALLER_INVALID",
    );
  }
  requireSourceDigest(
    violations,
    path,
    installer,
    SECURITY_TOOL_INSTALLER_SHA256,
    "SECURITY_TOOL_INSTALLER_INVALID",
  );
  return violations;
}

export function auditSbomToolInstaller(path, installer) {
  const violations = [];
  for (const pattern of SBOM_TOOL_INSTALLER_PATTERNS) {
    requirePattern(
      violations,
      path,
      installer,
      pattern,
      "SBOM_TOOL_INSTALLER_INVALID",
    );
  }
  requireSourceDigest(
    violations,
    path,
    installer,
    SBOM_TOOL_INSTALLER_SHA256,
    "SBOM_TOOL_INSTALLER_INVALID",
  );
  return violations;
}

export function auditAppImageValidatorInstaller(path, installer) {
  const violations = [];
  for (const pattern of APPIMAGE_VALIDATOR_INSTALLER_PATTERNS) {
    requirePattern(
      violations,
      path,
      installer,
      pattern,
      "APPIMAGE_VALIDATOR_INSTALLER_INVALID",
    );
  }
  requireSourceDigest(
    violations,
    path,
    installer,
    APPIMAGE_VALIDATOR_INSTALLER_SHA256,
    "APPIMAGE_VALIDATOR_INSTALLER_INVALID",
  );
  return violations;
}

export function auditPnpmWorkspace(path, source) {
  let document;
  try {
    document = parseYaml(source);
  } catch {
    return [{ code: "NODE_INSTALL_POLICY_INVALID", path }];
  }
  if (!isRecord(document)) {
    return [{ code: "NODE_INSTALL_POLICY_INVALID", path }];
  }
  const violations = [];
  const allowedKeys = new Set([
    "allowBuilds",
    "blockExoticSubdeps",
    "injectWorkspacePackages",
    "minimumReleaseAge",
    "minimumReleaseAgeIgnoreMissingTime",
    "overrides",
    "packages",
    "trustLockfile",
    "trustPolicy",
  ]);
  const allowBuilds = isRecord(document.allowBuilds)
    ? document.allowBuilds
    : {};
  const overrides = isRecord(document.overrides) ? document.overrides : {};
  const overridesMatch =
    Object.keys(overrides).length ===
      Object.keys(REQUIRED_PNPM_OVERRIDES).length &&
    Object.entries(REQUIRED_PNPM_OVERRIDES).every(
      ([name, version]) => overrides[name] === version,
    );
  if (
    !sameStringSet(document.packages, ["apps/*", "packages/*"]) ||
    Object.keys(allowBuilds).length !== 1 ||
    allowBuilds["@serialport/bindings-cpp"] !== true ||
    !overridesMatch ||
    document.minimumReleaseAge !== 1_440 ||
    document.minimumReleaseAgeIgnoreMissingTime !== false ||
    document.trustPolicy !== "no-downgrade" ||
    document.trustLockfile !== false ||
    document.blockExoticSubdeps !== true ||
    document.injectWorkspacePackages !== true
  ) {
    violations.push({ code: "NODE_INSTALL_POLICY_INVALID", path });
  }
  for (const key of Object.keys(document)) {
    if (!allowedKeys.has(key)) {
      violations.push({
        code: "NODE_INSTALL_POLICY_UNREVIEWED_SETTING",
        path,
        detail: key,
      });
    }
  }
  return violations;
}

export function auditPnpmLock(path, source) {
  let document;
  try {
    document = parseYaml(source);
  } catch {
    return [{ code: "NODE_DEPENDENCY_SECURITY_LOCK_INVALID", path }];
  }
  if (!isRecord(document)) {
    return [{ code: "NODE_DEPENDENCY_SECURITY_LOCK_INVALID", path }];
  }

  const violations = [];
  const overrides = isRecord(document.overrides) ? document.overrides : {};
  if (
    Object.keys(overrides).length !==
      Object.keys(REQUIRED_PNPM_OVERRIDES).length ||
    !Object.entries(REQUIRED_PNPM_OVERRIDES).every(
      ([name, version]) => overrides[name] === version,
    )
  ) {
    violations.push({
      code: "NODE_DEPENDENCY_SECURITY_LOCK_INVALID",
      path,
      detail: "overrides",
    });
  }

  for (const sectionName of ["packages", "snapshots"]) {
    const section = isRecord(document[sectionName])
      ? document[sectionName]
      : {};
    const versions = Object.keys(section)
      .map((key) => /^fast-uri@([^()]+)$/u.exec(key)?.[1])
      .filter((version) => version !== undefined);
    if (!sameStringSet(versions, REQUIRED_FAST_URI_VERSIONS)) {
      violations.push({
        code: "NODE_DEPENDENCY_SECURITY_LOCK_INVALID",
        path,
        detail: sectionName,
      });
    }
  }

  const dependencyVersions = [];
  collectPropertyValues(document, "fast-uri", dependencyVersions);
  if (
    dependencyVersions.length === 0 ||
    dependencyVersions.some(
      (version) =>
        typeof version !== "string" ||
        !REQUIRED_FAST_URI_VERSIONS.includes(version),
    ) ||
    !REQUIRED_FAST_URI_VERSIONS.every((version) =>
      dependencyVersions.includes(version),
    )
  ) {
    violations.push({
      code: "NODE_DEPENDENCY_SECURITY_LOCK_INVALID",
      path,
      detail: "dependency-edges",
    });
  }
  return violations;
}

export function auditProductionDeploy(path, source) {
  const expected = LOCKED_PRODUCTION_DEPLOYS.get(path);
  if (!expected) {
    return [{ code: "NODE_PRODUCTION_DEPLOY_POLICY_INVALID", path }];
  }
  const normalized = normalizeShellCommand(source);
  const deployMarker = "@cmclient/gateway deploy";
  const deployCount = normalized.split(deployMarker).length - 1;
  if (
    deployCount !== 1 ||
    !normalized.includes(expected) ||
    normalized.includes("--legacy")
  ) {
    return [{ code: "NODE_PRODUCTION_DEPLOY_POLICY_INVALID", path }];
  }
  return [];
}

export function auditRustSecWaivers(
  path,
  source,
  cargoLock,
  cargoMetadata,
  now = new Date(),
) {
  let document;
  try {
    document = JSON.parse(source.toString());
  } catch {
    return [{ code: "RUSTSEC_WAIVER_INVALID", path }];
  }
  const violations = [];
  const waiver =
    isRecord(document) &&
    document.schemaVersion === 1 &&
    Array.isArray(document.waivers) &&
    document.waivers.length === 1 &&
    isRecord(document.waivers[0])
      ? document.waivers[0]
      : undefined;
  if (
    !waiver ||
    Object.entries(RUSTSEC_WAIVER).some(
      ([key, value]) => waiver[key] !== value,
    ) ||
    typeof waiver.reason !== "string" ||
    waiver.reason.trim().length < 20
  ) {
    violations.push({ code: "RUSTSEC_WAIVER_INVALID", path });
    return violations;
  }

  const expiry = Date.parse(`${waiver.expiresOn}T23:59:59.999Z`);
  if (!Number.isFinite(expiry) || now.getTime() > expiry) {
    violations.push({
      code: "RUSTSEC_WAIVER_EXPIRED",
      path,
      detail: waiver.expiresOn,
    });
  }
  const versions = cargoLockVersions(cargoLock, waiver.package);
  if (versions.length !== 1 || versions[0] !== waiver.version) {
    violations.push({
      code: "RUSTSEC_WAIVER_PACKAGE_MISMATCH",
      path: "Cargo.lock",
      detail: `${waiver.package}@${versions.join(",") || "missing"}`,
    });
  }
  if (!rustsecDependencyPathValid(cargoMetadata, waiver)) {
    violations.push({
      code: "RUSTSEC_WAIVER_DEPENDENCY_PATH_INVALID",
      path,
    });
  }
  return violations;
}

export async function auditTrackedRepository() {
  const entries = trackedEntries();
  const trackedPaths = new Set(entries.map(({ path }) => path));
  const violations = [];
  let workflowActions = 0;
  for (const required of REQUIRED_TRACKED_FILES) {
    if (!trackedPaths.has(required)) {
      violations.push({
        code: "SECURITY_REQUIRED_FILE_MISSING",
        path: required,
      });
    }
  }

  for (const entry of entries) {
    if (!["100644", "100755"].includes(entry.mode)) {
      violations.push({
        code: "REPOSITORY_SPECIAL_MODE_FORBIDDEN",
        path: entry.path,
      });
      continue;
    }
    const bytes = await readFile(entry.path);
    violations.push(...scanSecretEntry(entry.path, bytes));
    if (/(?:^|\/)package\.json$/u.test(entry.path)) {
      violations.push(...auditPackageManifest(entry.path, bytes));
    }
    if (entry.path === "Dockerfile") {
      violations.push(
        ...auditProductionDeploy(entry.path, bytes.toString("utf8")),
      );
    }
    if (/^\.github\/workflows\/[^/]+\.ya?ml$/u.test(entry.path)) {
      const result = auditWorkflow(entry.path, bytes.toString("utf8"));
      workflowActions += result.actions;
      violations.push(...result.violations);
    }
  }

  const [
    cargo,
    cargoLock,
    pnpmWorkspace,
    pnpmLock,
    installer,
    sbomInstaller,
    appImageValidatorInstaller,
    rustsecWaivers,
  ] = await Promise.all([
    readFile("Cargo.toml", "utf8"),
    readFile("Cargo.lock", "utf8"),
    readFile("pnpm-workspace.yaml", "utf8"),
    readFile("pnpm-lock.yaml", "utf8"),
    readFile("scripts/install-security-audit-tools.sh", "utf8"),
    readFile("scripts/install-sbom-tool.sh", "utf8"),
    readFile("scripts/install-appimage-validator.sh", "utf8"),
    readFile("security/rustsec-waivers.json", "utf8"),
  ]);

  requirePattern(
    violations,
    "Cargo.toml",
    cargo,
    /unsafe_code\s*=\s*"forbid"/u,
    "RUST_UNSAFE_POLICY_MISSING",
  );
  for (const source of cargoLockSources(cargoLock)) {
    if (source !== "registry+https://github.com/rust-lang/crates.io-index") {
      violations.push({
        code: "RUST_DEPENDENCY_SOURCE_FORBIDDEN",
        path: "Cargo.lock",
      });
    }
  }
  if (/(?:git\+|github\.com\/[^\s}]+(?:#|\.git)|tarball:)/iu.test(pnpmLock)) {
    violations.push({
      code: "NODE_DEPENDENCY_SOURCE_FORBIDDEN",
      path: "pnpm-lock.yaml",
    });
  }
  violations.push(...auditPnpmLock("pnpm-lock.yaml", pnpmLock));
  violations.push(...auditPnpmWorkspace("pnpm-workspace.yaml", pnpmWorkspace));
  const cargoMetadata = rustsecDependencyMetadata();
  violations.push(
    ...auditRustSecWaivers(
      "security/rustsec-waivers.json",
      rustsecWaivers,
      cargoLock,
      cargoMetadata,
    ),
  );
  violations.push(
    ...auditSecurityToolInstaller(
      "scripts/install-security-audit-tools.sh",
      installer,
    ),
  );
  violations.push(
    ...auditSbomToolInstaller("scripts/install-sbom-tool.sh", sbomInstaller),
  );
  violations.push(
    ...auditAppImageValidatorInstaller(
      "scripts/install-appimage-validator.sh",
      appImageValidatorInstaller,
    ),
  );

  return {
    trackedFiles: entries.length,
    workflowActions,
    violations: violations.sort(compareViolation),
  };
}

export function repositoryEntries(cwd = process.cwd()) {
  const worktreeDeletions = new Set(
    gitList(["ls-files", "--deleted", "-z"], cwd).split("\0").filter(Boolean),
  );
  const tracked = gitList(["ls-files", "--stage", "-z"], cwd)
    .split("\0")
    .filter(Boolean)
    .map((entry) => {
      const match = entry.match(/^(\d+) [a-f0-9]+ \d+\t([\s\S]+)$/u);
      if (!match) {
        throw new Error("SECURITY_AUDIT_GIT_ENTRY_INVALID");
      }
      return { mode: match[1], path: match[2] };
    })
    .filter(({ path }) => !worktreeDeletions.has(path));
  const untracked = gitList(
    ["ls-files", "--others", "--exclude-standard", "-z"],
    cwd,
  )
    .split("\0")
    .filter(Boolean)
    .map((path) => ({ mode: "100644", path }));

  return [
    ...new Map(
      [...tracked, ...untracked].map((entry) => [entry.path, entry]),
    ).values(),
  ].sort((left, right) => left.path.localeCompare(right.path));
}

function trackedEntries() {
  return repositoryEntries();
}

function gitList(args, cwd) {
  const result = spawnSync("git", args, {
    cwd,
    encoding: "buffer",
    maxBuffer: 32 * 1024 * 1024,
  });
  if (result.status !== 0) {
    throw new Error("SECURITY_AUDIT_GIT_LIST_FAILED");
  }
  return result.stdout.toString("utf8");
}

function cargoLockSources(lockfile) {
  return [...lockfile.matchAll(/^source = "([^"]+)"$/gmu)].map(
    (match) => match[1],
  );
}

function cargoLockVersions(lockfile, packageName) {
  const versions = [];
  for (const match of lockfile.matchAll(
    /^\[\[package\]\]\n([\s\S]*?)(?=^\[\[package\]\]|(?![\s\S]))/gmu,
  )) {
    const block = match[1] ?? "";
    const name = block.match(/^name = "([^"]+)"$/mu)?.[1];
    const version = block.match(/^version = "([^"]+)"$/mu)?.[1];
    if (name === packageName && version) {
      versions.push(version);
    }
  }
  return [...new Set(versions)].sort();
}

function rustsecDependencyMetadata() {
  const result = spawnSync(
    "cargo",
    [
      "metadata",
      "--format-version",
      "1",
      "--locked",
      "--filter-platform",
      RUSTSEC_WAIVER_PLATFORM,
    ],
    { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  );
  if (result.status !== 0) {
    return undefined;
  }
  try {
    return JSON.parse(result.stdout);
  } catch {
    return undefined;
  }
}

function rustsecDependencyPathValid(metadata, waiver) {
  if (
    !isRecord(metadata) ||
    !Array.isArray(metadata.packages) ||
    !Array.isArray(metadata.workspace_members) ||
    !isRecord(metadata.resolve) ||
    !Array.isArray(metadata.resolve.nodes)
  ) {
    return false;
  }
  const packages = new Map(
    metadata.packages
      .filter(isRecord)
      .filter((item) => typeof item.id === "string")
      .map((item) => [item.id, item]),
  );
  const targets = [...packages.entries()].filter(
    ([, item]) =>
      item.name === waiver.package && item.version === waiver.version,
  );
  if (targets.length !== 1) {
    return false;
  }
  const targetId = targets[0][0];
  const blocked = new Set(
    [...packages.entries()]
      .filter(([, item]) => item.name === waiver.transitiveRoot)
      .map(([id]) => id),
  );
  if (blocked.size === 0) {
    return false;
  }
  const dependencies = new Map(
    metadata.resolve.nodes
      .filter(isRecord)
      .filter((node) => typeof node.id === "string")
      .map((node) => [
        node.id,
        Array.isArray(node.dependencies)
          ? node.dependencies.filter((id) => typeof id === "string")
          : [],
      ]),
  );

  for (const workspaceId of metadata.workspace_members) {
    if (typeof workspaceId !== "string") {
      return false;
    }
    const workspacePackage = packages.get(workspaceId);
    if (!workspacePackage) {
      return false;
    }
    const reachesTarget = dependencyReachable(
      dependencies,
      workspaceId,
      targetId,
      new Set(),
    );
    if (workspacePackage.name === "cmclient-desktop") {
      if (
        !reachesTarget ||
        dependencyReachable(dependencies, workspaceId, targetId, blocked)
      ) {
        return false;
      }
    } else if (reachesTarget) {
      return false;
    }
  }
  return true;
}

function dependencyReachable(dependencies, start, target, blocked) {
  const pending = [start];
  const visited = new Set();
  while (pending.length > 0) {
    const current = pending.pop();
    if (!current || visited.has(current) || blocked.has(current)) {
      continue;
    }
    if (current === target) {
      return true;
    }
    visited.add(current);
    pending.push(...(dependencies.get(current) ?? []));
  }
  return false;
}

function requirePattern(violations, path, value, pattern, code) {
  if (!pattern.test(value)) {
    violations.push({ code, path, detail: pattern.source });
  }
}

function requireSourceDigest(violations, path, value, expected, code) {
  const actual = createHash("sha256")
    .update(value.replaceAll("\r\n", "\n"))
    .digest("hex");
  if (actual !== expected) {
    violations.push({ code, path, detail: `source-sha256:${actual}` });
  }
}

function lineNumber(value, index) {
  return value.slice(0, index).split("\n").length;
}

function collectPropertyValues(value, propertyName, results) {
  if (Array.isArray(value)) {
    for (const item of value)
      collectPropertyValues(item, propertyName, results);
    return;
  }
  if (!isRecord(value)) return;
  for (const [name, item] of Object.entries(value)) {
    if (name === propertyName) results.push(item);
    collectPropertyValues(item, propertyName, results);
  }
}

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function workflowSteps(job) {
  return Array.isArray(job.steps) ? job.steps.filter(isRecord) : [];
}

function workflowTriggerNames(value) {
  if (typeof value === "string") {
    return [value];
  }
  if (Array.isArray(value)) {
    return value.filter((trigger) => typeof trigger === "string");
  }
  return isRecord(value) ? Object.keys(value) : [];
}

function workflowSecretExpressions(value) {
  if (typeof value === "string") {
    return [...value.matchAll(/\$\{\{(?:(?!\}\})[\s\S])*?\}\}/gu)]
      .map(([expression]) => expression)
      .filter((expression) => /\bsecrets\b/u.test(expression));
  }
  if (Array.isArray(value)) {
    return value.flatMap(workflowSecretExpressions);
  }
  return isRecord(value)
    ? Object.values(value).flatMap(workflowSecretExpressions)
    : [];
}

function releaseSigningSecretName(expression) {
  const match =
    /^\$\{\{\s*secrets(?:\.([A-Z0-9_]+)|\s*\[\s*['"]([A-Z0-9_]+)['"]\s*\])\s*\}\}$/u.exec(
      expression,
    );
  return match?.[1] ?? match?.[2];
}

function isAllowedReleaseSigningSecretExpression(expression) {
  const name = releaseSigningSecretName(expression);
  return name !== undefined && RELEASE_SIGNING_SECRET_SET.has(name);
}

function workflowSecretNames(value) {
  return workflowSecretExpressions(value)
    .map(releaseSigningSecretName)
    .filter((name) => name !== undefined);
}

function hasExactSecretEnvBinding(step, name, envName = name) {
  return (
    isRecord(step.env) && releaseSigningSecretName(step.env[envName]) === name
  );
}

function platformSecretScopesAreValid(job, steps, exactTagIndex) {
  const expectedNames = PLATFORM_SECRET_SCOPES.flatMap(({ names }) => names);
  if (
    !sameStringSet(workflowSecretNames(job), expectedNames) ||
    !sameStringSet(workflowSecretNames(steps), expectedNames)
  ) {
    return false;
  }
  for (const scope of PLATFORM_SECRET_SCOPES) {
    const matching = steps
      .map((step, index) => ({ index, names: workflowSecretNames(step), step }))
      .filter(({ names }) => names.some((name) => scope.names.includes(name)));
    if (matching.length !== 1) {
      return false;
    }
    const [{ index, names, step }] = matching;
    if (
      index <= exactTagIndex ||
      !sameStringSet(names, scope.names) ||
      normalizeWorkflowCondition(step.if) !== scope.condition ||
      step["continue-on-error"] === true ||
      !scope.names.every((name) =>
        hasExactSecretEnvBinding(step, name, scope.envNames?.[name] ?? name),
      ) ||
      !hasRunMarkers(step, scope.markers)
    ) {
      return false;
    }
  }
  return true;
}

function isProductionReleaseJob(job, expectedCondition) {
  return (
    job.environment === "production-release" &&
    job["continue-on-error"] !== true &&
    normalizeWorkflowCondition(job.if) === expectedCondition
  );
}

function normalizeWorkflowCondition(value) {
  return typeof value === "string" ? value.replace(/\s+/gu, " ").trim() : "";
}

function hasRunMarkers(step, markers) {
  return (
    typeof step.run === "string" &&
    markers.every((marker) => step.run.includes(marker))
  );
}

function findArtifactDownloadIndex(steps, pattern) {
  return steps.findIndex(
    (step) =>
      typeof step.uses === "string" &&
      step.uses.startsWith("actions/download-artifact@") &&
      isRecord(step.with) &&
      step.with.pattern === pattern,
  );
}

function findArtifactUploadIndex(steps, name) {
  return steps.findIndex(
    (step) =>
      typeof step.uses === "string" &&
      step.uses.startsWith("actions/upload-artifact@") &&
      isRecord(step.with) &&
      step.with.name === name,
  );
}

function isFailClosedStep(step) {
  return step["continue-on-error"] !== true && !Object.hasOwn(step, "if");
}

function normalizeShellCommand(value) {
  return value
    .replace(/\\\r?\n\s*/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

function isPinnedAction(value) {
  if (value.startsWith("docker://")) {
    return /^docker:\/\/[^\s@]+@sha256:[a-f0-9]{64}$/u.test(value);
  }
  return value.startsWith("./") || /^[^\s@]+@[a-f0-9]{40}$/u.test(value);
}

function sameStringSet(value, expected) {
  const actual = typeof value === "string" ? [value] : value;
  return (
    Array.isArray(actual) &&
    actual.every((entry) => typeof entry === "string") &&
    actual.length === expected.length &&
    [...actual]
      .sort()
      .every((entry, index) => entry === [...expected].sort()[index])
  );
}

function compareViolation(left, right) {
  return (
    left.path.localeCompare(right.path) ||
    left.code.localeCompare(right.code) ||
    (left.line ?? 0) - (right.line ?? 0)
  );
}

async function main() {
  const report = await auditTrackedRepository();
  if (report.violations.length > 0) {
    process.stderr.write(`${JSON.stringify(report, null, 2)}\n`);
    process.exitCode = 1;
    return;
  }
  process.stdout.write(
    `${JSON.stringify({
      status: "passed",
      trackedFiles: report.trackedFiles,
      workflowActions: report.workflowActions,
    })}\n`,
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  main().catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
