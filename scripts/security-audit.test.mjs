import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  auditPackageManifest,
  auditPnpmWorkspace,
  auditProductionDeploy,
  auditRustSecWaivers,
  auditSbomToolInstaller,
  auditSecurityToolInstaller,
  auditTrackedRepository,
  auditWorkflow,
  repositoryEntries,
  scanSecretEntry,
} from "./security-audit.mjs";

const checkoutSha = "a".repeat(40);

function workflow({
  action = `actions/checkout@${checkoutSha}`,
  persist = true,
  run = "echo verified",
} = {}) {
  return [
    "name: security fixture",
    "permissions:",
    "  contents: read",
    "jobs:",
    "  verify:",
    "    runs-on: ubuntu-22.04",
    "    steps:",
    `      - uses: ${action}`,
    ...(persist
      ? ["        with:", "          persist-credentials: false"]
      : []),
    `      - run: ${run}`,
  ].join("\n");
}

function violationCodes(violations) {
  return new Set(violations.map(({ code }) => code));
}

function rustsecMetadataFixture() {
  return {
    packages: [
      { id: "desktop", name: "cmclient-desktop", version: "2.0.0" },
      { id: "agent", name: "cmclient-agent", version: "2.0.0" },
      { id: "tauri", name: "tauri", version: "2.11.5" },
      { id: "glib", name: "glib", version: "0.18.5" },
    ],
    workspace_members: ["desktop", "agent"],
    resolve: {
      nodes: [
        { id: "desktop", dependencies: ["tauri"] },
        { id: "agent", dependencies: [] },
        { id: "tauri", dependencies: ["glib"] },
        { id: "glib", dependencies: [] },
      ],
    },
  };
}

test("secret scan rejects secret-bearing paths and provider credentials", () => {
  const githubToken = ["gh", "p_", "a".repeat(36)].join("");
  const npmToken = ["np", "m_", "b".repeat(36)].join("");
  const awsAccessKey = ["AK", "IA", "C".repeat(16)].join("");
  const input = Buffer.from(
    ["ordinary line", githubToken, npmToken, awsAccessKey].join("\n"),
  );

  const violations = scanSecretEntry("config/secrets.json", input);
  assert.deepEqual(
    violationCodes(violations),
    new Set([
      "SECRET_BEARING_PATH",
      "SECRET_GITHUB_TOKEN",
      "SECRET_NPM_TOKEN",
      "SECRET_AWS_ACCESS_KEY",
    ]),
  );
  assert.equal(
    violations.find(({ code }) => code === "SECRET_GITHUB_TOKEN")?.line,
    2,
  );
});

test("secret scan rejects generated artifacts and dependency hooks", () => {
  const cases = [
    ["config/.envrc", "SECRET_BEARING_PATH"],
    ["config/runtime.env.production", "SECRET_BEARING_PATH"],
    ["state/cache.sqlite3", "GENERATED_OR_SENSITIVE_ARTIFACT_PATH"],
    ["dist/release.tar.zst", "GENERATED_OR_SENSITIVE_ARTIFACT_PATH"],
    ["logs/runtime.log", "GENERATED_OR_SENSITIVE_ARTIFACT_PATH"],
    [".pnpmfile.cjs", "DEPENDENCY_RESOLUTION_HOOK_FORBIDDEN"],
  ];
  for (const [path, code] of cases) {
    assert.ok(
      violationCodes(scanSecretEntry(path, Buffer.from("fixture"))).has(code),
      path,
    );
  }
});

test("secret scan does not skip provider credentials in NUL-containing entries", () => {
  const githubToken = ["gh", "p_", "n".repeat(36)].join("");
  const input = Buffer.concat([
    Buffer.from(`binary prefix\0${githubToken}`),
    Buffer.from([0]),
  ]);

  assert.ok(
    violationCodes(scanSecretEntry("docs/malformed.md", input)).has(
      "SECRET_GITHUB_TOKEN",
    ),
  );
});

test("workflow audit accepts a SHA-pinned checkout with disabled credential persistence", () => {
  const result = auditWorkflow(".github/workflows/good.yml", workflow());

  assert.equal(result.actions, 1);
  assert.deepEqual(result.violations, []);
});

test("workflow audit rejects mutable actions and checkout credential persistence", () => {
  const mutable = auditWorkflow(
    ".github/workflows/mutable.yml",
    workflow({ action: "actions/checkout@v7" }),
  );
  const persistent = auditWorkflow(
    ".github/workflows/persistent.yml",
    workflow({ persist: false }),
  );

  assert.ok(
    violationCodes(mutable.violations).has("WORKFLOW_ACTION_NOT_SHA_PINNED"),
  );
  assert.ok(
    violationCodes(persistent.violations).has(
      "WORKFLOW_CHECKOUT_PERSISTS_CREDENTIALS",
    ),
  );
});

test("workflow audit rejects direct secret interpolation in a run script", () => {
  const expression = ["${{", " secrets.", "SIGNING_KEY", " }}"].join("");
  const result = auditWorkflow(
    ".github/workflows/interpolation.yml",
    workflow({ run: `printf '%s' '${expression}'` }),
  );

  assert.ok(
    violationCodes(result.violations).has(
      "WORKFLOW_SECRET_INTERPOLATED_IN_SCRIPT",
    ),
  );
});

test("workflow audit parses permissions, checkout inputs, and reusable jobs", () => {
  const checkoutSpoof = workflow({ persist: false }).replace(
    "    steps:",
    "    # persist-credentials: false\n    steps:",
  );
  const elevated = workflow().replace(
    "    runs-on: ubuntu-22.04",
    "    runs-on: ubuntu-22.04\n    permissions:\n      actions: write",
  );
  const reusable = [
    "name: reusable fixture",
    "permissions:",
    "  contents: read",
    "jobs:",
    "  delegated:",
    "    uses: owner/workflow@v1",
    "    secrets: inherit",
  ].join("\n");
  const dockerTag = workflow({
    action: `docker://registry.example.invalid/image@${"a".repeat(40)}`,
  });

  assert.ok(
    violationCodes(
      auditWorkflow(".github/workflows/spoof.yml", checkoutSpoof).violations,
    ).has("WORKFLOW_CHECKOUT_PERSISTS_CREDENTIALS"),
  );
  assert.ok(
    violationCodes(
      auditWorkflow(".github/workflows/elevated.yml", elevated).violations,
    ).has("WORKFLOW_EXCESS_PERMISSION"),
  );
  const reusableCodes = violationCodes(
    auditWorkflow(".github/workflows/reusable.yml", reusable).violations,
  );
  assert.ok(reusableCodes.has("WORKFLOW_ACTION_NOT_SHA_PINNED"));
  assert.ok(reusableCodes.has("WORKFLOW_REUSABLE_SECRETS_INHERITED"));
  assert.ok(
    violationCodes(
      auditWorkflow(".github/workflows/docker.yml", dockerTag).violations,
    ).has("WORKFLOW_ACTION_NOT_SHA_PINNED"),
  );
});

test("workflow audit rejects bracket secret interpolation", () => {
  const expression = ["${{", "secrets['SIGNING_KEY']", "}}"].join("");
  const result = auditWorkflow(
    ".github/workflows/bracket.yml",
    workflow({ run: `printf '%s' "${expression}"` }),
  );
  assert.ok(
    violationCodes(result.violations).has(
      "WORKFLOW_SECRET_INTERPOLATED_IN_SCRIPT",
    ),
  );
});

test("workflow audit rejects every valid pull_request_target trigger form", () => {
  for (const trigger of [
    "on: pull_request_target",
    "on: [push, pull_request_target]",
    "on:\n  pull_request_target:",
  ]) {
    const result = auditWorkflow(
      ".github/workflows/trigger.yml",
      workflow().replace("jobs:", `${trigger}\njobs:`),
    );
    assert.ok(
      violationCodes(result.violations).has(
        "WORKFLOW_UNTRUSTED_PRIVILEGED_TRIGGER",
      ),
      trigger,
    );
  }
});

test("workflow audit rejects broad and computed secret context exposure", () => {
  const expressions = [
    ["${{", " toJSON(secrets)", " }}"].join(""),
    ["${{", " secrets[vars.SECRET_NAME]", " }}"].join(""),
  ];
  for (const expression of expressions) {
    const result = auditWorkflow(
      ".github/workflows/secret-context.yml",
      workflow({ run: `printf '%s' '${expression}'` }),
    );
    const codes = violationCodes(result.violations);
    assert.ok(codes.has("WORKFLOW_SECRET_CONTEXT_UNEXPECTED"), expression);
    assert.ok(codes.has("WORKFLOW_SECRET_INTERPOLATED_IN_SCRIPT"), expression);
  }
});

test("required workflow gates are structural and fail closed", async () => {
  const ci = await readFile(".github/workflows/ci.yml", "utf8");
  const release = await readFile(".github/workflows/release-build.yml", "utf8");
  assert.deepEqual(
    auditWorkflow(".github/workflows/ci.yml", ci).violations,
    [],
  );
  assert.deepEqual(
    auditWorkflow(".github/workflows/release-build.yml", release).violations,
    [],
  );

  const decoy = ci.replace("  security-audit:", "  security-decoy:");
  assert.ok(
    violationCodes(
      auditWorkflow(".github/workflows/ci.yml", decoy).violations,
    ).has("CI_SECURITY_GATE_MISSING"),
  );
  const ignoredAudit = ci.replace(
    "      - run: pnpm audit signatures",
    "      - run: pnpm audit signatures\n        continue-on-error: true",
  );
  assert.ok(
    violationCodes(
      auditWorkflow(".github/workflows/ci.yml", ignoredAudit).violations,
    ).has("CI_SECURITY_GATE_MISSING"),
  );
  const swallowedAudit = ci.replace(
    "      - run: pnpm audit signatures",
    "      - run: pnpm audit signatures || true",
  );
  assert.ok(
    violationCodes(
      auditWorkflow(".github/workflows/ci.yml", swallowedAudit).violations,
    ).has("CI_SECURITY_GATE_MISSING"),
  );
  const ignoredTag = release.replace(
    "      - name: Verify the exact immutable release tag",
    "      - name: Verify the exact immutable release tag\n        continue-on-error: true",
  );
  assert.ok(
    violationCodes(
      auditWorkflow(".github/workflows/release-build.yml", ignoredTag)
        .violations,
    ).has("RELEASE_ATTESTATION_GATE_INVALID"),
  );
  const swallowedSbom = release.replace(
    '            --output "spdx-json=release-dist/cmclient-${{ steps.release.outputs.version }}.spdx.json"',
    '            --output "spdx-json=release-dist/cmclient-${{ steps.release.outputs.version }}.spdx.json" || true',
  );
  assert.ok(
    violationCodes(
      auditWorkflow(".github/workflows/release-build.yml", swallowedSbom)
        .violations,
    ).has("RELEASE_SBOM_INPUT_INVALID"),
  );
  const signingSecret = [
    "${{",
    " secrets.",
    "CMCLIENT_UPDATE_SIGNING_KEY",
    " }}",
  ].join("");
  const duplicatedSigningSecret = release.replace(
    "  supply-chain:\n",
    `  supply-chain:\n    env:\n      LEAKED_SIGNING_KEY: ${signingSecret}\n`,
  );
  assert.ok(
    violationCodes(
      auditWorkflow(
        ".github/workflows/release-build.yml",
        duplicatedSigningSecret,
      ).violations,
    ).has("RELEASE_MANIFEST_SIGNING_GATE_INVALID"),
  );
  const allSecretsExpression = ["${{", " toJSON(secrets)", " }}"].join("");
  const earlySecretExposure = release.replace(
    "  supply-chain:\n",
    `  supply-chain:\n    env:\n      LEAKED_SECRETS: ${allSecretsExpression}\n`,
  );
  const earlySecretCodes = violationCodes(
    auditWorkflow(".github/workflows/release-build.yml", earlySecretExposure)
      .violations,
  );
  assert.ok(earlySecretCodes.has("WORKFLOW_SECRET_CONTEXT_UNEXPECTED"));
  assert.ok(earlySecretCodes.has("RELEASE_MANIFEST_SIGNING_GATE_INVALID"));
});

test("package audit rejects unsafe dependency sources and lifecycle scripts", () => {
  const manifest = Buffer.from(
    JSON.stringify({
      dependencies: {
        wildcard: "*",
        remote: "https://packages.example.invalid/archive.tgz",
        gitHttps: "git+https://github.com/example/package.git#deadbeef",
        gitSsh: "git+ssh://git@github.com/example/package.git#deadbeef",
        npmAlias: "npm:other-package@latest",
        mutableTag: "beta",
      },
      scripts: { postinstall: "node install.mjs" },
    }),
  );

  const violations = auditPackageManifest(
    "packages/fixture/package.json",
    manifest,
  );
  assert.deepEqual(
    violationCodes(violations),
    new Set([
      "DEPENDENCY_SPECIFIER_UNSAFE",
      "DEPENDENCY_LIFECYCLE_SCRIPT_FORBIDDEN",
    ]),
  );
  assert.equal(
    violations.filter(({ code }) => code === "DEPENDENCY_SPECIFIER_UNSAFE")
      .length,
    6,
  );
});

test("pnpm policy audit parses effective values and rejects spoofed settings", async () => {
  const source = await readFile("pnpm-workspace.yaml", "utf8");
  assert.deepEqual(auditPnpmWorkspace("pnpm-workspace.yaml", source), []);

  const spoofed = `# minimumReleaseAge: 1440\n${source.replace(
    "minimumReleaseAge: 1440",
    "minimumReleaseAge: 0",
  )}`;
  assert.ok(
    violationCodes(
      auditPnpmWorkspace(
        "pnpm-workspace.yaml",
        source.replace(
          "injectWorkspacePackages: true",
          "injectWorkspacePackages: false",
        ),
      ),
    ).has("NODE_INSTALL_POLICY_INVALID"),
  );
  assert.ok(
    violationCodes(auditPnpmWorkspace("pnpm-workspace.yaml", spoofed)).has(
      "NODE_INSTALL_POLICY_INVALID",
    ),
  );
  assert.ok(
    violationCodes(
      auditPnpmWorkspace(
        "pnpm-workspace.yaml",
        `${source}dangerouslyAllowAllBuilds: true\n`,
      ),
    ).has("NODE_INSTALL_POLICY_UNREVIEWED_SETTING"),
  );
});

test("production deploy policy rejects legacy and unlocked dependency resolution", async () => {
  const [dockerfile, workflowSource] = await Promise.all([
    readFile("Dockerfile", "utf8"),
    readFile(".github/workflows/release-build.yml", "utf8"),
  ]);
  assert.deepEqual(auditProductionDeploy("Dockerfile", dockerfile), []);
  assert.deepEqual(
    auditProductionDeploy(
      ".github/workflows/release-build.yml",
      workflowSource,
    ),
    [],
  );

  for (const [path, source] of [
    [
      "Dockerfile",
      dockerfile.replace(
        "deploy --prod --frozen-lockfile",
        "deploy --legacy --prod",
      ),
    ],
    [
      ".github/workflows/release-build.yml",
      workflowSource.replace(
        "--prod --frozen-lockfile release-build-input/gateway",
        "--legacy --prod release-build-input/gateway",
      ),
    ],
    [
      "Dockerfile",
      dockerfile.replace("deploy --prod --frozen-lockfile", "deploy --prod"),
    ],
  ]) {
    assert.ok(
      violationCodes(auditProductionDeploy(path, source)).has(
        "NODE_PRODUCTION_DEPLOY_POLICY_INVALID",
      ),
    );
  }
});

test("RustSec waiver is exact, path-bound, and time-bound", () => {
  const waiver = JSON.stringify({
    schemaVersion: 1,
    waivers: [
      {
        advisory: "RUSTSEC-2024-0429",
        package: "glib",
        version: "0.18.5",
        transitiveRoot: "tauri",
        expiresOn: "2026-10-19",
        reason: "Tauri Linux GTK transitive dependency fixture only",
      },
    ],
  });
  const lockfile = [
    "[[package]]",
    'name = "glib"',
    'version = "0.18.5"',
    "",
    "[[package]]",
    'name = "tauri"',
    'version = "2.11.5"',
  ].join("\n");
  const metadata = rustsecMetadataFixture();

  assert.deepEqual(
    auditRustSecWaivers(
      "security/rustsec-waivers.json",
      waiver,
      lockfile,
      metadata,
      new Date("2026-10-19T23:59:59.999Z"),
    ),
    [],
  );
  assert.ok(
    violationCodes(
      auditRustSecWaivers(
        "security/rustsec-waivers.json",
        waiver,
        lockfile,
        metadata,
        new Date("2026-10-20T00:00:00.000Z"),
      ),
    ).has("RUSTSEC_WAIVER_EXPIRED"),
  );
  assert.ok(
    violationCodes(
      auditRustSecWaivers(
        "security/rustsec-waivers.json",
        waiver,
        lockfile.replace("0.18.5", "0.18.6"),
        metadata,
        new Date("2026-07-19T00:00:00.000Z"),
      ),
    ).has("RUSTSEC_WAIVER_PACKAGE_MISMATCH"),
  );

  const directPath = JSON.parse(JSON.stringify(metadata));
  directPath.resolve.nodes
    .find(({ id }) => id === "desktop")
    .dependencies.push("glib");
  assert.ok(
    violationCodes(
      auditRustSecWaivers(
        "security/rustsec-waivers.json",
        waiver,
        lockfile,
        directPath,
        new Date("2026-07-19T00:00:00.000Z"),
      ),
    ).has("RUSTSEC_WAIVER_DEPENDENCY_PATH_INVALID"),
  );
});

test("security tool installer audit locks the complete reviewed source", async () => {
  const installer = await readFile(
    "scripts/install-security-audit-tools.sh",
    "utf8",
  );

  assert.deepEqual(
    auditSecurityToolInstaller(
      "scripts/install-security-audit-tools.sh",
      installer,
    ),
    [],
  );
  assert.deepEqual(
    auditSecurityToolInstaller(
      "scripts/install-security-audit-tools.sh",
      installer.replaceAll("\n", "\r\n"),
    ),
    [],
  );

  const unpinned = installer
    .replace('GITLEAKS_VERSION="8.30.1"', 'GITLEAKS_VERSION="latest"')
    .replace(/551f6fc8[0-9a-f]+/u, "unverified");
  assert.ok(
    violationCodes(
      auditSecurityToolInstaller(
        "scripts/install-security-audit-tools.sh",
        unpinned,
      ),
    ).has("SECURITY_TOOL_INSTALLER_INVALID"),
  );
  const glibcBound = installer.replaceAll(
    "x86_64-unknown-linux-musl",
    "x86_64-unknown-linux-gnu",
  );
  assert.ok(
    violationCodes(
      auditSecurityToolInstaller(
        "scripts/install-security-audit-tools.sh",
        glibcBound,
      ),
    ).has("SECURITY_TOOL_INSTALLER_INVALID"),
  );
  const overwritten = `${installer}\ncurl -fsSL https://example.invalid/tool -o "$destination/cargo-audit"\n`;
  assert.ok(
    violationCodes(
      auditSecurityToolInstaller(
        "scripts/install-security-audit-tools.sh",
        overwritten,
      ),
    ).has("SECURITY_TOOL_INSTALLER_INVALID"),
  );
});

test("SBOM installer audit locks the complete reviewed source", async () => {
  const installer = await readFile("scripts/install-sbom-tool.sh", "utf8");
  assert.deepEqual(
    auditSbomToolInstaller("scripts/install-sbom-tool.sh", installer),
    [],
  );
  assert.ok(
    violationCodes(
      auditSbomToolInstaller(
        "scripts/install-sbom-tool.sh",
        installer.replace("1.42.3", "latest"),
      ),
    ).has("SBOM_TOOL_INSTALLER_INVALID"),
  );
  assert.ok(
    violationCodes(
      auditSbomToolInstaller(
        "scripts/install-sbom-tool.sh",
        `${installer}\nprintf 'replacement' > "$destination/syft"\n`,
      ),
    ).has("SBOM_TOOL_INSTALLER_INVALID"),
  );
});

test("repository entries include tracked and untracked non-ignored files", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-security-audit-"));
  t.after(() => rm(directory, { force: true, recursive: true }));

  const init = spawnSync("git", ["init", "--quiet"], {
    cwd: directory,
    encoding: "utf8",
  });
  assert.equal(init.status, 0, init.stderr);
  await writeFile(join(directory, ".gitignore"), "*.ignored\n");
  await writeFile(join(directory, "tracked.txt"), "tracked\n");
  await writeFile(join(directory, "untracked.txt"), "untracked\n");
  await writeFile(join(directory, "local.ignored"), "ignored\n");
  const add = spawnSync("git", ["add", ".gitignore", "tracked.txt"], {
    cwd: directory,
    encoding: "utf8",
  });
  assert.equal(add.status, 0, add.stderr);

  assert.deepEqual(
    repositoryEntries(directory).map(({ path }) => path),
    [".gitignore", "tracked.txt", "untracked.txt"],
  );
});

test("repository security policy passes against the complete working tree", async () => {
  const report = await auditTrackedRepository();
  assert.deepEqual(report.violations, []);
  assert.ok(report.workflowActions > 0);
  assert.ok(report.trackedFiles > 300);
});
