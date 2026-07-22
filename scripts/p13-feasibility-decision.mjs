import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

export const DECISION_RELATIVE_PATH =
  "test/fixtures/p13-feasibility-decision.json";

const DEFAULT_AUTHORITATIVE_DOCUMENTS = Object.freeze([
  "docs/architecture/agent-runtime.md",
  "docs/architecture/gateway-runtime.md",
  "docs/architecture/CMCLIENT_2_OVERVIEW.md",
]);

const IMMUTABLE_GRAPH_DOCUMENT = "scripts/unified-task-graph-lock.json";

const NATIVE_FIXED_PORT_SOURCES = Object.freeze([
  ".github/workflows/ci.yml",
  ".github/workflows/release-build.yml",
  "apps/agent/src/main.rs",
  "crates/agent-core/src/lib.rs",
  "docs/architecture/agent-runtime.md",
  "docs/architecture/gateway-runtime.md",
  "docs/admin/configuration-security.md",
  "scripts/release-bundle-smoke.sh",
  "scripts/cmclient-systemd-integration.sh",
]);

const DOCKER_STANDALONE_PORT_SOURCES = new Set([
  "Dockerfile",
  "docker-compose.yml",
  "docs/architecture/docker-deployment.md",
  "scripts/container-runtime.mjs",
  "scripts/container-runtime.test.mjs",
]);

const NATIVE_FIXED_PORT_PATTERNS = Object.freeze([
  {
    code: "NATIVE_GATEWAY_PORT_AUTHORITY",
    pattern: /^\s*gateway_port\s*=/im,
  },
  {
    code: "NATIVE_GATEWAY_PORT_ENV_INJECTION",
    pattern: /\bCMCLIENT_GATEWAY_PORT\b/i,
  },
]);

const LEGACY_PREBIND_PATTERNS = Object.freeze([
  {
    code: "AGENT_PREBINDS_LISTENER",
    pattern: /Agent\s+pre-binds[^.\n]*(?:listener|socket|handle)/i,
  },
  {
    code: "PREBINDS_LOOPBACK_LISTENER",
    pattern: /pre-binds\s+(?:the\s+)?(?:private\s+)?loopback\s+listener/i,
  },
  {
    code: "INHERITED_SOCKET_HANDOFF",
    pattern: /passes\s+(?:the\s+)?inherited\s+(?:socket|listener|handle)/i,
  },
  {
    code: "AGENT_PASSES_LISTEN_ADDRESS",
    pattern: /Agent\s+passes\s+a\s+loopback\s+listen\s+address/i,
  },
  {
    code: "NONZERO_GATEWAY_PORT_INJECTION",
    pattern:
      /Agent\s+injects[\s\S]{0,200}configured\s+non-zero\s+`?CMCLIENT_GATEWAY_PORT`?/i,
  },
  {
    code: "RELEASE_REBIND_FALLBACK",
    pattern:
      /Agent\s+(?:releases|closes)[^.\n]{0,120}(?:Gateway|child)[^.\n]{0,120}rebinds/i,
  },
]);

const EXPECTED_NODE_FACTS = Object.freeze({
  version: "24.18.0",
  releaseDate: "2026-06-23",
  lts: "Krypton",
  npmVersion: "11.16.0",
  releaseIndexUrl: "https://nodejs.org/dist/index.json",
  releaseDirectoryUrl: "https://nodejs.org/dist/v24.18.0/",
  checksumsUrl: "https://nodejs.org/dist/v24.18.0/SHASUMS256.txt",
  windowsArchiveUrl:
    "https://nodejs.org/dist/v24.18.0/node-v24.18.0-win-x64.zip",
  windowsArchiveSha256:
    "0ae68406b42d7725661da979b1403ec9926da205c6770827f33aac9d8f26e821",
  windowsArchiveSizeBytes: 37_176_245,
});

const EXPECTED_OFFICIAL_FACTS = Object.freeze([
  {
    id: "windows-listen-fd-unsupported",
    url: "https://nodejs.org/download/release/v24.18.0/docs/api/net.html#serverlistenhandle-backlog-callback",
    statement: "Listening on a file descriptor is not supported on Windows.",
  },
  {
    id: "windows-ipc-socket-send-unsupported",
    url: "https://nodejs.org/download/release/v24.18.0/docs/api/child_process.html#subprocesssendmessage-sendhandle-options-callback",
    statement: "Sending IPC sockets is not supported on Windows.",
  },
  {
    id: "port-zero-is-os-assigned",
    url: "https://nodejs.org/download/release/v24.18.0/docs/api/net.html#serverlistenport-host-backlog-callback",
    statement:
      "When port is 0, the operating system assigns an arbitrary unused port that is available after the listening event.",
  },
]);

const REQUIRED_FORBIDDEN_DESIGNS = Object.freeze([
  "agent-prebound-listener-inherited-by-node",
  "windows-inherited-file-descriptor-listen",
  "windows-ipc-socket-handoff",
  "agent-release-then-gateway-rebind",
  "fixed-port-fallback-after-bind-race",
]);

const REQUIRED_REPAIRED_CONTRACT_PATTERNS = Object.freeze([
  {
    code: "CHILD_PORT_ZERO_BIND_MISSING",
    pattern: /atomically\s+binds?\s+`?127\.0\.0\.1:0`?/i,
  },
  {
    code: "BOUNDED_BOOTSTRAP_FRAME_MISSING",
    pattern: /bounded[\s\S]{0,80}bootstrap\s+frame/i,
  },
  {
    code: "READY_FRAME_CONTRACT_MISSING",
    pattern: /ready\s+frame/i,
  },
]);

function error(code, detail) {
  return `${code}${detail ? `: ${detail}` : ""}`;
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function equalArray(left, right) {
  return Array.isArray(left) && JSON.stringify(left) === JSON.stringify(right);
}

function hasEvery(value, expected) {
  return Array.isArray(value) && expected.every((item) => value.includes(item));
}

/**
 * Validate the committed, offline feasibility decision without network access.
 */
export function validateDecisionDocument(decision) {
  const errors = [];
  if (!isObject(decision)) {
    return [error("P13_T12_DECISION_NOT_OBJECT")];
  }
  if (decision.schemaVersion !== 1) {
    errors.push(error("P13_T12_DECISION_SCHEMA_INVALID"));
  }
  if (decision.repairTask !== "P13-T12") {
    errors.push(error("P13_T12_DECISION_TASK_INVALID"));
  }
  if (
    decision.decisionId !== "p13-windows-gateway-bootstrap-and-updater-bounds"
  ) {
    errors.push(error("P13_T12_DECISION_ID_INVALID"));
  }
  if (decision.status !== "accepted-for-fixture-implementation") {
    errors.push(error("P13_T12_DECISION_STATUS_INVALID"));
  }
  if (
    !equalArray(
      decision.authoritativeDocuments,
      DEFAULT_AUTHORITATIVE_DOCUMENTS,
    )
  ) {
    errors.push(error("P13_T12_AUTHORITY_SET_INVALID"));
  }

  const node = decision.node;
  if (!isObject(node)) {
    errors.push(error("P13_T12_NODE_FACTS_MISSING"));
  } else {
    for (const [key, expected] of Object.entries(EXPECTED_NODE_FACTS)) {
      const actualKey =
        key === "windowsArchiveUrl"
          ? "windowsX64Archive.url"
          : key === "windowsArchiveSha256"
            ? "windowsX64Archive.sha256"
            : key === "windowsArchiveSizeBytes"
              ? "windowsX64Archive.sizeBytes"
              : key;
      const actual = actualKey.startsWith("windowsX64Archive.")
        ? node.windowsX64Archive?.[actualKey.split(".")[1]]
        : node[key];
      if (actual !== expected) {
        errors.push(
          error(
            "P13_T12_NODE_FACT_DRIFT",
            `${actualKey} expected ${String(expected)}, received ${String(actual)}`,
          ),
        );
      }
    }
    if (!/^[a-f0-9]{64}$/.test(node.windowsX64Archive?.sha256 ?? "")) {
      errors.push(error("P13_T12_NODE_CHECKSUM_INVALID"));
    }
    if (!equalArray(node.officialFacts, EXPECTED_OFFICIAL_FACTS)) {
      errors.push(error("P13_T12_NODE_OFFICIAL_FACTS_DRIFT"));
    }
  }

  const gateway = decision.gatewayBootstrap;
  if (!isObject(gateway)) {
    errors.push(error("P13_T12_GATEWAY_DECISION_MISSING"));
  } else {
    if (gateway.decision !== "gateway-child-atomic-loopback-bind") {
      errors.push(error("P13_T12_GATEWAY_DECISION_INVALID"));
    }
    if (!hasEvery(gateway.forbiddenDesigns, REQUIRED_FORBIDDEN_DESIGNS)) {
      errors.push(error("P13_T12_GATEWAY_FORBIDDEN_SET_INCOMPLETE"));
    }
    const bind = gateway.bind;
    if (!isObject(bind)) {
      errors.push(error("P13_T12_GATEWAY_BIND_MISSING"));
    } else {
      if (bind.owner !== "gateway-child") {
        errors.push(error("P13_T12_GATEWAY_BIND_OWNER_INVALID"));
      }
      if (bind.host !== "127.0.0.1" || bind.requestedPort !== 0) {
        errors.push(error("P13_T12_GATEWAY_BIND_ENDPOINT_INVALID"));
      }
      if (
        bind.atomicThroughReady !== true ||
        bind.readyOnlyAfterListening !== true
      ) {
        errors.push(error("P13_T12_GATEWAY_BIND_ATOMICITY_REQUIRED"));
      }
    }
    const channel = gateway.privateChannel;
    if (!isObject(channel)) {
      errors.push(error("P13_T12_GATEWAY_CHANNEL_MISSING"));
    } else {
      if (channel.transport !== "inherited-private-pipe") {
        errors.push(error("P13_T12_GATEWAY_CHANNEL_TRANSPORT_INVALID"));
      }
      if (
        channel.encoding !== "u32be-length-prefixed-utf8-json" ||
        !Number.isInteger(channel.maxFrameBytes) ||
        channel.maxFrameBytes < 256 ||
        channel.maxFrameBytes > 65_536 ||
        !Number.isInteger(channel.readyDeadlineMs) ||
        channel.readyDeadlineMs < 100 ||
        channel.readyDeadlineMs > 30_000
      ) {
        errors.push(error("P13_T12_GATEWAY_CHANNEL_BOUNDS_INVALID"));
      }
      if (
        !hasEvery(channel.forbiddenCarriers, [
          "argv",
          "environment",
          "disk",
          "logs",
        ])
      ) {
        errors.push(error("P13_T12_GATEWAY_CHANNEL_SECRET_CARRIERS_INVALID"));
      }
      if (
        !hasEvery(channel.bootstrapRequiredFields, [
          "schemaVersion",
          "type",
          "startupNonce",
          "capability",
        ])
      ) {
        errors.push(error("P13_T12_GATEWAY_BOOTSTRAP_FRAME_INVALID"));
      }
      if (
        !hasEvery(channel.readyRequiredFields, [
          "schemaVersion",
          "type",
          "pid",
          "startupNonce",
          "host",
          "port",
        ])
      ) {
        errors.push(error("P13_T12_GATEWAY_READY_FRAME_INVALID"));
      }
    }
    const policy = gateway.failurePolicy;
    if (
      !isObject(policy) ||
      policy.portTakeoverFallback !== false ||
      policy.capabilityReflection !== false ||
      policy.oversizeOrMalformedFrame !== "fail-startup" ||
      policy.timeoutOrEarlyExit !== "fail-startup" ||
      policy.wrongPidNonceHostOrPort !== "fail-startup"
    ) {
      errors.push(error("P13_T12_GATEWAY_FAILURE_POLICY_INVALID"));
    }
  }

  const updater = decision.updaterDownloadBounds;
  if (!isObject(updater)) {
    errors.push(error("P13_T12_UPDATER_BOUNDS_MISSING"));
  } else {
    if (
      updater.selectedDriver !== "pending-P13-T05" ||
      updater.previewRiskApproval !== null
    ) {
      errors.push(error("P13_T12_UPDATER_SELECTION_PREMATURE"));
    }
    for (const [key, expected] of [
      ["crate", "tauri-plugin-updater"],
      ["version", "2.10.1"],
    ]) {
      if (updater.officialTauri?.[key] !== expected) {
        errors.push(
          error("P13_T12_TAURI_DRIVER_FACT_DRIFT", `${key} mismatch`),
        );
      }
    }
    for (const [key, expected] of [
      ["crate", "cargo-packager-updater"],
      ["version", "0.2.3"],
      ["maturity", "public-preview"],
    ]) {
      if (updater.cargoPackager?.[key] !== expected) {
        errors.push(
          error("P13_T12_PACKAGER_DRIVER_FACT_DRIFT", `${key} mismatch`),
        );
      }
    }
    for (const driver of [updater.officialTauri, updater.cargoPackager]) {
      if (
        !isObject(driver) ||
        driver.builtInMaximumBytes !== false ||
        driver.progressCallbackCanAbort !== false ||
        typeof driver.behavior !== "string" ||
        !driver.behavior.includes("in-memory")
      ) {
        errors.push(error("P13_T12_UPDATER_OVERSIZE_BEHAVIOR_INVALID"));
      }
    }
    const format = updater.formatCompatibility;
    if (
      !isObject(format) ||
      format.tauriV1CompatibleWindowsArtifact !== "*.nsis.zip" ||
      format.officialTauriConsumesNsisZip !== true ||
      format.cargoPackagerExpectedWindowsNsisArtifact !== "raw-exe" ||
      format.cargoPackagerConsumesNsisZip !== false ||
      format.noConversionOrRepackAllowed !== true ||
      format.cargoPackagerSelected !== false ||
      format.cargoPackagerDecision !== "rejected-exact-format-incompatible" ||
      format.officialTauriSourceUrl !==
        "https://github.com/tauri-apps/plugins-workspace/blob/updater-v2.10.1/plugins/updater/src/updater.rs" ||
      format.cargoPackagerSourceUrl !==
        "https://github.com/crabnebula-dev/cargo-packager/blob/cargo-packager-updater-v0.2.3/crates/updater/src/lib.rs"
    ) {
      errors.push(error("P13_T12_UPDATER_FORMAT_COMPATIBILITY_INVALID"));
    }
    const mitigation = updater.requiredMitigation;
    if (
      !isObject(mitigation) ||
      mitigation.hardResourceCap !== true ||
      mitigation.boundedDeadline !== true ||
      mitigation.contentLengthAloneIsSufficient !== false ||
      mitigation.oversizeResult !==
        "terminate-helper-and-keep-current-install" ||
      mitigation.customDownloadCryptoArchiveFallback !== false
    ) {
      errors.push(error("P13_T12_UPDATER_RESOURCE_CAP_REQUIRED"));
    }
  }
  return errors;
}

export function findLegacyPrebindClaims(text, source = "<text>") {
  const errors = [];
  const value = String(text);
  for (const { code, pattern } of LEGACY_PREBIND_PATTERNS) {
    const match = pattern.exec(value);
    if (match) {
      const line = value.slice(0, match.index).split(/\r?\n/).length;
      errors.push(
        error(`P13_T12_LEGACY_PREBIND_CONTRACT:${code}`, `${source}:${line}`),
      );
    }
  }
  return errors;
}

export function findNativeFixedPortClaims(text, source = "<text>") {
  const normalizedSource = String(source)
    .replaceAll("\\", "/")
    .replace(/^\.\//, "");
  if (DOCKER_STANDALONE_PORT_SOURCES.has(normalizedSource)) {
    return [];
  }
  const errors = [];
  const value = String(text);
  for (const { code, pattern } of NATIVE_FIXED_PORT_PATTERNS) {
    const match = pattern.exec(value);
    if (match) {
      const line = value.slice(0, match.index).split(/\r?\n/).length;
      errors.push(
        error(
          `P13_T12_NATIVE_FIXED_PORT_CONTRACT:${code}`,
          `${source}:${line}`,
        ),
      );
    }
  }
  return errors;
}

export async function checkRepository(
  repositoryRoot = resolve("."),
  { includeImmutableGraph = true } = {},
) {
  const decisionPath = resolve(repositoryRoot, DECISION_RELATIVE_PATH);
  let decision;
  try {
    decision = JSON.parse(await readFile(decisionPath, "utf8"));
  } catch (cause) {
    return [error("P13_T12_DECISION_READ_FAILED", cause.message)];
  }
  const errors = validateDecisionDocument(decision);
  const documents = [
    ...(Array.isArray(decision.authoritativeDocuments)
      ? decision.authoritativeDocuments
      : DEFAULT_AUTHORITATIVE_DOCUMENTS),
    ...(includeImmutableGraph ? [IMMUTABLE_GRAPH_DOCUMENT] : []),
  ];
  let authorityText = "";
  for (const relativePath of documents) {
    const path = resolve(repositoryRoot, relativePath);
    let text;
    try {
      text = await readFile(path, "utf8");
    } catch (cause) {
      errors.push(
        error(
          "P13_T12_AUTHORITY_READ_FAILED",
          `${relativePath}: ${cause.message}`,
        ),
      );
      continue;
    }
    let activeText = text;
    if (relativePath === IMMUTABLE_GRAPH_DOCUMENT) {
      try {
        const graph = JSON.parse(text);
        activeText = JSON.stringify({
          tasks: graph.tasks,
          definitionAmendments: graph.definitionAmendments?.map((record) => ({
            task: record.task,
            newValue: record.newValue,
          })),
        });
      } catch (cause) {
        errors.push(
          error(
            "P13_T12_AUTHORITY_PARSE_FAILED",
            `${relativePath}: ${cause.message}`,
          ),
        );
        continue;
      }
    }
    authorityText += `\n${activeText}`;
    errors.push(...findLegacyPrebindClaims(activeText, relativePath));
  }
  for (const relativePath of NATIVE_FIXED_PORT_SOURCES) {
    let text;
    try {
      text = await readFile(resolve(repositoryRoot, relativePath), "utf8");
    } catch (cause) {
      errors.push(
        error(
          "P13_T12_NATIVE_PORT_SOURCE_READ_FAILED",
          `${relativePath}: ${cause.message}`,
        ),
      );
      continue;
    }
    errors.push(...findNativeFixedPortClaims(text, relativePath));
  }
  for (const { code, pattern } of REQUIRED_REPAIRED_CONTRACT_PATTERNS) {
    if (!pattern.test(authorityText)) {
      errors.push(error(`P13_T12_REPAIRED_CONTRACT_${code}`));
    }
  }
  return errors;
}

export async function main(argumentsList = process.argv.slice(2)) {
  const includeImmutableGraph = !argumentsList.includes(
    "--exclude-immutable-graph",
  );
  const errors = await checkRepository(resolve("."), { includeImmutableGraph });
  if (errors.length > 0) {
    process.stderr.write(`${errors.join("\n")}\n`);
    return 1;
  }
  process.stdout.write("P13_T12_FEASIBILITY_CONTRACT_OK\n");
  return 0;
}

const invokedPath = process.argv[1];
if (
  invokedPath &&
  import.meta.url === pathToFileURL(resolve(invokedPath)).href
) {
  process.exitCode = await main();
}
