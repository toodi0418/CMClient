import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { lstat, mkdir, realpath, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";

const MAX_CAPTURE_BYTES = 2 * 1024 * 1024;
const DEFAULT_OBSERVATION_TIMEOUT_MS = 5 * 60 * 1000;
const FORBIDDEN_CLAIMS = [
  "V3",
  "installed candidate",
  "final live",
  "recovery",
  "soak",
  "production",
];

// This observer covers cross-surface HWS projection only. The qualification
// driver must separately prove process/socket ownership, the physical ledger,
// disabled writers, campaign containment, and product shutdown.

export function isQualifyingPassiveEvent(event, readyAt) {
  const occurredAt = Date.parse(event?.occurredAt ?? "");
  return (
    event?.type === "mesh.observation.persisted" &&
    event?.source === "gateway" &&
    event?.payload?.transport === "tcp" &&
    ["packet", "other"].includes(event?.payload?.kind) &&
    Number.isFinite(occurredAt) &&
    occurredAt >= readyAt
  );
}

export function hardwareEventDigest(event) {
  const fields = [
    event?.eventId,
    event?.payload?.observationId,
    event?.payload?.transport,
    event?.payload?.kind,
  ];
  if (fields.some((value) => typeof value !== "string" || value.length === 0)) {
    throw new Error("EVENT_DIGEST_INPUT_INVALID");
  }
  return createHash("sha256").update(fields.join("\0"), "utf8").digest("hex");
}

export function identityTuple(report) {
  const identity = report?.identity;
  const target = identity?.target;
  return [
    identity?.product,
    identity?.version,
    identity?.sourceCommit,
    identity?.sourceTree,
    identity?.channel,
    target?.os,
    target?.architecture,
    target?.profile,
    target?.packageProfile,
  ];
}

export function identityMatches(
  report,
  expectedComponent,
  expectedCommit,
  expectedTree,
) {
  const tuple = identityTuple(report);
  return (
    report?.component === expectedComponent &&
    tuple[0] === "CMClient" &&
    tuple[2] === expectedCommit &&
    tuple[3] === expectedTree &&
    tuple[4] === "dev" &&
    tuple[5] === "windows" &&
    tuple[6] === "x86_64" &&
    tuple[7] === "native" &&
    tuple[8] === "workspace"
  );
}

export function appendSseChunk(state, chunk) {
  const frames = [];
  state.buffer = `${state.buffer}${chunk}`.replaceAll("\r\n", "\n");
  for (;;) {
    const boundary = state.buffer.indexOf("\n\n");
    if (boundary < 0) break;
    const frame = state.buffer.slice(0, boundary);
    state.buffer = state.buffer.slice(boundary + 2);
    const data = frame
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trimStart())
      .join("\n");
    if (data) frames.push(data);
  }
  if (state.buffer.length > MAX_CAPTURE_BYTES) {
    throw new Error("SSE_BUFFER_EXCEEDED");
  }
  return frames;
}

export function cliSawExactEvent(output, eventId) {
  return output
    .split(/\r?\n/)
    .some(
      (line) =>
        line.includes(" mesh.observation.persisted source=gateway id=") &&
        line.endsWith(`id=${eventId}`),
    );
}

export function qualificationCliEnvironment(environment) {
  const available = new Map(
    Object.entries(environment).map(([key, value]) => [
      key.toUpperCase(),
      value,
    ]),
  );
  const child = {};
  for (const [source, target] of [
    ["HOME", "HOME"],
    ["USERPROFILE", "USERPROFILE"],
    ["TEMP", "TEMP"],
    ["TMP", "TMP"],
    ["TMPDIR", "TMPDIR"],
    ["APPDATA", "APPDATA"],
    ["LOCALAPPDATA", "LOCALAPPDATA"],
    ["PROGRAMDATA", "PROGRAMDATA"],
    ["SYSTEMROOT", "SystemRoot"],
    ["WINDIR", "WINDIR"],
    ["COMSPEC", "ComSpec"],
  ]) {
    const value = available.get(source);
    if (typeof value === "string" && value.length > 0) child[target] = value;
  }
  return child;
}

function isInside(root, candidate) {
  const normalizedRoot =
    process.platform === "win32" ? root.toLowerCase() : root;
  const normalizedCandidate =
    process.platform === "win32" ? candidate.toLowerCase() : candidate;
  const value = relative(normalizedRoot, normalizedCandidate);
  return (
    value === "" ||
    (value !== ".." && !value.startsWith(`..${sep}`) && !isAbsolute(value))
  );
}

async function assertDirectoryChainHasNoLinks(root, target) {
  const segments = relative(root, target).split(sep).filter(Boolean);
  if (segments.some((segment) => segment.includes(":"))) {
    throw new Error("OBSERVER_EVIDENCE_PATH_INVALID");
  }
  let current = root;
  for (const segment of segments) {
    current = join(current, segment);
    try {
      const metadata = await lstat(current);
      if (metadata.isSymbolicLink() || !metadata.isDirectory()) {
        throw new Error("OBSERVER_EVIDENCE_PATH_UNSAFE");
      }
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw error;
    }
  }
}

export async function prepareEvidenceFile(campaignRoot, evidencePath) {
  const campaign = resolve(campaignRoot);
  const evidence = resolve(evidencePath);
  const parent = dirname(evidence);
  if (campaign === evidence || !isInside(campaign, evidence)) {
    throw new Error("OBSERVER_EVIDENCE_PATH_INVALID");
  }
  const campaignMetadata = await lstat(campaign);
  if (campaignMetadata.isSymbolicLink() || !campaignMetadata.isDirectory()) {
    throw new Error("OBSERVER_CAMPAIGN_ROOT_UNSAFE");
  }
  await assertDirectoryChainHasNoLinks(campaign, parent);
  await mkdir(parent, { recursive: true });
  await assertDirectoryChainHasNoLinks(campaign, parent);
  const [campaignCanonical, parentCanonical] = await Promise.all([
    realpath(campaign),
    realpath(parent),
  ]);
  if (!isInside(campaignCanonical, parentCanonical)) {
    throw new Error("OBSERVER_EVIDENCE_PATH_UNSAFE");
  }
  try {
    await lstat(evidence);
    throw new Error("OBSERVER_EVIDENCE_EXISTS");
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  return evidence;
}

function exactKeys(value, keys) {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    JSON.stringify(Object.keys(value).sort()) ===
      JSON.stringify([...keys].sort())
  );
}

export function isSanitizedHardwareEvidence(value) {
  return (
    exactKeys(value, [
      "schema",
      "result",
      "identityLevel",
      "observationOnly",
      "forbiddenClaims",
      "control",
      "identity",
      "web",
      "mesh",
      "passiveEvent",
    ]) &&
    value.schema === "cmclient-p18-t02-hardware-source/v1" &&
    value.result === "pass" &&
    value.identityLevel === "hardware-source" &&
    value.observationOnly === true &&
    JSON.stringify(value.forbiddenClaims) ===
      JSON.stringify(FORBIDDEN_CLAIMS) &&
    exactKeys(value.control, [
      "schemaVersion",
      "agentRunning",
      "gatewayRunning",
      "managementWebRunning",
      "latestErrorClear",
    ]) &&
    value.control.schemaVersion === 3 &&
    value.control.agentRunning === true &&
    value.control.gatewayRunning === true &&
    value.control.managementWebRunning === true &&
    value.control.latestErrorClear === true &&
    exactKeys(value.identity, ["exactPushedSourceAgreement"]) &&
    value.identity.exactPushedSourceAgreement === true &&
    exactKeys(value.web, ["sessionValid", "healthOk", "meshAgreement"]) &&
    value.web.sessionValid === true &&
    value.web.healthOk === true &&
    value.web.meshAgreement === true &&
    exactKeys(value.mesh, [
      "configured",
      "transport",
      "ready",
      "readyTimeValid",
      "framesSent",
      "framesReceived",
      "reconnects",
    ]) &&
    value.mesh.configured === true &&
    value.mesh.transport === "tcp" &&
    value.mesh.ready === true &&
    value.mesh.readyTimeValid === true &&
    Number.isInteger(value.mesh.framesSent) &&
    value.mesh.framesSent === 1 &&
    Number.isInteger(value.mesh.framesReceived) &&
    value.mesh.framesReceived >= 2 &&
    Number.isInteger(value.mesh.reconnects) &&
    value.mesh.reconnects === 0 &&
    exactKeys(value.passiveEvent, [
      "kind",
      "digest",
      "recentApiExact",
      "sseExact",
      "cliExact",
      "cliProjectionTextSafe",
    ]) &&
    ["packet", "other"].includes(value.passiveEvent.kind) &&
    /^[0-9a-f]{64}$/.test(value.passiveEvent.digest) &&
    value.passiveEvent.recentApiExact === true &&
    value.passiveEvent.sseExact === true &&
    value.passiveEvent.cliExact === true &&
    value.passiveEvent.cliProjectionTextSafe === true
  );
}

function configuration(environment) {
  const cli = environment.CMCLIENT_QUALIFICATION_CLI;
  const evidencePath = environment.CMCLIENT_QUALIFICATION_EVIDENCE;
  const campaignRoot = environment.CMCLIENT_CAMPAIGN_ROOT;
  const expectedCommit = environment.CMCLIENT_EXPECTED_COMMIT;
  const expectedTree = environment.CMCLIENT_EXPECTED_TREE;
  const deadlineMs = Number(
    environment.CMCLIENT_OBSERVATION_TIMEOUT_MS ??
      DEFAULT_OBSERVATION_TIMEOUT_MS,
  );
  if (
    !cli ||
    !isAbsolute(cli) ||
    !evidencePath ||
    !isAbsolute(evidencePath) ||
    !campaignRoot ||
    !isAbsolute(campaignRoot) ||
    !/^[0-9a-f]{40}$/.test(expectedCommit ?? "") ||
    !/^[0-9a-f]{40}$/.test(expectedTree ?? "") ||
    !Number.isInteger(deadlineMs) ||
    deadlineMs < 10_000 ||
    deadlineMs > 15 * 60 * 1000
  ) {
    throw new Error("OBSERVER_CONFIGURATION_INVALID");
  }
  const campaign = resolve(campaignRoot);
  const evidence = resolve(evidencePath);
  const evidenceRelative = relative(campaign, evidence);
  if (
    evidenceRelative === "" ||
    evidenceRelative === ".." ||
    evidenceRelative.startsWith(`..\\`) ||
    evidenceRelative.startsWith("../") ||
    isAbsolute(evidenceRelative)
  ) {
    throw new Error("OBSERVER_EVIDENCE_PATH_INVALID");
  }
  return {
    cli,
    campaign,
    evidence,
    expectedCommit,
    expectedTree,
    deadlineMs,
    cliEnvironment: qualificationCliEnvironment(environment),
  };
}

function runCli(config, args) {
  const result = spawnSync(config.cli, args, {
    encoding: "utf8",
    env: config.cliEnvironment,
    windowsHide: true,
    timeout: 35_000,
    maxBuffer: MAX_CAPTURE_BYTES,
  });
  if (result.status !== 0 || result.error) {
    throw new Error("CLI_COMMAND_FAILED");
  }
  return result.stdout;
}

function runCliJson(config, args) {
  try {
    return JSON.parse(runCli(config, ["--json", ...args]));
  } catch {
    throw new Error("CLI_JSON_INVALID");
  }
}

function sessionCookie(headers) {
  const values = headers.getSetCookie?.() ?? [];
  const raw = values[0] ?? headers.get("set-cookie");
  return raw?.split(";", 1)[0] ?? "";
}

async function jsonGet(url, cookie = "") {
  const response = await fetch(url, {
    headers: cookie ? { cookie } : undefined,
    signal: globalThis.AbortSignal.timeout(15_000),
  });
  if (!response.ok) throw new Error("HTTP_READ_FAILED");
  try {
    return await response.json();
  } catch {
    throw new Error("HTTP_JSON_INVALID");
  }
}

async function replayExactEvent(baseUrl, cookie, targetEventId, readyAt) {
  const response = await fetch(`${baseUrl}/api/v1/events`, {
    headers: {
      accept: "text/event-stream",
      cookie,
      "last-event-id": "cmclient-hws-replay-miss",
    },
    signal: globalThis.AbortSignal.timeout(30_000),
  });
  if (
    !response.ok ||
    !response.headers.get("content-type")?.startsWith("text/event-stream") ||
    !response.body
  ) {
    throw new Error("SSE_OPEN_FAILED");
  }
  const reader = response.body.getReader();
  const decoder = new globalThis.TextDecoder();
  const parser = { buffer: "" };
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    const frames = appendSseChunk(
      parser,
      decoder.decode(value, { stream: true }),
    );
    for (const data of frames) {
      let event;
      try {
        event = JSON.parse(data);
      } catch {
        throw new Error("SSE_JSON_INVALID");
      }
      if (event?.eventId === targetEventId) {
        if (!isQualifyingPassiveEvent(event, readyAt)) {
          throw new Error("SSE_EVENT_NOT_QUALIFYING");
        }
        await reader.cancel();
        return event;
      }
    }
  }
  throw new Error("SSE_EVENT_MISSING");
}

const wait = (milliseconds) =>
  new Promise((resolvePromise) => setTimeout(resolvePromise, milliseconds));

export async function observeHardwareSource(environment = process.env) {
  const config = configuration(environment);
  const started = Date.now();
  const version = runCliJson(config, ["version"]);
  let status;
  let mesh;
  while (Date.now() - started < config.deadlineMs) {
    status = runCliJson(config, ["status"]);
    if (status.gateway === "running" && status.managementWeb === "running") {
      try {
        mesh = runCliJson(config, ["meshtastic"]);
      } catch {
        mesh = undefined;
      }
      if (mesh?.connection?.status === "ready") break;
    }
    await wait(500);
  }
  if (mesh?.connection?.status !== "ready") {
    throw new Error("MESH_READY_TIMEOUT");
  }
  const readyAt = Date.parse(mesh.connection.changedAt ?? "");
  if (!Number.isFinite(readyAt)) throw new Error("MESH_READY_TIME_INVALID");
  const baseUrl = String(status.managementWebUrl ?? "").replace(/\/$/, "");
  if (!/^http:\/\/(?:127\.0\.0\.1|localhost):\d+$/.test(baseUrl)) {
    throw new Error("MANAGEMENT_URL_INVALID");
  }

  const sessionResponse = await fetch(`${baseUrl}/api/v1/auth/session`, {
    signal: globalThis.AbortSignal.timeout(15_000),
  });
  const cookie = sessionCookie(sessionResponse.headers);
  let session;
  try {
    session = await sessionResponse.json();
  } catch {
    throw new Error("SESSION_JSON_INVALID");
  }
  const sessionValid =
    sessionResponse.ok &&
    session?.schemaVersion === 1 &&
    /^[0-9a-f]{32}$/.test(session?.csrfToken ?? "") &&
    cookie.length > 0;
  if (!sessionValid) throw new Error("SESSION_INVALID");

  let hit;
  while (Date.now() - started < config.deadlineMs) {
    const recent = await jsonGet(
      `${baseUrl}/api/v1/events/recent?limit=200`,
      cookie,
    );
    hit = recent?.items?.find((event) =>
      isQualifyingPassiveEvent(event, readyAt),
    );
    if (hit) break;
    await wait(1000);
  }
  if (!hit) throw new Error("PASSIVE_EVENT_TIMEOUT");

  const digest = hardwareEventDigest(hit);
  const cliEvents = runCli(config, ["--no-color", "events"]);
  const cliExactEvent = cliSawExactEvent(cliEvents, hit.eventId);
  const cliNodes = runCli(config, ["--no-color", "nodes"]);
  const cliPositions = runCli(config, ["--no-color", "positions"]);
  const cliProjectionTextSafe =
    /^nodes: \d+ item\(s\)\r?\n?$/.test(cliNodes) &&
    /^positions: \d+ item\(s\)\r?\n?$/.test(cliPositions);
  const sseEvent = await replayExactEvent(
    baseUrl,
    cookie,
    hit.eventId,
    readyAt,
  );
  const webMesh = await jsonGet(`${baseUrl}/api/v1/meshtastic`, cookie);
  const gatewayVersion = await jsonGet(
    `${baseUrl}/api/v1/system/version`,
    cookie,
  );
  const health = await jsonGet(`${baseUrl}/api/v1/system/health`, cookie);
  const identityAgreement =
    identityMatches(
      status.identity,
      "agent",
      config.expectedCommit,
      config.expectedTree,
    ) &&
    identityMatches(
      version,
      "command-mode",
      config.expectedCommit,
      config.expectedTree,
    ) &&
    identityMatches(
      gatewayVersion,
      "gateway",
      config.expectedCommit,
      config.expectedTree,
    ) &&
    JSON.stringify(identityTuple(status.identity)) ===
      JSON.stringify(identityTuple(version)) &&
    JSON.stringify(identityTuple(status.identity)) ===
      JSON.stringify(identityTuple(gatewayVersion));
  const concreteMetrics = (metrics) =>
    metrics !== null &&
    typeof metrics === "object" &&
    ["framesSent", "framesReceived", "reconnects"].every(
      (field) => Number.isInteger(metrics[field]) && metrics[field] >= 0,
    );
  const webMeshAgreement =
    webMesh?.configured === true &&
    concreteMetrics(mesh.metrics) &&
    concreteMetrics(webMesh.metrics) &&
    webMesh?.connection?.transport === mesh.connection.transport &&
    webMesh?.connection?.status === mesh.connection.status &&
    webMesh?.connection?.changedAt === mesh.connection.changedAt &&
    webMesh?.metrics?.framesSent === mesh.metrics?.framesSent &&
    webMesh?.metrics?.framesReceived === mesh.metrics?.framesReceived &&
    webMesh?.metrics?.reconnects === mesh.metrics?.reconnects;
  const result = {
    schema: "cmclient-p18-t02-hardware-source/v1",
    result: "pass",
    identityLevel: "hardware-source",
    observationOnly: true,
    forbiddenClaims: [...FORBIDDEN_CLAIMS],
    control: {
      schemaVersion: status.schemaVersion,
      agentRunning: status.agent === "running",
      gatewayRunning: status.gateway === "running",
      managementWebRunning: status.managementWeb === "running",
      latestErrorClear: status.latestErrorCode == null,
    },
    identity: { exactPushedSourceAgreement: identityAgreement },
    web: {
      sessionValid,
      healthOk: health?.status === "ok",
      meshAgreement: webMeshAgreement,
    },
    mesh: {
      configured: mesh.configured === true,
      transport: mesh.connection.transport,
      ready: mesh.connection.status === "ready",
      readyTimeValid: true,
      framesSent: mesh.metrics?.framesSent,
      framesReceived: mesh.metrics?.framesReceived,
      reconnects: mesh.metrics?.reconnects,
    },
    passiveEvent: {
      kind: hit.payload.kind,
      digest,
      recentApiExact: true,
      sseExact: hardwareEventDigest(sseEvent) === digest,
      cliExact: cliExactEvent,
      cliProjectionTextSafe,
    },
  };
  const assertions = [
    result.control.agentRunning,
    result.control.gatewayRunning,
    result.control.managementWebRunning,
    result.control.latestErrorClear,
    result.identity.exactPushedSourceAgreement,
    result.web.sessionValid,
    result.web.healthOk,
    result.web.meshAgreement,
    result.mesh.configured,
    result.mesh.ready,
    result.mesh.framesSent === 1,
    Number.isInteger(result.mesh.framesReceived) &&
      result.mesh.framesReceived >= 2,
    result.mesh.reconnects === 0,
    result.passiveEvent.sseExact,
    result.passiveEvent.cliExact,
    result.passiveEvent.cliProjectionTextSafe,
  ];
  if (assertions.some((assertion) => !assertion)) {
    throw new Error("OBSERVATION_ASSERTION_FAILED");
  }
  if (!isSanitizedHardwareEvidence(result)) {
    throw new Error("OBSERVER_EVIDENCE_INVALID");
  }
  const evidence = await prepareEvidenceFile(config.campaign, config.evidence);
  await writeFile(evidence, `${JSON.stringify(result, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
    flag: "wx",
  });
  return result;
}

function fail(code) {
  process.stdout.write(
    `${JSON.stringify({ schema: "cmclient-p18-t02-observer-error/v1", result: "fail", code })}\n`,
  );
  process.exitCode = 1;
}

function stableFailureCode(error) {
  const code = error instanceof Error ? error.message : "";
  return /^[A-Z][A-Z0-9_]{2,127}$/.test(code) ? code : "OBSERVER_FAILED";
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  observeHardwareSource()
    .then((result) => process.stdout.write(`${JSON.stringify(result)}\n`))
    .catch((error) => fail(stableFailureCode(error)));
}
