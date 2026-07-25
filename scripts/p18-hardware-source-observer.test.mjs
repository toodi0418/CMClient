import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import {
  appendSseChunk,
  cliSawExactEvent,
  hardwareEventDigest,
  identityMatches,
  isSanitizedHardwareEvidence,
  isQualifyingPassiveEvent,
  prepareEvidenceFile,
  qualificationCliEnvironment,
} from "./p18-hardware-source-observer.mjs";

const readyAt = Date.parse("2026-07-25T00:00:00.000Z");

function event(overrides = {}) {
  return {
    eventId: "event-1",
    type: "mesh.observation.persisted",
    source: "gateway",
    occurredAt: "2026-07-25T00:00:00.001Z",
    payload: {
      observationId: "observation-1",
      transport: "tcp",
      kind: "packet",
    },
    ...overrides,
  };
}

function identity(component, commit = "a".repeat(40), tree = "b".repeat(40)) {
  return {
    component,
    identity: {
      product: "CMClient",
      version: "2.0.0-rc.1",
      sourceCommit: commit,
      sourceTree: tree,
      channel: "dev",
      target: {
        os: "windows",
        architecture: "x86_64",
        profile: "native",
        packageProfile: "workspace",
      },
    },
  };
}

function evidence() {
  return {
    schema: "cmclient-p18-t02-hardware-source/v1",
    result: "pass",
    identityLevel: "hardware-source",
    observationOnly: true,
    forbiddenClaims: [
      "V3",
      "installed candidate",
      "final live",
      "recovery",
      "soak",
      "production",
    ],
    control: {
      schemaVersion: 3,
      agentRunning: true,
      gatewayRunning: true,
      managementWebRunning: true,
      latestErrorClear: true,
    },
    identity: { exactPushedSourceAgreement: true },
    web: { sessionValid: true, healthOk: true, meshAgreement: true },
    mesh: {
      configured: true,
      transport: "tcp",
      ready: true,
      readyTimeValid: true,
      framesSent: 1,
      framesReceived: 2,
      reconnects: 0,
    },
    passiveEvent: {
      kind: "packet",
      digest: "d".repeat(64),
      recentApiExact: true,
      sseExact: true,
      cliExact: true,
      cliProjectionTextSafe: true,
    },
  };
}

test("qualifies only passive post-ready physical observations", () => {
  assert.equal(isQualifyingPassiveEvent(event(), readyAt), true);
  assert.equal(
    isQualifyingPassiveEvent(
      event({ payload: { ...event().payload, kind: "other" } }),
      readyAt,
    ),
    true,
  );
  assert.equal(
    isQualifyingPassiveEvent(
      event({ payload: { ...event().payload, kind: "config_complete" } }),
      readyAt,
    ),
    false,
  );
  assert.equal(
    isQualifyingPassiveEvent(
      event({ occurredAt: "2026-07-24T23:59:59.999Z" }),
      readyAt,
    ),
    false,
  );
  assert.equal(
    isQualifyingPassiveEvent(event({ source: "fixture" }), readyAt),
    false,
  );
  assert.equal(
    isQualifyingPassiveEvent(
      event({ payload: { ...event().payload, transport: "simulator" } }),
      readyAt,
    ),
    false,
  );
});

test("hashes only the allowlisted cross-surface event tuple", () => {
  const first = hardwareEventDigest(event());
  const second = hardwareEventDigest({
    ...event(),
    ignoredPrivateField: "never enters the digest",
  });
  assert.match(first, /^[0-9a-f]{64}$/);
  assert.equal(first, second);
  assert.notEqual(
    first,
    hardwareEventDigest(
      event({
        payload: { ...event().payload, observationId: "observation-2" },
      }),
    ),
  );
  assert.throws(
    () => hardwareEventDigest(event({ eventId: "" })),
    /EVENT_DIGEST_INPUT_INVALID/,
  );
});

test("parses fragmented SSE without retaining completed frames", () => {
  const state = { buffer: "" };
  assert.deepEqual(appendSseChunk(state, "id: event-1\r"), []);
  assert.deepEqual(
    appendSseChunk(
      state,
      '\ndata: {"eventId":"event-1"}\r\n\r\n: heartbeat\n\n',
    ),
    ['{"eventId":"event-1"}'],
  );
  assert.equal(state.buffer, "");
});

test("requires exact component and clean pushed source identity", () => {
  const commit = "a".repeat(40);
  const tree = "b".repeat(40);
  assert.equal(identityMatches(identity("agent"), "agent", commit, tree), true);
  assert.equal(
    identityMatches(identity("gateway"), "agent", commit, tree),
    false,
  );
  assert.equal(
    identityMatches(identity("agent", "c".repeat(40)), "agent", commit, tree),
    false,
  );
});

test("matches CLI text envelopes without reading payload JSON", () => {
  const output =
    "2026-07-25T00:00:00Z mesh.observation.persisted source=gateway id=event-1\n";
  assert.equal(cliSawExactEvent(output, "event-1"), true);
  assert.equal(cliSawExactEvent(output, "event-2"), false);
});

test("passes only state-root variables to the CLI child", () => {
  const child = qualificationCliEnvironment({
    HOME: "C:/fixture/home",
    USERPROFILE: "C:/fixture/home",
    SystemRoot: "C:/Windows",
    ComSpec: "C:/Windows/System32/cmd.exe",
    CALLMESH_API_KEY: "must-not-cross-the-process-boundary",
    CMCLIENT_EXPECTED_COMMIT: "a".repeat(40),
  });
  assert.deepEqual(child, {
    HOME: "C:/fixture/home",
    USERPROFILE: "C:/fixture/home",
    SystemRoot: "C:/Windows",
    ComSpec: "C:/Windows/System32/cmd.exe",
  });
});

test("accepts only the sanitized result envelope", () => {
  assert.equal(isSanitizedHardwareEvidence(evidence()), true);
  assert.equal(
    isSanitizedHardwareEvidence({ ...evidence(), endpoint: "fixture.invalid" }),
    false,
  );
  assert.equal(
    isSanitizedHardwareEvidence({
      ...evidence(),
      passiveEvent: { ...evidence().passiveEvent, eventId: "private-event" },
    }),
    false,
  );
  assert.equal(
    isSanitizedHardwareEvidence({
      ...evidence(),
      mesh: { ...evidence().mesh, framesReceived: undefined },
    }),
    false,
  );
});

test("keeps evidence below a canonical campaign directory", async (context) => {
  const campaign = await mkdtemp(join(tmpdir(), "cmclient-hws-campaign-"));
  context.after(() => rm(campaign, { recursive: true, force: true }));
  const accepted = join(campaign, "evidence", "run.json");
  assert.equal(await prepareEvidenceFile(campaign, accepted), accepted);
  await assert.rejects(
    prepareEvidenceFile(campaign, join(campaign, "..", "escape.json")),
    /OBSERVER_EVIDENCE_PATH_INVALID/,
  );
});

test("rejects a linked evidence directory", async (context) => {
  const campaign = await mkdtemp(join(tmpdir(), "cmclient-hws-campaign-"));
  const outside = await mkdtemp(join(tmpdir(), "cmclient-hws-outside-"));
  context.after(() => rm(campaign, { recursive: true, force: true }));
  context.after(() => rm(outside, { recursive: true, force: true }));
  const link = join(campaign, "linked");
  try {
    await symlink(
      outside,
      link,
      process.platform === "win32" ? "junction" : "dir",
    );
  } catch (error) {
    if (["EPERM", "EACCES", "ENOTSUP"].includes(error?.code)) {
      context.skip("host cannot create a test link");
      return;
    }
    throw error;
  }
  await mkdir(join(outside, "evidence"), { recursive: true });
  await assert.rejects(
    prepareEvidenceFile(campaign, join(link, "evidence", "run.json")),
    /OBSERVER_EVIDENCE_PATH_UNSAFE/,
  );
});
