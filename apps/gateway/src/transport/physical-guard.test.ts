import { readFileSync, renameSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  PhysicalWriteGuard,
  PhysicalWriteGuardError,
  type PhysicalWriteGuardOptions,
} from "./physical-guard";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((path) => rm(path, { recursive: true, force: true })),
  );
});

describe("PhysicalWriteGuard", () => {
  it("leaves non-physical transports unrestricted", () => {
    const guard = new PhysicalWriteGuard({ physicalProfile: false });

    guard.acquireSession(0);
    guard.authorizeConfigRequest(0, new Uint8Array());
    guard.rejectApplicationWrite();
    guard.accountIncomingBytes(Number.MAX_SAFE_INTEGER);
    guard.accountIncomingFrames(Number.MAX_SAFE_INTEGER);
    guard.releaseSession();
  });

  it("permits one correlated config request and permanently rejects application writes", async () => {
    const fixture = await guardFixture();
    const guard = fixture.guard();

    guard.acquireSession(12_345);
    guard.authorizeConfigRequest(12_345, wantConfig(12_345));
    expectCode(
      () => guard.authorizeConfigRequest(12_345, wantConfig(12_345)),
      "PHYSICAL_GUARD_DUPLICATE_CONFIG_REQUEST",
    );
    expectCode(
      () => guard.rejectApplicationWrite(),
      "PHYSICAL_GUARD_WRITER_DISABLED",
    );
    guard.recordConfigSuccess();
    guard.releaseSession();
  });

  it("rejects a mismatched nonce without consuming the config request", async () => {
    const fixture = await guardFixture();
    const guard = fixture.guard();

    guard.acquireSession(12_345);
    expectCode(
      () => guard.authorizeConfigRequest(54_321, wantConfig(54_321)),
      "PHYSICAL_GUARD_NONCE_MISMATCH",
    );
    guard.authorizeConfigRequest(12_345, wantConfig(12_345));
    guard.recordConfigSuccess();
    guard.releaseSession();
  });

  it("rejects any non-canonical ToRadio payload before authorization", async () => {
    const fixture = await guardFixture();
    const guard = fixture.guard();

    guard.acquireSession(12_345);
    expectCode(
      () => guard.authorizeConfigRequest(12_345, new Uint8Array([0x22, 0x00])),
      "PHYSICAL_GUARD_CONFIG_PAYLOAD_REJECTED",
    );
    expect(guard.automaticReconnectAllowed).toBe(false);
    guard.releaseSession("aborted");
  });

  it("holds one exclusive process lease until clean release", async () => {
    const fixture = await guardFixture();
    const first = fixture.guard();
    const second = fixture.guard();

    first.acquireSession(1);
    expectCode(() => second.acquireSession(2), "PHYSICAL_GUARD_LEASE_HELD");
    first.releaseSession("aborted");

    second.acquireSession(2);
    second.releaseSession("aborted");
  });

  it("opens the fuse before a fifth cycle inside ten minutes", async () => {
    let now = new Date("2026-07-22T00:00:00.000Z");
    const fixture = await guardFixture({ clock: () => now });
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const guard = fixture.guard();
      guard.acquireSession(attempt + 1);
      guard.releaseSession("connect");
      now = new Date(now.getTime() + 60_000);
    }

    expectCode(
      () => fixture.guard().acquireSession(5),
      "PHYSICAL_GUARD_ATTEMPT_WINDOW_EXCEEDED",
    );
    expectCode(
      () => fixture.guard().acquireSession(6),
      "PHYSICAL_GUARD_FUSE_OPEN",
    );
  });

  it("treats the ten-minute window as half-open at the exact boundary", async () => {
    let now = new Date("2026-07-22T00:00:00.000Z");
    const fixture = await guardFixture({ clock: () => now });
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const guard = fixture.guard();
      guard.acquireSession(attempt + 1);
      guard.releaseSession("connect");
      now = new Date(now.getTime() + 60_000);
    }
    now = new Date("2026-07-22T00:10:00.000Z");

    const guard = fixture.guard();
    guard.acquireSession(5);
    guard.releaseSession("connect");
  });

  it("opens the fuse on the third consecutive config failure", async () => {
    let now = new Date("2026-07-22T00:00:00.000Z");
    const fixture = await guardFixture({ clock: () => now });
    let lastGuard: PhysicalWriteGuard | undefined;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const guard = fixture.guard();
      lastGuard = guard;
      guard.acquireSession(attempt + 1);
      guard.authorizeConfigRequest(attempt + 1, wantConfig(attempt + 1));
      guard.recordConfigFailure("timeout");
      guard.releaseSession();
      now = new Date(now.getTime() + 60_000);
    }
    expect(lastGuard?.automaticReconnectAllowed).toBe(false);

    expectCode(
      () => fixture.guard().acquireSession(4),
      "PHYSICAL_GUARD_FUSE_OPEN",
    );
  });

  it("opens a stage fuse before its fifth config request", async () => {
    let now = new Date("2026-07-22T00:00:00.000Z");
    const fixture = await guardFixture({ clock: () => now });
    for (let attempt = 0; attempt < 4; attempt += 1) {
      completeSession(fixture.guard(), attempt + 1);
      now = new Date(now.getTime() + 11 * 60_000);
    }
    const fifth = fixture.guard();
    fifth.acquireSession(5);
    expectCode(
      () => fifth.authorizeConfigRequest(5, wantConfig(5)),
      "PHYSICAL_GUARD_STAGE_REQUEST_LIMIT_EXCEEDED",
    );
    fifth.releaseSession();
  });

  it("opens a candidate fuse before its seventeenth config request", async () => {
    let now = new Date("2026-07-22T00:00:00.000Z");
    const fixture = await guardFixture({ clock: () => now });
    for (let attempt = 0; attempt < 16; attempt += 1) {
      completeSession(
        fixture.guard({
          qualificationStage: `stage-${Math.floor(attempt / 4)}`,
        }),
        attempt + 1,
      );
      now = new Date(now.getTime() + 11 * 60_000);
    }
    const seventeenth = fixture.guard({ qualificationStage: "stage-4" });
    seventeenth.acquireSession(17);
    expectCode(
      () => seventeenth.authorizeConfigRequest(17, wantConfig(17)),
      "PHYSICAL_GUARD_CANDIDATE_REQUEST_LIMIT_EXCEEDED",
    );
    seventeenth.releaseSession();
  }, 30_000);

  it("fails closed on a corrupt existing ledger", async () => {
    const fixture = await guardFixture();
    writeFileSync(fixture.ledgerPath, "not-a-sqlite-ledger", { mode: 0o600 });

    expectCode(
      () => fixture.guard().acquireSession(1),
      "PHYSICAL_GUARD_LEDGER_UNAVAILABLE",
    );
    expect(readFileSync(fixture.ledgerPath, "utf8")).toBe(
      "not-a-sqlite-ledger",
    );
  });

  it("bounds physical session bytes, frames, and duration", async () => {
    let now = new Date("2026-07-22T00:00:00.000Z");
    const fixture = await guardFixture({
      clock: () => now,
      maximumSessionBytes: 4,
      maximumSessionFrames: 2,
      maximumSessionDurationMs: 1_000,
    });
    const guard = fixture.guard();
    guard.acquireSession(1);
    guard.accountIncomingBytes(4);
    guard.accountIncomingFrames(2);
    expectCode(
      () => guard.accountIncomingBytes(1),
      "PHYSICAL_GUARD_BYTE_BUDGET_EXCEEDED",
    );
    expectCode(
      () => guard.accountIncomingFrames(1),
      "PHYSICAL_GUARD_FRAME_BUDGET_EXCEEDED",
    );
    now = new Date(now.getTime() + 1_001);
    expectCode(
      () => guard.accountIncomingBytes(0),
      "PHYSICAL_GUARD_DURATION_BUDGET_EXCEEDED",
    );
    guard.releaseSession("budget");
  });

  it("fails closed when the wall clock moves behind the persisted fence", async () => {
    let now = new Date("2026-07-22T00:10:00.000Z");
    const fixture = await guardFixture({ clock: () => now });
    const first = fixture.guard();
    first.acquireSession(1);
    first.releaseSession("connect");

    now = new Date("2026-07-22T00:09:59.999Z");
    const second = fixture.guard();
    expectCode(() => second.acquireSession(2), "PHYSICAL_GUARD_CLOCK_ROLLBACK");
  });

  it("stores no candidate text and closes Windows file handles on release", async () => {
    const fixture = await guardFixture({
      candidateId: "private-callsign-location-message-api-key",
    });
    completeSession(fixture.guard(), 1);
    const rawLedger = readFileSync(fixture.ledgerPath);
    expect(rawLedger.includes(Buffer.from("private-callsign"))).toBe(false);

    const renamed = `${fixture.ledgerPath}.closed`;
    renameSync(fixture.ledgerPath, renamed);
    renameSync(renamed, fixture.ledgerPath);
  });
});

interface GuardFixture {
  ledgerPath: string;
  guard(overrides?: Partial<PhysicalWriteGuardOptions>): PhysicalWriteGuard;
}

async function guardFixture(
  overrides: Partial<PhysicalWriteGuardOptions> = {},
): Promise<GuardFixture> {
  const directory = await mkdtemp(join(tmpdir(), "cmclient-physical-guard-"));
  temporaryDirectories.push(directory);
  const ledgerPath = join(directory, "physical-write-ledger.sqlite");
  let sequence = 0;
  return {
    ledgerPath,
    guard(extra = {}) {
      sequence += 1;
      const tokenSequence = sequence;
      return new PhysicalWriteGuard({
        physicalProfile: true,
        allowedRoot: directory,
        ledgerPath,
        candidateId: "fixture-candidate",
        qualificationStage: "fixture-stage",
        sessionTokenFactory: () =>
          `fixture-session-${String(tokenSequence).padStart(8, "0")}`,
        ...overrides,
        ...extra,
      });
    },
  };
}

function completeSession(guard: PhysicalWriteGuard, nonce: number): void {
  guard.acquireSession(nonce);
  guard.authorizeConfigRequest(nonce, wantConfig(nonce));
  guard.recordConfigSuccess();
  guard.releaseSession();
}

function expectCode(operation: () => void, code: string): void {
  try {
    operation();
  } catch (error) {
    expect(error).toBeInstanceOf(PhysicalWriteGuardError);
    expect((error as PhysicalWriteGuardError).code).toBe(code);
    return;
  }
  throw new Error(`Expected ${code}`);
}

function wantConfig(nonce: number): Uint8Array {
  const bytes = [0x18];
  let remaining = nonce;
  while (remaining >= 0x80) {
    bytes.push((remaining % 0x80) | 0x80);
    remaining = Math.floor(remaining / 0x80);
  }
  bytes.push(remaining);
  return Uint8Array.from(bytes);
}
