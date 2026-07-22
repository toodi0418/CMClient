import { describe, it } from "node:test";
import { deepStrictEqual, ok, strictEqual, throws } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  PhysicalWriteGuard,
  PhysicalWriteGuardError,
} from "./physical-guard.js";

describe("PhysicalWriteGuard", () => {
  it("test mode permits unrestricted writes", async () => {
    const guard = new PhysicalWriteGuard({ physicalProfile: false });

    await guard.checkConnectionAttempt();
    await guard.checkConfigRequest(123);
    await guard.checkConfigRequest(456); // Duplicate allowed in test mode
    guard.rejectWrite("admin_message"); // Any packet type allowed
  });

  it("physical mode permits exactly one config request per session", async () => {
    const guard = new PhysicalWriteGuard({
      physicalProfile: true,
      sessionNonce: 12345,
    });

    await guard.checkConfigRequest(12345);

    await throws(
      () => guard.checkConfigRequest(12345),
      (err: Error) => {
        ok(err instanceof PhysicalWriteGuardError);
        strictEqual(
          err.code,
          "PHYSICAL_GUARD_DUPLICATE_CONFIG_REQUEST"
        );
        return true;
      }
    );
  });

  it("physical mode rejects mismatched nonce", async () => {
    const guard = new PhysicalWriteGuard({
      physicalProfile: true,
      sessionNonce: 12345,
    });

    await throws(
      () => guard.checkConfigRequest(99999),
      (err: Error) => {
        ok(err instanceof PhysicalWriteGuardError);
        strictEqual(err.code, "PHYSICAL_GUARD_NONCE_MISMATCH");
        return true;
      }
    );
  });

  it("physical mode rejects non-config packet types", () => {
    const guard = new PhysicalWriteGuard({ physicalProfile: true });

    throws(
      () => guard.rejectWrite("send_text"),
      (err: Error) => {
        ok(err instanceof PhysicalWriteGuardError);
        ok(err.code.includes("PHYSICAL_GUARD_PACKET_TYPE_REJECTED"));
        return true;
      }
    );

    throws(
      () => guard.rejectWrite("admin_message"),
      (err: Error) => {
        ok(err instanceof PhysicalWriteGuardError);
        return true;
      }
    );

    // want_config_id is allowed
    guard.rejectWrite("want_config_id");
  });

  it("aggregate ledger tracks attempts and opens fuse after threshold", async () => {
    const tmpDir = mkdtempSync(join(tmpdir(), "physical-guard-test-"));
    const ledgerPath = join(tmpDir, "ledger.json");

    try {
      let clock = new Date("2026-01-01T00:00:00Z");

      // Create 4 attempts within 10-minute window
      for (let i = 0; i < 4; i++) {
        const guard = new PhysicalWriteGuard({
          physicalProfile: true,
          ledgerPath,
          clock: () => clock,
        });
        await guard.checkConnectionAttempt();
        clock = new Date(clock.getTime() + 60_000); // +1 minute
      }

      // 5th attempt should open fuse
      const guard5 = new PhysicalWriteGuard({
        physicalProfile: true,
        ledgerPath,
        clock: () => clock,
      });

      await throws(
        () => guard5.checkConnectionAttempt(),
        (err: Error) => {
          ok(err instanceof PhysicalWriteGuardError);
          strictEqual(
            err.code,
            "PHYSICAL_GUARD_TOO_MANY_ATTEMPTS_IN_WINDOW"
          );
          return true;
        }
      );

      // Fuse remains open
      await throws(
        () => guard5.checkConnectionAttempt(),
        (err: Error) => {
          ok(err instanceof PhysicalWriteGuardError);
          strictEqual(err.code, "PHYSICAL_GUARD_FUSE_OPEN");
          return true;
        }
      );
    } finally {
      rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("consecutive config failures open fuse", async () => {
    const tmpDir = mkdtempSync(join(tmpdir(), "physical-guard-test-"));
    const ledgerPath = join(tmpDir, "ledger.json");

    try {
      const clock = new Date("2026-01-01T00:00:00Z");

      // Simulate 3 consecutive config failures by manually creating ledger
      const { writeFileSync } = await import("node:fs");
      writeFileSync(
        ledgerPath,
        JSON.stringify({
          schemaVersion: 1,
          attempts: [
            {
              timestamp: new Date(
                clock.getTime() - 180_000
              ).toISOString(),
              sessionId: "test1",
              kind: "config_request",
              result: "rejected",
            },
            {
              timestamp: new Date(
                clock.getTime() - 120_000
              ).toISOString(),
              sessionId: "test2",
              kind: "config_request",
              result: "rejected",
            },
            {
              timestamp: new Date(
                clock.getTime() - 60_000
              ).toISOString(),
              sessionId: "test3",
              kind: "config_request",
              result: "rejected",
            },
          ],
          fuseState: "closed",
        })
      );

      const guard = new PhysicalWriteGuard({
        physicalProfile: true,
        ledgerPath,
        clock: () => clock,
      });

      await throws(
        () => guard.checkConnectionAttempt(),
        (err: Error) => {
          ok(err instanceof PhysicalWriteGuardError);
          strictEqual(
            err.code,
            "PHYSICAL_GUARD_CONSECUTIVE_CONFIG_FAILURES"
          );
          return true;
        }
      );
    } finally {
      rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
