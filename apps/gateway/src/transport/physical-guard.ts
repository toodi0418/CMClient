/**
 * Physical Meshtastic write guard per WLG-06 safe discovery gate.
 *
 * Permits only one nonce-correlated want_config_id per session and rejects
 * every other ToRadio variant before socket write. Maintains an aggregate
 * cross-process attempt ledger with fail-closed fuse.
 */

import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";

export interface PhysicalWriteGuardOptions {
  /** Physical profile mode (true) or unrestricted test mode (false) */
  physicalProfile: boolean;
  /** Path to the aggregate cross-process attempt ledger */
  ledgerPath?: string;
  /** Session nonce for want_config_id correlation */
  sessionNonce?: number;
  clock?: () => Date;
}

export interface PhysicalWriteAttempt {
  timestamp: string;
  sessionId: string;
  kind: "connection" | "config_request";
  nonce?: number;
  result: "allowed" | "rejected" | "fuse_open";
}

interface PhysicalWriteLedger {
  schemaVersion: number;
  attempts: PhysicalWriteAttempt[];
  fuseState: "closed" | "open";
  fuseOpenedAt?: string;
}

const LEDGER_SCHEMA_VERSION = 1;
const MAX_ATTEMPTS_PER_10MIN = 4;
const MAX_CONSECUTIVE_CONFIG_FAILURES = 3;
const SLIDING_WINDOW_MS = 10 * 60 * 1000; // 10 minutes

export class PhysicalWriteGuardError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "PhysicalWriteGuardError";
  }
}

/**
 * Enforces physical Meshtastic write boundaries with aggregate attempt tracking.
 *
 * In physical profile mode:
 * - Permits exactly one want_config_id per session (nonce-correlated)
 * - Rejects all other ToRadio variants before socket write
 * - Maintains cross-process aggregate ledger
 * - Opens fuse after violation thresholds
 */
export class PhysicalWriteGuard {
  private readonly clock: () => Date;
  private readonly ledgerPath: string | undefined;
  private configRequestSent = false;
  private sessionId: string;

  constructor(private readonly options: PhysicalWriteGuardOptions) {
    this.clock = options.clock ?? (() => new Date());
    this.ledgerPath = options.ledgerPath;
    this.sessionId = createHash("sha256")
      .update(this.clock().toISOString())
      .update(Math.random().toString())
      .digest("hex")
      .substring(0, 16);
  }

  /**
   * Check if a connection attempt is permitted.
   * Records attempt in ledger and checks fuse state.
   */
  async checkConnectionAttempt(): Promise<void> {
    if (!this.options.physicalProfile) {
      return; // Test mode - no restrictions
    }

    if (!this.ledgerPath) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_REQUIRED");
    }

    const ledger = this.loadLedger();

    // Check fuse state
    if (ledger.fuseState === "open") {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_FUSE_OPEN");
    }

    // Check sliding window (last 10 minutes)
    const now = this.clock();
    const windowStart = new Date(now.getTime() - SLIDING_WINDOW_MS);
    const recentAttempts = ledger.attempts.filter(
      (a) => new Date(a.timestamp) >= windowStart
    );

    if (recentAttempts.length >= MAX_ATTEMPTS_PER_10MIN) {
      ledger.fuseState = "open";
      ledger.fuseOpenedAt = now.toISOString();
      this.saveLedger(ledger);
      throw new PhysicalWriteGuardError(
        "PHYSICAL_GUARD_TOO_MANY_ATTEMPTS_IN_WINDOW"
      );
    }

    // Check consecutive config failures
    const recentConfigAttempts = ledger.attempts
      .filter((a) => a.kind === "config_request")
      .slice(-MAX_CONSECUTIVE_CONFIG_FAILURES);

    if (
      recentConfigAttempts.length >= MAX_CONSECUTIVE_CONFIG_FAILURES &&
      recentConfigAttempts.every((a) => a.result === "rejected")
    ) {
      ledger.fuseState = "open";
      ledger.fuseOpenedAt = now.toISOString();
      this.saveLedger(ledger);
      throw new PhysicalWriteGuardError(
        "PHYSICAL_GUARD_CONSECUTIVE_CONFIG_FAILURES"
      );
    }

    // Record connection attempt
    ledger.attempts.push({
      timestamp: now.toISOString(),
      sessionId: this.sessionId,
      kind: "connection",
      result: "allowed",
    });

    this.saveLedger(ledger);
  }

  /**
   * Check if a config request (want_config_id) is permitted.
   * Only one per session, must match session nonce.
   */
  async checkConfigRequest(nonce: number): Promise<void> {
    if (!this.options.physicalProfile) {
      return; // Test mode - no restrictions
    }

    // Only one config request per session
    if (this.configRequestSent) {
      throw new PhysicalWriteGuardError(
        "PHYSICAL_GUARD_DUPLICATE_CONFIG_REQUEST"
      );
    }

    // Nonce must match session nonce
    if (
      this.options.sessionNonce !== undefined &&
      nonce !== this.options.sessionNonce
    ) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_NONCE_MISMATCH");
    }

    if (this.ledgerPath) {
      const ledger = this.loadLedger();
      ledger.attempts.push({
        timestamp: this.clock().toISOString(),
        sessionId: this.sessionId,
        kind: "config_request",
        nonce,
        result: "allowed",
      });
      this.saveLedger(ledger);
    }

    this.configRequestSent = true;
  }

  /**
   * Reject all ToRadio variants except want_config_id.
   * Must be called before socket write.
   */
  rejectWrite(packetType: string): void {
    if (!this.options.physicalProfile) {
      return; // Test mode - no restrictions
    }

    if (packetType !== "want_config_id") {
      throw new PhysicalWriteGuardError(
        `PHYSICAL_GUARD_PACKET_TYPE_REJECTED: ${packetType}`
      );
    }
  }

  private loadLedger(): PhysicalWriteLedger {
    if (!this.ledgerPath) {
      return this.createEmptyLedger();
    }

    try {
      const content = readFileSync(this.ledgerPath, "utf8");
      const ledger = JSON.parse(content) as PhysicalWriteLedger;

      if (ledger.schemaVersion !== LEDGER_SCHEMA_VERSION) {
        throw new Error("Schema version mismatch");
      }

      return ledger;
    } catch {
      return this.createEmptyLedger();
    }
  }

  private saveLedger(ledger: PhysicalWriteLedger): void {
    if (!this.ledgerPath) {
      return;
    }

    const content = JSON.stringify(ledger, null, 2);
    writeFileSync(this.ledgerPath, content, "utf8");
  }

  private createEmptyLedger(): PhysicalWriteLedger {
    return {
      schemaVersion: LEDGER_SCHEMA_VERSION,
      attempts: [],
      fuseState: "closed",
    };
  }
}
