/**
 * Fail-closed safety boundary for qualification sessions connected to a
 * physical Meshtastic device. The ledger stores only opaque digests and stable
 * enums; radio frames, endpoints, identities, and wall-clock strings are never
 * persisted here.
 */

import { createHash, randomUUID } from "node:crypto";
import { chmodSync, existsSync, lstatSync, mkdirSync, statSync } from "node:fs";
import { dirname, isAbsolute, relative, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

const LEDGER_SCHEMA_VERSION = 1;
const MAX_ATTEMPTS_PER_10_MINUTES = 4;
const MAX_CONSECUTIVE_CONFIG_FAILURES = 3;
const MAX_REQUESTS_PER_STAGE = 4;
const MAX_REQUESTS_PER_CANDIDATE = 16;
const MAX_LEDGER_ATTEMPTS = 4_096;
const MAX_LEDGER_BYTES = 4 * 1024 * 1024;
const WINDOW_MILLISECONDS = 10 * 60 * 1_000;
const DEFAULT_MAXIMUM_SESSION_DURATION_MS = 30 * 60 * 1_000;
const DEFAULT_MAXIMUM_SESSION_BYTES = 64 * 1024 * 1024;
const DEFAULT_MAXIMUM_SESSION_FRAMES = 100_000;

const FAILURE_REASONS = new Set([
  "connect",
  "write",
  "timeout",
  "decode",
  "closed",
  "aborted",
  "budget",
]);

export interface PhysicalWriteGuardOptions {
  physicalProfile: boolean;
  ledgerPath?: string;
  allowedRoot?: string;
  candidateId?: string;
  qualificationStage?: string;
  maximumSessionDurationMs?: number;
  maximumSessionBytes?: number;
  maximumSessionFrames?: number;
  clock?: () => Date;
  sessionTokenFactory?: () => string;
}

export class PhysicalWriteGuardError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "PhysicalWriteGuardError";
  }
}

interface ActiveSession {
  attemptId: number;
  candidateDigest: string;
  database: DatabaseSync;
  expectedNonce: number;
  ownerToken: string;
  sessionDigest: string;
  stageDigest: string;
  startedAtMs: number;
  lastObservedAtMs: number;
  bytesReceived: number;
  framesReceived: number;
  configAuthorized: boolean;
  configSettled: boolean;
}

/**
 * A single instance owns at most one persistent physical lease. Every session
 * reservation and config authorization commits before a socket can be opened or
 * written, so a crash can only fail closed.
 */
export class PhysicalWriteGuard {
  private readonly clock: () => Date;
  private readonly maximumSessionDurationMs: number;
  private readonly maximumSessionBytes: number;
  private readonly maximumSessionFrames: number;
  private session: ActiveSession | undefined;
  private reconnectAllowed = true;

  constructor(private readonly options: PhysicalWriteGuardOptions) {
    this.clock = options.clock ?? (() => new Date());
    this.maximumSessionDurationMs = boundedPositiveInteger(
      options.maximumSessionDurationMs,
      DEFAULT_MAXIMUM_SESSION_DURATION_MS,
      24 * 60 * 60 * 1_000,
    );
    this.maximumSessionBytes = boundedPositiveInteger(
      options.maximumSessionBytes,
      DEFAULT_MAXIMUM_SESSION_BYTES,
      1024 * 1024 * 1024,
    );
    this.maximumSessionFrames = boundedPositiveInteger(
      options.maximumSessionFrames,
      DEFAULT_MAXIMUM_SESSION_FRAMES,
      10_000_000,
    );
    if (options.physicalProfile) {
      validatePhysicalOptions(options);
    }
  }

  get physicalProfile(): boolean {
    return this.options.physicalProfile;
  }

  get sessionDurationLimitMs(): number {
    return this.maximumSessionDurationMs;
  }

  get automaticReconnectAllowed(): boolean {
    return this.reconnectAllowed;
  }

  /** Reserve the sole process lease and one connection/config cycle. */
  acquireSession(expectedNonce: number): void {
    if (!this.options.physicalProfile) {
      return;
    }
    validateNonce(expectedNonce);
    if (this.session) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_SESSION_ACTIVE");
    }

    const nowMs = this.nowMilliseconds();
    const candidateDigest = opaqueDigest(
      "candidate",
      this.options.candidateId!,
    );
    const stageDigest = opaqueDigest("stage", this.options.qualificationStage!);
    const ownerToken = validateSessionToken(
      (this.options.sessionTokenFactory ?? randomUUID)(),
    );
    const sessionDigest = opaqueDigest(
      "session",
      `${candidateDigest}:${stageDigest}:${ownerToken}`,
    );
    const database = openLedger(
      this.options.ledgerPath!,
      this.options.allowedRoot,
    );

    try {
      let rejection: string | undefined;
      let attemptId = 0;
      transaction(database, () => {
        assertLedgerHealthy(database);
        assertAndAdvanceClock(database, nowMs);
        rejection = currentFuseCode(database);
        if (rejection) {
          return;
        }
        if (
          database
            .prepare("SELECT 1 FROM physical_lease WHERE singleton = 1")
            .get()
        ) {
          rejection = "PHYSICAL_GUARD_LEASE_HELD";
          return;
        }
        const total = integerValue(
          database
            .prepare("SELECT COUNT(*) AS value FROM physical_attempt")
            .get()?.value,
        );
        if (total >= MAX_LEDGER_ATTEMPTS) {
          rejection = openFuse(
            database,
            nowMs,
            "PHYSICAL_GUARD_LEDGER_CAPACITY_EXCEEDED",
          );
          return;
        }
        const windowStart = nowMs - WINDOW_MILLISECONDS;
        const recent = integerValue(
          database
            .prepare(
              "SELECT COUNT(*) AS value FROM physical_attempt WHERE started_at_ms > ? AND started_at_ms <= ?",
            )
            .get(windowStart, nowMs)?.value,
        );
        if (recent >= MAX_ATTEMPTS_PER_10_MINUTES) {
          rejection = openFuse(
            database,
            nowMs,
            "PHYSICAL_GUARD_ATTEMPT_WINDOW_EXCEEDED",
          );
          return;
        }
        database
          .prepare(
            "INSERT INTO physical_lease (singleton, owner_token, session_digest, acquired_at_ms) VALUES (1, ?, ?, ?)",
          )
          .run(ownerToken, sessionDigest, nowMs);
        const inserted = database
          .prepare(
            "INSERT INTO physical_attempt (session_digest, candidate_digest, stage_digest, started_at_ms, config_request_count, outcome) VALUES (?, ?, ?, ?, 0, 'pending') RETURNING id",
          )
          .get(sessionDigest, candidateDigest, stageDigest, nowMs);
        attemptId = integerValue(inserted?.id);
      });
      if (rejection) {
        this.reconnectAllowed = false;
        throw new PhysicalWriteGuardError(rejection);
      }
      if (attemptId < 1) {
        throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_UNAVAILABLE");
      }
      this.session = {
        attemptId,
        candidateDigest,
        database,
        expectedNonce,
        ownerToken,
        sessionDigest,
        stageDigest,
        startedAtMs: nowMs,
        lastObservedAtMs: nowMs,
        bytesReceived: 0,
        framesReceived: 0,
        configAuthorized: false,
        configSettled: false,
      };
    } catch (error) {
      database.close();
      throw guardError(error);
    }
  }

  /** Commit the one allowed config request before its bytes reach socket.write. */
  authorizeConfigRequest(nonce: number, payload: Uint8Array): void {
    if (!this.options.physicalProfile) {
      return;
    }
    const session = this.requireSession();
    validateNonce(nonce);
    if (nonce !== session.expectedNonce) {
      this.reconnectAllowed = false;
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_NONCE_MISMATCH");
    }
    if (session.configAuthorized) {
      this.reconnectAllowed = false;
      throw new PhysicalWriteGuardError(
        "PHYSICAL_GUARD_DUPLICATE_CONFIG_REQUEST",
      );
    }
    if (!isExactWantConfigRequest(payload, nonce)) {
      this.reconnectAllowed = false;
      throw new PhysicalWriteGuardError(
        "PHYSICAL_GUARD_CONFIG_PAYLOAD_REJECTED",
      );
    }

    const nowMs = this.nowMilliseconds();
    let rejection: string | undefined;
    transaction(session.database, () => {
      assertLease(session);
      assertAndAdvanceClock(session.database, nowMs);
      rejection = currentFuseCode(session.database);
      if (rejection) {
        return;
      }
      const row = session.database
        .prepare(
          "SELECT config_request_count FROM physical_attempt WHERE id = ? AND session_digest = ?",
        )
        .get(session.attemptId, session.sessionDigest);
      if (!row) {
        throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_UNAVAILABLE");
      }
      if (integerValue(row.config_request_count) !== 0) {
        rejection = "PHYSICAL_GUARD_DUPLICATE_CONFIG_REQUEST";
        return;
      }
      const stageRequests = integerValue(
        session.database
          .prepare(
            "SELECT COUNT(*) AS value FROM physical_attempt WHERE stage_digest = ? AND config_request_count = 1",
          )
          .get(session.stageDigest)?.value,
      );
      if (stageRequests >= MAX_REQUESTS_PER_STAGE) {
        rejection = openFuse(
          session.database,
          nowMs,
          "PHYSICAL_GUARD_STAGE_REQUEST_LIMIT_EXCEEDED",
        );
        return;
      }
      const candidateRequests = integerValue(
        session.database
          .prepare(
            "SELECT COUNT(*) AS value FROM physical_attempt WHERE candidate_digest = ? AND config_request_count = 1",
          )
          .get(session.candidateDigest)?.value,
      );
      if (candidateRequests >= MAX_REQUESTS_PER_CANDIDATE) {
        rejection = openFuse(
          session.database,
          nowMs,
          "PHYSICAL_GUARD_CANDIDATE_REQUEST_LIMIT_EXCEEDED",
        );
        return;
      }
      const result = session.database
        .prepare(
          "UPDATE physical_attempt SET config_request_count = 1, nonce_digest = ? WHERE id = ? AND config_request_count = 0",
        )
        .run(
          opaqueDigest("nonce", `${session.sessionDigest}:${nonce}`),
          session.attemptId,
        );
      if (Number(result.changes) !== 1) {
        throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_UNAVAILABLE");
      }
    });
    if (rejection) {
      this.reconnectAllowed = false;
      throw new PhysicalWriteGuardError(rejection);
    }
    session.lastObservedAtMs = nowMs;
    session.configAuthorized = true;
  }

  /** Public/application writes are never permitted in a physical session. */
  rejectApplicationWrite(): void {
    if (this.options.physicalProfile) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_WRITER_DISABLED");
    }
  }

  recordConfigSuccess(): void {
    if (!this.options.physicalProfile) {
      return;
    }
    const session = this.requireSession();
    if (session.configSettled) {
      return;
    }
    if (!session.configAuthorized) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_CONFIG_NOT_AUTHORIZED");
    }
    const nowMs = this.nowMilliseconds();
    transaction(session.database, () => {
      assertLease(session);
      assertAndAdvanceClock(session.database, nowMs);
      const result = session.database
        .prepare(
          "UPDATE physical_attempt SET outcome = 'success', failure_reason = NULL WHERE id = ? AND outcome = 'pending' AND config_request_count = 1",
        )
        .run(session.attemptId);
      if (Number(result.changes) !== 1) {
        throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_UNAVAILABLE");
      }
    });
    session.lastObservedAtMs = nowMs;
    session.configSettled = true;
  }

  recordConfigFailure(reason: string): void {
    if (!this.options.physicalProfile) {
      return;
    }
    const session = this.requireSession();
    if (session.configSettled || !session.configAuthorized) {
      return;
    }
    if (!FAILURE_REASONS.has(reason)) {
      throw new PhysicalWriteGuardError(
        "PHYSICAL_GUARD_FAILURE_REASON_INVALID",
      );
    }
    const nowMs = this.nowMilliseconds();
    let fuseOpened = false;
    transaction(session.database, () => {
      assertLease(session);
      assertAndAdvanceClock(session.database, nowMs);
      const result = session.database
        .prepare(
          "UPDATE physical_attempt SET outcome = 'failure', failure_reason = ? WHERE id = ? AND outcome = 'pending' AND config_request_count = 1",
        )
        .run(reason, session.attemptId);
      if (Number(result.changes) !== 1) {
        throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_UNAVAILABLE");
      }
      const recent = session.database
        .prepare(
          "SELECT outcome FROM physical_attempt WHERE config_request_count = 1 ORDER BY id DESC LIMIT ?",
        )
        .all(MAX_CONSECUTIVE_CONFIG_FAILURES);
      if (
        recent.length === MAX_CONSECUTIVE_CONFIG_FAILURES &&
        recent.every((row) => row.outcome === "failure")
      ) {
        openFuse(
          session.database,
          nowMs,
          "PHYSICAL_GUARD_CONSECUTIVE_FAILURES",
        );
        fuseOpened = true;
      }
    });
    session.lastObservedAtMs = nowMs;
    if (fuseOpened) {
      this.reconnectAllowed = false;
    }
    session.configSettled = true;
  }

  accountIncomingBytes(byteCount: number): void {
    if (!this.options.physicalProfile) {
      return;
    }
    const session = this.requireSession();
    if (!Number.isInteger(byteCount) || byteCount < 0) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_BUDGET_INVALID");
    }
    this.assertDuration(session);
    if (session.bytesReceived + byteCount > this.maximumSessionBytes) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_BYTE_BUDGET_EXCEEDED");
    }
    session.bytesReceived += byteCount;
  }

  accountIncomingFrames(frameCount: number): void {
    if (!this.options.physicalProfile) {
      return;
    }
    const session = this.requireSession();
    if (!Number.isInteger(frameCount) || frameCount < 0) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_BUDGET_INVALID");
    }
    this.assertDuration(session);
    if (session.framesReceived + frameCount > this.maximumSessionFrames) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_FRAME_BUDGET_EXCEEDED");
    }
    session.framesReceived += frameCount;
  }

  /** Release the lease and close every ledger handle. Idempotent after success. */
  releaseSession(reason: string = "closed"): void {
    const session = this.session;
    if (!session) {
      return;
    }
    try {
      if (!session.configSettled && session.configAuthorized) {
        this.recordConfigFailure(reason);
      }
      const nowMs = this.nowMilliseconds();
      transaction(session.database, () => {
        assertLease(session);
        assertAndAdvanceClock(session.database, nowMs);
        if (!session.configAuthorized) {
          session.database
            .prepare(
              "UPDATE physical_attempt SET outcome = 'connection_failure', failure_reason = ? WHERE id = ? AND outcome = 'pending'",
            )
            .run(
              FAILURE_REASONS.has(reason) ? reason : "closed",
              session.attemptId,
            );
        }
        const result = session.database
          .prepare(
            "DELETE FROM physical_lease WHERE singleton = 1 AND owner_token = ?",
          )
          .run(session.ownerToken);
        if (Number(result.changes) !== 1) {
          throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEASE_LOST");
        }
      });
    } finally {
      this.session = undefined;
      session.database.close();
    }
  }

  private requireSession(): ActiveSession {
    if (!this.session) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_SESSION_REQUIRED");
    }
    return this.session;
  }

  private nowMilliseconds(): number {
    const milliseconds = this.clock().getTime();
    if (!Number.isSafeInteger(milliseconds) || milliseconds < 0) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_CLOCK_INVALID");
    }
    return milliseconds;
  }

  private assertDuration(session: ActiveSession): void {
    const nowMs = this.nowMilliseconds();
    if (nowMs < session.lastObservedAtMs) {
      this.reconnectAllowed = false;
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_CLOCK_ROLLBACK");
    }
    session.lastObservedAtMs = nowMs;
    if (nowMs - session.startedAtMs > this.maximumSessionDurationMs) {
      throw new PhysicalWriteGuardError(
        "PHYSICAL_GUARD_DURATION_BUDGET_EXCEEDED",
      );
    }
  }
}

function validatePhysicalOptions(options: PhysicalWriteGuardOptions): void {
  if (
    !options.ledgerPath ||
    !isAbsolute(options.ledgerPath) ||
    !boundedScope(options.candidateId) ||
    !boundedScope(options.qualificationStage)
  ) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_CONFIGURATION_INVALID");
  }
  validateLedgerPath(options.ledgerPath, options.allowedRoot);
}

function boundedScope(value: string | undefined): value is string {
  return Boolean(
    value &&
    value.length <= 512 &&
    !value.includes("\0") &&
    ![...value].some((character) => /[\r\n]/.test(character)),
  );
}

function boundedPositiveInteger(
  configured: number | undefined,
  fallback: number,
  maximum: number,
): number {
  const value = configured ?? fallback;
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_CONFIGURATION_INVALID");
  }
  return value;
}

function validateNonce(nonce: number): void {
  if (!Number.isInteger(nonce) || nonce < 1 || nonce > 0xffff_ffff) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_NONCE_INVALID");
  }
}

function isExactWantConfigRequest(payload: Uint8Array, nonce: number): boolean {
  if (!(payload instanceof Uint8Array)) {
    return false;
  }
  const expected = [0x18];
  let remaining = nonce;
  while (remaining >= 0x80) {
    expected.push((remaining % 0x80) | 0x80);
    remaining = Math.floor(remaining / 0x80);
  }
  expected.push(remaining);
  return (
    payload.length === expected.length &&
    expected.every((value, index) => payload[index] === value)
  );
}

function validateSessionToken(value: string): string {
  if (!/^[A-Za-z0-9._-]{8,128}$/.test(value)) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_SESSION_TOKEN_INVALID");
  }
  return value;
}

function validateLedgerPath(path: string, allowedRoot?: string): void {
  const resolved = resolve(path);
  if (allowedRoot) {
    if (!isAbsolute(allowedRoot)) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_CONFIGURATION_INVALID");
    }
    const root = resolve(allowedRoot);
    const child = relative(root, resolved);
    if (
      !child ||
      child === ".." ||
      child.startsWith(`..\\`) ||
      isAbsolute(child)
    ) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_PATH_OUTSIDE_ROOT");
    }
  }
  for (const candidate of existingPathChain(dirname(resolved))) {
    if (lstatSync(candidate).isSymbolicLink()) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_PATH_UNSAFE");
    }
  }
  if (existsSync(resolved)) {
    const metadata = lstatSync(resolved);
    if (metadata.isSymbolicLink() || !metadata.isFile()) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_PATH_UNSAFE");
    }
    if (metadata.size > MAX_LEDGER_BYTES) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_TOO_LARGE");
    }
  }
}

function existingPathChain(path: string): string[] {
  const paths: string[] = [];
  let current = resolve(path);
  while (existsSync(current)) {
    paths.push(current);
    const parent = dirname(current);
    if (parent === current) {
      break;
    }
    current = parent;
  }
  return paths;
}

function openLedger(path: string, allowedRoot?: string): DatabaseSync {
  validateLedgerPath(path, allowedRoot);
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  validateLedgerPath(path, allowedRoot);
  const existed = existsSync(path);
  let database: DatabaseSync | undefined;
  try {
    database = new DatabaseSync(path);
    database.exec("PRAGMA busy_timeout = 1000");
    if (!existed) {
      database.exec("PRAGMA journal_mode = DELETE");
      initializeLedger(database);
      chmodSync(path, 0o600);
    } else {
      const mode = database.prepare("PRAGMA journal_mode").get()?.journal_mode;
      if (String(mode).toLowerCase() !== "delete") {
        throw new PhysicalWriteGuardError(
          "PHYSICAL_GUARD_LEDGER_SCHEMA_INVALID",
        );
      }
    }
    database.exec("PRAGMA synchronous = FULL");
    database.exec("PRAGMA foreign_keys = ON");
    validateLedgerSchema(database);
    if (statSync(path).size > MAX_LEDGER_BYTES) {
      throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_TOO_LARGE");
    }
    return database;
  } catch (error) {
    try {
      database?.close();
    } catch {
      // Preserve the original fail-closed error.
    }
    throw guardError(error);
  }
}

function initializeLedger(database: DatabaseSync): void {
  database.exec(`
    BEGIN IMMEDIATE;
    CREATE TABLE physical_metadata (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    ) STRICT;
    CREATE TABLE physical_fuse (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      state TEXT NOT NULL CHECK (state IN ('closed', 'open')),
      reason TEXT,
      opened_at_ms INTEGER
    ) STRICT;
    CREATE TABLE physical_lease (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      owner_token TEXT NOT NULL,
      session_digest TEXT NOT NULL UNIQUE CHECK (length(session_digest) = 64),
      acquired_at_ms INTEGER NOT NULL CHECK (acquired_at_ms >= 0)
    ) STRICT;
    CREATE TABLE physical_attempt (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_digest TEXT NOT NULL UNIQUE CHECK (length(session_digest) = 64),
      candidate_digest TEXT NOT NULL CHECK (length(candidate_digest) = 64),
      stage_digest TEXT NOT NULL CHECK (length(stage_digest) = 64),
      started_at_ms INTEGER NOT NULL CHECK (started_at_ms >= 0),
      config_request_count INTEGER NOT NULL CHECK (config_request_count IN (0, 1)),
      nonce_digest TEXT CHECK (nonce_digest IS NULL OR length(nonce_digest) = 64),
      outcome TEXT NOT NULL CHECK (outcome IN ('pending', 'success', 'failure', 'connection_failure')),
      failure_reason TEXT CHECK (failure_reason IS NULL OR failure_reason IN ('connect', 'write', 'timeout', 'decode', 'closed', 'aborted', 'budget'))
    ) STRICT;
    INSERT INTO physical_metadata (key, value) VALUES ('schema_version', '${LEDGER_SCHEMA_VERSION}');
    INSERT INTO physical_metadata (key, value) VALUES ('last_seen_ms', '0');
    INSERT INTO physical_fuse (singleton, state) VALUES (1, 'closed');
    PRAGMA user_version = ${LEDGER_SCHEMA_VERSION};
    COMMIT;
  `);
}

function validateLedgerSchema(database: DatabaseSync): void {
  const version = integerValue(
    database.prepare("PRAGMA user_version").get()?.user_version,
  );
  const metadata = database
    .prepare("SELECT value FROM physical_metadata WHERE key = 'schema_version'")
    .get();
  const tables = database
    .prepare(
      "SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN ('physical_metadata', 'physical_fuse', 'physical_lease', 'physical_attempt') ORDER BY name",
    )
    .all()
    .map((row) => String(row.name));
  if (
    version !== LEDGER_SCHEMA_VERSION ||
    metadata?.value !== String(LEDGER_SCHEMA_VERSION) ||
    tables.join(",") !==
      "physical_attempt,physical_fuse,physical_lease,physical_metadata"
  ) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_SCHEMA_INVALID");
  }
  assertLedgerHealthy(database);
}

function assertLedgerHealthy(database: DatabaseSync): void {
  const rows = database.prepare("PRAGMA quick_check").all();
  if (
    rows.length !== 1 ||
    String(rows[0]?.quick_check).toLowerCase() !== "ok"
  ) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_CORRUPT");
  }
}

function assertAndAdvanceClock(database: DatabaseSync, nowMs: number): void {
  const row = database
    .prepare("SELECT value FROM physical_metadata WHERE key = 'last_seen_ms'")
    .get();
  if (!row) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_SCHEMA_INVALID");
  }
  const lastSeenMs = integerValue(row.value);
  if (nowMs < lastSeenMs) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_CLOCK_ROLLBACK");
  }
  const result = database
    .prepare(
      "UPDATE physical_metadata SET value = ? WHERE key = 'last_seen_ms'",
    )
    .run(String(nowMs));
  if (Number(result.changes) !== 1) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_SCHEMA_INVALID");
  }
}

function transaction(database: DatabaseSync, operation: () => void): void {
  try {
    database.exec("BEGIN IMMEDIATE");
    operation();
    database.exec("COMMIT");
  } catch (error) {
    try {
      database.exec("ROLLBACK");
    } catch {
      // The first error remains authoritative.
    }
    throw guardError(error);
  }
}

function assertLease(session: ActiveSession): void {
  const row = session.database
    .prepare(
      "SELECT owner_token, session_digest FROM physical_lease WHERE singleton = 1",
    )
    .get();
  if (
    row?.owner_token !== session.ownerToken ||
    row.session_digest !== session.sessionDigest
  ) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEASE_LOST");
  }
}

function currentFuseCode(database: DatabaseSync): string | undefined {
  const row = database
    .prepare("SELECT state, reason FROM physical_fuse WHERE singleton = 1")
    .get();
  if (!row || (row.state !== "closed" && row.state !== "open")) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_SCHEMA_INVALID");
  }
  return row.state === "open" ? "PHYSICAL_GUARD_FUSE_OPEN" : undefined;
}

function openFuse(
  database: DatabaseSync,
  nowMs: number,
  reason: string,
): string {
  database
    .prepare(
      "UPDATE physical_fuse SET state = 'open', reason = ?, opened_at_ms = ? WHERE singleton = 1 AND state = 'closed'",
    )
    .run(reason, nowMs);
  return reason;
}

function integerValue(value: unknown): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_SCHEMA_INVALID");
  }
  return number;
}

function opaqueDigest(kind: string, value: string): string {
  return createHash("sha256")
    .update(`cmclient-physical-guard-v1:${kind}:`, "utf8")
    .update(value, "utf8")
    .digest("hex");
}

function guardError(error: unknown): PhysicalWriteGuardError {
  return error instanceof PhysicalWriteGuardError
    ? error
    : new PhysicalWriteGuardError("PHYSICAL_GUARD_LEDGER_UNAVAILABLE");
}
