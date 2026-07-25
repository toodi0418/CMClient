import { createHash } from "node:crypto";
import type { BigIntStats } from "node:fs";
import {
  chmod,
  lstat,
  mkdir,
  open,
  readdir,
  rename,
  rm,
  rmdir,
  stat,
} from "node:fs/promises";
import {
  basename,
  dirname,
  isAbsolute,
  join,
  parse,
  relative,
  resolve,
  sep,
} from "node:path";
import { TextDecoder } from "node:util";
import type { Readable, Writable } from "node:stream";
import { DatabaseSync } from "node:sqlite";

import { backupDatabaseToPath, sha256File } from "./backup.js";
import {
  DatabaseMigrationError,
  DatabaseMigrationRetryableError,
  DatabaseSchemaHistoryError,
  GatewayDatabase,
  gatewayMigrations,
  inspectMigrationHistory,
  isSqliteBusyOrLocked,
  type SchemaHistoryReport,
} from "./persistence/database.js";
import { MigrationManifestError } from "./persistence/migrations.js";

export const OFFLINE_MAINTENANCE_INPUT_MAX_BYTES = 16 * 1024;
export const OFFLINE_MAINTENANCE_OUTPUT_MAX_BYTES = 64 * 1024;
const OFFLINE_MAINTENANCE_PATH_MAX_BYTES = 4096;
const SOURCE_DATABASE_MAX_BYTES = 1024 * 1024 * 1024;
const SOURCE_WAL_MAX_BYTES = 1024 * 1024 * 1024;
const SOURCE_SHM_MAX_BYTES = 64 * 1024 * 1024;
const SNAPSHOT_COPY_BUFFER_BYTES = 64 * 1024;
const WORK_DATABASE_MAX_BYTES = SOURCE_DATABASE_MAX_BYTES + 256 * 1024 * 1024;
const WORK_DIRECTORY_FILE_MAX_BYTES: Readonly<Record<string, number>> =
  Object.freeze({
    "source.sqlite": SOURCE_DATABASE_MAX_BYTES,
    "source.sqlite-journal": SOURCE_DATABASE_MAX_BYTES,
    "source.sqlite-shm": SOURCE_SHM_MAX_BYTES,
    "source.sqlite-wal": SOURCE_WAL_MAX_BYTES,
    "staged.sqlite": WORK_DATABASE_MAX_BYTES,
    "staged.sqlite-journal": WORK_DATABASE_MAX_BYTES,
    "staged.sqlite-shm": SOURCE_SHM_MAX_BYTES,
    "staged.sqlite-wal": SOURCE_WAL_MAX_BYTES,
  });

const DOMAIN_TABLES = Object.freeze([
  { name: "settings", migration: 1 },
  { name: "jobs", migration: 2 },
  { name: "mesh_observations", migration: 3 },
  { name: "nodes", migration: 4 },
  { name: "messages", migration: 4 },
  { name: "telemetry", migration: 4 },
  { name: "position_observations", migration: 5 },
  { name: "position_events", migration: 5 },
  { name: "position_decisions", migration: 5 },
  { name: "node_position_state", migration: 5 },
  { name: "aprs_outbox", migration: 6 },
  { name: "aprs_remote_high_water", migration: 7 },
  { name: "callmesh_mappings", migration: 8 },
  { name: "aprs_delivery_high_water", migration: 12 },
  { name: "callmesh_sync_state", migration: 14 },
  { name: "callmesh_sync_history", migration: 14 },
  { name: "aprs_observed_packets", migration: 18 },
  { name: "aprs_local_transmissions", migration: 18 },
] as const);

export interface OfflineMaintenanceRequest {
  readonly schemaVersion: 1;
  readonly type: "gateway.offline-maintenance";
  readonly sourceDatabasePath: string;
  readonly stagedDatabasePath: string;
}

export interface OfflineDatabaseReport {
  readonly integrity: "ok";
  readonly foreignKeys: "ok";
  readonly schemaHistory: SchemaHistoryReport;
  readonly domainCounts: Readonly<Record<string, number>>;
}

export interface OfflineMaintenanceReport {
  readonly schemaVersion: 1;
  readonly type: "gateway.offline-maintenance-report";
  readonly operation: "backup_migrate_verify";
  readonly sourceDatabaseSha256: string;
  readonly stagedDatabaseSha256: string;
  readonly stagedDatabaseBytes: number;
  readonly integrity: "ok";
  readonly foreignKeyViolations: 0;
  readonly schemaHistory: SchemaHistoryReport["entries"];
  readonly domainCounts: Readonly<Record<string, number>>;
}

interface SourceFileEvidence {
  readonly bytes: number;
  readonly device: bigint;
  readonly inode: bigint;
  readonly path: string;
}

interface HashedSourceFileEvidence extends SourceFileEvidence {
  readonly sha256: string;
}

interface SourceDatabaseInventory {
  readonly main: HashedSourceFileEvidence;
  readonly sharedMemory?: SourceFileEvidence;
  readonly writeAheadLog?: HashedSourceFileEvidence;
}

interface SourceFileEvidencePolicy {
  readonly allowEmpty: boolean;
  readonly includeDigest: boolean;
  readonly required: boolean;
}

interface DirectoryIdentity {
  readonly device: bigint;
  readonly inode: bigint;
  readonly path: string;
}

interface WorkFileIdentity extends DirectoryIdentity {
  readonly bytes: bigint;
}

export class OfflineMaintenanceError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "OfflineMaintenanceError";
  }
}

export class OfflineMaintenanceRetryableError extends OfflineMaintenanceError {
  constructor() {
    super("GATEWAY_OFFLINE_MAINTENANCE_RETRYABLE");
    this.name = "OfflineMaintenanceRetryableError";
  }
}

export async function readOfflineMaintenanceRequest(
  input: Readable,
): Promise<OfflineMaintenanceRequest> {
  const chunks: Buffer[] = [];
  let length = 0;
  for await (const chunk of input) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    length += bytes.length;
    if (length > OFFLINE_MAINTENANCE_INPUT_MAX_BYTES) {
      throw new OfflineMaintenanceError(
        "GATEWAY_OFFLINE_MAINTENANCE_INPUT_OVERSIZED",
      );
    }
    chunks.push(bytes);
  }
  if (length === 0) {
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_INPUT_INVALID",
    );
  }
  let value: unknown;
  try {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(
      Buffer.concat(chunks, length),
    );
    value = JSON.parse(text) as unknown;
  } catch {
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_INPUT_INVALID",
    );
  }
  return validateOfflineMaintenanceRequest(value);
}

export function validateOfflineMaintenanceRequest(
  value: unknown,
): OfflineMaintenanceRequest {
  const keys = [
    "schemaVersion",
    "sourceDatabasePath",
    "stagedDatabasePath",
    "type",
  ];
  if (
    !value ||
    Array.isArray(value) ||
    typeof value !== "object" ||
    JSON.stringify(Object.keys(value).sort()) !== JSON.stringify(keys) ||
    Reflect.get(value, "schemaVersion") !== 1 ||
    Reflect.get(value, "type") !== "gateway.offline-maintenance" ||
    !validPathString(Reflect.get(value, "sourceDatabasePath")) ||
    !validPathString(Reflect.get(value, "stagedDatabasePath"))
  ) {
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_INPUT_INVALID",
    );
  }
  return value as OfflineMaintenanceRequest;
}

export async function runOfflineMaintenanceCommand(
  input: Readable,
  output: Writable,
): Promise<void> {
  const request = await readOfflineMaintenanceRequest(input);
  const report = await runOfflineMaintenance(request);
  const encodedReport = `${JSON.stringify(report)}\n`;
  if (
    Buffer.byteLength(encodedReport, "utf8") >
    OFFLINE_MAINTENANCE_OUTPUT_MAX_BYTES
  ) {
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_OUTPUT_OVERSIZED",
    );
  }
  await new Promise<void>((resolvePromise, reject) => {
    output.write(encodedReport, "utf8", (error) => {
      if (error) {
        reject(error);
      } else {
        resolvePromise();
      }
    });
  });
}

export async function runOfflineMaintenance(
  request: OfflineMaintenanceRequest,
): Promise<OfflineMaintenanceReport> {
  validateOfflineMaintenanceRequest(request);
  const sourcePath = resolve(request.sourceDatabasePath);
  const stagedPath = resolve(request.stagedDatabasePath);
  if (
    databaseFamiliesOverlap(sourcePath, stagedPath) ||
    databaseParentsOverlap(sourcePath, stagedPath)
  ) {
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_PATH_CONFLICT",
    );
  }

  const sourceInventory = await inventorySourceDatabase(
    sourcePath,
    "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_INVALID",
  );
  const sourceParentChain = await directoryIdentityChain(
    dirname(sourcePath),
    "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_INVALID",
  );
  const stagedParentChain = await validateStagedParent(stagedPath);
  if (directoryParentChainsOverlap(sourceParentChain, stagedParentChain)) {
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_PATH_CONFLICT",
    );
  }
  const stagedParent = stagedParentChain.at(-1);
  if (!stagedParent) {
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
    );
  }
  await prepareStagedPath(stagedPath, stagedParent);
  let workingDirectory: DirectoryIdentity | undefined;
  let sourceSnapshot: DatabaseSync | undefined;
  let staged: GatewayDatabase | undefined;
  let report: OfflineMaintenanceReport | undefined;
  let primaryError: Error | undefined;
  let cleanupError: Error | undefined;
  try {
    workingDirectory = await createOwnedWorkingDirectory(
      stagedPath,
      stagedParent,
    );
    const temporaryPath = join(workingDirectory.path, "staged.sqlite");
    const sourceSnapshotPath = join(workingDirectory.path, "source.sqlite");
    await copySourceDatabaseSnapshot(sourceInventory, sourceSnapshotPath);
    await assertSourceDatabaseUnchanged(sourcePath, sourceInventory);
    sourceSnapshot = new DatabaseSync(sourceSnapshotPath, { readOnly: true });
    configureReadOnlyDatabase(sourceSnapshot);
    databaseReport(sourceSnapshot, false);
    await backupDatabaseToPath(sourceSnapshot, temporaryPath);
    sourceSnapshot.close();
    sourceSnapshot = undefined;

    staged = new GatewayDatabase(temporaryPath, gatewayMigrations, {
      atomicMigrationBatch: true,
      busyTimeoutMilliseconds: 0,
    });
    const stagedReport = databaseReport(staged.connection, true);
    checkpointForPublish(staged.connection);
    staged.close();
    staged = undefined;
    await removeWalSidecars(temporaryPath);
    await chmod(temporaryPath, 0o600);

    const verified = new DatabaseSync(temporaryPath, { readOnly: true });
    let verifiedReport: OfflineDatabaseReport;
    try {
      configureReadOnlyDatabase(verified);
      verifiedReport = databaseReport(verified, true);
    } finally {
      verified.close();
    }
    if (JSON.stringify(verifiedReport) !== JSON.stringify(stagedReport)) {
      throw new OfflineMaintenanceError(
        "GATEWAY_OFFLINE_MAINTENANCE_STAGED_CHANGED",
      );
    }
    const stagedFile = await fileFingerprint(temporaryPath);
    await assertDatabaseFamilyAbsent(stagedPath);
    await assertSourceDatabaseUnchanged(sourcePath, sourceInventory);
    await assertOwnedWorkingDirectoryUnchanged(
      stagedParent,
      workingDirectory,
      "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
    );
    await rename(temporaryPath, stagedPath);
    report = Object.freeze({
      schemaVersion: 1,
      type: "gateway.offline-maintenance-report",
      operation: "backup_migrate_verify",
      sourceDatabaseSha256: sourceInventory.main.sha256,
      stagedDatabaseSha256: stagedFile.sha256,
      stagedDatabaseBytes: stagedFile.bytes,
      integrity: verifiedReport.integrity,
      foreignKeyViolations: 0,
      schemaHistory: verifiedReport.schemaHistory.entries,
      domainCounts: verifiedReport.domainCounts,
    });
  } catch (error) {
    primaryError = classifyMaintenanceError(error);
  } finally {
    try {
      staged?.close();
    } catch {
      // Cleanup preserves the primary failure.
    }
    try {
      sourceSnapshot?.close();
    } catch {
      // Cleanup preserves the primary failure.
    }
    if (workingDirectory) {
      try {
        await cleanupOwnedWorkingDirectory(stagedParent, workingDirectory);
      } catch (error) {
        cleanupError = classifyCleanupError(error);
      }
    }
  }
  if (primaryError) {
    throw primaryError;
  }
  if (cleanupError) {
    throw cleanupError;
  }
  if (!report) {
    throw new OfflineMaintenanceError("GATEWAY_OFFLINE_MAINTENANCE_FAILED");
  }
  return report;
}

export function offlineMaintenanceExitCode(error: unknown): number {
  return error instanceof OfflineMaintenanceRetryableError ||
    error instanceof DatabaseMigrationRetryableError ||
    isSqliteBusyOrLocked(error)
    ? 75
    : 1;
}

function databaseReport(
  database: DatabaseSync,
  requireCompleteHistory: boolean,
): OfflineDatabaseReport {
  assertIntegrity(database);
  assertForeignKeys(database);
  const schemaHistory = inspectMigrationHistory(
    database,
    gatewayMigrations,
    requireCompleteHistory,
  );
  const schemaVersion = schemaHistory.entries.at(-1)?.version ?? 0;
  const tableNames = new Set(
    database
      .prepare(
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
      )
      .all()
      .map((row) => String(row.name)),
  );
  const expectedTableNames = new Set<string>();
  if (schemaHistory.digestStatus !== "absent") {
    expectedTableNames.add("schema_migrations");
  }
  for (const table of DOMAIN_TABLES) {
    if (table.migration <= schemaVersion) {
      expectedTableNames.add(table.name);
    }
  }
  if (
    tableNames.size !== expectedTableNames.size ||
    !Array.from(expectedTableNames).every((name) => tableNames.has(name))
  ) {
    throw new DatabaseSchemaHistoryError();
  }
  const counts: Record<string, number> = {};
  for (const table of DOMAIN_TABLES) {
    const expected = table.migration <= schemaVersion;
    if (expected) {
      const count = Number(
        database.prepare(`SELECT COUNT(*) AS count FROM ${table.name}`).get()
          ?.count,
      );
      if (!Number.isSafeInteger(count) || count < 0) {
        throw new OfflineMaintenanceError(
          "DATABASE_DOMAIN_COUNT_REPORT_FAILED",
        );
      }
      counts[table.name] = count;
    }
  }
  return Object.freeze({
    integrity: "ok",
    foreignKeys: "ok",
    schemaHistory,
    domainCounts: Object.freeze(counts),
  });
}

async function inventorySourceDatabase(
  sourcePath: string,
  errorCode: string,
): Promise<SourceDatabaseInventory> {
  try {
    const writeAheadLogPath = `${sourcePath}-wal`;
    const sharedMemoryPath = `${sourcePath}-shm`;
    const rollbackJournalPath = `${sourcePath}-journal`;
    if (
      !validPathString(writeAheadLogPath) ||
      !validPathString(sharedMemoryPath) ||
      !validPathString(rollbackJournalPath) ||
      (await pathExists(rollbackJournalPath))
    ) {
      throw new Error();
    }
    const main = await sourceFileEvidence(
      sourcePath,
      SOURCE_DATABASE_MAX_BYTES,
      { allowEmpty: false, includeDigest: true, required: true },
    );
    const writeAheadLog = await sourceFileEvidence(
      writeAheadLogPath,
      SOURCE_WAL_MAX_BYTES,
      { allowEmpty: true, includeDigest: true, required: false },
    );
    const sharedMemory = await sourceFileEvidence(
      sharedMemoryPath,
      SOURCE_SHM_MAX_BYTES,
      { allowEmpty: false, includeDigest: false, required: false },
    );
    if (!main || !("sha256" in main)) {
      throw new Error();
    }
    if (writeAheadLog && !("sha256" in writeAheadLog)) {
      throw new Error();
    }
    return Object.freeze({
      main,
      ...(sharedMemory ? { sharedMemory } : {}),
      ...(writeAheadLog
        ? { writeAheadLog: writeAheadLog as HashedSourceFileEvidence }
        : {}),
    });
  } catch {
    throw new OfflineMaintenanceError(errorCode);
  }
}

async function sourceFileEvidence(
  path: string,
  maximumBytes: number,
  policy: SourceFileEvidencePolicy,
): Promise<SourceFileEvidence | HashedSourceFileEvidence | undefined> {
  let metadata;
  try {
    metadata = await lstat(path, { bigint: true });
  } catch (error) {
    if (!policy.required && isNotFoundError(error)) {
      return undefined;
    }
    throw error;
  }
  await assertNoLinkedPathComponents(path);
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.nlink !== 1n ||
    (!policy.allowEmpty && metadata.size === 0n) ||
    metadata.size > BigInt(maximumBytes)
  ) {
    throw new Error();
  }
  const evidence = {
    bytes: Number(metadata.size),
    device: metadata.dev,
    inode: metadata.ino,
    path,
  };
  if (!policy.includeDigest) {
    return Object.freeze(evidence);
  }
  const sha256 = await sha256File(path, undefined, maximumBytes);
  const after = await lstat(path, { bigint: true });
  if (!sameSourceFileIdentity(metadata, after)) {
    throw new Error();
  }
  return Object.freeze({ ...evidence, sha256 });
}

async function copySourceDatabaseSnapshot(
  inventory: SourceDatabaseInventory,
  snapshotPath: string,
): Promise<void> {
  await copySourceFile(inventory.main, snapshotPath, SOURCE_DATABASE_MAX_BYTES);
  if (inventory.writeAheadLog) {
    await copySourceFile(
      inventory.writeAheadLog,
      `${snapshotPath}-wal`,
      SOURCE_WAL_MAX_BYTES,
    );
  }
}

async function copySourceFile(
  source: HashedSourceFileEvidence,
  destination: string,
  maximumBytes: number,
): Promise<void> {
  let sourceHandle: Awaited<ReturnType<typeof open>> | undefined;
  let destinationHandle: Awaited<ReturnType<typeof open>> | undefined;
  try {
    sourceHandle = await open(source.path, "r");
    const openedMetadata = await sourceHandle.stat({ bigint: true });
    if (!sourceEvidenceMatchesMetadata(source, openedMetadata)) {
      throw new OfflineMaintenanceError(
        "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_CHANGED",
      );
    }
    destinationHandle = await open(destination, "wx", 0o600);
    const digest = createHash("sha256");
    const buffer = Buffer.allocUnsafe(SNAPSHOT_COPY_BUFFER_BYTES);
    let copiedBytes = 0;
    for (;;) {
      const { bytesRead } = await sourceHandle.read(
        buffer,
        0,
        buffer.length,
        null,
      );
      if (bytesRead === 0) {
        break;
      }
      copiedBytes += bytesRead;
      if (!Number.isSafeInteger(copiedBytes) || copiedBytes > maximumBytes) {
        throw new OfflineMaintenanceError(
          "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_CHANGED",
        );
      }
      digest.update(buffer.subarray(0, bytesRead));
      let written = 0;
      while (written < bytesRead) {
        const result = await destinationHandle.write(
          buffer,
          written,
          bytesRead - written,
          null,
        );
        if (result.bytesWritten < 1) {
          throw new Error();
        }
        written += result.bytesWritten;
      }
    }
    const closedMetadata = await sourceHandle.stat({ bigint: true });
    if (
      copiedBytes !== source.bytes ||
      digest.digest("hex") !== source.sha256 ||
      !sourceEvidenceMatchesMetadata(source, closedMetadata)
    ) {
      throw new OfflineMaintenanceError(
        "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_CHANGED",
      );
    }
    await destinationHandle.sync();
  } catch (error) {
    if (error instanceof OfflineMaintenanceError) {
      throw error;
    }
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_SNAPSHOT_FAILED",
    );
  } finally {
    await destinationHandle?.close().catch(() => undefined);
    await sourceHandle?.close().catch(() => undefined);
  }
  await chmod(destination, 0o600);
}

async function assertSourceDatabaseUnchanged(
  sourcePath: string,
  expected: SourceDatabaseInventory,
): Promise<void> {
  const actual = await inventorySourceDatabase(
    sourcePath,
    "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_CHANGED",
  );
  if (!sameSourceDatabaseInventory(actual, expected)) {
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_SOURCE_CHANGED",
    );
  }
}

function sameSourceDatabaseInventory(
  left: SourceDatabaseInventory,
  right: SourceDatabaseInventory,
): boolean {
  return (
    sameHashedSourceFile(left.main, right.main) &&
    sameOptionalHashedSourceFile(left.writeAheadLog, right.writeAheadLog) &&
    sameOptionalSourceFile(left.sharedMemory, right.sharedMemory)
  );
}

function sameOptionalSourceFile(
  left: SourceFileEvidence | undefined,
  right: SourceFileEvidence | undefined,
): boolean {
  return left === undefined || right === undefined
    ? left === right
    : sameSourceFile(left, right);
}

function sameOptionalHashedSourceFile(
  left: HashedSourceFileEvidence | undefined,
  right: HashedSourceFileEvidence | undefined,
): boolean {
  return left === undefined || right === undefined
    ? left === right
    : sameHashedSourceFile(left, right);
}

function sameHashedSourceFile(
  left: HashedSourceFileEvidence,
  right: HashedSourceFileEvidence,
): boolean {
  return sameSourceFile(left, right) && left.sha256 === right.sha256;
}

function sameSourceFile(
  left: SourceFileEvidence,
  right: SourceFileEvidence,
): boolean {
  return (
    comparablePath(left.path) === comparablePath(right.path) &&
    left.bytes === right.bytes &&
    left.device === right.device &&
    left.inode === right.inode
  );
}

function sourceEvidenceMatchesMetadata(
  source: SourceFileEvidence,
  metadata: BigIntStats,
): boolean {
  return (
    metadata.isFile() &&
    metadata.nlink === 1n &&
    metadata.size === BigInt(source.bytes) &&
    metadata.dev === source.device &&
    metadata.ino === source.inode
  );
}

function sameSourceFileIdentity(
  left: BigIntStats,
  right: BigIntStats,
): boolean {
  return (
    right.isFile() &&
    !right.isSymbolicLink() &&
    right.nlink === 1n &&
    left.size === right.size &&
    left.dev === right.dev &&
    left.ino === right.ino
  );
}

function assertIntegrity(database: DatabaseSync): void {
  try {
    const rows = database.prepare("PRAGMA integrity_check").all();
    if (
      rows.length !== 1 ||
      String(rows[0]?.integrity_check ?? "").toLowerCase() !== "ok"
    ) {
      throw new Error();
    }
  } catch {
    throw new OfflineMaintenanceError("DATABASE_INTEGRITY_CHECK_FAILED");
  }
}

function assertForeignKeys(database: DatabaseSync): void {
  try {
    if (database.prepare("PRAGMA foreign_key_check").all().length !== 0) {
      throw new Error();
    }
  } catch {
    throw new OfflineMaintenanceError("DATABASE_FOREIGN_KEY_CHECK_FAILED");
  }
}

function checkpointForPublish(database: DatabaseSync): void {
  let row: Record<string, unknown> | undefined;
  try {
    row = database.prepare("PRAGMA wal_checkpoint(TRUNCATE)").get();
  } catch (error) {
    if (isSqliteBusyOrLocked(error)) {
      throw new OfflineMaintenanceRetryableError();
    }
    throw new OfflineMaintenanceError("DATABASE_CHECKPOINT_FAILED");
  }
  const busy = Number(row?.busy);
  const log = Number(row?.log);
  const checkpointed = Number(row?.checkpointed);
  if (busy !== 0) {
    throw new OfflineMaintenanceRetryableError();
  }
  if (!Number.isInteger(log) || !Number.isInteger(checkpointed)) {
    throw new OfflineMaintenanceError("DATABASE_CHECKPOINT_FAILED");
  }
}

function configureReadOnlyDatabase(database: DatabaseSync): void {
  try {
    database.exec("PRAGMA query_only = ON");
    database.exec("PRAGMA foreign_keys = ON");
    database.exec("PRAGMA busy_timeout = 0");
  } catch (error) {
    throw classifyMaintenanceError(error);
  }
}

async function validateStagedParent(
  path: string,
): Promise<readonly DirectoryIdentity[]> {
  return directoryIdentityChain(
    dirname(path),
    "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
  );
}

async function prepareStagedPath(
  path: string,
  identity: DirectoryIdentity,
): Promise<void> {
  await assertDirectoryIdentity(
    identity,
    "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
  );
  await reconcileStaleWorkingDirectory(path, identity);
  await assertDatabaseFamilyAbsent(path);
  await assertDirectoryIdentity(
    identity,
    "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
  );
}

async function directoryIdentityChain(
  path: string,
  errorCode: string,
): Promise<readonly DirectoryIdentity[]> {
  try {
    const root = parse(path).root;
    if (!root) {
      throw new Error();
    }
    const components = relative(root, path).split(sep).filter(Boolean);
    const identities: DirectoryIdentity[] = [];
    let current = root;
    for (const component of [undefined, ...components]) {
      if (component !== undefined) {
        current = join(current, component);
      }
      const metadataPath =
        component === undefined ? statCompatibleRoot(root) : current;
      const metadata = await lstat(metadataPath, { bigint: true });
      if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
        throw new Error();
      }
      identities.push(
        Object.freeze({
          device: metadata.dev,
          inode: metadata.ino,
          path: current,
        }),
      );
    }
    return Object.freeze(identities);
  } catch {
    throw new OfflineMaintenanceError(errorCode);
  }
}

function statCompatibleRoot(root: string): string {
  if (process.platform !== "win32" || !root.startsWith("\\\\?\\")) {
    return root;
  }
  if (root.startsWith("\\\\?\\UNC\\")) {
    return `\\\\${root.slice(8)}`;
  }
  return /^\\\\\?\\[A-Za-z]:\\$/u.test(root) ? root.slice(4) : root;
}

async function createOwnedWorkingDirectory(
  stagedPath: string,
  stagedParent: DirectoryIdentity,
): Promise<DirectoryIdentity> {
  await assertDirectoryIdentity(
    stagedParent,
    "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
  );
  const path = maintenanceWorkingDirectoryPath(stagedPath);
  try {
    await mkdir(path, { mode: 0o700 });
    await assertDirectoryIdentity(
      stagedParent,
      "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
    );
    await assertNoLinkedPathComponents(path);
    const metadata = await lstat(path, { bigint: true });
    if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
      throw new Error();
    }
    return Object.freeze({
      device: metadata.dev,
      inode: metadata.ino,
      path,
    });
  } catch (error) {
    if (error instanceof OfflineMaintenanceError) {
      throw error;
    }
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
    );
  }
}

async function reconcileStaleWorkingDirectory(
  stagedPath: string,
  stagedParent: DirectoryIdentity,
): Promise<void> {
  const path = maintenanceWorkingDirectoryPath(stagedPath);
  let metadata: BigIntStats;
  try {
    metadata = await lstat(path, { bigint: true });
  } catch (error) {
    if (isNotFoundError(error)) {
      return;
    }
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
    );
  }
  const errorCode = "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID";
  try {
    if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
      throw new Error();
    }
    const workingDirectory = Object.freeze({
      device: metadata.dev,
      inode: metadata.ino,
      path,
    });
    await removeValidatedWorkingDirectory(
      stagedParent,
      workingDirectory,
      errorCode,
    );
  } catch (error) {
    if (error instanceof OfflineMaintenanceError) {
      throw error;
    }
    throw new OfflineMaintenanceError(errorCode);
  }
}

async function removeValidatedWorkingDirectory(
  stagedParent: DirectoryIdentity,
  workingDirectory: DirectoryIdentity,
  errorCode: string,
): Promise<void> {
  try {
    await assertOwnedWorkingDirectoryUnchanged(
      stagedParent,
      workingDirectory,
      errorCode,
    );
    const entries = await readdir(workingDirectory.path, {
      withFileTypes: true,
    });
    const files: WorkFileIdentity[] = [];
    for (const entry of entries) {
      const maximumBytes = WORK_DIRECTORY_FILE_MAX_BYTES[entry.name];
      if (
        maximumBytes === undefined ||
        !entry.isFile() ||
        entry.isSymbolicLink()
      ) {
        throw new Error();
      }
      const entryPath = join(workingDirectory.path, entry.name);
      const entryMetadata = await lstat(entryPath, { bigint: true });
      if (
        !entryMetadata.isFile() ||
        entryMetadata.isSymbolicLink() ||
        entryMetadata.nlink !== 1n ||
        entryMetadata.size > BigInt(maximumBytes)
      ) {
        throw new Error();
      }
      files.push(
        Object.freeze({
          bytes: entryMetadata.size,
          device: entryMetadata.dev,
          inode: entryMetadata.ino,
          path: entryPath,
        }),
      );
    }

    await assertOwnedWorkingDirectoryUnchanged(
      stagedParent,
      workingDirectory,
      errorCode,
    );
    for (const file of files) {
      await assertWorkFileIdentity(file, errorCode);
    }
    for (const file of files) {
      await assertWorkFileIdentity(file, errorCode);
      await rm(file.path);
    }
    await assertOwnedWorkingDirectoryUnchanged(
      stagedParent,
      workingDirectory,
      errorCode,
    );
    if ((await readdir(workingDirectory.path)).length !== 0) {
      throw new Error();
    }
    await assertOwnedWorkingDirectoryUnchanged(
      stagedParent,
      workingDirectory,
      errorCode,
    );
    await rmdir(workingDirectory.path);
  } catch (error) {
    if (error instanceof OfflineMaintenanceError) {
      throw error;
    }
    throw new OfflineMaintenanceError(errorCode);
  }
}

async function assertWorkFileIdentity(
  expected: WorkFileIdentity,
  errorCode: string,
): Promise<void> {
  try {
    const actual = await lstat(expected.path, { bigint: true });
    if (
      !actual.isFile() ||
      actual.isSymbolicLink() ||
      actual.nlink !== 1n ||
      actual.size !== expected.bytes ||
      actual.dev !== expected.device ||
      actual.ino !== expected.inode
    ) {
      throw new Error();
    }
  } catch {
    throw new OfflineMaintenanceError(errorCode);
  }
}

function maintenanceWorkingDirectoryPath(stagedPath: string): string {
  const stagedName = basename(stagedPath);
  const hiddenPrefix = stagedName.startsWith(".") ? "" : ".";
  return join(
    dirname(stagedPath),
    `${hiddenPrefix}${stagedName}.maintenance-work`,
  );
}

async function assertOwnedWorkingDirectoryUnchanged(
  stagedParent: DirectoryIdentity,
  workingDirectory: DirectoryIdentity,
  errorCode: string,
): Promise<void> {
  await assertDirectoryIdentity(stagedParent, errorCode);
  if (
    comparablePath(dirname(workingDirectory.path)) !==
    comparablePath(stagedParent.path)
  ) {
    throw new OfflineMaintenanceError(errorCode);
  }
  await assertDirectoryIdentity(workingDirectory, errorCode);
}

async function cleanupOwnedWorkingDirectory(
  stagedParent: DirectoryIdentity,
  workingDirectory: DirectoryIdentity,
): Promise<void> {
  const errorCode = "GATEWAY_OFFLINE_MAINTENANCE_CLEANUP_FAILED";
  await removeValidatedWorkingDirectory(
    stagedParent,
    workingDirectory,
    errorCode,
  );
}

async function assertDirectoryIdentity(
  expected: DirectoryIdentity,
  errorCode: string,
): Promise<void> {
  try {
    await assertNoLinkedPathComponents(expected.path);
    const actual = await lstat(expected.path, { bigint: true });
    if (
      !actual.isDirectory() ||
      actual.isSymbolicLink() ||
      actual.dev !== expected.device ||
      actual.ino !== expected.inode
    ) {
      throw new Error();
    }
  } catch {
    throw new OfflineMaintenanceError(errorCode);
  }
}

async function assertDatabaseFamilyAbsent(path: string): Promise<void> {
  for (const member of databaseFamilyPaths(path)) {
    await assertPathAbsent(member);
  }
}

function databaseFamiliesOverlap(left: string, right: string): boolean {
  const leftFamily = new Set(
    databaseFamilyPaths(left).map((path) => comparablePath(path)),
  );
  return databaseFamilyPaths(right).some((path) =>
    leftFamily.has(comparablePath(path)),
  );
}

function databaseParentsOverlap(left: string, right: string): boolean {
  const leftParent = dirname(left);
  const rightParent = dirname(right);
  return (
    pathIsEqualOrDescendant(leftParent, rightParent) ||
    pathIsEqualOrDescendant(rightParent, leftParent)
  );
}

function directoryParentChainsOverlap(
  left: readonly DirectoryIdentity[],
  right: readonly DirectoryIdentity[],
): boolean {
  const leftParent = left.at(-1);
  const rightParent = right.at(-1);
  if (!leftParent || !rightParent) {
    return true;
  }
  return (
    right.some((identity) => sameDirectoryIdentity(leftParent, identity)) ||
    left.some((identity) => sameDirectoryIdentity(rightParent, identity))
  );
}

function sameDirectoryIdentity(
  left: DirectoryIdentity,
  right: DirectoryIdentity,
): boolean {
  return left.device === right.device && left.inode === right.inode;
}

function pathIsEqualOrDescendant(
  path: string,
  possibleParent: string,
): boolean {
  const pathDifference = relative(
    comparablePath(possibleParent),
    comparablePath(path),
  );
  return (
    pathDifference === "" ||
    (!isAbsolute(pathDifference) &&
      pathDifference !== ".." &&
      !pathDifference.startsWith(`..${sep}`))
  );
}

function databaseFamilyPaths(path: string): readonly string[] {
  return [path, `${path}-wal`, `${path}-shm`, `${path}-journal`];
}

async function assertNoLinkedPathComponents(path: string): Promise<void> {
  const root = parse(path).root;
  const components = relative(root, path).split(sep).filter(Boolean);
  let current = root;
  for (const component of components) {
    current = join(current, component);
    if ((await lstat(current)).isSymbolicLink()) {
      throw new OfflineMaintenanceError(
        "GATEWAY_OFFLINE_MAINTENANCE_LINKED_PATH",
      );
    }
  }
}

async function assertPathAbsent(path: string): Promise<void> {
  try {
    await lstat(path);
  } catch (error) {
    if (isNotFoundError(error)) {
      return;
    }
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_STAGED_PATH_INVALID",
    );
  }
  throw new OfflineMaintenanceError(
    "GATEWAY_OFFLINE_MAINTENANCE_STAGED_CONFLICT",
  );
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (isNotFoundError(error)) {
      return false;
    }
    throw error;
  }
}

function isNotFoundError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "ENOENT"
  );
}

async function fileFingerprint(path: string): Promise<{
  bytes: number;
  sha256: string;
}> {
  const metadata = await stat(path);
  if (!metadata.isFile() || !Number.isSafeInteger(metadata.size)) {
    throw new OfflineMaintenanceError(
      "GATEWAY_OFFLINE_MAINTENANCE_FILE_INVALID",
    );
  }
  return Object.freeze({
    bytes: metadata.size,
    sha256: await sha256File(path),
  });
}

async function removeWalSidecars(path: string): Promise<void> {
  await Promise.all([
    rm(`${path}-wal`, { force: true }),
    rm(`${path}-shm`, { force: true }),
  ]);
}

function classifyMaintenanceError(error: unknown): Error {
  if (error instanceof OfflineMaintenanceError) {
    return error;
  }
  if (
    error instanceof DatabaseMigrationRetryableError ||
    isSqliteBusyOrLocked(error)
  ) {
    return new OfflineMaintenanceRetryableError();
  }
  if (
    error instanceof DatabaseMigrationError ||
    error instanceof MigrationManifestError
  ) {
    return error;
  }
  return new OfflineMaintenanceError("GATEWAY_OFFLINE_MAINTENANCE_FAILED");
}

function classifyCleanupError(error: unknown): Error {
  return error instanceof OfflineMaintenanceError &&
    error.code === "GATEWAY_OFFLINE_MAINTENANCE_CLEANUP_FAILED"
    ? error
    : new OfflineMaintenanceError("GATEWAY_OFFLINE_MAINTENANCE_CLEANUP_FAILED");
}

function validPathString(value: unknown): value is string {
  return (
    typeof value === "string" &&
    isAbsolute(value) &&
    Buffer.byteLength(value, "utf8") <= OFFLINE_MAINTENANCE_PATH_MAX_BYTES &&
    !Array.from(value).some((character) => character.charCodeAt(0) < 0x20)
  );
}

function comparablePath(path: string): string {
  const normalized = resolve(path);
  return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}
