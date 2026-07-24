import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { chmod, mkdir, rm, stat } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { backup, DatabaseSync } from "node:sqlite";

export interface GatewayBackupResult {
  backupId: string;
  fileName: string;
  bytes: number;
  pages: number;
  sha256: string;
}

export class GatewayBackupError extends Error {
  readonly code = "DATABASE_BACKUP_FAILED";

  constructor() {
    super("DATABASE_BACKUP_FAILED");
    this.name = "GatewayBackupError";
  }
}

export async function backupDatabaseToPath(
  source: DatabaseSync,
  destination: string,
  signal?: AbortSignal,
): Promise<number> {
  if (!isAbsolute(destination)) {
    throw new GatewayBackupError();
  }
  try {
    signal?.throwIfAborted();
    const pages = await backup(source, destination);
    signal?.throwIfAborted();
    return pages;
  } catch (error) {
    await removeDatabaseFiles(destination);
    throw error;
  }
}

export async function createVerifiedGatewayBackup(
  source: DatabaseSync,
  backupDirectory: string,
  backupId: string,
  signal?: AbortSignal,
): Promise<GatewayBackupResult> {
  if (!isAbsolute(backupDirectory) || !/^[a-zA-Z0-9-]{1,96}$/.test(backupId)) {
    throw new GatewayBackupError();
  }
  const fileName = `${backupId}.sqlite`;
  const destination = join(backupDirectory, fileName);
  try {
    signal?.throwIfAborted();
    await mkdir(backupDirectory, { recursive: true, mode: 0o700 });
    await chmod(backupDirectory, 0o700);
    signal?.throwIfAborted();
    const pages = await backupDatabaseToPath(source, destination, signal);
    signal?.throwIfAborted();
    await chmod(destination, 0o600);
    verifyBackup(destination);
    signal?.throwIfAborted();
    const metadata = await stat(destination);
    return {
      backupId,
      fileName,
      bytes: metadata.size,
      pages,
      sha256: await sha256File(destination, signal),
    };
  } catch {
    await rm(destination, { force: true }).catch(() => undefined);
    throw new GatewayBackupError();
  }
}

function verifyBackup(path: string): void {
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    const rows = database.prepare("PRAGMA integrity_check").all();
    if (
      rows.length !== 1 ||
      String(rows[0]?.integrity_check ?? "").toLowerCase() !== "ok"
    ) {
      throw new GatewayBackupError();
    }
  } finally {
    database.close();
  }
}

export async function sha256File(
  path: string,
  signal?: AbortSignal,
  maximumBytes = Number.MAX_SAFE_INTEGER,
): Promise<string> {
  if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 0) {
    throw new GatewayBackupError();
  }
  const hash = createHash("sha256");
  let bytes = 0;
  for await (const chunk of createReadStream(path)) {
    signal?.throwIfAborted();
    bytes += chunk.length;
    if (!Number.isSafeInteger(bytes) || bytes > maximumBytes) {
      throw new GatewayBackupError();
    }
    hash.update(chunk);
  }
  return hash.digest("hex");
}

export async function removeDatabaseFiles(path: string): Promise<void> {
  await Promise.all([
    rm(path, { force: true }),
    rm(`${path}-wal`, { force: true }),
    rm(`${path}-shm`, { force: true }),
  ]);
}
