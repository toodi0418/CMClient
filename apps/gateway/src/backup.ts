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

export async function createVerifiedGatewayBackup(
  source: DatabaseSync,
  backupDirectory: string,
  backupId: string,
): Promise<GatewayBackupResult> {
  if (!isAbsolute(backupDirectory) || !/^[a-zA-Z0-9-]{1,96}$/.test(backupId)) {
    throw new GatewayBackupError();
  }
  const fileName = `${backupId}.sqlite`;
  const destination = join(backupDirectory, fileName);
  try {
    await mkdir(backupDirectory, { recursive: true, mode: 0o700 });
    await chmod(backupDirectory, 0o700);
    const pages = await backup(source, destination);
    await chmod(destination, 0o600);
    verifyBackup(destination);
    const metadata = await stat(destination);
    return {
      backupId,
      fileName,
      bytes: metadata.size,
      pages,
      sha256: await sha256File(destination),
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

async function sha256File(path: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) {
    hash.update(chunk);
  }
  return hash.digest("hex");
}
