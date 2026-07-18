import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { describe, expect, it } from "vitest";

import { createVerifiedGatewayBackup } from "./backup";
import { GatewayDatabase } from "./persistence/database";

describe("verified Gateway backup", () => {
  it("creates a standalone integrity-checked SQLite snapshot", async () => {
    const directory = await mkdtemp(join(tmpdir(), "cmclient-backup-"));
    const source = new GatewayDatabase(join(directory, "gateway.sqlite"));
    source.settings.set("fixture", { value: 42 });

    const result = await createVerifiedGatewayBackup(
      source.connection,
      join(directory, "backups"),
      "backup-fixture",
    );

    expect(result).toMatchObject({
      backupId: "backup-fixture",
      fileName: "backup-fixture.sqlite",
      bytes: expect.any(Number),
      pages: expect.any(Number),
      sha256: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(result.bytes).toBeGreaterThan(0);
    expect(
      await readFile(join(directory, "backups", result.fileName)),
    ).not.toHaveLength(0);
    const snapshot = new DatabaseSync(
      join(directory, "backups", result.fileName),
      {
        readOnly: true,
      },
    );
    expect(
      snapshot
        .prepare("SELECT value FROM settings WHERE key = 'fixture'")
        .get(),
    ).toEqual({ value: '{"value":42}' });
    snapshot.close();
    source.close();
    await rm(directory, { recursive: true, force: true });
  });
});
