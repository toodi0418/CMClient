# Legacy Settings Migration

`cmclient-migrate` is a one-shot offline migration tool. It reads a Legacy
`client-preferences.json` file (or an Electron response wrapper containing a
`preferences` object), produces a machine-readable dry-run report, and is the
only CMClient 2.0 code that understands this legacy shape. The Agent, Gateway,
Desktop, and normal `cmclient` control client do not parse or retain it.

```bash
cmclient-migrate settings \
  --source /absolute/path/client-preferences.json \
  --dry-run
```

The report includes stable codes and field names, never legacy values. It is
bounded to 64 KiB and rejects malformed JSON or a non-object root. The one
currently safe mapping is deliberately small:

| Legacy field | CMClient 2.0 result |
| --- | --- |
| `webDashboardEnabled` boolean | `[agent] management_web_enabled` candidate |
| TCP/serial host, path, baud, mode | `LEGACY_SETTINGS_TRANSPORT_REQUIRES_REVIEW` |
| APRS endpoint/interval | `LEGACY_SETTINGS_APRS_REQUIRES_REVIEW` |
| `shareWithTenmanMap` | `LEGACY_SETTINGS_REMOVED_TENMAN` |
| Legacy traceroute controls | `LEGACY_SETTINGS_REMOVED_LEGACY_RUNTIME_FEATURE` |
| Keys/tokens/passcodes/passwords/secrets | `LEGACY_SETTINGS_SECRET_SKIPPED` |

No legacy credential is moved to the Agent-selected secret backend automatically. An
operator must add a current credential through `cmclient secret set` after
reviewing the report. Transport and APRS fields require current CMClient 2.0
configuration because their legacy semantics cannot be trusted or mapped to a
runtime compatibility layer.

After review, write mode creates a new absolute Agent config path only:

```bash
cmclient-migrate settings \
  --source /absolute/path/client-preferences.json \
  --write-agent-config /absolute/path/new-config/agent.toml
```

Write mode never merges or overwrites an existing file, touches no SQLite data,
and uses a synced same-directory temporary file followed by an atomic
no-clobber target creation. Existing runtime configuration is therefore
protected from accidental migration changes.

## Legacy History Migration

`cmclient-migrate data` is a separate, offline import path for Legacy user
history. It never runs from the Agent, Gateway, Desktop, or normal `cmclient`
control client. Stop the Gateway first, inspect the report, and only then apply
with an explicit confirmation:

```bash
cmclient-migrate data import \
  --source-dir /absolute/path/legacy-artifacts \
  --target-database /absolute/path/gateway.sqlite \
  --mesh-network-id local-mesh \
  --backup-dir /absolute/path/migration-backups

cmclient-migrate data import \
  --source-dir /absolute/path/legacy-artifacts \
  --target-database /absolute/path/gateway.sqlite \
  --mesh-network-id local-mesh \
  --backup-dir /absolute/path/migration-backups \
  --apply --confirm-gateway-stopped
```

The dry-run report contains a deterministic migration ID, source filenames and
SHA-256 digests, bounded record counts, skipped-record codes, and no Legacy
payload values. Before mutation the tool checks the target Gateway schema and
SQLite integrity, then uses SQLite's backup API to create a verified standalone
snapshot. A `data_version` check brackets snapshot creation and an exclusive
SQLite transaction prevents writes during import. If another connection writes
inside the snapshot window, the import fails closed. The supplied backup
directory must be private and owned by the current user; the tool creates a
missing final directory with private permissions but never changes an existing
directory's permissions.

Each snapshot has a strict JSON proof manifest containing the migration ID,
Gateway schema version, canonical target-path digest, snapshot filename, and
snapshot SHA-256. The same manifest is stored in the target import marker. A
rollback must match all three artifacts before it can mutate a healthy target.
The backup and manifest form a durable migration journal: a repeated invocation
returns the already-verified result, a pre-commit orphan is removed only when
the target proves no import occurred, and ambiguous or tampered state returns
`LEGACY_DATA_IMPORT_RECOVERY_REQUIRED` without deleting evidence.

After commit the tool verifies projected row counts, marker equality, SQLite
integrity, and foreign keys. Verification failure restores the snapshot
automatically. `telemetry_time_seconds` values that cannot satisfy the current
Gateway schema, including zero, are imported as unknown (`NULL`) rather than
causing the whole migration to fail.

The importer reads only these Legacy artifacts when present:

| Source | Imported projection | Deliberately excluded |
| --- | --- | --- |
| `callmesh-data.sqlite` or `node-database.json` | Latest valid node identity/metadata | coordinates and legacy position/APRS state |
| `callmesh-data.sqlite`, `message-log.jsonl`, or its `.migrated` fallback | Valid text-message history with sender, packet ID, channel, and event time | replies as a destination, raw frames, radio metadata, relay details |
| `telemetry-records.sqlite`, `telemetry-records.jsonl`, or its `.migrated` fallback | Valid scalar telemetry metrics and sample time | arbitrary JSON metric payloads and raw observations |

Imported message and telemetry history is marked as `backlog` with a minimal
non-packet observation envelope. It therefore cannot become a current transport
event, advance APRS high-water state, enqueue APRS Data, or establish a position
claim. CallMesh credentials/mappings, APRS cache/backtrack data, TENMAN/TENMAP
state, traceroutes, raw payloads, logs, and all unrecognized artifacts are not
migrated.

To restore the verified pre-import snapshot, while the Gateway remains stopped:

```bash
cmclient-migrate data rollback \
  --target-database /absolute/path/gateway.sqlite \
  --backup-database /absolute/path/migration-backups/legacy-data-<id>.sqlite \
  --confirm-gateway-stopped
```

Rollback checks the manifest, target identity, snapshot SHA-256, current Gateway
schema, SQLite integrity, and foreign keys. A healthy target additionally needs
the exact import marker. A missing or corrupt target may be recovered from the
external proof: the tool verifies a private restore candidate, quarantines the
damaged DB/WAL/SHM files, and atomically switches the candidate into place. It
does not overwrite a healthy unrelated SQLite database.
