# Gateway Persistence Foundation

Gateway persistence uses local SQLite through Node's `node:sqlite` runtime.
Every database enables WAL mode, foreign keys, and a busy timeout before
migrations run. The migration journal is forward-only and each unapplied
migration plus its journal row executes in one `BEGIN IMMEDIATE` transaction.

The forward migration set contains settings, Jobs, Mesh observations/domain
records, Position observations/events/decisions/high-water state, APRS outbox
and remote high-water state, CallMesh mappings, and telemetry range indexes.
The Job table stores a type, JSON execution input/result, stable error
code/params, lifecycle timestamps, cancellation flag, and a partial unique
`(type, idempotency_key)` index. Public APIs never return stored execution input
or result. Repositories serialize settings values as JSON and never accept
secret storage through this layer.

Gateway runs telemetry retention in bounded batches and exposes integrity check
and verified SQLite backup as persistent Jobs. Backup publication is accepted
only after read-only `PRAGMA integrity_check` succeeds and the file digest is
computed. Agent updater backup/restore remains a separate release transaction;
the Gateway backup path never overwrites the live database.
