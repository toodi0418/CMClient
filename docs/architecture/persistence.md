# Gateway Persistence Foundation

Gateway persistence uses local SQLite through Node's `node:sqlite` runtime.
Every database enables WAL mode, foreign keys, and a busy timeout before
migrations run. The migration journal is forward-only and each unapplied
migration plus its journal row executes in one `BEGIN IMMEDIATE` transaction.

The forward migration set contains settings, Jobs, Mesh observations/domain
records, Position observations/events/decisions/high-water state, APRS outbox
and remote high-water state, CallMesh mappings, and telemetry range indexes.
Migration 10 adds the retention access paths used to delete bounded batches
without repeated full-table scans. Migration 11 adds exact ordering indexes for
the bounded Nodes, Messages, Telemetry, Positions, APRS outbox, and due-work
projections; query-plan tests reject a temporary sort regression. Migration 12
adds bounded Message and Position-history cleanup indexes, the queued Job
type/order index, and `aprs_delivery_high_water`. The delivery table has one row
per Mesh network, node, and callsign and records only a successfully persisted
APRS send; it does not replace the mapping-version-scoped local or remote
high-water tables.
Migration 13 backfills immutable source-order snapshots into existing APRS
outbox rows and records the mapping version on new delivery watermarks. Legacy
rows retain a null mapping version. Equal-time legacy events with different
canonical identities always fail closed because `NULL/NULL` cannot prove that
their mapping-derived sequence epochs are comparable.
The Job table stores a type, JSON execution input/result, stable error
code/params, lifecycle timestamps, cancellation flag, and a partial unique
`(type, idempotency_key)` index. Public APIs never return stored execution input
or result. Repositories serialize settings values as JSON and never accept
secret storage through this layer.

Gateway runs Telemetry, Message, and Position-history retention in bounded
batches. Telemetry, Messages, and Position history default to 30 days. Position
cleanup deletes decisions first, then canonical events and observations only
when no mapping high-water, delivery high-water, active or retained outbox row,
or remaining history row references them. The final Mesh-observation cleanup
similarly removes only old observations no longer referenced by a node or domain
record. Its cutoff is the latest of the Telemetry, Message, and Position
cutoffs, so asymmetric retention policies do not leave unreferenced rows behind
under the shortest domain policy.
Its single bounded delete is sized to at least the Telemetry, Message, and
Position batch limits combined plus a fixed 1,000-row headroom. The headroom
drains pre-existing orphans instead of merely keeping pace with references
released during the current cycle.

Terminal Jobs have an independent 90-day default and are also deleted
incrementally; queued and running Jobs are never retention candidates. The
idempotency key lifetime matches this configured terminal retention window.
Sent APRS outbox rows use a separate 90-day bounded cleanup. A sent row is
deleted only after the delivery high-water proves that exact event or a newer
ordered event was delivered. This
keeps exact upload idempotency after the outbox row expires and after a mapping
version rotates. Queued and failed rows are retained only while current or
ordering-ambiguous; enqueue and bounded maintenance cleanup delete rows proven
superseded without changing them to `sent`. Each maintenance cycle finishes
with a passive WAL checkpoint and publishes only its numeric frame/busy
counters. Batch size and retention age are configurable independently.

Job execution is bounded to two concurrent handlers by default. Additional
durable submissions remain queued and drain in submission order as slots become
available. The in-memory queue defaults to 1,024 entries; overflow fails before
SQLite insertion, while restart recovery reads and refills only a bounded
window.

Integrity check and verified SQLite backup remain persistent Jobs. Backup
publication is accepted only after read-only `PRAGMA integrity_check` succeeds
and the file digest is computed. Agent updater backup/restore remains a separate
release transaction; the Gateway backup path never overwrites the live database.
