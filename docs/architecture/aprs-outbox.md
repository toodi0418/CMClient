# APRS-IS Outbox Delivery

APRS uploads are first represented as a durable SQLite `aprs_outbox` entry.
Its identity is `(callsign, canonical_event_id)`, so retries, duplicate local
observations, and repeated command handling cannot create a second upload for
the same mapped callsign and canonical source event. The originally encoded
APRS Data is retained; a repeated enqueue never replaces it with local data.
Migration 13 stores the Mesh network, node, mapping version, trusted event
time, sequence epoch, and sequence number beside that Data. The mapping-scoped
epoch is frozen when the row is accepted and is never recovered by mutating or
rereading the shared canonical position event.

The worker atomically claims due queued or failed entries, transmits each
single APRS Data line through an APRS-IS TCP connection, and records either a
sent timestamp or the stable `APRS_TX_FAILED` error with exponential backoff.
Immediately before transport I/O it compares the row's durable snapshot with
the remote APRS high-water, the delivery watermark, and active snapshots for
the same Mesh network, node, and callsign. A provably older row is deleted
without being reported as sent. An exact remote marker remains eligible for
byte-identical multi-iGate delivery. Equal-time rows without comparable
mapping-scoped sequence evidence receive `APRS_ORDER_UNPROVEN` and remain
fail-closed.
An entry left in `sending` by an interrupted process is returned to `failed`
on the next worker flush with `APRS_TX_INTERRUPTED`, then retried. This makes
crash recovery explicit without treating a local write as a confirmed upload.

Enqueue and local position-state advancement share one SQLite transaction. A
strictly newer event removes older queued or failed rows for its identity, so
a prolonged APRS outage retains one current retry instead of every historical
position. A row already in transport is the only permitted race and claim
serialization limits it to one per identity. Cross-mapping equal-time conflicts
retain at most two durable proofs; later ambiguous candidates are suppressed,
and neither proof is uploaded until a strictly ordered successor resolves the
conflict. A suppressed candidate advances its mapping-local position state but
records `APRS_SKIPPED_OUT_OF_ORDER`; a newly retained conflict proof also emits
`aprs.outbox.failed`. Maintenance removes legacy queued or failed rows only
when trusted event time, exact identity, or a non-null equal mapping version
proves they were superseded. Missing legacy mapping versions are never treated
as comparable merely because both database values are `NULL`.

Marking an entry sent also advances `aprs_delivery_high_water` in the same
SQLite transaction. This bounded record is keyed by Mesh network, node, and
callsign and compares trusted event time before mapping-scoped sequence
epoch/number. Sent outbox rows may be deleted after the configured 90-day
default only when that
watermark proves the exact event or an ordered successor was delivered. The
proof survives outbox retention and blocks re-enqueue after a mapping-version
rotation. Active entries are never age-retention candidates; only queued or
failed entries with durable supersession proof are removed.

The client writes exactly two CRLF-terminated lines per connection: the
configured APRS-IS login followed by the deterministic APRS Data line. Both
inputs reject CR/LF injection. Gateway-specific path information and every
local observation attribute stay outside the stored Data and never alter the
canonical APRS payload.
