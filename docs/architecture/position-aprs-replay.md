# Position/APRS Replay Matrix

`test/fixtures/position-aprs-replay.json` is a synthetic, de-identified
timeline. The replay test applies records in server-ingest order while keeping
source event time, session time, ingest time, and backlog classification
separate.

The matrix verifies that two independent iGates create byte-identical APRS
Data despite different gateway, transport, and MQTT metadata. It then covers a
late live event after a newer one, API backlog exclusion, same-second sequence
ordering, a later-time reboot sequence epoch, invalid future clock rejection,
sequence-only cold start, remote marker reconciliation, and mapping-version
isolation. It also locks precision 32, MSL altitude zero, and partial
speed/track behavior.

Outbox retention regression coverage submits a canonical event, independently
observes the exact source, destination, and information tuple, expires its
observer-confirmed row, rotates the mapping version, and replays the same
event. The durable delivery watermark must keep the replay ineligible, produce
no new outbox row, and perform no second APRS upload while the
mapping-version-scoped position states remain isolated. A separate regression
expires an unobserved submission and proves that it creates no delivery
watermark.

Migration regression coverage also upgrades a newer legacy `sent` snapshot
beside an older queued snapshot. The old watermark becomes a permanent order
barrier rather than delivery proof; the worker flush performs zero transport
writes, and replay admission remains fail-closed. Destinationless observer and
local caches migrate as suppression wildcards but are never accepted as
confirmation evidence.

Ordering regressions also freeze a queued snapshot across a pre-send mapping
rotation, preserve a reboot-generated epoch across a new store instance, and
exercise `E1 failed -> E2 delivered -> E1 due`. The old retry must be absent at
its due time. Equal-time snapshots from different mapping versions remain
bounded and produce no transport I/O because their sequence epochs are not
comparable. The same rule applies to upgraded snapshots whose mapping version
cannot be recovered: `NULL/NULL` never makes their legacy sequence epochs
comparable. A queued local retry is also rechecked against a newer remote
marker before I/O so another iGate cannot cause a later cross-site rollback.

Backlog observations now receive `POSITION_BACKLOG` within the same SQLite
transaction that reads the mapping high-water. They are retained as decisions
but cannot create or replace a live marker. This keeps reconnect/API history
from becoming an APRS uplink candidate.
