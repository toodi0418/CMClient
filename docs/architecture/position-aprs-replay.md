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

Backlog observations now receive `POSITION_BACKLOG` within the same SQLite
transaction that reads the mapping high-water. They are retained as decisions
but cannot create or replace a live marker. This keeps reconnect/API history
from becoming an APRS uplink candidate.
