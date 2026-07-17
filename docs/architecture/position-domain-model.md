# Position Domain Model

Position processing persists four distinct records before any APRS operation:

- `PositionObservation` records one Gateway's local reception context. It
  contains gateway/transport metadata, all ingest/session timestamps, radio
  quality fields, backlog classification, payload hash, and the decoded
  position sample.
- `PositionCanonicalEvent` represents one source position after identity and
  ordering stages have selected it. Its event time source is explicitly
  `position_timestamp`, `position_time`, or `sequence`; device `rx_time` is
  never an event time source.
- `PositionDecision` records a stable accept, duplicate, historical, clock,
  precision, sequence, or quarantine code with typed parameters.
- `NodePositionState` is the high-water record. Its composite identity is
  `(mesh_network_id, node_num, callsign, mapping_version)`, so mapping changes
  and separate Mesh networks cannot overwrite one another's live marker.

SQLite stores observations, events, decisions, and node state in independent
tables. Later P05 slices fill canonical identity, sequence epochs, validation,
and transactional high-water updates. An older event may remain historical but
must not replace a state row's latest canonical event or drive APRS upload.

Canonical identity is SHA-256 over an explicit version marker, Mesh network,
node number, selected source-time representation, sequence, complete decoded
position sample, and payload hash. It excludes packet ID, Gateway ID, RSSI,
SNR, transport, and every local observation timestamp. Therefore the same RF
event observed by multiple iGates converges to one canonical key, while packet
ID reuse with changed payload content produces a separate event. SQLite's
unique canonical key records the first event and writes `POSITION_DUPLICATE`
for later local observations without replacing the source event.
