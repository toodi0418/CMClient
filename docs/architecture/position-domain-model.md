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

`aprs_delivery_high_water` is a separate delivery proof keyed by
`(mesh_network_id, node_num, callsign)`. It advances only when a durable outbox
submission is later observed by the receive monitor with an exact source,
destination, and information match. Confirmation and advancement share one
SQLite transaction and retain the canonical event time plus sequence order; a
socket write alone never advances it. It prevents an already delivered
event from becoming eligible again after sent-outbox retention or a mapping
version change. It never merges or replaces the per-mapping local state or
remote monitor state.

`aprs_legacy_submission_barriers` separately retains the newest ordering
snapshot that an older release recorded after a socket write. It is not proof
of APRS-IS delivery and never populates `aprs_delivery_high_water`, but it is a
permanent fail-closed boundary for position admission, enqueue, and final
pre-send authorization. This prevents downgrade replay after migration without
misrepresenting legacy transport success as observer confirmation.

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

Backlog eligibility is evaluated from the observation currently being
processed, not the arbitrary first observation referenced by the canonical
event. A source event first seen through an API backlog can therefore be
accepted when a later live observation proves it is current, while both iGates
still converge on the same canonical key and APRS bytes. Ordering decisions
retain the current observation ID for auditability.

High-water updates run in a SQLite `BEGIN IMMEDIATE` transaction scoped by the
state composite key. A later trusted event time advances the state; an earlier
one is retained as `POSITION_HISTORICAL`. When time is equal, a greater sequence
can advance the same epoch, while equal sequence is a conflict. A lower sequence
only starts a new epoch when its trusted source time is later, which accounts
for reboot or wrap without treating a late packet as live. A cold start with
sequence but no reliable source time is `APRS_SKIPPED_OUT_OF_ORDER` and does
not create a high-water row. Before creating a new mapping-version state, the
delivery proof is checked with event time first and sequence second. Missing or
ambiguous ordering evidence fails closed.

Sequence epochs belong to the `(network, node, callsign, mapping_version)`
state, not to `PositionCanonicalEvent`. An accepted callback receives the
mapping-scoped epoch for its outbox snapshot, while the canonical row remains
unchanged. Mapping rotation before transmission and process restart therefore
cannot rewrite the ordering evidence later consumed by the APRS worker.

Before high-water/APRS use, validation requires `precisionBits === 32` and
both integer coordinates. GPS source time must be after 2000, no more than five
minutes ahead of local time, and may be quarantined when a supplied trusted
baseline shows a forward jump over the configured limit. MSL altitude is used
only when explicitly present, including zero; HAE is retained as observation
data but never substituted for MSL. Speed and ground track are emitted only as
a pair. A partial pair is omitted, while an impossible speed or track produces
`POSITION_SPEED_ANOMALY`.
