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

The worker atomically claims due queued or failed entries and transmits each
single APRS Data line through an APRS-IS TCP connection. A completed socket
write records transport `status = sent` and delivery
`delivery_status = submitted`; it does not prove APRS-IS observation. A failed
write records the stable `APRS_TX_FAILED` error with exponential backoff.
Every stored and transmitted Data line is limited to 510 UTF-8 bytes so its
required CRLF remains inside APRS-IS's inclusive 512-byte line limit. Rows from
an older database that exceed that limit are discarded before transport.
Immediately before transport I/O it compares the row's durable snapshot with
the remote APRS high-water, the observer-confirmed delivery watermark, the
permanent legacy-submission order barrier, and active snapshots for the same
Mesh network, node, and callsign. A provably older row is deleted
without being reported as sent. An exact remote marker remains eligible for
byte-identical multi-iGate delivery. Equal-time rows without comparable
mapping-scoped sequence evidence receive `APRS_ORDER_UNPROVEN` and remain
fail-closed.
An entry left in `sending` by an interrupted process is returned to `failed`
on the next worker flush with `APRS_TX_INTERRUPTED`, then retried. This makes
crash recovery explicit without treating a local write as a confirmed upload.
The runtime also supplies a per-entry observer-readiness gate to the worker and
the concrete TCP writer. If the verified RX session terminates during a claimed
batch, the writer performs no later Data write and the worker releases every
unwritten claim back to `queued` without incrementing attempts. A fenced write
does not tear down a still-valid provision-scoped TX session. Mapping refreshes
only hot-swap the receive monitor's local exact matcher and do not interrupt this
readiness gate. A provision, derived observer identity, or fixed-filter change
fences the gate and reconnects RX; outbound work resumes only after the new
observer session is verified.

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

The receive-only monitor confirms a submitted entry only when source callsign,
destination, and information are byte-identical, the submission belongs to the
current provision fingerprint, the observation is inside its submission
window, and exactly one pending row matches. Ambiguity confirms nothing.
Exact observer evidence is durable, so connect/restart reconciliation can
complete a confirmation missed after persistence. If exact evidence arrives
after the local write begins but before its socket callback completes,
`markSubmitted` reconciles it in the same transaction; this is allowed only
when exactly one active row matches, so an ambiguous payload confirms nothing.
Destinationless cache rows
upgraded from older schemas act only as conservative duplicate-suppression
wildcards and can never confirm delivery. Confirmation and advancement of
`aprs_delivery_high_water` share one SQLite transaction. This bounded record is
keyed by Mesh network, node, and callsign and compares trusted event time before
mapping-scoped sequence epoch/number. It is never advanced by socket success.
An unobserved submission becomes `observation_expired` after three hours and
does not gain a delivery watermark or automatic duplicate-producing retry.

Observer-confirmed outbox rows may be deleted after the configured 90-day
default only when their watermark proves that exact event or an ordered
successor. The proof survives outbox retention and blocks re-enqueue after a
mapping-version rotation. Observation-expired rows are terminal retention
candidates but are not delivery proof. Active entries are never age-retention
candidates; only queued or failed entries with durable supersession proof are
removed.

Migration preserves each legacy socket-success watermark separately in
`aprs_legacy_submission_barriers`. It is permanent ordering evidence, not
delivery evidence: enqueue, position admission, maintenance, and the final
pre-send transaction all fail closed against it, while it never advances
`aprs_delivery_high_water` or labels a packet observer-confirmed.

The client keeps one provision-scoped persistent TX connection. It writes one
CRLF-terminated canonical iGate login, waits for the exact matching verified
`logresp`, and may then write deterministic APRS Data lines until the socket or
provision generation changes. Both login and Data reject CR/LF injection.
Gateway-specific path information and every local observation attribute stay
outside the stored Data and never alter the canonical APRS payload. The
separate receive-only monitor uses the deterministic
`<callsignBase>-C<uppercase hex abs(SSID)>` login and the fixed
`p/BM/BN/BO/BP/BQ/BU/BV/BW/BX` server filter; it is never a packet source or
path component. APRS-IS combines positive filter clauses additively with OR
semantics, so this subscription includes all packet types from the listed
Taiwan prefixes, plus any default message traffic on port `14580`; it does not
request the global `t/p` position feed. None of that wire traffic is delivery
proof by itself. Only a parseable packet selected by the current local mapping
or station matcher and matching one eligible outbox or station
source/destination/information tuple is persisted, published, or allowed to
confirm delivery.
