# APRS-IS Remote High-Water Monitoring

Each iGate independently opens a separate receive-only APRS-IS session. TX uses
the provision's canonical iGate callsign. The observer login is derived as
`<callsignBase>-C<uppercase hex abs(SSID)>`: SSID `0` produces `-C0`, `7`
produces `-C7`, and `15` produces `-CF`. It authenticates with the same
base-callsign passcode as TX and accepts only that exact observer callsign's
`verified` logresp. CMClient never writes APRS Data on this application-level
receive-only socket, and its distinct identity never enters APRS Data, the
iGate path, mapping state, or public status.

Before opening the socket, CMClient enforces the APRS-IS
[login limits](https://www.aprs-is.net/connecting.aspx): an alphanumeric base
of three through six characters, one one-or-two-character suffix, and no more
than nine callsign characters in total. It rejects an observer that collides
with the canonical TX identity or any provisioned mapping. The complete login,
fixed filter, and terminating CRLF must also fit the inclusive 512-byte APRS-IS
line limit.

Every observer session uses the fixed server filter
`p/BM/BN/BO/BP/BQ/BU/BV/BW/BX`. APRS-IS positive filter clauses are additive
and use OR semantics: this prefix clause admits all packet types from the
listed Taiwan source-call prefixes. CMClient deliberately does not request the
global `t/p` position feed. The user-defined port `14580` may still deliver its
default message traffic, as documented by the APRS-IS
[server-side filter commands](https://www.aprs-is.net/javAPRSFilter.aspx). This
prefix-only filter is a bounded coarse wire subscription, not CMClient's
delivery-confirmation boundary.

Mappings never enter the server filter or RX connection identity. A mapping
refresh atomically hot-swaps the monitor's local exact target matcher, including
the canonical station target, without fencing transmitters or reconnecting the
active RX socket. A provision fingerprint, derived observer identity, or fixed
filter change instead fences TX, invalidates the old observer token, closes the
old session, and restores TX only after the replacement login and filter are
verified. Socket termination follows the same fail-closed reconnect path. A
valid provision with no active Tracker mappings retains the station target and
observer session.

Incoming packets participate in persistence, ordering, events, or delivery
confirmation only when the APRS line is parseable and its source matches the
current local mapping or station target. Confirmation then requires an exact
source, destination, and information tuple matching one eligible outbox or
station submission under the current provision and observation window. Packets
admitted by the broad server feed but unrelated to those local targets are not
persisted or published. Only a valid Legacy-compatible untimestamped `!`
position participates in cross-iGate ordering. New monitor observations and
local transmissions are keyed by the exact source, destination, and information
tuple for three hours and 30 seconds respectively. Destinationless records
upgraded from an older schema remain conservative source-plus-information
wildcards for duplicate suppression only; they are excluded from delivery
reconciliation. Destination must match when an observation confirms a submitted
outbox entry. Receive time is observation metadata only; it never establishes
source-event order. Malformed or unrelated data is ignored.

The outbox repeats this comparison inside its final synchronous authorization
transaction immediately before transport I/O. This closes the interval between
initial enqueue and a later observation from another iGate: an older retry is
discarded, a same-minute unproven order is deferred, and an exact marker stays
eligible for byte-identical delivery.

Socket line callbacks are an explicit exception boundary. A parser, event, or
SQLite high-water failure cannot escape the socket `EventEmitter`; the APRS
runtime moves to `error` with `APRS_MONITOR_PERSISTENCE_FAILED` or the generic
`APRS_MONITOR_CALLBACK_FAILED` while retaining the session. A later valid
observation that completes successfully restores `connected`, so one bad
callback neither crashes Gateway nor permanently disables remote ordering.
Socket error, EOF, or close instead terminates the session, clears its connected
state, and starts a fresh verified login and filter restore. A dead session is
never relabelled `connected` by the periodic refresh loop.

Remote records live in `aprs_remote_high_water`, keyed by Mesh network, node,
callsign, and mapping version. They intentionally do not write a synthetic
`position_events` or `node_position_state` row: an APRS marker is a compact
remote observation, not a full canonical event with a local source observation
foreign key. They also remain distinct from the mapping-independent local
`aprs_delivery_high_water`, which proves only submitted entries later seen by
the receive monitor. Persisted exact observer-cache evidence can be reconciled
after reconnect or restart, but only for the current provision fingerprint and
only when one pending submission matches the observation window. Socket write
completion alone never advances it. This
preserves mapping state isolation and avoids a central server or an elected
primary iGate.

Station transmission intent and local write completion are separate durable
facts. An observation received while an intent is still `sending` remains only
in the exact observer cache; it cannot synthesize `submittedAt`,
`localWriteCompletedAt`, or a local-transmission cache row. Only the successful
return from the station transport write records those local timestamps. A
`transmission_uncertain` intent may still become `observer_confirmed` after an
exact network observation, but without `localWriteCompletedAt` it is not proof
that the current process completed that write. That exact observation still
suppresses a repeat for the bounded local window and advances an observed
telemetry sequence, without creating a local-transmission cache row.
Qualification requires the attempt, local write completion, and observer
confirmation to be ordered inside the same bounded run.
