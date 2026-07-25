# APRS-IS Remote High-Water Monitoring

Each iGate independently subscribes with an APRS-IS buddy filter made only from
its mapped APRS callsigns. TX uses the provision's canonical iGate callsign.
The separate receive-only monitor derives the provision-scoped login
`<callsignBase>-CM`, using the same base-callsign passcode. This distinct
APRS-IS client identity never enters APRS Data, the iGate path, mapping state,
or public status.

Incoming data is accepted for collaboration only when it has a mapped source
callsign and a valid Legacy-compatible untimestamped `!` position. The monitor
persists exact source-plus-information observations for three hours and exact
local transmissions for 30 seconds. Receive time is observation metadata only;
it never establishes source-event order. Malformed or unmapped data is ignored.

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
`aprs_delivery_high_water`, which proves only this Gateway's completed outbox
sends. This preserves mapping state isolation and avoids a central server or an
elected primary iGate.
