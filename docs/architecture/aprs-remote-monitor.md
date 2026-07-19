# APRS-IS Remote High-Water Monitoring

Each iGate independently subscribes with an APRS-IS buddy filter made only from
its mapped APRS callsigns. Incoming data is accepted for collaboration only
when it has a mapped source callsign, a valid uncompressed timestamped position,
and a trailing `CM2/<12 lowercase hex>` deterministic marker. The monitor does
not trust receive time as source event time and ignores malformed, unmapped, or
stale data.

APRS `DDHHMMz` timestamps have no year, month, or seconds. The monitor resolves
them against the receiving time's adjacent months and rejects candidates more
than 36 hours away. Stored remote time is therefore minute-precise. A local
event can proceed after a remote marker only when it is the same deterministic
marker in that minute, or its trusted event time is at least the following
minute. Any event without a trustworthy time remains blocked.

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

Remote records live in `aprs_remote_high_water`, keyed by Mesh network, node,
callsign, and mapping version. They intentionally do not write a synthetic
`position_events` or `node_position_state` row: an APRS marker is a compact
remote observation, not a full canonical event with a local source observation
foreign key. They also remain distinct from the mapping-independent local
`aprs_delivery_high_water`, which proves only this Gateway's completed outbox
sends. This preserves mapping state isolation and avoids a central server or an
elected primary iGate.
