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

Remote records live in `aprs_remote_high_water`, keyed by Mesh network, node,
callsign, and mapping version. They intentionally do not write a synthetic
`position_events` or `node_position_state` row: an APRS marker is a compact
remote observation, not a full canonical event with a local source observation
foreign key. This preserves state isolation and avoids a central server or an
elected primary iGate.
