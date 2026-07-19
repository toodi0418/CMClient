# SSE Events

`GET /api/v1/events` is the Gateway-wide SSE stream. It returns
`text/event-stream` and emits an event envelope using its `eventId` as the SSE
`id`, its `type` as the SSE `event`, and the complete envelope as JSON `data`.

```text
id: 550e8400-e29b-41d4-a716-446655440000
event: gateway.ready
data: {"eventId":"550e8400-e29b-41d4-a716-446655440000","schemaVersion":1,"type":"gateway.ready","occurredAt":"2026-07-18T00:00:00.000Z","source":"gateway","payload":{}}

```

Clients reconnect with `Last-Event-ID`. The process-local event bus replays
events that follow that ID from its bounded buffer. If that ID has already aged
out, the complete retained buffer is sent so clients can deduplicate by
`eventId` and refresh any affected REST projections. A new connection without
`Last-Event-ID` only receives newly published events.

The stream sends `: heartbeat` comments every 15 seconds, including once when
the connection opens. A socket that applies backpressure is closed immediately;
the Gateway records the stable reason `SSE_SLOW_CONSUMER` in structured logs.
This prevents one stalled browser from growing unbounded memory. Event payloads
are JSON-cloned and deeply frozen on publish, and event IDs are stable for
client-side deduplication. Payloads are limited to 56 KiB and complete SSE
frames to 60 KiB by UTF-8 byte count. These limits cannot be configured above
the protocol caps, and the event bus validates the complete encoded frame
before admitting an event to replay or live delivery. The shared browser parser
enforces the same per-frame limit even when a peer never sends a frame delimiter;
one large transport chunk may still contain any number of individually bounded
frames. Parsed events are deeply frozen and delivered from a listener snapshot;
one state, error, or event observer failure cannot skip later observers or
restart the transport.
Gateway admits at most 128 simultaneous event subscribers, including Job event
streams. Excess requests receive `503 SSE_SUBSCRIBER_LIMIT_REACHED` before SSE
headers are sent, and unsubscribe/disconnect immediately returns the slot.

Each bounded retention cycle emits `telemetry.retention.completed`. Its payload
reports `deleted` telemetry rows, `observationsDeleted` orphan observations,
`messagesDeleted`, `terminalJobsDeleted`, `sentAprsOutboxDeleted`,
`supersededAprsOutboxDeleted`, and the
`positionObservationsDeleted`, `positionEventsDeleted`, and
`positionDecisionsDeleted` history counts. Resource-specific cutoff and batch
fields make every deletion bound auditable. `observationBatchSize` is at least
the Telemetry, Message, and Position batch sizes combined plus the fixed
headroom, so a cycle cannot accumulate newly orphaned observations while all
three domain batches are full. `walCheckpoint` reports the passive checkpoint
result for that cycle. `observationCutoff` is the latest of the Telemetry,
Message, and Position cutoffs used by the final unreferenced-observation scan.
