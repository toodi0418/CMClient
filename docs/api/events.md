# SSE Events

`GET /api/v1/events` is the Gateway-wide SSE stream. It returns
`text/event-stream` and emits an event envelope using its `eventId` as the SSE
`id`, its `type` as the SSE `event`, and the complete envelope as JSON `data`.
`GET /api/v1/events/recent` returns the same envelope type in a process-local,
bounded JSON snapshot. It is not durable event history.

```text
id: 550e8400-e29b-41d4-a716-446655440000
event: mesh.transport.state
data: {"eventId":"550e8400-e29b-41d4-a716-446655440000","schemaVersion":1,"type":"mesh.transport.state","occurredAt":"2026-07-18T00:00:00.000Z","source":"gateway","payload":{"transport":"tcp","status":"ready","changedAt":"2026-07-18T00:00:00.000Z","metrics":{"bytesReceived":0,"bytesSent":0,"framesReceived":0,"framesSent":0,"malformedFrames":0,"reconnects":0}}}

```

Clients reconnect with `Last-Event-ID`. The process-local event bus replays
events that follow that ID from its bounded buffer. If that ID has already aged
out, the complete retained buffer is sent so clients can deduplicate by
`eventId` and refresh any affected REST projections. A new connection without
`Last-Event-ID` only receives newly published events.

The replay buffer holds at most 1,000 events with the production default and
is cleared on Gateway restart. `GET /api/v1/events/recent` accepts `limit` from
1 through 200 (default 100) and returns newest first; SSE replay is oldest
first. The Agent Control bridge's `/api/v1/control/events/recent` route forwards
the default snapshot only and does not accept a query string. Neither snapshot
can reconstruct events older than the current process buffer.

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

## Event catalog

The payload is event-specific and remains bounded. Consumers should treat
unknown future types as ignorable and refresh the relevant REST projection.
Current publishers use these types:

The campaign-only physical qualification profile may emit
`mesh.capture.sealed` after confirmed transport shutdown. Its payload contains
only the SHA-256 digest of canonical synthetic fixtures, the fixture count, and
`sanitized: true`. `mesh.capture.error` contains only the stable
`PACKET_FIXTURE_SANITIZATION_INVALID` code. Neither event may contain raw frame
bytes, decoded source values, identity, message, position, network metadata,
endpoint, credential, or original time anchors.

```text
gateway.heartbeat
mesh.transport.state
mesh.transport.error
mesh.observation.persisted
mesh.ingest.error
mesh.application.ignored
mesh.capture.sealed
mesh.capture.error
node.updated
message.received
telemetry.received
position.observed
position.unmapped
position.decision
callmesh.status
callmesh.error
callmesh.mapping.conflict
aprs.outbox.queued
aprs.outbox.sent
aprs.outbox.failed
aprs.outbox.error
aprs.monitor.idle
aprs.monitor.connected
aprs.monitor.observed
aprs.monitor.error
proxy.started
proxy.stopped
proxy.client
proxy.queue
proxy.backpressure
proxy.upstream
proxy.error
telemetry.retention.completed
job.created
job.status_changed
```

`GET /api/v1/jobs/:jobId/events` filters the same stream to that Job's
`job.created` and `job.status_changed` events. It does not send an initial Job
snapshot; fetch `GET /api/v1/jobs/:jobId` before subscribing and after any
reconnect. The Agent `/api/v1/control/events` stream adds
`gateway.heartbeat` and is not byte-identical to the direct Gateway stream.

`log.entry` remains a reserved CLI filter, but no production Gateway publisher
emits that type in this RC. Use the domain events above and the host service
logs for operational diagnosis.
