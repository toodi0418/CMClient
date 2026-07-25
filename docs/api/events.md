# SSE Events

`GET /api/v1/events` is the Gateway-wide SSE stream. It returns
`text/event-stream` and emits an event envelope using its `eventId` as the SSE
`id`, its `type` as the SSE `event`, and the complete envelope as JSON `data`.
`GET /api/v1/events/recent` returns the same envelope type in a process-local,
bounded JSON snapshot. It is not durable event history.

Gateway domain and durable-Job SSE use the exact `@fastify/sse` `0.5.0` pin.
The plugin owns content negotiation, SSE response headers and wire framing,
heartbeat scheduling, disconnect cleanup, and writable-stream backpressure.
CMClient owns event IDs, replay and unknown-ID semantics, domain/Job filters,
payload and frame caps, subscriber admission, the bounded pending queue, and the
stable slow-consumer close policy.

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
first. The local Agent Control `RecentEvents` projection forwards the default
snapshot only and has no query input. Neither snapshot can reconstruct events
older than the current process buffer.

After the SSE headers are committed, the plugin sends `: heartbeat` comments
every 15 seconds; the first comment is sent when that interval elapses. The
plugin waits for socket drain instead of writing through transport backpressure.
Replay and live events share CMClient's 60 KiB pending-data bound; queuing an
event beyond that bound closes the stream and records the stable reason
`SSE_SLOW_CONSUMER` in structured logs. This prevents one stalled browser from
growing unbounded memory. Event payloads are JSON-cloned and deeply frozen on
publish, and event IDs are stable for client-side deduplication. Payloads are
limited to 56 KiB and complete SSE frames to 60 KiB by UTF-8 byte count. These
limits cannot be configured above the protocol caps, and the event bus validates
the complete encoded frame before admitting an event to replay or live delivery.
The shared browser parser enforces the same per-frame limit even when a peer
never sends a frame delimiter; one large transport chunk may still contain any
number of individually bounded frames. Parsed events are deeply frozen and
delivered from a listener snapshot; one state, error, or event observer failure
cannot skip later observers or restart the transport.
Gateway admits at most 128 simultaneous event subscribers, including Job event
streams. Excess requests receive `503 SSE_SUBSCRIBER_LIMIT_REACHED` before SSE
headers are sent, and unsubscribe/disconnect immediately returns the slot.

After authorization, the Agent streaming proxy injects the memory-only Gateway
capability and forwards these response bytes without parsing or reframing them.
Gateway domain/Job SSE and Agent-owned setup, lifecycle, and update SSE use
separate route namespaces, event-ID spaces, and replay stores. A
`Last-Event-ID` value is meaningful only within the namespace that issued it.

The three Agent streams are `/api/v1/setup/events`,
`/api/v1/lifecycle/events`, and `/api/v1/updates/events`. Axum owns their SSE
framing and 15-second heartbeat. Their IDs are respectively
`agent:setup:*`, `agent:lifecycle:*`, and `agent:update:*`; each namespace has
an exact route-specific TypeBox/OpenAPI event schema, independent 64-event
process-local journal, and 32-subscriber cap. Cross-namespace event IDs are
invalid even when the rest of an envelope is well formed. A new
subscriber receives the latest projection immediately. A known cursor replays
the retained events after it; an unknown, expired, restarted-process, or
foreign-namespace cursor receives the latest projection so the client can
resynchronize. A lagged broadcast receiver is closed and must reconnect. Agent
events and public status omit setup generation, secrets, configuration,
credentials, and identity.

Agent records `AGENT_SSE_SLOW_CONSUMER` without an event ID or payload before a
lagged stream closes. Subscriber permits are released when the HTTP body is
dropped, including error closure. Gateway process crash, backoff, restart, and
route publication update lifecycle status/SSE directly from the background
supervisor; they do not wait for an unrelated CLI or Control status request.
The worker also refreshes the lifecycle snapshot once per second so Web uptime
advances and a running process whose private health route fails becomes
`degraded` without requiring another command.

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
aprs.outbox.submitted
aprs.outbox.observer_confirmed
aprs.outbox.failed
aprs.outbox.error
aprs.igate.submitted
aprs.igate.observer_confirmed
aprs.igate.error
aprs.igate.counter.error
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

`aprs.outbox.submitted` means the local socket write completed; it is not
server acceptance or delivery proof. `aprs.outbox.observer_confirmed` is emitted only after the
separate receive monitor observes an exact source, destination, and information
match at or after submission. Consumers must refresh the REST projection for
the durable `deliveryStatus`; they must not translate `status: sent` into a
confirmed delivery.

The iGate-family equivalents carry only the station packet kind or a stable
error code. They never expose the packet line, provision identity, coordinates,
or comment. `aprs.igate.submitted` is likewise transport-only;
`aprs.igate.observer_confirmed` is the corresponding exact receive observation.

`GET /api/v1/jobs/:jobId/events` filters the same stream to that Job's
`job.created` and `job.status_changed` events. It does not send an initial Job
snapshot; fetch `GET /api/v1/jobs/:jobId` before subscribing and after any
reconnect. The byte-preserving Agent HTTP streaming proxy is distinct from the
Agent's typed local Gateway-event subscription, which adds `gateway.heartbeat`
and is not byte-identical to the direct Gateway SSE stream.

`log.entry` remains a reserved CLI filter, but no production Gateway publisher
emits that type in this RC. Use the domain events above and the host service
logs for operational diagnosis.
