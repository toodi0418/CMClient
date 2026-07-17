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
are JSON-cloned on publish, and event IDs are stable for client-side
deduplication.
