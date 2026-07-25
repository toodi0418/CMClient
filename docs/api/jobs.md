# Async Jobs

Long-running domain commands create a persistent Job and return `202` with its
Job ID. The Job engine validates a registered job type, clones the execution
input before writing it to SQLite, and treats `(type, idempotencyKey)` as a
unique submission within the current setup generation. Repeating an idempotent
command in that generation returns the original Job instead of starting the
work again; the same key after a generation change creates distinct work.

`p-queue` provides bounded FIFO scheduling only. SQLite remains authoritative
for every Job state and transition. Each row persists its setup generation;
startup fails stale queued or active work closed, and an atomic compare-and-set
prevents a handler result from committing after its generation is no longer
current. A full ready-state reset must stop and fence the old Gateway before
the Agent rotates generation; that orchestration belongs to the operational
reset workflow.

At most two handlers run concurrently by default and at most 1,024 additional
Jobs are held in the in-memory dispatch queue. A new submission beyond that
boundary fails with `JOB_QUEUE_FULL` without inserting a SQLite row. An
idempotent retry still returns its existing Job while the queue is full. Startup
loads only one bounded queue window from SQLite and refills it as slots open.
During Gateway shutdown the engine stops accepting and dispatching work,
leaves not-yet-started Jobs queued for the next recovery pass, requests
cancellation of active handlers, and waits for their execution promises before
SQLite is closed. Repeated stop calls share the same drain operation. The
default drain deadline is 10 seconds; a handler that ignores its abort signal
produces `JOB_SHUTDOWN_TIMEOUT`, and Gateway will not close SQLite beneath that
still-active handler before terminating with a shutdown failure.

`GET /api/v1/jobs/{jobId}` returns the public lifecycle record. It contains the
Job ID, type, state, timestamps, and stable error code/parameters. Execution
input and result remain internal because command payloads can contain sensitive
values. A missing Job returns `JOB_NOT_FOUND`; an unavailable engine returns
`GATEWAY_JOB_ENGINE_UNAVAILABLE`. Handler error codes must match
`^[A-Z][A-Z0-9_]{0,127}$`; any invalid or oversized value is persisted and
projected only as `JOB_EXECUTION_FAILED`.

`POST /api/v1/jobs/{jobId}/cancel` returns `202` and is idempotent. A queued Job
becomes `cancelled`; a running or waiting Job becomes `cancelling` and receives
an abort signal. The handler then completes the transition to `cancelled`.

`GET /api/v1/jobs/{jobId}/events` is an SSE projection of only that Job's
`job.created` and `job.status_changed` events. It has the same `eventId`,
`Last-Event-ID`, heartbeat, replay buffer, and slow-consumer policy as the
global event stream.

The deterministic Management OpenAPI document binds `Idempotency-Key` on Job
submission and `Last-Event-ID` plus `text/event-stream` on both Gateway SSE
routes. The SSE operations reference the shared `DomainEvent` component.

At Gateway startup, incomplete `running`, `waiting`, `cancelling`, and
`rolling_back` Jobs fail closed with `JOB_INTERRUPTED_BY_RESTART`; they are not
silently replayed. Queued Jobs resume only when their type has a registered
handler. Rows for unknown types remain `queued` and do not consume an in-memory
dispatch slot.

Terminal Jobs are eligible for incremental retention after 90 days by default.
The idempotency guarantee therefore covers that configured retention window;
after expiry a repeated key may create a new Job. Queued or active Jobs are
never retention candidates.

## Submission endpoints

Both submission routes accept an optional `Idempotency-Key` header matching
`[a-zA-Z0-9._:-]{1,128}` and return:

```json
{ "jobId": "job-01J...", "reused": false }
```

`POST /api/v1/backups` creates a durable backup Job. `POST
/api/v1/diagnostics/integrity-check` creates the SQLite integrity-check Job;
the detailed contract is in [diagnostics.md](./diagnostics.md). A malformed
key returns `JOB_INPUT_INVALID` (`400`), a full queue returns `JOB_QUEUE_FULL`
(`503`), and an unavailable engine returns
`GATEWAY_JOB_ENGINE_UNAVAILABLE` (`503`). The current handlers ignore any
successfully parsed request body; clients must send an empty body because an
ignored field is not a supported extension contract. Results never expose file
paths, database rows, or secret values.

After acceptance, poll `GET /api/v1/jobs/:jobId`, cancel with `POST
/api/v1/jobs/:jobId/cancel`, and optionally subscribe to `GET
/api/v1/jobs/:jobId/events`. A cancel request returns `202` with the current
Job detail and is safe to repeat.

The cancel handler likewise ignores a successfully parsed request body. Job ID
path parameters remain bounded to 128 characters.
