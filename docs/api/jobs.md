# Async Jobs

Long-running domain commands create a persistent Job and return `202` with its
Job ID. The Job engine validates a registered job type, clones the execution
input before writing it to SQLite, and treats `(type, idempotencyKey)` as a
unique submission. Repeating an idempotent command returns the original Job
instead of starting the work again.

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
`GATEWAY_JOB_ENGINE_UNAVAILABLE`.

`POST /api/v1/jobs/{jobId}/cancel` returns `202` and is idempotent. A queued Job
becomes `cancelled`; a running or waiting Job becomes `cancelling` and receives
an abort signal. The handler then completes the transition to `cancelled`.

`GET /api/v1/jobs/{jobId}/events` is an SSE projection of only that Job's
`job.created` and `job.status_changed` events. It has the same `eventId`,
`Last-Event-ID`, heartbeat, replay buffer, and slow-consumer policy as the
global event stream.

At Gateway startup, incomplete `running`, `waiting`, `cancelling`, and
`rolling_back` Jobs fail closed with `JOB_INTERRUPTED_BY_RESTART`; they are not
silently replayed. Queued Jobs resume only when their type has a registered
handler. Rows for unknown types remain `queued` and do not consume an in-memory
dispatch slot.

Terminal Jobs are eligible for incremental retention after 90 days by default.
The idempotency guarantee therefore covers that configured retention window;
after expiry a repeated key may create a new Job. Queued or active Jobs are
never retention candidates.
