# Diagnostics API

`POST /api/v1/diagnostics/integrity-check` submits the SQLite integrity check as
an asynchronous Job. It accepts an optional `Idempotency-Key` header matching
`[a-zA-Z0-9._:-]{1,128}` and returns `202` with the shared `JobAccepted`
contract:

```json
{ "jobId": "...", "reused": false }
```

The status is retrieved through `GET /api/v1/jobs/:jobId` and may be cancelled
with the existing Job cancel endpoint while non-terminal. Job results do not
include database rows or file paths. Invalid idempotency keys return
`JOB_INPUT_INVALID`; a missing diagnostics Job handler returns
`GATEWAY_JOB_ENGINE_UNAVAILABLE`. A saturated durable Job queue returns
`JOB_QUEUE_FULL` with HTTP 503 and does not create another Job row.
