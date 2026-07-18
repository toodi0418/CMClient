# System Endpoints

Gateway exposes the following schema-backed, versioned endpoints:

- `GET /api/v1/system/health` returns `{ "status": "ok" }` while the
  Gateway process is accepting requests.
- `GET /api/v1/system/version` returns shared build metadata: version, source
  commit, release channel, and an optional verified build timestamp.
- `GET /api/v1/system/capabilities` returns every capability state. An
  unavailable capability always includes a stable reason code.
- `GET /api/v1/system/status` combines Gateway health and build metadata.
- `POST /api/v1/diagnostics/integrity-check` returns an accepted asynchronous
  diagnostics Job; see [Diagnostics API](./diagnostics.md).

The Agent remains authoritative for Agent-owned capabilities such as services,
updates, tray, and Management Web. Gateway returns them as unavailable with an
explicit ownership/configuration reason rather than inferring support.
