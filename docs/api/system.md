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
updates, and Management Web. The Desktop application owns the tray capability;
Gateway reports it unavailable with `CAPABILITY_OWNED_BY_DESKTOP`. Gateway
returns every other host-owned capability as unavailable with an explicit
ownership/configuration reason rather than inferring support.

The first-phase Remote Dispatch contract is deliberately fail closed. Gateway
always reports `remoteDispatch` as unavailable with
`REMOTE_DISPATCH_NOT_ENABLED`; the presence of the shared task schema does not
enable a sender or Legacy message bridge.

In the constrained Docker deployment, Gateway reports `docker` as available
and reports `update`, `serial`, `service`, and `autoStart` as unavailable with
`CAPABILITY_UNAVAILABLE_DOCKER`. This prevents Web clients from presenting
host-only controls in a container that has neither an Agent nor device access.
