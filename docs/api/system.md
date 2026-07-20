# System Endpoints

Gateway exposes schema-backed system projections:

- `GET /api/v1/system/health` returns `{ "status": "ok" }` while Gateway is
  accepting requests.
- `GET /api/v1/system/version` returns the Gateway component identity report.
- `GET /api/v1/system/capabilities` returns schema v2 capability state and the
  same component identity.
- `GET /api/v1/system/status` returns schema v2 health and the same component
  identity.
- `POST /api/v1/diagnostics/integrity-check` returns an accepted asynchronous
  diagnostics Job; see [Diagnostics API](./diagnostics.md).

The identity is one immutable CMClient product identity: semantic version,
source commit, source tree, explicit `dev | candidate | stable` channel, and an
exact OS/architecture/runtime/package target. The three system projections must
agree byte-for-byte on that identity. `builtAt`, host observations, and process
start time are not identity fields.

Every capability key is mandatory. An unavailable capability uses one closed
reason code rather than an OS guess. Gateway reports Agent-owned operations as
`owned_by_agent`, graphical mode as `owned_by_graphical_mode`, unconfigured
Serial as `not_configured`, and Remote Dispatch as `not_enabled`.

Docker reports `dockerPullRecreateUpdate` available. Graphical mode, login
autostart, Serial, and native update return `unavailable_in_docker`; this is an
expected profile distinction, not a broken product. Docker never exposes an
Agent self-update path.

Agent Control status uses schema v3 and carries the same product identity under
an `agent` component report. The old `agentVersion`-only wire and old build,
platform, Desktop, Headless, CLI, or Service selectors are rejected.
