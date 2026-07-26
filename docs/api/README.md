# API Reference

CMClient has two deliberately separate control surfaces. Do not infer the
security of one surface from another.

| Surface | Base | Authentication | Owner |
| --- | --- | --- | --- |
| Gateway public projection | Agent loopback `/api/v1/*` | Agent browser session/CSRF when reached through Management Web; Docker uses the same Agent boundary; no raw Gateway listener | Gateway, behind Agent |
| Local Agent Control | Unix socket `<root>/run/control.sock` or a root-hashed Windows local named pipe | Private POSIX endpoint or remote-rejected current-user Windows pipe; no token | Agent |

The local socket is always available while Agent is running. The optional Web
listener can be disabled without disabling local Control. Control uses bounded
length-delimited typed envelopes, not HTTP routes. Browser cookies and CSRF
tokens never authorize it, and CMClient exposes no remote CLI Control surface.
See [local-control.md](./local-control.md).

## Error envelopes

Gateway route-authored projection and Job errors use:

```json
{
  "code": "JOB_NOT_FOUND",
  "params": {},
  "traceId": "trace-01J..."
}
```

`code` is stable and suitable for automation. `params` is bounded and never a
secret. Gateway responses include `x-trace-id`; callers may send bounded
`x-trace-id` and `x-correlation-id` request headers. A correlation ID is copied
into Job and event envelopes when the operation is asynchronous.

Agent-owned Management Web and browser-auth errors deliberately use the smaller
HTTP envelope `{"code":"CONTROL_COMMAND_FAILED"}`. Typed local Control error
responses likewise contain a stable code but are framed IPC, not HTTP. Neither
adds Gateway `params` or `traceId` fields. A Gateway response proxied
successfully through Agent keeps its Gateway envelope; failure to reach Gateway
returns the Agent-owned code-only `GATEWAY_PROXY_UNAVAILABLE` response. Clients
accept the documented shape for their surface but must not invent a trace ID.

## Query and body validation

All `list` endpoints accept optional `limit` from 1 through 200 (default 100).
Their current Fastify validation removes and ignores additional query fields,
including on telemetry. Known telemetry fields are `limit`, `meshNetworkId`,
`nodeNum`, `metricKind`, `from`, and `to`; invalid values still fail schema or
range validation. Timestamps are UTC ISO strings, `from` must not be later than
`to`, and `nodeNum` requires `meshNetworkId`.

The backup, diagnostics, and Job-cancel handlers currently ignore any
successfully parsed request body because they have no body schema. Callers must
send an empty body: ignored query/body fields are current implementation
behaviour, not a forward-compatible extension mechanism.

## Agent browser routes

The Agent Management Web listener owns these browser-facing routes before it
proxies the remaining `/api/*` requests to loopback Gateway:

| Method and path | Contract |
| --- | --- |
| `POST /api/v1/auth/login` | Management LAN login; the body must contain exactly one `password` string |
| `GET /api/v1/auth/session` | Loopback-only local browser session bootstrap; denied to LAN and Docker peers |
| `GET /api/v1/setup/status` | Redacted Agent-authoritative setup projection |
| `GET /api/v1/setup/discovery` | Bounded passive Meshtastic discovery candidates and fixed CallMesh endpoint |
| `POST /api/v1/setup/terms` | Accept the exact current terms version during setup |
| `POST /api/v1/setup/configure` | Validate and atomically apply the first-run Meshtastic/CallMesh configuration |
| `POST /api/v1/setup/reset` | Confirm a setup-phase reset; full ready-state operational reset is owned by the reset workflow |
| `GET /api/v1/setup/events` | Agent-owned setup SSE with an immediate snapshot |
| `GET /api/v1/lifecycle/status` | Redacted Agent/Gateway/Management Web lifecycle projection |
| `GET /api/v1/lifecycle/events` | Agent-owned lifecycle SSE with an immediate snapshot |
| `GET /api/v1/updates` | Agent-owned durable update projection |
| `GET /api/v1/updates/events` | Agent-owned update SSE with an immediate snapshot |

On a configured LAN listener, login is Origin-bound, subsequent browser API
reads require the session cookie, and writes additionally require the matching
Origin and CSRF token. `/api/v1/control/*` is not a browser or CLI surface and
returns the stable Agent-owned `CONTROL_ROUTE_NOT_FOUND` response. CLI and
Desktop use only the separate local framed IPC endpoint.

Before setup reaches `ready`, the Agent serves the static setup shell and the
Agent-owned setup/reset/status routes above, but returns `503 SETUP_REQUIRED`
for the entire proxied Gateway namespace. Malformed setup command bodies use
the stable code-only `SETUP_REQUEST_INVALID` response and never expose an Axum
extractor message. Setup generation is internal and is never returned by the
public status or event contracts.

## Gateway route index

| Method and path | Reference |
| --- | --- |
| `GET /api/v1/system/health` | [system.md](./system.md) |
| `GET /api/v1/system/version` | [system.md](./system.md) |
| `GET /api/v1/system/capabilities` | [system.md](./system.md) |
| `GET /api/v1/system/status` | [system.md](./system.md) |
| `GET /api/v1/aprs` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/meshtastic` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/nodes` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/messages` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/telemetry` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/positions` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/callmesh` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/proxy` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/aprs/outbox` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/aprs/station-submissions` | [domain-projections.md](./domain-projections.md) |
| `GET /api/v1/events` | [events.md](./events.md) |
| `GET /api/v1/events/recent` | [events.md](./events.md) |
| `POST /api/v1/diagnostics/integrity-check` | [diagnostics.md](./diagnostics.md) |
| `POST /api/v1/backups` | [jobs.md](./jobs.md) |
| `GET /api/v1/jobs/:jobId` | [jobs.md](./jobs.md) |
| `POST /api/v1/jobs/:jobId/cancel` | [jobs.md](./jobs.md) |
| `GET /api/v1/jobs/:jobId/events` | [jobs.md](./jobs.md) |

Gateway does not expose a public update trigger. The Agent-owned update status
and event routes are listed in [local-control.md](./local-control.md).

## Shared contracts

The source of truth is `packages/contracts/src`: TypeBox schemas compile into
the Gateway and clients. Rust Control payloads are defined in
`crates/control-api/src/lib.rs`. The API version is part of the path and schema
version is part of each versioned envelope; permissive parsing at a route is
not permission to add contract fields.

The deterministic combined Management OpenAPI snapshot contains both the
Agent-owned setup/lifecycle/update operations and the private Gateway
operations. Agent operations reference the shared TypeBox request, response,
and event components and use browser-session security; Gateway operations keep
the private capability security requirement. The snapshot is test data for
contract drift, not a separately served documentation UI.
