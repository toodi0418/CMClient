# API Reference

CMClient has three deliberately separate control surfaces. Do not infer the
security of one surface from another.

| Surface | Base | Authentication | Owner |
| --- | --- | --- | --- |
| Gateway public projection | Agent loopback `/api/v1/*` or Docker Ingress | Agent browser session/CSRF when reached through LAN Management Web; Docker Ingress topology; no raw LAN Gateway | Gateway, behind Agent |
| Local Agent Control | Unix socket `<data-dir>/control.sock` or Windows `\\.\\pipe\\cmclient-control` | OS permissions and named-pipe ACL | Agent |
| Remote Agent Control | Agent HTTPS Management LAN listener `/api/v1/control/*` | TLS plus nonce/timestamp HMAC `control:admin` | Agent |

The local socket is always available while Agent is running. The optional Web
listener can be disabled without disabling local Control. Browser cookies and
CSRF tokens never authorize remote CLI Control; remote CLI uses the HMAC
contract in [local-control.md](./local-control.md).

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

Agent-owned Management Web, browser-auth, and Control errors deliberately use
the smaller envelope `{"code":"CONTROL_COMMAND_FAILED"}`. They do not add
Gateway `params` or `traceId` fields. A Gateway response proxied successfully
through Agent keeps its Gateway envelope; failure to reach Gateway returns the
Agent-owned code-only `GATEWAY_PROXY_UNAVAILABLE` response. Clients accept both
documented shapes but must not invent a trace ID for an Agent response.

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
| `GET /api/v1/updates` | Agent-owned durable update projection |
| `GET /api/v1/updates/events` | Agent-owned update SSE with an immediate snapshot |

On a configured LAN listener, login is Origin-bound, subsequent browser API
reads require the session cookie, and writes additionally require the matching
Origin and CSRF token. `/api/v1/control/*` is a separate HMAC-authenticated CLI
surface and is evaluated before browser-session authorization.

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
