# Local Agent Control API

The authoritative Agent Control endpoint is always local IPC, not the public
Gateway Business API: a mode-`0600` socket at `<data-dir>/control.sock` on Unix
or `\\.\pipe\cmclient-control` on Windows. Windows uses the service account's
named-pipe security descriptor and never falls back to TCP. This layer does not
bind a LAN address; the optional HTTPS bridge below authenticates requests and
forwards them to this local endpoint.

The Control routes are:

```text
GET /api/v1/control/status
POST /api/v1/control/start
POST /api/v1/control/stop
POST /api/v1/control/restart
POST /api/v1/control/web/enable
POST /api/v1/control/web/disable
GET /api/v1/control/updates
GET /api/v1/control/updates/events
GET /api/v1/control/diagnostics/bundle
GET /api/v1/control/gateway/meshtastic
GET /api/v1/control/gateway/nodes
GET /api/v1/control/gateway/positions
GET /api/v1/control/gateway/aprs
GET /api/v1/control/gateway/callmesh
GET /api/v1/control/gateway/proxy
GET /api/v1/control/events/recent
GET /api/v1/control/events
POST /api/v1/control/database/integrity-check
POST /api/v1/control/backups
PUT /api/v1/control/secrets/{callmesh-api-key|aprs-passcode|management-admin-token}
DELETE /api/v1/control/secrets/{callmesh-api-key|aprs-passcode|management-admin-token}
```

Lifecycle status routes return the schema version, Agent version/state, Gateway
lifecycle, Management Web listener state and its loopback URL (only when
running), uptime, and the latest stable error code. The enable/disable commands
control only the optional loopback Management Web listener; the private Control
API remains available in both states. This bounded endpoint exists to support
local Agent control, CLI, and Desktop operations while Gateway is unavailable.

Gateway projection routes are an Agent-owned bridge. The local client asks
Agent, Agent calls the loopback Gateway with a bounded timeout, and Agent
returns the schema-backed JSON or stable Control error. `events` streams the
bounded Gateway SSE feed; `events/recent` is its snapshot. Backup and database
integrity routes return accepted persistent Jobs rather than performing work in
the CLI or Desktop process.

When the supervisor has a live child process, Agent additionally probes the
Gateway loopback health endpoint. The control status is `running` only after a
successful probe; otherwise it reports `degraded`.

`GET /api/v1/control/updates` returns the Agent-owned persistent update job,
or `job: null` when there is no job. Its safe projection includes phase, update
time, optional bytes downloaded/total/speed, error code, and at most 64 stable
log codes. It never returns manifest URLs, signing material, archive paths,
server response text, or user configuration.

`GET /api/v1/control/updates/events` is a local `text/event-stream` feed.
Every connection receives an immediate `update.status_changed` snapshot, then
future state transitions and a 15-second heartbeat. The Agent retains no
unbounded per-client backlog: slow or disconnected subscribers are removed.
The durable source of truth is the update journal, so clients reconnect by
reading `/updates` before subscribing again.

`GET /api/v1/control/diagnostics/bundle` returns a JSON allowlist of Agent and
runtime state. It contains stable error/log codes but never paths, config,
environment, database content, packet data, credentials, or log payloads.

The secret routes never pass through Gateway. `PUT` accepts a single UTF-8 value
in its request body (maximum 4096 bytes, no control characters) and stores it in
the OS credential store. The response is only `{ "stored": true }`; secret
values are never returned. `DELETE` removes a named value and returns whether a
value existed. CLI users should use `cmclient secret set <kind>` with standard
input rather than constructing these requests by hand. Local IPC protects the
body with OS permissions; the optional remote bridge additionally requires TLS
and the HMAC control authorization described below.

## Authenticated HTTPS bridge

When the opt-in Management LAN HTTPS listener is configured, the same
`/api/v1/control/*` contract is available to the remote CLI. It is not
authorized by the browser session cookie. Every request uses the OS-stored
`management-admin-token` to authenticate an HMAC-SHA-256 signature bound to
schema version, `control:admin` scope, Unix timestamp, random nonce, HTTP method,
path, and SHA-256 body digest. Agent requires the four authentication headers,
compares signatures in constant time, accepts only a 30-second clock window,
and rejects nonce replay with a stable `REMOTE_CONTROL_*` code before forwarding
to local IPC.

```text
Authorization: CMClient-HMAC <lowercase HMAC-SHA-256 hex>
x-cmclient-timestamp: <Unix seconds>
x-cmclient-nonce: <32 hexadecimal characters>
x-cmclient-scope: control:admin
```

The signed bytes are the newline-delimited values `v1`, scope, timestamp,
nonce, uppercase method, path, and lowercase SHA-256 body digest. Query strings
and fragments are not accepted in a signed Control path.

The remote listener exposes no plaintext variant. Browser static files remain
public on the configured HTTPS origin, browser APIs retain session/CSRF/origin
authorization, and remote Control routes retain their independent HMAC gate.
