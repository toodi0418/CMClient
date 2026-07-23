# Local Agent Control API

The authoritative Agent Control endpoint is always local IPC, not the public
Gateway Business API: a mode-`0600` socket below `~/.cmclient/run` on macOS and
Linux, `/home/cmclient/.cmclient/run` in Docker, or
`\\.\pipe\cmclient-control` on Windows. Windows disables remote named-pipe
clients. Control never falls back to TCP or an HTTPS bridge, and CLI and
Desktop use only the current user's private endpoint.

The Control routes are:

```text
GET /api/v1/control/status
POST /api/v1/control/start
POST /api/v1/control/stop
POST /api/v1/control/restart
POST /api/v1/control/agent/shutdown
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
PUT /api/v1/control/secrets/callmesh-api-key
PUT /api/v1/control/secrets/{aprs-passcode|management-admin-token} (deprecated; always 410)
DELETE /api/v1/control/secrets/{callmesh-api-key|aprs-passcode|management-admin-token}
```

Local IPC exposes every route above, including `agent/shutdown` and secret
storage. No browser or network listener exposes these routes. Management Web
uses its session, Origin, and CSRF boundary instead of forwarding raw Control
requests, and no remote browser or CLI can terminate the host Agent.

Control routes use exact method/path matching. Query strings are not accepted;
an unmatched method or path returns the code-only
`{"code":"CONTROL_ROUTE_NOT_FOUND"}` envelope. Lifecycle, backup, diagnostics,
and projection commands currently ignore a request body after the HTTP request
has been parsed, so clients must send an empty body. Secret `PUT` is the only
value-bearing route, and secret `DELETE` requires an empty body.

Requests are capped at 8 KiB and JSON responses at 2 MiB. Control SSE events
are capped at 60 KiB, the connection pool is capped at 64 concurrent requests
or streams, and every operation keeps a bounded deadline. Control payloads use
camelCase Rust serde contracts; the nested update projection follows the shared
`UpdateControlStatus` field names. A
transport or size violation maps to a stable `CONTROL_*` error rather than a
raw parser message.

Lifecycle status schema v3 returns the Agent component identity/state, Gateway
lifecycle, Management Web listener state and its URL (only when running),
uptime, and the latest stable error code. The identity contains the exact shared
CMClient version, commit, tree/content digest, channel, and target. The
`agentVersion`-only schema v2 shape is rejected. The enable/disable commands
control only the optional loopback Management Web listener; the private Control
API remains available in both states. This bounded endpoint exists to support
local Agent control, CLI, and Desktop operations while Gateway is unavailable.
The IPC server admits at most 64 concurrent requests/SSE streams and applies
bounded server-side read/write deadlines. Excess clients receive the stable
`CONTROL_RESOURCE_EXHAUSTED` error; releasing a connection immediately returns
its slot.

`POST /api/v1/control/agent/shutdown` is reserved for local IPC and the Windows
Service Host. It requests one terminal Agent teardown and is not exposed by
Management Web or any network listener. Agent stops the supervisor worker,
cooperatively drains Gateway, and closes Management Web before exiting.
Once terminal teardown is requested, resource-starting commands (`start`,
`restart`, and `web/enable`) fail with `CONTROL_COMMAND_FAILED`; status and
resource-draining commands remain safe while teardown completes.

Gateway projection routes are an Agent-owned bridge. The local client asks
Agent, Agent calls the loopback Gateway with a bounded timeout, and Agent
returns the schema-backed JSON or stable Control error. `events` streams the
bounded Gateway SSE feed; `events/recent` forwards Gateway's default 100-item,
newest-first process-local snapshot and exposes no `limit` query. The bridge
applies a one-second upstream read poll even after HTTP connection setup. A
timed-out read checks the bounded downstream channel and continues the same
healthy stream, so
dropping a Control SSE client terminates its bridge thread and loopback socket
even when Gateway is half-open and sends no heartbeat. Backup and database
integrity routes return accepted persistent Jobs rather than performing work in
the CLI or Desktop process. Gateway events use non-blocking offers to the fixed
64-entry downstream queue; a full or disconnected queue closes that one bridge
instead of blocking its upstream reader. The bridge retains the last event ID
it successfully forwarded and sends it as `Last-Event-ID` after an upstream
reconnect, allowing Gateway's bounded replay window to fill short disconnects.
The snapshot and reconnect replay do not survive a Gateway restart and cannot
recover events that have already left the bounded Gateway buffer.

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

## Secret boundary

Secret routes never pass through Gateway. `PUT` accepts one bounded UTF-8 value
without control characters. `callmesh-api-key` is the only settable runtime
kind. `aprs-passcode` and `management-admin-token` are removal-only compatibility
names: `PUT` returns HTTP 410 with `CONTROL_SECRET_KIND_DEPRECATED` without
dispatching the value, while `DELETE` removes any value left by an older
installation.

Agent is the only owner of secret persistence. It atomically writes the sole
backend, root-level `~/.cmclient/secrets.json` (or
`/home/cmclient/.cmclient/secrets.json` in Docker). POSIX uses `0700` for the
root and `0600` for the file; Windows uses an ordinary current-user file without
UAC or a cross-principal ACL claim. There is no Keychain, Credential
Manager/DPAPI, Secret Service, systemd vault, persisted Control token, or
secret-bearing argv/environment path.

The response is only `{ "stored": true }`; values are never returned. CLI
users send a new CallMesh key over standard input with
`cmclient secret set callmesh-api-key`, and the CLI never reads `secrets.json`.
Agent transfers the key to a supervised Gateway only through its private
inherited bootstrap channel. Local IPC permissions and bounded typed messages
are the complete command-mode authorization boundary.
