# Gateway Production Runtime

The supervised TypeScript Gateway is the production owner of Meshtastic and
APRS domain work. Agent passes a bounded bootstrap frame through a private
inherited channel, plus exact absolute paths derived from the one Agent-owned
runtime root and validated non-secret operational configuration. The frame may
contain the CallMesh key; that value is never placed in argv or environment.
Gateway validates bootstrap,
atomically binds `127.0.0.1:0`, and reports its port, PID, and startup nonce back
through that channel. The address and capability are per-generation,
memory-only native session state; no native configuration owns or publishes a
Gateway port. It never reads Agent configuration, manages a service, or updates
itself. The production entrypoint rejects every non-supervised start with
`GATEWAY_SUPERVISION_REQUIRED` before opening the database or a listener. The
fixed `--offline-maintenance` command is the only direct-process exception and
does not start the runtime. Docker uses the same Agent-issued bootstrap and
capability boundary; the superseded multi-service Compose descriptor therefore
fails closed until its unified Agent entrypoint is delivered.

## Startup and shutdown

Gateway opens root-level `~/.cmclient/cmclient.db`, recovers persistent Jobs, and
constructs CallMesh, Proxy, maintenance, APRS, and Meshtastic runtimes. Under
Agent supervision it first binds the capability-protected Fastify control plane
to `127.0.0.1:0`, then validates CallMesh and starts configured external
runtimes before writing the ready frame. Setup uses a separate validation-only
launch: before authenticating the in-memory CallMesh key, it reserves the
physical safety lease and performs exactly one Meshtastic configuration
handshake against the selected TCP endpoint. It does not start Mesh ingestion,
APRS, Proxy, maintenance, or mapping synchronization. Agent stops that launch,
commits setup, records the recoverable ready marker, and starts a normal Gateway;
the Web receives ready only after normal startup succeeds. Any failure rolls the
marker, config, and secret back to the credentials phase.
Agent still withholds the session and the capability from ordinary HTTP until
listener ownership is proven. Gateway accepts only the exact HTTP/1.1 Upgrade
request on `/_cmclient/bootstrap/ownership`, with a fresh 64-character
lowercase-hex challenge, exact loopback host/port, zero body, and no capability
header. It returns a zero-body response containing only an HMAC-SHA256 proof
keyed by the memory-only capability over the domain-separated nonce, PID,
loopback address, port, and challenge transcript. The proof response is bounded
to 4 KiB and never returns or logs the raw capability. Agent then uses the
capability on the version endpoint and publishes the session only after exact
product identity verification succeeds.

A startup or ownership failure emits a stable code, stops already-started
runtimes, and closes SQLite. Supervisor additionally terminates and reaps the
whole process tree on a bootstrap timeout, malformed/oversized exchange, forged
proof, listener takeover, or early exit. Shutdown requests share one terminal
promise. Cleanup runs in ordered phases while settling every member of a phase:
HTTP and maintenance, Mesh/APRS/Proxy producers, Jobs, then SQLite.
A component failure produces `GATEWAY_SHUTDOWN_FAILED` only after all later
cleanup phases have run, so a failed transport close cannot skip Job drain or
the final database decision. In an Agent deployment, a private bounded stdin
command and parent-pipe EOF enter this same path; ordinary standalone and Docker
processes do not interpret closed stdin as shutdown. Job drain has a 10-second
deadline. If an active handler ignores cancellation, shutdown fails with a
stable code and deliberately leaves SQLite for process teardown instead of
closing it underneath live work.

The std process wrapper does not rely on a drop-triggered kill: explicit stop,
bootstrap rejection, and observed child exit terminate the Windows Job Object
or Unix process group and reap it where the wrapper permits. A hard Agent exit
is fenced by the inherited private stdin EOF, so Gateway shutdown is not
dependent on implicit process-wrapper drop behavior.

The complete wait-for-startup and Gateway cleanup sequence has a 30-second outer
deadline. On expiry the process exits with `GATEWAY_SHUTDOWN_FAILED`; this is
intentionally below the Supervisor's 40-second graceful-child deadline. When
the transitional Windows Service Host is exercised, its 50-second Agent deadline
remains above this boundary; standard native runtime does not require SCM or a
system service. The decreasing inner budgets give each owner time to observe
failure and reap its child before its own fallback termination fires.

When Meshtastic is enabled, the single framed transport path decodes
`FromRadio`, persists the observation, and writes NodeInfo, strict text,
Telemetry, or Position domain records. Position records continue through
canonical duplicate detection, active CallMesh mapping selection, validation,
local and APRS-monitor high-water checks, deterministic APRS encoding, and the
durable outbox. Mapping conflicts, inadequate precision, untrusted ordering,
and unmapped nodes do not enqueue an upload. Mapping conflicts and missing or
not-yet-effective mappings persist a stable position decision before retaining
the existing conflict or unmapped SSE event.

## CallMesh runtime

The isolated client uses the production Legacy contract at
`https://callmesh.tmmarc.org`. It uses POST for
`/api/v1/client/heartbeat`, followed only when requested or no local snapshot
exists by `/api/v1/client/mappings`. Both requests use `X-API-Key` and the bounded
`callmesh-client/<version> (<platform>; <architecture>)` agent; credentials are
never placed in URLs, request diagnostics, events, or projections. Redirects
are rejected and response bodies are incrementally limited to 512 KiB.

Heartbeat, normalized mappings, server time, payload fingerprints, and the
normalized provision are committed in one SQLite transaction. The accepted
server time remains a durable no-downgrade high-water across restarts, while
each mapping hash is permanently bound to its normalized mapping fingerprint.
When a hosted mapping item omits its own effective time, normalization reuses
that hash's durable first accepted server time rather than the current
heartbeat time, so refetching unchanged content remains byte-stable across
timestamp repeats, rollbacks, and restarts.
An otherwise identical heartbeat may refresh the successful local receive time
and three-minute provision lease when the server repeats or rolls back its
timestamp; the stored server high-water never moves backward. A newer server
revision may reuse a historical hash only after the fetched mapping fingerprint
matches that hash's immutable history. An older response with different content
is rejected and cannot renew the lease, but it cannot revoke the previously
trusted provision before that lease's original expiry. Equal-time content
conflicts, revocation, expiry, local clock rollback, or schema conflict make the
provision unavailable to APRS. Public status exposes only provision state,
never callsign identity, symbols, comment, or a derived passcode.

## APRS runtime

Agent supplies only APRS enablement plus optional endpoint and destination
overrides. Gateway owns the default `asia.aprs2.net:14580` endpoint and obtains
callsign/SSID, symbol, comment, and a derived runtime passcode from the current
valid CallMesh provision. A missing, revoked, expired, locally time-invalid, or
conflicting provision stops APRS authorization and fails closed; a rejected
stale reply never extends the current lease. Static identity/passcode
environment values are rejected. The runtime performs an immediate outbox
flush and APRS-monitor refresh, then repeats both on bounded configured
intervals. The receive-only observer derives its login as
`<callsignBase>-C<uppercase hex abs(SSID)>` and uses the fixed
`p/BM/BN/BO/BP/BQ/BU/BV/BW/BX` server filter. APRS-IS positive filters are
additive OR clauses, so that subscription admits all packet types from the
listed Taiwan source-call prefixes and the default message traffic that port
`14580` may supply. CMClient deliberately does not request the global `t/p`
position feed. The server feed is still broader than CMClient's local
confirmation boundary.

Active mappings and the canonical station identity form a local exact target
matcher. Mapping additions, removals, version changes, and effective-time
changes atomically hot-swap that matcher without changing the server filter,
fencing TX, or reconnecting an active observer socket. A provision fingerprint,
derived observer identity, or filter change does fence both APRS producers and
reconnect RX; TX resumes only after the replacement observer login is verified.
Socket loss also fences TX and enters bounded reconnect. Packets that do not
match a current local target are ignored without persistence or an
`aprs.monitor.observed` event. A matched packet still confirms nothing unless
its exact source, destination, and information tuple identifies one eligible
current-provision outbox or station submission. Duplicate local callsign targets
remain a fail-closed `CALLMESH_MAPPING_CONFLICT`.

`GET /api/v1/aprs` returns the schema-backed runtime projection:

- configured/running state;
- monitor state (`stopped`, `idle`, `connecting`, `connected`, or `error`);
- mapped callsign count;
- pending and failed durable Tracker outbox counts;
- pending and failed durable station-submission counts; and
- an optional stable last error code.

`GET /api/v1/aprs/outbox` remains the bounded per-entry projection and excludes
the deterministic APRS Data line. It exposes transport and delivery state
separately: a completed socket write is `submitted`, while only an exact later
RX-monitor observation is `observer_confirmed` and may advance delivery
high-water. The Agent's APRS control projection, Web,
Desktop, and CLI consume the runtime endpoint rather than deriving connection
health from outbox rows. Web loads runtime and outbox independently, so one
failed projection does not erase the last valid state from the other.

`GET /api/v1/aprs/station-submissions` exposes only station packet kind,
delivery state, and lifecycle timestamps. A `sending` row exists before I/O;
an interrupted write becomes `transmission_uncertain` and waits for exact
observer-cache reconciliation. `localWriteCompletedAt` is emitted only for a
completed local transport write and is never inferred from an observer packet.
The projection never returns station identity,
APRS Data, coordinates, comment, or provision fingerprint.

## Telemetry and maintenance

`GET /api/v1/telemetry` accepts a maximum result limit plus optional
`meshNetworkId`, `nodeNum`, `metricKind`, `from`, and `to` filters. `nodeNum`
requires `meshNetworkId`; timestamps are UTC ISO values; and an inverted range
returns `TELEMETRY_RANGE_INVALID`. The repository uses parameterized fixed
clauses and orders by persisted `observed_at`, not the device RTC. Migration 9
adds the metric/time query index.

Telemetry, Message, and Position-history retention run in independent bounded
batches and default to 30 days. Position cleanup preserves canonical history
referenced by mapping high-water state, the APRS delivery watermark, or any
outbox row; unreferenced Mesh observations are scanned last. Terminal Job and
terminal APRS outbox retention default to 90 days and use independent bounded
batches. Observer-confirmed cleanup requires a durable delivery-order proof,
so expiring a confirmed row cannot make the same canonical event eligible after
a mapping-version rotation. The final orphan scan uses the latest of the three
domain cutoffs. Queued or failed APRS rows proven older than another active or
delivered snapshot are removed in the same bounded maintenance cycle.

Retention ages and batch sizes are configured with the paired
`CMCLIENT_TELEMETRY_RETENTION_*`, `CMCLIENT_MESSAGE_RETENTION_*`,
`CMCLIENT_POSITION_RETENTION_*`, `CMCLIENT_JOB_RETENTION_*`, and
`CMCLIENT_APRS_OUTBOX_RETENTION_*` variables (`DAYS` and `BATCH_SIZE`). Each
cycle's final orphan scan uses `CMCLIENT_OBSERVATION_RETENTION_BATCH_SIZE`; it
defaults to the three domain batch sizes combined plus 1,000 and rejects a
smaller value. The repository accepts at most 40,000 rows for this one bounded
delete. Each cycle performs a passive WAL checkpoint and emits a code/data-only
`telemetry.retention.completed` event with per-resource deletion counts. A
backup request creates a persistent `backup.create` Job, uses SQLite's backup
API, applies private file permissions, verifies `PRAGMA integrity_check`, and
reports filename, size, page count, and SHA-256 only after verification.
Backup and hash stages check the Job abort signal between blocking operations
and remove an incomplete artifact on cancellation.

## Status and event surfaces

`GET /api/v1/meshtastic` exposes configured transport state and bounded metrics;
domain list endpoints expose persisted records. Gateway publishes transport,
ingest, domain, Position, APRS, CallMesh, Proxy, retention, and Job events to the
bounded SSE bus. Agent proxies those endpoints for Web and bridges selected
projections/SSE to Desktop and CLI; clients do not connect to the Gateway
listener directly in an Agent deployment.

Gateway domain and durable-Job SSE use the exact `@fastify/sse` `0.5.0` pin.
The plugin owns content negotiation, response headers, wire framing, heartbeat
scheduling, socket disconnect lifecycle, and writable-stream backpressure.
CMClient owns event IDs, replay semantics, domain/Job filters, payload and frame
caps, the subscriber cap, the bounded pending queue, and the stable
slow-consumer close policy. Replay snapshot selection and live subscription are
registered in one synchronous Gateway turn so an event cannot enter between
those operations.

After Web authorization, the Agent streaming proxy injects the memory-only
Gateway capability and forwards the Gateway stream bytes without parsing or
reframing them. Gateway domain/Job SSE and Agent-owned setup, lifecycle, and
update SSE retain separate route namespaces, event-ID spaces, and replay stores;
`Last-Event-ID` from one namespace is never replayed in the other.

The event bus rejects payloads larger than 56 KiB by UTF-8 byte count, and SSE
formatting rejects frames larger than 60 KiB. Listener failures are isolated so
one consumer cannot stop ingest or later consumers; counters expose published,
rejected, delivered, and failed-listener totals for resource tests.
