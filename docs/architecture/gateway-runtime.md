# Gateway Production Runtime

The supervised TypeScript Gateway is the production owner of Meshtastic and
APRS domain work. Agent passes a loopback listen address, shared data directory,
validated operational configuration, and only the OS-stored secrets required
by enabled integrations. Gateway does not read Agent configuration, manage a
service, or update itself.

## Startup and shutdown

Gateway opens the migrated SQLite database, recovers persistent Jobs, and
constructs CallMesh, Proxy, maintenance, APRS, and Meshtastic runtimes. Enabled
runtimes start before the Fastify listener. A startup failure emits a stable
code, stops already-started runtimes, and closes SQLite. Shutdown requests share
one terminal promise. Cleanup runs in ordered phases while settling every member
of a phase: HTTP and maintenance, Mesh/APRS/Proxy producers, Jobs, then SQLite.
A component failure produces `GATEWAY_SHUTDOWN_FAILED` only after all later
cleanup phases have run, so a failed transport close cannot skip Job drain or
the final database decision. In an Agent deployment, a private bounded stdin
command and parent-pipe EOF enter this same path; ordinary standalone and Docker
processes do not interpret closed stdin as shutdown. Job drain has a 10-second
deadline. If an active handler ignores cancellation, shutdown fails with a
stable code and deliberately leaves SQLite for process teardown instead of
closing it underneath live work.

The complete wait-for-startup and Gateway cleanup sequence has a 30-second outer
deadline. On expiry the process exits with `GATEWAY_SHUTDOWN_FAILED`; this is
intentionally below the Supervisor's 40-second graceful-child deadline, which
in turn is below the Windows Service Host's 50-second Agent deadline. The
decreasing inner budgets give each owner time to observe failure and reap its
child before its own fallback termination fires.

When Meshtastic is enabled, the single framed transport path decodes
`FromRadio`, persists the observation, and writes NodeInfo, strict text,
Telemetry, or Position domain records. Position records continue through
canonical duplicate detection, active CallMesh mapping selection, validation,
local and APRS-monitor high-water checks, deterministic APRS encoding, and the
durable outbox. Mapping conflicts, inadequate precision, untrusted ordering,
and unmapped nodes do not enqueue an upload.

## CallMesh runtime

The isolated client uses the production Legacy contract at
`https://callmesh.tmmarc.org`. It uses POST for
`/api/v1/client/heartbeat`, followed only when requested or no local snapshot
exists by `/api/v1/client/mappings`. Both requests use `X-API-Key` and the bounded
`callmesh-client/<version> (<platform>; <architecture>)` agent; credentials are
never placed in URLs, request diagnostics, events, or projections. Redirects
are rejected and response bodies are incrementally limited to 512 KiB.

Heartbeat, normalized mappings, server time, payload fingerprints, and the
normalized provision are committed in one SQLite transaction. Mapping hashes
are remembered as a durable no-downgrade high-water across restarts. A
provision has a three-minute lease measured from the successful local receive
time; revocation, expiry, clock rollback, schema conflict, or an untrusted
revision makes it unavailable to APRS. Public status exposes only provision
state, never callsign identity, symbols, comment, or a derived passcode.

## APRS runtime

APRS is enabled only when Agent supplies a validated login callsign and an
OS-stored passcode. The runtime performs an immediate outbox flush and
APRS-monitor refresh, then repeats both on bounded configured intervals. The
monitor filter is rebuilt from active mappings. Duplicate callsign targets are
treated as `CALLMESH_MAPPING_CONFLICT` and fail closed without opening a monitor
session.

`GET /api/v1/aprs` returns the schema-backed runtime projection:

- configured/running state;
- monitor state (`stopped`, `idle`, `connecting`, `connected`, or `error`);
- mapped callsign count;
- pending and failed durable outbox counts; and
- an optional stable last error code.

`GET /api/v1/aprs/outbox` remains the bounded per-entry projection and excludes
the deterministic APRS Data line. The Agent's APRS control projection, Web,
Desktop, and CLI consume the runtime endpoint rather than deriving connection
health from outbox rows. Web loads runtime and outbox independently, so one
failed projection does not erase the last valid state from the other.

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
sent APRS outbox retention default to 90 days and use independent bounded
batches. Sent outbox cleanup requires a durable delivery-order proof, so
expiring a sent row cannot make the same canonical event eligible after a
mapping-version rotation. The final orphan scan uses the latest of the three
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

The event bus rejects payloads larger than 56 KiB by UTF-8 byte count, and SSE
formatting rejects frames larger than 60 KiB. Listener failures are isolated so
one consumer cannot stop ingest or later consumers; counters expose published,
rejected, delivered, and failed-listener totals for resource tests.
