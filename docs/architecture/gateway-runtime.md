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
code, stops already-started runtimes, and closes SQLite; shutdown stops Mesh and
APRS activity before closing HTTP, Proxy, and persistence.

When Meshtastic is enabled, the single framed transport path decodes
`FromRadio`, persists the observation, and writes NodeInfo, strict text,
Telemetry, or Position domain records. Position records continue through
canonical duplicate detection, active CallMesh mapping selection, validation,
local and APRS-monitor high-water checks, deterministic APRS encoding, and the
durable outbox. Mapping conflicts, inadequate precision, untrusted ordering,
and unmapped nodes do not enqueue an upload.

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

Telemetry retention runs in bounded batches, defaults to 30 days, and emits a
code/data-only `telemetry.retention.completed` event. A backup request creates
a persistent `backup.create` Job, uses SQLite's backup API, applies private file
permissions, verifies `PRAGMA integrity_check`, and reports filename, size,
page count, and SHA-256 only after verification.

## Status and event surfaces

`GET /api/v1/meshtastic` exposes configured transport state and bounded metrics;
domain list endpoints expose persisted records. Gateway publishes transport,
ingest, domain, Position, APRS, CallMesh, Proxy, retention, and Job events to the
bounded SSE bus. Agent proxies those endpoints for Web and bridges selected
projections/SSE to Desktop and CLI; clients do not connect to the Gateway
listener directly in an Agent deployment.
