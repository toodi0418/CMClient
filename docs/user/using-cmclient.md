# Using CMClient

The Management Web shell is a projection client. It does not open SQLite,
operate a Meshtastic transport, or perform privileged lifecycle work directly.

## Web areas

| Area | What it shows |
| --- | --- |
| Overview and System | Agent/Gateway health, build identity, capabilities, and stable error codes |
| Meshtastic | Transport state, network identity, and bounded connection metrics |
| Nodes and Messages | Persisted node and message projections, at most 200 rows per request |
| Telemetry | Bounded metric queries and charts; invalid ranges fail with a stable code |
| Positions | Canonical position events ordered by trusted event time; no external map tiles |
| APRS | Monitor state, mapping count, outbox state, retries, and safe error codes |
| CallMesh | Synchronization state and validated mappings; credentials are never shown |
| Proxy | Listener policy, queue counters, upstream state, and a bounded redacted audit ring |
| Logs | The current SSE session's bounded event buffer, not a persistent audit log |
| Diagnostics | An idempotent SQLite integrity-check Job and its stable status |
| Updates | Agent-owned update phase, progress, rollback state, and stable log codes |
| Settings | Web theme and locale preferences only |

Every view distinguishes unavailable, degraded, and empty data. A missing
Gateway does not become a fake success state. `remoteDispatch` is intentionally
disabled (`REMOTE_DISPATCH_NOT_ENABLED`) and has no send action in this RC.

## Desktop supervisor

Desktop is a small Tauri client of the local Control API. Its red, yellow, and
green window controls exit, minimize, and hide the Desktop application; they do
not stop the Agent service or duplicate the Web application. A tray entry can
reopen the Management Web URL returned by Agent. Native packages embed the
complete portable runtime under `cmclient-runtime/`, but installation does not
silently register or start a privileged Agent service.

## CLI essentials

```bash
cmclient status
cmclient start
cmclient stop
cmclient restart
cmclient doctor
cmclient logs --follow
cmclient events --follow
cmclient update --follow
cmclient backup --json
cmclient diagnostics --json
cmclient database --json
```

Global options are `--json`, `--quiet`, `--no-color`, `--timeout`, and
`--endpoint`. `--json` is a stable projection, not a dump of internal rows.
Follow commands reconnect with bounded SSE state and stop cleanly on Ctrl+C.
Exit codes are documented in the [CLI contract](../api/cli.md).

`events` displays current domain events. `logs` only selects the reserved
`log.entry` event type, which has no production publisher in this RC; use
`events` and the host service manager's logs for operational output. The Web
Logs area instead keeps at most 50 events from the current browser SSE session.

Secrets are read from standard input and never appear in shell history:

```bash
printf '%s\n' "$CALLMESH_KEY" | cmclient secret set callmesh-api-key
printf '%s\n' "$APRS_PASSCODE" | cmclient secret set aprs-passcode
cmclient secret remove management-admin-token
```

For remote control, provision `management-admin-token` in the Agent's secret
store, then provide the same value to the remote CLI only through that calling
process's `CMCLIENT_CONTROL_TOKEN` environment variable. The CLI signs each
request with the nonce/timestamp HMAC contract; it does not accept a token
option or follow redirects.

## Theme, language, and limits

The Web preference store supports `light`, `dark`, and `system`, plus `zh-TW`
and `en-US`. Invalid browser storage falls back to system theme and a safe
locale. Position and telemetry views use bounded projections; SSE frames are
limited to 60 KiB and subscribers to 128. These limits are intentional. A
slow consumer is closed and can reconnect rather than causing unbounded memory
growth.

The Updates page reports what Agent already knows from its durable journal. It
does not make Gateway self-updating, and this RC has no public route that starts
an update from a browser. Only the Agent update boundary can verify, stage,
install, migrate, health-check, and roll back an archive.
