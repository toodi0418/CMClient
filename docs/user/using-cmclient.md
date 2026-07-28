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
disabled (`not_enabled`) and has no send action in this RC.

## Desktop supervisor

Desktop is a small Tauri client of the local Control API. Its red, yellow, and
green window controls hide, minimize, and hide the Desktop application; they do
not stop the resident Agent or duplicate the Web application. On an interactive
Windows session, the Agent owns the single native tray icon; its safe tooltip
distinguishes setup required, ready, Gateway offline, and CallMesh degraded.
Left-click/Open starts or focuses Desktop. `Exit CMClient Desktop` closes only
the Desktop UI and keeps Agent resident. The separate `Shut Down CMClient`
command explicitly closes Desktop before requesting Agent-owned graceful
shutdown. Session 0 services, headless/unsupported-tray deployments, Docker,
macOS, and Linux continue
through Web and CLI without a tray; graphical macOS/Linux tray support is not
yet provided. On first use, Desktop opens the Agent-advertised local Management
Web URL directly into the Setup Wizard; it does not invent a localhost fallback.
Each native package embeds the same private pinned Node runtime together with
Agent, Gateway, Web, and CLI. Login startup is a current-user setting and does
not require a privileged Agent service.

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

The CallMesh key is read from standard input and sent over local Control IPC;
it never appears in the command line or shell history:

```bash
cmclient secret set callmesh-api-key
cmclient secret remove aprs-passcode # cleanup for upgraded installations only
cmclient secret remove management-admin-token
```

Agent atomically stores the value only in `~/.cmclient/secrets.json`; CLI and
Desktop never open that file. CallMesh provisions the APRS callsign, symbol,
and comment, and Gateway derives the runtime passcode locally. CMClient does
not accept a static APRS passcode or a persisted Control token. A `secret set`
attempt for either legacy name fails with `CLI_SECRET_KIND_DEPRECATED`; removal
remains only so an upgraded installation can delete obsolete data.

CLI and Desktop control only the same user's local Agent endpoint. Remote
administration uses the authenticated Management Web session and never shares
a Control credential with command mode.

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
