# Desktop Supervisor

`apps/desktop` is a small Tauri 2 and Vue 3 supervisor, not a second
management Web application. Its Rust commands load the Agent configuration,
resolve the local control endpoint, and send only `status`, `start`, `stop`, or
`restart` commands through `cmclient-control-api`.

The Desktop process does not open SQLite, connect to Meshtastic, access APRS or
CallMesh credentials, or invoke Gateway APIs directly. Agent configuration and
control failures are reduced to stable Desktop error codes before crossing the
Tauri command boundary.

The initial application shell has a native window frame. The subsequent Desktop
tasks add the frameless window chrome, single-instance/tray semantics, and the
complete status and management controls without weakening the Agent's separate
process lock or local-only control boundary.
