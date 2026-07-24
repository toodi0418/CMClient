# Desktop Supervisor

`apps/desktop` is a small Tauri 2 and Vue 3 supervisor, not a second
management Web application. Its Rust commands load the Agent configuration,
resolve the local control endpoint, and send only status, Gateway lifecycle, or
Management Web listener commands through `cmclient-control-api`.

The Desktop process does not open SQLite, connect to Meshtastic, access APRS or
CallMesh credentials, or invoke Gateway APIs directly. Agent configuration and
control failures are reduced to stable Desktop error codes before crossing the
Tauri command boundary.

The Desktop window is frameless. Its 12px red, yellow, and green controls have
8px visual spacing, use Tauri's drag-region attribute with the controls
explicitly disabled, and map to app exit, minimize, and hide operations. The
official Tauri single-instance plugin focuses the existing main window on a
second launch. Closing the main window hides it instead, while tray left-click
and the Open menu item show and focus it; only an explicit Exit action ends the
Desktop process. These Desktop lifecycle and management controls do not weaken
the Agent's separate process lock or local-only control boundary.

Desktop reads its supervisor lights, Agent version, uptime, and latest stable
error from the local Control API. It toggles the optional Management Web
listener through the same private endpoint; disabling it never affects the
Control API. When the listener reports its loopback URL as running, Desktop
opens only that Agent-provided URL through the official Tauri opener plugin.
Gateway restart remains an Agent command, so the Desktop process never spawns,
kills, or probes Gateway directly.

In addition to Core, Web, and Gateway lifecycle state, Desktop loads four
bounded Gateway projections through Agent in parallel with a three-second
timeout. They produce Meshtastic transport/frame state, combined APRS monitor
and CallMesh mapping health, and TCP Proxy mode/client capacity. Invalid,
partial, or timed-out projections become stable unavailable/degraded states;
raw Gateway identifiers, mappings, APRS Data, and credentials do not cross the
Tauri command boundary. The view refreshes this service matrix periodically and
also supports an explicit refresh.

The Desktop update panel first reads the Agent-owned update journal through
the typed `UpdateStatus` Control operation. Its Tauri backend then follows the
local typed update-event subscription and forwards only the validated status
payload to the webview. It reports phase, transfer, speed, stable failure code,
and recent stable log codes without accessing an archive, signing key, or
Gateway update endpoint.

Desktop runtime and CI smoke coverage is documented in
`docs/testing/desktop-smoke.md`.
