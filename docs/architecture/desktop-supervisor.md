# Desktop Supervisor

`apps/desktop` is a small Tauri 2 and Vue 3 supervisor, not a second
management Web application. Its Rust commands derive the unified runtime root,
resolve the local control endpoint, and send only status, Gateway lifecycle, or
Management Web listener commands through `cmclient-control-api`.

The Desktop process does not open SQLite, connect to Meshtastic, access APRS or
CallMesh credentials, or invoke Gateway APIs directly. Agent configuration and
control failures are reduced to stable Desktop error codes before crossing the
Tauri command boundary.

The Desktop window is frameless. Its 12px red, yellow, and green controls have
8px visual spacing, use Tauri's drag-region attribute with the controls
explicitly disabled, and map to hide, minimize, and hide operations. The
official Tauri single-instance plugin focuses the existing main window on a
second launch. Closing the main window hides it instead. The resident Agent is
the sole native tray owner on supported Windows graphical hosts: its left-click
and Open menu action launch/focus the single Desktop instance. `Exit CMClient
Desktop` closes only the exact verified Desktop process and leaves Agent
resident. `Shut Down CMClient` is the separate explicit full-product command;
it first closes Desktop and then requests Agent-owned graceful shutdown through
the existing Control API. Product shutdown immediately fences new tray events,
so shutdown cannot reopen Desktop. An external Agent shutdown also terminates
and reaps any Desktop child that Agent launched, preventing an orphan process.

Desktop writes a bounded schema-versioned JSON identity to
`~/.cmclient/run/desktop.pid`: PID, Windows process creation time, and session
ID. The packaged Agent resolves the pinned `cmclient-desktop` main binary at the
install root before considering the adjacent portable-tree copy; it first
matches that executable allowlist, then requires the registered creation time
and current interactive session to match the system `Process` object. Desktop
exit performs the identity check, termination, and bounded wait through that
same system object. A stale or reused PID therefore fails closed instead of
targeting a new process. Both the install-root main binary and the packaged
portable-tree copy are in the exact executable allowlist, so either one can be
the existing single-instance primary without weakening identity verification.

The tooltip projects only bounded setup/ready/Gateway/CallMesh state. Desktop
does not create a second tray and cannot stop the Agent merely by closing its
window. Windows Session 0, other headless hosts, Docker, macOS, and Linux log a
stable no-tray status and continue through Web/CLI; graphical macOS and Linux
tray ownership remains a separate platform integration task. These Desktop
lifecycle and management controls do not weaken the Agent's separate process
lock or local-only control boundary.

The Desktop binary embeds the built Vue assets from `frontendDist`; it never
depends on a localhost Vite development server at runtime. This applies to
direct debug smoke launches as well as installed native packages, so an
Agent-launched Desktop cannot render a `localhost:1420` connection error.

When Agent reports `SETUP_REQUIRED`, Desktop asynchronously opens the Agent-
advertised local Management Web URL, where the existing Setup Wizard owns the
transaction. Reopening an existing hidden Desktop repeats this bounded check.
Desktop does not duplicate credential fields or retain setup state, block the
window event loop on Agent I/O, or construct a fallback localhost URL.

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
