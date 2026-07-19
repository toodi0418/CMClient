# Retained Feature Parity Audit

This P11 audit compares the retained behaviours in
[`legacy-feature-matrix.md`](../legacy-feature-matrix.md) with their CMClient
2.0 owners and test surfaces. Parity means preserving the supported behaviour,
not preserving the Legacy process layout or exposing every operation in every
client. The Management Web remains the complete UI; Desktop is intentionally a
small supervisor, and CLI is the automation surface.

References below identify implementation and automated-test evidence in the
repository. Final cross-platform release execution remains a P12 gate. The
repository-wide removal scan is enforced independently and rejects forbidden
code, environment, database, UI, documentation, packaged artifacts, root
Legacy runtime paths, gitlinks, and retired direct dependencies.

## Product surfaces

| Surface | Runtime composition and control boundary | Deliberate boundary | Evidence |
| --- | --- | --- | --- |
| Management Web | Agent serves the compiled Vue bundle and handles Agent-owned routes before proxying Gateway HTTP/SSE | No direct file, process, SQLite, Meshtastic, or updater access | `crates/agent-core/src/web.rs`, `apps/web/src`, `apps/web/e2e/management.spec.ts` |
| Desktop | Tauri calls the local Agent Control API for lifecycle, Web, update, and bounded service-status projections; portable and native packages carry the complete Agent composition | It does not duplicate the full Web, call Gateway directly, or silently register a privileged Agent service | `apps/desktop/src-tauri/src/main.rs`, `scripts/desktop-native-bundles.mjs`, native package and launch smoke |
| CLI client | Rust client uses local IPC or the authenticated HTTPS Control bridge for commands and SSE follow | It never opens SQLite or a radio transport; the standalone archive contains only the CLI | `apps/cli/src/main.rs`, `crates/control-api/src/lib.rs` |
| Headless | Agent, CLI, migration tool, production Gateway, compiled Web, and locked protobuf corpus share one staged layout | A supported Node.js runtime is currently an external runtime prerequisite for Gateway | `scripts/release-artifacts.mjs`, `scripts/release-bundle-smoke.sh` |
| systemd / launchd | The service manager starts the same Agent-owned Headless composition | Installers never accept or manufacture runtime credentials | `scripts/cmclient-systemd.sh`, `scripts/cmclient-launchd.sh` and their tests |
| Windows Service | SCM Service Host supervises the adjacent Agent and carries the Windows Headless composition | The Service Host does not own Gateway domain logic | `apps/service-host`, `scripts/cmclient-windows-service.ps1`, packaging tests |
| Docker | Constrained OCI deployment contains Gateway, Web, and a fixed-target Ingress proxy | No Agent, CLI, Desktop, Service Host, serial ownership, or self-update | `Dockerfile`, `docker-compose.yml`, `scripts/docker.test.mjs`, `scripts/docker-smoke.sh` |

Canonical archive roles and paths are defined in
[Release Artifact Composition](../architecture/release-artifacts.md). Supply
chain assembly validates that complete composition instead of treating one
executable as a product surface.

## Retained behaviour

| Retained capability | CMClient 2.0 implementation | Client parity and evidence |
| --- | --- | --- |
| Meshtastic TCP/Serial operation | Agent validates non-secret transport configuration and supervises the production Gateway runtime; Gateway owns framing, protobuf decode, reconnect state, persistence, and events | Web validates and renders live connection/metric state, Desktop projects connection state/frame count, and `cmclient meshtastic` reads the Agent projection. Evidence includes `apps/web/src/stores/meshtastic.ts`, its tests, `apps/gateway/src/mesh-runtime.test.ts`, and `runtime-config.test.ts`. |
| Node, message, telemetry, and position history | Gateway persists network-scoped records and exposes bounded schema-backed projections. Telemetry supports validated network/node/metric/time-range filters and bounded retention. | Web supplies the complete list, Leaflet position, and ECharts telemetry views; `cmclient nodes` and `cmclient positions` provide automation projections. Persistence/API coverage is in `apps/gateway/src/persistence/database.test.ts`, `app.test.ts`, and `packages/api-client/src/index.test.ts`. |
| Position-to-APRS safety | The production ingest path creates observations and canonical events, applies mapping isolation, precision/time/sequence checks, remote high-water ordering, deterministic encoding, and durable outbox insertion | Web exposes runtime and bounded outbox state with independent failure handling, Desktop combines APRS and CallMesh health, and `cmclient aprs` exposes the runtime projection. Replay/runtime evidence lives in the position/APRS suites plus `mesh-runtime.test.ts` and `aprs-runtime.test.ts`. |
| APRS-IS TX/RX monitoring | Gateway periodically flushes the durable outbox and refreshes a buddy-filter monitor from conflict-free active mappings; status includes monitor state and bounded queue counts | `GET /api/v1/aprs`, Agent `GatewayProjection::Aprs`, Web, Desktop, and CLI consume one runtime state. Web evidence is in `apps/web/src/stores/aprs.ts`, `stores/aprs.test.ts`, and `views/AprsView.vue`. |
| CallMesh verification and mappings | Isolated Gateway client implements the Legacy heartbeat/mappings POST contract, validates normalized mapping/provision snapshots, and persists a monotonic no-downgrade high-water | Web provides mapping and privacy-safe status including provision state; Desktop combines the bounded status with APRS. Agent carries only the configured HTTPS origin and injects the key from its selected backend; Unix/macOS field mode can explicitly use a Repository-external owner-only `0600` plaintext file without accessing Keychain. |
| Protocol-aware TCP Proxy | Gateway owns framed sessions, config cache, serialized writes, request/ACK routing, limits, audit, and backpressure | Web provides the complete proxy view, Desktop shows state/mode/client capacity, and `cmclient proxy` returns the shared projection. |
| Realtime status and logs/events | Gateway emits bounded, replayable domain SSE; Agent bridges it to local and authenticated remote control clients | Web refreshes affected projections from event types. CLI `events`/`logs` support snapshots and `--follow`, reconnect, bounded parsing, and clean Ctrl+C exit. |
| Lifecycle, diagnostics, backup, and database integrity | Agent owns process/Web lifecycle; Gateway executes integrity and verified SQLite backup operations as persistent Jobs | Desktop provides quick lifecycle/Web controls. CLI provides `status`, `start`, `stop`, `restart`, `doctor`, `web`, `backup`, `diagnostics`, and `database`. |
| Signed update and rollback visibility | Agent remains the only update owner and persists update state independently of Gateway | Web, Desktop, and CLI consume the Agent snapshot/SSE; no Gateway self-update path is introduced. |
| Legacy settings and history | Offline `cmclient-migrate` performs dry-run, create-only configuration output, backup, verified import, and rollback | It is included in deployable Headless/Desktop/Service layouts, but never runs inside Agent, Gateway, Desktop, or the ordinary CLI. |

The final Legacy `main` dashboard added DOM-only toggles for self packet-summary
rows and heartbeat log lines. CMClient 2.0 does not recreate that raw packet/log
surface: SSE heartbeats are control comments and are never stored as domain
events, while self-originated normalized records remain inspectable in their
owning domain views. This is an explicit replacement decision, not an omitted
Legacy file merge.

## Release verification boundary

The current portable composition does not embed Node.js. Its staged-bundle
smoke therefore requires Node.js `^22.18.0` or `>=24.11.0` on the target. This
is an explicit
portable-runtime prerequisite and remaining release risk, not a claim that the
archive is self-contained. The P12 Release Build Matrix produces and inspects
all native Desktop formats, launches every staged Desktop, starts the final
staged Windows Service Host through SCM, repeats the service lifecycle from the
supply-chain final ZIP, and launches the final Linux x64 Desktop archive.
Signed/notarized installation, interactive tray and single-instance behaviour,
hardware transports, and operator evidence remain RC field gates.

## Fail-closed Remote Dispatch boundary

Remote Message Dispatch is a new capability, not compatibility for removed
sharing or command features. The shared v1 task/status contract records target,
channel, expiry, deduplication, acknowledgement, and stable result state, but
Gateway always reports `remoteDispatch.available: false` with
`REMOTE_DISPATCH_NOT_ENABLED`. The Web route renders that capability state and
offers no send control. There is no dispatch transport, queue worker, or Legacy
message bridge in the P11-T03 implementation.
