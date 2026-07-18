# Legacy Feature Decision Matrix

This matrix converts the legacy audit into CMClient 2.0 migration decisions.
`Retain` always means retaining user-facing or protocol behaviour, not copying
the legacy Node.js/Electron implementation.

## Retain And Rebuild

| Legacy capability and evidence | CMClient 2.0 decision | Target boundary |
| --- | --- | --- |
| Meshtastic TCP and serial connections (`src/meshtasticClient.js`, `src/discovery.js`) | Retain and rebuild | Gateway transport abstraction with shared decoder, connection state, bounded reconnect, and device enumeration (P04) |
| Meshtastic protobuf definitions (`proto/meshtastic/`) | Retain as versioned schema reference | Gateway protobuf integration with locked version and compatibility fixtures; do not hand-maintain magic field numbers (P04) |
| Node inventory and packet summaries (`src/nodeDatabase.js`, dashboard) | Retain and rebuild | SQLite node registry, normalized packets, API, SSE, and Nodes page (P03/P04/P06) |
| Text messages and message history | Retain and rebuild | Gateway message store/events and API-driven Web page (P04/P06) |
| Telemetry ingestion, history, charts, and retention | Retain and rebuild | SQLite telemetry model, retention, API/SSE, and ECharts UI (P03/P04/P06) |
| Position-to-APRS operation (`src/callmesh/aprsBridge.js`, `src/aprs/client.js`) | Retain and redesign from first principles | Canonical observations/events, per-node high-water state, deterministic encoder, APRS-IS outbox and monitor (P05) |
| APRS-IS connection monitoring | Retain and rebuild | APRS-IS TX/RX clients with exact-duplicate cooperation only; CMClient owns semantic ordering (P05) |
| CallMesh key verification, heartbeat, mapping, and provision | Retain and isolate | Independent Gateway module with versioned mappings, effective times, stable error codes, and fail-closed conflict handling (P03/P06) |
| Desktop supervision and quick controls | Retain and rebuild | Tauri 2 shell controlling the Rust Agent only; no duplicate management UI (P08) |
| CLI operation and automation | Retain and rebuild | Rust CLI using Agent Control API, stable JSON and exit codes; never direct DB or Meshtastic access (P02) |
| Browser management UI and realtime updates | Retain and rebuild | Vue 3/Vite/Pinia UI over Fastify APIs and durable SSE contracts (P03/P06) |
| Offline map presentation | Retain capability, re-evaluate assets | Leaflet map page and positions API. Existing committed map tiles/fonts are legacy data assets and require size/licensing review before any carry-forward (P06/P11) |
| Legacy user data (SQLite, JSON, JSONL) | Retain data through explicit migration | Versioned import, dry-run report, backup, verification, and rollback; no in-place implicit conversion (P11) |
| Headless, service, and container deployment | Retain and rebuild | Agent-owned systemd/Windows Service/launchd installers and a capability-restricted Docker image (P10) |
| Cross-platform packaging | Retain and rebuild | CI build matrix for Agent, Gateway, CLI, Web, and Desktop artifacts with checksums, provenance, and signatures (P10) |
| Legacy test observations and packet examples | Retain as characterization input | Sanitized fixtures, contract tests, deterministic replay, E2E and packaging smoke tests (P00/P04/P05/P12) |

## Replace

| Legacy implementation or policy | Replacement | Reason |
| --- | --- | --- |
| Single CommonJS process that owns CLI, transport, persistence, web, and CallMesh | Rust Agent plus TypeScript Gateway with shared contracts | Establishes process ownership, isolation, supervision, and testable API boundaries |
| Raw Node `http` dashboard server (`src/web/server.js`) | Fastify Gateway behind Agent management listener/static/reverse proxy | Provides versioned schemas, async jobs, SSE replay, stable errors, and listener control |
| Electron process spawns and controls Node backend directly | Tauri desktop controls the local Rust Agent | Desktop is a small supervisor; Web remains the complete management experience |
| Direct CLI connection to Meshtastic and local stores | Rust CLI to Agent Control API | Makes CLI, Web, and Desktop use one control plane |
| Legacy APRS cache and receive-time anti-backtrack heuristics | Canonical event identity, GPS event-time/sequence high-water state, deterministic APRS payloads | `rx_time` is local observation only and cannot establish cross-iGate order |
| Embedded Node self-update and legacy updater scripts | Agent-only signed update, staging, backup, atomic install, health gate, and rollback | Gateway may not update itself; update must survive Gateway downtime |
| Ad hoc JSON/JSONL persistence and Node SQLite usage | SQLite migrations, WAL, retention, backup, integrity checks, and repositories | Supports durable jobs/events and safe forward migrations |
| Standalone manual diagnostic scripts | Unit, contract, integration, replay, E2E, update, and packaging suites | Makes regression coverage reproducible in CI |
| Electron/pkg-only build workflows | Cross-platform build/release matrix with SBOM, checksums, signatures, and provenance | Meets CMClient 2.0 release gates |

## Remove Without Compatibility Layer

| Legacy feature or artifact family | Decision | Evidence and constraint |
| --- | --- | --- |
| TENMAN and TENMAP sharing | Remove | Includes outbound sharing, queues, retries, environment variables, logs, privacy text, docs, UI, and database remnants |
| Old `@cm` auto-reply Bot | Remove | It is coupled to TENMAN/TENMAP and is not a CMClient 2.0 feature |
| TenManMap bidirectional message bridge | Remove | No protocol, WebSocket, queue, or compatibility shim remains |
| Gateway-specific APRS Data fields | Remove | Gateway name, RSSI, SNR, receive time, and path-specific data must not alter deterministic APRS Data |
| Legacy raw-socket sharing model | Do not retain | P07 implements a protocol-aware, framed multi-client proxy rather than a socket pipe |
| Docker self-update behaviour | Remove | Docker provides a constrained deployment mode; updates are not self-applied by Gateway containers |
| Direct API-key storage in tracked files, command-line arguments, or diagnostic output | Remove | Secrets move to OS-appropriate storage/redaction paths and LAN control requires authenticated sessions |

## Deliberately New, Not Legacy Compatibility

| CMClient 2.0 capability | Decision |
| --- | --- |
| Agent single-instance control, local IPC, supervision, and persistent asynchronous jobs | New foundation |
| Versioned `/api/v1` contract, TypeBox schemas, idempotency, and replayable SSE | New foundation |
| Protocol-aware shared Meshtastic TCP Proxy | New capability |
| Signed updater, rollback, power-loss recovery, and release provenance | New capability |
| LAN auth, sessions, CSRF/origin controls, rate limiting, and audit logs | New capability |
| Remote Message Dispatch | Later independent capability. It is neither a TENMAN replacement nor an `@cm` compatibility layer; only a feature flag/contract may appear before its later phase. |

## Parity Audit Rule

The Legacy settings reader is isolated in `cmclient-migrate`; its supported
mapping, dry-run report, and create-only output rule are documented in
[Legacy Settings Migration](./architecture/legacy-settings-migration.md).
The same offline tool imports only verified Legacy history projections with a
backup, post-import verification, and rollback; it never treats Legacy records
as live packets or position/APRS state.

P11 must use this matrix and `docs/legacy-inventory.md` to distinguish
intentional removal from an accidental regression. A retained capability needs
an implementation and test in the new architecture; a replaced implementation
must not remain reachable; a removed feature must have no code, environment
variable, schema, UI, test, or documentation compatibility path. The retained
implementation and per-surface evidence is recorded in
[Retained Feature Parity Audit](./testing/feature-parity.md). Repository-wide
removal proof remains the separate P11-T04 gate.
