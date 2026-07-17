# CMClient 2.0 Architecture Decisions

This index records the approved constraints that guide the 2.0 rebuild. It is
deliberately concise; implementation phases add API, event, position, update,
and testing contracts beside the code that enforces them.

## AD-001: One Control Plane

Web, Desktop, and CLI communicate through the Rust Agent. The Agent is the
single-instance operational owner for Gateway supervision, local control IPC,
management web listener, backup, update, rollback, and health checks. The
TypeScript Gateway does not update itself.

## AD-002: Process Responsibilities

The Gateway owns Meshtastic transport, normalization, persistence, jobs,
domain events, Position/APRS, CallMesh, and the TCP Proxy. The Tauri desktop
is a small supervisor rather than a duplicate management application. The
Rust CLI never directly accesses SQLite or Meshtastic devices.

## AD-003: Versioned Contracts And Realtime State

Management APIs are versioned and schema-backed. Commands use HTTP; long
operations create persistent asynchronous Jobs; realtime state is delivered
through SSE with event IDs, replay, heartbeat, and reconnection support.
Errors use stable codes and parameters rather than backend prose.

## AD-004: Durable Local State

SQLite is the persistence boundary. It uses migrations, WAL, bounded
retention, backup, restore verification, and integrity checks. Observations,
canonical events, decisions, node state, outbox, jobs, settings, audits, and
update history have explicit ownership and migrations.

## AD-005: Multi-iGate Position Safety

Every Mesh network/node/APRS callsign state is isolated. Trusted GPS position
event time is the primary ordering value and sequence is secondary; local
receive time is only an observation. If newness cannot be proven, CMClient
fails closed and does not upload APRS. Only `precision_bits === 32` is
eligible for position upload.

## AD-006: Deterministic APRS Data

The same canonical Mesh event must produce byte-identical APRS Data at every
iGate. Gateway name, RSSI, SNR, receive time, and path-specific observation
metadata cannot enter APRS Data. APRS-IS handles short-lived exact duplicates;
CMClient owns semantic ordering and remote high-water cooperation without a
central arbiter or elected primary.

## AD-007: Protocol-Aware Shared TCP Proxy

The shared Meshtastic proxy is a multi-client session manager with frame
codecs, config cache, request/ACK routing, serialized outbound writes,
backpressure, limits, metrics, and audit. A raw bidirectional socket pipe is
not an acceptable implementation.

## AD-008: Security And Release Boundaries

Secrets never enter Git, command arguments, logs, diagnostics, or full API
responses. LAN management adds authentication, sessions, CSRF/origin controls,
rate limiting, and audit. Releases require reproducible platform artifacts,
checksums, signatures, SBOM, and provenance; updates verify signed staged
artifacts before atomic installation and health-gated rollback.

## AD-009: Legacy Removal

TENMAN, TENMAP, their queues/privacy text/environment variables, and the old
`@cm` Bot are removed without a compatibility layer. The later Remote Message
Dispatch capability is separate from those features. Legacy Electron, raw HTTP
server, self-updater, and direct CLI resource access are reference material,
not architectural foundations.

## AD-010: Supported Deployment Modes

CMClient 2.0 supports Desktop, Headless, CLI client, systemd, Windows Service,
launchd, and Docker deployment. Platform capabilities are reported explicitly
so clients do not expose unsupported actions. User data remains separate from
binaries and survives upgrades.
