# Runtime And Onboarding Contract

This file defines the approved target. Implementation and package evidence are
added by the owning P13-P15 tasks.

## P14-T01 implementation boundary

The Agent owns the durable `state/setup.json` document through
`cmclient-agent-core::setup::SetupStore`. It persists a versioned state machine
and a monotonic `setupGeneration`; validation callbacks carry a generation fence
so a reset or terms-version change makes an older callback stale before it can
publish readiness. Public setup status contains only booleans and stable reason
codes.

Meshtastic setup discovery is also Agent-owned. Candidate order is a migrated
non-secret endpoint, IPv4/IPv6 loopback port `4403`, bounded
`_meshtastic._tcp.local.` mDNS results from the LAN, then explicit manual input.
Loopback mDNS advertisements are rejected because the canonical loopback
candidates already precede mDNS. The mDNS browse has a fixed event/count budget
and is stopped before returning; it does not probe every subnet address or
mutate radio state. Setup wire validation accepts only the
nonce-correlated configuration request/response actions. The Windows physical
source-smoke profile remains a separate campaign-only path guarded by the
product-integrated physical write fence.

## P14-T02 implementation boundary

The Agent now exposes redacted setup and lifecycle status plus separate Axum
setup, lifecycle, and update event streams. While the authoritative setup state
is not ready, Management Web serves the setup shell and Agent-owned setup
surface but rejects every Gateway proxy route with `SETUP_REQUIRED`. The
setup-phase reset endpoint may rotate generation only while external Gateway
work is already fenced. Ready-state `POST /api/v1/reset/operational` persists
its target generation before stopping the supervised Gateway, clears the
Gateway route and CallMesh key, removes operational configuration and every
runtime secret, publishes the new generation to revoke Management Web sessions,
and then returns to `terms_required`. Restart recovery replays that same
generation before migration so no legacy source can repopulate the reset root.
The fixture-only factory worker has no production route: it requires both
confirmation strings and an explicit backup disposition, accepts only a
nonce-marked temporary fixture root, clears only a fixed direct-child allowlist,
and replays every interrupted phase to a newly initialized setup root.

The supervised private bootstrap frame is schema version 2 and carries the
current positive JavaScript-safe setup generation beside the memory-only
capability. Gateway requires that capability on every HTTP, health, and SSE
route, removes it before route handling, and passes the bootstrap generation to
the SQLite-authoritative Job engine. `p-queue` schedules only bounded runnable
work; Job idempotency, recovery, and terminal transitions are generation
fenced. TypeBox schemas shared by Web/API clients and the deterministic OpenAPI
snapshot bind the public Agent projections and Gateway routes, including exact
per-stream Agent event IDs, Gateway `Last-Event-ID`/event-stream responses, and
Job `Idempotency-Key` headers. Invalid handler error codes are reduced to the
stable `JOB_EXECUTION_FAILED` code before SQLite persistence.

Malformed, schema-invalid, zero/out-of-range-generation, or oversized
`state/setup.json` content is atomically replaced by a minimal
`recovery_required` document. Its fresh random high-range JavaScript-safe
generation is disjoint from the ordinary incrementing range and makes a repeat
recovery collision negligible; no bytes or values from the invalid document
are retained or projected. Files that cannot be read
or safely replaced still fail closed. The recovery projection contains only
booleans, phase, schema version, and a stable reason code.

## P14-T03 setup transaction

The first Web screen is an Agent-owned wizard. It uses `GET
/api/v1/setup/discovery` for the bounded migrated/loopback/mDNS candidate list and
`POST /api/v1/setup/configure` for the one setup transaction. The CallMesh
endpoint is fixed to `https://callmesh.tmmarc.org`; it is not a user-editable
mapping authority. The request accepts only TCP port `4403`, a bounded host,
the optional local mesh/gateway identifiers, and the CallMesh API key.

The Agent starts a validation-only Gateway that reserves the product-integrated
physical lease and proves the selected endpoint with one nonce-correlated
Meshtastic configuration handshake. That process cannot start Mesh, Proxy,
APRS, or maintenance runtimes. It then authenticates the transient key against
the protected CallMesh heartbeat endpoint. Credential rejection and temporary
upstream failure return distinct
stable errors before any key or configuration is committed. Only successful
authentication may atomically replace the non-secret TOML configuration,
persist the key through the plaintext `secrets.json` backend, start the normal
Gateway, and reach `ready`. A later write, bootstrap, or runtime-start failure
restores the previous config and secret. The key is zeroized when the request
is dropped and never appears in TOML, argv, URLs, browser state, or logs. A
client disconnect after the transaction begins is treated as cancellation: the
Agent rolls back staged/promoted credentials, configuration, and ready state;
the durable transaction journal recovers the same invariant after a process
interruption. Setup does not send radio configuration or RF traffic.

## State Root

```text
Windows  %USERPROFILE%\.cmclient
macOS    $HOME/.cmclient
Linux    $HOME/.cmclient
Docker   /home/cmclient/.cmclient
```

The shared Rust resolver owns every mutable path. OS integration files such as
HKCU Run, shell-profile blocks, SMAppService/XDG registrations, desktop files,
and `~/.local/bin` are explicit integration records reconciled from a ledger
under `~/.cmclient`; they are not alternate state roots.

## Plaintext Secrets

`~/.cmclient/secrets.json` is the sole runtime secret backend. CMClient does not
use macOS Keychain, Windows Credential Manager/DPAPI, Linux Secret Service, or
the legacy systemd vault. Writes are bounded, validated, flushed, and atomically
replaced. POSIX creates ordinary private `0700`/`0600` paths, but complex
cross-principal ACL or hardlink qualification is not a startup gate. Legacy
platform-store secrets are not migrated; setup requests the key again.

Secrets never appear in argv, environment variables, process titles, URLs,
browser storage, logs, diagnostics, screenshots, evidence, or Git. Agent passes
staged values to Gateway only through a private inherited pipe/control channel.

## Setup And Reset

Agent persists a versioned state machine:

```text
uninitialized -> terms_required -> credentials_required -> validating -> ready
```

Missing, removed, authoritatively rejected, or revoked credentials enter the
mandatory Web wizard. Transient network failure is degraded/retryable, not
credential revocation. The wizard selects language, accepts versioned terms,
discovers bounded migrated, loopback, and LAN mDNS Meshtastic TCP 4403
candidates, checks the
selected endpoint, passes the CallMesh key through the private Gateway
bootstrap, applies provision-derived APRS identity, reviews defaults, and
commits each durable document atomically. There is no Agent/Gateway/Desktop/CLI
product choice.

Operational reset increments `setupGeneration`, cancels and fences old Jobs,
stops external activity, removes relevant secrets/config/terms, preserves the
database/history/backups/update history/login-start preference, and returns to
setup. Factory reset is a separate destructive confirmed Job and is never run
against real user state by unattended tests.

## Web Access

Native Web binds `0.0.0.0:7080` and `[::]:7080` when supported, advertises
`127.0.0.1`, and rejects non-loopback peers by default before assets/API/SSE.
Every mode validates Host, same-origin Origin/CSRF, and uses no wildcard CORS.
Authenticated LAN access is an explicit setting with Argon2 verification,
server-side HttpOnly/SameSite sessions, rate limits, and redacted audit events.

Docker publishes host loopback by default but never trusts NAT peer addresses;
every browser session is authenticated. Initial access uses a short-lived code
returned only by `docker compose exec -T cmclient cmclient setup-code`.

## Backup And Update State

Ordinary backup/update snapshots include non-secret config/setup metadata and a
SQLite backup-API snapshot with migration/integrity/domain manifests. They
exclude `secrets.json`, `run/`, update staging/prior payloads, credentials,
browser sessions, and pending setup sessions. Restore verifies staging before
atomic activation and then revalidates live credentials.

Native login startup is registered by default without opening a window and is
user-configurable. Closing graphical mode leaves Agent/Gateway/Web resident.
