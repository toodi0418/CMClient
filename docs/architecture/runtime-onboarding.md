# Runtime And Onboarding Contract

This file defines the approved target. Implementation and package evidence are
added by the owning P13-P15 tasks.

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
discovers bounded loopback/mDNS Meshtastic TCP 4403 candidates, validates the
CallMesh key, applies provision-derived APRS identity, reviews defaults, and
commits setup atomically. There is no Agent/Gateway/Desktop/CLI product choice.

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
