# Agent Runtime Configuration

The Rust Agent owns process lifecycle and derives every mutable path from one
immutable startup-environment snapshot. Windows uses
`%USERPROFILE%\.cmclient`; macOS and Linux use `$HOME/.cmclient`; Docker uses
`/home/cmclient/.cmclient`. Split AppData, Application Support, XDG,
`/var/lib`, `/etc`, installation-directory, and arbitrary path overrides are
rejected. Tests isolate state by supplying a synthetic home snapshot without
mutating the parent process environment.

The optional root-level `config.toml` accepts Agent-owned operational settings:

```toml
[agent]
management_web_enabled = true
```

Native deployments do not accept a Gateway host or port setting. For every
Gateway generation, the child atomically binds an OS-assigned loopback port and
returns it over the private bootstrap channel. Agent keeps that address and its
capability only in memory, rotates both on restart, and routes Web, graphical,
and command-mode requests through the current session. Operators and clients
must not probe, reserve, publish, or connect to a raw native Gateway port.

In a staged unified-product layout, Agent locates the adjacent private
`runtime/node` executable, `gateway/dist/main.js`, and `web/` production output
relative to its own executable. Native runtime never selects the legacy
`agent.gateway_command` value. Absolute `CMCLIENT_PRIVATE_NODE`,
`CMCLIENT_GATEWAY_ENTRYPOINT`, and `CMCLIENT_WEB_ROOT` inputs are available only
for controlled source and packaging qualification; an explicitly supplied
missing, relative, linked, or otherwise invalid child path fails closed. Debug
builds may locate Node from `PATH` when no private runtime input exists, but
that convenience is not package-portability or no-system-Node evidence. A
missing or invalid Web root fails listener startup instead of falling back to a
placeholder page.

CallMesh keys and other approved runtime credentials do not belong in this
file, command arguments, or environment variables. The only runtime backend is
root-level `secrets.json`. Agent validates its bounded versioned JSON schema and
atomically replaces the complete document. POSIX creates the root as `0700`
and the file as `0600`; Windows uses ordinary current-user files without UAC or
a cross-principal ACL gate. Keychain, Credential Manager, DPAPI, Secret Service,
the former systemd vault, and persisted Control tokens are not used.

```bash
printf '%s\n' '<CallMesh API key>' | cmclient secret set callmesh-api-key
```

`cmclient secret set <kind>` reads a single value from standard input and
forwards it over the private Control API; its response never contains the value.
The settable kind is `callmesh-api-key`. Legacy `aprs-passcode` and
`management-admin-token` names are deprecation-only and cannot create a new
persisted value. Update signing private keys remain outside the product
runtime.

CallMesh's non-secret endpoint is optional Agent configuration. Its URL must be
an exact HTTPS origin with no path, query, fragment, or embedded credential. At
Gateway launch Agent drops inherited application configuration and `PATH`,
retaining only required Windows runtime variables. It passes the non-secret URL
in the bounded child environment and sends an optional
CallMesh API key only inside the private bootstrap frame. Gateway therefore
cannot inherit a legacy API key from the parent shell.

```toml
[callmesh]
url = "https://callmesh.tmmarc.org"
```

Meshtastic, APRS, and Proxy operational settings are also strict non-secret
Agent configuration. TCP and Serial are mutually exclusive. CallMesh supplies
the provisioned APRS callsign/SSID, symbol, and comment to the Gateway, which
derives the runtime passcode locally. Agent APRS configuration can only provide
optional operator overrides for the APRS endpoint and destination. For rc.1
upgrade compatibility,
known `login_callsign`, `symbol_table`, `symbol_code`, and `comment` fields are
parsed but ignored and must be removed; they are never injected. Other unknown
fields and inline passcodes remain invalid. The `aprs-passcode` compatibility
route and CLI name permit deletion only; Agent rejects new values and never
reads or injects an old stored value for APRS launch.

```toml
[meshtastic]
transport = "tcp"
mesh_network_id = "local-mesh"
gateway_id = "gateway-1"
tcp_host = "127.0.0.1"
tcp_port = 4403

[aprs]
host = "asia.aprs2.net"
port = 14580
destination = "APCM20"

[proxy]
upstream_host = "127.0.0.1"
upstream_port = 4403
host = "127.0.0.1"
port = 4404
mode = "monitor"
allow_lan = false
```

Agent clears inherited application configuration and `PATH` before it passes
validated non-secret configuration and exact absolute runtime paths to Gateway.
Only required Windows runtime variables remain in the launcher allowlist; the
private Node executable is always addressed absolutely. The required CallMesh
key uses the private bootstrap pipe, never argv or environment. Gateway then owns transport,
protobuf/domain persistence, Position and APRS processing, CallMesh, Proxy,
retention, Jobs, and domain SSE. See
[Gateway Production Runtime](./gateway-runtime.md).

Management Web is independently switchable with
`agent.management_web_enabled`. When enabled, Agent binds separate
`0.0.0.0:<port>` and v6-only `[::]:<port>` sockets with address reuse disabled;
the default port is `7080`. Both families must bind the same port. A conflict or
unavailable family fails the complete start and drops the other socket rather
than leaving a partial listener. Agent still advertises the loopback URL, and
Native admission rejects non-loopback socket peers by default before static,
login, API, SSE, or proxy routing. Forwarding headers never affect peer
classification.

LAN Management Web is opt-in through a separate strict section. It requires an
Argon2 PHC password hash and at least one exact HTTP or HTTPS browser Origin.
CIDRs, additional Host authorities, and TLS are optional. Invalid input rejects
Agent configuration; it never falls back to an unauthenticated LAN policy.

```toml
[management_lan]
port = 7443
allowed_cidrs = ["192.168.1.0/24"]
allowed_hosts = ["cmclient.example:7443"]
password_hash = "$argon2id$..."
allowed_origins = ["https://cmclient.example:7443"]
session_ttl_seconds = 3600
audit_capacity = 512
certificate_path = "/absolute/path/to/management-cert.pem"
private_key_path = "/absolute/path/to/management-key.pem"
```

The actual sockets remain wildcard listeners. `allowed_cidrs` constrains
non-loopback socket peers after LAN authentication is enabled; an empty list
means no CIDR restriction. The optional non-loopback `bind` field is admission
shorthand that adds one `/32` or `/128` CIDR and its exact Host authority, not a
socket-bind selector. Local authorities for `localhost`, `127.0.0.1`, and
`[::1]` are always allowed on the selected port. Authorities extracted from
`allowed_origins` are added automatically, while `allowed_hosts` supplies any
additional exact case-insensitive values. Missing, duplicate, wildcard, or
unlisted Host headers fail before route handling; wildcard CORS is never
emitted.

TLS is enabled only when both absolute PEM paths are supplied and successfully
loaded; otherwise the listener serves HTTP. Plain authenticated LAN HTTP adds a
stable warning header to every response and a redacted `http_lan_warning` audit
outcome. The configured certificate must cover the browser origin's host. The
password itself is never accepted as an Agent argument, retained in the audit
trail, or returned through an API.

Successful login requires an exact configured Origin, cycles the opaque
server-side session ID, and returns a separate CSRF token. The cookie is
`HttpOnly; SameSite=Strict` and becomes `Secure` under TLS. LAN reads require a
valid admin role and current Web/setup generation; writes additionally require
the exact Origin and matching `x-csrf-token`. Native loopback writes receive the
same Origin/session/CSRF enforcement through a local-role session. Login and API
rate limits use the socket peer address, reserve capacity before Argon2 work,
and at most two password verifications run concurrently. Password PHC input
must be Argon2id version 19 with bounded memory, iteration, and lane parameters.
The bounded in-memory audit projection records only timestamp, stable action,
and stable outcome code, never addresses, credentials, cookies, tokens,
payloads, identities, or locations.
Setup reset and setup-generation rotation always add dedicated entries to this
code-only Agent audit and structured log, including loopback-only deployments
without a LAN authentication controller.

Docker uses the same listener implementation but has no Native local-session
bypass: starting its Web listener without the authenticated access controller
fails closed, and protected APIs always require an admin session. Its optional
CIDR, exact Host, Origin, CSRF, rate, and TLS policies are otherwise identical.
Stopping Web or changing its setup/session generation revokes sessions without
disabling the separate local Control IPC.

The Agent delivers a bounded memory-only schema-version-2 bootstrap frame
through the supervised Gateway's inherited private pipe; the frame carries the
startup nonce, capability, current setup generation, and optional CallMesh key.
The generation is positive and JavaScript-safe before the child can start. A
stopped or backoff Supervisor reloads the current durable generation immediately
before every start or restart, so a reset cannot reuse the generation captured
when the Agent process was first constructed. The key is never placed in a
secret-bearing environment value or returned in the ready frame.
Gateway validates that frame, atomically binds `127.0.0.1:0`, and returns the
OS-assigned port, child PID, and nonce through the same private channel.

Agent does not send the capability to that address yet. It creates a fresh
64-character lowercase-hex challenge and sends an exact HTTP/1.1 Upgrade request
to `/_cmclient/bootstrap/ownership` with no capability header. Gateway proves
listener ownership with HMAC-SHA256 keyed by the capability over
`cmclient.gateway.bootstrap-ownership.v1`, startup nonce, PID, loopback host,
dynamic port, and challenge, each separated by a newline. The exact zero-body
response is limited to 4 KiB and must complete within the shared monotonic
bootstrap deadline. The capability, challenge, and proof remain memory-only;
the raw capability is never returned by the Gateway or admitted to arguments,
environment, disk, logs, or evidence.

Only after that proof succeeds does Agent capability-authenticate the version
endpoint and verify its exact product identity before publishing the session.
Any timeout, malformed frame or response, wrong PID/nonce/proof, listener
takeover, early exit, or oversized input fails closed. Supervisor leaves no
route published and force-terminates and reaps the complete child process tree
when bounded graceful cleanup cannot finish. There is no native fixed-port
configuration, probe, or release/rebind window, and no Gateway listener is
exposed to the LAN.

Agent also marks only its child as `CMCLIENT_SUPERVISED=1` and owns a private
stdin shutdown pipe. Stop, restart, OS termination, and service teardown send a
bounded shutdown command; Gateway also treats parent-pipe EOF as parent death.
Gateway runs its phased cleanup before exit, while Supervisor uses a monotonic
40-second deadline and force termination only as a fallback. Agent installs
SIGINT/SIGTERM handling before starting the supervisor and wakes the blocking
local Control listener so the same once-only teardown path always runs.

Gateway supervision advances on an Agent-owned 100 ms background tick and does
not depend on status, Web, Desktop, or CLI traffic. A crash enters monotonic
bounded exponential backoff; consecutive crashes retain their attempt count
until the child survives a 30-second stable window. Stop and Agent teardown
terminate and reap the child. Real subprocess tests prove crash/restart,
deadline, stable-reset, stop, and drop behavior.

## Service Logging

Runtime deployments use application-owned JSONL under `~/.cmclient/logs`:
`agent.jsonl.YYYY-MM-DD` for Agent output and
`gateway.jsonl.YYYY-MM-DD` for the supervised Gateway. The Windows SCM wrapper
additionally uses `service-host.jsonl.YYYY-MM-DD` for failures that occur
before Agent can start. Every UTC daily file has a fixed byte ceiling, bounded
daily retention, restrictive permissions, and symlink/non-file rejection.
Reaching the daily ceiling fails with `RUNTIME_LOG_RETENTION_LIMIT` without a
custom rename rotation; a new file is selected on the next UTC date.

The logging drain accepts a stdout record only when it is a bounded JSON object,
recursively redacts sensitive field names, and writes the sanitized object.
Child stderr is treated only as a stable uppercase error-code channel. Unknown,
malformed, or oversized records become a generic stable code rather than raw
text. Sink initialization and write failures also become stable supervisor
error codes while both pipes continue to drain, so a broken log destination
cannot deadlock process shutdown. Stop, restart, and drop join the drain workers
and flush accepted records before returning.

Platform service managers tail only the newest dated file in each application
log family with a bounded line count and retain a fixed-name legacy fallback.
systemd may fall back to similarly bounded journal records
before the files exist, but its manager exposes only stable codes; launchd
routes unmanaged fallback stdout and stderr to `/dev/null`.

Verified update archives are transient Agent cache data under
`~/.cmclient/cache/updates/staging`. They are selected from a signed manifest, streamed
with an exact byte limit, SHA-256 verified, and atomically published by digest.
This cache must not be treated as user data and updates must not overwrite the
Agent data or configuration directories.

`cmclient diagnostics` retrieves a local, sanitized Agent diagnostic bundle.
Its allowlist contains component version, Gateway/Management Web state, and
stable current/update error or log codes only. It never reads or exports
credential values, configuration contents, environment variables, log records,
database rows, packet captures, absolute paths, or raw request payloads.
