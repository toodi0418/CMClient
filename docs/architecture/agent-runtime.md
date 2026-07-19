# Agent Runtime Configuration

The Rust Agent owns process lifecycle and uses OS-standard directories. On
macOS its data and configuration location is
`~/Library/Application Support/CMClient`; Linux uses XDG data/config/cache
locations; Windows uses roaming/local application-data locations.

`CMCLIENT_DATA_DIR`, `CMCLIENT_CONFIG_DIR`, `CMCLIENT_CACHE_DIR`,
`CMCLIENT_LOG_DIR`, and `CMCLIENT_AGENT_CONFIG` may override those locations
only with absolute paths. Relative overrides fail with a stable configuration
error code. The Agent creates its runtime directories after configuration
validation.

The optional `agent.toml` accepts loopback operational settings by default:

```toml
[agent]
gateway_port = 4810
management_web_enabled = true
```

In a staged Headless, Desktop, or Service layout, Agent locates the adjacent
`gateway/dist/main.js` and `web/` production outputs relative to its own
executable. An explicit `agent.gateway_command` remains an operator override;
an absolute `CMCLIENT_GATEWAY_ENTRYPOINT` and `CMCLIENT_WEB_ROOT` are available
for controlled development or packaging layouts. A missing/invalid Web root
fails listener startup instead of falling back to a placeholder page. The
current packaged Gateway command invokes `node`, so Node.js `^22.18.0` or
`>=24.11.0` must be present; release portability is verified separately from
this config contract.

CallMesh keys, APRS passcodes, and administrative tokens do not belong in this
file or in Agent command arguments. Interactive Agent sessions use the operating
system credential store: Keychain on macOS, Credential Manager on Windows, and
Secret Service on Linux. The packaged Linux systemd service cannot depend on a
user-session D-Bus service, so it instead receives a root-owned wrapping key
through systemd `LoadCredential` and keeps authenticated ciphertext under its
private data directory. The service fails closed when either half is missing or
invalid; see [systemd Agent Service](./systemd-service.md).

`cmclient secret set <kind>` reads a single value from standard input and
forwards it over the private Control API; its response never contains the value.
Supported kinds are `callmesh-api-key`, `aprs-passcode`, and
`management-admin-token`. Update signing private keys are deliberately not a
runtime secret kind: release signing remains outside the product runtime.

`management-admin-token` is the shared secret for remote CLI HMAC control, not
the browser login password or session. Provision at least 32 random printable
characters through local CLI standard input before using the HTTPS Control
bridge; the remote CLI reads the same value from `CMCLIENT_CONTROL_TOKEN` in its
own process environment. Neither side accepts it as a command argument.

CallMesh's non-secret endpoint is optional Agent configuration. Its URL must be
an exact HTTPS origin with no path, query, fragment, or embedded credential. At
Gateway launch Agent drops inherited application configuration, retaining only
launcher variables such as `PATH`/Windows runtime paths, and passes this URL
plus a CallMesh API key only when that key is present in the Agent-selected
secret backend. Gateway therefore cannot inherit a legacy API key from the
parent shell.

```toml
[callmesh]
url = "https://callmesh.tmmarc.org"
```

Meshtastic, APRS, and Proxy operational settings are also strict non-secret
Agent configuration. TCP and Serial are mutually exclusive. APRS configuration
does not contain the passcode: store that separately with
`cmclient secret set aprs-passcode`. Without a stored passcode Agent starts the
Gateway with APRS disabled rather than passing an empty credential.

```toml
[meshtastic]
transport = "tcp"
mesh_network_id = "local-mesh"
gateway_id = "gateway-1"
tcp_host = "127.0.0.1"
tcp_port = 4403

[aprs]
login_callsign = "N0CALL-7"
host = "rotate.aprs2.net"
port = 14580
destination = "APCM20"
symbol_table = "/"
symbol_code = ">"
comment = "CMClient"

[proxy]
upstream_host = "127.0.0.1"
upstream_port = 4403
host = "127.0.0.1"
port = 4404
mode = "monitor"
allow_lan = false
```

Agent clears inherited application configuration before it passes this
validated configuration, the data path, and only the required Agent-owned
secrets to Gateway. The small launcher allowlist retains `PATH` and required Windows
runtime paths so the configured process can start. Gateway then owns transport,
protobuf/domain persistence, Position and APRS processing, CallMesh, Proxy,
retention, Jobs, and domain SSE. See
[Gateway Production Runtime](./gateway-runtime.md).

LAN Management Web is opt-in through a separate strict section. It requires a
non-loopback bind, absolute paths to a PEM certificate and private key, an
Argon2 PHC password hash, one or more HTTPS browser origins, a bounded session
lifetime, and bounded audit capacity. A missing or invalid value rejects Agent
configuration; it never falls back to an unauthenticated LAN listener.

```toml
[management_lan]
bind = "192.168.1.10"
port = 7443
password_hash = "$argon2id$..."
allowed_origins = ["https://cmclient.example"]
session_ttl_seconds = 3600
audit_capacity = 512
certificate_path = "/absolute/path/to/management-cert.pem"
private_key_path = "/absolute/path/to/management-key.pem"
```

The listener serves TLS only when this section is enabled; the configured
certificate must cover the browser origin's host. The password itself is never
accepted as an Agent argument, retained in the audit trail, or returned through
an API. Successful login issues a short-lived
`Secure; HttpOnly; SameSite=Strict` session cookie and a separate CSRF token.
All proxied API requests require a session; writes additionally require a
matching allowed Origin and CSRF token. Login attempts reserve the per-source
budget before password verification, and at most two Argon2 verifications run
concurrently. Expired source windows and sessions are pruned; source windows
are capped at 4,096 and live sessions at 1,024. Password PHC input must be
Argon2id version 19 with bounded memory, iteration, and lane parameters. The
bounded audit projection records only timestamp, action, and stable outcome
code, never addresses, credentials, cookies, or tokens.

The Agent injects `CMCLIENT_GATEWAY_HOST=127.0.0.1`, the configured non-zero
`CMCLIENT_GATEWAY_PORT`, and its own `CMCLIENT_DATA_DIR` into the supervised
Gateway process. This keeps the Gateway data store and the Agent's health/proxy
endpoint aligned without exposing a Gateway listener to the LAN.

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

Service deployments use application-owned JSONL under `CMCLIENT_LOG_DIR`:
`agent.jsonl` for Agent output and `gateway.jsonl` for the supervised Gateway.
The Windows SCM wrapper additionally uses `service-host.jsonl` for failures
that occur before Agent can start. Every active file has a fixed byte ceiling,
bounded retained-file rotation, restrictive permissions, and symlink/non-file
rejection.

The logging drain accepts a stdout record only when it is a bounded JSON object,
recursively redacts sensitive field names, and writes the sanitized object.
Child stderr is treated only as a stable uppercase error-code channel. Unknown,
malformed, or oversized records become a generic stable code rather than raw
text. Sink initialization and write failures also become stable supervisor
error codes while both pipes continue to drain, so a broken log destination
cannot deadlock process shutdown. Stop, restart, and drop join the drain workers
and flush accepted records before returning.

Platform service managers tail only these active application logs with a
bounded line count. systemd may fall back to similarly bounded journal records
before the files exist, but its manager exposes only stable codes; launchd
routes unmanaged fallback stdout and stderr to `/dev/null`.

Verified update archives are transient Agent cache data under
`<cache_dir>/updates/staging`. They are selected from a signed manifest, streamed
with an exact byte limit, SHA-256 verified, and atomically published by digest.
This cache must not be treated as user data and updates must not overwrite the
Agent data or configuration directories.

`cmclient diagnostics` retrieves a local, sanitized Agent diagnostic bundle.
Its allowlist contains component version, Gateway/Management Web state, and
stable current/update error or log codes only. It never reads or exports
credential values, configuration contents, environment variables, log records,
database rows, packet captures, absolute paths, or raw request payloads.
