# Configuration and Security

The Agent accepts a strict, `deny_unknown_fields` `~/.cmclient/config.toml`.
Every mutable path is derived from that one home root; arbitrary path overrides
are rejected. Never put secret values in this file.

## Minimal configuration

```toml
[agent]
management_web_enabled = true

[callmesh]
url = "https://callmesh.tmmarc.org"

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

Native Agent configuration has no Gateway host or port authority. Gateway
binds an OS-assigned loopback port for each supervised generation and returns
the address over its private bootstrap channel; Agent keeps the address and
capability only in memory. A legacy `gateway_port` field is rejected as an
unknown setting. Web, graphical, and command modes use Agent-owned routes and
must not connect to the raw Gateway listener.

Before sending that capability on any ordinary Gateway request, Agent proves
that the process which returned the ready frame owns the loopback listener. It
sends a fresh 64-character lowercase-hex challenge in an exact HTTP/1.1 Upgrade
request to `/_cmclient/bootstrap/ownership`; the request contains no capability.
Gateway returns only a lowercase-hex HMAC-SHA256 proof keyed by the memory-only
capability over this domain-separated transcript:

```text
cmclient.gateway.bootstrap-ownership.v1
<startup nonce>
<child PID>
127.0.0.1
<dynamic port>
<fresh challenge>
```

The zero-body response is capped at 4 KiB and shares the bounded bootstrap
deadline. The raw capability is never returned, written to disk, or included in
logs or evidence. Only after ownership succeeds may Agent capability-authenticate
the version endpoint and verify the exact product identity. A malformed,
timed-out, oversized, stale, or forged exchange fails closed: Agent publishes no
session and terminates and reaps the supervised process tree.

Meshtastic `transport` is either `tcp` or `serial`, never both. Serial requires
a non-empty, control-character-free platform device identifier of at most 4096
bytes, such as `/dev/ttyUSB0` or `COM3`; it is not required to be an absolute
filesystem path. `serial_baud_rate` is a positive integer and defaults to
115200. CallMesh provisions the APRS identity, and Gateway derives the runtime
passcode locally. The APRS section above is limited to optional
endpoint/destination overrides. Existing
`login_callsign`, `symbol_table`, `symbol_code`, and `comment` fields from rc.1
are accepted only so an upgrade can start; Agent ignores them, never injects
them, and administrators must remove them. Other unknown fields, including an
inline passcode, are rejected. The CallMesh key lives only in root-level
`~/.cmclient/secrets.json`. The
control/API `aprs-passcode` name is retained only to remove values left by an
older installation. Setting it returns `CONTROL_SECRET_KIND_DEPRECATED`; Agent
does not read or inject an old stored value when launching APRS.

```bash
cmclient secret set callmesh-api-key
cmclient secret remove aprs-passcode # upgraded installations only
```

The command reads one bounded, non-control-character UTF-8 value from stdin.
The Agent returns only whether a value was stored. `secrets.json` is the sole
runtime backend on macOS, Windows, Linux, and Docker. Writes are atomic; POSIX
uses `0700` for the root and `0600` for the file, while Windows uses an ordinary
current-user file without UAC or cross-principal ACL qualification. Secret
values are never echoed, logged, serialized into diagnostics, copied to
backups, or passed through argv or environment.

Storage and removal complete immediately, but runtime consumers have different
refresh boundaries. At Gateway launch Agent sends only the CallMesh key through
the private bootstrap pipe and passes validated non-secret transport/endpoint overrides;
CallMesh-provisioned APRS identity and credential handling stay inside the
Gateway contract. `cmclient restart` restarts only that Gateway and reuses the
snapshot. After changing the CallMesh key, restart the entire Agent or its
platform service. An interactive Agent must likewise be terminated and
relaunched.

## Generate the Management password hash

`management_lan.password_hash` stores an Argon2id PHC string, never the raw
password. With the audited reference `argon2` CLI and OpenSSL installed, this
example reads the password without echoing it and selects parameters accepted
by Agent:

```bash
IFS= read -r -s -p 'Management password: ' password
printf '\n' >&2
salt="$(openssl rand -hex 16)"
printf '%s' "$password" | argon2 "$salt" \
  -id -v 13 -k 32768 -t 3 -p 1 -l 32 -e
unset password salt
```

Put the single emitted `$argon2id$v=19$...` line in `password_hash`. Agent
accepts only Argon2id version 19 with memory from 19,456 through 65,536 KiB,
2 through 6 iterations, 1 through 4 lanes, at least 8 decoded salt bytes, and a
16 through 64 byte output. It rejects other algorithms, omitted or extra PHC
parameters, and values outside those bounds.

## Management LAN boundary

The Management Web listener may be disabled with
`agent.management_web_enabled = false`. When enabled, Agent binds separate
IPv4 `0.0.0.0:<port>` and IPv6 `[::]:<port>` sockets; the IPv6 socket is
v6-only and address reuse is disabled. The default port is `7080`. Both binds
must succeed on the same port, so a conflict or unavailable address family
fails the complete listener start and leaves no partial one-family listener.
The URL advertised to local clients remains `http://127.0.0.1:7080/` (or the
configured scheme and port).

Wildcard sockets do not enable wildcard access. Native mode admits only a
loopback socket peer by default and ignores `Forwarded`, `X-Forwarded-*`, and
similar client-supplied addresses. LAN access is enabled only by a valid
`management_lan` section and always requires application authentication. CIDR
filtering is optional; an empty `allowed_cidrs` list permits any socket peer
that passes the authenticated LAN policy, so deployments should configure the
narrowest practical networks:

```toml
[management_lan]
port = 7443
allowed_cidrs = ["192.168.1.0/24"]
allowed_hosts = ["cmclient.example:7443"]
password_hash = "$argon2id$..."
allowed_origins = ["https://cmclient.example:7443"]
session_ttl_seconds = 3600
audit_capacity = 512
certificate_path = "/absolute/path/management-cert.pem"
private_key_path = "/absolute/path/management-key.pem"
```

`allowed_hosts` adds exact case-insensitive HTTP Host authorities; wildcard,
missing, duplicated, malformed, or unlisted Host values are rejected before
static files, login, API, SSE, or proxy handling. The exact authority of every
`allowed_origins` entry is added to the Host allowlist automatically. An
optional non-loopback `bind` value is only admission shorthand: it contributes
one `/32` or `/128` CIDR and its `<address>:<port>` Host authority, but the
actual sockets remain the two wildcard listeners above.

TLS is optional. `certificate_path` and `private_key_path` must either both be
absent or both be absolute paths to readable PEM material; invalid material
fails startup and is never downgraded to HTTP. Without TLS, authenticated LAN
HTTP remains available but every response carries the stable
`MANAGEMENT_HTTP_LAN_WARNING`, and Agent records the same code-only audit
outcome. Use TLS or a trusted private transport wherever credentials or session
traffic could be observed.

Browser login accepts only an exact configured HTTP or HTTPS Origin, verifies
the Argon2id PHC hash, cycles the server-side session ID, and returns a separate
CSRF token. The session cookie is `HttpOnly; SameSite=Strict` and is also
`Secure` when Agent serves TLS. LAN reads require a valid admin session; writes
also require an exact allowed Origin and the matching `x-csrf-token` header.
Native loopback writes use the corresponding exact local Origin, local session,
and CSRF token. No route emits wildcard CORS. Request and login rate limits are
keyed from the socket peer, not forwarding headers.

Docker Web startup also requires a valid `management_lan` access controller,
and Docker never grants the Native loopback/local-session bypass to its bridge
peer. Its protected APIs therefore always require the authenticated admin
session, with the same Host, Origin, CSRF, CIDR, and rate policies.

The bounded in-memory audit ring stores only timestamp, stable action, and
stable outcome code. It never stores the peer address, password, cookie, CSRF
token, request body, raw header, credential, identity, or location, and it is
not a persisted audit log. Stopping Web or changing its setup/session generation
revokes existing sessions while the separate local Control IPC remains
available.

The Gateway Fastify routes are not an independent LAN security boundary. In a
native deployment they are bound to an OS-assigned loopback port behind Agent,
authenticated with a per-generation capability, and never configured or
published directly. Docker has the same Agent-issued capability boundary; a
standalone Gateway is not a supported Docker topology. Until the unified Agent
container entrypoint replaces the superseded multi-service Compose descriptor,
that descriptor fails closed with `GATEWAY_SUPERVISION_REQUIRED`. Do not
publish a raw Gateway port and assume browser session/CSRF protection applies.

## Remote command access

The former persisted HMAC Control token and `CMCLIENT_CONTROL_TOKEN`
environment path are deprecated and unavailable. Command mode is local through
the framed Control IPC. Remote management uses the authenticated Web session
boundary owned by Agent; no Control credential is persisted separately.

## Fail-closed rules

Invalid paths, unknown TOML fields (including legacy `gateway_port`), mixed
transports, dual-stack bind conflicts, malformed CIDRs or Host authorities,
incomplete or invalid TLS material, weak Argon2 parameters, invalid origins,
expired sessions, missing or mismatched CSRF tokens, malformed HMAC headers,
replayed nonces or stale ownership proofs, insufficient GPS precision, and
unprovable position freshness are rejected.
Secrets and raw packet payloads are redacted from structured logs and
diagnostics. Error handling uses stable codes and bounded parameters rather
than backend prose.

Update signing keys are release credentials, not runtime configuration. The
production signing workflow exposes them only after protected human approval;
this RC workflow never treats an unsigned artifact as production signed.
