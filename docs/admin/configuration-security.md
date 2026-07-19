# Configuration and Security

The Agent accepts a strict, `deny_unknown_fields` `agent.toml`. Runtime paths
must be absolute. Keep executable/configuration paths separate from persistent
data and never put secret values in this file.

## Minimal configuration

```toml
[agent]
gateway_port = 4810
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
login_callsign = "N0CALL-7"
host = "rotate.aprs2.net"
port = 14580
destination = "APCM20"
symbol_table = "/"
symbol_code = ">"

[proxy]
upstream_host = "127.0.0.1"
upstream_port = 4403
host = "127.0.0.1"
port = 4404
mode = "monitor"
allow_lan = false
```

Meshtastic `transport` is either `tcp` or `serial`, never both. Serial requires
a non-empty, control-character-free platform device identifier of at most 4096
bytes, such as `/dev/ttyUSB0` or `COM3`; it is not required to be an absolute
filesystem path. `serial_baud_rate` is a positive integer and defaults to
115200. APRS passcode, CallMesh key, and the Management admin token are separate
OS credential-store entries:

```bash
cmclient secret set callmesh-api-key
cmclient secret set aprs-passcode
cmclient secret set management-admin-token
```

Those commands use the interactive user's default Control socket. For the
packaged systemd service, address its data-directory socket explicitly and run
the client with sufficient permission, for example:

```bash
sudo cmclient --endpoint unix:///var/lib/cmclient/control.sock \
  secret set callmesh-api-key
```

The command reads one bounded, non-control-character UTF-8 value from stdin.
The Agent returns only whether a value was stored. Interactive macOS, Windows,
and Linux sessions use Keychain, Credential Manager, or Secret Service. The
packaged systemd service instead receives a root-owned wrapping key through
`LoadCredential` and keeps XChaCha20-Poly1305 ciphertext under its private data
directory; neither half is placed in the unit environment. A missing,
malformed, or tampered service vault fails with
`AGENT_SECRET_STORE_UNAVAILABLE`. Secret values are never echoed, logged,
serialized into diagnostics, or passed as a CLI argument.

Storage and removal complete immediately, but runtime consumers have different
refresh boundaries. Agent reads `management-admin-token` for every remote
Control request, so that value changes immediately. It copies the CallMesh key
and APRS passcode into the supervised Gateway environment when Agent starts;
`cmclient restart` restarts only that Gateway and reuses the snapshot. After
setting or removing either Gateway credential, restart the entire Agent or its
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

The default Web listener is loopback HTTP and may be disabled. A non-loopback
listener requires the complete strict section:

```toml
[management_lan]
bind = "192.168.1.10"
port = 7443
password_hash = "$argon2id$..."
allowed_origins = ["https://cmclient.example"]
session_ttl_seconds = 3600
audit_capacity = 512
certificate_path = "/absolute/path/management-cert.pem"
private_key_path = "/absolute/path/management-key.pem"
```

The certificate and private key must be readable PEM files. TLS is never
downgraded. Browser login accepts only an allowed HTTPS Origin, verifies the
Argon2id PHC hash, and issues a short-lived `Secure; HttpOnly; SameSite=Strict`
session cookie plus a separate CSRF token. Reads require a session; writes
require the session, matching Origin, and CSRF header. Login attempts and
sessions are bounded and pruned, and the audit ring stores only stable action
and result codes.

The Gateway Fastify routes are not an independent LAN security boundary. They
are intended to be loopback-only behind Agent. Docker exposes only its fixed
Ingress. Do not publish a raw Gateway port and assume browser session/CSRF
protection still applies.

## Remote CLI HMAC

The remote CLI uses the opt-in Agent HTTPS Control bridge, not the browser
cookie. Agent verifies requests against its OS-stored `management-admin-token`;
the remote CLI cannot read that credential store and must receive the same
value in its own process's `CMCLIENT_CONTROL_TOKEN` environment variable. It
signs method, path, body digest, timestamp, nonce, and `control:admin` scope.
The value must be 32 through 4096 UTF-8 bytes without ASCII control characters.
The endpoint must be an HTTPS origin root: credentials, a non-root path, query,
fragment, and redirects are rejected. The Agent accepts a 30-second clock
window and rejects nonce replay. Required headers are documented in
[Local Agent Control API](../api/local-control.md).

## Fail-closed rules

Invalid paths, unknown TOML fields, zero Gateway ports, mixed transports,
missing TLS files, weak Argon2 parameters, invalid origins, expired sessions,
malformed HMAC headers, replayed nonces, insufficient GPS precision, and
unprovable position freshness are rejected. Secrets and raw packet payloads are
redacted from structured logs and diagnostics. Error handling uses stable codes
and bounded parameters rather than backend prose.

Update signing keys are release credentials, not runtime configuration. The
production signing workflow exposes them only after protected human approval;
this RC workflow never treats an unsigned artifact as production signed.
