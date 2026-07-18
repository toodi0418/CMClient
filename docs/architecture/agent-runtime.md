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
gateway_command = ["cmclient-gateway", "serve"]
gateway_port = 4810
management_web_enabled = true
```

CallMesh keys, APRS passcodes, administrative tokens, and signing keys do not
belong in this file or in Agent command arguments. Their storage and redaction
are introduced by the security and updater phases.

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
matching allowed Origin and CSRF token. Login attempts are rate-limited per
source address in memory. The bounded audit projection records only timestamp,
action, and stable outcome code, never addresses, credentials, cookies, or
tokens.

The Agent injects `CMCLIENT_GATEWAY_HOST=127.0.0.1`, the configured non-zero
`CMCLIENT_GATEWAY_PORT`, and its own `CMCLIENT_DATA_DIR` into the supervised
Gateway process. This keeps the Gateway data store and the Agent's health/proxy
endpoint aligned without exposing a Gateway listener to the LAN.

Verified update archives are transient Agent cache data under
`<cache_dir>/updates/staging`. They are selected from a signed manifest, streamed
with an exact byte limit, SHA-256 verified, and atomically published by digest.
This cache must not be treated as user data and updates must not overwrite the
Agent data or configuration directories.
