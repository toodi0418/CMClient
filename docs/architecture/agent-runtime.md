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

The optional `agent.toml` currently accepts only non-secret operational
settings:

```toml
[agent]
gateway_command = ["cmclient-gateway", "serve"]
management_web_enabled = true
```

CallMesh keys, APRS passcodes, administrative tokens, and signing keys do not
belong in this file or in Agent command arguments. Their storage and redaction
are introduced by the security and updater phases.
