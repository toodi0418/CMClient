# systemd Agent Service

`scripts/cmclient-systemd.sh` installs and manages one system service:
`cmclient-agent.service`. It replaces the Legacy Node, git-pull, Docker Compose,
and environment-file service scripts. The service executes only the Rust Agent;
Gateway lifecycle remains inside the Agent supervisor and the Gateway never
updates itself.

The installer defaults to a non-login `cmclient` account and these separated
paths:

| Purpose | Default path |
| --- | --- |
| Installed release | `/opt/cmclient/current` |
| Agent configuration | `/etc/cmclient` |
| Persistent Agent data | `/var/lib/cmclient` |
| Cache and verified update staging | `/var/cache/cmclient` |
| Logs | `/var/log/cmclient` |

The generated unit validates configuration before it starts the Agent, starts
`cmclient-agent --serve`, restarts only runtime failures, and retains the
Agent's existing process lock. It has no ambient Linux capabilities, uses a
non-privileged user, a restrictive umask, a private temporary directory,
read-only system paths, and a small required socket-address family set. Serial
devices remain available through normal device permissions; an administrator
may pass `--serial-group dialout` (or their platform's equivalent existing
group) when installing the unit.

## Operations

The manager requires systemd and root or `sudo` for its normal `/etc` install.
It never accepts API keys, APRS passcodes, tokens, or a signing key in an
argument or environment file. A non-login service account has no user-session
Secret Service, so the installer creates one random, root-only 32-byte wrapping
key at `/etc/cmclient/secret-store.key` and passes it to Agent with systemd
`LoadCredential`. Actual runtime secrets still enter only through the Agent
Control API. Agent stores XChaCha20-Poly1305 authenticated ciphertext under
`/var/lib/cmclient/secrets`; the root-only key and service-owned ciphertext are
separate at rest, and neither value appears in the unit or process environment.
Agent and its supervised Gateway remain one service-account trust boundary:
the credential mount is not a sandbox against hostile code running inside that
same unit. Agent still passes only enabled integration secrets in the Gateway
environment and never passes the Management admin token.

```bash
bash scripts/cmclient-systemd.sh install \
  --agent /opt/cmclient/current/bin/cmclient-agent
bash scripts/cmclient-systemd.sh status
bash scripts/cmclient-systemd.sh logs --lines 200
bash scripts/cmclient-systemd.sh restart
bash scripts/cmclient-systemd.sh uninstall
```

`logs --lines N` accepts `1..10000` and first tails at most `N` records from
the newest `/var/log/cmclient/agent.jsonl.YYYY-MM-DD` and
`/var/log/cmclient/gateway.jsonl.YYYY-MM-DD` files. Fixed-name legacy files
remain a fallback. The manager rejects symlinks and non-file
entries. If neither application log exists during early startup, it reads at
most `N` journal entries and emits only stable uppercase error codes; arbitrary
raw journal messages never pass through this command. Agent owns JSONL
sanitization, file permissions, per-day size limits, and bounded daily retention.

The service Control socket is `/var/lib/cmclient/control.sock`, not an
administrator's per-user XDG socket. Root can operate the service instance with
an explicit endpoint:

```bash
sudo /opt/cmclient/current/bin/cmclient \
  --endpoint unix:///var/lib/cmclient/control.sock status
sudo /opt/cmclient/current/bin/cmclient \
  --endpoint unix:///var/lib/cmclient/control.sock secret set callmesh-api-key
```

The socket is mode `0600`; an ordinary interactive user cannot connect. A
CallMesh key is copied into the Gateway environment only when the whole Agent
starts, so restart `cmclient-agent.service` after changing it. A valid CallMesh
provision supplies APRS identity and credential inside Gateway; Agent never
accepts or injects a static APRS passcode. An older stored value can only be
deleted with `secret remove aprs-passcode`. The Management admin token is read
for each remote request and takes effect immediately.

`install` is repeatable and regenerates the unit from
`packaging/systemd/cmclient-agent.service.in`. It creates missing runtime
directories but does not write `agent.toml`; package installation or the
administrator owns the non-secret Agent configuration. A supplied config file
must be readable by the `cmclient` group, for example `root:cmclient` with mode
`0640`. `uninstall` disables and removes only the unit. It deliberately retains
configuration (including the wrapping key), data, cache, and logs so a clean
reinstall or rollback cannot destroy user state. Reinstalling preserves the
same key. If ciphertext exists but the key is missing, installation fails with
`SYSTEMD_SECRET_STORE_KEY_MISSING`; Agent rejects a missing, malformed, or
tampered store with `AGENT_SECRET_STORE_UNAVAILABLE` rather than replacing the
key or treating the secret as absent.

CI runs `scripts/cmclient-systemd-integration.sh` only on Ubuntu 22.04 with
systemd 249. The smoke installs the real unit, exercises `LoadCredential` and
the dedicated Unix Control socket, writes encrypted secret state, preserves the
wrapping key across reinstall, and proves decryption after a service restart.
