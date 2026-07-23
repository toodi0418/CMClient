# systemd Agent Service

`scripts/cmclient-systemd.sh` installs and manages one system-level fallback:
`cmclient-agent.service`. It is intended for a Linux headless host where XDG
login startup is not suitable. It runs only the Rust Agent; Agent supervises
the private Gateway and Gateway never updates itself.

The installer defaults to the non-login `cmclient` account with effective HOME
`/home/cmclient`. A packaging workflow may select another absolute service HOME
with `--home`, but runtime never receives split state-directory overrides.

| Purpose | Default path |
| --- | --- |
| Installed release | `/opt/cmclient/current` |
| State root | `/home/cmclient/.cmclient` |
| Configuration | `/home/cmclient/.cmclient/config.toml` |
| Database | `/home/cmclient/.cmclient/cmclient.db` |
| Plaintext secrets | `/home/cmclient/.cmclient/secrets.json` |
| Local Control | `/home/cmclient/.cmclient/run/control.sock` |
| State and cache | `/home/cmclient/.cmclient/state` and `/home/cmclient/.cmclient/cache` |
| Logs | `/home/cmclient/.cmclient/logs` |
| Backups and updates | `/home/cmclient/.cmclient/backups` and `/home/cmclient/.cmclient/updates` |

The generated unit injects only HOME, validates configuration, and then starts
`cmclient-agent --serve`. Agent takes one immutable startup snapshot and derives
every mutable path from `$HOME/.cmclient`. The unit uses `UMask=0077`, no ambient
capabilities, `NoNewPrivileges`, restricted system paths, a private temporary
directory, and bounded restart behavior. Serial access remains governed by
ordinary device permissions; an administrator may add the existing service
account to the platform's serial-device group.

## Operations

The manager requires root or `sudo` to install the system unit. It never accepts
an API key, APRS passcode, Control token, browser password, or signing key in an
argument or environment file.

```bash
sudo bash scripts/cmclient-systemd.sh install \
  --agent /opt/cmclient/current/bin/cmclient-agent \
  --home /home/cmclient
sudo bash scripts/cmclient-systemd.sh status
sudo bash scripts/cmclient-systemd.sh logs --lines 200
sudo bash scripts/cmclient-systemd.sh restart
sudo bash scripts/cmclient-systemd.sh uninstall
```

Agent atomically writes the sole runtime secret backend at
`/home/cmclient/.cmclient/secrets.json`. The state root and its directories are
mode `0700` and the file is mode `0600`. CMClient does not use Keychain,
Credential Manager/DPAPI, Secret Service, a systemd credential mount, wrapping
key, or encrypted vault. A secret never enters the unit, argv, environment,
logs, diagnostics, backup, or evidence. Agent transfers a required CallMesh key
to Gateway only through the private inherited bootstrap channel.

`logs --lines N` accepts `1..10000` and first tails at most `N` records from
the newest `agent.jsonl.YYYY-MM-DD` and `gateway.jsonl.YYYY-MM-DD` files below
`/home/cmclient/.cmclient/logs`. Fixed-name legacy files remain a bounded
fallback. The manager rejects links and non-file entries. If neither application
log exists during early startup, it reads at most `N` journal entries and emits
only stable uppercase error codes; arbitrary raw journal messages never pass
through this command.

The Control socket is private to the service account. Root can operate the
instance by selecting its exact local endpoint:

```bash
sudo /opt/cmclient/current/bin/cmclient \
  --endpoint unix:///home/cmclient/.cmclient/run/control.sock status
sudo /opt/cmclient/current/bin/cmclient \
  --endpoint unix:///home/cmclient/.cmclient/run/control.sock \
  secret set callmesh-api-key
```

The CLI sends a new value over local IPC and never reads `secrets.json`.
Restart the whole Agent service after changing the CallMesh key so the next
Gateway generation receives it through private bootstrap. CallMesh provision
supplies APRS identity and Gateway derives the runtime passcode; a static APRS
passcode and persisted Control credential are unavailable.

`install` is repeatable and regenerates the unit from
`packaging/systemd/cmclient-agent.service.in`. It creates the service HOME and
private runtime directories but does not synthesize `config.toml` or secrets.
`uninstall` disables and removes only the unit. It deliberately retains the
service account's entire `.cmclient` root so reinstall, update, or rollback
cannot destroy user state.

CI runs the systemd integration smoke only on its declared Linux target. The
smoke installs the real unit, verifies the single HOME-derived path graph and
private Control socket, sets a sanitized fixture secret through stdin, proves
that no secret reaches argv/environment/logs/backup, restarts the service, and
checks state retention.
