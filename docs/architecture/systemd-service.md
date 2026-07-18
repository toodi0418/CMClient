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
argument or environment file. Runtime credentials belong to the service
account's supported OS credential backend; if that backend is unavailable, the
Agent fails closed rather than falling back to plaintext configuration.

```bash
bash scripts/cmclient-systemd.sh install \
  --agent /opt/cmclient/current/bin/cmclient-agent
bash scripts/cmclient-systemd.sh status
bash scripts/cmclient-systemd.sh logs --lines 200
bash scripts/cmclient-systemd.sh restart
bash scripts/cmclient-systemd.sh uninstall
```

`install` is repeatable and regenerates the unit from
`packaging/systemd/cmclient-agent.service.in`. It creates missing runtime
directories but does not write `agent.toml`; package installation or the
administrator owns the non-secret Agent configuration. A supplied config file
must be readable by the `cmclient` group, for example `root:cmclient` with mode
`0640`. `uninstall` disables and removes only the unit. It deliberately retains
configuration, data, cache, and logs so a clean reinstall or rollback cannot
destroy user state.
