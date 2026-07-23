# Deployment

CMClient has one runtime composition and several launch modes. Each native
package contains graphical mode, command mode, Agent, Gateway, full Web,
migration/update helpers, the locked protobuf corpus, and a pinned private Node
runtime. Docker omits graphical mode only. System Node, npm, and pnpm are not
runtime prerequisites.

## Portable archives

Portable component archives are internal build and qualification inputs, not
public install choices. When used by a developer, extract one immutable runtime
outside `~/.cmclient`; do not move binaries, caches, or generated build output
into the state root. There is no standalone Desktop, Headless, CLI, or Service
download.

## Native Desktop packages

| Target | Packages |
| --- | --- |
| macOS Apple Silicon / Intel | One Universal `CMClient.dmg` |
| Linux ARM64 / x64 | One AppImage per CPU |
| Windows x64 | One current-user `CMClient-Setup.exe` |

The package owns only installed program files and reversible user integration.
Mutable state remains in the effective user's `~/.cmclient`; Windows resolves
that to `%USERPROFILE%\.cmclient`. Native install registers current-user login
startup by default, and the Web setting can disable it. Windows uses no routine
UAC or SCM service, macOS uses `SMAppService`, and Linux graphical sessions use
one XDG autostart entry. Uninstall retains the entire state root.

## Linux systemd

Linux headless installations may use the system-level systemd fallback instead
of XDG autostart. It runs as the dedicated `cmclient` account, whose effective
HOME defaults to `/home/cmclient`; `--home` is a packaging-only override. The
unit injects only HOME before Agent takes its immutable startup snapshot:

```bash
sudo scripts/cmclient-systemd.sh install \
  --agent /opt/cmclient/current/bin/cmclient-agent \
  --home /home/cmclient
sudo scripts/cmclient-systemd.sh status
sudo scripts/cmclient-systemd.sh logs --lines 200
```

The manager's `status` command reports unit state, not Agent health. Root may
query the service account's private socket explicitly:

```bash
sudo cmclient \
  --endpoint unix:///home/cmclient/.cmclient/run/control.sock status --json
```

The unit uses `UMask=0077`, no new privileges, restricted filesystem access,
and bounded restart. It does not accept split directory overrides or create a
second service state root. Agent atomically stores runtime secrets only in
`/home/cmclient/.cmclient/secrets.json`; the unit has no credential mount,
wrapping key, secret environment file, or encrypted vault. `uninstall` removes
registration and retains the configured service HOME's `.cmclient` directory.

## macOS launchd

The package includes `scripts/cmclient-launchd.sh` and a per-user LaunchAgent
template. It runs as the logged-in user:

```bash
scripts/cmclient-launchd.sh install \
  --agent "$HOME/Applications/CMClient/current/bin/cmclient-agent"
scripts/cmclient-launchd.sh status
scripts/cmclient-launchd.sh logs --lines 200
```

`uninstall` removes only the plist and retains `~/.cmclient`. launchd
routes unmanaged stdout/stderr to `/dev/null`; Agent and Supervisor own the
bounded `agent.jsonl.YYYY-MM-DD` and `gateway.jsonl.YYYY-MM-DD` families. A launchd process is not a
system daemon and must not be described as one.

## Windows Service

The public Windows Setup uses current-user login startup, not SCM. The retained
`cmclient-service-host.exe` and `scripts/cmclient-windows-service.ps1` are a
transitional qualification surface only. They bridge SCM stop/shutdown into a
bounded Agent shutdown request; CLI and Desktop still connect directly to
Agent. They are not a second product, do not supply an external Node runtime,
and cannot create a ProgramData data root. If exercised in a controlled test,
the service identity must have a valid effective home and all mutable paths
derive from that identity's `.cmclient` directory.

```powershell
$manager = 'C:\Program Files\CMClient\scripts\cmclient-windows-service.ps1'
$hostPath = 'C:\Program Files\CMClient\bin\cmclient-service-host.exe'
& $manager -Command install -HostPath $hostPath
& $manager -Command status -HostPath $hostPath
& $manager -Command logs -HostPath $hostPath -Lines 200
& $manager -Command uninstall -HostPath $hostPath
```

`-Command` also accepts `start`, `stop`, `restart`, `logs`, and `render`;
installation accepts `-NoStart`. The SCM identity is the fixed singleton
`CMClientAgent`; custom service names and a `-ServiceName` parameter are not
supported. Qualification retains bounded `service-host.jsonl.YYYY-MM-DD`,
`agent.jsonl.YYYY-MM-DD`, and `gateway.jsonl.YYYY-MM-DD` families below the
effective home's `.cmclient\logs`. This does not qualify SCM as a public install
path.

## Docker OCI

Use the exact immutable digest from the release's multi-platform OCI index and
the checksum-covered `cmclient-docker-compose-2.0.0-rc.1.yml` descriptor. The
index contains `linux/amd64` and `linux/arm64`; Docker selects the matching child
manifest. Do not rebuild or combine per-architecture payloads locally:

```bash
CMCLIENT_IMAGE=registry.example/cmclient@sha256:<verified-index-digest> \
  docker compose --file cmclient-docker-compose-2.0.0-rc.1.yml \
  up --detach --no-build --force-recreate --wait
```

The one `cmclient` service runs Agent plus its supervised Gateway with
`init: true`; only Web is host-facing. The image is non-privileged and uses the
fixed `/home/cmclient/.cmclient` volume. Docker reports its capability limits
and fails closed for graphical, serial, host service, and self-update actions.

Release metadata binds the index digest, child-manifest/config/layer digests,
source identity, labels, SBOM, and no-rebuild runtime smoke. Current-host
evidence never substitutes for the other architecture.

## Upgrade and removal

For a signed update, only Agent may download and verify a selected
component/target bundle. It stages by digest, snapshots config/data outside the
release tree, migrates, starts the new release, and requires a health gate. A
failure records a stable code and rolls back the active pointer and backup.
Never overwrite a live release in place.

Native uninstall removes program files and reversible integration only. It must
retain `~/.cmclient/config.toml`, root-level `cmclient.db`, `secrets.json`, the
update journal, backups, cache, and logs. Secrets are excluded from backup and
rollback media. Docker `down` removes containers but does not imply deletion of
the `/home/cmclient/.cmclient` volume.
