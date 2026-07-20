# Deployment

> Historical P12 snapshot. Where this file conflicts with
> [Documentation Authority](../READ_ORDER.md), it is implementation/evidence
> history rather than the current install or release contract.

CMClient has one runtime composition and several launch surfaces. A native
Desktop installer is a packaging format, not a second Agent implementation.
All complete Desktop, Headless, and Service bundles contain Agent, CLI,
migration tooling, production Gateway/Web output, the locked protobuf corpus,
and the platform service support files.

## Portable archives

Use a verified `headless` archive for a server and a `desktop` archive for a
supervisor host. Extract to an immutable versioned directory such as
`/opt/cmclient/releases/2.0.0-rc.1` or a per-user application directory, then
point the service manager at that release. Keep data, config, cache, and logs
outside the release directory. The Agent updater owns this layout for future
signed manifests; do not replace `active-release` or its journal manually.

The standalone `cli` archive contains only `bin/cmclient` and does not start a
Gateway. The Windows `service` archive adds the Service Host and SCM manager.

## Native Desktop packages

| Target | Packages |
| --- | --- |
| macOS Apple Silicon / Intel | DMG |
| Linux ARM64 / x64 | DEB and AppImage |
| Windows x64 | MSI and NSIS (`setup.exe`) |

Each package embeds the portable composition at `cmclient-runtime/`. The
collector and native smoke gate verify that tree and bounded-launch the Tauri
executable. Windows MSI uses a numeric-only internal prerelease (`2.0.0-1` for
this RC) because WiX cannot encode `rc.1`; the external filename, manifest,
checksum, and release identity remain `2.0.0-rc.1`.

An AppImage is not a stable system-service installation path. A native package
does not create a privileged service or silently claim ownership of the Agent.
Register the embedded Agent explicitly with the platform manager below.

## Linux systemd

The package includes `scripts/cmclient-systemd.sh` and a unit template. Run it
as an administrator with an absolute Agent path:

```bash
sudo scripts/cmclient-systemd.sh install \
  --agent /opt/cmclient/current/bin/cmclient-agent \
  --config-dir /etc/cmclient \
  --data-dir /var/lib/cmclient \
  --cache-dir /var/cache/cmclient \
  --log-dir /var/log/cmclient
sudo scripts/cmclient-systemd.sh status
sudo scripts/cmclient-systemd.sh logs --lines 200
```

The manager's `status` command reports systemd unit state, not the Agent health
contract. Query the running service through its explicit private socket:

```bash
sudo cmclient --endpoint unix:///var/lib/cmclient/control.sock status --json
```

Replace `/var/lib/cmclient` when installing with a different `--data-dir`.

The unit uses a non-login `cmclient` account, `UMask=0077`, no new privileges,
restricted filesystem access, and a bounded restart policy. The installer
creates a root-only wrapping key, supplies it with systemd `LoadCredential`,
and keeps only authenticated ciphertext under the service data directory.
`uninstall` removes the unit and stops it but retains configuration, the
wrapping key, data, cache, and logs.

## macOS launchd

The package includes `scripts/cmclient-launchd.sh` and a per-user LaunchAgent
template. It runs as the logged-in user:

```bash
scripts/cmclient-launchd.sh install \
  --agent "$HOME/Applications/CMClient/current/bin/cmclient-agent"
scripts/cmclient-launchd.sh status
scripts/cmclient-launchd.sh logs --lines 200
```

`uninstall` removes only the plist and retains the user data and logs. launchd
routes unmanaged stdout/stderr to `/dev/null`; Agent and Supervisor own the
bounded `agent.jsonl` and `gateway.jsonl` files. A launchd process is not a
system daemon and must not be described as one.

## Windows Service

The Windows service archive contains `cmclient-service-host.exe`, the adjacent
Agent, and `scripts/cmclient-windows-service.ps1`. Register from an elevated
PowerShell session using an absolute bundle path; the Service Host locates the
adjacent Agent. It bridges only SCM stop/shutdown into one bounded Agent
shutdown request over the private pipe; CLI and Desktop connect directly to
Agent rather than proxying through Service Host. The service account must see
the documented external Node.js installation on `PATH`; `node.exe` is not
copied into the bundle.

The install/upgrade/uninstall operation retains the configured data directory.
The final release gate starts the SCM service, checks Agent/CLI/Gateway health
and exact version/commit/channel, then removes the registration without
deleting retained state. Run the PowerShell 5.1-compatible manager from an
elevated session with explicit parameters:

The Service Host deliberately overrides interactive-user paths. It runs Agent
with `config`, `data`, `cache`, and `logs` below
`%PROGRAMDATA%\CMClient`; place the service's `agent.toml` at
`%PROGRAMDATA%\CMClient\config\agent.toml`. `%APPDATA%\CMClient` is for an
interactive per-user Agent and is not read by the Windows Service.

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
supported. Windows operations retain bounded `service-host.jsonl`,
`agent.jsonl`, and `gateway.jsonl` files below `%PROGRAMDATA%\CMClient\logs`.

## Docker OCI

Import the exact platform archive and run the checksum-covered, versioned
`cmclient-docker-compose-2.0.0-rc.1.yml` descriptor with `--no-build`. The x64
and ARM64 images are separate single-platform OCI archives. Convert the OCI
layout for Docker Engine before loading it:

```bash
skopeo copy --format v2s2 \
  "oci-archive:cmclient-docker-linux-x86_64-2.0.0-rc.1.oci.tar" \
  "docker-archive:cmclient.docker.tar:cmclient:2.0.0-rc.1"
docker load --input cmclient.docker.tar
CMCLIENT_IMAGE=cmclient:2.0.0-rc.1 \
  docker compose --file cmclient-docker-compose-2.0.0-rc.1.yml \
  up --detach --no-build --force-recreate
```

Only the Ingress port is host-facing; Gateway and Web are internal, read-only,
non-privileged services. Docker reports `docker` capability and fails closed
for `update`, `serial`, `service`, and `autoStart`.

Docker archives are source-bound and timestamp-normalized, but not byte-for-byte
reproducible because Debian package indexes are resolved during the build. The
release metadata, outer SHA-256, OCI descriptor digests, config labels, SBOM,
and no-rebuild runtime smoke close that integrity boundary.

## Upgrade and removal

For a signed update, only Agent may download and verify a selected
component/target bundle. It stages by digest, snapshots config/data outside the
release tree, migrates, starts the new release, and requires a health gate. A
failure records a stable code and rolls back the active pointer and backup.
Never overwrite a live release in place.

Service-manager uninstall removes registration files only. It must retain
`agent.toml`, the SQLite database, update journal, backups, cache (unless an
operator explicitly purges it), and logs. Docker `down` removes containers but
does not imply deletion of an externally mounted data volume.
