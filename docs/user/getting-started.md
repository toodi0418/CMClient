# Getting Started

> Historical P12 snapshot. Where this file conflicts with
> [Documentation Authority](../READ_ORDER.md), it is implementation/evidence
> history rather than the current install or release contract.

CMClient 2.0 RC.1 is a local Agent, not a cloud account. Choose the product
surface that matches the host:

| Surface | Use it when | Host prerequisites |
| --- | --- | --- |
| Portable Desktop | You want the Tauri supervisor and the complete local runtime | Node.js `^22.18.0` or `>=24.11.0`; an Agent service must be started separately |
| Native Desktop package | You want a DMG, DEB, AppImage, MSI, or NSIS installer | Same runtime requirement; native package does not register a privileged Agent |
| Headless | A server or Raspberry Pi should run Agent, Gateway, Web, and CLI | Node.js `^22.18.0` or `>=24.11.0` |
| CLI | A shell or automation host only needs Agent control | No Node.js requirement for the standalone CLI |
| Docker OCI | Gateway, Web, and fixed Ingress should run without host control | Docker Compose; no Agent, serial, service, or updater capability |

The RC is not production-signed. Do not use it as evidence that a production
tag, notarization, or platform signing key has been approved.

## Verify an artifact

Download the archive, `SHA256SUMS`, and the matching release metadata from the
same workflow artifact. Select exactly one checksum row and require exactly one
match before extracting; do not use `--ignore-missing`, which can succeed
without checking the requested file:

```bash
set -eu
artifact='cmclient-headless-linux-x86_64-2.0.0-rc.1.tar.zst'
awk -v name="*$artifact" '$2 == name { print }' SHA256SUMS > "$artifact.sha256"
test "$(wc -l < "$artifact.sha256")" -eq 1
sha256sum -c "$artifact.sha256"
# macOS: shasum -a 256 -c "$artifact.sha256"
```

When the complete release directory is present, `sha256sum -c SHA256SUMS`
verifies the whole set and fails for every missing or changed subject.

The portable names are fixed:

```text
cmclient-<component>-<target>-2.0.0-rc.1.tar.zst
cmclient-<component>-windows-x86_64-2.0.0-rc.1.zip
```

Native Desktop files keep the same stem and use `.dmg`, `.deb`, `.AppImage`,
`.msi`, or `.setup.exe`. Docker is delivered as
`cmclient-docker-linux-x86_64-2.0.0-rc.1.oci.tar` and
`cmclient-docker-linux-aarch64-2.0.0-rc.1.oci.tar`. Native installers and OCI
archives are not Agent updater bundles.

## First local run

1. Extract a verified `headless` or `desktop` archive outside the data and
   configuration directories.
2. Put the required `agent.toml` in the platform configuration directory. The
   strict examples and path rules are in [configuration and security](../admin/configuration-security.md).
3. Ensure `node` resolves to the supported Node.js version for the service
   account. The Agent starts the adjacent production Gateway and Web bundle.
4. Start the Agent with `cmclient-agent --serve` (or use the documented service
   manager). `cmclient-agent --check-config` validates without starting a child.
5. Run the local checks:

```bash
cmclient status
cmclient doctor
cmclient web
cmclient --json version
```

`cmclient web` returns the Agent-owned Management Web URL. The default local
listener is `http://127.0.0.1:7080` when enabled. The private Control socket is
always available even when the Web listener is disabled. A live Gateway that
fails its health probe is reported as `degraded`, not `running`.

## Docker first run

Import the verified OCI archive without rebuilding it, then use the release
descriptor `cmclient-docker-compose-2.0.0-rc.1.yml` and the fixed Ingress port.
An OCI layout archive is not a Docker archive, so convert it with `skopeo`
before `docker load`. The compose topology exposes only Ingress to the host;
Gateway and Web stay on internal networks. The image reports its version,
source commit, and channel at `/api/v1/system/version`.

```bash
skopeo copy --format v2s2 \
  "oci-archive:cmclient-docker-linux-x86_64-2.0.0-rc.1.oci.tar" \
  "docker-archive:cmclient.docker.tar:cmclient:2.0.0-rc.1"
docker load --input cmclient.docker.tar
CMCLIENT_IMAGE=cmclient:2.0.0-rc.1 \
  docker compose --file cmclient-docker-compose-2.0.0-rc.1.yml \
  up --detach --no-build --force-recreate
```

Docker is an operator-managed deployment. It cannot start the host Agent,
access serial devices, install a system service, or perform an in-place Agent
update.

## Where state lives

CMClient keeps binaries separate from user data. macOS uses
`~/Library/Application Support/CMClient` and `~/Library/Caches/CMClient`;
Windows uses `%APPDATA%\CMClient` and `%LOCALAPPDATA%\CMClient\cache`;
Linux follows XDG data/config/cache directories under `~/.local/share`,
`~/.config`, and `~/.cache`. Logs default below the data directory. Absolute
`CMCLIENT_*_DIR` overrides are allowed; relative overrides fail closed.

Never put API keys, APRS credentials, browser passwords, or update signing keys
in `agent.toml`, a command argument, or a diagnostic bundle. Use standard input
to set only the CallMesh API key or Management admin token. APRS identity comes
from a valid CallMesh provision, and Gateway derives its runtime passcode
locally; neither is a static user-set secret.

For installation, upgrade, uninstall retention, and service registration see
[Deployment](../admin/deployment.md). For the day-to-day Web and CLI workflow
see [Using CMClient](./using-cmclient.md).
