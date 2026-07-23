# Getting Started

CMClient is one product with several launch modes. Every native package contains
graphical mode, command mode, full Web, Agent, Gateway, and a pinned private
Node runtime. Docker contains the same resident core, command mode, and Web but
omits graphical mode and native host integration. Users do not install Node,
npm, pnpm, a separate CLI, or a separate service package.

| Host | Public install object | Prerequisite |
| --- | --- | --- |
| Windows x86-64 | `CMClient-Setup.exe` | A supported Windows host; Setup carries offline WebView2 |
| macOS Intel / Apple Silicon | Universal `CMClient.dmg` | macOS 13.5 or newer |
| Linux x86-64 / ARM64 | One AppImage per CPU | Supported glibc/WebKitGTK host; FUSE or the documented extraction fallback |
| Docker amd64 / arm64 | One OCI image index plus Compose | Docker Engine 28+ and Compose |

The RC is not production-signed. Do not use it as evidence that a production
tag, notarization, or platform signing key has been approved.

## Verify an artifact

Download the install object, `SHA256SUMS`, and matching release metadata from
the same release set. Select exactly one checksum row and require exactly one
match before running it; do not use `--ignore-missing`, which can succeed
without checking the requested file:

```bash
set -eu
artifact='CMClient-x86_64.AppImage'
awk -v name="*$artifact" '$2 == name { print }' SHA256SUMS > "$artifact.sha256"
test "$(wc -l < "$artifact.sha256")" -eq 1
sha256sum -c "$artifact.sha256"
# macOS: shasum -a 256 -c "$artifact.sha256"
```

When the complete release directory is present, `sha256sum -c SHA256SUMS`
verifies the whole set and fails for every missing or changed subject.

Checksums, release metadata, SBOMs, provenance, and update payloads are support
files, not extra product choices. Candidate artifacts are not production signed
unless the release metadata explicitly records completion of that human gate.

## First local run

1. Install or launch the verified native object. It starts the resident core in
   the current-user context; no routine UAC or administrator service is needed.
2. Open graphical mode or run `cmclient web`. A missing key or reset redirects
   to the Web setup wizard.
3. Accept the terms, confirm the detected Meshtastic endpoint, and enter the
   CallMesh key. Agent stores it in `~/.cmclient/secrets.json` and never passes
   it through argv or environment.
4. Finish setup, then run the local checks:

```bash
cmclient status
cmclient doctor
cmclient web
cmclient --json version
```

`cmclient web` returns the Agent-owned Management Web URL. The default local
URL is `http://127.0.0.1:7080`; the listener binds wildcard addresses but rejects
non-loopback peers by default. The private Control endpoint remains available
when Web is disabled. A live Gateway that fails its health probe is `degraded`,
not `running`. Source-tree operators may use `cmclient-agent --serve` for a
bounded development run, but packaged users launch the unified product.

## Docker first run

Use the release Compose descriptor with the immutable image/index digest. One
service named `cmclient` runs Agent and its supervised Gateway; `init: true`
provides signal forwarding and orphan reaping. Compose publishes Web to host
loopback by default, and every Docker browser session still authenticates.

```bash
CMCLIENT_IMAGE=registry.example/cmclient@sha256:<verified-digest> \
  docker compose --file cmclient-docker-compose-2.0.0-rc.1.yml \
  up --detach --no-build --force-recreate --wait
docker compose exec -T cmclient cmclient setup-code
```

Docker is operator-managed. It does not include graphical mode, install a host
service, control the Docker socket, or perform an in-place image update.

## Where state lives

Every native platform uses the effective startup user's `~/.cmclient`; on
Windows this is `%USERPROFILE%\.cmclient`. Docker uses the fixed
`/home/cmclient/.cmclient`. Agent takes one immutable home snapshot at startup,
and split data/config/cache/log overrides or foreign roots fail closed.

```text
~/.cmclient/
  config.toml
  secrets.json
  cmclient.db
  state/
  run/
  cache/
  logs/
  backups/
  updates/
```

`secrets.json` is the sole plaintext runtime backend. It is atomically replaced;
POSIX uses private `0700`/`0600` paths and Windows uses a normal current-user
file without UAC. CMClient does not use Keychain, Credential Manager/DPAPI,
Secret Service, a systemd vault, or a persisted Control token. Never put keys,
browser passwords, APRS credentials, or signing keys in `config.toml`, command
arguments, environment variables, diagnostics, backups, or Git. APRS identity
comes from a valid CallMesh provision and Gateway derives its runtime passcode.

For installation, upgrade, uninstall retention, and service registration see
[Deployment](../admin/deployment.md). For the day-to-day Web and CLI workflow
see [Using CMClient](./using-cmclient.md).
