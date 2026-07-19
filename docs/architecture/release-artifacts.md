# Release Build Matrix and Artifact Composition

CMClient 2.0 has one canonical release matrix shared by build, archive,
checksum, signature, provenance, and updater-manifest generation. It uses the
same target identifiers accepted by the signed update contract:

| Target | CI runner | Rust target |
| --- | --- | --- |
| `darwin-aarch64` | `macos-15` | `aarch64-apple-darwin` |
| `darwin-x86_64` | `macos-15-intel` | `x86_64-apple-darwin` |
| `linux-aarch64` | `ubuntu-22.04-arm` | `aarch64-unknown-linux-gnu` |
| `linux-x86_64` | `ubuntu-22.04` | `x86_64-unknown-linux-gnu` |
| `windows-x86_64` | `windows-latest` | `x86_64-pc-windows-msvc` |

Every target builds `desktop`, `headless`, and `cli`; Windows additionally
builds `service`. An artifact name identifies a complete product surface, not
the single executable that happened to start its build:

| Surface | Canonical archive contents |
| --- | --- |
| CLI | `bin/cmclient[.exe]` only |
| Headless | Agent, CLI, Legacy migration tool, production Gateway deployment, compiled Web, locked protobuf corpus, and the target's systemd or launchd support files |
| Desktop | Desktop executable plus the complete Headless composition |
| Windows Service | Service Host plus the complete Windows Headless composition and SCM manager |
| Docker | Separate constrained OCI image containing Gateway and Web only |

The production Gateway directory contains its compiled `dist`, package
metadata, and production `node_modules`; root `proto/` contains the locked
Meshtastic schema corpus; the Web directory contains the Vite production
output. Install-time `node_modules/.bin` links are not runtime
inputs and are omitted so signed updater archives remain symlink-free. Linux
and macOS service managers are carried by Headless and Desktop because those
deployment modes execute the same Agent composition. The Windows Service Host
is kept in the Windows-only Service archive and locates the adjacent Agent.

Headless, Desktop, and Windows Service archives currently require Node.js 22
or newer on the service account's `PATH`; the standalone CLI does not. The
runtime smoke fails before starting the Agent when this prerequisite is not
met. Service installations must therefore expose the same Node installation to
systemd, launchd, or the Windows service account rather than relying on an
interactive shell's private `PATH`.

The current Desktop release role is the raw Tauri executable inside the
complete portable composition. It is not yet a platform installer such as DMG,
MSI, or a Linux package. The current runtime smoke starts staged Headless on
every target and the final Linux x64 Headless archive; final Desktop and Windows
Service archive launch smoke and platform installer production remain explicit
release gates rather than implied properties of this matrix.

Linux artifacts are built on the pinned Ubuntu 22.04 runners rather than
`ubuntu-latest`. This keeps the native glibc baseline compatible with current
Raspberry Pi ARM64 and long-lived x64 deployments while preventing a runner
image migration from silently raising it.

## Stable names

The final updater asset name is always:

```text
cmclient-<component>-<target>-<semver>.<archive>
```

Windows assets use `zip`; Darwin and Linux assets use `tar.zst`. For example:

```text
cmclient-desktop-darwin-aarch64-2.0.0.tar.zst
cmclient-headless-linux-aarch64-2.0.0.tar.zst
cmclient-cli-windows-x86_64-2.0.0.zip
cmclient-service-windows-x86_64-2.0.0.zip
```

`scripts/release-artifacts.mjs` is the executable source of truth. Its plan
schema records every artifact's canonical role/path list and the separate
Docker composition. Staging requires exactly those roles, verifies that the
Gateway and Web inputs are production outputs, rejects symlinks and special
files, and emits a schema-v2 build manifest with a deterministic nested file
inventory. Unknown, missing, or extra roles fail before an artifact can reach
the supply-chain job.

CI uploads staged inputs under
`cmclient-build-<component>-<target>-<semver>`; those are intentionally not
published release assets. `scripts/release-supply-chain.mjs` validates each
manifest against the canonical composition before producing archives,
checksums, SBOM, provenance subjects, and, when explicitly authorized, a
signed update manifest. Every target runs the staged Headless composition; the
supply-chain job also extracts and starts the final Linux x64 Headless archive
so archive creation cannot silently change its runtime layout or executable
modes.

The release workflow owns a `load-gate` job in addition to ordinary CI. Every
platform build and the Docker composition require it, so an independent manual
workflow dispatch cannot assemble, attest, or sign release inputs while the
bounded resource gate is failing or was never run for that revision.

## Docker image

Docker is a separately versioned deployment image, not an Agent-updater
component. The release workflow runs its static restriction test and real
compose smoke before binary supply-chain assembly can proceed. The image
contains Gateway and Web only; it cannot contain Agent, CLI, Desktop, Service
Host, or updater ownership, and it cannot self-update. The workflow verifies
the image but does not publish it from ordinary `dev` or pull-request runs.

The compose deployment and capability restrictions are documented in
[Docker Deployment](./docker-deployment.md). Archive, SBOM, checksum,
attestation, and offline manifest-signing boundaries are documented in
[Release Supply Chain](./supply-chain.md). Clean install, upgrade, and
uninstall retention semantics are defined in
[Packaging Lifecycle Matrix](./packaging-lifecycle.md).
