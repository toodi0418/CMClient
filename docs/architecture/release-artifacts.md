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
| Docker | Separate constrained OCI image running Gateway, Web, and a fixed-target Ingress proxy |

The production Gateway directory contains its compiled `dist`, package
metadata, and production `node_modules` deployed from the reviewed root lockfile
with injected workspace packages; root `proto/` contains the locked Meshtastic
schema corpus; the Web directory contains the Vite production output.
Install-time `node_modules/.bin` links are not runtime
inputs and are omitted so signed updater archives remain symlink-free. Linux
and macOS service managers are carried by Headless and Desktop because those
deployment modes execute the same Agent composition. The Windows Service Host
is kept in the Windows-only Service archive and locates the adjacent Agent.

Headless, Desktop, and Windows Service archives currently require Node.js
`^22.18.0` or `>=24.11.0` on the service account's `PATH`; the standalone CLI
does not. The
runtime smoke fails before starting the Agent when this prerequisite is not
met. Service installations must therefore expose the same Node installation to
systemd, launchd, or the Windows service account rather than relying on an
interactive shell's private `PATH`.

Desktop has two deliberately separate release forms. The updater-managed
portable archive keeps the raw Tauri executable beside the complete Headless
composition. Native packages are generated from that already validated staging
tree with the pinned Tauri 2 CLI and the Repository's 512px source icon:

| Target | Native Desktop packages |
| --- | --- |
| Darwin Apple Silicon / Intel | DMG containing the macOS application bundle |
| Linux ARM64 / x64 | DEB and AppImage |
| Windows x64 | MSI and NSIS installer |

Every native package embeds the complete portable staging tree at the Tauri
resource path `cmclient-runtime/`; it does not replace it with a GUI-only
binary. CI mounts or extracts every package and re-validates the embedded
schema-v2 Desktop build manifest, Agent, CLI, migration tool, production
Gateway dependencies, Web output, protobuf corpus, and platform service
support. The eight native files have canonical names in the artifact plan and
are covered by release checksums, SBOM, and provenance, but have
`updaterManaged: false` and never enter the Agent update manifest.

The GUI remains a Control API client. Installing a native package does not
silently create or start a privileged Agent service: an operator must register
the embedded Agent with the documented systemd, launchd, or Windows Service
manager, or provision the matching portable release first. In particular, an
AppImage mount is not a stable service installation path. All complete Desktop,
Headless, and Windows Service staging trees run the Agent/Gateway/Web runtime
smoke. Each native package also bounded-launches its installed Tauri executable.
Windows additionally starts the SCM service from the final staged service-host,
verifies that it launches the adjacent final Agent and returns a healthy CLI
status, then stops and removes the registration without removing retained
state. After supply-chain assembly, a separate native Windows job downloads the
unsigned output, expands the final Service ZIP, and repeats that same smoke
through `scripts/release-windows-service-smoke.ps1`. The gate temporarily
exposes setup-node's external `node.exe` to only the test service environment;
it rejects a Node executable inside the bundle and verifies that Gateway uses
the external Node as the final Agent's child process.

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

Native Desktop packages keep the same component/target/version stem and use
their platform suffix; NSIS is distinguished from MSI with `setup.exe`:

```text
cmclient-desktop-darwin-aarch64-2.0.0-rc.1.dmg
cmclient-desktop-linux-aarch64-2.0.0-rc.1.deb
cmclient-desktop-linux-aarch64-2.0.0-rc.1.AppImage
cmclient-desktop-windows-x86_64-2.0.0-rc.1.msi
cmclient-desktop-windows-x86_64-2.0.0-rc.1.setup.exe
```

`scripts/release-artifacts.mjs` is the executable source of truth. Its plan
schema records every portable artifact's canonical role/path list, all native
Desktop packages, and both constrained Docker OCI platform artifacts. Staging
requires exactly those roles, verifies that the
Gateway and Web inputs are production outputs, rejects symlinks and special
files, and emits a schema-v2 build manifest with a deterministic nested file
inventory. Unknown, missing, or extra roles fail before an artifact can reach
the supply-chain job.

CI uploads staged inputs under
`cmclient-build-<component>-<target>-<semver>`; those are intentionally not
published release assets. `scripts/release-supply-chain.mjs` validates each
manifest against the canonical composition before producing archives,
checksums, SBOM, provenance subjects, and, when explicitly authorized, a
signed update manifest. Every target runs each staged complete runtime
composition; the supply-chain job also extracts and starts the final Linux x64
Headless and Desktop archives, including a bounded Tauri launch, so archive
creation cannot silently change runtime layout or executable modes.
`scripts/desktop-native-bundles.mjs` separately generates
the release-only Tauri configuration, collects platform-dependent Tauri
filenames under stable names, and rejects missing, duplicate, extra, or empty
outputs.

The release workflow owns a `load-gate` job in addition to ordinary CI. Every
platform build and the Docker composition require it, so an independent manual
workflow dispatch cannot assemble, attest, or sign release inputs while the
bounded resource gate is failing or was never run for that revision.

## Docker image

Docker is a separately versioned deployment image, not an Agent-updater
component. The release workflow runs its static restriction test and real
compose smoke on native x64 and ARM64 runners before binary supply-chain
assembly can proceed. Each runner exports its tested source as a source-bound,
timestamp-normalized single-platform OCI archive:

```text
cmclient-docker-linux-x86_64-<semver>.oci.tar
cmclient-docker-linux-aarch64-<semver>.oci.tar
```

Both archives use the package version, the checked-out commit time as
`SOURCE_DATE_EPOCH`, a digest-pinned multi-architecture base image, normalized
output timestamps, and OCI version/revision labels. Canonical metadata
sidecars bind the source commit, per-platform image manifest digest, archive
checksum, size, platform, and constrained composition. The supply-chain job
requires exactly both platform artifacts. Once both are present, it also binds
the top-level release index to the same source commit, covering portable
archives and native Desktop installers as one immutable RC evidence set.
Before upload, each OCI archive is imported into the native runner and the
three-container topology smoke is repeated with image rebuilding disabled;
the runtime version endpoint must report the package version, source commit,
and derived release channel embedded in that archive.

The Docker build still resolves Debian package indexes during its native build,
so byte-for-byte layer reproducibility is not claimed. The exported archive is
instead bound to one source SHA and validated by OCI metadata, outer SHA-256,
`skopeo` import, a no-build runtime smoke, SBOM, and later provenance.

The image contains Gateway and Web application code plus the fixed Ingress
proxy; it cannot contain Agent, CLI, Desktop, Service Host, or updater
ownership, and it cannot self-update. Ordinary `dev` and pull-request runs
make the OCI archives available only as downloadable workflow artifacts; they
do not push them to a registry or enable signing. The archives and metadata
are included in the supply-chain checksum and attestation subjects, while
remaining absent from the Agent update manifest.

The compose deployment and capability restrictions are documented in
[Docker Deployment](./docker-deployment.md). Archive, SBOM, checksum,
attestation, and offline manifest-signing boundaries are documented in
[Release Supply Chain](./supply-chain.md). Clean install, upgrade, and
uninstall retention semantics are defined in
[Packaging Lifecycle Matrix](./packaging-lifecycle.md).
