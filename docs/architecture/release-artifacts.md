# Release Build Matrix and Artifact Names

CMClient 2.0 has one canonical release matrix shared by build, checksum,
signature, provenance, and updater-manifest generation. It deliberately uses
the same target identifiers accepted by the signed update contract:

| Target | CI runner | Rust target |
| --- | --- | --- |
| `darwin-aarch64` | `macos-14` | `aarch64-apple-darwin` |
| `darwin-x86_64` | `macos-13` | `x86_64-apple-darwin` |
| `linux-aarch64` | `ubuntu-24.04-arm` | `aarch64-unknown-linux-gnu` |
| `linux-x86_64` | `ubuntu-latest` | `x86_64-unknown-linux-gnu` |
| `windows-x86_64` | `windows-latest` | `x86_64-pc-windows-msvc` |

Every target builds `desktop`, `headless`, and `cli`; Windows additionally
builds the `service` host. The Desktop executable is a build input, not a
substitute for a platform installer; service installers and Docker are
independent deployment modes.

## Stable names

The final asset name is always:

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

`scripts/release-artifacts.mjs` is the executable source of truth. It rejects
unknown components, unknown targets, and non-SemVer versions, and records the
future asset name in each CI build manifest. CI uploads build inputs under
`cmclient-build-<component>-<target>-<semver>`; those are intentionally not
published release assets. `scripts/release-supply-chain.mjs` turns exactly this
matrix into archives, checksums, SBOM, provenance subjects, and (when an
offline key is explicitly supplied) a signed update manifest.

The workflow has `contents: read` only. Build verification on `dev` and pull
requests cannot create releases, publish containers, or upload assets.

## Docker image

Docker is a separately versioned deployment image rather than an updater
component: it contains Gateway and Web only, and cannot self-update. A future
release workflow will publish immutable image tags and digests after the same
SBOM, checksum, signature, and provenance gates that protect binary assets.
The compose deployment and its runtime capability restrictions are documented
in [Docker Deployment](./docker-deployment.md).

The archive, SBOM, checksum, keyless provenance, and offline manifest-signing
boundaries are documented in [Release Supply Chain](./supply-chain.md).

Clean install, upgrade, and uninstall retention semantics are defined in
[Packaging Lifecycle Matrix](./packaging-lifecycle.md).
