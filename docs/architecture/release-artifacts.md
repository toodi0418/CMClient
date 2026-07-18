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

Every target builds `desktop`, `headless`, and `cli`. The Desktop executable is
a build input, not a substitute for a platform installer; service installers
and Docker are independent deployment modes.

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
```

`scripts/release-artifacts.mjs` is the executable source of truth. It rejects
unknown components, unknown targets, and non-SemVer versions, and records the
future asset name in each CI build manifest. CI uploads build inputs under
`cmclient-build-<component>-<target>-<semver>`; those are intentionally not
published release assets. P10-T05 will create the archives, checksums, SBOM,
signatures, provenance, and signed update manifest from this exact matrix.

The workflow has `contents: read` only. Build verification on `dev` and pull
requests cannot create releases, publish containers, or upload assets.
