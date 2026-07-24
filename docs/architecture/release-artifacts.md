# Unified Release Objects

This is the target release contract. The current P12 artifact scripts and RC
catalog remain historical implementation evidence until P15/P16 replace them.

## Public Install Set

| Target | Public object | Architecture |
| --- | --- | --- |
| Windows | `CMClient-Setup.exe` | x86-64 only |
| macOS | `CMClient.dmg` | Universal Intel + Apple Silicon |
| Linux | `CMClient-x86_64.AppImage` | x86-64 |
| Linux | `CMClient-aarch64.AppImage` | ARM64 |
| Docker | one OCI image index plus Compose | linux/amd64 + linux/arm64 |

Update payloads, detached signatures, checksums, release indexes, SBOMs,
licenses, and provenance are support files, not additional user install choices.
There are no public Desktop, Headless, CLI, Service, MSI, DEB, or portable
component downloads.

## Self-contained Composition

Every native package contains the launcher modes, Agent, Gateway production
tree, compiled Web, pinned private Node LTS runtime, target-native addons,
updater/watchdog, schemas, migrations, licenses, terms, and integration assets.
No package needs system Node, npm, or pnpm. All components report one exact
product/version/source/channel/target identity.

Windows uses one current-user NSIS Setup with offline WebView2 and no routine
UAC. macOS uses one drag-to-Applications Universal DMG with user-level terminal
integration. Linux uses one AppImage per CPU and documents
`--appimage-extract-and-run` as the operator fallback when FUSE cannot mount.

## Private Node Staging Input

`packaging/runtime/private-node-runtime.json` is the canonical native-package
input manifest. It exact-pins Node `24.18.0`, the official Node.js
`SHASUMS256.txt` source, and the official Windows x86-64, macOS x86-64/ARM64,
and Linux x86-64/ARM64 archives. Windows ARM64 is explicitly unsupported.
Docker builds their runtime into each target image and do not consume these
native archives.

The staging tool never downloads an archive. An operator supplies the official
archive below the persisted campaign root, then runs:

```powershell
node scripts/private-node-runtime.mjs stage-windows `
  --archive <campaign>\inputs\node-v24.18.0-win-x64.zip `
  --campaign-root <campaign> `
  --stage-root <campaign>\package
```

The tool streams the path-backed ZIP through exact size/SHA-256, bounded-entry,
path-collision, traversal, link/reparse, and extracted-inventory checks. It
stages atomically only after `node.exe --version` reports `v24.18.0` and an
in-memory `node:sqlite` smoke succeeds. The native package paths are
`runtime/node/node.exe` on Windows and `runtime/node/bin/node` on Unix. P13
qualifies this current-host Windows staging contract; P15 still owns real
target-native archive and addon qualification for every release target.

## Candidate Identity

- `runtimeCandidate` identifies the exact source/tree and executable/image that
  actually ran qualification.
- `distributionCandidate` identifies exact candidate package/image bytes.
- production signing/notarization creates a new production distribution
  candidate; exact-byte package checks, checksums, SBOM, provenance, and update
  metadata must run again.

A local executable never inherits the identity of an undownloaded CI artifact.
GitHub artifact payloads are not downloaded or reconstructed in the local Goal.

## Release Boundary

Candidate workflows may use unsigned, ad-hoc, or campaign test-signed bytes and
must label them honestly. `main`, tags, production credentials, Developer ID,
Authenticode, notarization, registry/publication, and formal release require a
new explicit human approval. Repacking after identity finalization invalidates
the candidate evidence.
