# Release Supply Chain

`scripts/release-supply-chain.mjs` is the executable release boundary after
the platform matrix has staged a complete canonical composition for every
component/target pair. It verifies the schema-v2 role/path list against
`scripts/release-artifacts.mjs`, rejects undeclared files, symlinks, and special
files against the staged file inventory, normalizes modes and timestamps, adds
build metadata under `metadata/`, then creates a `tar.zst` or ZIP archive with
the stable artifact name. The
result therefore preserves the full Desktop, Headless, CLI, or Service layout
instead of reducing every product surface to one executable. Deployable
layouts include `cmclient-migrate` and the locked `proto/meshtastic` corpus;
the standalone CLI archive remains CLI-only. The Agent
installer independently rejects traversal, symlinks, special files, digest
changes, and oversized extraction.

Every pull request, `dev` run, and the unsigned first stage of a tagged release
builds the same unsigned artifact bundle containing:

- every canonical archive;
- all checksum-covered native Desktop installers and application packages,
  which are excluded from the Agent updater manifest;
- constrained Linux x64 and ARM64 Docker OCI archives and their source/digest
  metadata;
- Syft SPDX JSON SBOMs of the staged canonical `release-build` compositions
  and the exported OCI images;
- `SHA256SUMS` for every archive, OCI image, metadata sidecar, and SBOM; and
- `release-index.json`, which records the exact archive metadata used by a
  signed update manifest plus the separately managed Docker image identity and
  the source commit shared by the complete RC artifact set.

`SHA256SUMS` uses the standard `sha256 *filename` format. The local assembler
recomputes every digest and size before accepting the output, and its fixture
tests verify checksum tampering and Ed25519 canonical payload signing. Docker
verification additionally reads the OCI index, manifest, and config blobs,
recomputes their descriptor digests, and verifies the Linux/amd64 or
Linux/arm64 platform, package version, and checked-out source SHA labels after
the workflow artifacts have been downloaded into the supply-chain job. Both
platforms are mandatory and duplicate, missing, or additional Docker inputs
fail closed. Docker inclusion is also the final aggregation boundary: it writes
the checked-out source SHA into `release-index.json`, and later verification
requires that top-level identity, both OCI configs, and the workflow checkout
all name the same commit.

Native Desktop jobs first stage the same complete portable composition used by
the updater archive, then map that directory to the Tauri resource name
`cmclient-runtime/`. Platform-native smoke mounts the DMG, extracts both Linux
package formats, and administratively extracts MSI/NSIS without installing a
service. Each extracted resource tree is checked against the canonical Desktop
composition and its packaged Tauri executable must remain running through a
bounded native launch. The final Linux x64 portable Desktop archive separately
starts Agent, Gateway, Web, CLI, and Tauri after supply-chain assembly. These
packages are unsigned during ordinary `dev` and pull-request CI. A production
dispatch does not treat that unsigned bundle as final; it runs the protected
platform-signing and re-finalization boundary described below.

## Protected platform signing

Platform signing is a production-only matrix. It can run only from an exact
`v<package-version>` tag with `workflow_dispatch` and `attest=true` through the
protected `production-release` environment. The matrix downloads the exact
prebuilt Desktop staging tree and signs only the native package bytes for its
target:

- Darwin signs the application with Apple codesign, submits the DMG for
  notarization, staples the notarization ticket, and verifies codesign,
  Gatekeeper assessment, and staple validation.
- Windows signs both MSI and NSIS outputs with Authenticode and a trusted
  timestamp, then verifies the signer thumbprint and timestamp certificate.
- Linux embeds a GPG signature in each AppImage and verifies it with the
  checksum-pinned AppImageUpdate `validate` tool. The DEB is deliberately not
  treated as a separately signed package; its exact bytes are bound by the
  checksum and provenance chain.

Each target writes one canonical
`cmclient-platform-signing-<target>-<version>.json` receipt. A receipt binds
the schema, target, version, source SHA, non-secret identity reference, exact
package filename, byte size, SHA-256, and platform trust type. It contains no
private key, password, token, or host-specific path.

The `finalize-platform-signed` job downloads the unsigned supply-chain bundle,
all five target outputs, and the exact build stages. It overlays the signed
native bytes and receipts, regenerates the SPDX SBOM from that signed
composition, updates `release-index.json.platformSigning`, regenerates
`SHA256SUMS` (including the receipts), and runs
`verify --require-platform-signing`. The resulting
`cmclient-supply-chain-platform-signed-*` artifact is the only input accepted
by attestation. Keyless Cosign and GitHub provenance run next; the optional
Agent update-manifest signer runs only after that attested artifact has passed
the same platform-signing verification.

The workflow defines the protected secret and variable boundary, but ordinary
CI never receives those values. Actual Apple, Windows, and Linux identities,
notarization accounts, and release approval remain field/handoff prerequisites
and are not evidence that production signing has already executed.

The final Windows Service ZIP has an additional post-assembly gate on a native
Windows runner. The job downloads the exact unsigned supply-chain artifact,
expands the ZIP, registers and starts its Service Host through SCM, confirms the
adjacent Agent and CLI status, starts Gateway, and checks its version, source
commit, channel, and health projections. setup-node supplies the documented
external Node runtime through a service-scoped temporary environment value;
`node.exe` is neither copied into nor accepted from the archive. Checksum
attestation cannot start until this final archive gate succeeds.

The release matrix cannot build deployable inputs until both the resource load
gate and the independent security gate pass. The security gate checks package
signatures and dependency advisories, scans the tree for secrets, and enforces
the workflow/dependency policy documented in the
[P12 Release Security Audit](../security/release-audit.md). Security tools are
installed at exact versions with reviewed SHA-256 digests. SBOM generation
uses the same model: `scripts/install-sbom-tool.sh` downloads Syft 1.42.3 from
its immutable release archive, verifies the reviewed archive digest, and only
then scans `release-build`; no mutable installer script is executed.

Gateway production dependencies are deployed with injected workspace packages
and `--frozen-lockfile`. Both the portable release composition and Docker image
therefore consume the reviewed root `pnpm-lock.yaml`; pnpm reports zero newly
resolved packages during deployment. Repository policy rejects the legacy
deploy resolver or a production deploy command that omits the frozen lock.

## Attestation

Normal `dev` and pull-request runs keep `contents: read` and do not request an
OIDC token. A maintainer must manually dispatch `Release Build Matrix` from an
exact `v<package-version>` tag with `attest=true`, through the protected
`production-release` environment. Platform signing and re-finalization must
complete first. The attestation job then has only `contents: read`,
`id-token: write`, and `attestations: write`; it creates a keyless Cosign bundle
for the finalized `SHA256SUMS` and a GitHub SLSA provenance attestation whose
subjects are every checksummed archive, native installer, receipt, OCI image,
and SBOM. Every third-party workflow action is pinned to a reviewed complete
commit SHA, and checkout credentials are not persisted.

Stable promotion does not rename an RC artifact. After RC field validation and
P12-T05 approval, one reviewed release-only commit changes the synchronized
product declarations from `2.0.0-rc.1` to `2.0.0` and must pass CI before it is
promoted to `main`. The immutable `v2.0.0` tag points to that exact commit. RC
evidence keeps its original source binding, while `productionIdentity` binds
the stable commit/tree, same-commit CI, tagged Release Build Matrix, exact tag,
and `cmclient-supply-chain-attested` artifact digest independently.

## Agent Update Manifest

The optional `sign-update-manifest` job is also manual. It requires an
exact version tag, a completed attestation, the protected
`production-release` environment, an immutable HTTPS release directory,
repository variable
`CMCLIENT_UPDATE_SIGNING_KEY_ID`, and secret `CMCLIENT_UPDATE_SIGNING_KEY`.
The secret is a base64-encoded PKCS#8 Ed25519 private key. It is passed only as
an environment value to the signing process; it is never an action input,
command argument, artifact, log field, application setting, or runtime secret.
The signing job downloads only the attested, platform-signed supply-chain
artifact, checks out the exact workflow commit, checks that the tag is
`v<package-version>`, verifies the Cosign bundle identity, requires
`index.platformSigning`, and re-verifies every archive, native package, receipt,
SBOM, checksum, digest, size, and Docker source SHA against that commit's
canonical matrix before exposing the key. It fails closed when either signing
value or the platform-signing metadata is absent.

The script reconstructs the Rust Agent's compact JSON field order before
calling Ed25519 and emits unpadded standard Base64. The Agent already treats
the configured public key ID as authoritative, so a manifest key ID cannot
select an arbitrary verifier. Docker images are intentionally absent from this
manifest because they are operator-upgraded, not Agent-updated.
