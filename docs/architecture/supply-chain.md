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

The release workflow always builds an unsigned artifact bundle containing:

- every canonical archive;
- a Syft SPDX JSON SBOM of the staged canonical `release-build` compositions;
- `SHA256SUMS` for every archive and the SBOM; and
- `release-index.json`, which records the exact archive metadata used by a
  signed update manifest.

`SHA256SUMS` uses the standard `sha256 *filename` format. The local assembler
recomputes every digest and size before accepting the output, and its fixture
tests verify checksum tampering and Ed25519 canonical payload signing.

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
`production-release` environment, to enable the separate attestation job. That
job has only `contents: read`, `id-token: write`, and `attestations: write`; it
creates a keyless Cosign bundle for `SHA256SUMS` and a GitHub SLSA provenance
attestation whose subjects are the checksummed archives and SBOM. Every
third-party workflow action is pinned to a reviewed complete commit SHA, and
checkout credentials are not persisted.

## Agent Update Manifest

The optional `sign-update-manifest` job is also manual. It requires an
exact version tag, a completed attestation, the protected
`production-release` environment, an immutable HTTPS release directory,
repository variable
`CMCLIENT_UPDATE_SIGNING_KEY_ID`, and secret `CMCLIENT_UPDATE_SIGNING_KEY`.
The secret is a base64-encoded PKCS#8 Ed25519 private key. It is passed only as
an environment value to the signing process; it is never an action input,
command argument, artifact, log field, application setting, or runtime secret.
The signing job downloads only the attested supply-chain artifact, checks out
the exact workflow commit, checks that the tag is `v<package-version>`, verifies
the Cosign bundle identity, and re-verifies every archive, SBOM, checksum,
digest, and size against that commit's canonical matrix before exposing the
key. It fails closed when either signing value is absent.

The script reconstructs the Rust Agent's compact JSON field order before
calling Ed25519 and emits unpadded standard Base64. The Agent already treats
the configured public key ID as authoritative, so a manifest key ID cannot
select an arbitrary verifier. Docker images are intentionally absent from this
manifest because they are operator-upgraded, not Agent-updated.
