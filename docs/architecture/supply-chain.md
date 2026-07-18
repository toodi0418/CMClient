# Release Supply Chain

`scripts/release-supply-chain.mjs` is the executable release boundary after
the platform matrix has staged one binary per canonical component/target pair.
It copies each binary to `bin/<binary>` with a normalized timestamp and mode,
adds the build metadata under `metadata/`, then creates a `tar.zst` or ZIP
archive with the stable artifact name. The Agent installer accepts this safe
`bin/` layout and independently rejects traversal, symlinks, special files,
digest changes, and oversized extraction.

The release workflow always builds an unsigned artifact bundle containing:

- every canonical archive;
- a Syft SPDX JSON SBOM of the checked-out source and resolved inputs;
- `SHA256SUMS` for every archive and the SBOM; and
- `release-index.json`, which records the exact archive metadata used by a
  signed update manifest.

`SHA256SUMS` uses the standard `sha256 *filename` format. The local assembler
recomputes every digest and size before accepting the output, and its fixture
tests verify checksum tampering and Ed25519 canonical payload signing.

## Attestation

Normal `dev` and pull-request runs keep `contents: read` and do not request an
OIDC token. A maintainer must manually dispatch `Release Build Matrix` with
`attest=true` to enable the separate attestation job. That job has only
`contents: read`, `id-token: write`, and `attestations: write`; it creates a
keyless Cosign bundle for `SHA256SUMS` and a GitHub SLSA provenance attestation
whose subjects are the checksummed archives and SBOM. The workflow pins the
SBOM, Cosign installer, and provenance actions to reviewed commit hashes.

## Agent Update Manifest

The optional `sign-update-manifest` job is also manual. It requires an
immutable HTTPS release directory, repository variable
`CMCLIENT_UPDATE_SIGNING_KEY_ID`, and secret `CMCLIENT_UPDATE_SIGNING_KEY`.
The secret is a base64-encoded PKCS#8 Ed25519 private key. It is passed only as
an environment value to the signing process; it is never an action input,
command argument, artifact, log field, application setting, or runtime secret.

The script reconstructs the Rust Agent's compact JSON field order before
calling Ed25519 and emits unpadded standard Base64. The Agent already treats
the configured public key ID as authoritative, so a manifest key ID cannot
select an arbitrary verifier. Docker images are intentionally absent from this
manifest because they are operator-upgraded, not Agent-updated.
