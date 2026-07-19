# P12 Release Security Audit

This record captures the reproducible P12-T02 security baseline evaluated on
2026-07-19. It covers the repository dependency graph, tracked content,
release workflows, LAN authentication boundary, and the GitHub controls that
must be enabled before CMClient 2.0 is published. A later release candidate
must rerun these commands against the exact tag; this record is not a permanent
waiver for future dependency or workflow changes.

## Reproduction

Run the repository policy and dependency gates from a clean checkout with the
locked toolchains:

```bash
pnpm install --frozen-lockfile
pnpm audit:policy
pnpm audit --prod --audit-level high
pnpm audit --audit-level high
pnpm audit --dev --audit-level high
pnpm audit signatures

tools_dir="$(mktemp -d)"
trap 'rm -rf "$tools_dir"' EXIT
bash scripts/install-security-audit-tools.sh "$tools_dir"
"$tools_dir/actionlint" .github/workflows/*.yml
"$tools_dir/gitleaks" dir --redact --no-banner --no-color .
"$tools_dir/cargo-audit" audit --deny unsound --ignore RUSTSEC-2024-0429

sbom_tools_dir="$(mktemp -d)"
bash scripts/install-sbom-tool.sh "$sbom_tools_dir"
```

`scripts/install-security-audit-tools.sh` installs exact, checksum-verified
Gitleaks and cargo-audit binaries. The cargo-audit Linux asset is the upstream
`x86_64-unknown-linux-musl` static PIE, with release SHA-256
`7fb9497f8594b389e5fce5ef9b92db08432996895b2e0c5a0167a69ed445c428`;
it has no glibc runtime dependency and therefore runs on the pinned Ubuntu 22.04
security runners. `scripts/install-sbom-tool.sh` applies the same control to
Syft and avoids its upstream action's mutable installer path.
CI executes the same policy, secret, Node, and Rust gates; the release build
matrix cannot start without its independent `security-gate` succeeding.
The same policy requires injected workspace packages and frozen-lockfile pnpm
deploys for both portable Gateway compositions and the Docker image. It rejects
the legacy resolver, which can ignore the audited workspace lockfile and resolve
a new production graph on a clean runner.

## Dependency Results

| Audit | Result |
| --- | --- |
| pnpm production dependencies | 0 vulnerabilities |
| pnpm complete graph | 0 vulnerabilities |
| pnpm development dependencies | 0 vulnerabilities |
| pnpm package signatures | 367 valid, 0 invalid (367/367) |
| Actionlint 1.7.12 | 0 workflow findings |
| Gitleaks 8.30.1 | 0 findings |
| cargo-audit 0.22.2 | 588 dependencies; 0 vulnerabilities; 0 yanked; 1 unsound warning; 16 unmaintained warnings |

The Rust result used advisory database commit `b5fc89b`, dated 2026-07-17.
The Node 22 type graph overrides `undici-types` to same-major version `6.23.0`,
which carries registry provenance; pnpm rejects the upstream `6.21.0` trust
downgrade, and the repository policy rejects every other override.
The release and Docker Gateway deployments resolve zero new packages: their
production `node_modules` are derived from the same 367-entry lockfile verified
by the policy and signature gates.
Unmaintained warnings are inventory signals rather than a blanket acceptance:
direct dependencies must be replaced when an actively maintained supported API
exists, and transitive entries are reevaluated on every lockfile or framework
upgrade. The direct `rustls-pemfile` dependency and its API use were removed;
TLS PEM loading now uses the maintained `rustls::pki_types` API.

### Exact RustSec Exception

The only allowed unsoundness exception is `RUSTSEC-2024-0429` for `glib`
`0.18.5`. It enters only through Tauri's Linux GTK/WebKit dependency graph.
CMClient source does not import `glib` or call the affected `VariantStrIter`
API. This narrows exposure but does not prove the transitive code unreachable,
so the exception is deliberately time bounded.

- Advisory: `RUSTSEC-2024-0429`
- Exact package: `glib` `0.18.5`
- Allowed through: 2026-10-19, inclusive
- Owner: CMClient release maintainer
- Required exit: upgrade Tauri/GTK to a dependency graph without the advisory,
  then rerun the full Rust tests, Linux Desktop build, Desktop smoke, and
  cargo-audit without the ignore

The exception is invalid immediately if the package version changes, a second
unsound advisory appears, CMClient directly uses `glib` or `VariantStrIter`, or
the release date is after 2026-10-19. CI must continue to use
`--deny unsound --ignore RUSTSEC-2024-0429`, so every other unsound advisory
fails closed. P12-T05 must not approve a release after expiry without a new,
explicitly reviewed decision and evidence.

`security/rustsec-waivers.json` is the machine-readable form of this exception.
The policy gate checks its exact advisory/package/version, UTC expiry, Cargo.lock
entry, and the structured Cargo dependency graph. Only Desktop may reach this
`glib`, and every such path must traverse the exact `tauri` package; any direct
or alternate path fails the gate.

## Secret And Workflow Results

The tracked-tree policy scan and Gitleaks scan found no committed credential,
private-key, token, database, log, environment file, or generated release
archive in the audited tree. GitHub secret scanning and push protection were
enabled at the time of inspection, with zero open secret-scanning alerts.
Runtime credentials remain outside Git, command arguments, diagnostics, and
release artifacts.

All repository GitHub Actions `uses` references are pinned to complete commit
SHAs, and every checkout disables persisted credentials. The policy gate rejects
mutable action tags, excessive top-level write permissions, direct secret
interpolation into shell commands, unsafe dependency sources, unexpected
package lifecycle scripts, and forbidden tracked artifacts. Release OIDC write
permissions exist only in the attestation job.

The production release path is an ordered, tag-only chain in the protected
`production-release` environment: platform signing, re-finalization, keyless
checksum/provenance attestation, and only then Agent update-manifest signing.
Platform signing receives the Apple certificate/API-key material, Windows PFX
and password material, or Linux GPG key/passphrase only in the target-specific
step environment; the update signer receives its Ed25519 key only after it has
downloaded the attested artifact and verified the Cosign bundle. No signing
value is an action input, command argument, artifact, log field, application
setting, or runtime secret. The finalizer records one canonical receipt per
target, overlays the signed bytes, regenerates the SBOM, `release-index.json`,
and `SHA256SUMS`, and requires
`verify --require-platform-signing` before attestation can consume the output.
The Linux trust check installs the official AppImageUpdate validator from one
fixed release and verifies architecture-specific SHA-256 before executing it;
the installer source itself is also digest-locked by the repository policy.
The workflow contract does not prove that those external identities or
notarization accounts are configured; P12-T05 field evidence must establish
that separately.

The unsigned RC SBOM is generated from the staged canonical `release-build`
compositions, not merely from the source checkout. For a production artifact,
the finalizer regenerates the SBOM after signed native bytes and receipts are
overlaid. Checksums, staged-file inventories, archive verification, keyless
provenance, and final manifest verification are therefore tied to the exact
bytes that were accepted for release.

## LAN Authentication Hardening

The P12 review closed resource-exhaustion and parameter-abuse paths at the LAN
login boundary:

- the per-source attempt reservation occurs before password verification,
  preventing concurrent requests from bypassing the attempt budget;
- at most two Argon2 verifications may run concurrently in the active LAN
  access controller;
- failure-source windows are pruned and capped at 4,096 entries, while live
  management sessions are pruned and capped at 1,024 entries; and
- accepted PHC strings must be Argon2id version 19 with exactly the `m`, `t`,
  and `p` parameters, memory from 19,456 to 65,536 KiB, iterations from 2 to 6,
  lanes from 1 to 4, a decodable salt of at least 8 bytes, and a decoded hash
  output from 16 to 64 bytes.

Origin, CSRF, expiry, secure-cookie, and bounded code-only audit behavior remain
unchanged. Capacity exhaustion fails closed with the stable management login
rate-limit error and does not start additional Argon2 work.

## GitHub Controls And P12-T05 Gate

The following snapshot describes GitHub-hosted settings observed on
2026-07-19. Repository files cannot enforce these controls by themselves.

| Control | Audited state | Required before publication |
| --- | --- | --- |
| Secret scanning and push protection | Enabled; 0 open alerts | Keep enabled and confirm 0 open alerts on the release tag |
| Dependabot alerts and security updates | Disabled | Enable both and triage all alerts |
| Code scanning | No analysis configured | Enable the approved scanner and require a successful analysis of the release commit |
| `main` and `dev` branch protection | No protection rules | Protect both; require current CI and release security checks, review, and non-force-push history |
| GitHub environments | None configured | Create `production-release` with required human reviewers and restrict deployment to release tags |
| `v*` release tag protection | No protection rule | Prevent update and deletion of release tags, and record immutable-tag evidence before attestation |
| Actions policy | All actions allowed; SHA pinning not required by GitHub settings | Restrict allowed actions and require full-length SHA pinning in addition to the repository policy test |
| Legacy workflows on `main` | Legacy release and Docker workflows remain active | Disable or replace them before merging/tagging so they cannot publish a parallel Legacy release |

P12-T05 is the human approval boundary for every external change above and for
merging to `main`, creating the `v2.0.0` tag, exposing signing credentials, and
publishing GitHub Release or container assets. Work on `dev` must not change
`main`, create a release tag, disable a production workflow, or publish an
artifact before that approval. The approver must record the final setting
evidence, successful required checks for the exact commit, advisory recheck,
artifact/checksum/SBOM/provenance verification, and the decision to accept or
reject the time-bounded RustSec exception.
