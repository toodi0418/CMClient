# RC Field Validation

> Historical P12 snapshot. Where this file conflicts with
> [Documentation Authority](../READ_ORDER.md), it is implementation/evidence
> history rather than the current install or release contract.

`docs/testing/rc-field-validation-plan.json` is the executable CMClient
`2.0.0-rc.1` field-test scope. It covers every release target and deployment
mode. This document defines how an operator records results without putting
credentials, personal data, packet captures, databases, or mutable release
claims in the repository.

The plan is a requirement set, not evidence that field work has already run.
Automated CI results may satisfy cases whose validator is `machine`. Cases
marked `human` or `hardware` require the named environment and cannot be
replaced by a cross-build, unit fixture, or unchecked screenshot.

## RC identity

Create one evidence document outside the repository for one immutable RC. It
has an exact top-level schema. The required fields are `schemaVersion`,
`releaseVersion`, `sourceCommit`, `sourceTree`, `ciRunUrl`, `releaseRunUrl`,
`artifactName`, `artifactDigestSha256`, and `results`. Only
`productionIdentity` and `productionApproval` may be added for stable
promotion. A stored `summary` is never authoritative and is rejected, as are
other top-level additions such as `artifactDownload`, `releaseMetadata`, and
`targetEnvironments`.

The identity and referenced evidence store must establish:

- release version `2.0.0-rc.1`;
- full source commit and tree SHA;
- successful CI and Release Build Matrix URLs whose head SHA is the source
  commit;
- GitHub artifact name and artifact digest, plus externally recorded download
  time and expiry;
- SHA-256 of `release-index.json` and `SHA256SUMS`;
- the exact archive, installer, or OCI filename and SHA-256 used by each case;
- target OS, architecture, Node version, and a non-identifying host alias in
  the applicable result evidence, not as unvalidated top-level metadata.

The ordinary `dev` and pull-request workflow intentionally creates an unsigned
RC. A tagged production dispatch first creates the same unsigned base, then a
protected platform-signing matrix signs the exact native Desktop bytes: Apple
codesign/notarization/stapling on Darwin, Authenticode plus timestamp on
Windows, and an embedded GPG signature on Linux AppImage that is verified by
the checksum-pinned AppImageUpdate validator. Linux DEB remains a
checksum/provenance-bound package and is not described as independently signed.

Each target produces a canonical platform-signing receipt. A finalization step
overlays those signed bytes and receipts, regenerates the composition SPDX
SBOM, updates `release-index.json.platformSigning`, regenerates `SHA256SUMS`,
and must pass `verify --require-platform-signing`. Only that final artifact is
eligible for keyless Cosign/GitHub provenance; the Agent update manifest is
signed afterward from the attested artifact. These production jobs require the
protected `production-release` environment and remain unavailable until the
P12-T05 approval and real credential/evidence handoff. An unsigned RC must
never be described as signed or production-published.

## Result records

Expand every plan case into one result for every target and mode combination.
The target marker `all` expands to all five canonical release targets. A case
with two targets and two modes therefore requires four independently evidenced
results; one host transcript cannot satisfy another platform.

```json
{
  "caseId": "RC-CLI-HOST",
  "target": "windows-x86_64",
  "mode": "cli",
  "status": "pass",
  "operator": "lab-operator-01",
  "executedAt": "2026-07-19T08:00:00.000Z",
  "evidence": ["evidence://rc1/windows-cli-status"],
  "notes": "Node 22.23.1; exit codes matched the documented table"
}
```

Allowed states are:

| State | Required fields | Meaning |
| --- | --- | --- |
| `pending` | case ID, target, mode | Not yet executed. Never promotion-ready. |
| `pass` | operator, UTC timestamp, evidence reference | Every expected result was observed. |
| `fail` | pass fields plus defect ID | Executed and did not meet the expected result. |
| `blocked` | owner and unblock condition | Hardware, approved credentials, or authority is unavailable. |
| `notApplicable` | approval identity | Allowed only for a non-required conditional case. |

RC promotion requires every canonical `case x target x mode` execution for the
`rc` gate to be `pass`. Production promotion additionally requires every
`production` execution to be `pass`. The validator rejects missing, duplicate,
unknown, or reduced matrix entries. Summary prose, an overall green badge, or a
newer unrelated workflow run cannot override an individual failure, blocker,
or pending result.

Validate repository sources and the plan with:

```bash
node scripts/rc-readiness.mjs check-sources
node scripts/rc-readiness.mjs check-plan \
  --input docs/testing/rc-field-validation-plan.json
```

`check-sources` accepts the declared RC or its matching stable version only.
`check-plan` remains bound to the RC field plan when a release-only promotion
commit changes the product declarations from `2.0.0-rc.1` to `2.0.0`; another
version or release line fails closed. `check-evidence` applies the same source
version relation, so the RC evidence remains valid for the matching stable
production gate without accepting a different release line.

Validate a completed evidence file structurally:

```bash
node scripts/rc-readiness.mjs check-evidence \
  --plan docs/testing/rc-field-validation-plan.json \
  --input /approved/evidence/cmclient-2.0.0-rc.1.json
```

Promotion checks do not trust identity values from the evidence document. Bind
them explicitly to the values independently obtained from Git and the GitHub
Actions artifact API:

```bash
node scripts/rc-readiness.mjs check-evidence \
  --plan docs/testing/rc-field-validation-plan.json \
  --input /approved/evidence/cmclient-2.0.0-rc.1.json \
  --promotion-ready \
  --expected-version 2.0.0-rc.1 \
  --expected-source-commit "$SOURCE_COMMIT" \
  --expected-source-tree "$SOURCE_TREE" \
  --expected-ci-run-url "https://github.com/toodi0418/CMClient/actions/runs/$CI_RUN_ID" \
  --expected-release-run-url "https://github.com/toodi0418/CMClient/actions/runs/$RELEASE_RUN_ID" \
  --expected-artifact-name "cmclient-supply-chain-unsigned-2.0.0-rc.1" \
  --expected-artifact-digest-sha256 "$ARTIFACT_DIGEST_SHA256"
```

Only canonical run-level URLs for `toodi0418/CMClient` are accepted in the RC
identity fields. Job URLs, redirectors, query strings, and unrelated HTTPS
URLs are rejected there. Machine result evidence uses canonical job-level URLs
under one of those two exact runs, as described below.

## Stable promotion

Add `--production` only after every RC execution passes, P12-T05 is approved,
and the protected stable tag workflow completes. The approval authorizes a
reviewed release-only commit that changes all product version declarations and
release metadata to `2.0.0` without changing product behavior or dependencies.
That commit must pass CI, be promoted to `main`, and be tagged exactly
`v2.0.0`; the manually dispatched tagged Release Build Matrix must then produce
the platform-signed, re-finalized, and attested supply-chain artifact.

The evidence document must retain the RC identity and add both independently
verifiable production records:

```json
{
  "productionIdentity": {
    "releaseVersion": "2.0.0",
    "tag": "v2.0.0",
    "sourceCommit": "0123456789abcdef0123456789abcdef01234567",
    "sourceTree": "89abcdef0123456789abcdef0123456789abcdef",
    "ciRunUrl": "https://github.com/toodi0418/CMClient/actions/runs/2001",
    "releaseRunUrl": "https://github.com/toodi0418/CMClient/actions/runs/2002",
    "artifactName": "cmclient-supply-chain-attested",
    "artifactDigestSha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  },
  "productionApproval": {
    "taskId": "P12-T05",
    "identity": "release-approver",
    "approvedAt": "2026-07-20T08:00:00.000Z",
    "reference": "approval://P12-T05/2026-07-20"
  }
}
```

The production source commit/tree, successful CI and tagged release run URLs,
and artifact digest must be obtained independently from Git and the GitHub
Actions artifact API. Pass those values and the approval as separate bindings:

```bash
node scripts/rc-readiness.mjs check-evidence \
  --plan docs/testing/rc-field-validation-plan.json \
  --input /approved/evidence/cmclient-2.0.0-rc.1.json \
  --promotion-ready --production \
  --expected-version 2.0.0-rc.1 \
  --expected-source-commit "$SOURCE_COMMIT" \
  --expected-source-tree "$SOURCE_TREE" \
  --expected-ci-run-url "https://github.com/toodi0418/CMClient/actions/runs/$CI_RUN_ID" \
  --expected-release-run-url "https://github.com/toodi0418/CMClient/actions/runs/$RELEASE_RUN_ID" \
  --expected-artifact-name "cmclient-supply-chain-unsigned-2.0.0-rc.1" \
  --expected-artifact-digest-sha256 "$ARTIFACT_DIGEST_SHA256" \
  --expected-production-source-commit "$PRODUCTION_SOURCE_COMMIT" \
  --expected-production-source-tree "$PRODUCTION_SOURCE_TREE" \
  --expected-production-ci-run-url "https://github.com/toodi0418/CMClient/actions/runs/$PRODUCTION_CI_RUN_ID" \
  --expected-production-release-run-url "https://github.com/toodi0418/CMClient/actions/runs/$PRODUCTION_RELEASE_RUN_ID" \
  --expected-production-artifact-digest-sha256 "$PRODUCTION_ARTIFACT_DIGEST_SHA256" \
  --approval-identity "$APPROVER_IDENTITY" \
  --approval-at "$APPROVED_AT" \
  --approval-ref "$APPROVAL_REFERENCE"
```

The validator derives `2.0.0`, `v2.0.0`, and
`cmclient-supply-chain-attested` from the reviewed RC line and workflow
contract. A final artifact must include the complete platform-signing receipt
set and cannot reuse the RC commit, an unsigned `dev` artifact, a different
tag, or an unrelated workflow run.

## Machine evidence

Machine cases must point to the exact successful workflow jobs. Relevant
evidence includes:

- canonical artifact plan, build manifests, release index (including
  `platformSigning`), per-target signing receipts, archive extraction, native
  package inventory, OCI load, checksums, and SPDX SBOM;
- full verify, resource/load soak, Web Playwright, Control API, CLI, Desktop,
  service, and constrained Docker smoke;
- dependency signatures, advisory audit, secret scan, workflow policy, and
  least-privilege checks;
- deterministic Position/APRS replay, CallMesh mock conflict handling,
  protocol-aware multi-client Proxy pressure, updater interruption/rollback,
  and Legacy migration fixtures.

Machine coverage does not satisfy physical radio, APRS-IS, CallMesh tenant,
Raspberry Pi, native installer, or real service-account cases.

Every `pass` or `fail` result evidence entry must be an absolute `https://` or
`evidence://` URI. Credentials, query strings, fragments, raw or percent-encoded
control characters, relative references, and other URI schemes are rejected.
An `evidence://` URI is suitable for sanitized human or hardware records in the
approved evidence store. Every machine result additionally requires at least
one canonical GitHub Actions job URL of the form
`https://github.com/toodi0418/CMClient/actions/runs/<run-id>/job/<job-id>` whose
run is exactly the evidence document's `ciRunUrl` or `releaseRunUrl`; a job from
a newer or unrelated run cannot satisfy the case.

## Evidence hygiene

Never store any of the following in evidence:

- CallMesh API keys, APRS passcodes, admin or scoped tokens, session cookies,
  CSRF tokens, update private keys, or credential-store output;
- full environment dumps, request headers, `docker inspect` output, shell
  history, raw diagnostics, databases, logs, or packet captures;
- real callsigns, coordinates, Mesh node IDs, radio serials, IP/MAC addresses,
  usernames, or absolute user paths.

Secrets must enter only through the documented Agent secret input or approved
platform injection. A plaintext field backend must be outside the Repository
and pass the documented `0700` parent plus owner/regular-file/one-link/`0600`
checks. Record `{stored:true}`, stable error codes, bounded counts,
digests, and hashed lab aliases. Crop screenshots and inspect them for cookies,
headers, paths, and personal data before placing them in the approved evidence
store. Repository commits may contain the plan and sanitized documentation,
but never field evidence payloads or built release artifacts.
