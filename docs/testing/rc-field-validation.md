# RC Field Validation

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
must contain:

- release version `2.0.0-rc.1`;
- full source commit and tree SHA;
- successful CI and Release Build Matrix URLs whose head SHA is the source
  commit;
- GitHub artifact name, artifact digest, download time, and expiry;
- SHA-256 of `release-index.json` and `SHA256SUMS`;
- the exact archive, installer, or OCI filename and SHA-256 used by each case;
- target OS, architecture, Node version, and a non-identifying host alias.

The ordinary `dev` workflow intentionally creates an unsigned RC. Cosign,
GitHub provenance, and the signed Agent update manifest require an exact
version tag plus the protected `production-release` environment. Those are
the `production` cases in the plan and remain unavailable until the P12-T05
human approval gate. An unsigned RC must never be described as signed or
production-published.

Native Desktop packages are also unsigned RC inputs. Platform code signing,
macOS notarization, Windows Authenticode verification, and applicable Linux
package/AppImage signatures are a separate required production case. Checksum
Cosign and GitHub provenance do not substitute for those platform trust chains.

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

Only canonical run-level URLs for `toodi0418/CMClient` are accepted. Job URLs,
redirectors, query strings, and unrelated HTTPS URLs are rejected.

Add `--production` only after P12-T05 approval and the protected tag workflow.
The evidence document must then contain the exact approval record:

```json
{
  "productionApproval": {
    "taskId": "P12-T05",
    "identity": "release-approver",
    "approvedAt": "2026-07-20T08:00:00.000Z",
    "reference": "approval://P12-T05/2026-07-20"
  }
}
```

Pass the independently obtained approval values as a second binding:

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
  --approval-identity "$APPROVER_IDENTITY" \
  --approval-at "$APPROVED_AT" \
  --approval-ref "$APPROVAL_REFERENCE"
```

## Machine evidence

Machine cases must point to the exact successful workflow jobs. Relevant
evidence includes:

- canonical artifact plan, build manifests, release index, archive extraction,
  native package inventory, OCI load, checksums, and SPDX SBOM;
- full verify, resource/load soak, Web Playwright, Control API, CLI, Desktop,
  service, and constrained Docker smoke;
- dependency signatures, advisory audit, secret scan, workflow policy, and
  least-privilege checks;
- deterministic Position/APRS replay, CallMesh mock conflict handling,
  protocol-aware multi-client Proxy pressure, updater interruption/rollback,
  and Legacy migration fixtures.

Machine coverage does not satisfy physical radio, APRS-IS, CallMesh tenant,
Raspberry Pi, native installer, or real service-account cases.

## Evidence hygiene

Never store any of the following in evidence:

- CallMesh API keys, APRS passcodes, admin or scoped tokens, session cookies,
  CSRF tokens, update private keys, or credential-store output;
- full environment dumps, request headers, `docker inspect` output, shell
  history, raw diagnostics, databases, logs, or packet captures;
- real callsigns, coordinates, Mesh node IDs, radio serials, IP/MAC addresses,
  usernames, or absolute user paths.

Secrets must enter only through the documented Agent secret input or approved
platform injection. Record `{stored:true}`, stable error codes, bounded counts,
digests, and hashed lab aliases. Crop screenshots and inspect them for cookies,
headers, paths, and personal data before placing them in the approved evidence
store. Repository commits may contain the plan and sanitized documentation,
but never field evidence payloads or built release artifacts.
