# Task State Recovery And Goal Completion

Mutable task, candidate, evidence, and campaign ledgers live in the surrounding
workspace, outside this Git Repository. The Repository-owned implementations in
`scripts/` are the source of truth; workspace commands are thin wrappers that
delegate to them. Every product task is still bound to one pushed `dev` commit.

## State Ownership And Canonical Graph

`state/TASKS.json` is the mutable execution ledger. The committed
`scripts/unified-task-graph-lock.json` is the canonical `unified-product@1`
definition used by the completion checker. It locks graph metadata, the
completed-history digest, original active-task fields and dependencies, and
supersession mappings. Only a completed, candidate-reset repair and the matching
dependency appended to its parent may extend that graph.

Do not duplicate workflow logic in the workspace wrappers or treat an edited
task ledger as a new graph definition. Repository tests exercise the same
commands used by the wrappers.

## State Invariants

- Task IDs are unique and the dependency graph is acyclic.
- At most one task is `in_progress`.
- A task can enter `in_progress` only after every dependency is `done`. A normal
  task start requires a clean `dev` worktree and records that exact HEAD as its
  `checkpointBaseCommit`.
- `done` and `skipped` are terminal. A skipped required historical task must be
  listed in `activeGraph.supersededTaskIds` and name existing replacements.
- Optional/manual tasks may remain pending at autonomous Goal completion;
  required active implementation tasks may not.
- `NO_READY_TASK` describes scheduling state only and is never completion.

State writes use an exclusive lock, a same-directory temporary file, flush, and
atomic replacement. A transition validates the whole graph before publishing
the new JSON.

## Checkpoint Reconciliation

The canonical commit subject is:

```text
<type>(<scope>): [Pxx-Tyy] <summary>
```

The body includes exact `Task: Pxx-Tyy` and `Validation: passed` fields. Before
commit, `checkpoint.sh` requires `checkpointBaseCommit` to equal the unchanged
pre-commit HEAD. It snapshots the dirty, staged, deleted, and untracked path set
before verification, stages only that set, and rejects any new changed path
introduced by verification. Reconciliation also requires the checkpoint's sole
parent to equal that recorded base; a hand-authored checkpoint after an
intervening ordinary commit cannot repair state.

The reconciler accepts only one exact matching commit reachable from
`origin/dev`. If push succeeded before state was written, a clean retry records
that SHA and committer time as `done`; it does not create another commit. If one
matching local commit is directly ahead of `origin/dev`, `--push-local` pushes
that same SHA and then reconciles it.

With `CMCLIENT_AUTO_PUSH=0`, checkpoint creates the local commit, leaves the task
`in_progress`, and reports `exit 20`. That is a deliberate local-ahead state:
the next clean retry must push and reconcile the same SHA, never create another
commit. A failed automatic push records a recoverable blocked task. Dirty
recovery, duplicate IDs, SHA mismatch, non-fast-forward divergence, wrong
branch, and any `main` operation fail closed. Force push is never recovery.
Git and Git Credential Manager prompts are disabled for these unattended paths;
missing credentials fail instead of opening an interactive window.

## Repair Task Protocol

Repair allocation requires a clean `dev` worktree, the parent's recorded
`checkpointBaseCommit`, a title, and at least one `--affected-case`. Never stash,
reset, or mix uncommitted parent changes into a repair. If the parent worktree is
dirty, resolve ownership and return to its recorded clean baseline before using
the allocator. Affected cases must be IDs from the completion evidence contract
below; invented case names fail before state changes.

When an in-progress parent exposes a product defect:

1. Allocate the next unused checkpoint-compatible ID in the active phase.
2. Move the parent to `blocked` and start the required repair with `repairOf`,
   `candidateReset: true`, and only already-done parent predecessors.
3. Record both runtime and distribution candidate invalidation plus the exact
   affected case IDs.
4. Reproduce, add a failing regression, fix, run affected/adjacent/full
   verification and secret scan, then checkpoint and push the repair.
5. Resume with positional parent and repair IDs. This appends the done repair to
   the parent dependencies and advances the parent's `checkpointBaseCommit` to
   the pushed repair commit.
6. Rerun every affected and final case against the new candidate, then resolve
   the invalidation with that exact candidate identity.

The repair never depends on its blocked parent, so the graph remains acyclic and
the scheduler can select it.

When an independently audited implementation sequence has already been pushed
as ordinary, task-scoped commits, the task ledger may record a reconcile-audit
advance from its previous base to the verified sequence tip before creating the
single structured governance checkpoint. The audit note must name both full
object IDs and the checkpoint must use that tip as its sole parent; this keeps
the immutable repair history intact without rewriting implementation commits.

## Completion Inputs

These are the P13 checker input contracts for later P17 closeout. Their presence
in the checker does not claim that package, live-service, or soak evidence has
already been produced.

`state/CANDIDATE.json` uses `cmclient-unified-candidate/v1`. It binds one
Windows x86-64 runtime subject and one distribution candidate to the same exact
source commit and tree. The distribution contains exactly these public subjects:

```text
windows-x86_64-setup
macos-universal-dmg
linux-x86_64-appimage
linux-aarch64-appimage
docker-compose
cmclient-oci-index
cmclient-oci-amd64
cmclient-oci-arm64
```

Checksums, SBOM, provenance, and the native update manifest are exact support
subjects. Files use SHA-256 plus positive byte size; OCI subjects use exact
digests. The candidate ledger itself is also digest-bound by the evidence.

`state/EVIDENCE.json` uses `cmclient-unified-evidence/v1`. It contains exactly
one passing, sanitized, source/tree/candidate/campaign-bound record for each of:

```text
FULL_VERIFY
SECRET_SCAN
SUPPLY_CHAIN
TESTABILITY_GATES
PACKAGE_MATRIX
DOCKER_MATRIX
LIVE_DATA
CLIENTS
RECOVERY
LIVE_SOAK_24H
CLEANUP
DEFERRALS
```

Each record references a retained relative evidence file and its exact SHA-256.
`invalidationReruns` has exactly one entry per candidate invalidation and exact
evidence references for every affected case, executed after invalidation and the
final candidate freeze. `LIVE_SOAK_24H` must be continuous for at least 86,400
seconds and cover the required health/resource/database/log/upstream,
deduplication, APRS ordering, orphan, and recovery-budget checks.

Foreign candidate subjects may use retained GitHub Actions API metadata instead
of downloaded package bytes. Such metadata must point to one
`toodi0418/CMClient` run/job, match exact digest and byte size, and remain
unexpired when the checker runs.

Cleanup proves processes and listeners closed, the raw campaign removed, and
the Repository clean; the campaign ledger must also be closed with no remaining
physical/logical root, verification worktree, child home, temp, build, package,
runtime, raw evidence, or update-lab path. `DEFERRALS` contains exactly these
pending manual claims/actions:

```text
WINDOWS_11_V3
MACOS_INTEL_V3
MACOS_APPLE_SILICON_V3
LINUX_X86_64_V3
LINUX_AARCH64_V3
DOCKER_AMD64_V3
DOCKER_ARM64_V3
PRODUCTION_SIGNING
MAIN_PROMOTION
TAG_PUBLICATION
```

## Completion Gate

The machine checker validates the canonical graph lock, required task state,
supersessions, exact Git sync/cleanliness, candidate source/tree/artifact/image
identity, sanitized evidence, invalidation reruns, campaign cleanup, and manual
deferrals. It pins `origin` to `toodi0418/CMClient` and rejects missing,
indirect, expired, or secret-bearing evidence.

`P17-T07` uses two phases:

1. While P17-T07 is `in_progress`, run with `--exclude-task P17-T07` and
   `--write-precheck-attestation`.
2. Commit and push the evidence/docs-only P17-T07 change.
3. Reconcile P17-T07 to `done` and rerun without exclusions.

The successful first run atomically writes `state/GOAL_PRECHECK.json`, binding
its clean HEAD, normalized task-state
projection, exact candidate/evidence/graph/checker digests, source identity,
campaign, and expected origin. The second run is the Goal completion gate: it
requires that proof to predate and parent the P17-T07 checkpoint, and proves
that any commit after the runtime candidate freeze changed only allowed
documentation/evidence paths.
`main`, tags, production signing, notarization, and publication remain separate
explicit-human actions.

## Recovery Examples

Run these from the Repository or use the same interface through the workspace
thin wrappers:

```bash
python scripts/reconcile-task-state.py P13-T02
python scripts/reconcile-task-state.py P13-T02 --push-local
python scripts/repair-task.py start P14-T09 --title "fix shared upstream race" \
  --affected-case LIVE_DATA
python scripts/repair-task.py resume P14-T09 P14-T13
python scripts/repair-task.py resolve P14-T13 --candidate <sha256-digest>
python scripts/goal-completion-check.py --exclude-task P17-T07 \
  --write-precheck-attestation
python scripts/goal-completion-check.py
```

Repository CI owns and checks this contract. Workspace state remains outside
the Repository and must never be committed with product code.
