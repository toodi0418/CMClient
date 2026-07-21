# Documentation Authority

CMClient is being rebaselined from the P12 multi-surface implementation to one
unified product. Read documents in this order and apply the first applicable
authority when two documents conflict.

## Current Target Contract

1. Repository [AGENTS.md](../AGENTS.md) for architecture, Git, security, and
   claim boundaries.
2. [License and source provenance](architecture/license-provenance.md).
3. [Unified architecture](architecture/CMCLIENT_2_OVERVIEW.md).
4. [Runtime and onboarding](architecture/runtime-onboarding.md).
5. [Release objects](architecture/release-artifacts.md).
6. [Docker target](architecture/docker-deployment.md).

These files define the approved target even while owning P13-P16 tasks are still
implementing it. They must state transition status honestly and may not claim an
unimplemented package or runtime result.

The committed `scripts/unified-task-graph-lock.json` is the machine authority
for active P13-P17 definitions. Workspace task state may add only validated
repair tasks; license, CallMesh service, historical supersession, coverage, and
completion metadata may not drift from that lock.

## Implementation Detail

The remaining files under `docs/architecture`, `docs/api`, `docs/admin`, and
`docs/user` describe implemented boundaries and operational details. Keep them
as useful code evidence, but migrate conflicts in the task that owns the
affected behavior. Source and tests remain authoritative evidence of what the
current commit actually does; they do not silently redefine the approved target.

## Historical Snapshots

P12 RC notes, field-validation material, feature-parity evidence, legacy
deployment/security guides, and the changelog are historical evidence. In
particular, separate Desktop/Headless/CLI/Service downloads, system Node,
OS-specific mutable roots, three Docker containers, and the old artifact matrix
are not current release choices. Preserve those records until their owning
tasks replace the implementation and evidence schema.

## Branch And Release Boundary

Development and checkpoints use only `dev`. Do not modify or push `main` without
a new explicit user approval naming the operation. Tags, production signing,
notarization, publication, and formal release are separate human gates.
