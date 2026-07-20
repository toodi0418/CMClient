# Developer Guide

CMClient 2.0 is a boundary-driven workspace:

| Owner | Responsibility |
| --- | --- |
| Rust Agent | Process supervision, single-instance lock, local/remote Control, Web listener, updates, backup/recovery |
| TypeScript Gateway | Meshtastic transports, protobuf/domain normalization, SQLite, Position/APRS, CallMesh, Proxy, Jobs, SSE |
| Vue Web | Presentation and validated API/SSE clients; no privileged I/O |
| Tauri Desktop | Small Agent Control client and supervisor UI; no duplicate Gateway |
| Rust CLI | Human/automation Control client; no direct SQLite or radio access |
| Shared contracts | TypeBox/JSON Schema, stable errors, Jobs, events, system/update wire models |

## Toolchain and bootstrap

Use Node.js `^22.18.0` or `>=24.11.0`, pnpm 11.9.0, and the pinned Rust
toolchain. From a clean checkout:

```bash
pnpm install --frozen-lockfile
pnpm format:check
pnpm lint
pnpm typecheck
pnpm test
pnpm build
cargo fmt --all -- --check
cargo test --workspace --locked
cargo build --workspace --locked
pnpm test:e2e:web
```

A standalone repository checkout has no repository-local `verify.sh`; run the
commands above directly. The surrounding AI development workspace, when used,
provides `scripts/verify.sh` and thin task-state wrappers. Their implementation
source of truth is the Repository `scripts/` directory; mutable task, candidate,
evidence, and campaign ledgers stay in the workspace. See
[Task state recovery](task-state-recovery.md). Release-specific checks include
policy/secret/dependency audits, load/resource tests, native package smoke, OCI
import smoke, SBOM/checksum verification, and final Windows SCM lifecycle
validation.

## Change workflow

1. Read the relevant Repository architecture/API documents and shared contract
   source before editing. When working through the surrounding AI workspace,
   also read its task-specific `specs/`; those files are not part of a
   standalone Repository checkout.
2. Change shared contracts first when a wire shape changes; update TypeBox,
   Rust serde, API client, event client, and docs together.
3. Add a migration for every SQLite schema change. Migrations are forward-only,
   transactional, and must have rollback/failure tests.
4. Keep Gateway routes schema-backed and return `{code, params, traceId}` with a
   stable code. Do not make UI text or backend prose an API contract.
5. Keep SSE frames bounded, replay-aware, and safe for slow consumers. Document
   new event types and the snapshot needed after reconnect.
6. Test the vertical slice at unit, contract, integration, replay, E2E, or
   packaging level according to its blast radius.

## CI and release gates

Actions are pinned to reviewed commit SHAs. `CI` must be green before a release
matrix is useful. `Release Build Matrix` first runs artifact-plan, load, and
security gates, then builds every portable composition, native Desktop format,
and both Docker platforms. Supply-chain assembly requires exact inputs,
checksums, SBOMs, source binding, and no-build final smoke. Attestation and
update-manifest signing require a protected, human-approved production
environment and an exact version tag; ordinary `dev` runs never sign.

## Boundaries that must not regress

- Do not let Web, Desktop, or CLI touch files, processes, SQLite, or
  Meshtastic directly.
- Do not add a LAN Gateway listener outside Agent's authenticated boundary.
- Do not put secrets in Git, command arguments, logs, fixtures, API responses,
  or diagnostic bundles.
- Do not reintroduce any removed Legacy runtime or compatibility path. Remote
  Dispatch is a later independent capability and remains disabled in this RC.
- Do not let Gateway update itself or let Docker claim Agent ownership.
- Do not treat received time as cross-iGate event time, and do not upload APRS
  when freshness or `precisionBits === 32` cannot be proven.

The repository boundary and checkpoint rules are in the workspace
`AGENTS.md`; only the contents of `repository/CMClient` belong in commits.
