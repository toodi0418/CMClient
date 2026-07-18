# Update Rollback And Recovery

The Rust Agent stores the single active update job in
`<data_dir>/updates/update-job.json`. Writes use a synced temporary file and
same-directory rename. The journal records a schema version, opaque local job
ID, current phase, millisecond UTC times, stable error code, safe byte progress,
a bounded code-only log, and the rollback plan needed after activation.

The phases are `idle`, `checking`, `available`, `downloading`, `verifying`,
`staging`, `backing_up`, `stopping`, `installing`, `migrating`, `starting`,
`health_checking`, `completed`, `failed`, `rolling_back`, and
`rollback_completed`.

On Agent startup, an interrupted pre-mutation phase such as `downloading` is
persisted as `failed` with `UPDATE_INTERRUPTED_BY_RESTART`. For an interrupted
mutation phase from `backing_up` through `health_checking`, the Agent first
persists `rolling_back`, restores the backup data/config directories via
same-parent replacement, restores the previous active release pointer, then
persists `rollback_completed`. If filesystem rollback cannot finish, the
journal remains `failed` with `UPDATE_ROLLBACK_FAILED` for operator attention.

Agent Control exposes this state while Gateway is stopped at
`GET /api/v1/control/updates` and
`GET /api/v1/control/updates/events`. The SSE feed sends an initial snapshot,
subsequent `update.status_changed` projections, and heartbeat comments. It is
an observation channel only; release signing, download, installation, backup,
and rollback remain Agent-owned operations.
