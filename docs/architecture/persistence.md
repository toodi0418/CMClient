# Gateway Persistence Foundation

Gateway persistence uses local SQLite through Node's `node:sqlite` runtime.
Every database enables WAL mode, foreign keys, and a busy timeout before
migrations run. The migration journal is forward-only and each unapplied
migration plus its journal row executes in one `BEGIN IMMEDIATE` transaction.

The schema contains migration metadata, `settings`, and `jobs`. The Job table
stores a type, JSON execution input/result, stable error code/params, lifecycle
timestamps, cancellation flag, and a partial unique `(type, idempotency_key)`
index. Public APIs never return the stored execution input or result. Repositories
serialize settings values as JSON and never accept secret storage through this
layer.
Database backups, retention, integrity checks, and restore verification follow
in their dedicated maintenance and updater tasks.
