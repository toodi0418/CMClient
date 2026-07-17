# Gateway Persistence Foundation

Gateway persistence uses local SQLite through Node's `node:sqlite` runtime.
Every database enables WAL mode, foreign keys, and a busy timeout before
migrations run. The migration journal is forward-only and each unapplied
migration plus its journal row executes in one `BEGIN IMMEDIATE` transaction.

The initial schema contains only migration metadata and `settings`; domain
tables are introduced by their owning P03-P05 tasks. Repositories serialize
settings values as JSON and never accept secret storage through this layer.
Database backups, retention, integrity checks, and restore verification follow
in their dedicated maintenance and updater tasks.
