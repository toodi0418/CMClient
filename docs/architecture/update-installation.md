# Update Installation Transaction

Only the Rust Agent may run an installation transaction. It receives a
`StagedBundle` whose manifest signature and archive bytes have already been
verified, then verifies the staged file again immediately before extraction.

The transaction follows this order:

1. Decode the signed `tar.zst` or ZIP archive into a new digest-named release
   slot under `<installation_root>/releases`.
2. Reject absolute, empty, parent-traversal, symlink, hard-link, device, and
   other special archive entries. Refuse archives whose total unpacked file
   bytes exceed the request limit.
3. Publish the fully extracted slot with a same-volume directory rename. The
   prior active release remains selected while extraction is incomplete.
4. Stop the Agent-owned runtime before copying `data_dir` and `config_dir` to a
   caller-selected backup root outside both source trees. This preserves SQLite
   database, WAL, and configuration files without recursive self-backup.
5. Atomically replace `<installation_root>/active-release.json` with the new
   digest slot, invoke the new release's forward-only migration journal, start
it, then require a passing health check.

`active-release.json` records only a schema version and verified release digest;
it contains no URLs, signing keys, tokens, or user configuration. A failed
migration, start, or health check returns a stable error without reporting a
completed update. The Agent's durable update journal records the transaction
before each side effect and restores the previous active pointer plus backup
after failure or power loss; see `docs/architecture/update-recovery.md`.
