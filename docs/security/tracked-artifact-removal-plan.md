# Tracked Runtime Artifact Removal Plan

## Scope And Handling

This is a metadata-only inventory of runtime and generated artifacts tracked
by the legacy `dev` baseline. It intentionally does not read, print, parse, or
copy the content of `.env`, the SQLite file, the archive, or the log.

| Path | Git blob size | Classification | Required disposition |
| --- | ---: | --- | --- |
| `.env` | 496 bytes | Potential secret-bearing runtime configuration | Treat all possible values as exposed, rotate them outside Git, remove the tracked file, and provide only a sanitized `.env.example` when the 2.0 configuration contract exists |
| `callmesh-data.sqlite` | 0 bytes | Runtime database artifact | Remove from Git. It contains no committed data to migrate; real user databases are handled through P11 backup/import/verification flows |
| `callmesh-client.tar` | 92,791,808 bytes | Generated container/archive artifact | Remove from Git. Publish reproducible images and release artifacts through CI/registry instead |
| `nohup.out` | 30,320 bytes | Runtime log | Remove from Git. Retain operational logs only in the configured data/log directory with retention and redaction |

The current `.gitignore` is extended in this task so these names cannot be
reintroduced as new tracked files. Ignore rules do not untrack existing files;
the removal work must still update the Git index.

## Evidence And Boundaries

- The legacy runtime creates `callmesh-data.sqlite` under its configured data
  directory. The root-level zero-byte file is not a user-data source and must
  not be mistaken for a migration input.
- `callmesh-client.tar` is a POSIX tar archive. Its content is not a source of
  truth for the new release process.
- `nohup.out` is a text log and is not a test fixture or product document.
- The Meshtastic Git submodules and versioned protobuf sources are source
  dependencies, not generated artifacts. Their future disposition is governed
  by the protocol/schema migration work rather than this removal plan.
- Committed offline map tiles and fonts are separately identified in the
  feature matrix for license and size review. They are not silently retained
  merely because they are static files.

## Execution Sequence

1. Before anyone reuses legacy configuration, rotate any CallMesh keys, APRS
   passcodes, tokens, or other credentials that might have appeared in the
   tracked `.env`. Do not attempt to recover or copy values into the new tree.
2. When the 2.0 configuration package and migration boundary are in place,
   remove the four paths from the Git index and working tree. Add a sanitized
   `.env.example` only after its fields have a documented contract.
3. Keep historical Git commits intact: do not rewrite public history, force
   push, or use a history-rewriting cleanup as part of this project workflow.
4. P11 performs the remaining legacy runtime/build cleanup after the new
   implementations and user-data migration are verified. It must also remove
   any remaining references, generated output, and dead dependencies.

## Completion Checks For The Removal Commit

- `git ls-files` has no `.env`, SQLite, tar, or `nohup.out` entry.
- `git check-ignore -v` reports the matching ignore rule for an equivalent
  untracked candidate.
- The repository secret scan passes without reading a retained secret.
- CI obtains configuration from supported runtime secret storage or injection,
  never a tracked environment file.
- A clean install, legacy data migration, and diagnostic bundle check confirm
  that user data and secrets are not bundled with source or release artifacts.
