# Product State Migration

CMClient migrates known pre-unified product state before the Agent reads the
canonical configuration or starts Gateway. Migration is a product startup
transaction, not a general-purpose Legacy history importer and not a database
merge command.

The Agent derives the new `~/.cmclient` root and the platform's known older
roots from one immutable startup environment snapshot. It then acquires an
exclusive new-root migration lock and a source-specific lock. If more than one
eligible source is present, or the new root already contains live product
state, migration stops with a stable conflict code for the recovery UI. It
never chooses a source or merges state by guessing.

## Bounded Inventory

Only these known product leaves are eligible:

- the Agent configuration, mapped to `config.toml`;
- an existing plaintext `secrets.json` file;
- the Gateway SQLite database, mapped to `cmclient.db`;
- bounded user-created SQLite backup files below `backups/`.

Known leaves must be regular files. Symlinks, junctions, reparse points,
unexpected file types, excess files, oversized files, and aggregate size
overflow fail closed. Cache, temporary files, service metadata, logs, update
staging, raw captures, and unknown files are never inventoried or copied.
Unsupported Keychain, Credential Manager/DPAPI, Secret Service, and systemd
vault entries are not exported. Setup asks for any missing credential again.

Inventory evidence binds each main database, WAL, configuration, secret, and
backup leaf to its platform file identity as well as its length and digest.
Replacing a source leaf with byte-identical content is therefore still a
source change and fails closed. Existing files are opened without following a
symbolic-link or reparse-point leaf; the opened handle and final path identity
must remain equal through the bounded read.

## Durable Transaction

The compact journal at `~/.cmclient/state/migration.json` has exactly these
durable phases:

1. `detected`
2. `staged`
3. `verified`
4. `activated`
5. `complete`

Regular files are copied into the bounded
`~/.cmclient/cache/migration-stage` tree. The pinned private Node Gateway
maintenance command creates the SQLite backup, applies the exact compiled
forward migrations, and reports integrity, foreign keys, schema history, and
domain counts. Rust coordinates paths, locks, fingerprints, the journal, and
atomic activation; it does not implement Gateway schema or SQLite backup
logic.

Before each durable transition and activation, CMClient rechecks source and
stage fingerprints. Activation publishes only the known target leaves with
atomic writes. A restart reconciles already-published leaves by their expected
digests and resumes from the last durable phase. Source mutation, stage
tampering, a malformed journal, database verification failure, or an
unexplained target produces a stable recovery state instead of deleting
evidence or starting Gateway.

Atomic-write residue is recognized only in the exact reserved sibling-name
namespace for a known destination. Gateway maintenance residue is recognized
only at its deterministic transaction work directory with its fixed flat
allowlist. Cleanup is bounded and non-recursive, rejects linked or multiply
linked files, and rechecks the captured directory identity chain immediately
before each removal. Unknown or unsafe residue is retained and migration fails
closed.

Source payload files and directories are never modified or deleted. The sole
permitted source mutation is creating and retaining an empty `agent.lock` in an
existing legacy root when neither supported lock leaf exists; CMClient holds
that coordination file exclusively while it inventories and migrates. An
existing root-level or `run/agent.lock` must be an empty regular, single-link
file and is reused byte-for-byte. A successful migration leaves every legacy
payload available for operator recovery, and a repeated startup is idempotent.

The `complete` phase is written durably before plaintext staging cleanup. A
restart may therefore finish cleanup even when the legacy source no longer
exists. It removes only stage leaves whose identity, size, and digest still
match journal-owned evidence, with `secrets.json` removed first, followed by
verified empty directories. Cleanup failure preserves the durable `complete`
phase, records the stable cleanup recovery code, and is retried on the next
start; successful retry clears that code.

Windows and macOS candidates are single-root sources. Linux treats its legacy
XDG configuration and data directories as one logical candidate, so config and
plaintext secrets may come from the config root while the database and backups
come from the data root. If only one side exists it becomes the single source;
two distinct populated logical candidates remain an ambiguity. Backup
inventory is recursive within its fixed depth/count/size bounds, accepts only
`.sqlite` and `.db` leaves, and preserves their relative subdirectories.

When a legacy SQLite `-wal` exists, its exact bytes and length, including a
valid zero-length WAL, are part of the source evidence. Offline maintenance
opens only a byte-for-byte staging snapshot; it never opens the live source
database through SQLite. A present shared-memory file must be a non-empty safe
regular file and is topology evidence only, including its platform file
identity; its volatile contents are not hashed. Main, WAL, shared-memory,
config, secret, and backup evidence is rechecked before each durable
transition.

## Offline Maintenance Boundary

Gateway's offline maintenance mode is internal to the Agent and migration
tooling. It receives a bounded JSON request over private standard input with
absolute source and staged database paths, performs no network activity, and
emits one bounded JSON report. It does not start HTTP, CallMesh, Meshtastic,
APRS, Proxy, or normal background maintenance.

The standalone `cmclient-migrate product` entry point exists for controlled
recovery and automated kill/resume qualification. It uses the same transaction
and Gateway maintenance command as Agent startup; it is not an alternate data
import format or a public way to edit a live database.
