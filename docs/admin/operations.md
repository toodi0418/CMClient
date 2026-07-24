# Operations

## Health and identity

Start with the Agent-owned checks, then query Gateway projections:

```bash
cmclient status --json
cmclient doctor --json
cmclient --json version
curl --fail http://127.0.0.1:7080/api/v1/system/health
curl --fail http://127.0.0.1:7080/api/v1/system/version
```

The `cmclient` commands use the current user's private Control endpoint:
`~/.cmclient/run/control.sock` on macOS/Linux or the local named pipe on
Windows. The Linux headless system unit uses the dedicated account's
`/home/cmclient/.cmclient/run/control.sock`; root may select it explicitly.
`cmclient-systemd.sh status` reports only unit state. There is no separate
system-service data root or Control token. If Management Web is disabled, use
local command mode only; a configured LAN listener uses Web authentication
instead of exposing Control routes.

`running` means Agent's child passed the Gateway health probe. A live process
that fails the probe is `degraded`. Always record `version`, source `commit`,
channel, and `x-trace-id` when opening an incident.

## Logs and events

`cmclient events` reads the bounded domain event stream. `cmclient logs` is a
reserved filter for `log.entry`, which no production Gateway publisher emits
in this RC, so it can be empty. Use `cmclient events` and the platform service
manager's logs for current diagnosis. Follow streams receive a 15-second
heartbeat, reconnect after transient failures, and close slow consumers.
Agent's Gateway bridge and the Web event client preserve `Last-Event-ID` within
their reconnect paths. When CLI itself opens a new Control subscription it
does not carry the previous ID, so refresh the recent REST/CLI projection after
a gap; update subscriptions instead receive an immediate durable snapshot. The
Web Logs page is an in-memory current-session event buffer, not a durable audit
record.

Agent writes the `agent.jsonl.YYYY-MM-DD` daily family; Supervisor drains
Gateway stdout/stderr into `gateway.jsonl.YYYY-MM-DD`; the Windows SCM wrapper
additionally writes `service-host.jsonl.YYYY-MM-DD`. Each JSONL record has
`schemaVersion: 1`. Structured
stdout is allowlisted and recursively redacted, while child stderr accepts only
a stable uppercase code; malformed, raw, or oversized output becomes a generic
`RUNTIME_LOG_*` record. Unix log files are mode `0600`, and symlink, reparse
point, or non-file destinations fail closed. Platform manager `logs` commands
accept only `1..10000` lines. launchd, systemd, and Windows select the newest
UTC-dated file in each family and retain fixed-name legacy fallback. systemd
falls back, only when both application families are absent,
to the bounded journal tail filtered to stable uppercase codes. No manager
exposes unbounded raw child output.

The active daily file and its retained daily generations are bounded by strict
decimal environment settings supplied to Agent or Service Host:

| Setting | Default | Accepted range |
| --- | --- | --- |
| `CMCLIENT_LOG_MAX_BYTES` | 10 MiB | 128 KiB through 64 MiB |
| `CMCLIENT_LOG_RETAINED_FILES` | 5 | 1 through 16 |
| `CMCLIENT_LOG_MAX_LINE_BYTES` | 64 KiB | 256 bytes through 1 MiB, and no more than half the file limit |

Invalid or out-of-range values report `RUNTIME_LOG_POLICY_INVALID`. A daily
file stops accepting records at its byte quota with
`RUNTIME_LOG_RETENTION_LIMIT`; the next UTC day starts a new file. Within Agent
and Supervisor, rollover, write, and bounded-queue failures remain stable
runtime codes in Agent `latestErrorCode` and never cause a shutdown
reader/writer deadlock. If Windows Service Host rejects policy before it can
open its sink or start Agent, SCM reports a service-start failure; no JSONL or
Agent health projection exists for that pre-start failure. Changes take effect
when the owning process is restarted.

## Jobs, backup, and diagnostics

Backups and SQLite integrity checks are persistent asynchronous Jobs. Agent
diagnostics are a separate, immediate sanitized projection:

```bash
cmclient backup --json
cmclient database --json
cmclient diagnostics --json
```

The first two commands return a Job ID to poll through the API or Web
Diagnostics view; `diagnostics` returns the bounded Agent bundle directly. Job
input and database rows stay internal. Idempotency keys prevent duplicate work;
queue saturation returns `JOB_QUEUE_FULL` without creating a row. A bounded
retention task eventually removes terminal Jobs after the configured window.

## Updates and recovery

`cmclient update` reports Agent journal state; `cmclient update --follow` reads
the private update SSE. The public Gateway has no update trigger. Agent alone
verifies the signed manifest before network staging, checks exact bytes and
SHA-256, snapshots non-secret state below `~/.cmclient`, installs a digest-named
release, migrates, and health-checks it. An interruption in a mutation phase first records durable
`rolling_back`, restores the backup and active pointer, and exposes a stable
failure code. Do not delete the journal or staging directory while recovering.

## Legacy migration

Agent detects and migrates known older product state before it reads
`~/.cmclient/config.toml` or starts Gateway. Do not run a manual SQLite import
against a live or populated target. The native database is always
`$HOME/.cmclient/cmclient.db`. CMClient stages known configuration, plaintext
secrets, database, and user backup leaves; the pinned private Node Gateway
performs SQLite backup, forward migration, integrity, foreign-key,
schema-history, and domain-count verification.

Progress is recorded at `~/.cmclient/state/migration.json` as `detected`,
`staged`, `verified`, `activated`, then `complete`. Interrupted work resumes
automatically. CMClient never modifies or deletes source payload. It may create
and retain only an empty `agent.lock` coordination file in an existing legacy
root, or reuse an existing empty root-level or `run/agent.lock`; a non-empty or
linked lock fails closed. It never imports cache, temporary files, service
metadata, logs, unknown files, links, junctions, or credentials from an
operating-system secret store.

If Agent reports `LEGACY_MIGRATION_TARGET_POPULATED`, preserve both roots and
use the Web recovery flow; do not move individual files into the new root. Source
mutation, digest mismatch, or database verification errors likewise require
preserving the journal and staging tree for diagnosis. The internal
`cmclient-migrate product` command uses the same transaction for controlled
recovery and qualification, not for merging arbitrary history.

## Common stable codes

| Code | Meaning | First action |
| --- | --- | --- |
| `AGENT_INSTANCE_ALREADY_RUNNING` | Another Agent owns `~/.cmclient/run/agent.lock` | Inspect the existing process; do not delete the lock while it runs |
| `AGENT_CONFIG_INVALID` | Strict TOML or path validation failed | Run `cmclient-agent --check-config` and fix the named section |
| `AGENT_INSTANCE_STATE_INVALID` | The bounded Agent instance-state document is malformed or has an unsupported schema | Stop the Agent, preserve the file for diagnosis, and remove it only after confirming no Agent instance is running |
| `GATEWAY_PROXY_UNAVAILABLE` | Agent cannot reach its supervised Gateway | Check Agent status, child logs, and Gateway health |
| `CONTROL_RESOURCE_EXHAUSTED` | Local IPC connection limit reached | Close stale clients; retry with bounded backoff |
| `SSE_SUBSCRIBER_LIMIT_REACHED` | Gateway event subscriber cap reached | Disconnect unused streams and reconnect |
| `JOB_QUEUE_FULL` | Bounded Job queue is full | Wait for existing Jobs; do not submit an unbounded retry loop |
| `JOB_INTERRUPTED_BY_RESTART` | A non-terminal Job was interrupted | Inspect the persisted Job; resubmit only if its operation is safe |
| `UPDATE_ROLLBACK_FAILED` | Update recovery did not complete | Preserve the journal/backups and stop automatic retries |
| `unavailable_in_docker` | Native-only capability is unavailable in Docker | Use the applicable native mode or the Docker operator workflow |

Never “fix” a stable error by weakening validation, exposing a raw Gateway
port, or copying a credential into a log. Preserve the trace ID and sanitized
diagnostic projection for investigation.
