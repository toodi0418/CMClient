# CMClient CLI Contract

`cmclient` is a Rust Agent client. It never opens the SQLite database or a
Meshtastic device directly. Global options are `--json`, `--quiet`, `--no-color`,
`--background`, `--timeout`, and `--endpoint`. With no subcommand, `cmclient`
starts or attaches the resident Agent and asks it to launch or focus Desktop.
`cmclient --background` starts or attaches only the resident Agent. Any subcommand
remains command mode; combining one with `--background` fails closed. Desktop
launch is an Agent-owned Control command, so the CLI never owns a Desktop process
or reads local state.

Endpoints are local IPC (`local`, a Unix socket below `~/.cmclient/run`, or the
root-hashed Windows named pipe). The typed client uses bounded length-delimited
request, response, and event envelopes. CMClient does not expose a remote CLI Control
endpoint. It never accepts a Control token through arguments, environment, or
persistent storage.

Exit codes are stable: 0 success, 2 usage, 3 connection, 4 authentication, 5
validation, 6 operation failure, 7 partial/degraded, and 8 timeout. The command
surface is `status`, `start`, `stop`, `restart`, `reset`, `version`, `logs`, `events`,
`doctor`, `web`, `meshtastic`, `nodes`, `positions`, `aprs`, `proxy`, `update`,
`backup`, `diagnostics`, `secret`, and `database`. `version` has no Agent
dependency and returns the command-mode component plus the complete product,
source, channel, and target identity; all other runtime commands use the
Control API. `doctor` returns 7
when the combined status/diagnostic projection is degraded. Backup and database
commands return persistent Job acceptance rather than touching SQLite.

`reset --confirm operational-reset` is the only command-mode operational reset
form. It has no default and rejects every other confirmation value with
`CLI_RESET_CONFIRMATION_INVALID` and validation exit code 5. The Agent then
performs the same durable generation fence, supervised Gateway stop, secret and
configuration clearing, and setup return used by the Management Web.

`update` reads the Agent-owned persistent update projection and reports the
current phase, transfer, speed, and stable failure code without contacting
Gateway. `update --follow` reconnects to the private typed update-event
subscription and prints each complete status projection. With `--json`, each
line is one stable
`UpdateControlStatus` JSON document; `--quiet` suppresses normal output.

`diagnostics` reads the Agent's sanitized diagnostic bundle through the private
Control API. `secret set callmesh-api-key` reads one value from standard input
and sends it to Agent; `secret remove <kind>` asks Agent to remove an entry.
The CLI never opens `~/.cmclient/secrets.json`, and secret values are never
accepted as command arguments, environment variables, printed output, or JSON.
The legacy `aprs-passcode` and `management-admin-token` kinds are accepted only
by `secret remove` so upgrades can delete obsolete entries; trying to set either
returns `CLI_SECRET_KIND_DEPRECATED` with validation exit code 5.

`logs --follow`, `events --follow`, and `update --follow` consume bounded typed
Control event frames, reconnect after transient disconnects, and exit cleanly
on Ctrl+C. The Agent's internal Gateway bridge still consumes Gateway SSE, but
SSE text never crosses the local Control endpoint. `--timeout` starts before
local endpoint connection and bounds connect and request setup, including the
request write and initial response or subscription acceptance. A timeout maps
to exit 8 rather than an ordinary connection failure. Human output, JSON, quiet
mode, and colour suppression are handled by the CLI after a shared projection
is returned.

`events` displays every validated domain event. `logs` is currently a reserved
projection that filters for `log.entry`; no production Gateway publisher emits
that event type in this RC, so `logs` can remain empty. Use `events` plus the
platform service manager's logs for current operational output.

`--endpoint` may select another local Unix socket for diagnosis, but relative
paths, URL credentials, TCP, HTTP, and HTTPS endpoints fail closed. Management
LAN access belongs to the Web login/session boundary and does not make the CLI
remotely addressable. CLI and Desktop therefore need no stored credential and
cannot read the Agent's plaintext secret backend.
