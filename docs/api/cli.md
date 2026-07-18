# CMClient CLI Contract

`cmclient` is a Rust Agent client. It never opens the SQLite database or a
Meshtastic device directly. Global options are `--json`, `--quiet`,
`--no-color`, `--timeout`, and `--endpoint`.

Endpoints are local IPC (`local`, `unix:///absolute/path`, or Windows named
pipe) or explicitly remote `https://` endpoints. Tokens are not accepted as
CLI arguments.

Exit codes are stable: 0 success, 2 usage, 3 connection, 4 authentication, 5
validation, 6 operation failure, 7 partial/degraded, and 8 timeout. `status`,
`start`, `stop`, and `restart` invoke the local Control API; `version` has no
Agent dependency and supports stable JSON output.

`update` reads the Agent-owned persistent update projection and reports the
current phase, transfer, speed, and stable failure code without contacting
Gateway. `update --follow` reconnects to the private
`/api/v1/control/updates/events` SSE feed and prints each complete status
projection. With `--json`, each line is one stable
`UpdateControlStatus` JSON document; `--quiet` suppresses normal output.
