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
