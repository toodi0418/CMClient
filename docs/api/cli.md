# CMClient CLI Contract

`cmclient` is a Rust Agent client. It never opens the SQLite database or a
Meshtastic device directly. Global options are `--json`, `--quiet`,
`--no-color`, `--timeout`, and `--endpoint`.

Endpoints are local IPC (`local`, `unix:///absolute/path`, or Windows named
pipe) or explicitly remote `https://` endpoints. Tokens are not accepted as
CLI arguments.

Exit codes are stable: 0 success, 2 usage, 3 connection, 4 authentication, 5
validation, 6 operation failure, 7 partial/degraded, and 8 timeout. The command
surface is `status`, `start`, `stop`, `restart`, `version`, `logs`, `events`,
`doctor`, `web`, `meshtastic`, `nodes`, `positions`, `aprs`, `proxy`, `update`,
`backup`, `diagnostics`, `secret`, and `database`. `version` has no Agent
dependency; all other runtime commands use the Control API. `doctor` returns 7
when the combined status/diagnostic projection is degraded. Backup and database
commands return persistent Job acceptance rather than touching SQLite.

`update` reads the Agent-owned persistent update projection and reports the
current phase, transfer, speed, and stable failure code without contacting
Gateway. `update --follow` reconnects to the private
`/api/v1/control/updates/events` SSE feed and prints each complete status
projection. With `--json`, each line is one stable
`UpdateControlStatus` JSON document; `--quiet` suppresses normal output.

`diagnostics` reads the Agent's sanitized diagnostic bundle through the private
Control API. `secret set <kind>` reads one value from standard input and stores
it through the Agent; `secret remove <kind>` removes it. Secret values are never
accepted as command arguments, printed, or returned as JSON.

`logs --follow`, `events --follow`, and `update --follow` use bounded SSE
parsing, reconnect after transient disconnects, and exit cleanly on Ctrl+C.
`--timeout` applies to setup and bounded requests; timeout maps to exit 8 rather
than an ordinary connection failure. Human output, JSON, quiet mode, and colour
suppression are handled by the CLI after a shared projection is returned.

`events` displays every validated domain event. `logs` is currently a reserved
projection that filters for `log.entry`; no production Gateway publisher emits
that event type in this RC, so `logs` can remain empty. Use `events` plus the
platform service manager's logs for current operational output.

For a remote endpoint, configure the Agent's Management LAN HTTPS listener,
provision its OS-stored management admin token, and give the same value to the
remote CLI through only the calling process's `CMCLIENT_CONTROL_TOKEN`
environment variable. This Bash example avoids placing the value directly in
the command or shell history:

```bash
IFS= read -r -s -p 'CMClient control token: ' CMCLIENT_CONTROL_TOKEN
printf '\n' >&2
export CMCLIENT_CONTROL_TOKEN
cmclient --endpoint https://cmclient.example --timeout 30 status
unset CMCLIENT_CONTROL_TOKEN
```

The token is never accepted by a CLI option. The client rejects non-HTTPS URLs,
URL credentials, non-root paths, query/fragment, and tokens outside 32 through
4096 UTF-8 bytes or containing ASCII control characters, then signs each
request with the shared `control:admin` HMAC contract. It does not follow
redirects. Agent rejects expired or replayed signatures.
Missing/malformed tokens and remote 401/403 responses map to authentication
exit 4.
