# Windows Service and macOS launchd

## Windows

`cmclient-service-host.exe` is a real Windows SCM process, not an `sc.exe`
wrapper around a console binary. It receives SCM start/stop/shutdown controls,
starts the adjacent `cmclient-agent.exe`, and reports the Agent child exit as a
service failure. On stop it repeatedly requests the local-only Agent shutdown
route, waits up to 50 seconds for Agent and Gateway cooperative teardown, then
uses process termination only as a bounded fallback. The supervised Gateway
also treats its Agent stdin pipe closing as parent death, so fallback cannot
leave an orphan Gateway holding the loopback port. The Agent's durable update
recovery handles an interrupted update on its next start.

Each local shutdown request runs in a single bounded worker, and both its wait
and the following poll sleep are capped by the remaining 50-second budget. A
stalled named-pipe request therefore cannot delay the process fallback beyond
the service deadline.

`scripts/cmclient-windows-service.ps1` registers `CMClientAgent` with the
`LocalService` account and automatic start. The host gives the child explicit,
absolute paths under `%ProgramData%\CMClient` for data, configuration, cache,
and logs. The manager accepts only the service-host path and operational
actions; it never accepts or writes a CallMesh key, APRS passcode, token, or
signing key. `CMClientAgent` is the fixed singleton SCM identity shared by the
manager and the compiled service host; there is no service-name override.
Re-running install updates that registration's executable target. Uninstall
removes the SCM registration only and retains `%ProgramData%\CMClient`.

The private Agent Control API is the local named pipe
`\\.\pipe\cmclient-control`. It uses the default LocalService security
descriptor, so control clients require an OS principal allowed by that local
pipe, typically an elevated administrator. It never falls back to a TCP
listener. This restrictive default is intentional until a user-scoped pipe ACL
is configured as part of a future authenticated installer flow.

Windows byte-mode `PIPE_NOWAIT` reports both a temporarily empty pipe and a
closed peer as the same zero-byte read through the synchronous transport. The
client therefore waits for its bounded deadline instead of treating zero bytes
as EOF: no complete response, including a peer that closes silently, maps to
`CONTROL_TIMEOUT`. Complete malformed HTTP still maps to
`CONTROL_HTTP_INVALID`. A future overlapped transport may distinguish EOF
without changing the deadline guarantee.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/cmclient-windows-service.ps1 install `
  -HostPath "C:\Program Files\CMClient\current\bin\cmclient-service-host.exe"
powershell -ExecutionPolicy Bypass -File scripts/cmclient-windows-service.ps1 status
powershell -ExecutionPolicy Bypass -File scripts/cmclient-windows-service.ps1 logs -Lines 200
powershell -ExecutionPolicy Bypass -File scripts/cmclient-windows-service.ps1 uninstall
```

The `logs` action accepts between 1 and 10,000 lines and reads only regular,
non-reparse-point application logs under `%ProgramData%\CMClient\logs`:
`service-host.jsonl`, `agent.jsonl`, and `gateway.jsonl`. These are sanitized,
size-bounded, retained JSONL files; the service manager never returns raw child
stdout or stderr. A missing log set or an unsafe path fails with a stable
manager error code.

## macOS

`scripts/cmclient-launchd.sh` installs `io.cmclient.agent` as a per-user
LaunchAgent at `~/Library/LaunchAgents`. It launches the Agent with the owning
user's standard CMClient Application Support and Caches paths, so Keychain
credentials remain in that user's security context. It is intentionally not a
root LaunchDaemon.

```bash
bash scripts/cmclient-launchd.sh install \
  --agent /Applications/CMClient/current/bin/cmclient-agent
bash scripts/cmclient-launchd.sh status
bash scripts/cmclient-launchd.sh logs --lines 200
bash scripts/cmclient-launchd.sh uninstall
```

The generated plist has `RunAtLoad`, restarts only an unsuccessful Agent exit,
and uses a five-second throttle. launchd sends fallback stdout and stderr to
`/dev/null`, preventing its own unbounded `agent.stdout.log` and
`agent.stderr.log` files. Agent and Supervisor instead own sanitized,
size-bounded `agent.jsonl` and `gateway.jsonl` files under the user's Agent log
directory; `logs --lines N` accepts `1..10000` and tails only their active regular files. Its
uninstall operation removes only the plist; configuration, data, cache, and
logs remain available for reinstall and rollback.
