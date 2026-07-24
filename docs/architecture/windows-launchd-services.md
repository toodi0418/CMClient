# Windows Service and macOS launchd

## Windows

`cmclient-service-host.exe` is a real Windows SCM process, not an `sc.exe`
wrapper around a console binary. It receives SCM start/stop/shutdown controls,
starts the adjacent `cmclient-agent.exe`, and reports the Agent child exit as a
service failure. On stop it repeatedly requests the local-only Agent shutdown
operation, waits up to 50 seconds for Agent and Gateway cooperative teardown, then
uses process termination only as a bounded fallback. The supervised Gateway
also treats its Agent stdin pipe closing as parent death, so fallback cannot
leave an orphan Gateway holding the loopback port. The Agent's durable update
recovery handles an interrupted update on its next start.

Each local shutdown request runs in a single bounded worker, and both its wait
and the following poll sleep are capped by the remaining 50-second budget. A
stalled named-pipe request therefore cannot delay the process fallback beyond
the service deadline.

`scripts/cmclient-windows-service.ps1` and `cmclient-service-host.exe` are
retained transition and qualification surfaces, not the public Windows install
model. Public Setup uses current-user login startup without routine UAC. When
the SCM fixture is exercised, Agent derives every mutable path from the service
identity's effective `%USERPROFILE%\.cmclient`; the host does not inject split
data, configuration, cache, or log roots. The manager accepts only the
service-host path and operational actions; it never accepts or writes a
CallMesh key, APRS credential, Control token, or signing key. `CMClientAgent`
remains the fixed singleton fixture identity. Uninstall removes that test
registration and retains the effective home's `.cmclient` directory.

The private Agent Control API is an `interprocess` local named pipe derived from
a SHA-256 digest of the canonical state root. It never falls back to TCP and
carries no bearer token. The public current-user product uses the same user's
pipe with remote clients rejected by the named-pipe transport; it makes no
cross-account access claim and requires no SID impersonation, custom ACL,
unsafe FFI, PID identity, or UAC flow.

Control carries bounded length-delimited typed envelopes rather than HTTP.
Malformed or oversized frames fail with stable Control codes, a peer that does
not complete a frame reaches the bounded deadline, and disconnects release the
request slot. The Service Host uses the same typed client as command mode.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/cmclient-windows-service.ps1 install `
  -HostPath "C:\Program Files\CMClient\current\bin\cmclient-service-host.exe"
powershell -ExecutionPolicy Bypass -File scripts/cmclient-windows-service.ps1 status
powershell -ExecutionPolicy Bypass -File scripts/cmclient-windows-service.ps1 logs -Lines 200
powershell -ExecutionPolicy Bypass -File scripts/cmclient-windows-service.ps1 uninstall
```

The `logs` action accepts between 1 and 10,000 lines and reads only regular,
non-reparse-point application logs under the effective user's
`.cmclient\logs`:
the newest `service-host.jsonl.YYYY-MM-DD`, `agent.jsonl.YYYY-MM-DD`, and
`gateway.jsonl.YYYY-MM-DD` file in each family, with fixed-name legacy
fallback. These are sanitized, size-bounded, retained JSONL files; the service
manager never returns raw child stdout or stderr. A missing log set or an
unsafe path fails with a stable manager error code.

## macOS

`scripts/cmclient-launchd.sh` installs `io.cmclient.agent` as a per-user
LaunchAgent at `~/Library/LaunchAgents`. The generated plist injects only the
owning user's `HOME`; Agent derives its sole mutable root as `~/.cmclient`.
Runtime credentials exist only in `~/.cmclient/secrets.json`, with the root at
mode `0700` and the atomically replaced file at mode `0600`. There is no
Keychain mode, external secret selector, or secret-bearing launchd environment.
It is intentionally not a root LaunchDaemon.

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
size-bounded `agent.jsonl.YYYY-MM-DD` and `gateway.jsonl.YYYY-MM-DD` families
under `~/.cmclient/logs`; `logs --lines N` accepts `1..10000` and
tails only each family's newest regular file, with fixed-name legacy fallback. Its
uninstall operation removes only the plist; `~/.cmclient` remains available for
reinstall and rollback.
