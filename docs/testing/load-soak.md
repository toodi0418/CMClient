# Load, Soak, and Backpressure Gate

`pnpm test:load` is the repeatable release hardening gate for bounded runtime
resources. It uses synthetic local fixtures only; no Meshtastic hardware,
CallMesh key, APRS passcode, or Internet connection is required.

The default run executes one iteration. RC field validation should increase the
same deterministic suite rather than use a different workload:

```bash
pnpm test:load
CMCLIENT_SOAK_ITERATIONS=10 pnpm test:load
```

`CMCLIENT_SOAK_ITERATIONS` accepts integers from 1 through 100. All long-lived
Gateway cycles execute in one Node process against one post-warmup resource
baseline, so increasing the value extends leak detection instead of resetting
RSS, descriptor, subscriber, and active-resource measurements. The Node and
Rust surface suites still repeat once per iteration. The runner exits on the
first failed command, gives each child command a 10 minute execution deadline,
and prints a final JSON summary only after every iteration passes. At the
deadline it signals the entire POSIX process group (or the Windows process tree),
waits five seconds for graceful termination, then uses `SIGKILL` or
`taskkill /T /F` and waits at most another five seconds. Descendant test
processes therefore cannot survive a timed-out gate. Override the execution
deadline within 30 seconds through 60 minutes with
`CMCLIENT_LOAD_COMMAND_TIMEOUT_MS`; the two bounded termination windows are
additional to that deadline.

| Surface | Workload and invariant |
| --- | --- |
| Domain events | 50,000 publications retain only the configured replay window; payload, frame, listener, and subscriber counts remain bounded |
| Event client | 10,000 coalesced frames parse successfully; an unterminated frame fails at 60 KiB |
| Jobs | Executions respect the configured concurrency and queue caps, drain FIFO, and cancel queued work without consuming a slot |
| SQLite maintenance | More than 2,000 expired telemetry/Job rows drain in bounded batches while retained rows and integrity remain valid |
| Position/APRS | Backlog-to-live duplicate recovery is order independent; 32 start/stop cycles close every monitor session and suppress late work |
| Meshtastic transports | Repeated blackholed TCP and Serial attempts retain at most one active socket or native open; timeout/backoff cycles plateau, stop prevents later retries, and an unresolved Serial open fails teardown closed until a retry confirms cleanup |
| TCP Proxy | 512 KiB malformed input resynchronizes linearly; a 256-frame burst serializes; global queue and per-client backpressure isolate excess clients |
| Agent Control/Web | Active connections have fixed caps, excess clients receive stable errors, shutdown releases stalled sockets, and repeated Control receiver drops terminate half-open Gateway SSE bridge threads and loopback sockets |
| Gateway shutdown | Repeated requests share one cleanup promise; failures are isolated while all producer, Job, and SQLite phases still run |
| Supervisor | A real crashing child restarts without control requests, observes monotonic backoff deadlines, and is reaped on stop/drop |
| Long-lived Gateway | One real Fastify process spans every requested soak iteration while cycling SSE clients, paced event bursts, and HTTP requests; every client must receive the final burst event before explicit close, subscribers must then return to zero, the replay buffer must grow only to its fixed 256-event capacity, RSS may grow at most 64 MiB, Node active resources and platform file descriptors at most 8, and event-loop p99 at most 500 ms |

The runtime limits can be tightened for a release host with the
`CMCLIENT_RUNTIME_SOAK_*` variables documented by
`node scripts/runtime-soak.mjs --describe`. These focused files are also part of
the normal Vitest and Cargo suites. The runner provides a single operational
command and repeated soak mode without creating a separate implementation path.
On platforms without `/proc/self/fd` or `/dev/fd`, the mandatory Node
active-resource plateau is the handle-leak fallback and the JSON summary labels
that mode explicitly. CI runs the complete `pnpm test:load` gate, not only its
contract check.
