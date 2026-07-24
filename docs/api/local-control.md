# Local Agent Control IPC

The authoritative Agent Control endpoint is private local IPC, not an HTTP API
and not the public Gateway Business API. On macOS, Linux, and Docker it is the
mode-`0600` socket `~/.cmclient/run/control.sock` (under
`/home/cmclient/.cmclient/run` in Docker). The owning root and run directory are
mode `0700`. On Windows it is an `interprocess` local named pipe whose name
contains a SHA-256 digest of the canonical `~/.cmclient` state root, so two
different roots cannot collide. Windows rejects remote named-pipe clients. The
`interprocess` listener keeps `accept_remote` disabled, which maps to
`PIPE_REJECT_REMOTE_CLIENTS`.

Control never falls back to TCP, HTTP, HTTPS, or a Management Web bridge. It
has no bearer token, HMAC credential, persisted Control credential, or
`CMCLIENT_CONTROL_TOKEN` environment path. CLI and Desktop connect only to the
current user's endpoint and never open `~/.cmclient/secrets.json`. The Windows
current-user product claims remote-client rejection, not isolation between two
accounts on the same host; it does not use SID impersonation, a custom ACL,
unsafe FFI, PID identity, or UAC. After an ambiguous nonblocking zero-byte read,
Windows handles this with a zero-length write on the underlying local named
pipe. The write is solely a liveness probe and emits no protocol bytes.
Peer PID is not consulted for liveness, authentication, identity, or
authorization, and neither probe result changes command behavior.

## Wire contract

`interprocess::local_socket` carries byte streams. CMClient frames those bytes
with `tokio_util::codec::LengthDelimitedCodec`: a four-byte big-endian length
prefix followed by one bounded Serde JSON envelope. `CONTROL_PROTOCOL_VERSION`
identifies the envelope contract and `MAX_CONTROL_FRAME_BYTES` fixes its maximum
size. A peer must send exactly one complete request envelope before it receives
a response or subscription event. There is no HTTP request line, header block,
query string, status code, chunking, or SSE text in this protocol.

Every request contains the protocol version, a fresh request ID, and one typed
operation. Every response repeats the version and request ID and contains
either the operation's typed result or one stable `CONTROL_*` error code.
Subscription events carry their own typed envelope and retain the subscribing
request ID. Unknown versions, unknown operations, mismatched request IDs,
malformed JSON, trailing data, oversized frames, and oversized serialized
results fail closed without reflecting parser text or payload data.

The server checks for trailing available bytes before dispatch. Any already
available byte after the first complete request, including an incomplete second
frame, rejects the entire request; the first operation is not executed. This is
a fail-closed framing check, not permission for a second request on the same
stream.

The frame limit, connection limit, and read/write deadlines are fixed by the
Control contract. A length prefix over the limit is rejected before allocation.
A slow partial frame, silent peer, or incomplete response reaches the bounded
deadline; disconnects release their server slot and terminate any associated
Gateway bridge. The configured client timeout starts before local connect and
bounds connect and request setup, including the request write and initial
response or subscription acceptance.

ControlServer shutdown cancels and joins every active request or subscription
stream before the endpoint can be reused. No detached stream survives the
server, so a restart invalidates existing streams before the same root resolves
to and rebinds the same deterministic endpoint.

On Unix, an existing socket path is probed before binding. A successful probe
means the endpoint is in use; only a probe that fails with `ConnectionRefused`
allows removal as a stale socket. All other probe failures fail closed and
leave the path intact.

## Operations

The request operation, rather than an HTTP method/path pair, selects behavior:

| Operation | Result |
| --- | --- |
| `Status` | Agent, Gateway, Management Web, identity, uptime, and stable error status |
| `Start` | Start Gateway and return updated lifecycle status |
| `Stop` | Stop Gateway and return updated lifecycle status |
| `Restart` | Restart Gateway and return updated lifecycle status |
| `ShutdownAgent` | Terminal local Agent teardown status |
| `EnableManagementWeb` | Enable the optional Web listener and return updated lifecycle status |
| `DisableManagementWeb` | Disable the optional Web listener and return updated lifecycle status |
| `UpdateStatus` | Safe persistent update-job projection |
| `SubscribeUpdateEvents` | Initial update snapshot followed by typed update events |
| `DiagnosticsBundle` | Sanitized Agent/runtime allowlist |
| `GatewayProjection` | Meshtastic, nodes, positions, APRS, CallMesh, Proxy, recent events, integrity Job, or backup Job projection |
| `SubscribeGatewayEvents` | Typed events bridged from the bounded Gateway stream |
| `StoreSecret` | Store the bounded CallMesh API key and return only `stored: true` |
| `RemoveSecret` | Remove a supported or legacy-removal secret kind |

Lifecycle status schema v3 returns component identity/state, Gateway lifecycle,
Management Web listener state and its URL only when running, uptime, and the
latest stable error code. The identity contains the exact shared CMClient
version, commit, tree/content digest, channel, and target. The obsolete
`agentVersion`-only schema v2 shape is rejected. Management Web enable/disable
controls only the optional Web listener; local Control remains available.

`ShutdownAgent` is reserved for local IPC and the Windows Service Host. It
requests one terminal teardown and is never available from Management Web or a
network listener. Agent commits the shutdown request only after the typed
success response has been written, so Service Host can observe the graceful
acknowledgement before Control teardown. Agent stops the supervisor worker, cooperatively drains
Gateway, and closes Management Web before exiting. Once teardown begins,
resource-starting operations fail with `CONTROL_COMMAND_FAILED`; status and
resource-draining operations remain safe while teardown completes.

Gateway projections are Agent-owned bridges. Agent calls its private loopback
Gateway with bounded timeouts and returns schema-backed JSON or a stable Control
error. Recent events use Gateway's bounded default snapshot. Integrity and
backup return accepted persistent Jobs rather than doing database work in CLI
or Desktop. A Gateway event subscription uses a fixed downstream queue, closes
one slow subscriber without blocking Gateway, remembers the last successfully
forwarded event ID for bounded Gateway replay, and adds Agent heartbeat events.
Replay remains process-local and cannot recover events older than Gateway's
bounded buffer or survive a Gateway restart.

`UpdateStatus` returns the Agent-owned persistent update job or `job: null`.
Its safe projection includes phase, update time, optional transfer progress,
error code, and bounded stable log codes. It never returns manifest URLs,
signing material, archive paths, server text, or user configuration.
`SubscribeUpdateEvents` sends an immediate typed status snapshot and future
state transitions. The durable journal remains authoritative, so a reconnecting
client first reads `UpdateStatus` and then opens a new subscription.

`DiagnosticsBundle` returns a JSON allowlist of Agent and runtime state. It may
contain stable error/log codes but never paths, configuration, environment,
database content, packet data, credentials, or log payloads.

## Secret boundary

Secret operations never pass through Gateway. `StoreSecret` accepts one
bounded UTF-8 value without control characters. `callmesh-api-key` is the only
settable runtime kind. `aprs-passcode` and `management-admin-token` are
removal-only compatibility names: attempts to store them return
`CONTROL_SECRET_KIND_DEPRECATED` without dispatching the value, while removal
deletes any value left by an older installation.

Agent is the only owner of secret persistence. It atomically writes the sole
backend, root-level `~/.cmclient/secrets.json` (or
`/home/cmclient/.cmclient/secrets.json` in Docker). POSIX uses `0700` for the
root and `0600` for the file; Windows uses an ordinary current-user file without
a cross-principal security claim. There is no Keychain, Credential
Manager/DPAPI, Secret Service, systemd vault, or secret-bearing argv/environment
path.

CLI reads a new CallMesh key from standard input and sends it in one bounded
local request. The value is never returned. Agent transfers the key to a
supervised Gateway only through its private inherited bootstrap channel. Local
endpoint access and bounded typed frames are the complete command-mode
authorization boundary.
