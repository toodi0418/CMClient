# TCP Proxy Upstream Boundary

`ProxyUpstreamManager` owns one `MeshtasticTransport` and never hands that
socket to a local client. `ProxyFrameCodec` applies the bounded Meshtastic
length-delimited codec at the proxy boundary, so fragmented/coalesced data is
handled before any future client session or writer path sees it.

The in-memory config cache stores only replay-safe `FromRadio` configuration
fragments: MyNodeInfo, NodeInfo, Config, ModuleConfig, Channel, and metadata.
It excludes packets, logs, config-complete nonces, and unknown variants. Cache
entries are copied, bounded, keyed by their protocol identity where available,
and cleared whenever the upstream begins a new config session or stops.

This boundary has no client sockets or authorization policy. Those
responsibilities are added in the later P07 tasks so a single upstream cannot
regress into a raw bidirectional socket pipe.

`ProxySessionManager` attaches bounded local client sinks to that upstream. A
new client receives copied cache frames before live broadcast frames. Every
client has its own FIFO and byte/frame limits; an in-flight write remains part
of the limit, and only the slow client is closed with
`PROXY_CLIENT_BACKPRESSURE`. Defaults allow 1,024 frames and 1 MiB per client,
which covers the maximum 512-frame config snapshot plus live headroom. A custom
limit that cannot replay the snapshot rejects that attachment with
`PROXY_CONFIG_REPLAY_BACKPRESSURE` instead of reporting a closed client as
connected.

`ProxyOutboundRouter` is the sole writer path. It validates bounded `ToRadio`
payloads, serializes writes, and copies bytes before calling the one upstream
manager. It tracks these protocol correlations after a write is accepted:

- `wantConfigId` to `configCompleteId`;
- a response-requesting `MeshPacket.id` to `Data.replyId` (including client
  notifications);
- a reliable `MeshPacket.id` to an incoming `Data.requestId` ACK/NAK.

Correlation IDs are globally reserved while pending, bounded, and expire with
stable codes. The session manager invokes the router before its regular
broadcast path: a matched frame is delivered only to its owning client; an
unmatched frame remains a normal live broadcast.

`ProxyAccessController` is the policy layer for the future TCP listener. It
defaults to a `127.0.0.1` bind and `monitor` mode. A non-loopback bind requires
an explicit `allowLan` flag plus a non-empty, numeric-IP exact allowlist; DNS
names are rejected. Admission is bounded by `maxClients`. `monitor` accepts no
outbound command, `message` accepts only decoded `TEXT_MESSAGE_APP` packets,
and `full` accepts the valid `ToRadio` variants controlled by the writer.
Every active client has an independent bounded one-minute write window.

Policy decisions emit a bounded audit trail and optional redacted structured
log. Audit entries contain only one-way client/address fingerprints, mode,
allowed command variant, and stable code: never a raw remote address, client
identifier, message text, or protobuf payload. `ProxyRuntime` owns listener
lifecycle and exposes its privacy-safe snapshot through `/api/v1/proxy`; its
`proxy.*` lifecycle, upstream, client, queue, and error events use the global
bounded SSE journal.

The TCP listener pauses each client socket while decoding and awaiting its
frames in order, then resumes it only after the shared writer accepts them.
The global writer queue is fixed at 128 entries by default. A client that
reaches that boundary is closed with `PROXY_OUTBOUND_QUEUE_FULL` and emits
redacted queue/backpressure events; other clients and the upstream session stay
active.

When `CMCLIENT_PROXY_ENABLED=true`, Gateway starts the listener only after a
TCP Meshtastic upstream has completed its config session. Required upstream
variables are `CMCLIENT_PROXY_UPSTREAM_HOST` and
`CMCLIENT_PROXY_UPSTREAM_PORT`; `CMCLIENT_PROXY_PORT` defaults to `4403` and
`CMCLIENT_PROXY_HOST` defaults to `127.0.0.1`. `CMCLIENT_PROXY_MODE` defaults
to `monitor`. A LAN listener additionally requires
`CMCLIENT_PROXY_ALLOW_LAN=true` and comma-separated numeric IP addresses in
`CMCLIENT_PROXY_ALLOWLIST`. Invalid configuration or upstream startup exits
with a stable proxy error code instead of leaving a half-started Gateway.
Start and stop are serialized by a lifecycle generation. A stop that races an
upstream connect invalidates the late start, drains it within one 10-second
deadline, disconnects any late-established upstream, and then closes listener,
session, and writer resources. Disconnect failure or timeout is reported while
the runtime still transitions to `stopped`; it cannot leave shutdown waiting on
an arbitrary transport promise.
