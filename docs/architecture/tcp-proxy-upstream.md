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
`PROXY_CLIENT_BACKPRESSURE`.

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
unmatched frame remains a normal live broadcast. The P07-T04 policy layer will
decide which `ToRadio` variants each client mode may submit.
