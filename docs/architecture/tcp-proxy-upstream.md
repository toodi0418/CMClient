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

This boundary has no client sockets, broadcast policy, request/ACK routing, or
outbound queue. Those responsibilities are added in the later P07 tasks so a
single upstream cannot regress into a raw bidirectional socket pipe.

`ProxySessionManager` attaches bounded local client sinks to that upstream. A
new client receives copied cache frames before live broadcast frames. Every
client has its own FIFO and byte/frame limits; an in-flight write remains part
of the limit, and only the slow client is closed with
`PROXY_CLIENT_BACKPRESSURE`. This manager still has no outbound writer or ACK
authority; P07-T03 adds those serialized request paths.
