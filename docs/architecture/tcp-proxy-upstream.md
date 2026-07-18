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
