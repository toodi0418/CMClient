# Meshtastic Transport Boundary

The Gateway has one `MeshtasticTransport` boundary for TCP, Serial, and the
deterministic simulator. It exposes framed bytes, metrics, stable error codes,
and connection state; domain decoders do not read sockets or serial ports.

The state machine is `disconnected -> connecting -> configuring -> ready`.
Transport failures can enter `degraded` or `backoff`; entering either requires a
stable reason code, and `backoff` additionally requires a positive retry
attempt. A user stop may return any state to `disconnected`. Invalid phase skips
fail closed with `TRANSPORT_STATE_TRANSITION_INVALID`.

P04-T02 supplies TCP framing, configuration session, and reconnect policy;
P04-T03 supplies Serial; P04-T04 consumes the shared frame boundary for locked
protobuf decoding and normalization.

TCP frames use the Meshtastic `0x94c3` header followed by a big-endian 16-bit
payload length. The decoder accepts fragmented and coalesced streams, discards
malformed input until it can resynchronize, and enforces a bounded payload
size. Configuration is a nonce-correlated session behind a codec interface:
the TCP layer never hard-codes protobuf field numbers. P04-T04 supplies the
version-locked protobuf codec.

TCP reconnect delay uses bounded exponential backoff with injectable jitter.
Every failed attempt enters `backoff` with a stable reason and retry number;
the first matching config-complete response promotes the connection to `ready`.
A bounded config-session timeout fails as `TCP_CONFIG_TIMEOUT` and reconnects;
retry escalation resets only after the configuration session reaches `ready`.
