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
