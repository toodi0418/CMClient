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
size. Resynchronization scans each input byte once and retains only the bounded
incomplete tail, so a large malformed chunk cannot trigger quadratic copying
or unbounded retained memory. Decoder diagnostics count scan steps, copied
bytes, and copy operations so the load test enforces that property without a
wall-clock assertion. Configuration is a nonce-correlated session behind a
codec interface: the TCP layer never hard-codes protobuf field numbers. P04-T04
supplies the version-locked protobuf codec.

TCP reconnect delay uses bounded exponential backoff with injectable jitter.
Every failed attempt enters `backoff` with a stable reason and retry number;
the first matching config-complete response promotes the connection to `ready`.
A TCP connection attempt has a 10-second default deadline and a configurable
120-second maximum. Expiry destroys the pending socket, reports
`TCP_CONNECT_TIMEOUT`, and enters the same bounded backoff path.
A bounded config-session timeout fails as `TCP_CONFIG_TIMEOUT` and reconnects;
retry escalation resets only after the configuration session reaches `ready`.

Serial uses the same transport, frame, configuration, and reconnect contracts
through a `SerialPortAdapter`. The native adapter is the only module that
imports `serialport`; it exposes deterministic `listSerialDevices()` records
and opens/drains a selected device. Device open and I/O failures enter the same
bounded backoff lifecycle with `SERIAL_*` reason codes. Serial open uses the
same 10-second default and 120-second maximum. `SERIAL_OPEN_TIMEOUT` aborts the
adapter operation, retains at most one in-flight native open, and closes a late
handle before any requested retry. Disconnect does not wait indefinitely for an
adapter that ignores cancellation, but it fails closed with
`SERIAL_DISCONNECT_PENDING_OPEN` and does not report `disconnected` until a
later stop confirms that the native open and any late handle are gone.

`MeshGatewayRuntime` binds subscriptions and pending connect work to a lifecycle
generation. Stop invalidates that generation, requests disconnect, waits for a
pending connect only within one 10-second deadline, and retries cleanup when a
connect raced stop or the first disconnect failed. A failed or timed-out stop
keeps teardown latched: later stop requests retry the bounded disconnect and
start remains rejected until the transport confirms `disconnected`. A
transient first disconnect error is cleared only when the in-deadline retry
succeeds, the pending connect has settled, and the transport confirms that
state. A late
connect belongs to the invalidated generation, cannot publish or restart work,
and must be closed by a successful retry. Serial disconnect rejects its pending
connect in a `finally` path even when device close fails, so shutdown cannot wait
forever on an orphaned connection promise.
