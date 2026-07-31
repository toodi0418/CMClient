# CMCloud Raw Transport

CMClient 2.0 can run in a CMCloud-authoritative mode. It has one upstream:
the authenticated Agent WebSocket at `wss://cmcloud.tmmarc.org/agent/v1` using
the `cmcloud.agent.v1` subprotocol. CMCloud, rather than a local CallMesh
mapping cache or local APRS runtime, owns mapping selection, duplicate
suppression, APRS eligibility, and dispatch.

## Activation

CMCloud mode is explicitly enabled with the following non-secret Gateway
configuration:

```text
CMCLIENT_CMCLOUD_MODE=required
CMCLIENT_CMCLOUD_URL=wss://cmcloud.tmmarc.org/agent/v1
CMCLIENT_CMCLOUD_INSTALLATION_ID=<uuid>
CMCLIENT_CMCLOUD_INSTALLATION_GENERATION=<non-negative integer>
CMCLIENT_CMCLOUD_CREDENTIAL_VERSION=<positive integer>
```

The device credential is never an environment variable, command-line argument,
browser value, log field, or SQLite field. The Rust Agent must inject it only
as `cmCloudDeviceCredential` in the bounded private Gateway bootstrap frame.
Cloud mode fails before transport startup when that private credential is absent,
when a credential is supplied through the environment, or when local APRS or
Proxy enablement is requested. Legacy mode remains explicit by leaving
`CMCLIENT_CMCLOUD_MODE` disabled.

## Delivery

Every inbound Meshtastic `FromRadio` frame is first copied byte-for-byte into
the SQLite `cmcloud_raw_outbox`. This occurs before protobuf decoding or local
position processing. Each row receives an immutable UUID and a monotonic
`live` lane sequence inside the same SQLite transaction. Sequence allocation
survives acknowledged-row retention, so a restart cannot reuse a sequence.

After `server_hello`, the Gateway sends one `CMC1` binary frame at a time. Its
header carries the stored message identity and lane sequence plus the current
connection epoch. The body is always the exact durable `FromRadio` byte string.
The row remains pending until CMCloud returns a matching `raw_ack` after its own
database commit. A reconnect replays the same stored identity, sequence, and
body with the new connection epoch; a `duplicate` acknowledgement completes
the row just like a new receipt.

Acknowledged rows are retained for seven days by default and then deleted in
bounded maintenance batches. Pending rows are never removed by retention.
`CMCLIENT_CMCLOUD_OUTBOX_RETENTION_DAYS` and
`CMCLIENT_CMCLOUD_OUTBOX_RETENTION_BATCH_SIZE` control that bounded cleanup.

## Failure Boundaries

Transient socket failures retain pending rows and reconnect with bounded
backoff. Credential rejection, protocol/version incompatibility, lane gaps,
session-epoch failures, and sequence reuse conflicts block the transport with a
stable error code instead of silently switching to local CallMesh or APRS
authority.

CMCloud pairing has an additional Agent-owned transaction: the Agent must
durably save an issued device credential before the Gateway sends
`enrollment_ack` on the same socket. Until that private Agent bridge is present,
the Gateway deliberately stops with `CMCLOUD_ENROLLMENT_REQUIRES_AGENT` rather
than retaining an issued credential only in Node process memory.
