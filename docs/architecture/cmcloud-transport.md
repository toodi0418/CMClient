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
when a credential is supplied through the environment, or when legacy APRS or
Proxy enablement is requested. Legacy mode remains explicit by leaving
`CMCLIENT_CMCLOUD_MODE` disabled.

## Direct APRS Egress

CMCloud-required mode does not start the legacy mapping-driven APRS runtime.
It may instead maintain one narrow APRS-IS egress solely for a CMCloud-selected
dispatch. A client receives that capability only in `server_hello`:

```json
{
  "directAprs": { "callsign": "BM5GSV-5", "verified": true }
}
```

CMCloud emits this optional field only after an administrator has verified the
station's callsign claim. Its absence, `shadow`/`disabled` APRS policy, a failed
APRS-IS login, or a disconnected socket all force `directAprsReady: false` on
the next heartbeat. CMClient has no environment setting for an APRS callsign or
passcode in this mode; a static identity remains rejected. The standard
APRS-IS endpoint settings may be used, and
`CMCLIENT_CMCLOUD_DIRECT_APRS_ENABLED=false` explicitly disables this egress.

For `aprs_dispatch`, CMClient writes the supplied TNC2 `data` unchanged (other
than the required wire CRLF) exactly once. It returns `submitted` only after
the local socket write completes, `retryable_failure` when it can prove the
write never started, and `uncertain` for a write/connection outcome that cannot
be proven. An uncertain dispatch is never retried locally.

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

Re-pairing an active endpoint retains its installation ID and requests the
active installation generation; CMCloud advances that installation rather than
creating a fresh raw lane at sequence zero. The Agent performs this local
transaction preflight while an already-active Gateway remains running. An
unissued transaction may be replaced by a different pairing code, but an issued
credential is retained for recovery and returns
`CMCLOUD_ENROLLMENT_RECOVERY_REQUIRED` instead of being discarded. Once the
Agent sends a new pairing `client_hello`, CMCloud must keep the active device
credential and live session provisional until it receives the matching durable
`enrollment_ack`; the Agent cannot safely restore a credential that the server
has already revoked.
