# Meshtastic Domain Records

`MeshtasticApplicationDecoder` consumes the normalized `MeshPacket` contract
and supports only these explicit application ports:

- `NODEINFO_APP`: protobuf `User`, used to enrich node identity metadata.
- `TEXT_MESSAGE_APP`: strict UTF-8 text.
- `TELEMETRY_APP`: protobuf `Telemetry` with its active oneof metric variant.
- `POSITION_APP`: protobuf `Position`, normalized to the shared Position sample
  units with a SHA-256 payload identity before canonical processing.

Unsupported ports, missing identifiers, invalid UTF-8, missing telemetry or
Position fields, and out-of-range message channels return stable `MESH_*`
reason codes. They do not create partial domain records. Compressed text is not
silently guessed or decompressed by this layer.

## Persistence

Node registry identity is the composite key `(mesh_network_id, node_num)`.
This prevents observations from different Mesh networks sharing a numeric node
address from merging. A NodeInfo, message, or telemetry observation updates
`lastSeenAt` only when its CMClient `ingestedAt` is not older than the saved
value. Older observations cannot overwrite the current profile or move the
registry backwards.

Messages and telemetry retain the originating `observation_id`, use one record
per observation, and return the existing record during deterministic replay.
Telemetry keeps its device-supplied `time` separately as
`telemetryTimeSeconds`; it is not used as a position source event time or a
node registry ordering key. All `observedAt` values come from the separately
persisted CMClient API ingest timestamp.

Position payloads do not enter the Node/Message/Telemetry repositories. The
production runtime converts them to an observation and canonical event before
mapping, validation, high-water ordering, and APRS outbox decisions described
in [Gateway Production Runtime](./gateway-runtime.md).
