# Meshtastic Observation Times

Every decoded `FromRadio` frame is recorded as a transport observation before
the position domain decides whether it represents a canonical source event.
The persisted `MeshObservation` contract keeps these values separate:

- `sessionConnectedAt` is when the transport completed its correlated config
  session and entered `ready`.
- `ingestedAt` is when CMClient read the frame from its local transport API.
- `serverIngestedAt` is when the Gateway accepted the normalized observation.
- `deviceRxTimeSeconds` is `MeshPacket.rx_time`, a local device observation.

`deviceRxTimeSeconds` is kept in both the normalized payload and an indexed
SQLite column. It is never copied into a position event timestamp and must not
be used for cross-iGate ordering.

Backlog classification compares the device observation's one-second interval
with the transport session boundary. A definitely pre-session interval is
`backlog`, one starting at or after the boundary is `live`, and a missing or
straddling value is `unknown`. This classification is only observation
metadata; later position acceptance still fails closed when source event
newness cannot be proved.
