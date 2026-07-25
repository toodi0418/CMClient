# Domain Projections

These endpoints are read-only projections. They expose bounded, schema-backed
records and stable error codes; they do not expose raw packet bytes, APRS Data,
credentials, or Gateway-specific routing metadata.

## Runtime projections

| Endpoint | Success shape | Unavailable/error |
| --- | --- | --- |
| `GET /api/v1/meshtastic` | `MeshtasticRuntimeStatus` with `configured`, connection state, network, and metrics | `200 {"configured":false}` when no runtime is configured |
| `GET /api/v1/aprs` | `AprsRuntimeStatus`: configured/running, monitor status, mapped callsigns, pending/failed outbox, optional error code | `200` with a safe stopped/unconfigured projection when absent |
| `GET /api/v1/callmesh` | `CallMeshOverview`: sync status and at most 200 validated mappings | `CALLMESH_CLIENT_UNAVAILABLE` / `503` when the client is absent |
| `GET /api/v1/proxy` | `ProxyStatus`: listener, policy, queue, upstream, and at most 50 redacted audit entries | `PROXY_RUNTIME_UNAVAILABLE` / `503` when the runtime is absent |

The Proxy projection contains fingerprints, not client addresses or raw
payloads. APRS status never contains the deterministic APRS Data line.

## Mesh lists

`GET /api/v1/nodes?limit=100` returns `{items: MeshNode[]}`. Each node includes
schema version, `meshNetworkId`, `nodeNum`, first/last observation times,
observation ID, and optional user/name/model/role fields.

`GET /api/v1/messages?limit=100` returns `{items: MeshMessage[]}`. Message text
is bounded to 512 characters and is associated with the observation ID,
network, sender, optional destination/packet/channel, and trusted observation
time.

`GET /api/v1/telemetry` returns `{items: MeshTelemetry[]}`. Optional filters
are:

```text
?limit=100&meshNetworkId=mesh-a&nodeNum=42&metricKind=deviceMetrics&from=2026-07-19T00:00:00.000Z&to=2026-07-19T01:00:00.000Z
```

`nodeNum` without `meshNetworkId` or `from > to` returns
`TELEMETRY_RANGE_INVALID` with HTTP `400`. A malformed timestamp or invalid
field type fails Fastify request-schema validation before the range handler.
Metrics are bounded strings, numbers, or booleans; telemetry time is not
silently substituted for event time.

When the domain read adapter is absent, nodes, messages, telemetry, positions,
and APRS outbox routes return `GATEWAY_DOMAIN_DATA_UNAVAILABLE` with HTTP
`503`. This is distinct from an unconfigured Meshtastic or APRS runtime, which
has a useful safe `200` projection as shown above.

## Positions and APRS outbox

`GET /api/v1/positions?limit=100` returns canonical position events. A position
contains network/node identity, canonical key, source observation, payload hash,
trusted event-time source (`position_timestamp`, `position_time`, or `sequence`),
coordinates, optional speed/track/altitude, and creation time. Received time is
not a cross-iGate ordering key. Events that are duplicates, historical,
backlog, imprecise, or unverifiable remain decisions rather than being silently
uploaded.

`GET /api/v1/aprs/outbox?limit=100` returns entries with callsign, canonical
event ID, transport `status` (`queued|sending|sent|failed`), observer delivery
`deliveryStatus`
(`queued|sending|failed|submitted|observer_confirmed|observation_expired`),
attempt count, bounded lifecycle timestamps, and stable last-error code. A
transport status of `sent` means only that the socket write completed. Only
`observer_confirmed` proves that the receive monitor later saw an exact source,
destination, and information match. `observation_expired` is terminal and does
not create a delivery high-water proof. The projection never returns APRS Data.
APRS upload requires `precisionBits === 32`, valid coordinates, and provably
new position state.

`GET /api/v1/aprs/station-submissions?limit=100` returns the station packet
kind, durable delivery state, and bounded lifecycle timestamps. Its state is
`sending|transmission_uncertain|submitted|observer_confirmed|observation_expired`.
`sending` is a durable pre-I/O intent. `transmission_uncertain` means the local
write outcome could not be proven and is held for exact observer
reconciliation instead of being resent blindly. The projection excludes
callsign, destination, information field, provision fingerprint, coordinates,
and comment.

## CallMesh mapping

`CallMeshOverview.status` is an object containing `state` (`unavailable`,
`checking`, `ready`, or `degraded`), `updatedAt`, optional `reasonCode`, optional
`activeMappingVersion`, optional `activeMappingHash`, `activeMappingCount`,
`provisionState`, and optional `lastServerTime`. The separate `mappings` array
contains validated version, effective time, network, node, and callsign values.
The API key and provision identity/configuration are never part of this
response. Invalid schema, credentials, stale revisions, or mapping conflicts
make the set ineligible for downstream use while retaining the last durable
high-water needed to reject a later downgrade.

## Events snapshot

`GET /api/v1/events/recent?limit=100` returns `{items: DomainEvent[]}` from the
process-local bounded replay buffer, newest first. It accepts 1 through 200
items (default 100), cannot recover events that have left the buffer or survive
a Gateway restart, and is only a refresh aid for consumers that missed the
live stream. See [events.md](./events.md) for replay semantics.
