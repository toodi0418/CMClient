# Meshtastic Protobuf Compatibility

Gateway loads `meshtastic.FromRadio`, `ToRadio`, and `PortNum` from the tracked
`proto/meshtastic` corpus through protobufjs. The corpus is locked by a sorted
filename/content SHA-256 in `schema.ts`; a schema update must intentionally
update that value and the compatibility fixture.

The config session codec serializes `wantConfigId` and verifies
`configCompleteId` by decoding the schema, not by hand-maintained wire field
numbers. `normalizeFromRadio` converts a decoded packet into the versioned
`NormalizedFromRadio` contract. Device `rxTime` is named
`deviceRxTimeSeconds`: it remains a local observation and is never used as a
cross-iGate source event time.
