# Legacy Characterization Baseline

## Purpose

CMClient 2.0 does not preserve the Legacy implementation. It preserves the
behaviour identified in the migration matrix through independent, repeatable
tests. `test/fixtures/sanitized-packets.json` is a synthetic seed fixture: it
contains no recorded production frame, credential, real callsign, or personal
location.

Run the current fixture contract with:

```bash
npm run test:fixtures
```

## Fixture Rules

- Every fixture must declare `sanitized: true` and use a `fixture-*` gateway,
  `fixture-network-*` network, and synthetic Mesh node ID.
- Raw frames in this initial set are deliberately marked `synthetic-hex`; they
  are stable recorder/sanitizer inputs, not claims of a real protobuf capture.
- `PacketFixtureSanitizer` is the only recorder export path. It replaces raw
  frames and payload bytes with synthetic data, aliases identifiers, and shifts
  the observation timeline together before a fixture can be retained.
- A fixture must retain observation time, ingest time, session time, and
  server ingest time, and transport independently. No test may reinterpret
  local receive time as a global source event time.

## Characterization Matrix

| Area | Legacy evidence | Required 2.0 characterization | Target phase |
| --- | --- | --- | --- |
| TCP framing | `src/meshtasticClient.js` | Fragmented/combined frame decoding, bounded size, config session, reconnect/backoff, and backlog classification | P04 |
| Serial transport | CLI/Electron serial settings | Device enumeration, serial config, shared normalized output, failure/reconnect behaviour | P04 |
| Protobuf compatibility | `proto/meshtastic/` | Version lock and schema compatibility against sanitized fixture corpus | P04 |
| Packet normalization | Legacy packet summaries | Preserve transport, receive, ingest, session, packet ID, sender, port, payload hash, and decode errors | P04 |
| Nodes | `nodeDatabase.js` and dashboards | Node merge/identity, history, bounded registry, API/SSE projection | P04/P06 |
| Messages | Legacy text summaries/history | Text decode, persistence, ordering, API/SSE delivery, and UI empty/error states | P04/P06 |
| Telemetry | `telemetryDatabase.js` and charts | Metric normalization, persistence, retention, range query, chart/table presentation | P04/P06 |
| Position acceptance | Legacy APRS bridge | GPS event time, sequence epoch, duplicate identity, precision 32, altitude 0, paired speed/course | P05 |
| Position safety | Legacy anti-backtrack logic | Same-event multi-iGate, old backlog, missing/invalid/future clock, sequence conflict/wrap/reboot, cold start fail-closed | P05 |
| APRS encoding | Legacy APRS client/bridge | Byte-identical Data for same canonical event; gateway metadata excluded from Data; outbox idempotency | P05 |
| APRS monitor | Legacy feed cache | Exact duplicate handling and remote high-water update without central arbitration | P05 |
| CallMesh | `src/callmesh/` | Valid/invalid key, timeout/retry/backoff, mapping version/effective time/conflict, redacted observability | P03/P06 |
| Web events | Raw dashboard server | API contract, SSE IDs/replay/heartbeat, reconnect, slow consumer, client de-duplication | P03/P06 |
| Desktop and CLI | Electron and yargs surfaces | Agent-only control, stable status/actions/exit codes/JSON, desktop smoke and single-instance behaviour | P02/P08 |
| Proxy | No valid legacy equivalent | Multi-client framed manager, ACK routing, modes, limits, audit, and backpressure | P07 |
| Removed features | TENMAN/TENMAP and `@cm` code/docs | Repository scan proves no code, environment variable, database, UI, fixture, or documentation compatibility path remains | P11 |
| Release and migration | Legacy scripts/data paths | Clean install, import dry run, backup/restore, signed update/rollback, service and Docker smoke | P09-P12 |

## Position Replay Cases Required Before Release

The P05/P12 replay suites must cover in-order events, exact duplicates, packet
ID reuse with a different payload, two iGates receiving one event, old event
arriving after a newer event, a disconnected APRS monitor, API backlog,
missing/invalid/future timestamps, same-second events, sequence wrap/reboot,
mapping conflict, precision below 32, partial speed/course, altitude zero, and
clock/sequence disagreement. Any uncertainty about newness must produce no
APRS upload.
