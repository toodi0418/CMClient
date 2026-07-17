# Meshtastic Packet Recorder and Replay

The Gateway packet recorder keeps a bounded in-memory copy of raw frames for
local diagnostics. It copies every input frame and normalized observation, so a
transport buffer or caller cannot mutate a retained capture. The recorder never
logs or exports raw bytes directly.

Fixture export goes through `PacketFixtureSanitizer`. It creates a versioned,
synthetic-only fixture set with all required transport observation times:
session connected, frame received, CMClient API ingested, and Gateway server
ingested. A common shift moves the session/API timeline to a fixed fixture
epoch while preserving durations and device `rx_time` relationships.

Gateway IDs and Mesh network IDs receive deterministic fixture aliases. Node
and packet identifiers receive deterministic numeric aliases. Raw frames,
decoded payload bytes, and encrypted payload bytes are replaced with synthetic
values generated from the safe capture sequence only, never from original
content or a hash of it. This prevents exporting real text, location bytes, or
short secrets that could be recovered through a dictionary hash.

`replayPacketFixtures` sorts fixture entries by `serverIngestedAt` and fixture
ID, invokes one handler at a time, and hands the handler a clone. This gives
tests deterministic ordering without wall-clock delays or mutable shared
fixture state.
