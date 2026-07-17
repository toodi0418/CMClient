# Deterministic APRS Position Encoding

The position encoder accepts only a canonical position event and an explicit
mapping-derived source callsign, destination, symbol, and optional comment. It
does not accept gateway ID, transport, receive time, RSSI, SNR, hop count, or
APRS path, so those local observations cannot affect APRS Data.

It emits a fixed `SOURCE>DEST:/DDHHMMz...` line using UTC source event time,
uncompressed hundredths-minute coordinates, explicit MSL meters-to-feet
altitude, and paired course/knots. MSL zero is encoded; HAE is never used as a
substitute. Missing speed or track omits the pair rather than creating
`000/000`.

Each line ends with `CM2/<canonical-key-prefix>`. The marker is derived only
from the canonical event key and lets APRS monitoring reconcile remote uploads
without a central election. Golden tests lock the exact byte sequence.
