# Deterministic APRS Position Encoding

The position encoder accepts only a canonical position event and an explicit
mapping-derived source callsign, destination, symbol, and optional comment. It
does not accept gateway ID, transport, receive time, RSSI, SNR, hop count, or
APRS path, so those local observations cannot affect APRS Data.

It emits the Legacy-compatible untimestamped
`SOURCE>APTMAG,MESHD*,qAO,IGATE:!...` line, with uncompressed
hundredths-minute coordinates and paired course/knots. It never includes a
gateway name, receive time, RSSI, SNR, hop count, or CMClient marker. Missing
speed or track omits the pair rather than creating `000/000`.

The optional `/A=xxxxxx` field is placed before the free-form comment and is
expressed in rounded feet. Its altitude selection order is explicit MSL
(including zero), HAE minus geoidal separation when both source fields are
present, then HAE-only as a Legacy/Meshtastic compatibility fallback, followed
by mapping or self-provision altitude. HAE-only data is retained because older
CMClient uploads use that Meshtastic source convention; it is not a substitute
for a measured MSL reference. Negative values use the widely supported
six-character `/A=-12345` form. Golden tests lock the exact byte sequence.
