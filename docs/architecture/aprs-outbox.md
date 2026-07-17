# APRS-IS Outbox Delivery

APRS uploads are first represented as a durable SQLite `aprs_outbox` entry.
Its identity is `(callsign, canonical_event_id)`, so retries, duplicate local
observations, and repeated command handling cannot create a second upload for
the same mapped callsign and canonical source event. The originally encoded
APRS Data is retained; a repeated enqueue never replaces it with local data.

The worker atomically claims due queued or failed entries, transmits each
single APRS Data line through an APRS-IS TCP connection, and records either a
sent timestamp or the stable `APRS_TX_FAILED` error with exponential backoff.
An entry left in `sending` by an interrupted process is returned to `failed`
on the next worker flush with `APRS_TX_INTERRUPTED`, then retried. This makes
crash recovery explicit without treating a local write as a confirmed upload.

The client writes exactly two CRLF-terminated lines per connection: the
configured APRS-IS login followed by the deterministic APRS Data line. Both
inputs reject CR/LF injection. Gateway-specific path information and every
local observation attribute stay outside the stored Data and never alter the
canonical APRS payload.
