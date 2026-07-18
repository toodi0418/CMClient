# Update Manifest Trust Boundary

The Agent owns the manifest verification boundary before an update job enters
`downloading`. It validates the protocol schema, strict SemVer values, UTC
timestamp, exact component/target uniqueness, HTTPS transport, SHA-256 shape,
and bundle size before it asks a downloader to fetch anything.

An update uses one configured, trusted Ed25519 public key selected by its known
key identifier. The remote key identifier is compared to that local selection;
it cannot redirect verification to an arbitrary key. Signing private keys stay
outside the product runtime and must never be supplied through a Control API,
CLI option, job payload, diagnostic bundle, or application log.

The signed bytes are `serde_json` compact serialization of `UpdateManifest` in
the protocol field order. This is intentionally defined by the Rust Agent,
which is the verifier and executor. `@cmclient/contracts` mirrors the public
wire shape for clients, while duplicate component/target checks and Ed25519
verification remain Agent-only trust decisions.

P09-T02 consumes the selected bundle by streaming it to staging, verifying its
SHA-256 against the authenticated manifest, and retaining it only after both
checks complete.
