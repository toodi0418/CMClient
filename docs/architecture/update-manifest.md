# Update Manifest Trust Boundary

The Agent verifies signed manifest schema v2 before an update job enters
`downloading`. It validates the unified release identity, strict SemVer values,
UTC timestamp, exact native target uniqueness, HTTPS transport, SHA-256 shape,
and bundle size before opening a network stream.

One configured Ed25519 public key is selected locally by its known identifier.
Remote metadata cannot redirect verification to an arbitrary key. Signing
private keys never enter Control, CLI input, a job payload, diagnostics, or a
runtime log.

The signed bytes are compact `serde_json` serialization of `UpdateManifest` in
protocol field order. The Rust Agent is the verifier/executor and
`@cmclient/contracts` mirrors the same public wire; the shared golden identity
fixture guards cross-language drift. The manifest has no product-component
selector. Bundle selection is by one exact native `ProductTarget`, while Docker
OCI is intentionally unavailable to this updater.

The selected archive streams to Agent-owned staging, is checked against the
authenticated size and SHA-256, and is retained only after both checks pass.
The production client follows no redirects and rejects status, length,
underflow, overflow, traversal, or extraction-limit failures. A staged archive
is revalidated before reuse and before the runtime is stopped.
