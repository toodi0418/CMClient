# Signed Update Manifest Contract

The Rust Agent is the only component that authenticates, downloads, and stages
a native update. Other modes display its bounded status; they do not select a
key or download an unverified archive.

```json
{
  "manifest": {
    "schemaVersion": 2,
    "release": {
      "schemaVersion": 1,
      "product": "CMClient",
      "version": "2.0.1",
      "sourceCommit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "sourceTree": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "channel": "stable"
    },
    "publishedAt": "2026-07-18T02:40:00.000Z",
    "minimumAgentVersion": "2.0.0",
    "bundles": [
      {
        "target": {
          "os": "macos",
          "architecture": "universal",
          "profile": "native",
          "packageProfile": "dmg"
        },
        "archive": "tar.zst",
        "url": "https://releases.example.invalid/cmclient/2.0.1/macos-universal.tar.zst",
        "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "sizeBytes": 4096
      }
    ]
  },
  "signingKeyId": "release-2026",
  "signatureAlgorithm": "ed25519",
  "signature": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
}
```

Compact UTF-8 `manifest` JSON in declared field order is the Ed25519 signed
payload. `signingKeyId` must equal the locally selected trusted key identifier.
All fields are mandatory and reject unknown properties. The release identity,
SemVer values, UTC millisecond timestamp, HTTPS URL, lowercase SHA-256, nonzero
size, and unique native distribution target are validated before networking.

There is no component selector. `desktop`, `headless`, `cli`, and `service`,
manifest schema v1, and the old `beta` channel all fail deserialization or
validation. A bundle targets the one installed CMClient product. Docker targets
are also rejected because Docker update is the operator-owned pull/recreate
workflow, never native Agent staging.
