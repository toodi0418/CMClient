# Signed Update Manifest Contract

The Rust Agent is the only component that consumes and executes an update.
Gateway, Web, Desktop, and CLI may display or request an update job, but they
never download an unverified archive or select a signing key.

The release service returns this JSON document:

```json
{
  "manifest": {
    "schemaVersion": 1,
    "channel": "stable",
    "version": "2.0.1",
    "publishedAt": "2026-07-18T02:40:00.000Z",
    "minimumAgentVersion": "2.0.0",
    "bundles": [
      {
        "component": "desktop",
        "target": "darwin-aarch64",
        "archive": "tar.zst",
        "url": "https://releases.example.invalid/cmclient/2.0.1/darwin-aarch64.tar.zst",
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

`manifest` is serialized as compact UTF-8 JSON in the declared field order and
that exact byte sequence is signed with Ed25519. The signature does not cover
the outer metadata. The Agent accepts a document only when `signingKeyId`
equals its locally configured trusted key identifier and verification succeeds
with that corresponding public key.

All fields are mandatory and reject unknown properties. `version` and
`minimumAgentVersion` are strict SemVer. `publishedAt` is UTC with millisecond
precision. A bundle is HTTPS only, has one lowercase 64-character SHA-256
digest, a non-zero size, and is unique for its `(component, target)` pair.

| Field | Values |
| --- | --- |
| `channel` | `stable`, `beta`, `dev` |
| `component` | `desktop`, `headless`, `cli`, `service` |
| `target` | `darwin-aarch64`, `darwin-x86_64`, `linux-aarch64`, `linux-x86_64`, `windows-x86_64` |
| `archive` | `tar.zst`, `zip` |

Docker images are not in-place update bundles. They are published and upgraded
by their deployment tooling, never by Gateway or the Agent updater.
