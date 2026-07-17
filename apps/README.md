# Applications

CMClient 2.0 deployable applications live here:

- `web`: Vue 3 management UI.
- `gateway`: Fastify domain runtime for Meshtastic, APRS, CallMesh, Proxy,
  persistence, jobs, and events.
- `desktop`: Tauri supervisor shell.
- `agent`: Rust Agent launcher integration.
- `cli`: Rust CLI launcher integration.

Each application owns its runtime entrypoint only. Shared domain contracts and
client libraries belong in `packages/` or `crates/`.
