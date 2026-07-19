# Applications

CMClient 2.0 deployable applications live here:

- `web`: Vue 3 management UI.
- `gateway`: Fastify domain runtime for Meshtastic, APRS, CallMesh, Proxy,
  persistence, jobs, and events.
- `desktop`: Tauri supervisor shell.
- `agent`: Rust Agent launcher integration.
- `cli`: Rust CLI launcher integration.
- `service-host`: Windows Service Host that locates and controls the adjacent
  Agent through the private Control API.

Each application owns its runtime entrypoint only. Shared domain contracts and
client libraries belong in `packages/` or `crates/`.
