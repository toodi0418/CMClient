# CMClient 2.0

This `dev` branch contains the CMClient 2.0 implementation. The product is
split into a Rust Agent and CLI, a Fastify Gateway, a Vue management Web app,
and a small Tauri Desktop supervisor. Runtime state and secrets are not stored
in this repository.

## Development

Required toolchains:

- Node.js `^22.18.0` or `>=24.11.0`
- pnpm 11 or newer
- the Rust toolchain pinned by `rust-toolchain.toml`

Install dependencies and run the repository gates:

```bash
pnpm install --frozen-lockfile
pnpm format:check
pnpm lint
pnpm typecheck
pnpm test
pnpm build
cargo fmt --all -- --check
cargo test --workspace
```

Run the repository and Node dependency security gates separately when changing
dependencies, workflows, or release composition:

```bash
pnpm audit:policy
pnpm audit:dependencies:node
```

The pinned secret and Rust dependency audit commands, current results, and the
only time-bounded advisory exception are recorded in the
[P12 release security audit](docs/security/release-audit.md).

Run the management Web end-to-end suite separately:

```bash
pnpm test:e2e:web
```

## Documentation

- [Architecture overview](docs/architecture/CMCLIENT_2_OVERVIEW.md)
- [Agent runtime](docs/architecture/agent-runtime.md)
- [Gateway runtime](docs/architecture/gateway-runtime.md)
- [Release artifacts](docs/architecture/release-artifacts.md)
- [Release security audit](docs/security/release-audit.md)
- [Feature parity evidence](docs/testing/feature-parity.md)

Release candidates, installation instructions, upgrade procedures, and final
release notes are completed by the P12 release gates before publication.
