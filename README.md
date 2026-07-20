# CMClient 2.0

CMClient is one product with graphical mode, command mode, the full management
Web, a Rust Agent, and a TypeScript Gateway. Agent and Gateway are internal
components, not separate products. The Repository is currently transitioning
the existing P12 implementation to this unified contract; documents marked as
historical describe the previous package model and must not be read as current
release choices.

Read [Documentation authority](docs/READ_ORDER.md) first.

## Target Product

- `cmclient` with no arguments opens graphical mode.
- `cmclient <command>` uses command mode through the resident Agent.
- `cmclient --background` starts the resident core without a window.
- The Web UI contains every setup and operational workflow. Graphical mode is a
  compact status/tray/control surface with an Open Web action.
- Native packages contain the private Node/Gateway runtime and need no system
  Node or npm. Docker contains everything except graphical mode.
- Mutable state is under `~/.cmclient`; `secrets.json` is the only runtime secret
  backend.

The planned public install set is exactly:

| Target | Install object |
| --- | --- |
| Windows x86-64 only | `CMClient-Setup.exe` |
| macOS Intel + Apple Silicon | one Universal `CMClient.dmg` |
| Linux x86-64 / ARM64 | one `CMClient-<arch>.AppImage` per CPU |
| Docker amd64 / arm64 | one OCI index plus Compose |

There are no separate Desktop, Headless, CLI, Service, MSI, DEB, or portable
downloads in the target release contract.

## Development

Required toolchains:

- Node.js `^22.18.0` or `>=24.11.0`
- pnpm 11 or newer
- the Rust toolchain pinned by `rust-toolchain.toml`

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

Additional gates:

```bash
pnpm audit:policy
pnpm audit:dependencies:node
pnpm test:e2e:web
```

## Documentation

- [Documentation authority](docs/READ_ORDER.md)
- [Unified architecture](docs/architecture/CMCLIENT_2_OVERVIEW.md)
- [Runtime and onboarding](docs/architecture/runtime-onboarding.md)
- [Release objects](docs/architecture/release-artifacts.md)
- [Docker target](docs/architecture/docker-deployment.md)
- [Agent runtime](docs/architecture/agent-runtime.md)
- [Gateway runtime](docs/architecture/gateway-runtime.md)
- [API reference](docs/api/README.md)
- [Domain projections](docs/api/domain-projections.md)
- [Getting started snapshot](docs/user/getting-started.md)
- [Using CMClient snapshot](docs/user/using-cmclient.md)
- [Deployment snapshot](docs/admin/deployment.md)
- [Configuration and security snapshot](docs/admin/configuration-security.md)
- [Operations](docs/admin/operations.md)
- [Developer guide](docs/developer/README.md)
- [Task state recovery](docs/developer/task-state-recovery.md)
- [RC field-validation snapshot](docs/testing/rc-field-validation.md)
- [P12 RC snapshot](docs/releases/2.0.0-rc.1.md)
- [Changelog](CHANGELOG.md)

Implementation work is exclusively on `dev`. `main` may be modified only after
a new explicit user approval naming the exact operation; no such approval is
part of the current Goal.
