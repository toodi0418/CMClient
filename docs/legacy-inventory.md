# Legacy Repository Inventory

## Scope

This inventory records the legacy `dev` baseline at commit `da24ae0` before
CMClient 2.0 restructuring begins. It is an evidence record, not an
architecture specification or an endorsement of the legacy implementation.

## Repository Shape

- 1,193 tracked paths, totalling approximately 200 MiB.
- `src/` contains 1,105 paths. Most are committed offline map tiles and font
  assets under `src/web/public/map/`; 41 Meshtastic protobuf source files live
  under `proto/`.
- The runtime is a single CommonJS Node.js application (`package.json` version
  `0.2.29`) with Electron embedded in the same codebase. It is not a monorepo,
  TypeScript project, Cargo workspace, or Tauri application.
- The largest tracked standalone artifact is `callmesh-client.tar` (about
  88 MiB). Tracked runtime data and generated-output candidates are catalogued
  separately by P00-T04.

## Entrypoints And Runtime Boundaries

| Surface | Legacy entrypoint | Observed responsibility |
| --- | --- | --- |
| CLI | `src/index.js` | yargs command parsing, Meshtastic TCP/serial connection, CallMesh and APRS setup, optional web dashboard |
| Desktop | `src/electron/main.js` | Electron windows, IPC, preferences, API-key handling, and a child Node backend |
| Web | `src/web/server.js` | Raw Node `http` server, in-memory dashboard state, static assets, and event streaming |
| Meshtastic | `src/meshtasticClient.js`, `proto/meshtastic/` | TCP/serial frame handling and protobuf decoding |
| APRS | `src/aprs/client.js`, `src/callmesh/aprsBridge.js` | APRS-IS client and legacy position bridge/deduplication |
| Persistence | `src/storage/`, `src/nodeDatabase.js` | Node built-in SQLite plus JSON/JSONL compatibility storage |

The current desktop process directly owns backend lifecycle and reads/writes
user configuration. CMClient 2.0 replaces this with the Rust Agent control
plane; these modules are reference material only.

## Dependencies

The production dependency set is JavaScript-only: `protobufjs`, `serialport`,
`ws`, `yargs`, `bonjour-service`, `chart.js`, `maplibre-gl`, `geojson-vt`,
`vt-pbf`, and `unishox2.siara.cc`. Development packaging uses Electron,
`electron-packager`, and `@yao-pkg/pkg`. No TypeScript, Vue, Fastify, Pinia,
Rust, Tauri, workspace, lint, formatter, or test-runner configuration exists.

## CI And Test Baseline

- `.github/workflows/build-macos-linux.yml` builds Electron and `pkg` CLI
  artifacts for macOS and Linux.
- `.github/workflows/build-windows.yml` builds the Windows Electron and CLI
  artifacts.
- `.github/workflows/docker-image.yml` builds and publishes a multi-platform
  Node Docker image.
- `.github/workflows/release.yml` uploads packaging artifacts to tagged
  releases.
- `package.json` exposes only `start`, `desktop`, and platform build scripts.
  There are no lint, format, typecheck, or test scripts.
- Root-level `test_*.js` files and `scripts/test-*.js` are standalone/manual
  diagnostic scripts rather than an automated test suite.

## Deployment Assets

- `Dockerfile`, `docker-compose.yml`, and `docker-entrypoint.sh` package the
  Node runtime and expose its legacy dashboard on port 7080.
- `scripts/bootstrap-linux.sh`, `scripts/install-linux.sh`, and
  `scripts/manage-service-linux.sh` implement legacy Linux installation and
  service management.
- `scripts/build-win.js`, `scripts/build-linux.js`, and `scripts/run-electron.js`
  support Electron packaging and local desktop launch.

These deploy and update mechanisms are legacy artifacts. CMClient 2.0 will
replace them with Agent-owned service, update, rollback, and release flows.

## Migration Constraint

Useful protocol samples, protobuf definitions, data migration knowledge, and
behavioural regression cases may be retained as references. Legacy Electron,
raw HTTP server, CallMesh coupling, TENMAN/TENMAP, and self-updating Node
runtime must not become foundations of the CMClient 2.0 implementation.

## Final Disposition

P11 removed the complete root `src/` runtime, its committed offline map corpus,
manual root tests, obsolete build/debug scripts, and both Meshtastic gitlinks.
The locked 41-file `proto/meshtastic/` corpus remains because the production
Gateway, Docker image, release bundles, and compatibility tests consume it
directly. Historical paths above identify the audited baseline only; none is a
current entrypoint or dependency.
