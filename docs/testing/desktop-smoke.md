# Desktop Smoke Tests

The `desktop-smoke` GitHub Actions matrix runs on Ubuntu, macOS, and Windows.
Every runner installs the pinned Node, pnpm, and Rust toolchains, builds the
Vue Desktop frontend, then runs the Desktop Rust tests and binary build. This
is a compile and command-contract gate; it does not claim a GUI launch or
single-instance runtime check.

Ubuntu installs the Tauri WebKit/tray prerequisites before compiling the Rust
workspace. This is also required by the primary Ubuntu verification job because
the Desktop crate is a workspace member.

The Release Build Matrix adds the packaging gate omitted by that fast smoke. It
builds DMG on both Darwin architectures, DEB and AppImage on both Linux
architectures, and MSI plus NSIS on Windows x64. It then mounts or extracts
each package and validates that `cmclient-runtime/` contains the complete
portable Desktop composition. Each packaged Tauri executable must survive a
bounded native launch. The staged Desktop tree and the final extracted Linux
x64 archive also start the real Agent/Gateway/Web composition through the
canonical CLI before launching Tauri in the same runtime environment. A signed
RC field run remains responsible for interactive GUI, tray, focus, and
single-instance validation on installed hosts.
