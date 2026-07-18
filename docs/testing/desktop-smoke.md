# Desktop Smoke Tests

The `desktop-smoke` GitHub Actions matrix runs on Ubuntu, macOS, and Windows.
Every runner installs the pinned Node, pnpm, and Rust toolchains, builds the
Vue Desktop frontend, then runs the Desktop Rust tests and binary build.

Ubuntu installs the Tauri WebKit/tray prerequisites before compiling the Rust
workspace. This is also required by the primary Ubuntu verification job because
the Desktop crate is a workspace member.

The cross-platform job validates compilation and the Tauri command/tray test
surface on each supported runner. A macOS runtime smoke additionally starts
the real Tauri app, launches a second instance, and verifies that only one
Desktop process remains. This covers the official single-instance path while
the local Agent Web toggle is verified through the private Control API.
