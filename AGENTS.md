# CMClient 2.0 Repository Rules

Read [docs/READ_ORDER.md](docs/READ_ORDER.md) before changing this Repository.
The workspace task graph and checkpoint scripts govern task execution; this
file defines Repository-local architecture and Git boundaries.

## Product Contract

- CMClient is one public product. Graphical and command operation are modes,
  while Agent and Gateway are internal components.
- The full management experience is Web. The Tauri graphical mode is a small
  status, tray, and control surface.
- Native packages include graphical mode, command mode, Web, Agent, Gateway,
  and a pinned private Node runtime. Docker omits graphical mode only.
- Mutable state resolves below `~/.cmclient` on every platform. Runtime secrets
  use only `secrets.json`; do not use Keychain, Credential Manager/DPAPI,
  Secret Service, or the legacy systemd vault.
- Web, graphical mode, and command mode use Agent-owned APIs. They never touch
  SQLite, secrets, or Meshtastic directly.
- Agent owns Control IPC, Web admission, setup, process supervision, backup,
  update, and rollback. Gateway owns Meshtastic, CallMesh, APRS, Proxy, domain
  persistence, Jobs, and events.
- Preserve deterministic APRS and position invariants, `precision_bits === 32`,
  fail-closed ordering, one shared Meshtastic upstream, and protocol-aware
  multi-client Proxy behavior.

## Git And Claims

- Work only on `dev` and push coherent task checkpoints to `origin/dev`.
- Never force push or rewrite pushed history.
- Do not modify or push `main` without a new explicit user approval naming the
  exact operation. This Goal grants no such approval.
- Do not tag, sign with production credentials, publish, or create a formal
  release without a separate explicit approval.
- Windows release support is x86-64 only. Never claim an untested target,
  production signature, real device, or external service result.
- Keep secrets, private identity, raw captures, databases, and generated
  campaign output out of Git.

## Change Quality

- Follow existing Rust, TypeScript, Vue, Tauri, SQLite, HTTP/SSE, and Job
  boundaries.
- Add regression coverage before fixing a reproduced defect.
- Run formatting, lint, typecheck, tests, documentation contracts, and the
  relevant package/runtime gate. Do not weaken checks to make a task pass.
- Current P12 documents are retained snapshots. Do not treat them as the active
  install or release contract where `docs/READ_ORDER.md` marks them historical.
