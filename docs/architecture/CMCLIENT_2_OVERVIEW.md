# CMClient 2.0 Unified Architecture

This is the normative target contract. The P13-P16 tasks migrate the current
implementation to it; a target statement here is not proof that its package or
runtime qualification has already passed.

## One Product

CMClient has modes, not separate products:

```text
cmclient                 open or focus graphical mode
cmclient <command>       command mode through local Control
cmclient --background    resident Agent without a visible window
cmclient web             open the full management Web
```

The Web UI owns all setup and operational workflows. Graphical mode is a small
status, notification-area/menu-bar, and control surface. Docker omits graphical
mode only. Agent and Gateway remain private implementation components.

## Runtime Ownership

```text
launcher / login integration
             |
             v
          Rust Agent ---- local Control ---- command / graphical modes
             |
             +---- Web listener and admission ---- full Vue Web
             |
             +---- private pipe ---- Node/Fastify Gateway
                                      |
                                      +-- Meshtastic / CallMesh / APRS / Proxy
                                      +-- SQLite domain persistence / Jobs / SSE
```

Agent owns setup state, local Control IPC, Web listener/admission, process
supervision, backup, update, rollback, and single-instance locks. Gateway owns
the Meshtastic session, CallMesh, APRS-IS, protocol-aware Proxy, domain Jobs,
events, and application persistence. UI modes never access SQLite, secrets, or
Meshtastic directly.

When `CMCLIENT_CMCLOUD_MODE=required`, CMCloud becomes the sole upstream raw
transport authority for CMClient 2.0. The Gateway durably forwards exact
`FromRadio` protobuf bytes over one authenticated WebSocket and does not invoke
local CallMesh mapping synchronization or local APRS/Proxy delivery. See
[CMCloud Raw Transport](cmcloud-transport.md).

Before setup is ready, Agent may run Gateway only in `setup_safe` mode. That
mode exposes bounded setup RPC and starts no operational Job, Proxy listener,
APRS session, mutable radio action, or background CallMesh heartbeat. Secrets
move through a private inherited pipe/control channel, never argv or an
environment variable.

## Shared Invariants

- Mutable state resolves below `~/.cmclient` on every native platform and
  `/home/cmclient/.cmclient` in Docker.
- `secrets.json` is the only runtime secret backend.
- HTTP commands, SSE events, persistent Jobs, error codes, and capability
  contracts are shared across Web, graphical, and command modes.
- Exactly one Meshtastic upstream is shared by ingest and all Proxy clients.
- Position ordering uses trusted GPS event time, sequence second, and fails
  closed when freshness cannot be proven.
- Only `precision_bits === 32` positions can reach APRS.
- The same position event produces byte-identical APRS Data at every iGate.

## Deployment Profiles

Native packages include graphical mode, command mode, Web, Agent, Gateway, and
a pinned private Node runtime. Docker includes command mode, Web, Agent, and
Gateway, but no graphical mode, native self-update, or host service manager.

Windows support is x86-64 only. All implementation commits use `dev`; `main`,
tagging, production signing, notarization, and publication require a separate
explicit human approval.
