# Docker Unified Product Target

This is the P16 target contract. The current three-container Gateway/Web/Ingress
implementation is a superseded snapshot and must not be presented as the final
Docker product before P16 verification passes.

## Composition

One Compose service named `cmclient` runs one non-root container containing
Agent, private Node/Gateway, compiled Web, command mode, and migrations. Docker
omits graphical mode only. Compose uses `init: true`: tini is PID 1, Agent is
its child, and Gateway is Agent's child. Tini forwards termination and reaps
adopted orphans; Agent owns Gateway health, restart backoff, and bounded shutdown.

The container has no Docker socket, self-update, host network/PID, privileged
mode, device by default, ingress sidecar, or Node entrypoint. It runs as
`10001:10001`, drops all capabilities, sets `no-new-privileges`, uses a read-only
root filesystem, and writes only the state volume at
`/home/cmclient/.cmclient`.

## Access And Setup

Inside the container Web listens on `0.0.0.0:8080`; Gateway and Control remain
private. Compose publishes `127.0.0.1` by default. NAT peer addresses never
prove local access, so every Docker browser session requires application
authentication regardless of binding.

Initial access uses a short-lived, one-time code:

```bash
docker compose exec -T cmclient cmclient setup-code
```

Only its verifier is stored. The code never appears in logs, image layers,
environment/Compose values, URLs, diagnostics, or evidence. Wider host
publishing is an explicit operator change and does not disable authentication.
`setup_required` remains healthy and exposes only setup/login plus read-only
health.

## Lifecycle And Update

```bash
docker compose up -d --wait
docker compose exec -T cmclient cmclient status
docker compose exec -T cmclient cmclient backup
```

The application never controls Docker. Update records the current immutable
digest, performs operator-owned pull/recreate, verifies version/health/database/
volume sentinels, and pins/recreates the prior digest on failure. `docker compose
down` retains the named volume; `down --volumes` is a separate destructive
operator action.

The release exposes one OCI index with exactly linux/amd64 and linux/arm64
children plus the Compose file. Cross-build or emulation evidence is labeled as
such; real native qualification is never inferred.
