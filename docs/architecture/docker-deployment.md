# Docker Deployment

CMClient's Docker mode runs the TypeScript Gateway and the compiled Vue Web
application. It deliberately does not run the Rust Agent, Desktop, CLI,
service installer, updater, or a host device manager. The `docker` capability
is therefore available, while update, serial, service, and auto-start report
`CAPABILITY_UNAVAILABLE_DOCKER`.

## Start

The compose file builds one non-root production image and creates two
containers. `gateway` writes only its SQLite data under the named
`cmclient-data` volume. `web` is the sole published endpoint; it serves the
SPA and proxies `/api/*` across the private Docker network to `gateway`.

```bash
CMCLIENT_WEB_PORT=8080 docker compose up --build --detach
```

Browse `http://127.0.0.1:8080`. The Web port is loopback-only by default and
Gateway is never published to the host. Docker mode does not include the
Agent's LAN session, CSRF, and origin controls; remote access therefore needs
an independently authenticated TLS reverse proxy rather than a wider Docker
port binding. For a bind-mounted data directory, the host directory must be
writable by UID/GID `10001`; do not mount source code, a Docker socket, or
device nodes.

To update, pull or build a new immutable image and recreate the containers.
The container never fetches Git, executes a self-update loop, or modifies its
own root filesystem. A Host Agent deployment is required for signed update,
rollback, local service management, and direct serial-device support.

## Restrictions

Both containers are non-root, use a read-only root filesystem, drop every
Linux capability, set `no-new-privileges`, run without `privileged`, and have
bounded process and memory limits. The Web container has only the internal
network; Gateway also has a separate egress network for its configured remote
services, but no Gateway port is published. `/tmp` is the only ephemeral
writable filesystem besides the Gateway data volume.

The compose manifest intentionally contains no credentials. Provide secrets
through the deployment platform's secret mechanism rather than image layers,
command arguments, repository files, or logs. Docker's runtime restriction is
verified by the static packaging test and by an Ubuntu CI build-and-start smoke
test that queries the proxied capability endpoint.
