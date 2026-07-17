# Local Agent Control API

The Agent control plane is local IPC, not the public Gateway Business API. On
Unix it listens on `<data-dir>/control.sock` with mode `0600`; Windows uses the
equivalent `\\.\pipe\cmclient-control` endpoint abstraction. No listener is
bound to a LAN address by this layer.

The initial endpoints are:

```text
GET /api/v1/control/status
POST /api/v1/control/start
POST /api/v1/control/stop
POST /api/v1/control/restart
```

Each route returns schema version, Agent state, and Gateway lifecycle state.
The endpoint is intentionally small because its purpose is to support local
Agent control, CLI, and Desktop operations while Gateway is unavailable.

When the supervisor has a live child process, Agent additionally probes the
Gateway loopback health endpoint. The control status is `running` only after a
successful probe; otherwise it reports `degraded`.
