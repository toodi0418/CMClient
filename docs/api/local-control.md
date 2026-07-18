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
POST /api/v1/control/web/enable
POST /api/v1/control/web/disable
```

Each route returns the schema version, Agent version/state, Gateway lifecycle,
Management Web listener state and its loopback URL (only when running), uptime,
and the latest stable error code. The enable/disable commands control only the
optional loopback Management Web listener; the private Control API remains
available in both states. The endpoint is intentionally small because its
purpose is to support local Agent control, CLI, and Desktop operations while
Gateway is unavailable.

When the supervisor has a live child process, Agent additionally probes the
Gateway loopback health endpoint. The control status is `running` only after a
successful probe; otherwise it reports `degraded`.
