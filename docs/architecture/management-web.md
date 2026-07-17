# Management Web Listener

The Agent owns the optional Management Web listener. It is separate from the
always-on local Control API: disabling the web listener cannot disable Agent,
CLI, update, or recovery control.

It binds `127.0.0.1:7080` only, serves the static shell, and reverse-proxies
`/api/*` to the Agent-supervised loopback Gateway. The proxy streams upstream
responses, including SSE, and forces the upstream connection to close after
ordinary responses. If the Gateway cannot be reached it returns the stable
`GATEWAY_PROXY_UNAVAILABLE` code without exposing transport details.

The Agent checks `GET /api/v1/system/health` before reporting a running Gateway
through local Control API; a live process that fails the probe is `degraded`.
Non-loopback binds fail closed. LAN binding is deferred until P09 supplies
authentication, sessions, CSRF/origin checks, rate limits, and audit logging.

`apps/web` is the Vue 3/Vite management shell. It owns presentation-only
navigation, responsive rail/drawer state, and route composition; it does not
open SQLite, operate Meshtastic transports, or make privileged local calls.
Gateway data and commands will enter through shared HTTP/SSE clients in the
later P06 API client slice. The Vite development listener remains loopback-only
and defaults to `127.0.0.1:5174`; the shipped static bundle is served by the
Agent listener above.
