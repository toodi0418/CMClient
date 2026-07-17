# Management Web Listener

The Agent owns the optional Management Web listener. It is separate from the
always-on local Control API: disabling the web listener cannot disable Agent,
CLI, update, or recovery control.

This P02 skeleton binds `127.0.0.1:7080` only, serves a minimal static shell,
and returns `GATEWAY_PROXY_UNAVAILABLE` for `/api/*` until P03 starts the
Gateway business API. Non-loopback binds fail closed. LAN binding is deferred
until P09 supplies authentication, sessions, CSRF/origin checks, rate limits,
and audit logging.
