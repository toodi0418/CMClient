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
Gateway data and commands enter through `@cmclient/api-client` and
`@cmclient/event-client`, never through page-specific transport code. The API
client validates versioned Gateway responses and maps network, proxy, HTTP, and
malformed-response failures to stable codes without surfacing backend prose.
The event client reads the SSE fetch stream, validates each DomainEvent envelope,
preserves `Last-Event-ID`, and reconnects with bounded exponential backoff. The
Vite development listener remains loopback-only and defaults to
`127.0.0.1:5174`; the shipped static bundle is served by the Agent listener
above.

The Web Gateway store owns the system status/capability projection and the SSE
connection state. Dashboard, System, and Meshtastic consume that store instead
of inventing local transport state. In particular, the Meshtastic page reports
the Agent-provided serial capability and its stable reason code until a live
transport status API exists; it does not fabricate a radio connection, node
count, or telemetry value.

The Nodes, Messages, Telemetry, and Positions views consume bounded (maximum
200 records) read projections from Gateway persistence. Nodes, messages, and
telemetry are ordered by their persisted observation timestamps; positions are
canonical events ordered by trusted event time with creation time only as the
stable fallback. The Web position plot is local-only and uses the persisted
coordinates directly, so opening the management interface does not disclose
Mesh locations to an external map tile provider.

The shell uses Tailwind 4 through the Vite integration and maps its `cm-*`
utility names to semantic CSS variables in `apps/web/src/theme/tokens.css`.
Those tokens identify canvas, surfaces, content, accent, warning, danger and
focus states instead of binding components to raw colours. PrimeVue is
registered with the Aura preset and a `.cm-dark` selector so the P06 theme
controller can synchronise PrimeVue with the same token vocabulary. The shell
uses PrimeVue's unstyled button primitive where custom control treatment is
needed and Lucide icons for navigation; controls keep a 44px minimum target in
desktop, collapsed-rail and mobile-drawer layouts.

At document head, a small bootstrap reads the versioned local preference record
and applies the resolved `data-theme`, `.cm-dark`, `color-scheme`, and document
language before the module entry loads. The Pinia preferences store then keeps
`light`, `dark`, or `system` and `zh-TW` or `en-US` in sync with that document
state, the theme-color meta tag, the system-colour media query, and vue-i18n.
Invalid or unavailable storage values fall back to `system` and a browser
locale match with `zh-TW` as the final fallback. Management routes carry
translation keys rather than rendered text so section headings, navigation and
the display controls switch immediately without changing their route identity.
