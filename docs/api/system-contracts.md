# System Version And Capabilities Contract

`@cmclient/contracts` defines the schema-versioned payload shared by Agent,
Gateway, Web, Desktop, and CLI for system version and capabilities. Gateway
exposes this contract at `/api/v1/system/version` and
`/api/v1/system/capabilities`.

`build` always identifies the version, source commit, and release channel.
`builtAt` is present only when the build pipeline supplies a verified UTC
timestamp. Development builds do not invent a build time.

Every key in `capabilities` is mandatory. A capability with `available: false`
must include a stable `reasonCode`, such as
`CAPABILITY_UNAVAILABLE_PLATFORM` or `CAPABILITY_UNAVAILABLE_DOCKER`. Clients
use these fields to hide or explain unavailable controls rather than guessing
from the operating system.

`remoteDispatch` is a required capability key even though its first-phase
contract is intentionally disabled. Gateway returns `available: false` with
`REMOTE_DISPATCH_NOT_ENABLED`; clients must not infer availability from the
presence of a route or task schema. The shared dispatch task/status schema does
not create a compatibility path for removed sharing or command features.

The capability owner is part of the reason code: Desktop owns tray behavior,
Agent owns lifecycle/update/Web behavior, and Docker owns only its constrained
Gateway/Web/Ingress surface. A client must hide an unavailable action instead
of rendering a button that cannot be executed.
