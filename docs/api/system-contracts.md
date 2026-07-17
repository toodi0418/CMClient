# System Version And Capabilities Contract

`@cmclient/contracts` defines the schema-versioned payload shared by Agent,
Gateway, Web, Desktop, and CLI for system version and capabilities. P03 exposes
this contract at `/api/v1/system/version` and `/api/v1/system/capabilities`.

`build` always identifies the version, source commit, and release channel.
`builtAt` is present only when the build pipeline supplies a verified UTC
timestamp. Development builds do not invent a build time.

Every key in `capabilities` is mandatory. A capability with `available: false`
must include a stable `reasonCode`, such as
`CAPABILITY_UNAVAILABLE_PLATFORM` or `CAPABILITY_UNAVAILABLE_DOCKER`. Clients
use these fields to hide or explain unavailable controls rather than guessing
from the operating system.
