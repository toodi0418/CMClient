# Shared Packages

Shared TypeScript packages live here. Their public APIs must be explicit and
may not import application internals.

The implemented packages are `contracts`, `api-client`, `event-client`,
`config`, `validation`, and `testing`; `i18n`, `ui`, and `theme` remain planned
shared package boundaries. `api-client` validates versioned HTTP projections
and maps failures to stable codes. `event-client` consumes the SSE fetch stream
with validated envelopes, `Last-Event-ID`, and bounded reconnects. This
workspace manifest does not make Legacy source code part of the new build graph.
