# Shared Packages

Shared TypeScript packages live here. Their public APIs must be explicit and
may not import application internals.

The planned packages are `contracts`, `api-client`, `event-client`, `config`,
`validation`, `i18n`, `ui`, `theme`, and `testing`. Package implementations
are added by their owning foundation or feature task; this workspace manifest
does not make Legacy source code part of the new build graph.
