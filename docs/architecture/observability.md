# Gateway Observability

Gateway request logging is structured. Every request receives a UUID trace ID
and may carry a validated correlation ID; the trace ID is returned in
`x-trace-id` for diagnosis. Logs use stable event names and structured fields,
not translated backend prose.

Fields named like API keys, authorization, passcodes, passwords, secrets,
tokens, credentials, cookies, sessions, or private keys are recursively
redacted before any logger receives them. Logging and API error handling must
never use request payloads as a convenience dump.
