# Gateway Observability

Gateway request logging is structured. Every request receives a UUID trace ID
and may carry a validated correlation ID; the trace ID is returned in
`x-trace-id` for diagnosis. Unsuccessful requests and methods that can mutate
state produce durable completion records. Successful `GET` and `HEAD`
projections are deliberately omitted because the Agent health loop and the Web
event-driven refresh path would otherwise exhaust the bounded daily log file.
Logs use stable event names and structured fields, not translated backend
prose.

Fields named like API keys, authorization, passcodes, passwords, secrets,
tokens, credentials, cookies, sessions, or private keys are recursively
redacted before any logger receives them. Logging and API error handling must
never use request payloads as a convenience dump.

The supervised Gateway reserves stdout for its bounded private ready frame.
After that frame is verified, allowlisted structured runtime records travel on
the Supervisor's dedicated Gateway stderr capture mode. That mode applies the
same recursive redaction and schema validation as structured stdout while
retaining plain stderr as a stable error-code channel; malformed data is never
preserved verbatim.
