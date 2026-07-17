import { randomUUID } from "node:crypto";

const SENSITIVE_KEY =
  /(api[_-]?key|authorization|passcode|password|secret|token)/i;
const REDACTED = "[REDACTED]";

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface LogEntry {
  level: LogLevel;
  message: string;
  traceId: string;
  correlationId?: string;
  fields?: Record<string, unknown>;
}

export interface StructuredLogger {
  log(entry: LogEntry): void;
}

export class ConsoleStructuredLogger implements StructuredLogger {
  log(entry: LogEntry): void {
    process.stdout.write(`${JSON.stringify(redact(entry))}\n`);
  }
}

export class MemoryLogger implements StructuredLogger {
  readonly entries: LogEntry[] = [];

  log(entry: LogEntry): void {
    this.entries.push(redact(entry) as LogEntry);
  }
}

export function createTraceId(): string {
  return randomUUID();
}

export function resolveTraceId(value: unknown): string {
  return typeof value === "string" && /^[a-f0-9-]{36}$/i.test(value)
    ? value.toLowerCase()
    : createTraceId();
}

export function resolveCorrelationId(value: unknown): string | undefined {
  return typeof value === "string" && /^[a-zA-Z0-9._:-]{1,128}$/.test(value)
    ? value
    : undefined;
}

export function redact(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(redact);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, nestedValue]) => [
        key,
        SENSITIVE_KEY.test(key) ? REDACTED : redact(nestedValue),
      ]),
    );
  }
  return value;
}
