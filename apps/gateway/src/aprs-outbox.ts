import { randomUUID } from "node:crypto";
import net, { type Socket } from "node:net";
import { DatabaseSync } from "node:sqlite";

import type { AprsOutboxEntry as PublicAprsOutboxEntry } from "@cmclient/contracts";

export type AprsOutboxStatus = "queued" | "sending" | "sent" | "failed";

export interface AprsOutboxEntry extends PublicAprsOutboxEntry {
  data: string;
}

export interface EnqueueAprsInput {
  callsign: string;
  canonicalEventId: string;
  data: string;
  now: string;
}

export interface AprsTransport {
  send(data: string): Promise<void>;
}

export interface AprsRetryOptions {
  initialDelayMs?: number;
  maximumDelayMs?: number;
}

export class AprsOutboxError extends Error {
  readonly code = "APRS_OUTBOX_FAILED";

  constructor() {
    super("APRS_OUTBOX_FAILED");
  }
}

export class AprsOutboxRepository {
  constructor(private readonly database: DatabaseSync) {}

  enqueue(input: EnqueueAprsInput): {
    created: boolean;
    entry: AprsOutboxEntry;
  } {
    if (
      !/^[A-Z0-9]{1,6}(?:-[0-9]{1,2})?$/.test(input.callsign) ||
      !input.canonicalEventId.trim() ||
      !input.data.trim() ||
      /[\r\n]/.test(input.data) ||
      !isTimestamp(input.now)
    ) {
      throw new AprsOutboxError();
    }
    const existing = this.findByIdentity(
      input.callsign,
      input.canonicalEventId,
    );
    if (existing) {
      return { created: false, entry: existing };
    }
    const id = `aprs-outbox-${randomUUID()}`;
    try {
      this.database
        .prepare(
          "INSERT OR IGNORE INTO aprs_outbox (id, callsign, canonical_event_id, data, status, attempts, next_attempt_at, created_at, updated_at) VALUES (?, ?, ?, ?, 'queued', 0, ?, ?, ?)",
        )
        .run(
          id,
          input.callsign,
          input.canonicalEventId,
          input.data,
          input.now,
          input.now,
          input.now,
        );
    } catch {
      throw new AprsOutboxError();
    }
    const entry = this.findByIdentity(input.callsign, input.canonicalEventId);
    if (!entry) {
      throw new AprsOutboxError();
    }
    return { created: entry.id === id, entry };
  }

  claimDue(now: string, limit = 10): AprsOutboxEntry[] {
    if (!isTimestamp(now) || !Number.isInteger(limit) || limit < 1) {
      throw new AprsOutboxError();
    }
    const claimed: AprsOutboxEntry[] = [];
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const rows = this.database
        .prepare(
          "SELECT id FROM aprs_outbox WHERE status IN ('queued', 'failed') AND next_attempt_at <= ? ORDER BY next_attempt_at, created_at LIMIT ?",
        )
        .all(now, limit);
      for (const row of rows) {
        const id = String(row.id);
        this.database
          .prepare(
            "UPDATE aprs_outbox SET status = 'sending', updated_at = ? WHERE id = ?",
          )
          .run(now, id);
        const entry = this.find(id);
        if (entry) {
          claimed.push(entry);
        }
      }
      this.database.exec("COMMIT");
      return claimed;
    } catch {
      this.database.exec("ROLLBACK");
      throw new AprsOutboxError();
    }
  }

  markSent(id: string, now: string): AprsOutboxEntry {
    if (!isTimestamp(now)) {
      throw new AprsOutboxError();
    }
    this.database
      .prepare(
        "UPDATE aprs_outbox SET status = 'sent', sent_at = ?, updated_at = ?, last_error_code = NULL WHERE id = ? AND status = 'sending'",
      )
      .run(now, now, id);
    const entry = this.required(id);
    if (entry.status !== "sent") {
      throw new AprsOutboxError();
    }
    return entry;
  }

  markFailed(
    id: string,
    now: string,
    retryDelayMs: number,
    errorCode: string,
  ): AprsOutboxEntry {
    const current = this.required(id);
    if (
      current.status !== "sending" ||
      !isTimestamp(now) ||
      !Number.isFinite(retryDelayMs) ||
      retryDelayMs < 0 ||
      !errorCode.trim() ||
      /[\r\n]/.test(errorCode)
    ) {
      throw new AprsOutboxError();
    }
    const nextAttemptAt = new Date(
      Date.parse(now) + retryDelayMs,
    ).toISOString();
    this.database
      .prepare(
        "UPDATE aprs_outbox SET status = 'failed', attempts = ?, next_attempt_at = ?, last_error_code = ?, updated_at = ? WHERE id = ? AND status = 'sending'",
      )
      .run(current.attempts + 1, nextAttemptAt, errorCode, now, id);
    const entry = this.required(id);
    if (entry.status !== "failed") {
      throw new AprsOutboxError();
    }
    return entry;
  }

  resumeInterrupted(now: string): number {
    if (!isTimestamp(now)) {
      throw new AprsOutboxError();
    }
    const result = this.database
      .prepare(
        "UPDATE aprs_outbox SET status = 'failed', attempts = attempts + 1, next_attempt_at = ?, last_error_code = 'APRS_TX_INTERRUPTED', updated_at = ? WHERE status = 'sending'",
      )
      .run(now, now);
    return Number(result.changes);
  }

  find(id: string): AprsOutboxEntry | undefined {
    const row = this.database
      .prepare("SELECT * FROM aprs_outbox WHERE id = ?")
      .get(id);
    return row ? toEntry(row) : undefined;
  }

  findByIdentity(
    callsign: string,
    canonicalEventId: string,
  ): AprsOutboxEntry | undefined {
    const row = this.database
      .prepare(
        "SELECT * FROM aprs_outbox WHERE callsign = ? AND canonical_event_id = ?",
      )
      .get(callsign, canonicalEventId);
    return row ? toEntry(row) : undefined;
  }

  list(limit: number): PublicAprsOutboxEntry[] {
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
      throw new AprsOutboxError();
    }
    return this.database
      .prepare(
        "SELECT * FROM aprs_outbox ORDER BY updated_at DESC, id ASC LIMIT ?",
      )
      .all(limit)
      .map((row) => publicEntry(toEntry(row as Record<string, unknown>)));
  }

  private required(id: string): AprsOutboxEntry {
    const entry = this.find(id);
    if (!entry) {
      throw new AprsOutboxError();
    }
    return entry;
  }
}

function publicEntry(entry: AprsOutboxEntry): PublicAprsOutboxEntry {
  return {
    id: entry.id,
    callsign: entry.callsign,
    canonicalEventId: entry.canonicalEventId,
    status: entry.status,
    attempts: entry.attempts,
    nextAttemptAt: entry.nextAttemptAt,
    createdAt: entry.createdAt,
    updatedAt: entry.updatedAt,
    ...(entry.lastErrorCode ? { lastErrorCode: entry.lastErrorCode } : {}),
    ...(entry.sentAt ? { sentAt: entry.sentAt } : {}),
  };
}

export class AprsOutboxWorker {
  private readonly clock: () => Date;
  private readonly retry: Required<AprsRetryOptions>;

  constructor(
    private readonly repository: AprsOutboxRepository,
    private readonly transport: AprsTransport,
    options: AprsRetryOptions & { clock?: () => Date } = {},
  ) {
    this.clock = options.clock ?? (() => new Date());
    this.retry = {
      initialDelayMs: options.initialDelayMs ?? 1_000,
      maximumDelayMs: options.maximumDelayMs ?? 60_000,
    };
    if (
      !Number.isFinite(this.retry.initialDelayMs) ||
      this.retry.initialDelayMs <= 0 ||
      !Number.isFinite(this.retry.maximumDelayMs) ||
      this.retry.maximumDelayMs < this.retry.initialDelayMs
    ) {
      throw new AprsOutboxError();
    }
  }

  async flush(limit?: number): Promise<AprsOutboxEntry[]> {
    const now = this.clock().toISOString();
    this.repository.resumeInterrupted(now);
    const entries = this.repository.claimDue(now, limit);
    const results: AprsOutboxEntry[] = [];
    for (const entry of entries) {
      try {
        await this.transport.send(entry.data);
        results.push(
          this.repository.markSent(entry.id, this.clock().toISOString()),
        );
      } catch {
        results.push(
          this.repository.markFailed(
            entry.id,
            this.clock().toISOString(),
            retryDelay(entry.attempts + 1, this.retry),
            "APRS_TX_FAILED",
          ),
        );
      }
    }
    return results;
  }
}

export class AprsIsTcpClient implements AprsTransport {
  constructor(
    private readonly options: {
      host: string;
      port: number;
      loginLine: string;
      timeoutMs?: number;
    },
  ) {
    if (
      !options.host.trim() ||
      !Number.isInteger(options.port) ||
      options.port < 1 ||
      options.port > 65_535 ||
      !options.loginLine.trim() ||
      /[\r\n]/.test(options.loginLine) ||
      (options.timeoutMs !== undefined &&
        (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0))
    ) {
      throw new AprsOutboxError();
    }
  }

  async send(data: string): Promise<void> {
    if (!data.trim() || /[\r\n]/.test(data)) {
      throw new AprsOutboxError();
    }
    const socket = net.createConnection({
      host: this.options.host,
      port: this.options.port,
    });
    try {
      await onceConnected(socket, this.options.timeoutMs ?? 10_000);
      await write(socket, `${this.options.loginLine}\r\n`);
      await write(socket, `${data}\r\n`);
      socket.end();
    } catch {
      socket.destroy();
      throw new AprsOutboxError();
    }
  }
}

function retryDelay(
  attempt: number,
  retry: Required<AprsRetryOptions>,
): number {
  return Math.min(
    retry.maximumDelayMs,
    retry.initialDelayMs * 2 ** Math.max(0, attempt - 1),
  );
}

function onceConnected(socket: Socket, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new AprsOutboxError());
    }, timeoutMs);
    socket.once("connect", () => {
      clearTimeout(timer);
      resolve();
    });
    socket.once("error", () => {
      clearTimeout(timer);
      reject(new AprsOutboxError());
    });
  });
}

function write(socket: Socket, value: string): Promise<void> {
  return new Promise((resolve, reject) =>
    socket.write(value, (error) => (error ? reject(error) : resolve())),
  );
}

function toEntry(row: Record<string, unknown>): AprsOutboxEntry {
  const status = String(row.status);
  if (!["queued", "sending", "sent", "failed"].includes(status)) {
    throw new AprsOutboxError();
  }
  return {
    id: String(row.id),
    callsign: String(row.callsign),
    canonicalEventId: String(row.canonical_event_id),
    data: String(row.data),
    status: status as AprsOutboxStatus,
    attempts: Number(row.attempts),
    nextAttemptAt: String(row.next_attempt_at),
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
    ...(typeof row.last_error_code === "string"
      ? { lastErrorCode: row.last_error_code }
      : {}),
    ...(typeof row.sent_at === "string" ? { sentAt: row.sent_at } : {}),
  };
}

function isTimestamp(value: string): boolean {
  return Number.isFinite(Date.parse(value));
}
