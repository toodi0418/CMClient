import type { MeshtasticSchema } from "../protobuf/schema.js";
import type { ProxyUpstreamEvent } from "./upstream.js";

const TO_OBJECT_OPTIONS = {
  bytes: Uint8Array,
  defaults: false,
  enums: String,
  longs: Number,
  oneofs: true,
};

const CLIENT_ID_PATTERN = /^[a-zA-Z0-9._:-]{1,128}$/;

export type ProxyCorrelationKind = "ack" | "config" | "request";

export interface ProxyCorrelation {
  id: number;
  kind: ProxyCorrelationKind;
}

export interface ProxyOutboundUpstream {
  writeFrame(frame: Uint8Array): Promise<void>;
}

export interface ProxyReplySink {
  deliver(clientId: string, frame: Uint8Array): boolean;
}

export interface ProxyOutboundRouterOptions {
  maxFrameBytes?: number;
  maxPendingCorrelations?: number;
  maxQueuedWrites?: number;
  responseTimeoutMs?: number;
}

export interface ProxyOutboundSubmission {
  clientId: string;
  frame: Uint8Array;
}

export interface ProxyOutboundReceipt {
  correlations: ProxyCorrelation[];
}

export interface ProxyOutboundSnapshot {
  pendingCorrelations: number;
  queuedWrites: number;
  writing: boolean;
}

export type ProxyOutboundEvent =
  | {
      kind: "reply";
      clientId: string;
      correlation: ProxyCorrelation;
      frame: Uint8Array;
    }
  | {
      kind: "expired" | "write_failed";
      clientId: string;
      correlation?: ProxyCorrelation;
      code: string;
    };

export type ProxyOutboundListener = (event: ProxyOutboundEvent) => void;

export class ProxyOutboundError extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

/**
 * Owns the only write queue for a shared Meshtastic upstream. Its caller must
 * feed upstream events through handleUpstreamEvent before broadcasting frames.
 */
export class ProxyOutboundRouter {
  private readonly correlations = new Map<string, PendingCorrelation>();
  private readonly listeners = new Set<ProxyOutboundListener>();
  private readonly queue: PendingWrite[] = [];
  private readonly maxFrameBytes: number;
  private readonly maxPendingCorrelations: number;
  private readonly maxQueuedWrites: number;
  private readonly responseTimeoutMs: number;
  private activeWrite: PendingWrite | undefined;
  private writing = false;
  private stopped = false;

  constructor(
    private readonly schema: MeshtasticSchema,
    private readonly upstream: ProxyOutboundUpstream,
    private readonly replies: ProxyReplySink,
    options: ProxyOutboundRouterOptions = {},
  ) {
    this.maxFrameBytes = options.maxFrameBytes ?? 512;
    this.maxPendingCorrelations = options.maxPendingCorrelations ?? 128;
    this.maxQueuedWrites = options.maxQueuedWrites ?? 128;
    this.responseTimeoutMs = options.responseTimeoutMs ?? 15_000;
    if (
      !Number.isInteger(this.maxFrameBytes) ||
      this.maxFrameBytes < 1 ||
      this.maxFrameBytes > 4_096 ||
      !Number.isInteger(this.maxPendingCorrelations) ||
      this.maxPendingCorrelations < 1 ||
      this.maxPendingCorrelations > 4_096 ||
      !Number.isInteger(this.maxQueuedWrites) ||
      this.maxQueuedWrites < 1 ||
      this.maxQueuedWrites > 4_096 ||
      !Number.isInteger(this.responseTimeoutMs) ||
      this.responseTimeoutMs < 100 ||
      this.responseTimeoutMs > 120_000
    ) {
      throw new ProxyOutboundError("PROXY_OUTBOUND_CONFIGURATION_INVALID");
    }
  }

  get snapshot(): ProxyOutboundSnapshot {
    return {
      pendingCorrelations: this.correlations.size,
      queuedWrites: this.queue.length,
      writing: this.writing,
    };
  }

  subscribe(listener: ProxyOutboundListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  submit(input: ProxyOutboundSubmission): Promise<ProxyOutboundReceipt> {
    if (this.stopped) {
      return Promise.reject(new ProxyOutboundError("PROXY_OUTBOUND_STOPPED"));
    }
    if (!CLIENT_ID_PATTERN.test(input.clientId)) {
      return Promise.reject(new ProxyOutboundError("PROXY_CLIENT_ID_INVALID"));
    }
    if (
      !(input.frame instanceof Uint8Array) ||
      input.frame.length === 0 ||
      input.frame.length > this.maxFrameBytes
    ) {
      return Promise.reject(
        new ProxyOutboundError("PROXY_OUTBOUND_FRAME_INVALID"),
      );
    }
    if (this.queue.length + Number(this.writing) >= this.maxQueuedWrites) {
      return Promise.reject(
        new ProxyOutboundError("PROXY_OUTBOUND_QUEUE_FULL"),
      );
    }

    let correlations: ProxyCorrelation[];
    try {
      correlations = correlationsFromToRadio(this.schema, input.frame);
    } catch (error) {
      return Promise.reject(error);
    }
    if (
      this.correlations.size + correlations.length >
      this.maxPendingCorrelations
    ) {
      return Promise.reject(
        new ProxyOutboundError("PROXY_PENDING_CORRELATION_LIMIT_REACHED"),
      );
    }
    for (const correlation of correlations) {
      if (this.correlations.has(correlationKey(correlation))) {
        return Promise.reject(
          new ProxyOutboundError("PROXY_CORRELATION_CONFLICT"),
        );
      }
    }

    for (const correlation of correlations) {
      this.correlations.set(correlationKey(correlation), {
        clientId: input.clientId,
        correlation,
      });
    }
    const frame = new Uint8Array(input.frame);
    const result = new Promise<ProxyOutboundReceipt>((resolve, reject) => {
      this.queue.push({
        clientId: input.clientId,
        correlations,
        frame,
        reject,
        resolve,
      });
    });
    void this.drain();
    return result;
  }

  handleUpstreamEvent(event: ProxyUpstreamEvent): boolean {
    if (event.kind === "error") {
      this.abortWrites("PROXY_UPSTREAM_UNAVAILABLE");
      this.expireAll("PROXY_UPSTREAM_UNAVAILABLE");
      return false;
    }
    if (event.kind !== "frame") {
      return false;
    }
    let matches: ProxyCorrelation[];
    try {
      matches = correlationsFromFromRadio(this.schema, event.frame);
    } catch {
      return false;
    }
    let routed = false;
    for (const correlation of matches) {
      const pending = this.correlations.get(correlationKey(correlation));
      if (!pending) {
        continue;
      }
      this.removeCorrelation(pending);
      if (this.replies.deliver(pending.clientId, event.frame)) {
        this.emit({
          kind: "reply",
          clientId: pending.clientId,
          correlation: { ...pending.correlation },
          frame: new Uint8Array(event.frame),
        });
      } else {
        this.emit({
          kind: "expired",
          clientId: pending.clientId,
          correlation: { ...pending.correlation },
          code: "PROXY_CLIENT_UNAVAILABLE",
        });
      }
      routed = true;
    }
    return routed;
  }

  cancelClient(clientId: string): void {
    const error = new ProxyOutboundError("PROXY_CLIENT_DISCONNECTED");
    for (let index = this.queue.length - 1; index >= 0; index -= 1) {
      const queued = this.queue[index];
      if (!queued || queued.clientId !== clientId) {
        continue;
      }
      this.queue.splice(index, 1);
      this.rejectWrite(queued, error);
    }
    if (this.activeWrite?.clientId === clientId) {
      this.activeWrite.cancelled = true;
      this.rejectWrite(this.activeWrite, error);
    }
    for (const pending of [...this.correlations.values()]) {
      if (pending.clientId === clientId) {
        this.removeCorrelation(pending);
      }
    }
  }

  stop(): void {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    const error = new ProxyOutboundError("PROXY_OUTBOUND_STOPPED");
    for (const queued of this.queue.splice(0)) {
      this.rejectWrite(queued, error);
    }
    if (this.activeWrite) {
      this.activeWrite.cancelled = true;
      this.rejectWrite(this.activeWrite, error);
    }
    this.expireAll("PROXY_OUTBOUND_STOPPED");
  }

  private async drain(): Promise<void> {
    if (this.writing || this.stopped) {
      return;
    }
    this.writing = true;
    try {
      while (!this.stopped) {
        const queued = this.queue.shift();
        if (!queued) {
          return;
        }
        this.activeWrite = queued;
        try {
          try {
            await this.upstream.writeFrame(new Uint8Array(queued.frame));
          } catch {
            if (!queued.settled) {
              this.rejectWrite(
                queued,
                new ProxyOutboundError("PROXY_UPSTREAM_WRITE_FAILED"),
              );
              this.emit({
                kind: "write_failed",
                clientId: queued.clientId,
                code: "PROXY_UPSTREAM_WRITE_FAILED",
              });
            }
            continue;
          }
          if (this.stopped || queued.cancelled) {
            this.rejectWrite(
              queued,
              new ProxyOutboundError("PROXY_OUTBOUND_STOPPED"),
            );
            continue;
          }
          for (const correlation of queued.correlations) {
            const pending = this.correlations.get(correlationKey(correlation));
            if (pending) {
              pending.timer = setTimeout(() => {
                if (
                  this.correlations.get(correlationKey(correlation)) === pending
                ) {
                  this.removeCorrelation(pending);
                  this.emit({
                    kind: "expired",
                    clientId: pending.clientId,
                    correlation: { ...pending.correlation },
                    code: "PROXY_RESPONSE_TIMEOUT",
                  });
                }
              }, this.responseTimeoutMs);
              pending.timer.unref();
            }
          }
          this.resolveWrite(queued);
        } finally {
          this.activeWrite = undefined;
        }
      }
    } finally {
      this.writing = false;
      if (!this.stopped && this.queue.length > 0) {
        void this.drain();
      }
    }
  }

  private expireAll(code: string): void {
    for (const pending of [...this.correlations.values()]) {
      this.removeCorrelation(pending);
      this.emit({
        kind: "expired",
        clientId: pending.clientId,
        correlation: { ...pending.correlation },
        code,
      });
    }
  }

  private abortWrites(code: string): void {
    const error = new ProxyOutboundError(code);
    for (const queued of this.queue.splice(0)) {
      this.rejectWrite(queued, error);
      this.emit({ kind: "write_failed", clientId: queued.clientId, code });
    }
    if (this.activeWrite) {
      this.activeWrite.cancelled = true;
      this.rejectWrite(this.activeWrite, error);
      this.emit({
        kind: "write_failed",
        clientId: this.activeWrite.clientId,
        code,
      });
    }
  }

  private rejectWrite(write: PendingWrite, error: Error): void {
    if (write.settled) {
      return;
    }
    write.settled = true;
    this.removeCorrelations(write.correlations);
    write.reject(error);
  }

  private resolveWrite(write: PendingWrite): void {
    if (write.settled) {
      return;
    }
    write.settled = true;
    write.resolve({
      correlations: write.correlations.map((correlation) => ({
        ...correlation,
      })),
    });
  }

  private removeCorrelations(correlations: ProxyCorrelation[]): void {
    for (const correlation of correlations) {
      const pending = this.correlations.get(correlationKey(correlation));
      if (pending) {
        this.removeCorrelation(pending);
      }
    }
  }

  private removeCorrelation(pending: PendingCorrelation): void {
    this.correlations.delete(correlationKey(pending.correlation));
    if (pending.timer) {
      clearTimeout(pending.timer);
    }
  }

  private emit(event: ProxyOutboundEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

interface PendingCorrelation {
  clientId: string;
  correlation: ProxyCorrelation;
  timer?: NodeJS.Timeout;
}

interface PendingWrite {
  cancelled?: boolean;
  clientId: string;
  correlations: ProxyCorrelation[];
  frame: Uint8Array;
  reject: (error: Error) => void;
  resolve: (receipt: ProxyOutboundReceipt) => void;
  settled?: boolean;
}

function correlationsFromToRadio(
  schema: MeshtasticSchema,
  frame: Uint8Array,
): ProxyCorrelation[] {
  const source = decodeToObject(
    schema.toRadio,
    frame,
    "PROXY_OUTBOUND_FRAME_INVALID",
  );
  const variant = source.payloadVariant;
  switch (variant) {
    case "wantConfigId":
      return [
        {
          kind: "config",
          id: requiredCorrelationId(
            source.wantConfigId,
            "PROXY_OUTBOUND_CORRELATION_INVALID",
          ),
        },
      ];
    case "packet":
      return packetCorrelations(source.packet);
    case "disconnect":
    case "heartbeat":
    case "mqttClientProxyMessage":
    case "xmodemPacket":
      return [];
    default:
      throw new ProxyOutboundError("PROXY_OUTBOUND_FRAME_INVALID");
  }
}

function packetCorrelations(value: unknown): ProxyCorrelation[] {
  const packet = asRecord(value);
  if (!packet) {
    throw new ProxyOutboundError("PROXY_OUTBOUND_FRAME_INVALID");
  }
  const correlations: ProxyCorrelation[] = [];
  const decoded = asRecord(packet.decoded);
  const packetId = optionalCorrelationId(packet.id);
  if (decoded?.wantResponse === true) {
    correlations.push({
      kind: "request",
      id: requiredCorrelationId(packetId, "PROXY_REQUEST_CORRELATION_INVALID"),
    });
  }
  if (packet.wantAck === true) {
    correlations.push({
      kind: "ack",
      id: requiredCorrelationId(packetId, "PROXY_ACK_CORRELATION_INVALID"),
    });
  }
  return correlations;
}

function correlationsFromFromRadio(
  schema: MeshtasticSchema,
  frame: Uint8Array,
): ProxyCorrelation[] {
  const source = decodeToObject(
    schema.fromRadio,
    frame,
    "PROXY_INBOUND_FRAME_INVALID",
  );
  switch (source.payloadVariant) {
    case "configCompleteId":
      return [
        {
          kind: "config",
          id: requiredCorrelationId(
            source.configCompleteId,
            "PROXY_INBOUND_FRAME_INVALID",
          ),
        },
      ];
    case "packet": {
      const packet = asRecord(source.packet);
      const decoded = asRecord(packet?.decoded);
      const correlations: ProxyCorrelation[] = [];
      const replyId = optionalCorrelationId(decoded?.replyId);
      if (replyId !== undefined) {
        correlations.push({ kind: "request", id: replyId });
      }
      const requestId = optionalCorrelationId(decoded?.requestId);
      if (requestId !== undefined) {
        correlations.push({ kind: "ack", id: requestId });
      }
      return correlations;
    }
    case "clientNotification": {
      const notification = asRecord(source.clientNotification);
      const replyId = optionalCorrelationId(notification?.replyId);
      return replyId === undefined ? [] : [{ kind: "request", id: replyId }];
    }
    default:
      return [];
  }
}

function decodeToObject(
  type: MeshtasticSchema["toRadio"],
  frame: Uint8Array,
  code: string,
): Record<string, unknown> {
  try {
    const source = asRecord(
      type.toObject(type.decode(frame), TO_OBJECT_OPTIONS),
    );
    if (!source || typeof source.payloadVariant !== "string") {
      throw new ProxyOutboundError(code);
    }
    return source;
  } catch (error) {
    if (error instanceof ProxyOutboundError) {
      throw error;
    }
    throw new ProxyOutboundError(code);
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function optionalCorrelationId(value: unknown): number | undefined {
  return typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 1 &&
    value <= 0xffff_ffff
    ? value
    : undefined;
}

function requiredCorrelationId(value: unknown, code: string): number {
  const id = optionalCorrelationId(value);
  if (id === undefined) {
    throw new ProxyOutboundError(code);
  }
  return id;
}

function correlationKey(correlation: ProxyCorrelation): string {
  return `${correlation.kind}:${correlation.id}`;
}
