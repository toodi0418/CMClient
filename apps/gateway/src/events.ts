import { randomUUID } from "node:crypto";

import type { DomainEvent } from "@cmclient/contracts";

const EVENT_NAME = /^[a-z][a-z0-9_.-]{0,127}$/;
export const DEFAULT_EVENT_PAYLOAD_MAX_BYTES = 56 * 1024;
export const DEFAULT_SSE_FRAME_MAX_BYTES = 60 * 1024;
export const DEFAULT_EVENT_SUBSCRIBER_MAX = 128;

export interface DomainEventListenerFailure {
  code: "EVENT_LISTENER_FAILED";
  eventId: string;
  source: string;
  type: string;
}

export interface DomainEventBusMetrics {
  listenerDeliveries: number;
  listenerErrors: number;
  payloadRejections: number;
  published: number;
  replayBufferSize: number;
  subscriberCount: number;
  subscriberRejections: number;
}

export interface DomainEventInput {
  type: string;
  source: string;
  payload: Record<string, unknown>;
  correlationId?: string;
  occurredAt?: Date;
}

export interface DomainEventBusOptions {
  bufferSize?: number;
  clock?: () => Date;
  eventIdFactory?: () => string;
  maxPayloadBytes?: number;
  maxSubscribers?: number;
  onListenerError?: (failure: DomainEventListenerFailure) => void;
}

export type DomainEventListener = (event: DomainEvent) => void;

/**
 * A bounded, process-local journal. Durable event history is added by the job
 * and domain persistence layers; this buffer only bridges short SSE reconnects.
 */
export class DomainEventBus {
  private readonly bufferSize: number;
  private readonly clock: () => Date;
  private readonly eventIdFactory: () => string;
  private readonly maxPayloadBytes: number;
  private readonly maxSubscribers: number;
  private readonly onListenerError:
    ((failure: DomainEventListenerFailure) => void) | undefined;
  private readonly events: DomainEvent[] = [];
  private readonly listeners = new Set<DomainEventListener>();
  private published = 0;
  private payloadRejections = 0;
  private listenerDeliveries = 0;
  private listenerErrors = 0;
  private subscriberRejections = 0;

  constructor(options: DomainEventBusOptions = {}) {
    const bufferSize = options.bufferSize ?? 1_000;
    if (!Number.isInteger(bufferSize) || bufferSize < 1) {
      throw new RangeError(
        "Event replay buffer size must be a positive integer",
      );
    }
    this.bufferSize = bufferSize;
    const maxPayloadBytes =
      options.maxPayloadBytes ?? DEFAULT_EVENT_PAYLOAD_MAX_BYTES;
    if (
      !Number.isInteger(maxPayloadBytes) ||
      maxPayloadBytes < 1 ||
      maxPayloadBytes > DEFAULT_EVENT_PAYLOAD_MAX_BYTES
    ) {
      throw new RangeError("Event payload byte limit is invalid");
    }
    this.maxPayloadBytes = maxPayloadBytes;
    const maxSubscribers =
      options.maxSubscribers ?? DEFAULT_EVENT_SUBSCRIBER_MAX;
    if (
      !Number.isInteger(maxSubscribers) ||
      maxSubscribers < 1 ||
      maxSubscribers > 4_096
    ) {
      throw new RangeError("Event subscriber limit is invalid");
    }
    this.maxSubscribers = maxSubscribers;
    this.clock = options.clock ?? (() => new Date());
    this.eventIdFactory = options.eventIdFactory ?? randomUUID;
    this.onListenerError = options.onListenerError;
  }

  publish(input: DomainEventInput): DomainEvent {
    validateEventName("type", input.type);
    validateEventName("source", input.source);
    if (
      input.correlationId !== undefined &&
      !/^[a-zA-Z0-9._:-]{1,128}$/.test(input.correlationId)
    ) {
      throw new TypeError("Event correlationId is invalid");
    }

    const eventId = this.eventIdFactory();
    if (!/^[a-zA-Z0-9-]{1,128}$/.test(eventId)) {
      throw new TypeError("Event ID factory returned an invalid ID");
    }

    let payload: Record<string, unknown>;
    try {
      payload = clonePayload(input.payload, this.maxPayloadBytes);
    } catch (error) {
      this.payloadRejections += 1;
      throw error;
    }
    const event = deepFreeze({
      eventId,
      schemaVersion: 1,
      type: input.type,
      occurredAt: (input.occurredAt ?? this.clock()).toISOString(),
      source: input.source,
      ...(input.correlationId ? { correlationId: input.correlationId } : {}),
      payload,
    }) as DomainEvent;
    try {
      formatSseEvent(event);
    } catch (error) {
      this.payloadRejections += 1;
      throw error;
    }
    this.events.push(event);
    if (this.events.length > this.bufferSize) {
      this.events.splice(0, this.events.length - this.bufferSize);
    }
    for (const listener of [...this.listeners]) {
      try {
        listener(event);
        this.listenerDeliveries += 1;
      } catch {
        this.listenerErrors += 1;
        try {
          this.onListenerError?.({
            code: "EVENT_LISTENER_FAILED",
            eventId: event.eventId,
            source: event.source,
            type: event.type,
          });
        } catch {
          // Observability hooks cannot disrupt event delivery.
        }
      }
    }
    this.published += 1;
    return event;
  }

  replayAfter(lastEventId?: string): DomainEvent[] {
    if (!lastEventId) {
      return [];
    }
    const index = this.events.findIndex(
      (event) => event.eventId === lastEventId,
    );
    return index === -1 ? [...this.events] : this.events.slice(index + 1);
  }

  recent(limit = 100): DomainEvent[] {
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
      throw new RangeError("Event projection limit is invalid");
    }
    return this.events
      .slice(-limit)
      .reverse()
      .map((event) => structuredClone(event));
  }

  subscribe(listener: DomainEventListener): () => void {
    if (!this.listeners.has(listener)) {
      this.assertSubscriberCapacity();
    }
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  assertSubscriberCapacity(): void {
    if (!this.hasSubscriberCapacity) {
      this.subscriberRejections += 1;
      throw new DomainEventSubscriberLimitError();
    }
  }

  get hasSubscriberCapacity(): boolean {
    return this.listeners.size < this.maxSubscribers;
  }

  get replayBufferSize(): number {
    return this.events.length;
  }

  get metricsSnapshot(): DomainEventBusMetrics {
    return {
      listenerDeliveries: this.listenerDeliveries,
      listenerErrors: this.listenerErrors,
      payloadRejections: this.payloadRejections,
      published: this.published,
      replayBufferSize: this.events.length,
      subscriberCount: this.listeners.size,
      subscriberRejections: this.subscriberRejections,
    };
  }
}

export class DomainEventSubscriberLimitError extends Error {
  readonly code = "SSE_SUBSCRIBER_LIMIT_REACHED";

  constructor() {
    super("SSE_SUBSCRIBER_LIMIT_REACHED");
    this.name = "DomainEventSubscriberLimitError";
  }
}

export function formatSseEvent(
  event: DomainEvent,
  maxFrameBytes = DEFAULT_SSE_FRAME_MAX_BYTES,
): string {
  if (
    !Number.isInteger(maxFrameBytes) ||
    maxFrameBytes < 1 ||
    maxFrameBytes > DEFAULT_SSE_FRAME_MAX_BYTES
  ) {
    throw new RangeError("SSE frame byte limit is invalid");
  }
  const frame = `id: ${event.eventId}\nevent: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`;
  if (Buffer.byteLength(frame, "utf8") > maxFrameBytes) {
    throw new RangeError("SSE frame exceeds byte limit");
  }
  return frame;
}

export function formatSseHeartbeat(): string {
  return ": heartbeat\n\n";
}

function validateEventName(label: string, value: string): void {
  if (!EVENT_NAME.test(value)) {
    throw new TypeError(`Event ${label} is invalid`);
  }
}

function clonePayload(
  payload: Record<string, unknown>,
  maxPayloadBytes: number,
): Record<string, unknown> {
  if (!payload || Array.isArray(payload)) {
    throw new TypeError("Event payload must be an object");
  }
  let serialized: string | undefined;
  try {
    serialized = JSON.stringify(payload);
  } catch {
    throw new TypeError("Event payload must be JSON serializable");
  }
  if (!serialized) {
    throw new TypeError("Event payload must be JSON serializable");
  }
  if (Buffer.byteLength(serialized, "utf8") > maxPayloadBytes) {
    throw new RangeError("Event payload exceeds byte limit");
  }
  return JSON.parse(serialized) as Record<string, unknown>;
}

function deepFreeze<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    for (const nested of Object.values(value)) {
      deepFreeze(nested);
    }
    Object.freeze(value);
  }
  return value;
}
