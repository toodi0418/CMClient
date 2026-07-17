import { randomUUID } from "node:crypto";

import type { DomainEvent } from "@cmclient/contracts";

const EVENT_NAME = /^[a-z][a-z0-9_.-]{0,127}$/;

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
  private readonly events: DomainEvent[] = [];
  private readonly listeners = new Set<DomainEventListener>();

  constructor(options: DomainEventBusOptions = {}) {
    const bufferSize = options.bufferSize ?? 1_000;
    if (!Number.isInteger(bufferSize) || bufferSize < 1) {
      throw new RangeError(
        "Event replay buffer size must be a positive integer",
      );
    }
    this.bufferSize = bufferSize;
    this.clock = options.clock ?? (() => new Date());
    this.eventIdFactory = options.eventIdFactory ?? randomUUID;
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

    const event: DomainEvent = {
      eventId,
      schemaVersion: 1,
      type: input.type,
      occurredAt: (input.occurredAt ?? this.clock()).toISOString(),
      source: input.source,
      ...(input.correlationId ? { correlationId: input.correlationId } : {}),
      payload: clonePayload(input.payload),
    };
    this.events.push(event);
    if (this.events.length > this.bufferSize) {
      this.events.splice(0, this.events.length - this.bufferSize);
    }
    for (const listener of this.listeners) {
      listener(event);
    }
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

  subscribe(listener: DomainEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  get replayBufferSize(): number {
    return this.events.length;
  }
}

export function formatSseEvent(event: DomainEvent): string {
  return `id: ${event.eventId}\nevent: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`;
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
  return JSON.parse(serialized) as Record<string, unknown>;
}
