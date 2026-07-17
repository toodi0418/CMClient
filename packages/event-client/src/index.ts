import {
  createNetworkError,
  GatewayApiError,
  isGatewayApiError,
  mapGatewayResponseError,
} from "@cmclient/api-client";
import { DomainEventSchema, type DomainEvent } from "@cmclient/contracts";
import { TypeCompiler } from "@sinclair/typebox/compiler";

export const DEFAULT_GATEWAY_EVENTS_URL = "/api/v1/events";
const domainEventValidator = TypeCompiler.Compile(DomainEventSchema);

export type SseConnectionState =
  "idle" | "connecting" | "open" | "reconnecting" | "stopped";

export interface SseReconnectOptions {
  initialDelayMs: number;
  maximumDelayMs: number;
}

export interface GatewayEventClientOptions {
  url?: string;
  fetch?: typeof fetch;
  reconnect?: Partial<SseReconnectOptions>;
  sleep?: (delayMs: number, signal: AbortSignal) => Promise<void>;
}

export interface SseFrame {
  id?: string;
  event?: string;
  data?: string;
}

export class SseFrameParser {
  private buffer = "";

  push(chunk: string): SseFrame[] {
    this.buffer += chunk;
    const frames: SseFrame[] = [];
    let boundary: RegExpExecArray | null;

    while ((boundary = /\r?\n\r?\n/.exec(this.buffer))) {
      const block = this.buffer.slice(0, boundary.index);
      this.buffer = this.buffer.slice(boundary.index + boundary[0].length);
      const frame = parseSseBlock(block);
      if (frame) {
        frames.push(frame);
      }
    }

    return frames;
  }

  finish(): SseFrame[] {
    const remaining = this.buffer;
    this.buffer = "";
    const frame = parseSseBlock(remaining);
    return frame ? [frame] : [];
  }
}

export class GatewayEventClient {
  private readonly url: string;
  private readonly fetchImplementation: typeof fetch;
  private readonly reconnect: SseReconnectOptions;
  private readonly sleep: (
    delayMs: number,
    signal: AbortSignal,
  ) => Promise<void>;
  private readonly eventListeners = new Set<(event: DomainEvent) => void>();
  private readonly errorListeners = new Set<(error: GatewayApiError) => void>();
  private readonly stateListeners = new Set<
    (state: SseConnectionState) => void
  >();
  private controller: AbortController | undefined;
  private running = false;
  private state: SseConnectionState = "idle";
  private lastEventId: string | undefined;

  constructor(options: GatewayEventClientOptions = {}) {
    this.url = validateEventsUrl(options.url ?? DEFAULT_GATEWAY_EVENTS_URL);
    this.fetchImplementation = options.fetch ?? globalThis.fetch;
    this.reconnect = resolveReconnectOptions(options.reconnect);
    this.sleep = options.sleep ?? sleepWithAbort;
  }

  get connectionState(): SseConnectionState {
    return this.state;
  }

  get lastReceivedEventId(): string | undefined {
    return this.lastEventId;
  }

  onEvent(listener: (event: DomainEvent) => void): () => void {
    this.eventListeners.add(listener);
    return () => this.eventListeners.delete(listener);
  }

  onError(listener: (error: GatewayApiError) => void): () => void {
    this.errorListeners.add(listener);
    return () => this.errorListeners.delete(listener);
  }

  onStateChange(listener: (state: SseConnectionState) => void): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    const controller = new AbortController();
    this.controller = controller;
    void this.run(controller);
  }

  stop(): void {
    this.running = false;
    this.controller?.abort();
    this.setState("stopped");
  }

  private async run(controller: AbortController): Promise<void> {
    let attempt = 0;

    try {
      while (this.running && !controller.signal.aborted) {
        this.setState(attempt === 0 ? "connecting" : "reconnecting");

        try {
          const response = await this.openStream(controller.signal);
          if (!response.ok) {
            throw await mapGatewayResponseError(response);
          }
          if (!response.body) {
            throw new GatewayApiError({
              code: "SSE_STREAM_UNAVAILABLE",
              status: response.status,
              retryable: true,
            });
          }

          attempt = 0;
          this.setState("open");
          await this.consume(response.body, controller.signal);
          if (!this.running || controller.signal.aborted) {
            break;
          }
          throw new GatewayApiError({
            code: "SSE_STREAM_CLOSED",
            retryable: true,
          });
        } catch (error) {
          if (!this.running || controller.signal.aborted) {
            break;
          }

          const mapped = isGatewayApiError(error)
            ? error
            : createNetworkError(error);
          this.emitError(mapped);
          attempt += 1;
          this.setState("reconnecting");
          try {
            await this.sleep(
              reconnectDelay(this.reconnect, attempt),
              controller.signal,
            );
          } catch (sleepError) {
            if (!this.running || controller.signal.aborted) {
              break;
            }
            this.emitError(createNetworkError(sleepError));
          }
        }
      }
    } finally {
      if (this.controller === controller) {
        this.controller = undefined;
      }
      if (!this.running) {
        this.setState("stopped");
      }
    }
  }

  private async openStream(signal: AbortSignal): Promise<Response> {
    const headers: Record<string, string> = {
      accept: "text/event-stream",
    };
    if (this.lastEventId) {
      headers["last-event-id"] = this.lastEventId;
    }

    try {
      return await this.fetchImplementation(this.url, {
        headers,
        signal,
      });
    } catch (error) {
      throw createNetworkError(error);
    }
  }

  private async consume(
    body: ReadableStream<Uint8Array>,
    signal: AbortSignal,
  ): Promise<void> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    const parser = new SseFrameParser();

    try {
      while (!signal.aborted) {
        const result = await reader.read();
        if (result.done) {
          break;
        }
        for (const frame of parser.push(
          decoder.decode(result.value, { stream: true }),
        )) {
          this.handleFrame(frame);
        }
      }
      for (const frame of parser.push(decoder.decode())) {
        this.handleFrame(frame);
      }
      for (const frame of parser.finish()) {
        this.handleFrame(frame);
      }
    } finally {
      reader.releaseLock();
    }
  }

  private handleFrame(frame: SseFrame): void {
    if (!frame.data) {
      return;
    }

    let payload: unknown;
    try {
      payload = JSON.parse(frame.data);
    } catch {
      throw new GatewayApiError({ code: "SSE_EVENT_INVALID" });
    }
    if (!domainEventValidator.Check(payload)) {
      throw new GatewayApiError({ code: "SSE_EVENT_INVALID" });
    }
    const event = payload as DomainEvent;
    if (frame.id && frame.id !== event.eventId) {
      throw new GatewayApiError({ code: "SSE_EVENT_ID_MISMATCH" });
    }
    if (frame.event && frame.event !== event.type) {
      throw new GatewayApiError({ code: "SSE_EVENT_TYPE_MISMATCH" });
    }

    this.lastEventId = event.eventId;
    for (const listener of this.eventListeners) {
      listener(event);
    }
  }

  private emitError(error: GatewayApiError): void {
    for (const listener of this.errorListeners) {
      listener(error);
    }
  }

  private setState(state: SseConnectionState): void {
    if (this.state === state) {
      return;
    }
    this.state = state;
    for (const listener of this.stateListeners) {
      listener(state);
    }
  }
}

function parseSseBlock(block: string): SseFrame | undefined {
  if (!block) {
    return undefined;
  }
  const data: string[] = [];
  const frame: SseFrame = {};

  for (const line of block.split(/\r?\n/)) {
    if (!line || line.startsWith(":")) {
      continue;
    }
    const separator = line.indexOf(":");
    const field = separator === -1 ? line : line.slice(0, separator);
    const value =
      separator === -1 ? "" : line.slice(separator + 1).replace(/^ /, "");

    if (field === "id") {
      frame.id = value;
    } else if (field === "event") {
      frame.event = value;
    } else if (field === "data") {
      data.push(value);
    }
  }

  if (data.length > 0) {
    frame.data = data.join("\n");
  }
  return frame.data || frame.id || frame.event ? frame : undefined;
}

function validateEventsUrl(url: string): string {
  const normalized = url.trim();
  if (!normalized.startsWith("/")) {
    throw new TypeError("Gateway event URL must be an absolute path");
  }
  return normalized;
}

function resolveReconnectOptions(
  options: Partial<SseReconnectOptions> | undefined,
): SseReconnectOptions {
  const resolved = {
    initialDelayMs: options?.initialDelayMs ?? 500,
    maximumDelayMs: options?.maximumDelayMs ?? 30_000,
  };
  if (
    !Number.isInteger(resolved.initialDelayMs) ||
    !Number.isInteger(resolved.maximumDelayMs) ||
    resolved.initialDelayMs < 0 ||
    resolved.maximumDelayMs < resolved.initialDelayMs
  ) {
    throw new RangeError("SSE reconnect delays are invalid");
  }
  return resolved;
}

function reconnectDelay(options: SseReconnectOptions, attempt: number): number {
  return Math.min(
    options.maximumDelayMs,
    options.initialDelayMs * 2 ** Math.max(0, attempt - 1),
  );
}

function sleepWithAbort(delayMs: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      clearTimeout(timer);
      reject(new DOMException("Aborted", "AbortError"));
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, delayMs);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
