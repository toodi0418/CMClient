import {
  createNetworkError,
  GatewayApiError,
  isGatewayApiError,
  mapGatewayResponseError,
} from "@cmclient/api-client";
import { DomainEventSchema, type DomainEvent } from "@cmclient/contracts";
import { Value } from "@sinclair/typebox/value";

export const DEFAULT_GATEWAY_EVENTS_URL = "/api/v1/events";
export const DEFAULT_SSE_FRAME_MAX_BYTES = 60 * 1024;
const PARSER_INPUT_SLICE_CODE_UNITS = 8 * 1024;

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

export interface SseFrameParserOptions {
  maxFrameBytes?: number;
}

export class SseFrameParser {
  private buffer = "";
  private readonly maxFrameBytes: number;

  constructor(options: SseFrameParserOptions = {}) {
    this.maxFrameBytes = options.maxFrameBytes ?? DEFAULT_SSE_FRAME_MAX_BYTES;
    if (
      !Number.isInteger(this.maxFrameBytes) ||
      this.maxFrameBytes < 1 ||
      this.maxFrameBytes > DEFAULT_SSE_FRAME_MAX_BYTES
    ) {
      throw new RangeError("SSE frame byte limit is invalid");
    }
  }

  push(chunk: string): SseFrame[] {
    const frames: SseFrame[] = [];
    for (
      let offset = 0;
      offset < chunk.length;
      offset += PARSER_INPUT_SLICE_CODE_UNITS
    ) {
      this.buffer += chunk.slice(
        offset,
        offset + PARSER_INPUT_SLICE_CODE_UNITS,
      );
      let boundary: RegExpExecArray | null;
      while ((boundary = /\r?\n\r?\n/.exec(this.buffer))) {
        const consumedLength = boundary.index + boundary[0].length;
        const block = this.buffer.slice(0, boundary.index);
        if (
          utf8ByteLength(this.buffer.slice(0, consumedLength)) >
          this.maxFrameBytes
        ) {
          this.buffer = "";
          throw new GatewayApiError({ code: "SSE_EVENT_TOO_LARGE" });
        }
        this.buffer = this.buffer.slice(consumedLength);
        const frame = parseSseBlock(block);
        if (frame) {
          frames.push(frame);
        }
      }
      if (utf8ByteLength(this.buffer) > this.maxFrameBytes) {
        this.buffer = "";
        throw new GatewayApiError({ code: "SSE_EVENT_TOO_LARGE" });
      }
    }

    return frames;
  }

  finish(): SseFrame[] {
    const remaining = this.buffer;
    this.buffer = "";
    if (utf8ByteLength(remaining) > this.maxFrameBytes) {
      throw new GatewayApiError({ code: "SSE_EVENT_TOO_LARGE" });
    }
    const frame = parseSseBlock(remaining);
    return frame ? [frame] : [];
  }

  get bufferedBytes(): number {
    return utf8ByteLength(this.buffer);
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
    this.fetchImplementation =
      options.fetch ?? ((input, init) => globalThis.fetch(input, init));
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
    void this.run(controller).catch((error: unknown) => {
      if (!this.running || controller.signal.aborted) {
        return;
      }
      this.running = false;
      this.emitError(
        isGatewayApiError(error) ? error : createNetworkError(error),
      );
      this.setState("stopped");
    });
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
        if (!this.running || controller.signal.aborted) {
          break;
        }

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
          if (!this.running || controller.signal.aborted) {
            break;
          }
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
    if (!Value.Check(DomainEventSchema, payload)) {
      throw new GatewayApiError({ code: "SSE_EVENT_INVALID" });
    }
    const event = deepFreeze(payload as DomainEvent);
    if (frame.id && frame.id !== event.eventId) {
      throw new GatewayApiError({ code: "SSE_EVENT_ID_MISMATCH" });
    }
    if (frame.event && frame.event !== event.type) {
      throw new GatewayApiError({ code: "SSE_EVENT_TYPE_MISMATCH" });
    }

    this.lastEventId = event.eventId;
    notifyListeners(this.eventListeners, event);
  }

  private emitError(error: GatewayApiError): void {
    notifyListeners(this.errorListeners, error);
  }

  private setState(state: SseConnectionState): void {
    if (this.state === state) {
      return;
    }
    this.state = state;
    notifyListeners(this.stateListeners, state);
  }
}

function notifyListeners<T>(
  listeners: Set<(value: T) => void>,
  value: T,
): void {
  for (const listener of [...listeners]) {
    try {
      listener(value);
    } catch {
      // Observer failures must not disrupt the stream or later observers.
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

function utf8ByteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength;
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
