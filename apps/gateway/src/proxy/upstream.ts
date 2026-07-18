import { createHash } from "node:crypto";

import type {
  TransportConnectionState,
  TransportMetrics,
} from "@cmclient/contracts";

import {
  MeshtasticFrameDecoder,
  encodeMeshtasticFrame,
  type FrameDecoderMetrics,
} from "../transport/framing.js";
import type {
  MeshtasticTransport,
  TransportEvent,
} from "../transport/types.js";
import type { MeshtasticSchema } from "../protobuf/schema.js";

const TO_OBJECT_OPTIONS = {
  bytes: Uint8Array,
  defaults: false,
  enums: String,
  longs: Number,
  oneofs: true,
};

export type ProxyConfigFrameKind =
  "my_info" | "node_info" | "config" | "module_config" | "channel" | "metadata";

export interface ProxyConfigFrame {
  kind: ProxyConfigFrameKind;
  frame: Uint8Array;
  receivedAt: string;
}

export interface ProxyUpstreamSnapshot {
  state: TransportConnectionState;
  metrics: TransportMetrics;
  configFrameCount: number;
  lastErrorCode?: string;
}

export type ProxyUpstreamEvent =
  | { kind: "state"; state: TransportConnectionState }
  | { kind: "frame"; frame: Uint8Array; receivedAt: string }
  | { kind: "error"; code: string };

export type ProxyUpstreamListener = (event: ProxyUpstreamEvent) => void;

export interface ProxyConfigCacheOptions {
  maxEntries?: number;
}

export class ProxyFrameCodec {
  private readonly decoder: MeshtasticFrameDecoder;
  private readonly maxPayloadBytes: number;

  constructor(options: { maxPayloadBytes?: number } = {}) {
    this.maxPayloadBytes = options.maxPayloadBytes ?? 512;
    this.decoder = new MeshtasticFrameDecoder({
      maxPayloadBytes: this.maxPayloadBytes,
    });
  }

  decode(chunk: Uint8Array): Uint8Array[] {
    return this.decoder.push(chunk);
  }

  encode(payload: Uint8Array): Uint8Array {
    return encodeMeshtasticFrame(payload, this.maxPayloadBytes);
  }

  get metrics(): FrameDecoderMetrics {
    return this.decoder.metrics;
  }
}

export class ProxyConfigCache {
  private readonly entries = new Map<string, ProxyConfigFrame>();
  private readonly maxEntries: number;

  constructor(
    private readonly schema: MeshtasticSchema,
    options: ProxyConfigCacheOptions = {},
  ) {
    this.maxEntries = options.maxEntries ?? 512;
    if (
      !Number.isInteger(this.maxEntries) ||
      this.maxEntries < 1 ||
      this.maxEntries > 4_096
    ) {
      throw new ProxyConfigCacheError("PROXY_CONFIG_CACHE_INVALID");
    }
  }

  observe(frame: Uint8Array, receivedAt: string): boolean {
    const classified = classifyConfigFrame(this.schema, frame);
    if (!classified || !isUtcTimestamp(receivedAt)) {
      return false;
    }
    if (
      !this.entries.has(classified.key) &&
      this.entries.size >= this.maxEntries
    ) {
      throw new ProxyConfigCacheError("PROXY_CONFIG_CACHE_FULL");
    }
    this.entries.set(classified.key, {
      kind: classified.kind,
      frame: new Uint8Array(frame),
      receivedAt,
    });
    return true;
  }

  clear(): void {
    this.entries.clear();
  }

  snapshot(): ProxyConfigFrame[] {
    return [...this.entries.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([, entry]) => ({ ...entry, frame: new Uint8Array(entry.frame) }));
  }
}

export class ProxyUpstreamManager {
  private readonly listeners = new Set<ProxyUpstreamListener>();
  private unsubscribe: (() => void) | undefined;
  private started = false;
  private lastErrorCode: string | undefined;

  constructor(
    private readonly transport: MeshtasticTransport,
    readonly configCache: ProxyConfigCache,
  ) {}

  get snapshot(): ProxyUpstreamSnapshot {
    return {
      state: this.transport.state,
      metrics: this.transport.metrics,
      configFrameCount: this.configCache.snapshot().length,
      ...(this.lastErrorCode ? { lastErrorCode: this.lastErrorCode } : {}),
    };
  }

  async start(): Promise<void> {
    if (this.started) {
      return;
    }
    this.started = true;
    this.unsubscribe = this.transport.subscribe((event) =>
      this.onTransport(event),
    );
    try {
      await this.transport.connect();
    } catch (error) {
      this.unsubscribe?.();
      this.unsubscribe = undefined;
      this.started = false;
      throw error;
    }
  }

  async stop(): Promise<void> {
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    this.started = false;
    this.lastErrorCode = undefined;
    this.configCache.clear();
    await this.transport.disconnect();
  }

  subscribe(listener: ProxyUpstreamListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private onTransport(event: TransportEvent): void {
    if (event.kind === "state") {
      if (event.state.status === "configuring") {
        this.configCache.clear();
      }
      this.emit({ kind: "state", state: event.state });
      return;
    }
    if (event.kind === "error") {
      this.lastErrorCode = event.code;
      this.emit({ kind: "error", code: event.code });
      return;
    }
    try {
      this.configCache.observe(event.frame, event.receivedAt);
    } catch (error) {
      this.lastErrorCode =
        error instanceof ProxyConfigCacheError
          ? error.code
          : "PROXY_CONFIG_CACHE_FAILED";
      this.emit({ kind: "error", code: this.lastErrorCode });
      return;
    }
    this.emit({
      kind: "frame",
      frame: new Uint8Array(event.frame),
      receivedAt: event.receivedAt,
    });
  }

  private emit(event: ProxyUpstreamEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

export class ProxyConfigCacheError extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

function classifyConfigFrame(
  schema: MeshtasticSchema,
  frame: Uint8Array,
): { key: string; kind: ProxyConfigFrameKind } | undefined {
  try {
    const source = asRecord(
      schema.fromRadio.toObject(
        schema.fromRadio.decode(frame),
        TO_OBJECT_OPTIONS,
      ),
    );
    if (!source) {
      return undefined;
    }
    const variant = source.payloadVariant;
    switch (variant) {
      case "myInfo":
        return { key: "my_info", kind: "my_info" };
      case "nodeInfo":
        return {
          key: `node_info:${identityFrom(source.nodeInfo, "num", frame)}`,
          kind: "node_info",
        };
      case "config":
        return { key: "config", kind: "config" };
      case "moduleConfig":
        return { key: "module_config", kind: "module_config" };
      case "channel":
        return {
          key: `channel:${identityFrom(source.channel, "index", frame)}`,
          kind: "channel",
        };
      case "metadata":
        return { key: "metadata", kind: "metadata" };
      default:
        return undefined;
    }
  } catch {
    return undefined;
  }
}

function identityFrom(source: unknown, key: string, frame: Uint8Array): string {
  const value = asRecord(source)?.[key];
  if (typeof value === "number" && Number.isInteger(value) && value >= 0) {
    return String(value);
  }
  return createHash("sha256").update(frame).digest("hex");
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function isUtcTimestamp(value: string): boolean {
  return (
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value) &&
    !Number.isNaN(Date.parse(value))
  );
}
