import type { ProxyConfigCache, ProxyUpstreamEvent } from "./upstream.js";

export interface ProxyClientSink {
  id: string;
  close(code: string): void;
  write(frame: Uint8Array): Promise<void>;
}

export interface ProxyUpstreamFrameSource {
  configCache: Pick<ProxyConfigCache, "snapshot">;
  subscribe(listener: (event: ProxyUpstreamEvent) => void): () => void;
}

export interface ProxySessionManagerOptions {
  maxClients?: number;
  maxQueuedBytes?: number;
  maxQueuedFrames?: number;
}

export type ProxyUpstreamEventRouter = (event: ProxyUpstreamEvent) => boolean;

export interface ProxyBroadcastResult {
  accepted: number;
  dropped: number;
}

export interface ProxySessionSnapshot {
  id: string;
  queuedBytes: number;
  queuedFrames: number;
}

export class ProxySessionError extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

export class ProxySessionManager {
  private readonly sessions = new Map<string, ProxyClientSession>();
  private readonly maxClients: number;
  private readonly maxQueuedBytes: number;
  private readonly maxQueuedFrames: number;
  private unsubscribe: (() => void) | undefined;

  constructor(
    private readonly upstream: ProxyUpstreamFrameSource,
    options: ProxySessionManagerOptions = {},
  ) {
    this.maxClients = options.maxClients ?? 16;
    this.maxQueuedBytes = options.maxQueuedBytes ?? 256 * 1024;
    this.maxQueuedFrames = options.maxQueuedFrames ?? 128;
    if (
      !Number.isInteger(this.maxClients) ||
      this.maxClients < 1 ||
      this.maxClients > 256 ||
      !Number.isInteger(this.maxQueuedBytes) ||
      this.maxQueuedBytes < 1_024 ||
      this.maxQueuedBytes > 16 * 1024 * 1024 ||
      !Number.isInteger(this.maxQueuedFrames) ||
      this.maxQueuedFrames < 1 ||
      this.maxQueuedFrames > 4_096
    ) {
      throw new ProxySessionError("PROXY_SESSION_CONFIGURATION_INVALID");
    }
  }

  start(routeEvent?: ProxyUpstreamEventRouter): void {
    if (!this.unsubscribe) {
      this.unsubscribe = this.upstream.subscribe((event) => {
        if (routeEvent?.(event)) {
          return;
        }
        if (event.kind === "frame") {
          this.broadcast(event.frame);
        }
      });
    }
  }

  stop(): void {
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    for (const session of this.sessions.values()) {
      session.close("PROXY_SESSION_MANAGER_STOPPED");
    }
    this.sessions.clear();
  }

  attach(sink: ProxyClientSink): ProxySessionSnapshot {
    if (!/^[a-zA-Z0-9._:-]{1,128}$/.test(sink.id)) {
      throw new ProxySessionError("PROXY_CLIENT_ID_INVALID");
    }
    if (this.sessions.has(sink.id)) {
      throw new ProxySessionError("PROXY_CLIENT_DUPLICATE");
    }
    if (this.sessions.size >= this.maxClients) {
      throw new ProxySessionError("PROXY_CLIENT_LIMIT_REACHED");
    }
    const session = new ProxyClientSession(
      sink,
      this.maxQueuedFrames,
      this.maxQueuedBytes,
      () => this.sessions.delete(sink.id),
    );
    this.sessions.set(sink.id, session);
    for (const entry of this.upstream.configCache.snapshot()) {
      session.enqueue(entry.frame);
      if (session.closed) {
        break;
      }
    }
    return session.snapshot;
  }

  detach(id: string, code = "PROXY_CLIENT_DISCONNECTED"): void {
    const session = this.sessions.get(id);
    if (session) {
      session.close(code);
    }
  }

  broadcast(frame: Uint8Array): ProxyBroadcastResult {
    let accepted = 0;
    let dropped = 0;
    for (const session of this.sessions.values()) {
      if (session.enqueue(frame)) {
        accepted += 1;
      } else {
        dropped += 1;
      }
    }
    return { accepted, dropped };
  }

  deliver(id: string, frame: Uint8Array): boolean {
    return this.sessions.get(id)?.enqueue(frame) ?? false;
  }

  get snapshot(): ProxySessionSnapshot[] {
    return [...this.sessions.values()]
      .map((session) => session.snapshot)
      .sort((left, right) => left.id.localeCompare(right.id));
  }
}

class ProxyClientSession {
  private readonly queue: Uint8Array[] = [];
  private queuedBytes = 0;
  private draining = false;
  closed = false;

  constructor(
    private readonly sink: ProxyClientSink,
    private readonly maxQueuedFrames: number,
    private readonly maxQueuedBytes: number,
    private readonly onClose: () => void,
  ) {}

  get snapshot(): ProxySessionSnapshot {
    return {
      id: this.sink.id,
      queuedBytes: this.queuedBytes,
      queuedFrames: this.queue.length,
    };
  }

  enqueue(frame: Uint8Array): boolean {
    if (this.closed) {
      return false;
    }
    if (
      frame.length === 0 ||
      this.queue.length >= this.maxQueuedFrames ||
      this.queuedBytes + frame.length > this.maxQueuedBytes
    ) {
      this.close("PROXY_CLIENT_BACKPRESSURE");
      return false;
    }
    this.queue.push(new Uint8Array(frame));
    this.queuedBytes += frame.length;
    void this.drain();
    return true;
  }

  close(code: string): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.queue.length = 0;
    this.queuedBytes = 0;
    this.sink.close(code);
    this.onClose();
  }

  private async drain(): Promise<void> {
    if (this.draining || this.closed) {
      return;
    }
    this.draining = true;
    try {
      while (!this.closed && this.queue.length) {
        const frame = this.queue[0];
        if (!frame) {
          return;
        }
        try {
          await this.sink.write(frame);
          this.queue.shift();
          this.queuedBytes -= frame.length;
        } catch {
          this.close("PROXY_CLIENT_WRITE_FAILED");
        }
      }
    } finally {
      this.draining = false;
    }
  }
}
