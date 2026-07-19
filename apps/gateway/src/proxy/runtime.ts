import net, { type Server, type Socket } from "node:net";

import type { ProxyStatus } from "@cmclient/contracts";

import type { DomainEventBus } from "../events.js";
import type { MeshtasticSchema } from "../protobuf/schema.js";
import { ProxyOutboundRouter, type ProxyOutboundEvent } from "./outbound.js";
import { ProxyAccessController } from "./policy.js";
import {
  ProxyFrameCodec,
  ProxyUpstreamManager,
  type ProxyUpstreamEvent,
} from "./upstream.js";
import { ProxySessionManager, type ProxyClientSink } from "./sessions.js";

export interface ProxyRuntimeOptions {
  eventBus?: DomainEventBus;
  frameMaxPayloadBytes?: number;
  listenPort?: number;
  policy: ProxyAccessController;
  schema: MeshtasticSchema;
  stopTimeoutMs?: number;
  upstream: ProxyUpstreamManager;
}

export class ProxyRuntimeError extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

/**
 * Composes exactly one Meshtastic upstream with bounded local TCP sessions.
 * No local client receives the upstream socket or bypasses policy/router paths.
 */
export class ProxyRuntime {
  readonly outbound: ProxyOutboundRouter;
  readonly sessions: ProxySessionManager;
  private readonly listener: ProxyTcpListener;
  private readonly unsubscribeUpstream: () => void;
  private readonly eventBus: DomainEventBus | undefined;
  private readonly stopTimeoutMs: number;
  private state: ProxyStatus["state"] = "stopped";
  private lastErrorCode: string | undefined;
  private startPromise: Promise<ProxyStatus> | undefined;
  private stopPromise: Promise<void> | undefined;
  private cleanupComplete = false;
  private lifecycleGeneration = 0;
  private startedOnce = false;
  private stopping = false;

  constructor(private readonly options: ProxyRuntimeOptions) {
    if (
      !Number.isInteger(options.listenPort ?? 0) ||
      (options.listenPort ?? 0) < 0 ||
      (options.listenPort ?? 0) > 65_535
    ) {
      throw new ProxyRuntimeError("PROXY_LISTEN_CONFIGURATION_INVALID");
    }
    this.stopTimeoutMs = options.stopTimeoutMs ?? 10_000;
    if (
      !Number.isInteger(this.stopTimeoutMs) ||
      this.stopTimeoutMs < 10 ||
      this.stopTimeoutMs > 120_000
    ) {
      throw new ProxyRuntimeError("PROXY_STOP_TIMEOUT_INVALID");
    }
    this.eventBus = options.eventBus;
    this.sessions = new ProxySessionManager(options.upstream, {
      maxClients: options.policy.snapshot.maxClients,
    });
    this.outbound = new ProxyOutboundRouter(
      options.schema,
      options.upstream,
      this.sessions,
      {
        authorizer: options.policy,
        ...(options.frameMaxPayloadBytes === undefined
          ? {}
          : { maxFrameBytes: options.frameMaxPayloadBytes }),
      },
    );
    this.listener = new ProxyTcpListener({
      host: options.policy.snapshot.bindHost,
      port: options.listenPort ?? 0,
      policy: options.policy,
      router: this.outbound,
      sessions: this.sessions,
      onClient: (kind, code) => {
        this.publish("proxy.client", { kind, ...(code ? { code } : {}) });
      },
      onBackpressure: (code) => {
        this.lastErrorCode = code;
        this.publish("proxy.backpressure", { code });
      },
      onCommandRejected: (code) => {
        this.lastErrorCode = code;
        this.publish("proxy.queue", { kind: "rejected", code });
      },
      onError: (code) => {
        this.lastErrorCode = code;
        this.state = "degraded";
        this.publish("proxy.error", { code });
      },
      ...(options.frameMaxPayloadBytes === undefined
        ? {}
        : { frameMaxPayloadBytes: options.frameMaxPayloadBytes }),
    });
    this.outbound.subscribe((event) => this.onOutboundEvent(event));
    this.unsubscribeUpstream = options.upstream.subscribe((event) =>
      this.onUpstreamEvent(event),
    );
  }

  status(): ProxyStatus {
    const policy = this.options.policy.snapshot;
    const upstream = this.options.upstream.snapshot;
    const queue = this.outbound.snapshot;
    const sessions = this.sessions.metricsSnapshot;
    return {
      state: this.state,
      listener: {
        host: policy.bindHost,
        port: this.listener.port,
      },
      policy: {
        activeClients: policy.activeClientIds.length,
        allowLan: policy.allowLan,
        allowedAddressCount: policy.allowedAddressCount,
        maxClients: policy.maxClients,
        maxWritesPerMinute: policy.maxWritesPerMinute,
        mode: policy.mode,
      },
      queue: {
        ...queue,
        ...sessions,
      },
      recentAudit: this.options.policy.auditSnapshot().slice(-50),
      upstream,
      ...(this.lastErrorCode ? { lastErrorCode: this.lastErrorCode } : {}),
    };
  }

  start(): Promise<ProxyStatus> {
    if (this.state === "running") {
      return Promise.resolve(this.status());
    }
    if (this.startPromise) {
      return this.startPromise;
    }
    if (this.startedOnce) {
      return Promise.reject(new ProxyRuntimeError("PROXY_RUNTIME_STOPPED"));
    }
    this.startedOnce = true;
    const generation = ++this.lifecycleGeneration;
    const startPromise = this.startInternal(generation).finally(() => {
      if (this.startPromise === startPromise) {
        this.startPromise = undefined;
      }
    });
    this.startPromise = startPromise;
    return startPromise;
  }

  stop(): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }
    if (this.cleanupComplete) {
      return Promise.resolve();
    }
    this.startedOnce = true;
    this.stopping = true;
    this.lifecycleGeneration += 1;
    const pendingStart = this.startPromise;
    const stopPromise = this.stopInternal(pendingStart).finally(() => {
      if (this.startPromise === pendingStart) {
        this.startPromise = undefined;
      }
      if (this.stopPromise === stopPromise) {
        this.stopPromise = undefined;
      }
    });
    this.stopPromise = stopPromise;
    return stopPromise;
  }

  private async stopInternal(
    pendingStart: Promise<ProxyStatus> | undefined,
  ): Promise<void> {
    const deadline = Date.now() + this.stopTimeoutMs;
    let stopError: unknown;
    try {
      await settleBefore(this.options.upstream.stop(), deadline);
    } catch (error) {
      stopError = error;
    }
    if (pendingStart) {
      try {
        await settleBefore(pendingStart, deadline);
      } catch (error) {
        if (
          error instanceof ProxyRuntimeError &&
          error.code === "PROXY_STOP_TIMEOUT"
        ) {
          stopError ??= error;
        }
      }
      try {
        await settleBefore(this.options.upstream.stop(), deadline);
      } catch (error) {
        stopError ??= error;
      }
    }
    try {
      await settleBefore(this.listener.stop(), deadline);
    } catch (error) {
      stopError ??= error;
    }
    this.sessions.stop();
    this.outbound.stop();
    this.unsubscribeUpstream();
    this.state = "stopped";
    this.cleanupComplete = true;
    this.publish("proxy.stopped", {});
    if (stopError) {
      throw new ProxyRuntimeError(errorCode(stopError, "PROXY_STOP_FAILED"));
    }
  }

  private async startInternal(generation: number): Promise<ProxyStatus> {
    this.state = "starting";
    try {
      await this.options.upstream.start();
      this.assertActiveGeneration(generation);
      this.sessions.start((event) => this.outbound.handleUpstreamEvent(event));
      await this.listener.start();
      this.assertActiveGeneration(generation);
      this.state = "running";
      this.publish("proxy.started", {
        port: this.listener.port,
        mode: this.options.policy.snapshot.mode,
      });
      return this.status();
    } catch (error) {
      if (!this.isActiveGeneration(generation)) {
        this.sessions.stop();
        await Promise.allSettled([
          this.listener.stop(),
          this.options.upstream.stop(),
        ]);
        throw new ProxyRuntimeError("PROXY_RUNTIME_STOPPED");
      }
      this.lastErrorCode = errorCode(error, "PROXY_START_FAILED");
      this.state = "degraded";
      this.sessions.stop();
      await this.options.upstream.stop();
      this.publish("proxy.error", { code: this.lastErrorCode });
      throw new ProxyRuntimeError(this.lastErrorCode);
    }
  }

  private onUpstreamEvent(event: ProxyUpstreamEvent): void {
    if (this.stopping || this.state === "stopped") {
      return;
    }
    if (event.kind === "error") {
      this.lastErrorCode = event.code;
      this.publish("proxy.upstream", { kind: "error", code: event.code });
      return;
    }
    if (event.kind === "state") {
      if (event.state.status === "ready") {
        this.state = "running";
      } else if (
        event.state.status === "backoff" ||
        event.state.status === "degraded" ||
        event.state.status === "disconnected"
      ) {
        this.state = "degraded";
      }
      this.publish("proxy.upstream", {
        kind: "state",
        state: event.state.status,
      });
    }
  }

  private onOutboundEvent(event: ProxyOutboundEvent): void {
    if (event.kind !== "reply") {
      this.lastErrorCode = event.code;
    }
    this.publish("proxy.queue", {
      kind: event.kind,
      ...(event.kind === "reply" ? {} : { code: event.code }),
    });
  }

  private publish(type: string, payload: Record<string, unknown>): void {
    this.eventBus?.publish({ type, source: "proxy", payload });
  }

  private isActiveGeneration(generation: number): boolean {
    return !this.stopping && generation === this.lifecycleGeneration;
  }

  private assertActiveGeneration(generation: number): void {
    if (!this.isActiveGeneration(generation)) {
      throw new ProxyRuntimeError("PROXY_RUNTIME_STOPPED");
    }
  }
}

async function settleBefore<T>(
  promise: Promise<T>,
  deadline: number,
): Promise<T> {
  const remainingMs = deadline - Date.now();
  if (remainingMs <= 0) {
    void promise.catch(() => undefined);
    throw new ProxyRuntimeError("PROXY_STOP_TIMEOUT");
  }
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new ProxyRuntimeError("PROXY_STOP_TIMEOUT")),
          remainingMs,
        );
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

interface ProxyTcpListenerOptions {
  frameMaxPayloadBytes?: number;
  host: string;
  onClient(
    kind: "connected" | "disconnected" | "rejected",
    code?: string,
  ): void;
  onBackpressure(code: string): void;
  onCommandRejected(code: string): void;
  onError(code: string): void;
  policy: ProxyAccessController;
  port: number;
  router: ProxyOutboundRouter;
  sessions: ProxySessionManager;
}

class ProxyTcpListener {
  private readonly server: Server;
  private readonly sockets = new Set<Socket>();
  private clientSequence = 0;
  private started = false;

  constructor(private readonly options: ProxyTcpListenerOptions) {
    this.server = net.createServer((socket) => this.onConnection(socket));
    this.server.on("error", () => this.options.onError("PROXY_LISTENER_ERROR"));
  }

  get port(): number {
    const address = this.server.address();
    return address && typeof address !== "string" ? address.port : 0;
  }

  async start(): Promise<void> {
    if (this.started) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const onError = () => {
        this.server.off("listening", onListening);
        reject(new ProxyRuntimeError("PROXY_LISTENER_START_FAILED"));
      };
      const onListening = () => {
        this.server.off("error", onError);
        resolve();
      };
      this.server.once("error", onError);
      this.server.once("listening", onListening);
      this.server.listen({ host: this.options.host, port: this.options.port });
    });
    this.started = true;
  }

  async stop(): Promise<void> {
    for (const socket of this.sockets) {
      socket.destroy();
    }
    this.sockets.clear();
    if (!this.started) {
      return;
    }
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
    this.started = false;
  }

  private onConnection(socket: Socket): void {
    this.sockets.add(socket);
    socket.setNoDelay(true);
    const id = `client-${++this.clientSequence}`;
    const address = socket.remoteAddress ?? "";
    try {
      this.options.policy.admit({ address, id });
    } catch (error) {
      this.options.onClient(
        "rejected",
        errorCode(error, "PROXY_CLIENT_REJECTED"),
      );
      socket.destroy();
      this.sockets.delete(socket);
      return;
    }

    const codec = new ProxyFrameCodec({
      ...(this.options.frameMaxPayloadBytes === undefined
        ? {}
        : { maxPayloadBytes: this.options.frameMaxPayloadBytes }),
    });
    let cleaned = false;
    let closing = false;
    let processingInput = false;
    let closingCode = "PROXY_CLIENT_DISCONNECTED";
    const closeClient = (code: string) => {
      if (closing) {
        return;
      }
      closing = true;
      closingCode = code;
      if (isBackpressureCode(code)) {
        this.options.onBackpressure(code);
      }
      socket.destroy();
    };
    const cleanup = () => {
      if (cleaned) {
        return;
      }
      cleaned = true;
      this.sockets.delete(socket);
      this.options.router.cancelClient(id);
      this.options.sessions.detach(id, closingCode);
      this.options.policy.release(id, closingCode);
      this.options.onClient("disconnected", closingCode);
    };
    const sink: ProxyClientSink = {
      id,
      close: (code) => closeClient(code),
      write: (frame) => this.writeFrame(socket, codec, frame),
    };
    try {
      this.options.sessions.attach(sink);
    } catch (error) {
      this.options.policy.release(
        id,
        errorCode(error, "PROXY_CLIENT_REJECTED"),
      );
      this.options.onClient(
        "rejected",
        errorCode(error, "PROXY_CLIENT_REJECTED"),
      );
      socket.destroy();
      this.sockets.delete(socket);
      return;
    }
    this.options.onClient("connected");
    socket.on("data", (chunk: Buffer) => {
      if (processingInput) {
        closeClient("PROXY_CLIENT_BACKPRESSURE");
        return;
      }
      processingInput = true;
      socket.pause();
      void (async () => {
        let frames: Uint8Array[];
        try {
          frames = codec.decode(chunk);
        } catch {
          closeClient("PROXY_CLIENT_FRAME_INVALID");
          return;
        }
        for (const frame of frames) {
          if (closing || cleaned || socket.destroyed) {
            return;
          }
          try {
            await this.options.router.submit({ clientId: id, frame });
          } catch (error) {
            const code = errorCode(error, "PROXY_OUTBOUND_REJECTED");
            if (isTerminalRuntimeCode(code)) {
              return;
            }
            this.options.onCommandRejected(code);
            if (code === "PROXY_OUTBOUND_QUEUE_FULL") {
              closeClient(code);
              return;
            }
          }
        }
      })()
        .catch(() => closeClient("PROXY_CLIENT_PROCESSING_FAILED"))
        .finally(() => {
          processingInput = false;
          if (!closing && !cleaned && !socket.destroyed) {
            socket.resume();
          }
        });
    });
    socket.on("error", () => closeClient("PROXY_CLIENT_SOCKET_ERROR"));
    socket.once("close", cleanup);
  }

  private writeFrame(
    socket: Socket,
    codec: ProxyFrameCodec,
    frame: Uint8Array,
  ): Promise<void> {
    if (socket.destroyed) {
      return Promise.reject(new ProxyRuntimeError("PROXY_CLIENT_WRITE_FAILED"));
    }
    let encoded: Uint8Array;
    try {
      encoded = codec.encode(frame);
    } catch {
      return Promise.reject(new ProxyRuntimeError("PROXY_CLIENT_WRITE_FAILED"));
    }
    return new Promise<void>((resolve, reject) => {
      socket.write(encoded, (error) => (error ? reject(error) : resolve()));
    });
  }
}

function errorCode(error: unknown, fallback: string): string {
  return error instanceof Error && "code" in error
    ? String(error.code)
    : fallback;
}

function isBackpressureCode(code: string): boolean {
  return (
    code === "PROXY_CLIENT_BACKPRESSURE" || code === "PROXY_OUTBOUND_QUEUE_FULL"
  );
}

function isTerminalRuntimeCode(code: string): boolean {
  return (
    code === "PROXY_CLIENT_DISCONNECTED" || code === "PROXY_OUTBOUND_STOPPED"
  );
}
