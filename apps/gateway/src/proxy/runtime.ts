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
  private state: ProxyStatus["state"] = "stopped";
  private lastErrorCode: string | undefined;
  private startPromise: Promise<ProxyStatus> | undefined;
  private startedOnce = false;

  constructor(private readonly options: ProxyRuntimeOptions) {
    if (
      !Number.isInteger(options.listenPort ?? 0) ||
      (options.listenPort ?? 0) < 0 ||
      (options.listenPort ?? 0) > 65_535
    ) {
      throw new ProxyRuntimeError("PROXY_LISTEN_CONFIGURATION_INVALID");
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
    if (this.startedOnce) {
      return Promise.reject(new ProxyRuntimeError("PROXY_RUNTIME_STOPPED"));
    }
    if (!this.startPromise) {
      this.startPromise = this.startInternal().finally(() => {
        this.startPromise = undefined;
      });
    }
    return this.startPromise;
  }

  async stop(): Promise<void> {
    if (!this.startedOnce && this.state === "stopped") {
      this.unsubscribeUpstream();
      return;
    }
    this.startedOnce = true;
    await this.listener.stop();
    this.sessions.stop();
    this.outbound.stop();
    this.unsubscribeUpstream();
    await this.options.upstream.stop();
    this.state = "stopped";
    this.publish("proxy.stopped", {});
  }

  private async startInternal(): Promise<ProxyStatus> {
    this.state = "starting";
    try {
      await this.options.upstream.start();
      this.sessions.start((event) => this.outbound.handleUpstreamEvent(event));
      await this.listener.start();
      this.startedOnce = true;
      this.state = "running";
      this.publish("proxy.started", {
        port: this.listener.port,
        mode: this.options.policy.snapshot.mode,
      });
      return this.status();
    } catch (error) {
      this.lastErrorCode = errorCode(error, "PROXY_START_FAILED");
      this.state = "degraded";
      this.sessions.stop();
      await this.options.upstream.stop();
      this.publish("proxy.error", { code: this.lastErrorCode });
      throw new ProxyRuntimeError(this.lastErrorCode);
    }
  }

  private onUpstreamEvent(event: ProxyUpstreamEvent): void {
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
}

interface ProxyTcpListenerOptions {
  frameMaxPayloadBytes?: number;
  host: string;
  onClient(
    kind: "connected" | "disconnected" | "rejected",
    code?: string,
  ): void;
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
    let closingCode = "PROXY_CLIENT_DISCONNECTED";
    const closeClient = (code: string) => {
      closingCode = code;
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
      let frames: Uint8Array[];
      try {
        frames = codec.decode(chunk);
      } catch {
        closeClient("PROXY_CLIENT_FRAME_INVALID");
        return;
      }
      for (const frame of frames) {
        void this.options.router
          .submit({ clientId: id, frame })
          .catch((error) => {
            const code = errorCode(error, "PROXY_OUTBOUND_REJECTED");
            if (
              code !== "PROXY_CLIENT_DISCONNECTED" &&
              code !== "PROXY_OUTBOUND_STOPPED"
            ) {
              this.options.onCommandRejected(code);
            }
          });
      }
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
