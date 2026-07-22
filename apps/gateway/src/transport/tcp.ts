import net, { type Socket } from "node:net";

import type {
  TransportConnectionState,
  TransportMetrics,
} from "@cmclient/contracts";

import { ReconnectBackoff, type ReconnectBackoffOptions } from "./backoff.js";
import { ConfigSession, type ConfigSessionCodec } from "./config-session.js";
import { MeshtasticFrameDecoder, encodeMeshtasticFrame } from "./framing.js";
import {
  PhysicalWriteGuard,
  PhysicalWriteGuardError,
} from "./physical-guard.js";
import { TransportConnectionStateMachine } from "./state.js";
import type {
  MeshtasticTransport,
  TransportEvent,
  TransportEventListener,
} from "./types.js";

const PHYSICAL_RECONNECT_DELAYS_MS = [5_000, 30_000, 120_000] as const;

const DEFAULT_TCP_CONNECT_TIMEOUT_MS = 10_000;
const MAX_TCP_TIMEOUT_MS = 120_000;

type TcpSocketFactory = (options: { host: string; port: number }) => Socket;

export interface TcpMeshtasticTransportOptions {
  host: string;
  port: number;
  configSession: ConfigSessionCodec;
  maxPayloadBytes?: number;
  configTimeoutMs?: number;
  connectTimeoutMs?: number;
  reconnect?: ReconnectBackoffOptions;
  random?: () => number;
  clock?: () => Date;
  physicalGuard?: PhysicalWriteGuard;
}

export class TcpMeshtasticTransport implements MeshtasticTransport {
  readonly kind = "tcp" as const;
  private readonly stateMachine: TransportConnectionStateMachine;
  private readonly decoder: MeshtasticFrameDecoder;
  private readonly configSession: ConfigSession;
  private readonly reconnect: ReconnectBackoff;
  private readonly listeners = new Set<TransportEventListener>();
  private readonly counters: TransportMetrics = {
    bytesReceived: 0,
    bytesSent: 0,
    framesReceived: 0,
    framesSent: 0,
    malformedFrames: 0,
    reconnects: 0,
  };
  private socket: Socket | undefined;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private configTimer: NodeJS.Timeout | undefined;
  private connectTimer: NodeJS.Timeout | undefined;
  private physicalSessionTimer: NodeJS.Timeout | undefined;
  private manualDisconnect = false;
  private attempts = 0;
  private connectPromise: Promise<void> | undefined;
  private resolveConnected: (() => void) | undefined;
  private rejectConnected: ((error: Error) => void) | undefined;
  private failureCode = "TCP_CONNECTION_CLOSED";
  private sessionConnectedAt: string | undefined;

  constructor(
    private readonly options: TcpMeshtasticTransportOptions,
    private readonly socketFactory: TcpSocketFactory = (socketOptions) =>
      net.createConnection(socketOptions),
  ) {
    if (
      !options.host.trim() ||
      !Number.isInteger(options.port) ||
      options.port < 1 ||
      options.port > 65_535 ||
      !Number.isInteger(options.configTimeoutMs ?? 15_000) ||
      (options.configTimeoutMs ?? 15_000) < 1 ||
      (options.configTimeoutMs ?? 15_000) > MAX_TCP_TIMEOUT_MS ||
      !Number.isInteger(
        options.connectTimeoutMs ?? DEFAULT_TCP_CONNECT_TIMEOUT_MS,
      ) ||
      (options.connectTimeoutMs ?? DEFAULT_TCP_CONNECT_TIMEOUT_MS) < 1 ||
      (options.connectTimeoutMs ?? DEFAULT_TCP_CONNECT_TIMEOUT_MS) >
        MAX_TCP_TIMEOUT_MS
    ) {
      throw new TcpTransportError("TCP_CONFIGURATION_INVALID");
    }
    this.stateMachine = new TransportConnectionStateMachine(
      "tcp",
      options.clock,
    );
    this.decoder = new MeshtasticFrameDecoder(
      options.maxPayloadBytes === undefined
        ? {}
        : { maxPayloadBytes: options.maxPayloadBytes },
    );
    this.configSession = new ConfigSession(options.configSession, () => {
      const sample = (options.random ?? Math.random)();
      if (!Number.isFinite(sample) || sample < 0 || sample >= 1) {
        return 0;
      }
      return Math.floor(sample * 0xffff_ffff) + 1;
    });
    this.reconnect = new ReconnectBackoff(
      options.physicalGuard?.physicalProfile
        ? { fixedDelaysMs: PHYSICAL_RECONNECT_DELAYS_MS }
        : options.reconnect,
    );
  }

  get state(): TransportConnectionState {
    return this.stateMachine.state;
  }

  get metrics(): TransportMetrics {
    return {
      ...this.counters,
      malformedFrames: this.decoder.metrics.malformedFrames,
    };
  }

  connect(): Promise<void> {
    this.manualDisconnect = false;
    if (this.state.status === "ready") {
      return Promise.resolve();
    }
    if (this.connectPromise) {
      return this.connectPromise;
    }
    const connectPromise = new Promise<void>((resolve, reject) => {
      this.resolveConnected = resolve;
      this.rejectConnected = reject;
    });
    this.connectPromise = connectPromise;
    this.openSocket();
    return connectPromise;
  }

  async disconnect(): Promise<void> {
    this.manualDisconnect = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.clearConnectTimeout();
    this.clearConfigTimeout();
    this.clearPhysicalSessionTimeout();
    this.configSession.reset();
    this.decoder.resetBufferedState();
    this.sessionConnectedAt = undefined;
    const socket = this.socket;
    this.socket = undefined;
    socket?.destroy();
    try {
      this.options.physicalGuard?.releaseSession("aborted");
    } catch (error) {
      this.emit({
        kind: "error",
        code: stableGuardCode(error, "PHYSICAL_GUARD_RELEASE_FAILED"),
      });
    }
    if (this.state.status !== "disconnected") {
      this.emitState(this.stateMachine.transition("disconnected"));
    }
    this.rejectPending(new TcpTransportError("TRANSPORT_DISCONNECTED"));
  }

  writeFrame(payload: Uint8Array): Promise<void> {
    try {
      this.options.physicalGuard?.rejectApplicationWrite();
    } catch (error) {
      return Promise.reject(
        new TcpTransportError(
          stableGuardCode(error, "PHYSICAL_GUARD_WRITER_DISABLED"),
        ),
      );
    }
    if (this.state.status !== "ready" || !this.socket) {
      return Promise.reject(new TcpTransportError("TRANSPORT_NOT_READY"));
    }
    return this.writeEncoded(
      encodeMeshtasticFrame(payload, this.maxPayloadBytes),
    );
  }

  subscribe(listener: TransportEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private get maxPayloadBytes(): number {
    return this.options.maxPayloadBytes ?? 512;
  }

  private get configTimeoutMs(): number {
    return this.options.configTimeoutMs ?? 15_000;
  }

  private get connectTimeoutMs(): number {
    return this.options.connectTimeoutMs ?? DEFAULT_TCP_CONNECT_TIMEOUT_MS;
  }

  private openSocket(): void {
    if (this.manualDisconnect || this.socket) {
      return;
    }
    this.decoder.resetBufferedState();
    this.emitState(this.stateMachine.transition("connecting"));
    this.configSession.reset();
    let configRequest: ReturnType<ConfigSession["beginRequest"]>;
    try {
      configRequest = this.configSession.beginRequest();
      this.options.physicalGuard?.acquireSession(configRequest.nonce);
    } catch (error) {
      this.failureCode = stableGuardCode(error, "TCP_CONFIG_SESSION_FAILED");
      this.emit({ kind: "error", code: this.failureCode });
      this.emitState(this.stateMachine.transition("disconnected"));
      this.rejectPending(new TcpTransportError(this.failureCode));
      return;
    }
    let socket: Socket;
    try {
      socket = this.socketFactory({
        host: this.options.host,
        port: this.options.port,
      });
    } catch {
      this.failureCode = "TCP_CONNECT_FAILED";
      this.emit({ kind: "error", code: this.failureCode });
      this.options.physicalGuard?.releaseSession("connect");
      this.scheduleReconnect();
      return;
    }
    this.socket = socket;
    if (this.options.physicalGuard?.physicalProfile) {
      this.physicalSessionTimer = setTimeout(() => {
        if (this.socket !== socket) {
          return;
        }
        this.failureCode = "PHYSICAL_GUARD_DURATION_BUDGET_EXCEEDED";
        this.emit({ kind: "error", code: this.failureCode });
        socket.destroy();
      }, this.options.physicalGuard.sessionDurationLimitMs);
      this.physicalSessionTimer.unref();
    }
    this.connectTimer = setTimeout(() => {
      if (this.socket !== socket || this.state.status !== "connecting") {
        return;
      }
      this.failureCode = "TCP_CONNECT_TIMEOUT";
      this.emit({ kind: "error", code: this.failureCode });
      socket.destroy();
    }, this.connectTimeoutMs);
    this.connectTimer.unref();
    socket.once("connect", () => {
      if (this.socket !== socket || this.manualDisconnect) {
        socket.destroy();
        return;
      }
      this.clearConnectTimeout();
      this.failureCode = "TCP_CONNECTION_CLOSED";
      this.sessionConnectedAt = undefined;
      this.emitState(this.stateMachine.transition("configuring"));
      try {
        this.options.physicalGuard?.authorizeConfigRequest(
          configRequest.nonce,
          configRequest.payload,
        );
        this.writeEncoded(
          encodeMeshtasticFrame(configRequest.payload, this.maxPayloadBytes),
        ).catch((error: unknown) => {
          const code =
            error instanceof TcpTransportError
              ? error.code
              : "TCP_WRITE_FAILED";
          this.emit({ kind: "error", code });
          socket.destroy();
        });
        this.configTimer = setTimeout(() => {
          if (this.socket !== socket || this.state.status !== "configuring") {
            return;
          }
          this.failureCode = "TCP_CONFIG_TIMEOUT";
          this.emit({ kind: "error", code: this.failureCode });
          socket.destroy();
        }, this.configTimeoutMs);
        this.configTimer.unref();
      } catch (error) {
        this.failureCode = stableGuardCode(error, "TCP_CONFIG_SESSION_FAILED");
        this.emit({ kind: "error", code: this.failureCode });
        socket.destroy();
      }
    });
    socket.on("data", (chunk: Buffer) => {
      if (this.socket === socket) {
        this.onData(chunk);
      }
    });
    socket.on("error", () => {
      if (this.socket !== socket || this.manualDisconnect) {
        return;
      }
      this.failureCode = "TCP_SOCKET_ERROR";
      this.emit({ kind: "error", code: this.failureCode });
    });
    socket.once("close", () => this.onClose(socket));
  }

  private onData(chunk: Uint8Array): void {
    try {
      this.options.physicalGuard?.accountIncomingBytes(chunk.length);
    } catch (error) {
      this.failureCode = stableGuardCode(
        error,
        "PHYSICAL_GUARD_BYTE_BUDGET_EXCEEDED",
      );
      this.emit({ kind: "error", code: this.failureCode });
      this.socket?.destroy();
      return;
    }
    this.counters.bytesReceived += chunk.length;
    const frames = this.decoder.push(chunk);
    try {
      this.options.physicalGuard?.accountIncomingFrames(frames.length);
    } catch (error) {
      this.failureCode = stableGuardCode(
        error,
        "PHYSICAL_GUARD_FRAME_BUDGET_EXCEEDED",
      );
      this.emit({ kind: "error", code: this.failureCode });
      this.socket?.destroy();
      return;
    }
    for (const frame of frames) {
      this.counters.framesReceived += 1;
      if (this.state.status === "configuring") {
        try {
          if (this.configSession.observe(frame)) {
            this.options.physicalGuard?.recordConfigSuccess();
            this.clearConfigTimeout();
            this.attempts = 0;
            const readyState = this.stateMachine.transition("ready");
            this.sessionConnectedAt = readyState.changedAt;
            this.emitState(readyState);
            this.resolvePending();
          }
        } catch (error) {
          this.failureCode = stableGuardCode(error, "TCP_CONFIG_DECODE_FAILED");
          this.emit({ kind: "error", code: this.failureCode });
          this.socket?.destroy();
        }
      }
      this.emit({
        kind: "frame",
        frame,
        receivedAt: (this.options.clock ?? (() => new Date()))().toISOString(),
        ...(this.sessionConnectedAt
          ? { sessionConnectedAt: this.sessionConnectedAt }
          : {}),
      });
    }
  }

  private onClose(socket: Socket): void {
    if (this.socket !== socket) {
      return;
    }
    this.socket = undefined;
    this.configSession.reset();
    this.decoder.resetBufferedState();
    this.sessionConnectedAt = undefined;
    this.clearConnectTimeout();
    this.clearConfigTimeout();
    this.clearPhysicalSessionTimeout();
    try {
      this.options.physicalGuard?.releaseSession(
        physicalFailureReason(this.failureCode),
      );
    } catch (error) {
      this.failureCode = stableGuardCode(
        error,
        "PHYSICAL_GUARD_RELEASE_FAILED",
      );
      this.emit({ kind: "error", code: this.failureCode });
      this.emitState(this.stateMachine.transition("disconnected"));
      this.rejectPending(new TcpTransportError(this.failureCode));
      return;
    }
    if (
      this.options.physicalGuard?.physicalProfile &&
      !this.options.physicalGuard.automaticReconnectAllowed
    ) {
      if (!this.failureCode.startsWith("PHYSICAL_GUARD_")) {
        this.failureCode = "PHYSICAL_GUARD_FUSE_OPEN";
      }
      this.emitState(this.stateMachine.transition("disconnected"));
      this.rejectPending(new TcpTransportError(this.failureCode));
      return;
    }
    if (this.manualDisconnect) {
      return;
    }
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.manualDisconnect || this.reconnectTimer) {
      return;
    }
    this.attempts += 1;
    this.counters.reconnects += 1;
    const delay = this.reconnect.delayForAttempt(
      this.attempts,
      this.options.random,
    );
    this.emitState(
      this.stateMachine.transition("backoff", {
        attempt: this.attempts,
        reasonCode: this.failureCode,
      }),
    );
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.openSocket();
    }, delay);
    this.reconnectTimer.unref();
  }

  private writeEncoded(frame: Uint8Array): Promise<void> {
    const socket = this.socket;
    if (!socket) {
      return Promise.reject(new TcpTransportError("TRANSPORT_NOT_READY"));
    }
    return new Promise((resolve, reject) => {
      socket.write(frame, (error) => {
        if (error) {
          this.failureCode = "TCP_WRITE_FAILED";
          reject(new TcpTransportError(this.failureCode));
          return;
        }
        this.counters.bytesSent += frame.length;
        this.counters.framesSent += 1;
        resolve();
      });
    });
  }

  private emitState(state: TransportConnectionState): void {
    this.emit({ kind: "state", state });
  }

  private emit(event: TransportEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  private resolvePending(): void {
    this.resolveConnected?.();
    this.connectPromise = undefined;
    this.resolveConnected = undefined;
    this.rejectConnected = undefined;
  }

  private rejectPending(error: Error): void {
    this.rejectConnected?.(error);
    this.connectPromise = undefined;
    this.resolveConnected = undefined;
    this.rejectConnected = undefined;
  }

  private clearConfigTimeout(): void {
    if (this.configTimer) {
      clearTimeout(this.configTimer);
      this.configTimer = undefined;
    }
  }

  private clearConnectTimeout(): void {
    if (this.connectTimer) {
      clearTimeout(this.connectTimer);
      this.connectTimer = undefined;
    }
  }

  private clearPhysicalSessionTimeout(): void {
    if (this.physicalSessionTimer) {
      clearTimeout(this.physicalSessionTimer);
      this.physicalSessionTimer = undefined;
    }
  }
}

export class TcpTransportError extends Error {
  readonly code: string;

  constructor(code: string) {
    super(code);
    this.code = code;
  }
}

function stableGuardCode(error: unknown, fallback: string): string {
  return error instanceof PhysicalWriteGuardError ? error.code : fallback;
}

function physicalFailureReason(code: string): string {
  if (code.includes("TIMEOUT")) {
    return "timeout";
  }
  if (code.includes("DECODE")) {
    return "decode";
  }
  if (code.includes("WRITE")) {
    return "write";
  }
  if (code.includes("BUDGET")) {
    return "budget";
  }
  if (code.includes("CONNECT") || code.includes("SOCKET")) {
    return "connect";
  }
  return "closed";
}
