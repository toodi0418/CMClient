import { SerialPort } from "serialport";

import type {
  SerialDevice,
  TransportConnectionState,
  TransportMetrics,
} from "@cmclient/contracts";

import { ReconnectBackoff, type ReconnectBackoffOptions } from "./backoff.js";
import { ConfigSession, type ConfigSessionCodec } from "./config-session.js";
import { MeshtasticFrameDecoder, encodeMeshtasticFrame } from "./framing.js";
import { TransportConnectionStateMachine } from "./state.js";
import type {
  MeshtasticTransport,
  TransportEvent,
  TransportEventListener,
} from "./types.js";

export interface SerialConnection {
  close(): Promise<void>;
  onClose(listener: () => void): void;
  onData(listener: (chunk: Uint8Array) => void): void;
  onError(listener: () => void): void;
  write(frame: Uint8Array): Promise<void>;
}

export interface SerialPortAdapter {
  list(): Promise<SerialDevice[]>;
  open(options: { path: string; baudRate: number }): Promise<SerialConnection>;
}

export interface SerialMeshtasticTransportOptions {
  adapter: SerialPortAdapter;
  baudRate?: number;
  configSession: ConfigSessionCodec;
  configTimeoutMs?: number;
  maxPayloadBytes?: number;
  path: string;
  reconnect?: ReconnectBackoffOptions;
  random?: () => number;
  clock?: () => Date;
}

export class NativeSerialPortAdapter implements SerialPortAdapter {
  async list(): Promise<SerialDevice[]> {
    const ports = await SerialPort.list();
    return ports
      .filter((port) => port.path.trim().length > 0)
      .map((port): SerialDevice => {
        const manufacturer = optional(port.manufacturer);
        const serialNumber = optional(port.serialNumber);
        const vendorId = optional(port.vendorId);
        const productId = optional(port.productId);
        return {
          path: port.path,
          ...(manufacturer ? { manufacturer } : {}),
          ...(serialNumber ? { serialNumber } : {}),
          ...(vendorId ? { vendorId } : {}),
          ...(productId ? { productId } : {}),
        };
      })
      .sort((left, right) => left.path.localeCompare(right.path));
  }

  async open(options: {
    path: string;
    baudRate: number;
  }): Promise<SerialConnection> {
    const port = new SerialPort({ ...options, autoOpen: false });
    await new Promise<void>((resolve, reject) => {
      port.open((error) => (error ? reject(error) : resolve()));
    });
    return new NativeSerialConnection(port);
  }
}

export async function listSerialDevices(
  adapter: SerialPortAdapter = new NativeSerialPortAdapter(),
): Promise<SerialDevice[]> {
  return adapter.list();
}

export class SerialMeshtasticTransport implements MeshtasticTransport {
  readonly kind = "serial" as const;
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
  private connection: SerialConnection | undefined;
  private configTimer: NodeJS.Timeout | undefined;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private manualDisconnect = false;
  private attempts = 0;
  private generation = 0;
  private connectPromise: Promise<void> | undefined;
  private resolveConnected: (() => void) | undefined;
  private rejectConnected: ((error: Error) => void) | undefined;
  private failureCode = "SERIAL_CONNECTION_CLOSED";

  constructor(private readonly options: SerialMeshtasticTransportOptions) {
    if (
      !options.path.trim() ||
      !Number.isInteger(options.baudRate ?? 115_200) ||
      (options.baudRate ?? 115_200) < 1 ||
      !Number.isInteger(options.configTimeoutMs ?? 15_000) ||
      (options.configTimeoutMs ?? 15_000) < 1
    ) {
      throw new SerialTransportError("SERIAL_CONFIGURATION_INVALID");
    }
    this.stateMachine = new TransportConnectionStateMachine(
      "serial",
      options.clock,
    );
    this.decoder = new MeshtasticFrameDecoder(
      options.maxPayloadBytes === undefined
        ? {}
        : { maxPayloadBytes: options.maxPayloadBytes },
    );
    this.configSession = new ConfigSession(options.configSession, () => {
      const sample = (options.random ?? Math.random)();
      return Number.isFinite(sample) && sample >= 0 && sample < 1
        ? Math.floor(sample * 0xffff_ffff) + 1
        : 0;
    });
    this.reconnect = new ReconnectBackoff(options.reconnect);
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
    this.connectPromise = new Promise<void>((resolve, reject) => {
      this.resolveConnected = resolve;
      this.rejectConnected = reject;
    });
    void this.open();
    return this.connectPromise;
  }

  async disconnect(): Promise<void> {
    this.manualDisconnect = true;
    this.generation += 1;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.clearConfigTimeout();
    this.configSession.reset();
    const connection = this.connection;
    this.connection = undefined;
    await connection?.close();
    if (this.state.status !== "disconnected") {
      this.emitState(this.stateMachine.transition("disconnected"));
    }
    this.rejectPending(new SerialTransportError("TRANSPORT_DISCONNECTED"));
  }

  writeFrame(payload: Uint8Array): Promise<void> {
    if (this.state.status !== "ready" || !this.connection) {
      return Promise.reject(new SerialTransportError("TRANSPORT_NOT_READY"));
    }
    return this.writeEncoded(
      encodeMeshtasticFrame(payload, this.maxPayloadBytes),
    );
  }

  subscribe(listener: TransportEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private get baudRate(): number {
    return this.options.baudRate ?? 115_200;
  }

  private get configTimeoutMs(): number {
    return this.options.configTimeoutMs ?? 15_000;
  }

  private get maxPayloadBytes(): number {
    return this.options.maxPayloadBytes ?? 512;
  }

  private async open(): Promise<void> {
    if (this.manualDisconnect || this.connection) {
      return;
    }
    const generation = ++this.generation;
    this.emitState(this.stateMachine.transition("connecting"));
    let connection: SerialConnection;
    try {
      connection = await this.options.adapter.open({
        path: this.options.path,
        baudRate: this.baudRate,
      });
    } catch {
      this.failureCode = "SERIAL_OPEN_FAILED";
      this.emit({ kind: "error", code: this.failureCode });
      this.scheduleReconnect();
      return;
    }
    if (this.manualDisconnect || generation !== this.generation) {
      await connection.close();
      return;
    }
    this.connection = connection;
    connection.onData((chunk) => {
      if (this.connection === connection) {
        this.onData(chunk);
      }
    });
    connection.onError(() => {
      this.failureCode = "SERIAL_IO_FAILED";
      this.emit({ kind: "error", code: this.failureCode });
      void connection.close();
    });
    connection.onClose(() => {
      if (this.connection === connection) {
        this.connection = undefined;
        this.configSession.reset();
        this.clearConfigTimeout();
        this.scheduleReconnect();
      }
    });
    this.configSession.reset();
    this.emitState(this.stateMachine.transition("configuring"));
    try {
      await this.writeEncoded(
        encodeMeshtasticFrame(this.configSession.begin(), this.maxPayloadBytes),
      );
    } catch {
      this.failureCode = "SERIAL_CONFIG_WRITE_FAILED";
      this.emit({ kind: "error", code: this.failureCode });
      await connection.close();
      return;
    }
    this.configTimer = setTimeout(() => {
      if (
        this.connection !== connection ||
        this.state.status !== "configuring"
      ) {
        return;
      }
      this.failureCode = "SERIAL_CONFIG_TIMEOUT";
      this.emit({ kind: "error", code: this.failureCode });
      void connection.close();
    }, this.configTimeoutMs);
    this.configTimer.unref();
  }

  private onData(chunk: Uint8Array): void {
    this.counters.bytesReceived += chunk.length;
    for (const frame of this.decoder.push(chunk)) {
      this.counters.framesReceived += 1;
      if (this.state.status === "configuring") {
        try {
          if (this.configSession.observe(frame)) {
            this.clearConfigTimeout();
            this.attempts = 0;
            this.emitState(this.stateMachine.transition("ready"));
            this.resolvePending();
          }
        } catch {
          this.failureCode = "SERIAL_CONFIG_DECODE_FAILED";
          this.emit({ kind: "error", code: this.failureCode });
          void this.connection?.close();
        }
      }
      this.emit({
        kind: "frame",
        frame,
        receivedAt: (this.options.clock ?? (() => new Date()))().toISOString(),
      });
    }
  }

  private scheduleReconnect(): void {
    this.clearConfigTimeout();
    if (this.manualDisconnect) {
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
      void this.open();
    }, delay);
    this.reconnectTimer.unref();
  }

  private writeEncoded(frame: Uint8Array): Promise<void> {
    if (!this.connection) {
      return Promise.reject(new SerialTransportError("TRANSPORT_NOT_READY"));
    }
    return this.connection.write(frame).then(() => {
      this.counters.bytesSent += frame.length;
      this.counters.framesSent += 1;
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
}

export class SerialTransportError extends Error {
  readonly code: string;

  constructor(code: string) {
    super(code);
    this.code = code;
  }
}

class NativeSerialConnection implements SerialConnection {
  constructor(private readonly port: SerialPort) {}

  close(): Promise<void> {
    if (!this.port.isOpen) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      this.port.close((error) => (error ? reject(error) : resolve()));
    });
  }

  onClose(listener: () => void): void {
    this.port.once("close", listener);
  }

  onData(listener: (chunk: Uint8Array) => void): void {
    this.port.on("data", listener);
  }

  onError(listener: () => void): void {
    this.port.on("error", listener);
  }

  write(frame: Uint8Array): Promise<void> {
    return new Promise((resolve, reject) => {
      this.port.write(frame, (error) => {
        if (error) {
          reject(error);
          return;
        }
        this.port.drain((drainError) =>
          drainError ? reject(drainError) : resolve(),
        );
      });
    });
  }
}

function optional(value: string | undefined): string | undefined {
  return value?.trim() || undefined;
}
