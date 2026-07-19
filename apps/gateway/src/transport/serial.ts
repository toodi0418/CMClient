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

const DEFAULT_SERIAL_OPEN_TIMEOUT_MS = 10_000;
const MAX_SERIAL_TIMEOUT_MS = 120_000;

export interface SerialConnection {
  close(): Promise<void>;
  onClose(listener: () => void): void;
  onData(listener: (chunk: Uint8Array) => void): void;
  onError(listener: () => void): void;
  write(frame: Uint8Array): Promise<void>;
}

export interface SerialPortAdapter {
  list(): Promise<SerialDevice[]>;
  open(options: {
    path: string;
    baudRate: number;
    signal?: AbortSignal;
  }): Promise<SerialConnection>;
}

export interface SerialMeshtasticTransportOptions {
  adapter: SerialPortAdapter;
  baudRate?: number;
  configSession: ConfigSessionCodec;
  configTimeoutMs?: number;
  maxPayloadBytes?: number;
  openTimeoutMs?: number;
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
    signal?: AbortSignal;
  }): Promise<SerialConnection> {
    const { signal, ...portOptions } = options;
    const port = new SerialPort({ ...portOptions, autoOpen: false });
    await new Promise<void>((resolve, reject) => {
      let aborted = false;
      const onAbort = () => {
        aborted = true;
      };
      if (signal?.aborted) {
        reject(new Error("SERIAL_OPEN_ABORTED"));
        return;
      }
      signal?.addEventListener("abort", onAbort, { once: true });
      try {
        port.open((error) => {
          signal?.removeEventListener("abort", onAbort);
          if (error) {
            reject(error);
            return;
          }
          if (!aborted) {
            resolve();
            return;
          }
          try {
            port.close((closeError) => {
              if (closeError) {
                port.destroy();
              }
              reject(new Error("SERIAL_OPEN_ABORTED"));
            });
          } catch {
            port.destroy();
            reject(new Error("SERIAL_OPEN_ABORTED"));
          }
        });
      } catch (error) {
        signal?.removeEventListener("abort", onAbort);
        reject(error);
      }
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
  private openAbortController: AbortController | undefined;
  private openOperation: Promise<void> | undefined;
  private openResourcePending = false;
  private openRequested = false;
  private closingConnection: SerialConnection | undefined;
  private closeOperation: Promise<void> | undefined;
  private manualDisconnect = false;
  private attempts = 0;
  private generation = 0;
  private connectPromise: Promise<void> | undefined;
  private resolveConnected: (() => void) | undefined;
  private rejectConnected: ((error: Error) => void) | undefined;
  private failureCode = "SERIAL_CONNECTION_CLOSED";
  private sessionConnectedAt: string | undefined;

  constructor(private readonly options: SerialMeshtasticTransportOptions) {
    if (
      !options.path.trim() ||
      !Number.isInteger(options.baudRate ?? 115_200) ||
      (options.baudRate ?? 115_200) < 1 ||
      !Number.isInteger(options.configTimeoutMs ?? 15_000) ||
      (options.configTimeoutMs ?? 15_000) < 1 ||
      (options.configTimeoutMs ?? 15_000) > MAX_SERIAL_TIMEOUT_MS ||
      !Number.isInteger(
        options.openTimeoutMs ?? DEFAULT_SERIAL_OPEN_TIMEOUT_MS,
      ) ||
      (options.openTimeoutMs ?? DEFAULT_SERIAL_OPEN_TIMEOUT_MS) < 1 ||
      (options.openTimeoutMs ?? DEFAULT_SERIAL_OPEN_TIMEOUT_MS) >
        MAX_SERIAL_TIMEOUT_MS
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
    this.startOpen();
    return this.connectPromise;
  }

  async disconnect(): Promise<void> {
    this.manualDisconnect = true;
    this.openRequested = false;
    this.generation += 1;
    this.openAbortController?.abort();
    this.openAbortController = undefined;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.clearConfigTimeout();
    this.configSession.reset();
    this.sessionConnectedAt = undefined;
    const connection = this.connection;
    this.connection = undefined;
    let closeFailed = false;
    try {
      if (connection) {
        await this.closeConnection(connection);
      }
    } catch {
      closeFailed = true;
      if (!this.connection && connection) {
        this.connection = connection;
      }
    } finally {
      if (
        !closeFailed &&
        !this.openResourcePending &&
        !this.connection &&
        this.state.status !== "disconnected"
      ) {
        this.emitState(this.stateMachine.transition("disconnected"));
      }
      this.rejectPending(new SerialTransportError("TRANSPORT_DISCONNECTED"));
    }
    if (this.openResourcePending) {
      throw new SerialTransportError("SERIAL_DISCONNECT_PENDING_OPEN");
    }
    if (closeFailed) {
      throw new SerialTransportError("SERIAL_DISCONNECT_FAILED");
    }
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

  private get openTimeoutMs(): number {
    return this.options.openTimeoutMs ?? DEFAULT_SERIAL_OPEN_TIMEOUT_MS;
  }

  private startOpen(): void {
    if (this.manualDisconnect || this.connection) {
      return;
    }
    if (this.openOperation) {
      this.openRequested = true;
      return;
    }
    const operation = this.open();
    this.openOperation = operation;
    void operation.then(
      () => this.completeOpen(operation),
      () => this.completeOpen(operation),
    );
  }

  private completeOpen(operation: Promise<void>): void {
    if (this.openOperation !== operation) {
      return;
    }
    this.openOperation = undefined;
    const openRequested = this.openRequested;
    this.openRequested = false;
    if (openRequested && !this.manualDisconnect && !this.connection) {
      this.startOpen();
    }
  }

  private async open(): Promise<void> {
    if (this.manualDisconnect || this.connection) {
      return;
    }
    const generation = ++this.generation;
    const abortController = new AbortController();
    this.openAbortController = abortController;
    this.emitState(this.stateMachine.transition("connecting"));
    this.openResourcePending = true;
    const openPromise = Promise.resolve().then(() =>
      this.options.adapter.open({
        path: this.options.path,
        baudRate: this.baudRate,
        signal: abortController.signal,
      }),
    );
    const outcome = await waitForSerialOpen(openPromise, this.openTimeoutMs);
    if (outcome.kind === "timeout") {
      abortController.abort();
    }
    if (this.openAbortController === abortController) {
      this.openAbortController = undefined;
    }
    if (outcome.kind === "timeout") {
      try {
        if (!this.manualDisconnect && generation === this.generation) {
          this.failureCode = "SERIAL_OPEN_TIMEOUT";
          this.emit({ kind: "error", code: this.failureCode });
          this.scheduleReconnect();
        }
        const lateConnection = await openPromise.catch(() => undefined);
        if (lateConnection) {
          await this.closeLateConnection(lateConnection);
        }
      } finally {
        this.openResourcePending = false;
      }
      return;
    }
    if (outcome.kind === "failed") {
      this.openResourcePending = false;
      if (this.manualDisconnect || generation !== this.generation) {
        return;
      }
      this.failureCode = "SERIAL_OPEN_FAILED";
      this.emit({ kind: "error", code: this.failureCode });
      this.scheduleReconnect();
      return;
    }
    const connection = outcome.connection;
    if (this.manualDisconnect || generation !== this.generation) {
      try {
        await this.closeLateConnection(connection);
      } finally {
        this.openResourcePending = false;
      }
      return;
    }
    this.connection = connection;
    this.openResourcePending = false;
    connection.onData((chunk) => {
      if (this.connection === connection) {
        this.onData(chunk);
      }
    });
    connection.onError(() => {
      if (this.connection !== connection || this.manualDisconnect) {
        return;
      }
      this.failureCode = "SERIAL_IO_FAILED";
      this.emit({ kind: "error", code: this.failureCode });
      this.closeInBackground(connection, "SERIAL_CLOSE_FAILED");
    });
    connection.onClose(() => {
      if (this.connection === connection) {
        this.connection = undefined;
        this.configSession.reset();
        this.sessionConnectedAt = undefined;
        this.clearConfigTimeout();
        this.scheduleReconnect();
      }
    });
    this.configSession.reset();
    this.sessionConnectedAt = undefined;
    this.emitState(this.stateMachine.transition("configuring"));
    this.configTimer = setTimeout(() => {
      if (
        this.connection !== connection ||
        this.state.status !== "configuring"
      ) {
        return;
      }
      this.failureCode = "SERIAL_CONFIG_TIMEOUT";
      this.emit({ kind: "error", code: this.failureCode });
      this.closeInBackground(connection, "SERIAL_CLOSE_FAILED");
    }, this.configTimeoutMs);
    this.configTimer.unref();
    try {
      await this.writeEncoded(
        encodeMeshtasticFrame(this.configSession.begin(), this.maxPayloadBytes),
      );
    } catch {
      if (
        this.connection !== connection ||
        this.manualDisconnect ||
        generation !== this.generation
      ) {
        return;
      }
      this.failureCode = "SERIAL_CONFIG_WRITE_FAILED";
      this.emit({ kind: "error", code: this.failureCode });
      this.closeInBackground(connection, "SERIAL_CLOSE_FAILED");
      return;
    }
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
            const readyState = this.stateMachine.transition("ready");
            this.sessionConnectedAt = readyState.changedAt;
            this.emitState(readyState);
            this.resolvePending();
          }
        } catch {
          this.failureCode = "SERIAL_CONFIG_DECODE_FAILED";
          this.emit({ kind: "error", code: this.failureCode });
          const connection = this.connection;
          if (connection) {
            this.closeInBackground(connection, "SERIAL_CLOSE_FAILED");
          }
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

  private scheduleReconnect(): void {
    this.clearConfigTimeout();
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
      this.startOpen();
    }, delay);
    this.reconnectTimer.unref();
  }

  private closeConnection(
    connection: SerialConnection,
    maximumAttempts = 1,
  ): Promise<void> {
    if (this.closingConnection === connection && this.closeOperation) {
      return this.closeOperation;
    }
    const operation = closeSerialConnection(connection, maximumAttempts);
    this.closingConnection = connection;
    this.closeOperation = operation;
    const complete = () => {
      if (this.closeOperation === operation) {
        this.closeOperation = undefined;
        this.closingConnection = undefined;
      }
    };
    void operation.then(complete, complete);
    return operation;
  }

  private closeInBackground(
    connection: SerialConnection,
    failureCode: string,
  ): void {
    const operation = this.closeConnection(connection, 2);
    void operation.then(
      () => undefined,
      () => {
        try {
          if (this.connection === connection && !this.manualDisconnect) {
            this.failureCode = failureCode;
            this.emit({ kind: "error", code: failureCode });
          }
        } catch {
          // Detached close failures cannot escape into the Node.js event loop.
        }
      },
    );
  }

  private async closeLateConnection(
    connection: SerialConnection,
  ): Promise<void> {
    try {
      await this.closeConnection(connection, 2);
    } catch {
      if (!this.connection) {
        this.connection = connection;
      }
      if (!this.manualDisconnect) {
        this.failureCode = "SERIAL_LATE_CLOSE_FAILED";
        this.emit({ kind: "error", code: this.failureCode });
      }
    }
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

type SerialOpenOutcome =
  | { kind: "opened"; connection: SerialConnection }
  | { kind: "failed" }
  | { kind: "timeout" };

async function waitForSerialOpen(
  open: Promise<SerialConnection>,
  timeoutMs: number,
): Promise<SerialOpenOutcome> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      open.then(
        (connection): SerialOpenOutcome => ({ kind: "opened", connection }),
        (): SerialOpenOutcome => ({ kind: "failed" }),
      ),
      new Promise<SerialOpenOutcome>((resolve) => {
        timer = setTimeout(() => resolve({ kind: "timeout" }), timeoutMs);
        timer.unref();
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

async function closeSerialConnection(
  connection: SerialConnection,
  maximumAttempts: number,
): Promise<void> {
  let lastError: unknown;
  for (let attempt = 0; attempt < maximumAttempts; attempt += 1) {
    try {
      await Promise.resolve().then(() => connection.close());
      return;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError;
}

function optional(value: string | undefined): string | undefined {
  return value?.trim() || undefined;
}
