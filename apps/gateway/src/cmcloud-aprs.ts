import net, { type Socket } from "node:net";

import { deriveAprsPasscode } from "./aprs-identity.js";

export const CMCLOUD_APRS_MAX_TNC2_BYTES = 510;

const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
const DEFAULT_RECONNECT_DELAY_MS = 1_000;
const MAX_APRS_LOGIN_LINE_BYTES = 512;
const MAX_APRS_GREETING_BYTES = 4 * 1024;
const APRS_CALLSIGN = /^([A-Z0-9]{3,6})(?:-([1-9]|1[0-5]))?$/u;

export interface CmCloudDirectAprsCapability {
  readonly callsign: string;
  readonly verified: true;
}

export interface CmCloudDirectAprsDispatchResult {
  readonly outcome: "submitted" | "retryable_failure" | "uncertain";
  readonly errorCode?: string;
}

export interface CmCloudDirectAprsEgress {
  configure(capability?: CmCloudDirectAprsCapability): Promise<void>;
  ready(): boolean;
  submit(data: string): Promise<CmCloudDirectAprsDispatchResult>;
  stop(): Promise<void>;
  setReadinessListener(listener: () => void): void;
}

export class CmCloudDirectAprsError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "CmCloudDirectAprsError";
  }
}

interface DirectAprsSession {
  readonly capability: CmCloudDirectAprsCapability;
  readonly generation: number;
  readonly socket: Socket;
}

export interface CmCloudDirectAprsEgressOptions {
  readonly host: string;
  readonly port: number;
  readonly timeoutMs?: number;
  readonly reconnectDelayMs?: number;
  readonly socketFactory?: (options: { host: string; port: number }) => Socket;
}

/**
 * A narrowly scoped APRS-IS writer for CMCloud-selected TNC2 dispatches.
 * CMCloud grants the callsign capability only after administrative verification.
 */
export class CmCloudDirectAprsEgressRuntime implements CmCloudDirectAprsEgress {
  private readonly timeoutMs: number;
  private readonly reconnectDelayMs: number;
  private readonly socketFactory: (options: {
    host: string;
    port: number;
  }) => Socket;
  private capability: CmCloudDirectAprsCapability | undefined;
  private session: DirectAprsSession | undefined;
  private connectingSocket: Socket | undefined;
  private connecting: Promise<void> | undefined;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private capabilityGeneration = 0;
  private sessionGeneration = 0;
  private lastReportedReady = false;
  private readinessListener: (() => void) | undefined;

  constructor(private readonly options: CmCloudDirectAprsEgressOptions) {
    if (
      !options.host.trim() ||
      !Number.isInteger(options.port) ||
      options.port < 1 ||
      options.port > 65_535
    ) {
      throw new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_ENDPOINT_INVALID");
    }
    this.timeoutMs = boundedTimeout(
      options.timeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
    );
    this.reconnectDelayMs = boundedReconnectDelay(
      options.reconnectDelayMs ?? DEFAULT_RECONNECT_DELAY_MS,
    );
    this.socketFactory =
      options.socketFactory ??
      ((connection) =>
        net.createConnection({ host: connection.host, port: connection.port }));
  }

  setReadinessListener(listener: () => void): void {
    this.readinessListener = listener;
    listener();
  }

  ready(): boolean {
    const session = this.session;
    return Boolean(
      session && !session.socket.destroyed && session.socket.writable,
    );
  }

  async configure(capability?: CmCloudDirectAprsCapability): Promise<void> {
    const normalized = capability
      ? parseCmCloudDirectAprsCapability(capability)
      : undefined;
    if (
      sameCapability(this.capability, normalized) &&
      (this.ready() || this.connecting)
    ) {
      return this.connecting ?? Promise.resolve();
    }
    this.capability = normalized;
    this.capabilityGeneration += 1;
    this.clearReconnectTimer();
    const previousSession = this.session;
    const previousConnectingSocket = this.connectingSocket;
    this.session = undefined;
    this.connectingSocket = undefined;
    this.connecting = undefined;
    this.reportReadiness();
    if (previousConnectingSocket && !previousConnectingSocket.destroyed) {
      previousConnectingSocket.destroy();
    }
    if (previousSession) {
      await closeSocket(previousSession.socket);
    }
    if (!normalized) {
      return;
    }
    return this.ensureReady();
  }

  async stop(): Promise<void> {
    await this.configure(undefined);
  }

  async submit(data: string): Promise<CmCloudDirectAprsDispatchResult> {
    if (!validTnc2Data(data)) {
      return {
        outcome: "retryable_failure",
        errorCode: "CMCLOUD_APRS_DISPATCH_INVALID",
      };
    }
    const session = this.session;
    if (!session || !this.ready()) {
      void this.ensureReady();
      return {
        outcome: "retryable_failure",
        errorCode: "CMCLOUD_DIRECT_APRS_NOT_READY",
      };
    }
    const outcome = await writeOnce(session.socket, data);
    if (outcome.outcome !== "submitted") {
      this.invalidateSession(session);
    }
    return outcome;
  }

  private ensureReady(): Promise<void> {
    if (!this.capability || this.ready()) {
      return Promise.resolve();
    }
    if (this.connecting) {
      return this.connecting;
    }
    const capability = this.capability;
    const generation = this.capabilityGeneration;
    const operation = this.openVerifiedSession(generation, capability).finally(
      () => {
        if (this.connecting === operation) {
          this.connecting = undefined;
        }
      },
    );
    this.connecting = operation;
    return operation;
  }

  private async openVerifiedSession(
    capabilityGeneration: number,
    capability: CmCloudDirectAprsCapability,
  ): Promise<void> {
    let socket: Socket;
    try {
      socket = this.socketFactory({
        host: this.options.host,
        port: this.options.port,
      });
    } catch {
      this.handleConnectionFailure(capabilityGeneration, capability);
      return;
    }
    this.connectingSocket = socket;
    try {
      socket.setNoDelay(true);
      const verified = waitForVerifiedLogresp(
        socket,
        capability.callsign,
        this.timeoutMs,
      );
      // `waitForConnect` may fail first. Keep the login observer drained so a
      // later close cannot surface as an unhandled rejection.
      void verified.catch(() => undefined);
      await waitForConnect(socket, this.timeoutMs);
      if (!this.isCurrentCapability(capabilityGeneration, capability)) {
        await closeSocket(socket);
        return;
      }
      const login = buildLoginLine(capability.callsign);
      await writeLogin(socket, login);
      await verified;
      if (!this.isCurrentCapability(capabilityGeneration, capability)) {
        await closeSocket(socket);
        return;
      }
      const session: DirectAprsSession = {
        capability,
        generation: ++this.sessionGeneration,
        socket,
      };
      this.connectingSocket = undefined;
      this.session = session;
      socket.on("close", () => this.invalidateSession(session));
      socket.on("error", () => this.invalidateSession(session));
      socket.unref();
      this.reportReadiness();
    } catch {
      this.connectingSocket = undefined;
      await closeSocket(socket);
      this.handleConnectionFailure(capabilityGeneration, capability);
    }
  }

  private handleConnectionFailure(
    capabilityGeneration: number,
    capability: CmCloudDirectAprsCapability,
  ): void {
    if (!this.isCurrentCapability(capabilityGeneration, capability)) {
      return;
    }
    this.reportReadiness();
    this.scheduleReconnect();
  }

  private invalidateSession(session: DirectAprsSession): void {
    if (this.session !== session) return;
    this.session = undefined;
    if (!session.socket.destroyed) {
      session.socket.destroy();
    }
    this.reportReadiness();
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (!this.capability || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.ensureReady();
    }, this.reconnectDelayMs);
    this.reconnectTimer.unref();
  }

  private clearReconnectTimer(): void {
    if (!this.reconnectTimer) return;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
  }

  private isCurrentCapability(
    capabilityGeneration: number,
    capability: CmCloudDirectAprsCapability,
  ): boolean {
    return (
      this.capabilityGeneration === capabilityGeneration &&
      sameCapability(this.capability, capability)
    );
  }

  private reportReadiness(): void {
    const current = this.ready();
    if (current === this.lastReportedReady) return;
    this.lastReportedReady = current;
    this.readinessListener?.();
  }
}

export function parseCmCloudDirectAprsCapability(
  value: unknown,
): CmCloudDirectAprsCapability {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CAPABILITY_INVALID");
  }
  const candidate = value as Record<string, unknown>;
  if (candidate.verified !== true || typeof candidate.callsign !== "string") {
    throw new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CAPABILITY_INVALID");
  }
  const callsign = candidate.callsign;
  if (!APRS_CALLSIGN.test(callsign)) {
    throw new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CAPABILITY_INVALID");
  }
  return { callsign, verified: true };
}

function sameCapability(
  left: CmCloudDirectAprsCapability | undefined,
  right: CmCloudDirectAprsCapability | undefined,
): boolean {
  return (
    left?.callsign === right?.callsign && left?.verified === right?.verified
  );
}

function buildLoginLine(callsign: string): string {
  const parsed = APRS_CALLSIGN.exec(callsign);
  if (!parsed) {
    throw new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CAPABILITY_INVALID");
  }
  const base = parsed[1]!;
  const login = `user ${callsign} pass ${deriveAprsPasscode(base)} vers CMClient 2.0`;
  if (Buffer.byteLength(login, "utf8") > MAX_APRS_LOGIN_LINE_BYTES) {
    throw new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CAPABILITY_INVALID");
  }
  return login;
}

function validTnc2Data(value: unknown): value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    /[\r\n]/u.test(value) ||
    Buffer.byteLength(value, "utf8") > CMCLOUD_APRS_MAX_TNC2_BYTES
  ) {
    return false;
  }
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0;
    if (code < 0x20 || code === 0x7f) return false;
  }
  return true;
}

async function writeOnce(
  socket: Socket,
  data: string,
): Promise<CmCloudDirectAprsDispatchResult> {
  if (socket.destroyed || !socket.writable) {
    return {
      outcome: "retryable_failure",
      errorCode: "CMCLOUD_DIRECT_APRS_NOT_READY",
    };
  }
  const wire = Buffer.concat([Buffer.from(data, "utf8"), Buffer.from("\r\n")]);
  return new Promise((resolve) => {
    let writeStarted = false;
    let settled = false;
    const finish = (result: CmCloudDirectAprsDispatchResult): void => {
      if (settled) return;
      settled = true;
      socket.off("close", onClose);
      socket.off("error", onError);
      resolve(result);
    };
    const ambiguous = (): void =>
      finish({
        outcome: "uncertain",
        errorCode: "CMCLOUD_DIRECT_APRS_WRITE_UNCERTAIN",
      });
    const onClose = (): void => {
      if (writeStarted) ambiguous();
    };
    const onError = (): void => {
      if (writeStarted) ambiguous();
    };
    socket.once("close", onClose);
    socket.once("error", onError);
    try {
      writeStarted = true;
      socket.write(wire, (error) => {
        if (error) {
          ambiguous();
          return;
        }
        finish({ outcome: "submitted" });
      });
    } catch {
      if (writeStarted) {
        ambiguous();
      } else {
        finish({
          outcome: "retryable_failure",
          errorCode: "CMCLOUD_DIRECT_APRS_WRITE_FAILED",
        });
      }
    }
  });
}

function waitForConnect(socket: Socket, timeoutMs: number): Promise<void> {
  if (!socket.connecting) {
    return socket.destroyed
      ? Promise.reject(
          new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CONNECT_FAILED"),
        )
      : Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    const finish = (error?: Error): void => {
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("error", onError);
      socket.off("close", onClose);
      if (error) reject(error);
      else resolve();
    };
    const onConnect = (): void => finish();
    const onError = (): void =>
      finish(new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CONNECT_FAILED"));
    const onClose = (): void =>
      finish(new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CONNECT_FAILED"));
    const timer = setTimeout(onError, timeoutMs);
    timer.unref();
    socket.once("connect", onConnect);
    socket.once("error", onError);
    socket.once("close", onClose);
  });
}

function waitForVerifiedLogresp(
  socket: Socket,
  callsign: string,
  timeoutMs: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let buffered = Buffer.alloc(0);
    const finish = (error?: Error): void => {
      clearTimeout(timer);
      socket.off("data", onData);
      socket.off("error", onError);
      socket.off("close", onClose);
      if (error) reject(error);
      else resolve();
    };
    const onError = (): void =>
      finish(new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CONNECT_FAILED"));
    const onClose = (): void =>
      finish(new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CONNECT_FAILED"));
    const onData = (chunk: Buffer): void => {
      buffered = Buffer.concat([buffered, Buffer.from(chunk)]);
      if (buffered.length > MAX_APRS_GREETING_BYTES) {
        finish(new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_LOGIN_INVALID"));
        return;
      }
      let delimiter: number;
      while ((delimiter = buffered.indexOf(0x0a)) >= 0) {
        const line = buffered
          .subarray(0, delimiter)
          .toString("utf8")
          .replace(/\r$/u, "");
        buffered = buffered.subarray(delimiter + 1);
        const match =
          /^#\s*logresp\s+([A-Z0-9-]+)\s+(verified|unverified)\b/iu.exec(line);
        if (!match || match[1] !== callsign) continue;
        if (match[2] === "verified") {
          finish();
        } else {
          finish(
            new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_LOGIN_REJECTED"),
          );
        }
        return;
      }
    };
    const timer = setTimeout(
      () =>
        finish(
          new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CONNECT_FAILED"),
        ),
      timeoutMs,
    );
    timer.unref();
    socket.on("data", onData);
    socket.once("error", onError);
    socket.once("close", onClose);
  });
}

function writeLogin(socket: Socket, line: string): Promise<void> {
  return new Promise((resolve, reject) => {
    try {
      socket.write(`${line}\r\n`, (error) => {
        if (error) {
          reject(
            new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CONNECT_FAILED"),
          );
        } else {
          resolve();
        }
      });
    } catch {
      reject(new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_CONNECT_FAILED"));
    }
  });
}

function closeSocket(socket: Socket): Promise<void> {
  if (socket.destroyed) return Promise.resolve();
  return new Promise((resolve) => {
    socket.once("close", resolve);
    socket.destroy();
  });
}

function boundedTimeout(value: number): number {
  if (!Number.isInteger(value) || value < 100 || value > 60_000) {
    throw new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_TIMEOUT_INVALID");
  }
  return value;
}

function boundedReconnectDelay(value: number): number {
  if (!Number.isInteger(value) || value < 100 || value > 60_000) {
    throw new CmCloudDirectAprsError("CMCLOUD_DIRECT_APRS_RECONNECT_INVALID");
  }
  return value;
}
