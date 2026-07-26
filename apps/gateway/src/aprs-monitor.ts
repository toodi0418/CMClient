import { createHash } from "node:crypto";
import net, { type Socket } from "node:net";
import { DatabaseSync } from "node:sqlite";

import {
  PROVISION_FINGERPRINT_PATTERN,
  type AprsAuthorizationProvider,
  type AprsConnectionAuthorization,
} from "./aprs-identity.js";

const DEFAULT_SESSION_CLOSE_TIMEOUT_MS = 5_000;
const APRS_OBSERVER_TTL_MS = 3 * 60 * 60 * 1_000;
const APRS_LOCAL_TX_TTL_MS = 30_000;
const MAX_APRS_LINE_BYTES = 512;
const MAX_APRS_BUFFER_BYTES = 1_024;
const APRS_CALLSIGN_PATTERN = /^[A-Z0-9]{1,6}(?:-(?:[1-9]|1[0-5]))?$/;
const APRS_LOGIN_CALLSIGN_PATTERN = /^[A-Z0-9]{3,6}(?:-[A-Z0-9]{1,2})?$/;
export const APRS_RX_FILTER_EXPRESSION = "p/BM/BN/BO/BP/BQ/BU/BV/BW/BX t/p";

export interface AprsMonitorTarget {
  callsign: string;
  mappingVersion: string;
  meshNetworkId: string;
  nodeNum: number;
}

export interface AprsRemotePosition {
  callsign: string;
  destination: string;
  info: string;
  infoDigest: string;
}

export interface AprsRemoteHighWaterState extends AprsMonitorTarget {
  latestInfoDigest: string;
  receivedAt: string;
}

export interface AprsMonitorResult {
  kind: "advanced" | "ignored" | "not_new";
  reason?: "malformed" | "unmapped_callsign";
  remote?: AprsRemotePosition;
  state?: AprsRemoteHighWaterState;
}

export interface AprsIsRxSession {
  close(): Promise<void>;
  readonly terminated?: Promise<void>;
}

export class AprsMonitorError extends Error {
  readonly code = "APRS_MONITOR_INVALID";

  constructor() {
    super("APRS_MONITOR_INVALID");
  }
}

export class AprsMonitorAuthorizationError extends Error {
  readonly code = "APRS_PROVISION_UNAVAILABLE";

  constructor() {
    super("APRS_PROVISION_UNAVAILABLE");
    this.name = "AprsMonitorAuthorizationError";
  }
}

export class AprsMonitorPersistenceError extends Error {
  readonly code = "APRS_MONITOR_PERSISTENCE_FAILED";

  constructor() {
    super("APRS_MONITOR_PERSISTENCE_FAILED");
  }
}

export class AprsRemoteHighWaterStore {
  constructor(private readonly database: DatabaseSync) {}

  apply(
    remote: AprsRemotePosition,
    target: AprsMonitorTarget,
    receivedAt: string,
  ): { advanced: boolean; state: AprsRemoteHighWaterState } {
    validateTarget(target);
    if (
      remote.callsign !== target.callsign ||
      remote.infoDigest !== digestInfo(remote.callsign, remote.info) ||
      !isTimestamp(receivedAt)
    ) {
      throw new AprsMonitorError();
    }
    let transactionOpen = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionOpen = true;
      const existing = this.database
        .prepare(
          "SELECT last_observed_at FROM aprs_observed_packets WHERE callsign = ? AND destination = ? AND info = ?",
        )
        .get(remote.callsign, remote.destination, remote.info);
      const advanced = !existing;
      this.database
        .prepare(
          "INSERT INTO aprs_observed_packets (callsign, destination, info, first_observed_at, last_observed_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(callsign, destination, info) DO UPDATE SET last_observed_at = excluded.last_observed_at",
        )
        .run(
          remote.callsign,
          remote.destination,
          remote.info,
          receivedAt,
          receivedAt,
        );
      this.pruneInsideTransaction(receivedAt);
      this.database.exec("COMMIT");
      transactionOpen = false;
      return {
        advanced,
        state: {
          ...target,
          latestInfoDigest: remote.infoDigest,
          receivedAt,
        },
      };
    } catch {
      if (transactionOpen) {
        try {
          this.database.exec("ROLLBACK");
        } catch {
          // Keep the stable persistence error.
        }
      }
      throw new AprsMonitorPersistenceError();
    }
  }

  canUploadData(data: string, target: AprsMonitorTarget, at: string): boolean {
    validateTarget(target);
    if (!isTimestamp(at)) {
      return false;
    }
    const packet = parseCmClientAprsLine(data);
    if (!packet || packet.callsign !== target.callsign) {
      return false;
    }
    try {
      const observerCutoff = new Date(
        Date.parse(at) - APRS_OBSERVER_TTL_MS,
      ).toISOString();
      const localCutoff = new Date(
        Date.parse(at) - APRS_LOCAL_TX_TTL_MS,
      ).toISOString();
      const observed = this.database
        .prepare(
          "SELECT 1 FROM aprs_observed_packets WHERE callsign = ? AND info = ? AND (destination = ? OR destination = '') AND last_observed_at >= ?",
        )
        .get(packet.callsign, packet.info, packet.destination, observerCutoff);
      const transmitted = this.database
        .prepare(
          "SELECT 1 FROM aprs_local_transmissions WHERE callsign = ? AND info = ? AND (destination = ? OR destination = '') AND transmitted_at >= ?",
        )
        .get(packet.callsign, packet.info, packet.destination, localCutoff);
      return !observed && !transmitted;
    } catch {
      return false;
    }
  }

  recordLocalTransmission(data: string, transmittedAt: string): void {
    const packet = parseCmClientAprsLine(data);
    if (!packet || !isTimestamp(transmittedAt)) {
      throw new AprsMonitorError();
    }
    try {
      this.database
        .prepare(
          "INSERT INTO aprs_local_transmissions (callsign, destination, info, transmitted_at) VALUES (?, ?, ?, ?) ON CONFLICT(callsign, destination, info) DO UPDATE SET transmitted_at = excluded.transmitted_at",
        )
        .run(packet.callsign, packet.destination, packet.info, transmittedAt);
    } catch {
      throw new AprsMonitorPersistenceError();
    }
  }

  private pruneInsideTransaction(at: string): void {
    const observerCutoff = new Date(
      Date.parse(at) - APRS_OBSERVER_TTL_MS,
    ).toISOString();
    const localCutoff = new Date(
      Date.parse(at) - APRS_LOCAL_TX_TTL_MS,
    ).toISOString();
    this.database
      .prepare("DELETE FROM aprs_observed_packets WHERE last_observed_at < ?")
      .run(observerCutoff);
    this.database
      .prepare("DELETE FROM aprs_local_transmissions WHERE transmitted_at < ?")
      .run(localCutoff);
  }
}

export class AprsIsMonitor {
  private targetsByCallsign: ReadonlyMap<string, AprsMonitorTarget>;

  constructor(
    targets: readonly AprsMonitorTarget[],
    private readonly highWater: AprsRemoteHighWaterStore,
  ) {
    this.targetsByCallsign = targetMap(targets);
  }

  filterExpression(): string {
    if (this.targetsByCallsign.size === 0) {
      throw new AprsMonitorError();
    }
    return APRS_RX_FILTER_EXPRESSION;
  }

  replaceTargets(targets: readonly AprsMonitorTarget[]): void {
    this.targetsByCallsign = targetMap(targets);
  }

  observeLine(line: string, receivedAt: string): AprsMonitorResult {
    const remote = parseCmClientAprsLine(line);
    if (!remote || !isTimestamp(receivedAt)) {
      return { kind: "ignored", reason: "malformed" };
    }
    const target = this.targetsByCallsign.get(remote.callsign);
    if (!target) {
      return { kind: "ignored", reason: "unmapped_callsign" };
    }
    const result = this.highWater.apply(remote, target, receivedAt);
    return {
      kind: result.advanced ? "advanced" : "not_new",
      remote,
      state: result.state,
    };
  }
}

export class AprsIsRxClient {
  constructor(
    private readonly options: {
      host: string;
      port: number;
      authorizationProvider: AprsAuthorizationProvider;
      provisionFingerprint: string;
      filterExpression: string;
      timeoutMs?: number;
      closeTimeoutMs?: number;
    },
  ) {
    if (
      !options.host.trim() ||
      !Number.isInteger(options.port) ||
      options.port < 1 ||
      options.port > 65_535 ||
      typeof options.authorizationProvider !== "function" ||
      !PROVISION_FINGERPRINT_PATTERN.test(options.provisionFingerprint) ||
      !isFilterExpression(options.filterExpression) ||
      (options.timeoutMs !== undefined &&
        (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0)) ||
      (options.closeTimeoutMs !== undefined &&
        (!Number.isFinite(options.closeTimeoutMs) ||
          options.closeTimeoutMs <= 0))
    ) {
      throw new AprsMonitorError();
    }
  }

  async connect(
    onLine: (line: string) => void,
    onLineError: (error: unknown) => void = () => undefined,
  ): Promise<AprsIsRxSession> {
    const authorization = resolveAuthorization(
      this.options.authorizationProvider,
      this.options.provisionFingerprint,
    );
    const login = authorizationLogin(authorization);
    const loginLine = `${authorization.loginLine} filter ${this.options.filterExpression}\r\n`;
    if (Buffer.byteLength(loginLine, "utf8") > MAX_APRS_LINE_BYTES) {
      throw new AprsMonitorAuthorizationError();
    }
    const socket = net.createConnection({
      host: this.options.host,
      port: this.options.port,
    });
    const reader = attachVerifiedLineReader(
      socket,
      login.callsign,
      login.expectedStatus,
      this.options.timeoutMs ?? 10_000,
      onLine,
      onLineError,
    );
    try {
      await onceConnected(socket, this.options.timeoutMs ?? 10_000);
      const connectedAuthorization = resolveAuthorization(
        this.options.authorizationProvider,
        this.options.provisionFingerprint,
      );
      if (!authorizationMatches(authorization, connectedAuthorization)) {
        throw new AprsMonitorAuthorizationError();
      }
      await write(socket, loginLine);
      await reader.verified;
      const verifiedAuthorization = resolveAuthorization(
        this.options.authorizationProvider,
        this.options.provisionFingerprint,
      );
      if (!authorizationMatches(authorization, verifiedAuthorization)) {
        throw new AprsMonitorAuthorizationError();
      }
      socket.unref();
      return {
        terminated: reader.terminated,
        close: async () => {
          reader.beginClose();
          await closeSocket(
            socket,
            this.options.closeTimeoutMs ?? DEFAULT_SESSION_CLOSE_TIMEOUT_MS,
          );
        },
      };
    } catch (error) {
      reader.beginClose();
      socket.destroy();
      if (error instanceof AprsMonitorAuthorizationError) {
        throw error;
      }
      throw new AprsMonitorError();
    }
  }
}

export function parseCmClientAprsLine(
  line: string,
): AprsRemotePosition | undefined {
  if (
    Buffer.byteLength(line, "utf8") > MAX_APRS_LINE_BYTES ||
    /[\r\n]/.test(line)
  ) {
    return undefined;
  }
  const header =
    /^([A-Z0-9]{1,6}(?:-(?:[1-9]|1[0-5]))?)>([A-Z0-9]{1,6}(?:-[0-9]{1,2})?)(?:,[^:\r\n]*)?:(.*)$/i.exec(
      line,
    );
  if (!header) {
    return undefined;
  }
  const callsign = header[1]!.toUpperCase();
  const destination = header[2]!.toUpperCase();
  const info = header[3]!;
  if (!APRS_CALLSIGN_PATTERN.test(callsign) || info.length === 0) {
    return undefined;
  }
  return {
    callsign,
    destination,
    info,
    infoDigest: digestInfo(callsign, info),
  };
}

function digestInfo(callsign: string, info: string): string {
  return createHash("sha256")
    .update(callsign, "utf8")
    .update("\u0000", "utf8")
    .update(info, "utf8")
    .digest("hex");
}

function validateTarget(target: AprsMonitorTarget): void {
  if (
    !APRS_CALLSIGN_PATTERN.test(target.callsign) ||
    !target.mappingVersion.trim() ||
    !target.meshNetworkId.trim() ||
    !Number.isInteger(target.nodeNum) ||
    target.nodeNum < 0 ||
    target.nodeNum > 4_294_967_295
  ) {
    throw new AprsMonitorError();
  }
}

function targetMap(
  targets: readonly AprsMonitorTarget[],
): ReadonlyMap<string, AprsMonitorTarget> {
  const targetsByCallsign = new Map<string, AprsMonitorTarget>();
  for (const target of targets) {
    validateTarget(target);
    if (targetsByCallsign.has(target.callsign)) {
      throw new AprsMonitorError();
    }
    targetsByCallsign.set(target.callsign, { ...target });
  }
  return targetsByCallsign;
}

function isFilterExpression(value: string): boolean {
  return value === APRS_RX_FILTER_EXPRESSION;
}

function isTimestamp(value: string): boolean {
  return Number.isFinite(Date.parse(value));
}

function onceConnected(socket: Socket, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const finish = (error?: AprsMonitorError) => {
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("error", onError);
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    };
    const onConnect = () => finish();
    const onError = () => finish(new AprsMonitorError());
    const timer = setTimeout(onError, timeoutMs);
    timer.unref();
    socket.once("connect", onConnect);
    socket.once("error", onError);
  });
}

function write(socket: Socket, value: string): Promise<void> {
  return new Promise((resolve, reject) =>
    socket.write(value, (error) => (error ? reject(error) : resolve())),
  );
}

function resolveAuthorization(
  provider: AprsAuthorizationProvider,
  expectedProvisionFingerprint: string,
): AprsConnectionAuthorization {
  let authorization: AprsConnectionAuthorization | undefined;
  try {
    authorization = provider();
  } catch {
    throw new AprsMonitorAuthorizationError();
  }
  if (
    !authorization ||
    authorization.provisionFingerprint !== expectedProvisionFingerprint ||
    !PROVISION_FINGERPRINT_PATTERN.test(authorization.provisionFingerprint) ||
    !isValidLoginLine(authorization.loginLine)
  ) {
    throw new AprsMonitorAuthorizationError();
  }
  return authorization;
}

function isValidLoginLine(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.trim().length > 0 &&
    !/[\r\n]/.test(value) &&
    Buffer.byteLength(value, "utf8") <= MAX_APRS_LINE_BYTES
  );
}

function authorizationLogin(authorization: AprsConnectionAuthorization): {
  callsign: string;
  expectedStatus: "verified";
} {
  const match = /^user\s+([^\s]+)\s+pass\s+([0-9]{1,5})(?:\s|$)/.exec(
    authorization.loginLine,
  );
  const passcode = Number(match?.[2]);
  if (
    !match ||
    !APRS_LOGIN_CALLSIGN_PATTERN.test(match[1]!) ||
    !Number.isInteger(passcode) ||
    passcode < 0 ||
    passcode > 32_767
  ) {
    throw new AprsMonitorAuthorizationError();
  }
  return {
    callsign: match[1]!,
    expectedStatus: "verified",
  };
}

function authorizationMatches(
  expected: AprsConnectionAuthorization,
  current: AprsConnectionAuthorization,
): boolean {
  return (
    expected.provisionFingerprint === current.provisionFingerprint &&
    expected.loginLine === current.loginLine
  );
}

function parseLogresp(
  line: string,
):
  | { callsign: string; status: "verified" | "unverified" }
  | "malformed"
  | undefined {
  if (!/^#\s*logresp\b/i.test(line)) {
    return undefined;
  }
  const match =
    /^#\s*logresp\s+([^\s,]+)\s+(verified|unverified)(?:[\s,]|$)/i.exec(line);
  if (!match || !APRS_LOGIN_CALLSIGN_PATTERN.test(match[1]!)) {
    return "malformed";
  }
  return {
    callsign: match[1]!,
    status: match[2]!.toLowerCase() as "verified" | "unverified",
  };
}

function attachVerifiedLineReader(
  socket: Socket,
  expectedCallsign: string,
  expectedStatus: "verified",
  timeoutMs: number,
  onLine: (line: string) => void,
  onLineError: (error: unknown) => void,
): {
  verified: Promise<void>;
  terminated: Promise<void>;
  beginClose: () => void;
} {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let state: "awaiting" | "verified" | "failed" | "closing" = "awaiting";
  let resolveVerified!: () => void;
  let rejectVerified!: (error: AprsMonitorError) => void;
  const verified = new Promise<void>((resolve, reject) => {
    resolveVerified = resolve;
    rejectVerified = reject;
  });
  let resolveTerminated!: () => void;
  const terminated = new Promise<void>((resolve) => {
    resolveTerminated = resolve;
  });
  let terminationSettled = false;
  const markTerminated = () => {
    if (!terminationSettled) {
      terminationSettled = true;
      resolveTerminated();
    }
  };
  void verified.catch(() => undefined);
  const fail = (error = new AprsMonitorError()) => {
    if (state === "failed" || state === "closing") {
      return;
    }
    const wasAwaiting = state === "awaiting";
    state = "failed";
    clearTimeout(timer);
    if (wasAwaiting) {
      rejectVerified(error);
    } else {
      try {
        onLineError(error);
      } catch {
        // Consumer reporting must not escape a socket callback.
      }
    }
    markTerminated();
    socket.destroy();
  };
  const processLine = (line: string) => {
    if (Buffer.byteLength(line, "utf8") > MAX_APRS_LINE_BYTES) {
      fail();
      return;
    }
    const logresp = parseLogresp(line);
    if (logresp !== undefined) {
      if (
        logresp === "malformed" ||
        logresp.callsign !== expectedCallsign ||
        logresp.status !== expectedStatus
      ) {
        fail();
        return;
      }
      if (state === "awaiting") {
        state = "verified";
        clearTimeout(timer);
        resolveVerified();
      }
      return;
    }
    if (state !== "verified" || line.startsWith("#")) {
      return;
    }
    try {
      onLine(line);
    } catch (error) {
      try {
        onLineError(error);
      } catch {
        // Keep callback failures isolated from the socket EventEmitter.
      }
    }
  };
  const onData = (chunk: Buffer) => {
    if (state === "failed" || state === "closing") {
      return;
    }
    try {
      buffer += decoder.decode(chunk, { stream: true });
    } catch {
      fail();
      return;
    }
    let lineEnd = buffer.indexOf("\n");
    while (lineEnd >= 0) {
      const line = buffer.slice(0, lineEnd).replace(/\r$/, "");
      buffer = buffer.slice(lineEnd + 1);
      processLine(line);
      if (socket.destroyed) {
        return;
      }
      lineEnd = buffer.indexOf("\n");
    }
    if (Buffer.byteLength(buffer, "utf8") > MAX_APRS_BUFFER_BYTES) {
      fail();
    }
  };
  const timer = setTimeout(() => fail(), timeoutMs);
  timer.unref();
  socket.on("data", onData);
  socket.on("error", () => fail());
  socket.on("end", () => fail());
  socket.on("close", () => fail());
  return {
    verified,
    terminated,
    beginClose: () => {
      if (state === "awaiting") {
        rejectVerified(new AprsMonitorError());
      }
      state = "closing";
      clearTimeout(timer);
      markTerminated();
    },
  };
}

function closeSocket(socket: Socket, timeoutMs: number): Promise<void> {
  if (socket.destroyed) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      socket.off("close", finish);
      resolve();
    };
    const timer = setTimeout(() => {
      socket.destroy();
      finish();
    }, timeoutMs);
    timer.unref();
    socket.once("close", finish);
    socket.end();
  });
}
