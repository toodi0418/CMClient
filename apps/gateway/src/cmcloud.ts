import { createHash, randomUUID } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";

import WebSocket from "ws";

import {
  parseCmCloudDirectAprsCapability,
  type CmCloudDirectAprsCapability,
  type CmCloudDirectAprsDispatchResult,
  type CmCloudDirectAprsEgress,
} from "./cmcloud-aprs.js";
import type { CmCloudDirectAprsIgateRuntime } from "./cmcloud-igate.js";
import type { DomainEventBus } from "./events.js";

export const CMCLOUD_AGENT_SUBPROTOCOL = "cmcloud.agent.v1";
export const CMCLOUD_AGENT_PROTOCOL_VERSION = 1;
export const CMCLOUD_RAW_FRAME_MAGIC = Buffer.from("CMC1", "ascii");
export const CMCLOUD_RAW_FRAME_TYPE = 1;
export const CMCLOUD_RAW_FRAME_MAX_BYTES = 512 * 1024;

const MAX_SAFE_SEQUENCE = Number.MAX_SAFE_INTEGER;
const DEFAULT_ACK_TIMEOUT_MS = 15_000;
const DEFAULT_HANDSHAKE_TIMEOUT_MS = 10_000;
const DEFAULT_RECONNECT_INITIAL_DELAY_MS = 1_000;
const DEFAULT_RECONNECT_MAX_DELAY_MS = 60_000;
const MAX_CONTROL_MESSAGE_BYTES = 64 * 1024;
const MAX_CACHED_APRS_DISPATCH_ACKS = 1_024;
const TERMINAL_SERVER_ERROR_CODES = new Set([
  "CLIENT_UPGRADE_REQUIRED",
  "INVALID_DEVICE_CREDENTIAL",
  "INVALID_AGENT_FRAME",
  "INVALID_SESSION_EPOCH",
  "LANE_GAP",
  "SEQUENCE_REUSE_CONFLICT",
  "UNSUPPORTED_PROTOCOL",
]);

export type CmCloudLane = "live";

export interface CmCloudRawOutboxEntry {
  readonly messageId: string;
  readonly lane: CmCloudLane;
  readonly laneSequence: number;
  readonly capturedAt: string;
  readonly body: Uint8Array;
  readonly bodySha256: string;
  readonly attempts: number;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly lastAttemptAt?: string;
  readonly lastErrorCode?: string;
  readonly receiptId?: string;
  readonly acknowledgedAt?: string;
}

export interface CmCloudRawFrameSink {
  enqueueRawFrame(body: Uint8Array, capturedAt: string): CmCloudRawOutboxEntry;
}

export interface EnqueueCmCloudRawFrame {
  readonly body: Uint8Array;
  readonly capturedAt: string;
  readonly messageId?: string;
}

export class CmCloudOutboxError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "CmCloudOutboxError";
  }
}

/**
 * Persists the exact unframed `meshtastic.FromRadio` bytes before any network
 * operation. The monotonically allocated lane sequence survives acknowledged
 * row retention, so a restart cannot reuse a CMCloud sequence.
 */
export class CmCloudRawOutboxRepository {
  constructor(
    private readonly database: DatabaseSync,
    private readonly idFactory: () => string = randomUUID,
  ) {}

  enqueue(input: EnqueueCmCloudRawFrame): CmCloudRawOutboxEntry {
    const body = validateRawBody(input.body);
    const capturedAt = canonicalTimestamp(input.capturedAt);
    const messageId = validateUuid(input.messageId ?? this.idFactory());
    const bodySha256 = createHash("sha256").update(body).digest("hex");
    let started = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      started = true;
      const laneSequence = this.allocateLiveSequence();
      this.database
        .prepare(
          "INSERT INTO cmcloud_raw_outbox (message_id, lane, lane_sequence, captured_at, body, body_sha256, status, attempts, created_at, updated_at) VALUES (?, 'live', ?, ?, ?, ?, 'pending', 0, ?, ?)",
        )
        .run(
          messageId,
          laneSequence,
          capturedAt,
          Buffer.from(body),
          bodySha256,
          capturedAt,
          capturedAt,
        );
      this.database.exec("COMMIT");
      started = false;
      const stored = this.find(messageId);
      if (!stored) {
        throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STORE_FAILED");
      }
      return stored;
    } catch (error) {
      if (started) rollbackQuietly(this.database);
      if (error instanceof CmCloudOutboxError) throw error;
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STORE_FAILED");
    }
  }

  find(messageId: string): CmCloudRawOutboxEntry | undefined {
    const row = this.database
      .prepare("SELECT * FROM cmcloud_raw_outbox WHERE message_id = ?")
      .get(validateUuid(messageId));
    return row ? toCmCloudOutboxEntry(row) : undefined;
  }

  nextPending(): CmCloudRawOutboxEntry | undefined {
    const row = this.database
      .prepare(
        "SELECT * FROM cmcloud_raw_outbox INDEXED BY cmcloud_raw_outbox_pending_sequence_index WHERE status = 'pending' ORDER BY lane_sequence ASC LIMIT 1",
      )
      .get();
    return row ? toCmCloudOutboxEntry(row) : undefined;
  }

  recordAttempt(messageId: string, attemptedAt: string): CmCloudRawOutboxEntry {
    const now = canonicalTimestamp(attemptedAt);
    const result = this.database
      .prepare(
        "UPDATE cmcloud_raw_outbox SET attempts = attempts + 1, last_attempt_at = ?, last_error_code = NULL, updated_at = ? WHERE message_id = ? AND status = 'pending'",
      )
      .run(now, now, validateUuid(messageId));
    if (Number(result.changes) !== 1) {
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STATE_INVALID");
    }
    const entry = this.find(messageId);
    if (!entry) throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STATE_INVALID");
    return entry;
  }

  recordError(messageId: string, code: string, occurredAt: string): void {
    const now = canonicalTimestamp(occurredAt);
    const errorCode = validateStableErrorCode(code);
    const result = this.database
      .prepare(
        "UPDATE cmcloud_raw_outbox SET last_error_code = ?, updated_at = ? WHERE message_id = ? AND status = 'pending'",
      )
      .run(errorCode, now, validateUuid(messageId));
    if (Number(result.changes) !== 1) {
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STATE_INVALID");
    }
  }

  acknowledge(input: {
    messageId: string;
    lane: CmCloudLane;
    laneSequence: number;
    receiptId: string;
    acknowledgedAt: string;
  }): CmCloudRawOutboxEntry {
    if (input.lane !== "live" || !validPositiveInteger(input.laneSequence)) {
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_ACK_INVALID");
    }
    const messageId = validateUuid(input.messageId);
    const receiptId = validateUuid(input.receiptId);
    const acknowledgedAt = canonicalTimestamp(input.acknowledgedAt);
    const current = this.find(messageId);
    if (
      !current ||
      current.lane !== input.lane ||
      current.laneSequence !== input.laneSequence
    ) {
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_ACK_INVALID");
    }
    if (current.acknowledgedAt) {
      if (current.receiptId !== receiptId) {
        throw new CmCloudOutboxError("CMCLOUD_OUTBOX_ACK_INVALID");
      }
      return current;
    }
    const result = this.database
      .prepare(
        "UPDATE cmcloud_raw_outbox SET status = 'acknowledged', receipt_id = ?, acknowledged_at = ?, last_error_code = NULL, updated_at = ? WHERE message_id = ? AND lane = ? AND lane_sequence = ? AND status = 'pending'",
      )
      .run(
        receiptId,
        acknowledgedAt,
        acknowledgedAt,
        messageId,
        input.lane,
        input.laneSequence,
      );
    if (Number(result.changes) !== 1) {
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_ACK_INVALID");
    }
    const stored = this.find(messageId);
    if (!stored) throw new CmCloudOutboxError("CMCLOUD_OUTBOX_ACK_INVALID");
    return stored;
  }

  deleteAcknowledgedBefore(cutoffExclusive: string, limit = 1_000): number {
    const cutoff = canonicalTimestamp(cutoffExclusive);
    if (!Number.isInteger(limit) || limit < 1 || limit > 10_000) {
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_RETENTION_INVALID");
    }
    try {
      const result = this.database
        .prepare(
          "DELETE FROM cmcloud_raw_outbox WHERE message_id IN (SELECT message_id FROM cmcloud_raw_outbox WHERE status = 'acknowledged' AND acknowledged_at < ? ORDER BY acknowledged_at ASC, lane_sequence ASC LIMIT ?)",
        )
        .run(cutoff, limit);
      return Number(result.changes);
    } catch {
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_RETENTION_FAILED");
    }
  }

  pendingCount(): number {
    const row = this.database
      .prepare(
        "SELECT COUNT(*) AS count FROM cmcloud_raw_outbox WHERE status = 'pending'",
      )
      .get();
    const count = Number(row?.count);
    if (!Number.isSafeInteger(count) || count < 0) {
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STATE_INVALID");
    }
    return count;
  }

  private allocateLiveSequence(): number {
    const current = this.database
      .prepare(
        "SELECT next_sequence FROM cmcloud_lane_state WHERE lane = 'live'",
      )
      .get();
    if (!current) {
      this.database
        .prepare(
          "INSERT INTO cmcloud_lane_state (lane, next_sequence) VALUES ('live', 2)",
        )
        .run();
      return 1;
    }
    const next = Number(current.next_sequence);
    if (!validPositiveInteger(next) || next >= MAX_SAFE_SEQUENCE) {
      throw new CmCloudOutboxError("CMCLOUD_LANE_SEQUENCE_EXHAUSTED");
    }
    const result = this.database
      .prepare(
        "UPDATE cmcloud_lane_state SET next_sequence = ? WHERE lane = 'live' AND next_sequence = ?",
      )
      .run(next + 1, next);
    if (Number(result.changes) !== 1) {
      throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STORE_FAILED");
    }
    return next;
  }
}

export interface CmCloudRuntimeConfiguration {
  readonly url: string;
  readonly installationId: string;
  readonly installationGeneration: number;
  readonly credentialVersion: number;
}

export class CmCloudConfigurationError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "CmCloudConfigurationError";
  }
}

/**
 * Cloud mode is deliberately opt-in so legacy clients retain their existing
 * CallMesh/APRS behavior until migration. In required mode all identity fields
 * must be explicit; the device credential itself only arrives through the
 * Agent's private bootstrap pipe, never an environment variable.
 */
export function parseCmCloudRuntimeConfiguration(
  environment: Record<string, string | undefined>,
): CmCloudRuntimeConfiguration | undefined {
  if (environment.CMCLIENT_CMCLOUD_DEVICE_CREDENTIAL?.trim()) {
    throw new CmCloudConfigurationError("CMCLOUD_SECRET_ENVIRONMENT_FORBIDDEN");
  }
  const mode =
    environment.CMCLIENT_CMCLOUD_MODE?.trim().toLowerCase() || "disabled";
  if (mode === "disabled") {
    return undefined;
  }
  if (mode !== "required") {
    throw new CmCloudConfigurationError("CMCLOUD_MODE_CONFIGURATION_INVALID");
  }
  const url = parseCmCloudEndpoint(environment.CMCLIENT_CMCLOUD_URL);
  const installationId = validateUuid(
    environment.CMCLIENT_CMCLOUD_INSTALLATION_ID?.trim() ?? "",
    "CMCLOUD_INSTALLATION_CONFIGURATION_INVALID",
  );
  const installationGeneration = parseNonNegativeInteger(
    environment.CMCLIENT_CMCLOUD_INSTALLATION_GENERATION,
    "CMCLOUD_INSTALLATION_CONFIGURATION_INVALID",
  );
  const credentialVersion = parsePositiveInteger(
    environment.CMCLIENT_CMCLOUD_CREDENTIAL_VERSION,
    "CMCLOUD_CREDENTIAL_CONFIGURATION_INVALID",
  );
  return {
    url,
    installationId,
    installationGeneration,
    credentialVersion,
  };
}

export function parseCmCloudEndpoint(value: string | undefined): string {
  const source = value?.trim();
  if (
    !source ||
    source.length > 2_048 ||
    source.includes("\r") ||
    source.includes("\n") ||
    source.includes("\u0000")
  ) {
    throw new CmCloudConfigurationError("CMCLOUD_URL_CONFIGURATION_INVALID");
  }
  let parsed: URL;
  try {
    parsed = new URL(source);
  } catch {
    throw new CmCloudConfigurationError("CMCLOUD_URL_CONFIGURATION_INVALID");
  }
  if (
    parsed.protocol !== "wss:" ||
    !parsed.hostname ||
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash ||
    parsed.pathname !== "/agent/v1"
  ) {
    throw new CmCloudConfigurationError("CMCLOUD_URL_CONFIGURATION_INVALID");
  }
  return parsed.toString();
}

export interface CmCloudSocket {
  on(event: "open", listener: () => void): unknown;
  on(
    event: "message",
    listener: (data: unknown, isBinary: boolean) => void,
  ): unknown;
  on(event: "close", listener: (code: number, reason: Buffer) => void): unknown;
  on(event: "error", listener: (error: Error) => void): unknown;
  send(
    data: string | Uint8Array,
    options: { binary: boolean },
    callback: (error?: Error) => void,
  ): void;
  close(code?: number, reason?: string): void;
}

export interface CmCloudSocketFactoryOptions {
  readonly headers: Readonly<Record<string, string>>;
  readonly perMessageDeflate: false;
  readonly maxPayload: number;
}

export type CmCloudSocketFactory = (
  url: string,
  protocols: readonly string[],
  options: CmCloudSocketFactoryOptions,
) => CmCloudSocket;

export interface CmCloudAgentClientOptions extends CmCloudRuntimeConfiguration {
  readonly deviceCredential: string;
  readonly clientVersion: string;
  readonly outbox: CmCloudRawOutboxRepository;
  readonly events?: DomainEventBus;
  readonly clock?: () => Date;
  readonly socketFactory?: CmCloudSocketFactory;
  readonly bootIdFactory?: () => string;
  readonly ackTimeoutMs?: number;
  readonly handshakeTimeoutMs?: number;
  readonly reconnectInitialDelayMs?: number;
  readonly reconnectMaximumDelayMs?: number;
  /**
   * This egress is intentionally capability-driven. It never receives a
   * CallMesh mapping or a locally configured APRS callsign.
   */
  readonly directAprsEgress?: CmCloudDirectAprsEgress;
  /** Schedules only CMCloud-granted station self-identification packets. */
  readonly directAprsIgate?: CmCloudDirectAprsIgateRuntime;
}

export interface CmCloudAgentStatus {
  readonly configured: true;
  readonly state:
    "stopped" | "connecting" | "awaiting_hello" | "ready" | "blocked";
  readonly pendingOutbox: number;
  readonly reconnectAttempt: number;
  readonly directAprs?: ReturnType<CmCloudDirectAprsIgateRuntime["status"]>;
  readonly terminalCode?: string;
  readonly lastErrorCode?: string;
}

interface CmCloudSession {
  readonly connectionEpoch: number;
  readonly installationGeneration: number;
  readonly credentialVersion: number;
  readonly heartbeatIntervalMs: number;
  readonly aprsMode: "disabled" | "shadow" | "enabled";
  readonly directAprs?: CmCloudDirectAprsCapability;
}

interface CmCloudAprsDispatchAck {
  readonly type: "aprs_dispatch_ack";
  readonly dispatchId: string;
  readonly outcome: "submitted" | "retryable_failure" | "uncertain";
  readonly errorCode?: string;
}

/**
 * One authenticated CMCloud WebSocket owns all 2.0 upstream raw transport.
 * A sent frame remains pending until `raw_ack`; close/reconnect simply encodes
 * the stored byte-identical body with the newly issued connection epoch.
 */
export class CmCloudAgentClient implements CmCloudRawFrameSink {
  private readonly endpoint: string;
  private readonly clock: () => Date;
  private readonly socketFactory: CmCloudSocketFactory;
  private readonly bootId: string;
  private readonly ackTimeoutMs: number;
  private readonly handshakeTimeoutMs: number;
  private readonly reconnectInitialDelayMs: number;
  private readonly reconnectMaximumDelayMs: number;
  private running = false;
  private state: CmCloudAgentStatus["state"] = "stopped";
  private reconnectAttempt = 0;
  private terminalCode: string | undefined;
  private lastErrorCode: string | undefined;
  private socket: CmCloudSocket | undefined;
  private socketGeneration = 0;
  private session: CmCloudSession | undefined;
  private inFlight: CmCloudRawOutboxEntry | undefined;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private handshakeTimer: NodeJS.Timeout | undefined;
  private heartbeatTimer: NodeJS.Timeout | undefined;
  private ackTimer: NodeJS.Timeout | undefined;
  private flushPromise: Promise<void> | undefined;
  private flushRequested = false;
  // Fences asynchronous APRS-IS reconfiguration from a prior CMCloud socket.
  // The scheduler must be restored immediately on a new hello even while the
  // APRS-IS login is still in progress.
  private directAprsConfigurationGeneration = 0;
  private pendingAprsDispatchId: string | undefined;
  private readonly completedAprsDispatches = new Map<
    string,
    CmCloudAprsDispatchAck
  >();

  constructor(private readonly options: CmCloudAgentClientOptions) {
    this.endpoint = parseCmCloudEndpoint(options.url);
    validateCredential(options.deviceCredential);
    validateUuid(
      options.installationId,
      "CMCLOUD_INSTALLATION_CONFIGURATION_INVALID",
    );
    if (!validNonNegativeInteger(options.installationGeneration)) {
      throw new CmCloudConfigurationError(
        "CMCLOUD_INSTALLATION_CONFIGURATION_INVALID",
      );
    }
    if (!validPositiveInteger(options.credentialVersion)) {
      throw new CmCloudConfigurationError(
        "CMCLOUD_CREDENTIAL_CONFIGURATION_INVALID",
      );
    }
    validateClientVersion(options.clientVersion);
    this.clock = options.clock ?? (() => new Date());
    this.socketFactory = options.socketFactory ?? defaultCmCloudSocketFactory;
    this.bootId = validateUuid(
      (options.bootIdFactory ?? randomUUID)(),
      "CMCLOUD_BOOT_ID_INVALID",
    );
    this.ackTimeoutMs = boundedDelay(
      options.ackTimeoutMs ?? DEFAULT_ACK_TIMEOUT_MS,
      "CMCLOUD_ACK_TIMEOUT_CONFIGURATION_INVALID",
    );
    this.handshakeTimeoutMs = boundedDelay(
      options.handshakeTimeoutMs ?? DEFAULT_HANDSHAKE_TIMEOUT_MS,
      "CMCLOUD_HANDSHAKE_TIMEOUT_CONFIGURATION_INVALID",
    );
    this.reconnectInitialDelayMs = boundedDelay(
      options.reconnectInitialDelayMs ?? DEFAULT_RECONNECT_INITIAL_DELAY_MS,
      "CMCLOUD_RECONNECT_CONFIGURATION_INVALID",
    );
    this.reconnectMaximumDelayMs = boundedDelay(
      options.reconnectMaximumDelayMs ?? DEFAULT_RECONNECT_MAX_DELAY_MS,
      "CMCLOUD_RECONNECT_CONFIGURATION_INVALID",
    );
    if (this.reconnectMaximumDelayMs < this.reconnectInitialDelayMs) {
      throw new CmCloudConfigurationError(
        "CMCLOUD_RECONNECT_CONFIGURATION_INVALID",
      );
    }
    options.directAprsEgress?.setReadinessListener(() => {
      options.directAprsIgate?.onEgressReadinessChanged();
      this.onDirectAprsReadinessChanged();
    });
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.terminalCode = undefined;
    this.openSocket();
  }

  async stop(): Promise<void> {
    this.running = false;
    this.directAprsConfigurationGeneration += 1;
    this.clearTimers();
    this.session = undefined;
    this.inFlight = undefined;
    this.pendingAprsDispatchId = undefined;
    const socket = this.socket;
    this.socket = undefined;
    this.state = "stopped";
    if (socket) {
      try {
        socket.close(1000, "CMClient shutting down");
      } catch {
        // The socket is already terminal. The outbox is intentionally retained.
      }
    }
    await this.options.directAprsIgate?.stop();
    await this.options.directAprsEgress?.stop();
  }

  /**
   * CMCloud-only mesh ingest uses this to feed decoded counters to the direct
   * station telemetry family without constructing a legacy APRS runtime.
   */
  directAprsIgate(): CmCloudDirectAprsIgateRuntime | undefined {
    return this.options.directAprsIgate;
  }

  enqueueRawFrame(body: Uint8Array, capturedAt: string): CmCloudRawOutboxEntry {
    const entry = this.options.outbox.enqueue({ body, capturedAt });
    this.publish("cmcloud.raw.queued", {
      messageId: entry.messageId,
      lane: entry.lane,
      laneSequence: entry.laneSequence,
      bytes: entry.body.length,
    });
    void this.flush();
    return entry;
  }

  status(): CmCloudAgentStatus {
    return {
      configured: true,
      state: this.state,
      pendingOutbox: this.options.outbox.pendingCount(),
      reconnectAttempt: this.reconnectAttempt,
      ...(this.options.directAprsIgate
        ? { directAprs: this.options.directAprsIgate.status() }
        : {}),
      ...(this.terminalCode ? { terminalCode: this.terminalCode } : {}),
      ...(this.lastErrorCode ? { lastErrorCode: this.lastErrorCode } : {}),
    };
  }

  private openSocket(): void {
    if (!this.running || this.terminalCode || this.socket) return;
    this.state = "connecting";
    const generation = ++this.socketGeneration;
    let socket: CmCloudSocket;
    try {
      socket = this.socketFactory(this.endpoint, [CMCLOUD_AGENT_SUBPROTOCOL], {
        headers: { Authorization: `Bearer ${this.options.deviceCredential}` },
        perMessageDeflate: false,
        maxPayload: CMCLOUD_RAW_FRAME_MAX_BYTES + MAX_CONTROL_MESSAGE_BYTES,
      });
    } catch {
      this.recordConnectionError("CMCLOUD_CONNECT_FAILED");
      this.scheduleReconnect();
      return;
    }
    this.socket = socket;
    socket.on("open", () => {
      if (!this.isActiveSocket(generation, socket)) return;
      this.state = "awaiting_hello";
      this.handshakeTimer = setTimeout(() => {
        if (this.isActiveSocket(generation, socket) && !this.session) {
          this.recordConnectionError("CMCLOUD_HELLO_TIMEOUT");
          this.closeCurrentSocket(4008, "server hello timeout");
        }
      }, this.handshakeTimeoutMs);
      this.handshakeTimer.unref();
      void this.sendControl(socket, {
        type: "client_hello",
        protocolVersion: CMCLOUD_AGENT_PROTOCOL_VERSION,
        clientVersion: this.options.clientVersion,
        installationId: this.options.installationId,
        installationGeneration: this.options.installationGeneration,
        credentialVersion: this.options.credentialVersion,
        bootId: this.bootId,
      }).catch(() => {
        this.recordConnectionError("CMCLOUD_HELLO_SEND_FAILED");
        this.closeCurrentSocket(1011, "hello send failed");
      });
    });
    socket.on("message", (data, isBinary) => {
      if (!this.isActiveSocket(generation, socket)) return;
      this.handleMessage(socket, data, isBinary);
    });
    socket.on("error", () => {
      if (!this.isActiveSocket(generation, socket)) return;
      this.recordConnectionError("CMCLOUD_SOCKET_ERROR");
      this.closeCurrentSocket(1011, "socket error");
    });
    socket.on("close", () => {
      if (generation !== this.socketGeneration) return;
      this.detachSocket(socket);
    });
  }

  private handleMessage(
    socket: CmCloudSocket,
    data: unknown,
    isBinary: boolean,
  ): void {
    if (isBinary) {
      this.failTerminal("CMCLOUD_UNEXPECTED_BINARY_CONTROL");
      return;
    }
    const control = parseControlMessage(data);
    if (!control) {
      this.failTerminal("CMCLOUD_CONTROL_FRAME_INVALID");
      return;
    }
    if (!this.session) {
      if (control.type === "error") {
        this.handleServerError(control);
        return;
      }
      if (control.type !== "server_hello") {
        this.failTerminal("CMCLOUD_SERVER_HELLO_REQUIRED");
        return;
      }
      this.acceptServerHello(socket, control);
      return;
    }
    switch (control.type) {
      case "raw_ack":
        this.acceptRawAcknowledgement(control);
        return;
      case "heartbeat_ack":
        if (!validTimestamp(control.leaseExpiresAt)) {
          this.failTerminal("CMCLOUD_HEARTBEAT_ACK_INVALID");
        }
        return;
      case "aprs_dispatch":
        void this.acceptAprsDispatch(socket, control);
        return;
      case "error":
        this.handleServerError(control);
        return;
      default:
        this.failTerminal("CMCLOUD_CONTROL_FRAME_INVALID");
    }
  }

  private handleServerError(control: Record<string, unknown>): void {
    if (typeof control.code !== "string" || !stableErrorCode(control.code)) {
      this.failTerminal("CMCLOUD_SERVER_ERROR_INVALID");
      return;
    }
    if (TERMINAL_SERVER_ERROR_CODES.has(control.code)) {
      this.failTerminal(control.code);
      return;
    }
    this.recordConnectionError(control.code);
    this.closeCurrentSocket(4409, control.code);
  }

  private acceptServerHello(
    socket: CmCloudSocket,
    control: Record<string, unknown>,
  ): void {
    if (
      control.protocolVersion !== CMCLOUD_AGENT_PROTOCOL_VERSION ||
      !validPositiveInteger(control.connectionEpoch) ||
      !validNonNegativeInteger(control.installationGeneration) ||
      !validPositiveInteger(control.credentialVersion) ||
      !validHeartbeatInterval(control.heartbeatIntervalMs) ||
      typeof control.minimumClientVersion !== "string" ||
      control.minimumClientVersion.length === 0 ||
      control.minimumClientVersion.length > 512 ||
      !validAprsMode(control.aprsMode)
    ) {
      this.failTerminal("CMCLOUD_SERVER_HELLO_INVALID");
      return;
    }
    if (
      control.installationGeneration !== this.options.installationGeneration ||
      control.credentialVersion !== this.options.credentialVersion
    ) {
      this.failTerminal("CMCLOUD_SESSION_FENCE_MISMATCH");
      return;
    }
    if (control.enrollmentAckRequired || control.issuedDeviceCredential) {
      // Pairing requires Agent-owned durable secret promotion; never keep the
      // issued device credential only in this Node process.
      this.failTerminal("CMCLOUD_ENROLLMENT_REQUIRES_AGENT");
      return;
    }
    let directAprs: CmCloudDirectAprsCapability | undefined;
    try {
      if (control.directAprs !== undefined) {
        directAprs = parseCmCloudDirectAprsCapability(control.directAprs);
      }
    } catch {
      this.failTerminal("CMCLOUD_SERVER_HELLO_INVALID");
      return;
    }
    this.session = {
      connectionEpoch: control.connectionEpoch,
      installationGeneration: control.installationGeneration,
      credentialVersion: control.credentialVersion,
      heartbeatIntervalMs: control.heartbeatIntervalMs,
      aprsMode: control.aprsMode,
      ...(directAprs ? { directAprs } : {}),
    };
    this.reconnectAttempt = 0;
    this.state = "ready";
    if (this.handshakeTimer) {
      clearTimeout(this.handshakeTimer);
      this.handshakeTimer = undefined;
    }
    this.heartbeatTimer = setInterval(() => {
      const session = this.session;
      if (!session || this.socket !== socket) return;
      void this.sendHeartbeat(socket).catch(() => {
        this.recordConnectionError("CMCLOUD_HEARTBEAT_SEND_FAILED");
        this.closeCurrentSocket(1011, "heartbeat send failed");
      });
    }, this.session.heartbeatIntervalMs);
    this.heartbeatTimer.unref();
    this.publish("cmcloud.connection.ready", {
      connectionEpoch: this.session.connectionEpoch,
      pendingOutbox: this.options.outbox.pendingCount(),
    });
    void this.sendHeartbeat(socket).catch(() => {
      this.recordConnectionError("CMCLOUD_HEARTBEAT_SEND_FAILED");
      this.closeCurrentSocket(1011, "heartbeat send failed");
    });
    void this.configureDirectAprs(socket, this.session);
    void this.flush();
  }

  private async configureDirectAprs(
    socket: CmCloudSocket,
    session: CmCloudSession,
  ): Promise<void> {
    const egress = this.options.directAprsEgress;
    const capability =
      session.aprsMode === "enabled" ? session.directAprs : undefined;

    const generation = ++this.directAprsConfigurationGeneration;
    let egressConfiguration: Promise<void> | undefined;
    try {
      // `configure()` synchronously revokes the previous egress identity,
      // then may wait for a TCP/APRS-IS login. Do not hold the station
      // scheduler behind that network operation: it must remain running and
      // wait for the readiness callback so reconnects cannot leave it stopped.
      egressConfiguration = egress?.configure(capability);
    } catch {
      this.recordConnectionError("CMCLOUD_DIRECT_APRS_CONFIGURATION_FAILED");
    }
    try {
      if (!this.isCurrentDirectAprsConfiguration(generation, socket, session)) {
        return;
      }
      await this.options.directAprsIgate?.configure(capability);
    } catch {
      this.recordConnectionError("CMCLOUD_DIRECT_APRS_CONFIGURATION_FAILED");
    }
    if (egressConfiguration) {
      void egressConfiguration.catch(() => {
        if (
          this.isCurrentDirectAprsConfiguration(generation, socket, session)
        ) {
          this.recordConnectionError(
            "CMCLOUD_DIRECT_APRS_CONFIGURATION_FAILED",
          );
        }
      });
    }
    if (this.isCurrentDirectAprsConfiguration(generation, socket, session)) {
      void this.sendHeartbeat(socket).catch(() => {
        this.recordConnectionError("CMCLOUD_HEARTBEAT_SEND_FAILED");
        this.closeCurrentSocket(1011, "heartbeat send failed");
      });
    }
  }

  private isCurrentDirectAprsConfiguration(
    generation: number,
    socket: CmCloudSocket,
    session: CmCloudSession,
  ): boolean {
    return (
      this.directAprsConfigurationGeneration === generation &&
      this.socket === socket &&
      this.session === session &&
      !this.terminalCode
    );
  }

  private onDirectAprsReadinessChanged(): void {
    const socket = this.socket;
    if (!socket || !this.session) return;
    void this.sendHeartbeat(socket).catch(() => {
      this.recordConnectionError("CMCLOUD_HEARTBEAT_SEND_FAILED");
      this.closeCurrentSocket(1011, "heartbeat send failed");
    });
  }

  private sendHeartbeat(socket: CmCloudSocket): Promise<void> {
    const session = this.session;
    if (!session || this.socket !== socket) return Promise.resolve();
    return this.sendControl(socket, {
      type: "client_heartbeat",
      connectionEpoch: session.connectionEpoch,
      installationGeneration: session.installationGeneration,
      credentialVersion: session.credentialVersion,
      directAprsReady: this.directAprsReady(session),
    });
  }

  private directAprsReady(session: CmCloudSession): boolean {
    return Boolean(
      session.aprsMode === "enabled" &&
      session.directAprs &&
      this.options.directAprsEgress?.ready(),
    );
  }

  private async acceptAprsDispatch(
    socket: CmCloudSocket,
    control: Record<string, unknown>,
  ): Promise<void> {
    if (!isUuid(control.dispatchId) || typeof control.data !== "string") {
      this.failTerminal("CMCLOUD_APRS_DISPATCH_INVALID");
      return;
    }
    const dispatchId = control.dispatchId;
    const cached = this.completedAprsDispatches.get(dispatchId);
    if (cached) {
      await this.sendAprsDispatchAck(socket, cached);
      return;
    }
    if (this.pendingAprsDispatchId) {
      if (this.pendingAprsDispatchId === dispatchId) return;
      await this.sendAprsDispatchAck(socket, {
        type: "aprs_dispatch_ack",
        dispatchId,
        outcome: "retryable_failure",
        errorCode: "CMCLOUD_APRS_DISPATCH_BUSY",
      });
      return;
    }
    const session = this.session;
    const egress = this.options.directAprsEgress;
    let result: CmCloudDirectAprsDispatchResult;
    if (!session || !egress || !this.directAprsReady(session)) {
      result = {
        outcome: "retryable_failure",
        errorCode: "CMCLOUD_DIRECT_APRS_NOT_READY",
      };
    } else {
      this.pendingAprsDispatchId = dispatchId;
      const recordTrackerForward =
        this.options.directAprsIgate?.captureTrackerForwardRecorder();
      try {
        result = await egress.submit(control.data);
        if (result.outcome === "submitted") {
          // The central dispatch is a Tracker forward. Count it at the same
          // successful socket-write boundary used by the legacy APRS outbox,
          // against the station family that owned this dispatch at submission.
          recordTrackerForward?.(this.clock().getTime());
        }
      } catch {
        // A throw cannot prove the write remained before the APRS socket.
        result = {
          outcome: "uncertain",
          errorCode: "CMCLOUD_DIRECT_APRS_WRITE_UNCERTAIN",
        };
      } finally {
        this.pendingAprsDispatchId = undefined;
      }
    }
    const acknowledgement = toAprsDispatchAck(dispatchId, result);
    // CMCloud owns retry scheduling. A retryable result proves no APRS write
    // occurred, so the same dispatch ID must remain eligible for a later send.
    if (acknowledgement.outcome !== "retryable_failure") {
      this.cacheAprsDispatchAck(acknowledgement);
    }
    if (this.socket !== socket || this.session !== session) {
      return;
    }
    await this.sendAprsDispatchAck(socket, acknowledgement);
  }

  private async sendAprsDispatchAck(
    socket: CmCloudSocket,
    acknowledgement: CmCloudAprsDispatchAck,
  ): Promise<void> {
    try {
      await this.sendControl(socket, acknowledgement);
    } catch {
      this.recordConnectionError("CMCLOUD_APRS_DISPATCH_ACK_SEND_FAILED");
      this.closeCurrentSocket(
        1011,
        "APRS dispatch acknowledgement send failed",
      );
    }
  }

  private cacheAprsDispatchAck(acknowledgement: CmCloudAprsDispatchAck): void {
    this.completedAprsDispatches.set(
      acknowledgement.dispatchId,
      acknowledgement,
    );
    while (this.completedAprsDispatches.size > MAX_CACHED_APRS_DISPATCH_ACKS) {
      const oldest = this.completedAprsDispatches.keys().next().value;
      if (typeof oldest !== "string") return;
      this.completedAprsDispatches.delete(oldest);
    }
  }

  private acceptRawAcknowledgement(control: Record<string, unknown>): void {
    const inFlight = this.inFlight;
    if (
      !inFlight ||
      !isUuid(control.messageId) ||
      control.messageId !== inFlight.messageId ||
      control.lane !== inFlight.lane ||
      control.laneSequence !== inFlight.laneSequence ||
      !isUuid(control.receiptId) ||
      (control.disposition !== "received" &&
        control.disposition !== "duplicate" &&
        control.disposition !== "dropped_mqtt")
    ) {
      this.failTerminal("CMCLOUD_RAW_ACK_INVALID");
      return;
    }
    try {
      const entry = this.options.outbox.acknowledge({
        messageId: inFlight.messageId,
        lane: inFlight.lane,
        laneSequence: inFlight.laneSequence,
        receiptId: control.receiptId,
        acknowledgedAt: this.now(),
      });
      this.inFlight = undefined;
      if (this.ackTimer) {
        clearTimeout(this.ackTimer);
        this.ackTimer = undefined;
      }
      this.publish("cmcloud.raw.acknowledged", {
        messageId: entry.messageId,
        lane: entry.lane,
        laneSequence: entry.laneSequence,
        disposition: control.disposition,
        pendingOutbox: this.options.outbox.pendingCount(),
      });
      void this.flush();
    } catch (error) {
      this.failTerminal(
        error instanceof CmCloudOutboxError
          ? error.code
          : "CMCLOUD_OUTBOX_ACK_INVALID",
      );
    }
  }

  private async flush(): Promise<void> {
    this.flushRequested = true;
    if (this.flushPromise) return this.flushPromise;
    const work = this.drainFlush().finally(() => {
      if (this.flushPromise === work) this.flushPromise = undefined;
    });
    this.flushPromise = work;
    return work;
  }

  private async drainFlush(): Promise<void> {
    while (this.flushRequested) {
      this.flushRequested = false;
      await this.flushInternal();
    }
  }

  private async flushInternal(): Promise<void> {
    const socket = this.socket;
    const session = this.session;
    if (
      !this.running ||
      this.terminalCode ||
      !socket ||
      !session ||
      this.inFlight
    ) {
      return;
    }
    const entry = this.options.outbox.nextPending();
    if (!entry) return;
    this.inFlight = entry;
    try {
      this.options.outbox.recordAttempt(entry.messageId, this.now());
      const frame = encodeCmCloudRawFrame(
        {
          messageId: entry.messageId,
          lane: entry.lane,
          laneSequence: entry.laneSequence,
          capturedAt: entry.capturedAt,
          connectionEpoch: session.connectionEpoch,
          installationGeneration: session.installationGeneration,
          credentialVersion: session.credentialVersion,
          wireKind: "meshtastic.FromRadio",
        },
        entry.body,
      );
      await sendSocket(socket, frame, true);
      if (
        this.inFlight?.messageId !== entry.messageId ||
        this.socket !== socket
      ) {
        return;
      }
      this.ackTimer = setTimeout(() => {
        if (this.inFlight?.messageId !== entry.messageId) return;
        this.recordEntryError(entry.messageId, "CMCLOUD_RAW_ACK_TIMEOUT");
        this.closeCurrentSocket(4008, "raw acknowledgement timeout");
      }, this.ackTimeoutMs);
      this.ackTimer.unref();
    } catch (error) {
      if (this.inFlight?.messageId !== entry.messageId) return;
      this.recordEntryError(
        entry.messageId,
        error instanceof CmCloudOutboxError
          ? error.code
          : "CMCLOUD_RAW_SEND_FAILED",
      );
      this.closeCurrentSocket(1011, "raw send failed");
    }
  }

  private async sendControl(
    socket: CmCloudSocket,
    payload: object,
  ): Promise<void> {
    await sendSocket(socket, JSON.stringify(payload), false);
  }

  private closeCurrentSocket(code: number, reason: string): void {
    const socket = this.socket;
    if (!socket) return;
    // A WebSocket close handshake is asynchronous. Revoke the session before
    // requesting it so an already-buffered control frame cannot reach APRS.
    this.detachSocket(socket);
    try {
      socket.close(code, reason);
    } catch {
      // `close` is advisory. The reconnect path remains driven by close/error.
    }
  }

  private failTerminal(code: string): void {
    this.terminalCode = validateStableErrorCode(code);
    this.lastErrorCode = this.terminalCode;
    if (this.inFlight) {
      this.recordEntryError(this.inFlight.messageId, this.terminalCode);
    }
    this.publish("cmcloud.connection.blocked", { code: this.terminalCode });
    this.clearTimers();
    this.closeCurrentSocket(4409, this.terminalCode);
    if (!this.socket) this.state = "blocked";
  }

  private detachSocket(socket: CmCloudSocket): void {
    if (this.socket !== socket) return;
    this.directAprsConfigurationGeneration += 1;
    this.socket = undefined;
    this.session = undefined;
    this.clearSessionTimers();
    this.inFlight = undefined;
    this.options.directAprsIgate?.suspend();
    try {
      void this.options.directAprsEgress?.configure(undefined).catch(() => {
        // The socket is already fenced. A later CMCloud hello will establish
        // a fresh egress configuration.
      });
    } catch {
      // An egress implementation must not prevent the CMCloud reconnect.
    }
    if (this.running && !this.terminalCode) {
      this.scheduleReconnect();
    } else if (this.terminalCode) {
      this.state = "blocked";
    } else {
      this.state = "stopped";
    }
  }

  private recordConnectionError(code: string): void {
    this.lastErrorCode = validateStableErrorCode(code);
    this.publish("cmcloud.connection.error", { code: this.lastErrorCode });
  }

  private recordEntryError(messageId: string, code: string): void {
    try {
      this.options.outbox.recordError(messageId, code, this.now());
    } catch {
      // A corrupt outbox must stop delivery, but this helper is also called
      // while entering a terminal state. Do not recurse through failTerminal.
      this.terminalCode ??= "CMCLOUD_OUTBOX_STATE_INVALID";
      this.lastErrorCode = "CMCLOUD_OUTBOX_STATE_INVALID";
    }
  }

  private scheduleReconnect(): void {
    if (!this.running || this.terminalCode || this.reconnectTimer) return;
    this.reconnectAttempt += 1;
    const exponent = Math.min(this.reconnectAttempt - 1, 16);
    const delay = Math.min(
      this.reconnectMaximumDelayMs,
      this.reconnectInitialDelayMs * 2 ** exponent,
    );
    this.state = "connecting";
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.openSocket();
    }, delay);
    this.reconnectTimer.unref();
    this.publish("cmcloud.connection.reconnecting", {
      attempt: this.reconnectAttempt,
      delayMs: delay,
    });
  }

  private clearSessionTimers(): void {
    if (this.handshakeTimer) {
      clearTimeout(this.handshakeTimer);
      this.handshakeTimer = undefined;
    }
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
    if (this.ackTimer) {
      clearTimeout(this.ackTimer);
      this.ackTimer = undefined;
    }
  }

  private clearTimers(): void {
    this.clearSessionTimers();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
  }

  private isActiveSocket(generation: number, socket: CmCloudSocket): boolean {
    return (
      this.running &&
      generation === this.socketGeneration &&
      this.socket === socket
    );
  }

  private now(): string {
    return this.clock().toISOString();
  }

  private publish(type: string, payload: Record<string, unknown>): void {
    this.options.events?.publish({ type, source: "gateway", payload });
  }
}

export interface CmCloudRawFrameHeader {
  readonly messageId: string;
  readonly lane: CmCloudLane;
  readonly laneSequence: number;
  readonly capturedAt: string;
  readonly connectionEpoch: number;
  readonly installationGeneration: number;
  readonly credentialVersion: number;
  readonly wireKind: "meshtastic.FromRadio";
}

export function encodeCmCloudRawFrame(
  header: CmCloudRawFrameHeader,
  body: Uint8Array,
): Buffer {
  validateUuid(header.messageId);
  if (
    header.lane !== "live" ||
    !validPositiveInteger(header.laneSequence) ||
    !validTimestamp(header.capturedAt) ||
    !validPositiveInteger(header.connectionEpoch) ||
    !validNonNegativeInteger(header.installationGeneration) ||
    !validPositiveInteger(header.credentialVersion) ||
    header.wireKind !== "meshtastic.FromRadio"
  ) {
    throw new CmCloudOutboxError("CMCLOUD_RAW_FRAME_INVALID");
  }
  const exactBody = validateRawBody(body);
  const headerBytes = Buffer.from(JSON.stringify(header), "utf8");
  if (headerBytes.length > 65_535) {
    throw new CmCloudOutboxError("CMCLOUD_RAW_FRAME_INVALID");
  }
  const prefix = Buffer.allocUnsafe(7);
  CMCLOUD_RAW_FRAME_MAGIC.copy(prefix, 0);
  prefix[4] = CMCLOUD_RAW_FRAME_TYPE;
  prefix.writeUInt16BE(headerBytes.length, 5);
  return Buffer.concat([prefix, headerBytes, exactBody]);
}

function defaultCmCloudSocketFactory(
  url: string,
  protocols: readonly string[],
  options: CmCloudSocketFactoryOptions,
): CmCloudSocket {
  return new WebSocket(
    url,
    [...protocols],
    options,
  ) as unknown as CmCloudSocket;
}

function sendSocket(
  socket: CmCloudSocket,
  data: string | Uint8Array,
  binary: boolean,
): Promise<void> {
  return new Promise((resolve, reject) => {
    try {
      socket.send(data, { binary }, (error) => {
        if (error) reject(error);
        else resolve();
      });
    } catch (error) {
      reject(error);
    }
  });
}

function parseControlMessage(
  input: unknown,
): Record<string, unknown> | undefined {
  let bytes: Buffer;
  try {
    if (typeof input === "string") {
      bytes = Buffer.from(input, "utf8");
    } else if (Buffer.isBuffer(input) || input instanceof Uint8Array) {
      bytes = Buffer.from(input);
    } else if (input instanceof ArrayBuffer) {
      bytes = Buffer.from(input);
    } else if (
      Array.isArray(input) &&
      input.every((item) => Buffer.isBuffer(item))
    ) {
      bytes = Buffer.concat(input);
    } else {
      return undefined;
    }
    if (bytes.length === 0 || bytes.length > MAX_CONTROL_MESSAGE_BYTES)
      return undefined;
    const value: unknown = JSON.parse(bytes.toString("utf8"));
    return value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : undefined;
  } catch {
    return undefined;
  }
}

function toCmCloudOutboxEntry(
  row: Record<string, unknown>,
): CmCloudRawOutboxEntry {
  const messageId = validateUuid(String(row.message_id));
  const lane = String(row.lane);
  const laneSequence = Number(row.lane_sequence);
  const capturedAt = canonicalTimestamp(String(row.captured_at));
  const body = validateRawBody(asRawBody(row.body));
  const bodySha256 = String(row.body_sha256);
  const attempts = Number(row.attempts);
  const createdAt = canonicalTimestamp(String(row.created_at));
  const updatedAt = canonicalTimestamp(String(row.updated_at));
  const lastAttemptAt = nullableTimestamp(row.last_attempt_at);
  const lastErrorCode = nullableStableErrorCode(row.last_error_code);
  const receiptId = nullableUuid(row.receipt_id);
  const acknowledgedAt = nullableTimestamp(row.acknowledged_at);
  const status = String(row.status);
  if (
    lane !== "live" ||
    !validPositiveInteger(laneSequence) ||
    !/^[a-f0-9]{64}$/.test(bodySha256) ||
    createHash("sha256").update(body).digest("hex") !== bodySha256 ||
    !Number.isSafeInteger(attempts) ||
    attempts < 0 ||
    (status !== "pending" && status !== "acknowledged") ||
    (status === "acknowledged" && (!receiptId || !acknowledgedAt)) ||
    (status === "pending" && (receiptId || acknowledgedAt))
  ) {
    throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STATE_INVALID");
  }
  return {
    messageId,
    lane: "live",
    laneSequence,
    capturedAt,
    body: new Uint8Array(body),
    bodySha256,
    attempts,
    createdAt,
    updatedAt,
    ...(lastAttemptAt ? { lastAttemptAt } : {}),
    ...(lastErrorCode ? { lastErrorCode } : {}),
    ...(receiptId ? { receiptId } : {}),
    ...(acknowledgedAt ? { acknowledgedAt } : {}),
  };
}

function asRawBody(value: unknown): Uint8Array {
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return new Uint8Array(value);
  }
  throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STATE_INVALID");
}

function validateRawBody(value: Uint8Array): Uint8Array {
  if (
    !(value instanceof Uint8Array) ||
    value.length === 0 ||
    value.length > CMCLOUD_RAW_FRAME_MAX_BYTES
  ) {
    throw new CmCloudOutboxError("CMCLOUD_RAW_FRAME_INVALID");
  }
  return new Uint8Array(value);
}

function validateCredential(value: string): void {
  if (!/^[A-Za-z0-9_-]{16,512}$/.test(value)) {
    throw new CmCloudConfigurationError(
      "CMCLOUD_CREDENTIAL_CONFIGURATION_INVALID",
    );
  }
}

function validateClientVersion(value: string): void {
  if (
    !value ||
    value.length > 512 ||
    value.includes("\r") ||
    value.includes("\n") ||
    value.includes("\u0000")
  ) {
    throw new CmCloudConfigurationError("CMCLOUD_CLIENT_VERSION_INVALID");
  }
}

function validateUuid(
  value: string,
  code = "CMCLOUD_OUTBOX_STATE_INVALID",
): string {
  if (!isUuid(value)) {
    throw new CmCloudConfigurationError(code);
  }
  return value;
}

function isUuid(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      value,
    )
  );
}

function canonicalTimestamp(value: string): string {
  if (!validTimestamp(value)) {
    throw new CmCloudOutboxError("CMCLOUD_OUTBOX_TIMESTAMP_INVALID");
  }
  return new Date(value).toISOString();
}

function validTimestamp(value: unknown): value is string {
  if (
    typeof value !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(value)
  ) {
    return false;
  }
  return Number.isFinite(Date.parse(value));
}

function nullableTimestamp(value: unknown): string | undefined {
  return value === null || value === undefined
    ? undefined
    : canonicalTimestamp(String(value));
}

function nullableUuid(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  return validateUuid(String(value));
}

function nullableStableErrorCode(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  return validateStableErrorCode(String(value));
}

function validateStableErrorCode(value: string): string {
  if (!stableErrorCode(value)) {
    throw new CmCloudOutboxError("CMCLOUD_OUTBOX_STATE_INVALID");
  }
  return value;
}

function stableErrorCode(value: string): boolean {
  return /^[A-Z][A-Z0-9_]{0,127}$/.test(value);
}

function validAprsMode(
  value: unknown,
): value is "disabled" | "shadow" | "enabled" {
  return value === "disabled" || value === "shadow" || value === "enabled";
}

function toAprsDispatchAck(
  dispatchId: string,
  result: CmCloudDirectAprsDispatchResult,
): CmCloudAprsDispatchAck {
  if (
    (result.outcome !== "submitted" &&
      result.outcome !== "retryable_failure" &&
      result.outcome !== "uncertain") ||
    (result.errorCode !== undefined && !stableErrorCode(result.errorCode))
  ) {
    return {
      type: "aprs_dispatch_ack",
      dispatchId,
      outcome: "uncertain",
      errorCode: "CMCLOUD_DIRECT_APRS_WRITE_UNCERTAIN",
    };
  }
  return {
    type: "aprs_dispatch_ack",
    dispatchId,
    outcome: result.outcome,
    ...(result.errorCode ? { errorCode: result.errorCode } : {}),
  };
}

function validPositiveInteger(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 1 &&
    value <= MAX_SAFE_SEQUENCE
  );
}

function validNonNegativeInteger(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 0 &&
    value <= MAX_SAFE_SEQUENCE
  );
}

function parsePositiveInteger(value: string | undefined, code: string): number {
  const source = value?.trim();
  if (!source || !/^\d+$/.test(source)) {
    throw new CmCloudConfigurationError(code);
  }
  const parsed = Number(source);
  if (!validPositiveInteger(parsed)) throw new CmCloudConfigurationError(code);
  return parsed;
}

function parseNonNegativeInteger(
  value: string | undefined,
  code: string,
): number {
  const source = value?.trim();
  if (source === undefined || !/^\d+$/.test(source)) {
    throw new CmCloudConfigurationError(code);
  }
  const parsed = Number(source);
  if (!validNonNegativeInteger(parsed))
    throw new CmCloudConfigurationError(code);
  return parsed;
}

function validHeartbeatInterval(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 1_000 &&
    value <= 5 * 60_000
  );
}

function boundedDelay(value: number, code: string): number {
  if (!Number.isInteger(value) || value < 10 || value > 10 * 60_000) {
    throw new CmCloudConfigurationError(code);
  }
  return value;
}

function rollbackQuietly(database: DatabaseSync): void {
  try {
    database.exec("ROLLBACK");
  } catch {
    // A failed BEGIN has no transaction to roll back.
  }
}
