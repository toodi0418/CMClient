import { createHash } from "node:crypto";
import { isIP } from "node:net";

import type { MeshtasticSchema } from "../protobuf/schema.js";
import { createTraceId, type StructuredLogger } from "../observability.js";
import type { ProxyOutboundAuthorizer } from "./outbound.js";

const TO_OBJECT_OPTIONS = {
  bytes: Uint8Array,
  defaults: false,
  enums: String,
  longs: Number,
  oneofs: true,
};

const CLIENT_ID_PATTERN = /^[a-zA-Z0-9._:-]{1,128}$/;
const PROXY_MODES = ["monitor", "message", "full"] as const;

export type ProxyMode = (typeof PROXY_MODES)[number];

export interface ProxyClientIdentity {
  address: string;
  id: string;
}

export interface ProxyAuditEntry {
  action:
    | "client_admitted"
    | "client_released"
    | "client_rejected"
    | "write_allowed"
    | "write_rejected";
  addressFingerprint?: string;
  clientFingerprint: string;
  code?: string;
  mode: ProxyMode;
  occurredAt: string;
  variant?: string;
}

export interface ProxyPolicySnapshot {
  activeClientIds: string[];
  allowedAddressCount: number;
  allowLan: boolean;
  bindHost: string;
  maxClients: number;
  maxWritesPerMinute: number;
  mode: ProxyMode;
}

export interface ProxyAccessControllerOptions {
  allowLan?: boolean;
  allowlist?: string[];
  auditCapacity?: number;
  bindHost?: string;
  clock?: () => Date;
  logger?: StructuredLogger;
  maxClients?: number;
  maxWritesPerMinute?: number;
  mode?: ProxyMode;
  traceIdFactory?: () => string;
}

export class ProxyPolicyError extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

/**
 * Security authority for proxy client admission and outbound commands. It
 * records one-way client and remote-address fingerprints, never frame content.
 */
export class ProxyAccessController implements ProxyOutboundAuthorizer {
  private readonly allowLan: boolean;
  private readonly allowlist: Set<string>;
  private readonly auditCapacity: number;
  private readonly bindHost: string;
  private readonly clock: () => Date;
  private readonly clients = new Map<string, ActiveClient>();
  private readonly logger: StructuredLogger | undefined;
  private readonly maxClients: number;
  private readonly maxWritesPerMinute: number;
  private readonly mode: ProxyMode;
  private readonly traceIdFactory: () => string;
  private readonly auditEntries: ProxyAuditEntry[] = [];

  constructor(
    private readonly schema: MeshtasticSchema,
    options: ProxyAccessControllerOptions = {},
  ) {
    this.bindHost = normalizeBindHost(options.bindHost ?? "127.0.0.1");
    this.allowLan = options.allowLan ?? false;
    this.allowlist = new Set(
      (options.allowlist ?? []).map((address) => normalizeAddress(address)),
    );
    this.auditCapacity = options.auditCapacity ?? 512;
    this.clock = options.clock ?? (() => new Date());
    this.logger = options.logger;
    this.maxClients = options.maxClients ?? 16;
    this.maxWritesPerMinute = options.maxWritesPerMinute ?? 120;
    this.mode = options.mode ?? "monitor";
    this.traceIdFactory = options.traceIdFactory ?? createTraceId;

    if (
      !Number.isInteger(this.auditCapacity) ||
      this.auditCapacity < 1 ||
      this.auditCapacity > 4_096 ||
      !Number.isInteger(this.maxClients) ||
      this.maxClients < 1 ||
      this.maxClients > 256 ||
      !Number.isInteger(this.maxWritesPerMinute) ||
      this.maxWritesPerMinute < 1 ||
      this.maxWritesPerMinute > 3_600 ||
      !PROXY_MODES.includes(this.mode)
    ) {
      throw new ProxyPolicyError("PROXY_POLICY_CONFIGURATION_INVALID");
    }
    if (!isLoopbackAddress(this.bindHost) && !this.allowLan) {
      throw new ProxyPolicyError("PROXY_LAN_BIND_FORBIDDEN");
    }
    if (this.allowLan && this.allowlist.size === 0) {
      throw new ProxyPolicyError("PROXY_LAN_ALLOWLIST_REQUIRED");
    }
    if (!this.allowLan && this.allowlist.size > 0) {
      throw new ProxyPolicyError("PROXY_ALLOWLIST_REQUIRES_LAN");
    }
  }

  get snapshot(): ProxyPolicySnapshot {
    return {
      activeClientIds: [...this.clients.keys()].sort((left, right) =>
        left.localeCompare(right),
      ),
      allowedAddressCount: this.allowlist.size,
      allowLan: this.allowLan,
      bindHost: this.bindHost,
      maxClients: this.maxClients,
      maxWritesPerMinute: this.maxWritesPerMinute,
      mode: this.mode,
    };
  }

  auditSnapshot(): ProxyAuditEntry[] {
    return this.auditEntries.map((entry) => ({ ...entry }));
  }

  admit(client: ProxyClientIdentity): void {
    const fingerprint = fingerprintAddress(client.address);
    if (!CLIENT_ID_PATTERN.test(client.id)) {
      this.record(
        "client_rejected",
        client.id,
        fingerprint,
        "PROXY_CLIENT_ID_INVALID",
      );
      throw new ProxyPolicyError("PROXY_CLIENT_ID_INVALID");
    }
    let address: string;
    try {
      address = normalizeAddress(client.address);
    } catch {
      this.record(
        "client_rejected",
        client.id,
        fingerprint,
        "PROXY_CLIENT_ADDRESS_INVALID",
      );
      throw new ProxyPolicyError("PROXY_CLIENT_ADDRESS_INVALID");
    }
    if (this.clients.has(client.id)) {
      this.record(
        "client_rejected",
        client.id,
        fingerprintAddress(address),
        "PROXY_CLIENT_DUPLICATE",
      );
      throw new ProxyPolicyError("PROXY_CLIENT_DUPLICATE");
    }
    if (this.clients.size >= this.maxClients) {
      this.record(
        "client_rejected",
        client.id,
        fingerprintAddress(address),
        "PROXY_CLIENT_LIMIT_REACHED",
      );
      throw new ProxyPolicyError("PROXY_CLIENT_LIMIT_REACHED");
    }
    if (!this.isAddressAllowed(address)) {
      this.record(
        "client_rejected",
        client.id,
        fingerprintAddress(address),
        "PROXY_CLIENT_ADDRESS_NOT_ALLOWED",
      );
      throw new ProxyPolicyError("PROXY_CLIENT_ADDRESS_NOT_ALLOWED");
    }
    this.clients.set(client.id, { address, writeTimes: [] });
    this.record("client_admitted", client.id, fingerprintAddress(address));
  }

  release(clientId: string, code = "PROXY_CLIENT_DISCONNECTED"): void {
    const client = this.clients.get(clientId);
    if (!client) {
      return;
    }
    this.clients.delete(clientId);
    this.record(
      "client_released",
      clientId,
      fingerprintAddress(client.address),
      code,
    );
  }

  authorizeOutbound(clientId: string, frame: Uint8Array): void {
    const client = this.clients.get(clientId);
    if (!client) {
      this.record(
        "write_rejected",
        clientId,
        undefined,
        "PROXY_CLIENT_UNKNOWN",
      );
      throw new ProxyPolicyError("PROXY_CLIENT_UNKNOWN");
    }
    const fingerprint = fingerprintAddress(client.address);
    const command = parseToRadio(this.schema, frame);
    if (this.mode === "monitor") {
      this.record(
        "write_rejected",
        clientId,
        fingerprint,
        "PROXY_MODE_MONITOR_READ_ONLY",
        command.variant,
      );
      throw new ProxyPolicyError("PROXY_MODE_MONITOR_READ_ONLY");
    }
    if (this.mode === "message" && !isTextMessage(command)) {
      this.record(
        "write_rejected",
        clientId,
        fingerprint,
        "PROXY_MODE_MESSAGE_WRITE_FORBIDDEN",
        command.variant,
      );
      throw new ProxyPolicyError("PROXY_MODE_MESSAGE_WRITE_FORBIDDEN");
    }
    const now = this.clock().getTime();
    const windowStart = now - 60_000;
    client.writeTimes = client.writeTimes.filter((time) => time > windowStart);
    if (client.writeTimes.length >= this.maxWritesPerMinute) {
      this.record(
        "write_rejected",
        clientId,
        fingerprint,
        "PROXY_WRITE_RATE_LIMITED",
        command.variant,
      );
      throw new ProxyPolicyError("PROXY_WRITE_RATE_LIMITED");
    }
    client.writeTimes.push(now);
    this.record(
      "write_allowed",
      clientId,
      fingerprint,
      undefined,
      command.variant,
    );
  }

  private isAddressAllowed(address: string): boolean {
    return this.allowLan
      ? this.allowlist.has(address)
      : isLoopbackAddress(address);
  }

  private record(
    action: ProxyAuditEntry["action"],
    clientId: string,
    addressFingerprint: string | undefined,
    code?: string,
    variant?: string,
  ): void {
    const entry: ProxyAuditEntry = {
      action,
      clientFingerprint: fingerprintClientId(clientId),
      mode: this.mode,
      occurredAt: this.clock().toISOString(),
      ...(addressFingerprint ? { addressFingerprint } : {}),
      ...(code ? { code } : {}),
      ...(variant ? { variant } : {}),
    };
    this.auditEntries.push(entry);
    if (this.auditEntries.length > this.auditCapacity) {
      this.auditEntries.splice(
        0,
        this.auditEntries.length - this.auditCapacity,
      );
    }
    this.logger?.log({
      level: action.endsWith("rejected") ? "warn" : "info",
      message: "proxy.audit",
      traceId: this.traceIdFactory(),
      fields: { ...entry },
    });
  }
}

interface ActiveClient {
  address: string;
  writeTimes: number[];
}

interface ToRadioCommand {
  decodedPortNum?: string;
  variant: string;
}

function parseToRadio(
  schema: MeshtasticSchema,
  frame: Uint8Array,
): ToRadioCommand {
  try {
    const source = asRecord(
      schema.toRadio.toObject(schema.toRadio.decode(frame), TO_OBJECT_OPTIONS),
    );
    if (!source || typeof source.payloadVariant !== "string") {
      throw new ProxyPolicyError("PROXY_POLICY_FRAME_INVALID");
    }
    const variant = source.payloadVariant;
    const packet = asRecord(source.packet);
    const decoded = asRecord(packet?.decoded);
    return {
      variant,
      ...(typeof decoded?.portnum === "string"
        ? { decodedPortNum: decoded.portnum }
        : {}),
    };
  } catch (error) {
    if (error instanceof ProxyPolicyError) {
      throw error;
    }
    throw new ProxyPolicyError("PROXY_POLICY_FRAME_INVALID");
  }
}

function isTextMessage(command: ToRadioCommand): boolean {
  return (
    command.variant === "packet" &&
    command.decodedPortNum === "TEXT_MESSAGE_APP"
  );
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function normalizeBindHost(value: string): string {
  const host = value.trim().toLowerCase();
  if (host === "localhost") {
    return host;
  }
  return normalizeAddress(host);
}

function normalizeAddress(value: string): string {
  const address = value.trim().toLowerCase();
  const mappedV4 = address.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
  const normalized = mappedV4?.[1] ?? address;
  if (!isIP(normalized)) {
    throw new ProxyPolicyError("PROXY_CLIENT_ADDRESS_INVALID");
  }
  return normalized;
}

function isLoopbackAddress(address: string): boolean {
  return (
    address === "localhost" ||
    address === "::1" ||
    /^127(?:\.\d{1,3}){3}$/.test(address)
  );
}

function fingerprintAddress(address: string): string | undefined {
  const value = address.trim();
  return value
    ? createHash("sha256").update(value).digest("hex").slice(0, 16)
    : undefined;
}

function fingerprintClientId(clientId: string): string {
  return createHash("sha256").update(clientId).digest("hex").slice(0, 16);
}
