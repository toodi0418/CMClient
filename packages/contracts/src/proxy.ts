import { Type, type Static } from "@sinclair/typebox";

import {
  TransportConnectionStateSchema,
  TransportMetricsSchema,
} from "./transport.js";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";

const ProxyModeSchema = Type.Union([
  Type.Literal("monitor"),
  Type.Literal("message"),
  Type.Literal("full"),
]);
const ProxyStateSchema = Type.Union([
  Type.Literal("stopped"),
  Type.Literal("starting"),
  Type.Literal("running"),
  Type.Literal("degraded"),
]);
const ProxyAuditActionSchema = Type.Union([
  Type.Literal("client_admitted"),
  Type.Literal("client_released"),
  Type.Literal("client_rejected"),
  Type.Literal("write_allowed"),
  Type.Literal("write_rejected"),
]);

export const ProxyAuditEntrySchema = Type.Object(
  {
    action: ProxyAuditActionSchema,
    clientFingerprint: Type.String({ pattern: "^[a-f0-9]{16}$" }),
    mode: ProxyModeSchema,
    occurredAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    addressFingerprint: Type.Optional(
      Type.String({ pattern: "^[a-f0-9]{16}$" }),
    ),
    code: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    variant: Type.Optional(Type.String({ minLength: 1, maxLength: 64 })),
  },
  { additionalProperties: false },
);

export const ProxyStatusSchema = Type.Object(
  {
    state: ProxyStateSchema,
    listener: Type.Object(
      {
        host: Type.String({ minLength: 1, maxLength: 255 }),
        port: Type.Integer({ minimum: 0, maximum: 65_535 }),
      },
      { additionalProperties: false },
    ),
    policy: Type.Object(
      {
        activeClients: Type.Integer({ minimum: 0, maximum: 256 }),
        allowLan: Type.Boolean(),
        allowedAddressCount: Type.Integer({ minimum: 0, maximum: 4_096 }),
        maxClients: Type.Integer({ minimum: 1, maximum: 256 }),
        maxWritesPerMinute: Type.Integer({ minimum: 1, maximum: 3_600 }),
        mode: ProxyModeSchema,
      },
      { additionalProperties: false },
    ),
    queue: Type.Object(
      {
        broadcastAccepted: Type.Integer({ minimum: 0 }),
        broadcastDropped: Type.Integer({ minimum: 0 }),
        broadcastFrames: Type.Integer({ minimum: 0 }),
        directAccepted: Type.Integer({ minimum: 0 }),
        directDropped: Type.Integer({ minimum: 0 }),
        pendingCorrelations: Type.Integer({ minimum: 0 }),
        queuedWrites: Type.Integer({ minimum: 0 }),
        writing: Type.Boolean(),
      },
      { additionalProperties: false },
    ),
    recentAudit: Type.Array(ProxyAuditEntrySchema, { maxItems: 50 }),
    upstream: Type.Object(
      {
        configFrameCount: Type.Integer({ minimum: 0 }),
        metrics: TransportMetricsSchema,
        state: TransportConnectionStateSchema,
        lastErrorCode: Type.Optional(
          Type.String({ minLength: 1, maxLength: 128 }),
        ),
      },
      { additionalProperties: false },
    ),
    lastErrorCode: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
  },
  { additionalProperties: false },
);

export type ProxyAuditEntry = Static<typeof ProxyAuditEntrySchema>;
export type ProxyStatus = Static<typeof ProxyStatusSchema>;
