import { Type, type Static } from "@sinclair/typebox";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";

export const TRANSPORT_KINDS = ["tcp", "serial", "simulator"] as const;
export const CONNECTION_STATUSES = [
  "disconnected",
  "connecting",
  "configuring",
  "ready",
  "degraded",
  "backoff",
] as const;

export const TransportKindSchema = Type.Union(
  TRANSPORT_KINDS.map((kind) => Type.Literal(kind)),
);
export const ConnectionStatusSchema = Type.Union(
  CONNECTION_STATUSES.map((status) => Type.Literal(status)),
);
export const TransportConnectionStateSchema = Type.Object(
  {
    transport: TransportKindSchema,
    status: ConnectionStatusSchema,
    changedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    attempt: Type.Optional(Type.Integer({ minimum: 1 })),
    reasonCode: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
  },
  { additionalProperties: false },
);
export const TransportMetricsSchema = Type.Object(
  {
    bytesReceived: Type.Integer({ minimum: 0 }),
    bytesSent: Type.Integer({ minimum: 0 }),
    framesReceived: Type.Integer({ minimum: 0 }),
    framesSent: Type.Integer({ minimum: 0 }),
    malformedFrames: Type.Integer({ minimum: 0 }),
    reconnects: Type.Integer({ minimum: 0 }),
  },
  { additionalProperties: false },
);

export type TransportKind = (typeof TRANSPORT_KINDS)[number];
export type ConnectionStatus = (typeof CONNECTION_STATUSES)[number];
export type TransportConnectionState = Static<
  typeof TransportConnectionStateSchema
>;
export type TransportMetrics = Static<typeof TransportMetricsSchema>;
