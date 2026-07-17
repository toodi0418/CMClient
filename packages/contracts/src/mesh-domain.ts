import { Type, type Static } from "@sinclair/typebox";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";
const NodeNumberSchema = Type.Integer({ minimum: 0, maximum: 4_294_967_295 });
const MetricValueSchema = Type.Union([
  Type.String({ maxLength: 512 }),
  Type.Number(),
  Type.Boolean(),
]);

export const MeshNodeSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    meshNetworkId: Type.String({ minLength: 1, maxLength: 128 }),
    nodeNum: NodeNumberSchema,
    firstSeenAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    lastSeenAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    lastObservationId: Type.String({ minLength: 1, maxLength: 128 }),
    userId: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    longName: Type.Optional(Type.String({ minLength: 1, maxLength: 256 })),
    shortName: Type.Optional(Type.String({ minLength: 1, maxLength: 64 })),
    hardwareModel: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    role: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
  },
  { additionalProperties: false },
);

export const MeshMessageSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    id: Type.String({ minLength: 1, maxLength: 128 }),
    observationId: Type.String({ minLength: 1, maxLength: 128 }),
    meshNetworkId: Type.String({ minLength: 1, maxLength: 128 }),
    sender: NodeNumberSchema,
    destination: Type.Optional(NodeNumberSchema),
    packetId: Type.Optional(NodeNumberSchema),
    channel: Type.Optional(Type.Integer({ minimum: 0, maximum: 255 })),
    text: Type.String({ minLength: 1, maxLength: 512 }),
    observedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
  },
  { additionalProperties: false },
);

export const MeshTelemetrySchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    id: Type.String({ minLength: 1, maxLength: 128 }),
    observationId: Type.String({ minLength: 1, maxLength: 128 }),
    meshNetworkId: Type.String({ minLength: 1, maxLength: 128 }),
    nodeNum: NodeNumberSchema,
    packetId: Type.Optional(NodeNumberSchema),
    metricKind: Type.String({ minLength: 1, maxLength: 64 }),
    metrics: Type.Record(
      Type.String({ minLength: 1, maxLength: 96 }),
      MetricValueSchema,
    ),
    observedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    telemetryTimeSeconds: Type.Optional(NodeNumberSchema),
  },
  { additionalProperties: false },
);

export const MeshNodeListSchema = Type.Object(
  { items: Type.Array(MeshNodeSchema, { maxItems: 200 }) },
  { additionalProperties: false },
);

export const MeshMessageListSchema = Type.Object(
  { items: Type.Array(MeshMessageSchema, { maxItems: 200 }) },
  { additionalProperties: false },
);

export const MeshTelemetryListSchema = Type.Object(
  { items: Type.Array(MeshTelemetrySchema, { maxItems: 200 }) },
  { additionalProperties: false },
);

export type MeshNode = Static<typeof MeshNodeSchema>;
export type MeshMessage = Static<typeof MeshMessageSchema>;
export type MeshTelemetry = Static<typeof MeshTelemetrySchema>;
export type MeshNodeList = Static<typeof MeshNodeListSchema>;
export type MeshMessageList = Static<typeof MeshMessageListSchema>;
export type MeshTelemetryList = Static<typeof MeshTelemetryListSchema>;
