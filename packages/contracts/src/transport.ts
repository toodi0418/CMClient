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
export const BACKLOG_CLASSIFICATIONS = ["backlog", "live", "unknown"] as const;

export const TransportKindSchema = Type.Union(
  TRANSPORT_KINDS.map((kind) => Type.Literal(kind)),
);
export const ConnectionStatusSchema = Type.Union(
  CONNECTION_STATUSES.map((status) => Type.Literal(status)),
);
export const BacklogClassificationSchema = Type.Union(
  BACKLOG_CLASSIFICATIONS.map((classification) => Type.Literal(classification)),
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
export const SerialDeviceSchema = Type.Object(
  {
    path: Type.String({ minLength: 1 }),
    manufacturer: Type.Optional(Type.String({ minLength: 1 })),
    serialNumber: Type.Optional(Type.String({ minLength: 1 })),
    vendorId: Type.Optional(Type.String({ minLength: 1 })),
    productId: Type.Optional(Type.String({ minLength: 1 })),
    friendlyName: Type.Optional(Type.String({ minLength: 1 })),
  },
  { additionalProperties: false },
);
export const NormalizedMeshPacketSchema = Type.Object(
  {
    sender: Type.Optional(Type.Integer({ minimum: 0 })),
    destination: Type.Optional(Type.Integer({ minimum: 0 })),
    packetId: Type.Optional(Type.Integer({ minimum: 0 })),
    channel: Type.Optional(Type.Integer({ minimum: 0 })),
    portNum: Type.Optional(Type.String({ minLength: 1 })),
    payloadBase64: Type.Optional(Type.String({ minLength: 1 })),
    encryptedPayloadBase64: Type.Optional(Type.String({ minLength: 1 })),
    deviceRxTimeSeconds: Type.Optional(
      Type.Integer({ minimum: 0, maximum: 4_294_967_295 }),
    ),
    rxSnr: Type.Optional(Type.Number()),
    rxRssi: Type.Optional(Type.Integer()),
    hopLimit: Type.Optional(Type.Integer({ minimum: 0 })),
    hopStart: Type.Optional(Type.Integer({ minimum: 0 })),
    viaMqtt: Type.Optional(Type.Boolean()),
    transportMechanism: Type.Optional(Type.String({ minLength: 1 })),
  },
  { additionalProperties: false },
);
export const NormalizedFromRadioSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    kind: Type.Union([
      Type.Literal("packet"),
      Type.Literal("config_complete"),
      Type.Literal("other"),
    ]),
    fromRadioId: Type.Optional(Type.Integer({ minimum: 0 })),
    configCompleteId: Type.Optional(Type.Integer({ minimum: 0 })),
    packet: Type.Optional(NormalizedMeshPacketSchema),
  },
  { additionalProperties: false },
);
export const MeshObservationSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    id: Type.String({ minLength: 1, maxLength: 128 }),
    transport: TransportKindSchema,
    sessionConnectedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    ingestedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    serverIngestedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    deviceRxTimeSeconds: Type.Optional(
      Type.Integer({ minimum: 0, maximum: 4_294_967_295 }),
    ),
    backlogClassification: BacklogClassificationSchema,
    normalizedFromRadio: NormalizedFromRadioSchema,
  },
  { additionalProperties: false },
);
export const MeshtasticRuntimeStatusSchema = Type.Object(
  {
    configured: Type.Boolean(),
    meshNetworkId: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    gatewayId: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    connection: Type.Optional(TransportConnectionStateSchema),
    metrics: Type.Optional(TransportMetricsSchema),
  },
  { additionalProperties: false },
);

export type TransportKind = (typeof TRANSPORT_KINDS)[number];
export type ConnectionStatus = (typeof CONNECTION_STATUSES)[number];
export type BacklogClassification = (typeof BACKLOG_CLASSIFICATIONS)[number];
export type TransportConnectionState = Static<
  typeof TransportConnectionStateSchema
>;
export type TransportMetrics = Static<typeof TransportMetricsSchema>;
export type SerialDevice = Static<typeof SerialDeviceSchema>;
export type NormalizedMeshPacket = Static<typeof NormalizedMeshPacketSchema>;
export type NormalizedFromRadio = Static<typeof NormalizedFromRadioSchema>;
export type MeshObservation = Static<typeof MeshObservationSchema>;
export type MeshtasticRuntimeStatus = Static<
  typeof MeshtasticRuntimeStatusSchema
>;
