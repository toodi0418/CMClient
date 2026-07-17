import { Type, type Static } from "@sinclair/typebox";

import {
  BacklogClassificationSchema,
  TransportKindSchema,
} from "./transport.js";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";
const NodeNumberSchema = Type.Integer({ minimum: 0, maximum: 4_294_967_295 });
const PayloadHashSchema = Type.String({ pattern: "^[a-f0-9]{64}$" });
const CoordinatesSchema = Type.Object(
  {
    latitudeI: Type.Optional(
      Type.Integer({ minimum: -900_000_000, maximum: 900_000_000 }),
    ),
    longitudeI: Type.Optional(
      Type.Integer({ minimum: -1_800_000_000, maximum: 1_800_000_000 }),
    ),
    altitudeMslMeters: Type.Optional(Type.Integer()),
    altitudeHaeMeters: Type.Optional(Type.Integer()),
    altitudeGeoidalSeparationMeters: Type.Optional(Type.Integer()),
    positionTimestampSeconds: Type.Optional(NodeNumberSchema),
    positionTimestampMillisAdjust: Type.Optional(Type.Integer()),
    positionTimeSeconds: Type.Optional(NodeNumberSchema),
    sequenceNumber: Type.Optional(NodeNumberSchema),
    precisionBits: Type.Optional(Type.Integer({ minimum: 0, maximum: 32 })),
    groundSpeedMetersPerSecond: Type.Optional(Type.Number({ minimum: 0 })),
    groundTrackDegrees: Type.Optional(
      Type.Number({ minimum: 0, maximum: 360 }),
    ),
  },
  { additionalProperties: false },
);

export const POSITION_DECISION_CODES = [
  "POSITION_ACCEPTED",
  "POSITION_DUPLICATE",
  "POSITION_HISTORICAL",
  "POSITION_BACKLOG",
  "POSITION_CLOCK_INVALID",
  "POSITION_SEQUENCE_CONFLICT",
  "POSITION_PRECISION_INSUFFICIENT",
  "POSITION_SPEED_ANOMALY",
  "POSITION_QUARANTINED",
  "APRS_SKIPPED_OUT_OF_ORDER",
] as const;
export const POSITION_EVENT_TIME_SOURCES = [
  "position_timestamp",
  "position_time",
  "sequence",
] as const;

export const PositionSampleSchema = CoordinatesSchema;
export const PositionObservationSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    id: Type.String({ minLength: 1, maxLength: 128 }),
    meshNetworkId: Type.String({ minLength: 1, maxLength: 128 }),
    nodeNum: NodeNumberSchema,
    meshObservationId: Type.String({ minLength: 1, maxLength: 128 }),
    gatewayId: Type.String({ minLength: 1, maxLength: 128 }),
    transport: TransportKindSchema,
    sessionConnectedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    ingestedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    serverIngestedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    deviceRxTimeSeconds: Type.Optional(NodeNumberSchema),
    backlogClassification: BacklogClassificationSchema,
    packetId: Type.Optional(NodeNumberSchema),
    payloadHash: PayloadHashSchema,
    viaMqtt: Type.Optional(Type.Boolean()),
    rxSnr: Type.Optional(Type.Number()),
    rxRssi: Type.Optional(Type.Integer()),
    hopLimit: Type.Optional(Type.Integer({ minimum: 0 })),
    hopStart: Type.Optional(Type.Integer({ minimum: 0 })),
    position: PositionSampleSchema,
  },
  { additionalProperties: false },
);

export const PositionCanonicalEventSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    id: Type.String({ minLength: 1, maxLength: 128 }),
    canonicalKey: Type.String({ minLength: 1, maxLength: 128 }),
    meshNetworkId: Type.String({ minLength: 1, maxLength: 128 }),
    nodeNum: NodeNumberSchema,
    sourceObservationId: Type.String({ minLength: 1, maxLength: 128 }),
    payloadHash: PayloadHashSchema,
    eventTime: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
    eventTimeSource: Type.Optional(
      Type.Union(
        POSITION_EVENT_TIME_SOURCES.map((source) => Type.Literal(source)),
      ),
    ),
    sequenceEpoch: Type.Optional(Type.Integer({ minimum: 0 })),
    sequenceNumber: Type.Optional(NodeNumberSchema),
    position: PositionSampleSchema,
    createdAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
  },
  { additionalProperties: false },
);

export const PositionDecisionCodeSchema = Type.Union(
  POSITION_DECISION_CODES.map((code) => Type.Literal(code)),
);
export const PositionDecisionSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    id: Type.String({ minLength: 1, maxLength: 128 }),
    observationId: Type.String({ minLength: 1, maxLength: 128 }),
    canonicalEventId: Type.Optional(
      Type.String({ minLength: 1, maxLength: 128 }),
    ),
    code: PositionDecisionCodeSchema,
    decidedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    parameters: Type.Record(
      Type.String({ minLength: 1, maxLength: 64 }),
      Type.Union([
        Type.String({ maxLength: 256 }),
        Type.Number(),
        Type.Boolean(),
      ]),
    ),
  },
  { additionalProperties: false },
);

export const NodePositionStateSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    meshNetworkId: Type.String({ minLength: 1, maxLength: 128 }),
    nodeNum: NodeNumberSchema,
    callsign: Type.String({ minLength: 1, maxLength: 16 }),
    mappingVersion: Type.String({ minLength: 1, maxLength: 128 }),
    latestCanonicalEventId: Type.Optional(
      Type.String({ minLength: 1, maxLength: 128 }),
    ),
    latestEventTime: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
    latestSequenceEpoch: Type.Optional(Type.Integer({ minimum: 0 })),
    latestSequenceNumber: Type.Optional(NodeNumberSchema),
    latestLatitudeI: Type.Optional(
      Type.Integer({ minimum: -900_000_000, maximum: 900_000_000 }),
    ),
    latestLongitudeI: Type.Optional(
      Type.Integer({ minimum: -1_800_000_000, maximum: 1_800_000_000 }),
    ),
    updatedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
  },
  { additionalProperties: false },
);

export type PositionDecisionCode = (typeof POSITION_DECISION_CODES)[number];
export type PositionEventTimeSource =
  (typeof POSITION_EVENT_TIME_SOURCES)[number];
export type PositionSample = Static<typeof PositionSampleSchema>;
export type PositionObservation = Static<typeof PositionObservationSchema>;
export type PositionCanonicalEvent = Static<
  typeof PositionCanonicalEventSchema
>;
export type PositionDecision = Static<typeof PositionDecisionSchema>;
export type NodePositionState = Static<typeof NodePositionStateSchema>;
