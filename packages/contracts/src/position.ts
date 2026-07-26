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
  "APRS_SKIPPED_RECENT_DUPLICATE",
  "APRS_PROVISION_UNAVAILABLE",
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

export const PositionCanonicalEventListSchema = Type.Object(
  { items: Type.Array(PositionCanonicalEventSchema, { maxItems: 200 }) },
  { additionalProperties: false },
);

export const AprsOutboxStatusSchema = Type.Union(
  ["queued", "sending", "sent", "failed"].map((status) => Type.Literal(status)),
);

export const AprsDeliveryStatusSchema = Type.Union(
  [
    "queued",
    "sending",
    "failed",
    "submitted",
    "observer_confirmed",
    "observation_expired",
  ].map((status) => Type.Literal(status)),
);

export const AprsOutboxEntrySchema = Type.Object(
  {
    id: Type.String({ minLength: 1, maxLength: 128 }),
    callsign: Type.String({ minLength: 1, maxLength: 16 }),
    canonicalEventId: Type.String({ minLength: 1, maxLength: 128 }),
    status: AprsOutboxStatusSchema,
    deliveryStatus: AprsDeliveryStatusSchema,
    attempts: Type.Integer({ minimum: 0 }),
    nextAttemptAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    createdAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    updatedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    lastErrorCode: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    sentAt: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
    submittedAt: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
    observerConfirmedAt: Type.Optional(
      Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    ),
    observationExpiresAt: Type.Optional(
      Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    ),
  },
  { additionalProperties: false },
);

export const AprsOutboxEntryListSchema = Type.Object(
  { items: Type.Array(AprsOutboxEntrySchema, { maxItems: 200 }) },
  { additionalProperties: false },
);

export const APRS_IGATE_PACKET_KINDS = [
  "beacon",
  "status",
  "telemetry-parm",
  "telemetry-unit",
  "telemetry-eqns",
  "telemetry-data",
] as const;

export const APRS_IGATE_DELIVERY_STATUSES = [
  "sending",
  "transmission_uncertain",
  "submitted",
  "observer_confirmed",
  "observation_expired",
] as const;

export const AprsIgateSubmissionSchema = Type.Object(
  {
    id: Type.String({ minLength: 1, maxLength: 128 }),
    packetKind: Type.Union(
      APRS_IGATE_PACKET_KINDS.map((kind) => Type.Literal(kind)),
    ),
    deliveryStatus: Type.Union(
      APRS_IGATE_DELIVERY_STATUSES.map((status) => Type.Literal(status)),
    ),
    attemptedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    updatedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    submittedAt: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
    localWriteCompletedAt: Type.Optional(
      Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    ),
    observerConfirmedAt: Type.Optional(
      Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    ),
    observationExpiresAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
  },
  { additionalProperties: false },
);

export const AprsIgateSubmissionListSchema = Type.Object(
  { items: Type.Array(AprsIgateSubmissionSchema, { maxItems: 200 }) },
  { additionalProperties: false },
);

export const APRS_MONITOR_STATUSES = [
  "stopped",
  "idle",
  "connecting",
  "connected",
  "error",
] as const;
export const AprsRuntimeStatusSchema = Type.Object(
  {
    configured: Type.Boolean(),
    running: Type.Boolean(),
    monitorStatus: Type.Union(
      APRS_MONITOR_STATUSES.map((status) => Type.Literal(status)),
    ),
    mappedCallsigns: Type.Integer({ minimum: 0 }),
    pendingOutbox: Type.Integer({ minimum: 0 }),
    failedOutbox: Type.Integer({ minimum: 0 }),
    pendingStationSubmissions: Type.Integer({ minimum: 0 }),
    failedStationSubmissions: Type.Integer({ minimum: 0 }),
    lastErrorCode: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
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
export type PositionCanonicalEventList = Static<
  typeof PositionCanonicalEventListSchema
>;
export type AprsOutboxEntry = Static<typeof AprsOutboxEntrySchema>;
export type AprsOutboxEntryList = Static<typeof AprsOutboxEntryListSchema>;
export type AprsIgatePacketKind = (typeof APRS_IGATE_PACKET_KINDS)[number];
export type AprsIgateDeliveryStatus =
  (typeof APRS_IGATE_DELIVERY_STATUSES)[number];
export type AprsIgateSubmission = Static<typeof AprsIgateSubmissionSchema>;
export type AprsIgateSubmissionList = Static<
  typeof AprsIgateSubmissionListSchema
>;
export type AprsMonitorStatus = (typeof APRS_MONITOR_STATUSES)[number];
export type AprsRuntimeStatus = Static<typeof AprsRuntimeStatusSchema>;
export type PositionDecision = Static<typeof PositionDecisionSchema>;
export type NodePositionState = Static<typeof NodePositionStateSchema>;
