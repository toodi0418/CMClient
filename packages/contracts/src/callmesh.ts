import { Type, type Static } from "@sinclair/typebox";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";

export const CALLMESH_STATES = [
  "unavailable",
  "checking",
  "ready",
  "degraded",
] as const;

export const CALLMESH_PROVISION_STATES = [
  "unavailable",
  "valid",
  "expired",
  "revoked",
  "invalid",
] as const;

const APRS_PRINTABLE_CHARACTER = "^[ -~]$";

export const CallMeshProvisionSchema = Type.Object(
  {
    callsignBase: Type.String({ pattern: "^[A-Z0-9]{1,6}$" }),
    ssid: Type.Integer({ minimum: -15, maximum: 15 }),
    symbolTable: Type.String({
      minLength: 1,
      maxLength: 1,
      pattern: APRS_PRINTABLE_CHARACTER,
    }),
    symbolCode: Type.String({
      minLength: 1,
      maxLength: 1,
      pattern: APRS_PRINTABLE_CHARACTER,
    }),
    symbolOverlay: Type.Optional(
      Type.Union([
        Type.String({
          minLength: 1,
          maxLength: 1,
          pattern: APRS_PRINTABLE_CHARACTER,
        }),
        Type.Null(),
      ]),
    ),
    comment: Type.Optional(
      Type.String({ minLength: 1, maxLength: 80, pattern: "^[^\\r\\n]*$" }),
    ),
    latitude: Type.Optional(Type.Number({ minimum: -90, maximum: 90 })),
    longitude: Type.Optional(Type.Number({ minimum: -180, maximum: 180 })),
    txPowerW: Type.Optional(Type.Number({ minimum: 0, maximum: 10_000 })),
    antennaGainDbi: Type.Optional(Type.Number({ minimum: -100, maximum: 100 })),
    antennaHeightM: Type.Optional(
      Type.Number({ minimum: 0, maximum: 100_000 }),
    ),
  },
  { additionalProperties: false },
);

export const CallMeshMappingSchema = Type.Object(
  {
    version: Type.String({ minLength: 1, maxLength: 128 }),
    effectiveAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    meshNetworkId: Type.String({ minLength: 1, maxLength: 128 }),
    nodeNum: Type.Integer({ minimum: 0, maximum: 4_294_967_295 }),
    callsign: Type.String({ minLength: 1, maxLength: 16 }),
    symbolTable: Type.Optional(
      Type.Union([
        Type.String({
          minLength: 1,
          maxLength: 1,
          pattern: APRS_PRINTABLE_CHARACTER,
        }),
        Type.Null(),
      ]),
    ),
    symbolCode: Type.Optional(
      Type.Union([
        Type.String({
          minLength: 1,
          maxLength: 1,
          pattern: APRS_PRINTABLE_CHARACTER,
        }),
        Type.Null(),
      ]),
    ),
    symbolOverlay: Type.Optional(
      Type.Union([
        Type.String({
          minLength: 1,
          maxLength: 1,
          pattern: APRS_PRINTABLE_CHARACTER,
        }),
        Type.Null(),
      ]),
    ),
    comment: Type.Optional(
      Type.Union([
        Type.String({ maxLength: 80, pattern: "^[^\\r\\n]*$" }),
        Type.Null(),
      ]),
    ),
    altitudeMeters: Type.Optional(Type.Union([Type.Number(), Type.Null()])),
  },
  { additionalProperties: false },
);

export const CallMeshStatusSchema = Type.Object(
  {
    state: Type.Union(CALLMESH_STATES.map((state) => Type.Literal(state))),
    updatedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    reasonCode: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    activeMappingVersion: Type.Optional(
      Type.String({ minLength: 1, maxLength: 128 }),
    ),
    activeMappingHash: Type.Optional(
      Type.String({ minLength: 1, maxLength: 128 }),
    ),
    activeMappingCount: Type.Integer({ minimum: 0 }),
    provisionState: Type.Union(
      CALLMESH_PROVISION_STATES.map((state) => Type.Literal(state)),
    ),
    lastServerTime: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
  },
  { additionalProperties: false },
);

export const CallMeshOverviewSchema = Type.Object(
  {
    status: CallMeshStatusSchema,
    mappings: Type.Array(CallMeshMappingSchema, { maxItems: 200 }),
  },
  { additionalProperties: false },
);

export type CallMeshMapping = Static<typeof CallMeshMappingSchema>;
export type CallMeshProvision = Static<typeof CallMeshProvisionSchema>;
export type CallMeshStatus = Static<typeof CallMeshStatusSchema>;
export type CallMeshOverview = Static<typeof CallMeshOverviewSchema>;
