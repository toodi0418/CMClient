import { Type, type Static } from "@sinclair/typebox";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";

export const CALLMESH_STATES = [
  "unavailable",
  "checking",
  "ready",
  "degraded",
] as const;

export const CallMeshMappingSchema = Type.Object(
  {
    version: Type.String({ minLength: 1, maxLength: 128 }),
    effectiveAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    meshNetworkId: Type.String({ minLength: 1, maxLength: 128 }),
    nodeNum: Type.Integer({ minimum: 0, maximum: 4_294_967_295 }),
    callsign: Type.String({ minLength: 1, maxLength: 16 }),
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
    activeMappingCount: Type.Integer({ minimum: 0 }),
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
export type CallMeshStatus = Static<typeof CallMeshStatusSchema>;
export type CallMeshOverview = Static<typeof CallMeshOverviewSchema>;
