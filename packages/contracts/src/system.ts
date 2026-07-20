import { Type, type Static } from "@sinclair/typebox";

import { ComponentIdentityReportSchema } from "./identity.js";

export const CAPABILITY_KEYS = [
  "managementWeb",
  "commandMode",
  "graphicalMode",
  "loginAutostart",
  "serial",
  "nativeUpdate",
  "dockerPullRecreateUpdate",
  "localControl",
  "remoteDispatch",
] as const;

export const CAPABILITY_REASON_CODES = [
  "owned_by_agent",
  "owned_by_graphical_mode",
  "not_configured",
  "unavailable_in_docker",
  "unavailable_in_native",
  "not_enabled",
] as const;

const CapabilityReasonCodeSchema = Type.Union(
  CAPABILITY_REASON_CODES.map((reason) => Type.Literal(reason)),
);

export const SystemHealthSchema = Type.Object(
  { status: Type.Literal("ok") },
  { additionalProperties: false },
);

export const SystemStatusSchema = Type.Object(
  {
    schemaVersion: Type.Literal(2),
    health: Type.Literal("ok"),
    identity: ComponentIdentityReportSchema,
  },
  { additionalProperties: false },
);

export const CapabilityStateSchema = Type.Union([
  Type.Object(
    { available: Type.Literal(true) },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      available: Type.Literal(false),
      reasonCode: CapabilityReasonCodeSchema,
    },
    { additionalProperties: false },
  ),
]);

export const SystemCapabilitiesSchema = Type.Object(
  {
    schemaVersion: Type.Literal(2),
    identity: ComponentIdentityReportSchema,
    capabilities: Type.Object(
      {
        managementWeb: CapabilityStateSchema,
        commandMode: CapabilityStateSchema,
        graphicalMode: CapabilityStateSchema,
        loginAutostart: CapabilityStateSchema,
        serial: CapabilityStateSchema,
        nativeUpdate: CapabilityStateSchema,
        dockerPullRecreateUpdate: CapabilityStateSchema,
        localControl: CapabilityStateSchema,
        remoteDispatch: CapabilityStateSchema,
      },
      { additionalProperties: false },
    ),
  },
  { additionalProperties: false },
);

export type CapabilityKey = (typeof CAPABILITY_KEYS)[number];
export type CapabilityReasonCode = (typeof CAPABILITY_REASON_CODES)[number];
export type SystemHealth = Static<typeof SystemHealthSchema>;
export type SystemStatus = Static<typeof SystemStatusSchema>;
export type CapabilityState = Static<typeof CapabilityStateSchema>;
export type SystemCapabilities = Static<typeof SystemCapabilitiesSchema>;
