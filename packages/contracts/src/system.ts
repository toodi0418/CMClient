import { Type, type Static } from "@sinclair/typebox";

export const BUILD_CHANNELS = ["stable", "beta", "dev"] as const;
export const PLATFORM_IDS = ["darwin", "linux", "windows", "unknown"] as const;
export const CAPABILITY_KEYS = [
  "managementWeb",
  "update",
  "tray",
  "serial",
  "service",
  "autoStart",
  "docker",
] as const;

const BuildChannelSchema = Type.Union(
  BUILD_CHANNELS.map((channel) => Type.Literal(channel)),
);
const PlatformSchema = Type.Union(
  PLATFORM_IDS.map((platform) => Type.Literal(platform)),
);

export const BuildMetadataSchema = Type.Object(
  {
    version: Type.String({ minLength: 1 }),
    commit: Type.String({ minLength: 1 }),
    channel: BuildChannelSchema,
    builtAt: Type.Optional(Type.String({ format: "date-time" })),
  },
  { additionalProperties: false },
);

export const SystemHealthSchema = Type.Object(
  { status: Type.Literal("ok") },
  { additionalProperties: false },
);

export const SystemStatusSchema = Type.Object(
  {
    health: Type.Literal("ok"),
    build: BuildMetadataSchema,
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
      reasonCode: Type.String({ minLength: 1 }),
    },
    { additionalProperties: false },
  ),
]);

export const SystemCapabilitiesSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    platform: PlatformSchema,
    build: BuildMetadataSchema,
    capabilities: Type.Object(
      {
        managementWeb: CapabilityStateSchema,
        update: CapabilityStateSchema,
        tray: CapabilityStateSchema,
        serial: CapabilityStateSchema,
        service: CapabilityStateSchema,
        autoStart: CapabilityStateSchema,
        docker: CapabilityStateSchema,
      },
      { additionalProperties: false },
    ),
  },
  { additionalProperties: false },
);

export type BuildChannel = (typeof BUILD_CHANNELS)[number];
export type PlatformId = (typeof PLATFORM_IDS)[number];
export type CapabilityKey = (typeof CAPABILITY_KEYS)[number];
export type BuildMetadata = Static<typeof BuildMetadataSchema>;
export type SystemHealth = Static<typeof SystemHealthSchema>;
export type SystemStatus = Static<typeof SystemStatusSchema>;
export type CapabilityState = Static<typeof CapabilityStateSchema>;
export type SystemCapabilities = Static<typeof SystemCapabilitiesSchema>;
