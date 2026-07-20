import { Type, type Static } from "@sinclair/typebox";

export const PRODUCT_NAME = "CMClient" as const;
export const RELEASE_CHANNELS = ["dev", "candidate", "stable"] as const;
export const RUNTIME_PROFILES = ["native", "docker"] as const;
export const TARGET_OPERATING_SYSTEMS = ["windows", "macos", "linux"] as const;
export const TARGET_ARCHITECTURES = ["x86_64", "aarch64", "universal"] as const;
export const PACKAGE_PROFILES = [
  "workspace",
  "setup",
  "dmg",
  "appimage",
  "oci",
] as const;
export const INTERNAL_COMPONENTS = [
  "agent",
  "gateway",
  "web",
  "graphical-mode",
  "command-mode",
  "updater",
] as const;

export const ReleaseChannelSchema = Type.Union(
  RELEASE_CHANNELS.map((channel) => Type.Literal(channel)),
);
export const RuntimeProfileSchema = Type.Union(
  RUNTIME_PROFILES.map((profile) => Type.Literal(profile)),
);
export const InternalComponentSchema = Type.Union(
  INTERNAL_COMPONENTS.map((component) => Type.Literal(component)),
);

export const SemVerSchema = Type.String({
  pattern:
    "^(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)(?:-(?:(?:0|[1-9]\\d*|\\d*[A-Za-z-][0-9A-Za-z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\\+[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?$",
});

export const GitObjectIdSchema = Type.String({
  pattern: "^[a-f0-9]{40}$",
});

export const SourceTreeIdentitySchema = Type.String({
  pattern: "^(?:[a-f0-9]{40}|sha256:[a-f0-9]{64})$",
});

const releaseIdentityProperties = {
  schemaVersion: Type.Literal(1),
  product: Type.Literal(PRODUCT_NAME),
  version: SemVerSchema,
  sourceCommit: GitObjectIdSchema,
  sourceTree: SourceTreeIdentitySchema,
  channel: ReleaseChannelSchema,
} as const;

export const ReleaseIdentitySchema = Type.Object(releaseIdentityProperties, {
  additionalProperties: false,
});

export const NativeSourceTargetSchema = Type.Union([
  Type.Object(
    {
      os: Type.Literal("windows"),
      architecture: Type.Literal("x86_64"),
      profile: Type.Literal("native"),
      packageProfile: Type.Literal("workspace"),
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      os: Type.Literal("macos"),
      architecture: Type.Union([
        Type.Literal("x86_64"),
        Type.Literal("aarch64"),
        Type.Literal("universal"),
      ]),
      profile: Type.Literal("native"),
      packageProfile: Type.Literal("workspace"),
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      os: Type.Literal("linux"),
      architecture: Type.Union([
        Type.Literal("x86_64"),
        Type.Literal("aarch64"),
      ]),
      profile: Type.Literal("native"),
      packageProfile: Type.Literal("workspace"),
    },
    { additionalProperties: false },
  ),
]);

export const NativeDistributionTargetSchema = Type.Union([
  Type.Object(
    {
      os: Type.Literal("windows"),
      architecture: Type.Literal("x86_64"),
      profile: Type.Literal("native"),
      packageProfile: Type.Literal("setup"),
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      os: Type.Literal("macos"),
      architecture: Type.Literal("universal"),
      profile: Type.Literal("native"),
      packageProfile: Type.Literal("dmg"),
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      os: Type.Literal("linux"),
      architecture: Type.Union([
        Type.Literal("x86_64"),
        Type.Literal("aarch64"),
      ]),
      profile: Type.Literal("native"),
      packageProfile: Type.Literal("appimage"),
    },
    { additionalProperties: false },
  ),
]);

export const NativeTargetSchema = Type.Union([
  NativeSourceTargetSchema,
  NativeDistributionTargetSchema,
]);

export const DockerTargetSchema = Type.Object(
  {
    os: Type.Literal("linux"),
    architecture: Type.Union([Type.Literal("x86_64"), Type.Literal("aarch64")]),
    profile: Type.Literal("docker"),
    packageProfile: Type.Literal("oci"),
  },
  { additionalProperties: false },
);

export const ProductTargetSchema = Type.Union([
  NativeTargetSchema,
  DockerTargetSchema,
]);

export const ProductIdentitySchema = Type.Object(
  {
    ...releaseIdentityProperties,
    target: ProductTargetSchema,
  },
  { additionalProperties: false },
);

export const ComponentIdentityReportSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    component: InternalComponentSchema,
    identity: ProductIdentitySchema,
  },
  { additionalProperties: false },
);

export type ReleaseChannel = (typeof RELEASE_CHANNELS)[number];
export type RuntimeProfile = (typeof RUNTIME_PROFILES)[number];
export type TargetOperatingSystem = (typeof TARGET_OPERATING_SYSTEMS)[number];
export type TargetArchitecture = (typeof TARGET_ARCHITECTURES)[number];
export type PackageProfile = (typeof PACKAGE_PROFILES)[number];
export type InternalComponent = (typeof INTERNAL_COMPONENTS)[number];
export type ReleaseIdentity = Static<typeof ReleaseIdentitySchema>;
export type NativeTarget = Static<typeof NativeTargetSchema>;
export type NativeDistributionTarget = Static<
  typeof NativeDistributionTargetSchema
>;
export type DockerTarget = Static<typeof DockerTargetSchema>;
export type ProductTarget = Static<typeof ProductTargetSchema>;
export type ProductIdentity = Static<typeof ProductIdentitySchema>;
export type ComponentIdentityReport = Static<
  typeof ComponentIdentityReportSchema
>;
