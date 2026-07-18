import { Type, type Static } from "@sinclair/typebox";

export const UPDATE_CHANNELS = ["stable", "beta", "dev"] as const;
export const UPDATE_COMPONENTS = [
  "desktop",
  "headless",
  "cli",
  "service",
] as const;
export const UPDATE_TARGETS = [
  "darwin-aarch64",
  "darwin-x86_64",
  "linux-aarch64",
  "linux-x86_64",
  "windows-x86_64",
] as const;
export const UPDATE_ARCHIVES = ["tar.zst", "zip"] as const;

const SemVerSchema = Type.String({
  pattern:
    "^(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)(?:-(?:(?:0|[1-9]\\d*|\\d*[A-Za-z-][0-9A-Za-z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\\+[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?$",
});
const PublishedAtSchema = Type.String({
  pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$",
});
const SigningKeyIdSchema = Type.String({ pattern: "^[A-Za-z0-9._-]{1,128}$" });

export const UpdateChannelSchema = Type.Union(
  UPDATE_CHANNELS.map((channel) => Type.Literal(channel)),
);
export const UpdateComponentSchema = Type.Union(
  UPDATE_COMPONENTS.map((component) => Type.Literal(component)),
);
export const UpdateTargetSchema = Type.Union(
  UPDATE_TARGETS.map((target) => Type.Literal(target)),
);
export const UpdateArchiveSchema = Type.Union(
  UPDATE_ARCHIVES.map((archive) => Type.Literal(archive)),
);

export const UpdateBundleSchema = Type.Object(
  {
    component: UpdateComponentSchema,
    target: UpdateTargetSchema,
    archive: UpdateArchiveSchema,
    url: Type.String({
      minLength: 9,
      pattern: "^https://[^\\s/@#]+(?:[/:?][^\\s@#]*)?$",
    }),
    sha256: Type.String({ pattern: "^[a-f0-9]{64}$" }),
    sizeBytes: Type.Integer({ minimum: 1 }),
  },
  { additionalProperties: false },
);

export const UpdateManifestSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    channel: UpdateChannelSchema,
    version: SemVerSchema,
    publishedAt: PublishedAtSchema,
    minimumAgentVersion: SemVerSchema,
    bundles: Type.Array(UpdateBundleSchema, { minItems: 1 }),
  },
  { additionalProperties: false },
);

export const SignedUpdateManifestSchema = Type.Object(
  {
    manifest: UpdateManifestSchema,
    signingKeyId: SigningKeyIdSchema,
    signatureAlgorithm: Type.Literal("ed25519"),
    signature: Type.String({
      minLength: 86,
      maxLength: 86,
      pattern: "^[A-Za-z0-9+/]+$",
    }),
  },
  { additionalProperties: false },
);

export type UpdateChannel = (typeof UPDATE_CHANNELS)[number];
export type UpdateComponent = (typeof UPDATE_COMPONENTS)[number];
export type UpdateTarget = (typeof UPDATE_TARGETS)[number];
export type UpdateArchive = (typeof UPDATE_ARCHIVES)[number];
export type UpdateBundle = Static<typeof UpdateBundleSchema>;
export type UpdateManifest = Static<typeof UpdateManifestSchema>;
export type SignedUpdateManifest = Static<typeof SignedUpdateManifestSchema>;
