import { Type, type Static } from "@sinclair/typebox";

import {
  NativeDistributionTargetSchema,
  ReleaseIdentitySchema,
  SemVerSchema,
  type NativeDistributionTarget,
  type ReleaseIdentity,
} from "./identity.js";

export const UPDATE_ARCHIVES = ["tar.zst", "zip"] as const;
export const UPDATE_PHASES = [
  "idle",
  "checking",
  "available",
  "downloading",
  "verifying",
  "staging",
  "backing_up",
  "stopping",
  "installing",
  "migrating",
  "starting",
  "health_checking",
  "completed",
  "failed",
  "rolling_back",
  "rollback_completed",
] as const;

const PublishedAtSchema = Type.String({
  pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$",
});
const SigningKeyIdSchema = Type.String({ pattern: "^[A-Za-z0-9._-]{1,128}$" });

export const UpdateArchiveSchema = Type.Union(
  UPDATE_ARCHIVES.map((archive) => Type.Literal(archive)),
);
export const UpdatePhaseSchema = Type.Union(
  UPDATE_PHASES.map((phase) => Type.Literal(phase)),
);

export const UpdateBundleSchema = Type.Object(
  {
    target: NativeDistributionTargetSchema,
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
    schemaVersion: Type.Literal(2),
    release: ReleaseIdentitySchema,
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

export const UpdateProgressSchema = Type.Object(
  {
    bytesDownloaded: Type.Integer({ minimum: 0 }),
    bytesTotal: Type.Union([Type.Integer({ minimum: 1 }), Type.Null()]),
    bytesPerSecond: Type.Union([Type.Integer({ minimum: 0 }), Type.Null()]),
  },
  { additionalProperties: false },
);

export const UpdateLogEntrySchema = Type.Object(
  {
    occurredAt: PublishedAtSchema,
    phase: UpdatePhaseSchema,
    code: Type.String({ pattern: "^[A-Z0-9_]{1,128}$" }),
  },
  { additionalProperties: false },
);

export const UpdateControlJobSchema = Type.Object(
  {
    id: Type.String({ minLength: 1, maxLength: 128 }),
    phase: UpdatePhaseSchema,
    updatedAt: PublishedAtSchema,
    errorCode: Type.Union([
      Type.String({ pattern: "^[A-Z0-9_]{1,128}$" }),
      Type.Null(),
    ]),
    bytesDownloaded: Type.Union([Type.Integer({ minimum: 0 }), Type.Null()]),
    bytesTotal: Type.Union([Type.Integer({ minimum: 1 }), Type.Null()]),
    bytesPerSecond: Type.Union([Type.Integer({ minimum: 0 }), Type.Null()]),
    recentLogCodes: Type.Array(Type.String({ pattern: "^[A-Z0-9_]{1,128}$" }), {
      maxItems: 64,
    }),
  },
  { additionalProperties: false },
);

export const UpdateControlStatusSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    job: Type.Union([UpdateControlJobSchema, Type.Null()]),
  },
  { $id: "UpdateControlStatus", additionalProperties: false },
);

export type UpdateReleaseIdentity = ReleaseIdentity;
export type UpdateTarget = NativeDistributionTarget;
export type UpdateArchive = (typeof UPDATE_ARCHIVES)[number];
export type UpdatePhase = (typeof UPDATE_PHASES)[number];
export type UpdateBundle = Static<typeof UpdateBundleSchema>;
export type UpdateManifest = Static<typeof UpdateManifestSchema>;
export type SignedUpdateManifest = Static<typeof SignedUpdateManifestSchema>;
export type UpdateProgress = Static<typeof UpdateProgressSchema>;
export type UpdateLogEntry = Static<typeof UpdateLogEntrySchema>;
export type UpdateControlJob = Static<typeof UpdateControlJobSchema>;
export type UpdateControlStatus = Static<typeof UpdateControlStatusSchema>;
