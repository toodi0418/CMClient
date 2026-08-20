import { Type, type Static } from "@sinclair/typebox";

const CMCLOUD_ENDPOINT = "^wss://[^\\s]+$";
const CMCLOUD_PAIRING_CODE = "^[A-Za-z0-9_-]{16,512}$";

export const CMCLOUD_ENROLLMENT_STATES = [
  "not_configured",
  "credentials_required",
  "pending_enrollment",
  "active",
] as const;

export const CMCloudEnrollmentStateSchema = Type.Union(
  CMCLOUD_ENROLLMENT_STATES.map((state) => Type.Literal(state)),
  { $id: "CMCloudEnrollmentState" },
);

export const CMCloudEnrollmentStatusSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    state: CMCloudEnrollmentStateSchema,
    endpoint: Type.Union([
      Type.String({ pattern: CMCLOUD_ENDPOINT, maxLength: 512 }),
      Type.Null(),
    ]),
    installationGeneration: Type.Union([
      Type.Integer({ minimum: 0 }),
      Type.Null(),
    ]),
    credentialVersion: Type.Union([Type.Integer({ minimum: 0 }), Type.Null()]),
  },
  { $id: "CMCloudEnrollmentStatus", additionalProperties: false },
);

export const CMCloudEnrollmentRequestSchema = Type.Object(
  {
    pairingCode: Type.String({ pattern: CMCLOUD_PAIRING_CODE }),
  },
  { $id: "CMCloudEnrollmentRequest", additionalProperties: false },
);

const CMCloudAccountProjectionErrorSchema = Type.Object(
  {
    code: Type.String({ minLength: 1, maxLength: 96 }),
    since: Type.String({ format: "date-time" }),
  },
  { additionalProperties: false },
);

export const CMCloudAccountProjectionSchema = Type.Object(
  {
    type: Type.Literal("account_projection"),
    schemaVersion: Type.Literal(1),
    revision: Type.Integer({ minimum: 0 }),
    generation: Type.Integer({ minimum: 0 }),
    tenant: Type.Object(
      {
        id: Type.String({ format: "uuid" }),
        name: Type.String({ minLength: 1, maxLength: 160 }),
      },
      { additionalProperties: false },
    ),
    account: Type.Object(
      {
        issuer: Type.String({ minLength: 1, maxLength: 512 }),
        subject: Type.String({ minLength: 1, maxLength: 512 }),
        displayName: Type.String({ minLength: 1, maxLength: 160 }),
        email: Type.Optional(Type.String({ format: "email" })),
        role: Type.Union([
          Type.Literal("member"),
          Type.Literal("operator"),
          Type.Literal("admin"),
        ]),
        state: Type.Union([
          Type.Literal("pending"),
          Type.Literal("approved"),
          Type.Literal("suspended"),
        ]),
        mappingFreezeEpoch: Type.Integer({ minimum: 0 }),
        mappingFrozenAt: Type.Optional(Type.String({ format: "date-time" })),
      },
      { additionalProperties: false },
    ),
    stations: Type.Array(
      Type.Object(
        {
          id: Type.String({ format: "uuid" }),
          label: Type.String({ minLength: 1, maxLength: 160 }),
          kind: Type.Union([
            Type.Literal("cmclient"),
            Type.Literal("mqtt_only"),
          ]),
          state: Type.Union([
            Type.Literal("online"),
            Type.Literal("offline"),
            Type.Literal("pending"),
            Type.Literal("suspended"),
          ]),
          callsign: Type.Optional(Type.String({ minLength: 1, maxLength: 16 })),
        },
        { additionalProperties: false },
      ),
    ),
    authority: Type.Object(
      {
        cmcloud: Type.Literal(true),
        epoch: Type.Integer({ minimum: 0 }),
        revision: Type.Integer({ minimum: 0 }),
      },
      { additionalProperties: false },
    ),
    freshness: Type.Object(
      {
        projectedAt: Type.String({ format: "date-time" }),
        staleAfterMs: Type.Integer({ minimum: 1 }),
      },
      { additionalProperties: false },
    ),
    errorState: Type.Union([CMCloudAccountProjectionErrorSchema, Type.Null()]),
  },
  { $id: "CMCloudAccountProjection", additionalProperties: false },
);

export type CMCloudEnrollmentState = Static<
  typeof CMCloudEnrollmentStateSchema
>;
export type CMCloudEnrollmentStatus = Static<
  typeof CMCloudEnrollmentStatusSchema
>;
export type CMCloudEnrollmentRequest = Static<
  typeof CMCloudEnrollmentRequestSchema
>;
export type CMCloudAccountProjection = Static<
  typeof CMCloudAccountProjectionSchema
>;
