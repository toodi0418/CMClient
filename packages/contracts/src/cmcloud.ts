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

export type CMCloudEnrollmentState = Static<
  typeof CMCloudEnrollmentStateSchema
>;
export type CMCloudEnrollmentStatus = Static<
  typeof CMCloudEnrollmentStatusSchema
>;
export type CMCloudEnrollmentRequest = Static<
  typeof CMCloudEnrollmentRequestSchema
>;
