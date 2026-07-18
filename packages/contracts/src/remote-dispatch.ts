import { Type, type Static } from "@sinclair/typebox";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";

export const REMOTE_DISPATCH_STATUSES = [
  "queued",
  "accepted",
  "sending",
  "sent",
  "failed",
  "expired",
  "duplicate",
] as const;

export const RemoteDispatchStatusSchema = Type.Union(
  REMOTE_DISPATCH_STATUSES.map((status) => Type.Literal(status)),
);

export const RemoteDispatchTaskSchema = Type.Object(
  {
    schemaVersion: Type.Literal(1),
    jobId: Type.String({ minLength: 1, maxLength: 128 }),
    gatewayTarget: Type.String({ minLength: 1, maxLength: 128 }),
    meshNetworkId: Type.String({ minLength: 1, maxLength: 128 }),
    nodeTarget: Type.Integer({ minimum: 0, maximum: 4_294_967_295 }),
    channel: Type.Integer({ minimum: 0, maximum: 255 }),
    message: Type.String({ minLength: 1, maxLength: 512 }),
    expiresAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    dedupKey: Type.String({ minLength: 1, maxLength: 128 }),
    status: RemoteDispatchStatusSchema,
    acknowledgedAt: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
    completedAt: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
    errorCode: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
  },
  { additionalProperties: false },
);

export type RemoteDispatchStatus = (typeof REMOTE_DISPATCH_STATUSES)[number];
export type RemoteDispatchTask = Static<typeof RemoteDispatchTaskSchema>;
