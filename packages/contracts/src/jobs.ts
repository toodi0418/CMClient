import { Type, type Static } from "@sinclair/typebox";

const UTC_ISO_TIMESTAMP =
  "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$";

export const JOB_STATUSES = [
  "queued",
  "running",
  "waiting",
  "succeeded",
  "failed",
  "cancelling",
  "cancelled",
  "rolling_back",
  "rolled_back",
] as const;

export const JobStatusSchema = Type.Union(
  JOB_STATUSES.map((status) => Type.Literal(status)),
);

export const JobErrorSchema = Type.Object(
  {
    code: Type.String({ minLength: 1, maxLength: 128 }),
    params: Type.Record(
      Type.String(),
      Type.Union([Type.String(), Type.Number(), Type.Boolean(), Type.Null()]),
    ),
  },
  { additionalProperties: false },
);

export const JobSummarySchema = Type.Object(
  {
    id: Type.String({ minLength: 1, maxLength: 128 }),
    type: Type.String({ minLength: 1, maxLength: 128 }),
    status: JobStatusSchema,
    createdAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    updatedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
  },
  { additionalProperties: false },
);

export const JobDetailSchema = Type.Object(
  {
    id: Type.String({ minLength: 1, maxLength: 128 }),
    type: Type.String({ minLength: 1, maxLength: 128 }),
    status: JobStatusSchema,
    createdAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    updatedAt: Type.String({ pattern: UTC_ISO_TIMESTAMP }),
    startedAt: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
    completedAt: Type.Optional(Type.String({ pattern: UTC_ISO_TIMESTAMP })),
    error: Type.Optional(JobErrorSchema),
  },
  { additionalProperties: false },
);

export const JobAcceptedSchema = Type.Object(
  {
    jobId: Type.String({ minLength: 1, maxLength: 128 }),
    reused: Type.Boolean(),
  },
  { additionalProperties: false },
);

export const ApiErrorSchema = Type.Object(
  {
    code: Type.String({ minLength: 1, maxLength: 128 }),
    params: Type.Record(
      Type.String(),
      Type.Union([Type.String(), Type.Number(), Type.Boolean(), Type.Null()]),
    ),
    traceId: Type.String({ minLength: 1, maxLength: 128 }),
  },
  { additionalProperties: false },
);

export type JobStatus = (typeof JOB_STATUSES)[number];
export type JobError = Static<typeof JobErrorSchema>;
export type JobSummary = Static<typeof JobSummarySchema>;
export type JobDetail = Static<typeof JobDetailSchema>;
export type JobAccepted = Static<typeof JobAcceptedSchema>;

export interface ApiError {
  code: string;
  params: Record<string, string | number | boolean | null>;
  traceId: string;
}
