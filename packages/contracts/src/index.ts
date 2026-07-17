export const JOB_STATUSES = [
  "queued",
  "running",
  "waiting",
  "succeeded",
  "failed",
  "cancelling",
  "cancelled",
  "rolling_back",
  "rolled_back"
] as const;

export type JobStatus = (typeof JOB_STATUSES)[number];

export interface ApiError {
  code: string;
  params: Record<string, string | number | boolean | null>;
  traceId: string;
}

export interface JobSummary {
  id: string;
  status: JobStatus;
  createdAt: string;
  updatedAt: string;
}
