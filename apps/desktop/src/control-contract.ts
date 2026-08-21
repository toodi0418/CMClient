import type { ComponentIdentityReport } from "@cmclient/contracts";

export type GatewayStatus =
  "stopped" | "starting" | "running" | "backoff" | "degraded";
export type ManagementWebStatus = "disabled" | "running";
export type AgentCommand =
  "start" | "stop" | "restart" | "enable_web" | "disable_web";

export interface ControlStatus {
  schemaVersion: number;
  agent: string;
  identity: ComponentIdentityReport;
  gateway: GatewayStatus;
  managementWeb: ManagementWebStatus;
  managementWebUrl: string | null;
  uptimeSeconds: number;
  latestErrorCode: string | null;
}

export interface UpdateControlJob {
  id: string;
  phase: string;
  updatedAt: string;
  errorCode: string | null;
  bytesDownloaded: number | null;
  bytesTotal: number | null;
  bytesPerSecond: number | null;
  recentLogCodes: string[];
}

export interface UpdateControlStatus {
  schemaVersion: number;
  job: UpdateControlJob | null;
}
