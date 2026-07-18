export type RuntimeServiceState =
  | "disabled"
  | "stopped"
  | "starting"
  | "running"
  | "backoff"
  | "degraded"
  | "unavailable";

export interface MeshtasticServiceStatus {
  state: RuntimeServiceState;
  transport?: string;
  framesReceived?: number;
  reasonCode?: string;
}

export interface AprsCallMeshServiceStatus {
  state: RuntimeServiceState;
  aprsState?: string;
  callmeshState?: string;
  activeMappingCount?: number;
  pendingCount?: number;
  failedCount?: number;
  reasonCode?: string;
}

export interface ProxyServiceStatus {
  state: RuntimeServiceState;
  mode?: string;
  activeClients?: number;
  maxClients?: number;
  reasonCode?: string;
}

export interface DesktopServiceStatus {
  schemaVersion: 1;
  meshtastic: MeshtasticServiceStatus;
  aprsCallmesh: AprsCallMeshServiceStatus;
  proxy: ProxyServiceStatus;
}

export function meshtasticDetail(
  status: MeshtasticServiceStatus | undefined,
): string {
  if (!status) return "checking";
  if (status.reasonCode) return status.reasonCode;
  const details = [status.transport];
  if (status.framesReceived !== undefined) {
    details.push(`${status.framesReceived} frames`);
  }
  return details.filter(Boolean).join(" / ") || status.state;
}

export function aprsCallmeshDetail(
  status: AprsCallMeshServiceStatus | undefined,
): string {
  if (!status) return "checking";
  if (status.reasonCode) return status.reasonCode;
  const details = [status.aprsState, status.callmeshState];
  if (status.activeMappingCount !== undefined) {
    details.push(`${status.activeMappingCount} maps`);
  }
  if (status.pendingCount !== undefined) {
    details.push(`${status.pendingCount} pending`);
  }
  if (status.failedCount) {
    details.push(`${status.failedCount} failed`);
  }
  return details.filter(Boolean).join(" / ") || status.state;
}

export function proxyDetail(status: ProxyServiceStatus | undefined): string {
  if (!status) return "checking";
  if (status.reasonCode) return status.reasonCode;
  const clients =
    status.activeClients === undefined || status.maxClients === undefined
      ? undefined
      : `${status.activeClients}/${status.maxClients} clients`;
  return [status.mode, clients].filter(Boolean).join(" / ") || status.state;
}
