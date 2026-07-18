export type RealtimeProjection = "domain" | "aprs" | "callmesh" | "proxy";

export function projectionForEvent(
  type: string,
): RealtimeProjection | undefined {
  if (/^(node|message|telemetry|position|mesh)\./.test(type)) {
    return "domain";
  }
  if (type.startsWith("aprs.")) {
    return "aprs";
  }
  if (type.startsWith("callmesh.")) {
    return "callmesh";
  }
  if (type.startsWith("proxy.")) {
    return "proxy";
  }
  return undefined;
}
