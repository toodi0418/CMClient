import {
  type BuildMetadata,
  type SystemCapabilities,
} from "@cmclient/contracts";

export interface GatewaySystemState {
  build: BuildMetadata;
  capabilities: SystemCapabilities;
}

export function defaultGatewaySystemState(): GatewaySystemState {
  const platform =
    process.platform === "darwin" ||
    process.platform === "linux" ||
    process.platform === "win32"
      ? process.platform === "win32"
        ? "windows"
        : process.platform
      : "unknown";
  const build: BuildMetadata = {
    version: process.env.CMCLIENT_BUILD_VERSION?.trim() || "2.0.0-dev.0",
    commit: process.env.CMCLIENT_BUILD_COMMIT?.trim() || "unknown",
    channel: "dev",
  };
  return {
    build,
    capabilities: {
      schemaVersion: 1,
      platform,
      build,
      capabilities: {
        managementWeb: {
          available: false,
          reasonCode: "CAPABILITY_OWNED_BY_AGENT",
        },
        update: { available: false, reasonCode: "CAPABILITY_OWNED_BY_AGENT" },
        tray: { available: false, reasonCode: "CAPABILITY_OWNED_BY_DESKTOP" },
        serial: { available: false, reasonCode: "CAPABILITY_NOT_CONFIGURED" },
        service: { available: false, reasonCode: "CAPABILITY_OWNED_BY_AGENT" },
        autoStart: {
          available: false,
          reasonCode: "CAPABILITY_OWNED_BY_AGENT",
        },
        docker: { available: false, reasonCode: "CAPABILITY_NOT_CONFIGURED" },
      },
    },
  };
}
