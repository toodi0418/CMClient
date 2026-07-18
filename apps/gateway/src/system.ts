import {
  BUILD_CHANNELS,
  type BuildMetadata,
  type SystemCapabilities,
} from "@cmclient/contracts";

export interface GatewaySystemState {
  build: BuildMetadata;
  capabilities: SystemCapabilities;
}

export function isDockerDeployment(
  environment: NodeJS.ProcessEnv = process.env,
): boolean {
  return (
    environment.CMCLIENT_DEPLOYMENT_MODE?.trim().toLowerCase() === "docker"
  );
}

export function defaultGatewaySystemState(
  environment: NodeJS.ProcessEnv = process.env,
): GatewaySystemState {
  const platform =
    process.platform === "darwin" ||
    process.platform === "linux" ||
    process.platform === "win32"
      ? process.platform === "win32"
        ? "windows"
        : process.platform
      : "unknown";
  const build: BuildMetadata = {
    version: environment.CMCLIENT_BUILD_VERSION?.trim() || "2.0.0-dev.0",
    commit: environment.CMCLIENT_BUILD_COMMIT?.trim() || "unknown",
    channel: BUILD_CHANNELS.includes(
      environment.CMCLIENT_BUILD_CHANNEL?.trim() as BuildMetadata["channel"],
    )
      ? (environment.CMCLIENT_BUILD_CHANNEL?.trim() as BuildMetadata["channel"])
      : "dev",
    ...(environment.CMCLIENT_BUILD_AT &&
    Number.isFinite(Date.parse(environment.CMCLIENT_BUILD_AT))
      ? { builtAt: new Date(environment.CMCLIENT_BUILD_AT).toISOString() }
      : {}),
  };
  const dockerDeployment = isDockerDeployment(environment);
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
        update: dockerDeployment
          ? { available: false, reasonCode: "CAPABILITY_UNAVAILABLE_DOCKER" }
          : { available: false, reasonCode: "CAPABILITY_OWNED_BY_AGENT" },
        tray: { available: false, reasonCode: "CAPABILITY_OWNED_BY_DESKTOP" },
        serial: dockerDeployment
          ? { available: false, reasonCode: "CAPABILITY_UNAVAILABLE_DOCKER" }
          : { available: false, reasonCode: "CAPABILITY_NOT_CONFIGURED" },
        service: dockerDeployment
          ? { available: false, reasonCode: "CAPABILITY_UNAVAILABLE_DOCKER" }
          : { available: false, reasonCode: "CAPABILITY_OWNED_BY_AGENT" },
        autoStart: dockerDeployment
          ? { available: false, reasonCode: "CAPABILITY_UNAVAILABLE_DOCKER" }
          : { available: false, reasonCode: "CAPABILITY_OWNED_BY_AGENT" },
        docker: dockerDeployment
          ? { available: true }
          : { available: false, reasonCode: "CAPABILITY_NOT_CONFIGURED" },
        remoteDispatch: {
          available: false,
          reasonCode: "REMOTE_DISPATCH_NOT_ENABLED",
        },
      },
    },
  };
}
