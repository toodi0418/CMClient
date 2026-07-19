import {
  type BuildMetadata,
  type SystemCapabilities,
} from "@cmclient/contracts";

const COMPILED_BUILD_VERSION = "2.0.0-rc.1";

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
  const injectedVersion = environment.CMCLIENT_BUILD_VERSION?.trim();
  if (injectedVersion && injectedVersion !== COMPILED_BUILD_VERSION) {
    throw new Error("BUILD_VERSION_MISMATCH");
  }
  const version = COMPILED_BUILD_VERSION;
  const channel: BuildMetadata["channel"] = version.includes("-dev.")
    ? "dev"
    : version.includes("-")
      ? "beta"
      : "stable";
  const injectedChannel = environment.CMCLIENT_BUILD_CHANNEL?.trim();
  if (injectedChannel && injectedChannel !== channel) {
    throw new Error("BUILD_CHANNEL_MISMATCH");
  }
  const injectedCommit = environment.CMCLIENT_BUILD_COMMIT?.trim();
  if (injectedCommit && !/^[a-f0-9]{40}$/.test(injectedCommit)) {
    throw new Error("BUILD_COMMIT_INVALID");
  }
  const build: BuildMetadata = {
    version,
    commit: injectedCommit || "unknown",
    channel,
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
