import type { BuildChannel, BuildMetadata } from "@cmclient/contracts";

export interface RuntimeConfig {
  managementHost: string;
  managementPort: number;
  webEnabled: boolean;
}

export type Environment = Record<string, string | undefined>;

export interface BuildMetadataDefaults {
  version: string;
  commit: string;
  channel?: BuildChannel;
}

export function parseBoolean(
  value: string | undefined,
  fallback: boolean,
): boolean {
  if (value === undefined || value.trim() === "") {
    return fallback;
  }

  return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
}

export function parsePort(value: string | undefined, fallback: number): number {
  if (value === undefined || value.trim() === "") {
    return fallback;
  }

  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    return fallback;
  }

  return port;
}

export function parseRuntimeConfig(environment: Environment): RuntimeConfig {
  return {
    managementHost: environment.CMCLIENT_MANAGEMENT_HOST?.trim() || "127.0.0.1",
    managementPort: parsePort(environment.CMCLIENT_MANAGEMENT_PORT, 7080),
    webEnabled: parseBoolean(environment.CMCLIENT_WEB_ENABLED, true),
  };
}

export function parseBuildMetadata(
  environment: Environment,
  defaults: BuildMetadataDefaults,
): BuildMetadata {
  const channel = environment.CMCLIENT_RELEASE_CHANNEL?.trim();
  const supportedChannel = ["stable", "beta", "dev"].includes(channel ?? "")
    ? (channel as BuildChannel)
    : (defaults.channel ?? "dev");
  const builtAt = environment.CMCLIENT_BUILD_TIMESTAMP?.trim();

  return {
    version: environment.CMCLIENT_BUILD_VERSION?.trim() || defaults.version,
    commit: environment.CMCLIENT_BUILD_COMMIT?.trim() || defaults.commit,
    channel: supportedChannel,
    ...(builtAt ? { builtAt } : {}),
  };
}
