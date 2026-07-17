export interface RuntimeConfig {
  managementHost: string;
  managementPort: number;
  webEnabled: boolean;
}

export type Environment = Record<string, string | undefined>;

export function parseBoolean(value: string | undefined, fallback: boolean): boolean {
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
    webEnabled: parseBoolean(environment.CMCLIENT_WEB_ENABLED, true)
  };
}
