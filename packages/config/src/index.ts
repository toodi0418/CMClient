import {
  ProductIdentitySchema,
  type ProductIdentity,
  type ProductTarget,
  type ReleaseChannel,
} from "@cmclient/contracts";
import { Value } from "@sinclair/typebox/value";

export interface RuntimeConfig {
  managementHost: string;
  managementPort: number;
  webEnabled: boolean;
}

export type Environment = Record<string, string | undefined>;

export interface ProductIdentityDefaults {
  version: string;
  sourceCommit: string;
  sourceTree: string;
  channel: ReleaseChannel;
  target: ProductTarget;
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

export function parseProductIdentity(
  environment: Environment,
  defaults: ProductIdentityDefaults,
): ProductIdentity {
  const identity = {
    schemaVersion: 1,
    product: "CMClient",
    version: environment.CMCLIENT_BUILD_VERSION?.trim() || defaults.version,
    sourceCommit:
      environment.CMCLIENT_BUILD_COMMIT?.trim() || defaults.sourceCommit,
    sourceTree: environment.CMCLIENT_BUILD_TREE?.trim() || defaults.sourceTree,
    channel: environment.CMCLIENT_BUILD_CHANNEL?.trim() || defaults.channel,
    target: {
      os: environment.CMCLIENT_TARGET_OS?.trim() || defaults.target.os,
      architecture:
        environment.CMCLIENT_TARGET_ARCHITECTURE?.trim() ||
        defaults.target.architecture,
      profile:
        environment.CMCLIENT_RUNTIME_PROFILE?.trim() || defaults.target.profile,
      packageProfile:
        environment.CMCLIENT_PACKAGE_PROFILE?.trim() ||
        defaults.target.packageProfile,
    },
  };
  if (!Value.Check(ProductIdentitySchema, identity)) {
    throw new Error("BUILD_IDENTITY_INVALID");
  }
  return identity as ProductIdentity;
}
