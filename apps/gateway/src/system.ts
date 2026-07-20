import {
  ProductIdentitySchema,
  type ComponentIdentityReport,
  type ProductIdentity,
  type SystemCapabilities,
} from "@cmclient/contracts";
import { Value } from "@sinclair/typebox/value";

const COMPILED_BUILD_VERSION = "2.0.0-rc.1";
const UNBOUND_WORKSPACE_OBJECT_ID = "0".repeat(40);

export function compiledGatewayBuildVersion(): string {
  return COMPILED_BUILD_VERSION;
}

export interface GatewaySystemState {
  identity: ComponentIdentityReport;
  capabilities: SystemCapabilities;
}

export function isDockerDeployment(
  environment: NodeJS.ProcessEnv = process.env,
): boolean {
  return (
    environment.CMCLIENT_RUNTIME_PROFILE?.trim().toLowerCase() === "docker"
  );
}

export function defaultGatewaySystemState(
  environment: NodeJS.ProcessEnv = process.env,
): GatewaySystemState {
  const injectedVersion = environment.CMCLIENT_BUILD_VERSION?.trim();
  if (injectedVersion && injectedVersion !== COMPILED_BUILD_VERSION) {
    throw new Error("BUILD_VERSION_MISMATCH");
  }

  const supervised = environment.CMCLIENT_SUPERVISED?.trim() === "1";
  const sourceCommit = requiredGitObject(
    environment.CMCLIENT_BUILD_COMMIT,
    supervised,
    "BUILD_COMMIT",
  );
  const sourceTree = requiredSourceTree(
    environment.CMCLIENT_BUILD_TREE,
    supervised,
    "BUILD_TREE",
  );
  const channel = environment.CMCLIENT_BUILD_CHANNEL?.trim() || "dev";
  const profile = environment.CMCLIENT_RUNTIME_PROFILE?.trim() || "native";
  const packageProfile =
    environment.CMCLIENT_PACKAGE_PROFILE?.trim() || "workspace";
  const os =
    environment.CMCLIENT_TARGET_OS?.trim() || platformOperatingSystem();
  const architecture =
    environment.CMCLIENT_TARGET_ARCHITECTURE?.trim() || platformArchitecture();

  const productIdentity = {
    schemaVersion: 1,
    product: "CMClient",
    version: COMPILED_BUILD_VERSION,
    sourceCommit,
    sourceTree,
    channel,
    target: { os, architecture, profile, packageProfile },
  };
  if (!Value.Check(ProductIdentitySchema, productIdentity)) {
    throw new Error("BUILD_IDENTITY_INVALID");
  }
  const identity: ComponentIdentityReport = {
    schemaVersion: 1,
    component: "gateway",
    identity: productIdentity as ProductIdentity,
  };
  const docker = identity.identity.target.profile === "docker";

  return {
    identity,
    capabilities: {
      schemaVersion: 2,
      identity,
      capabilities: {
        managementWeb: { available: false, reasonCode: "owned_by_agent" },
        commandMode: { available: false, reasonCode: "owned_by_agent" },
        graphicalMode: docker
          ? { available: false, reasonCode: "unavailable_in_docker" }
          : { available: false, reasonCode: "owned_by_graphical_mode" },
        loginAutostart: docker
          ? { available: false, reasonCode: "unavailable_in_docker" }
          : { available: false, reasonCode: "owned_by_agent" },
        serial: docker
          ? { available: false, reasonCode: "unavailable_in_docker" }
          : { available: false, reasonCode: "not_configured" },
        nativeUpdate: docker
          ? { available: false, reasonCode: "unavailable_in_docker" }
          : { available: false, reasonCode: "owned_by_agent" },
        dockerPullRecreateUpdate: docker
          ? { available: true }
          : { available: false, reasonCode: "unavailable_in_native" },
        localControl: { available: false, reasonCode: "owned_by_agent" },
        remoteDispatch: { available: false, reasonCode: "not_enabled" },
      },
    },
  };
}

function requiredGitObject(
  input: string | undefined,
  required: boolean,
  field: string,
): string {
  const value = input?.trim();
  if (!value) {
    if (required) {
      throw new Error(`${field}_MISSING`);
    }
    return UNBOUND_WORKSPACE_OBJECT_ID;
  }
  if (!/^[a-f0-9]{40}$/.test(value)) {
    throw new Error(`${field}_INVALID`);
  }
  return value;
}

function requiredSourceTree(
  input: string | undefined,
  required: boolean,
  field: string,
): string {
  const value = input?.trim();
  if (!value) {
    if (required) {
      throw new Error(`${field}_MISSING`);
    }
    return UNBOUND_WORKSPACE_OBJECT_ID;
  }
  if (!/^(?:[a-f0-9]{40}|sha256:[a-f0-9]{64})$/.test(value)) {
    throw new Error(`${field}_INVALID`);
  }
  return value;
}

function platformOperatingSystem(): string {
  switch (process.platform) {
    case "win32":
      return "windows";
    case "darwin":
      return "macos";
    case "linux":
      return "linux";
    default:
      return "unsupported";
  }
}

function platformArchitecture(): string {
  switch (process.arch) {
    case "x64":
      return "x86_64";
    case "arm64":
      return "aarch64";
    default:
      return "unsupported";
  }
}
