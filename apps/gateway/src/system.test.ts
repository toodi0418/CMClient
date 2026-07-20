import { describe, expect, it } from "vitest";

import { defaultGatewaySystemState, isDockerDeployment } from "./system";

const exactSource = {
  CMCLIENT_BUILD_VERSION: "2.0.0-rc.1",
  CMCLIENT_BUILD_COMMIT: "a".repeat(40),
  CMCLIENT_BUILD_TREE: "b".repeat(40),
  CMCLIENT_BUILD_CHANNEL: "candidate",
};

describe("unified system identity and capabilities", () => {
  it("reports the Docker profile without pretending native controls are broken", () => {
    const environment = {
      ...exactSource,
      CMCLIENT_RUNTIME_PROFILE: "docker",
      CMCLIENT_PACKAGE_PROFILE: "oci",
      CMCLIENT_TARGET_OS: "linux",
      CMCLIENT_TARGET_ARCHITECTURE: "x86_64",
    };
    const state = defaultGatewaySystemState(environment);

    expect(isDockerDeployment(environment)).toBe(true);
    expect(state.identity.identity.target).toEqual({
      os: "linux",
      architecture: "x86_64",
      profile: "docker",
      packageProfile: "oci",
    });
    expect(state.capabilities.identity).toEqual(state.identity);
    expect(state.capabilities.capabilities).toMatchObject({
      dockerPullRecreateUpdate: { available: true },
      nativeUpdate: {
        available: false,
        reasonCode: "unavailable_in_docker",
      },
      serial: {
        available: false,
        reasonCode: "unavailable_in_docker",
      },
      graphicalMode: {
        available: false,
        reasonCode: "unavailable_in_docker",
      },
      loginAutostart: {
        available: false,
        reasonCode: "unavailable_in_docker",
      },
    });
  });

  it("uses an explicit runtime profile instead of an old deployment selector", () => {
    const state = defaultGatewaySystemState();
    expect(state.identity.identity).toMatchObject({
      product: "CMClient",
      version: "2.0.0-rc.1",
      channel: "dev",
      target: { profile: "native", packageProfile: "workspace" },
    });
    expect(state.capabilities.capabilities.dockerPullRecreateUpdate).toEqual({
      available: false,
      reasonCode: "unavailable_in_native",
    });
    expect(isDockerDeployment({ CMCLIENT_DEPLOYMENT_MODE: "docker" })).toBe(
      false,
    );
  });

  it("rejects incomplete, legacy, or conflicting identity inputs", () => {
    expect(() =>
      defaultGatewaySystemState({ CMCLIENT_BUILD_VERSION: "2.0.0" }),
    ).toThrow("BUILD_VERSION_MISMATCH");
    expect(() =>
      defaultGatewaySystemState({ CMCLIENT_BUILD_CHANNEL: "beta" }),
    ).toThrow("BUILD_IDENTITY_INVALID");
    expect(() =>
      defaultGatewaySystemState({ CMCLIENT_BUILD_COMMIT: "not-a-commit" }),
    ).toThrow("BUILD_COMMIT_INVALID");
    expect(() =>
      defaultGatewaySystemState({
        CMCLIENT_SUPERVISED: "1",
        ...exactSource,
        CMCLIENT_BUILD_TREE: undefined,
      }),
    ).toThrow("BUILD_TREE_MISSING");
    expect(() =>
      defaultGatewaySystemState({
        ...exactSource,
        CMCLIENT_RUNTIME_PROFILE: "docker",
        CMCLIENT_PACKAGE_PROFILE: "setup",
        CMCLIENT_TARGET_OS: "windows",
        CMCLIENT_TARGET_ARCHITECTURE: "x86_64",
      }),
    ).toThrow("BUILD_IDENTITY_INVALID");
  });
});
