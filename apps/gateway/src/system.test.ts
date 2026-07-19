import { describe, expect, it } from "vitest";

import { defaultGatewaySystemState, isDockerDeployment } from "./system";

describe("Docker system capability state", () => {
  it("reports only the deployment mode that the constrained image can provide", () => {
    const state = defaultGatewaySystemState({
      CMCLIENT_DEPLOYMENT_MODE: "docker",
    });

    expect(isDockerDeployment({ CMCLIENT_DEPLOYMENT_MODE: "docker" })).toBe(
      true,
    );
    expect(state.capabilities.capabilities).toMatchObject({
      docker: { available: true },
      update: {
        available: false,
        reasonCode: "CAPABILITY_UNAVAILABLE_DOCKER",
      },
      serial: {
        available: false,
        reasonCode: "CAPABILITY_UNAVAILABLE_DOCKER",
      },
      service: {
        available: false,
        reasonCode: "CAPABILITY_UNAVAILABLE_DOCKER",
      },
      autoStart: {
        available: false,
        reasonCode: "CAPABILITY_UNAVAILABLE_DOCKER",
      },
    });
  });

  it("does not infer Docker mode from the host platform", () => {
    const state = defaultGatewaySystemState();
    expect(state.build).toMatchObject({
      version: "2.0.0-rc.1",
      channel: "beta",
    });
    expect(state.capabilities.capabilities.docker).toEqual({
      available: false,
      reasonCode: "CAPABILITY_NOT_CONFIGURED",
    });
    expect(isDockerDeployment({ CMCLIENT_DEPLOYMENT_MODE: "container" })).toBe(
      false,
    );
  });

  it("rejects runtime metadata that conflicts with the compiled identity", () => {
    expect(() =>
      defaultGatewaySystemState({ CMCLIENT_BUILD_VERSION: "2.0.0" }),
    ).toThrow("BUILD_VERSION_MISMATCH");
    expect(() =>
      defaultGatewaySystemState({ CMCLIENT_BUILD_CHANNEL: "stable" }),
    ).toThrow("BUILD_CHANNEL_MISMATCH");
    expect(() =>
      defaultGatewaySystemState({ CMCLIENT_BUILD_COMMIT: "not-a-commit" }),
    ).toThrow("BUILD_COMMIT_INVALID");

    expect(
      defaultGatewaySystemState({
        CMCLIENT_BUILD_VERSION: "2.0.0-rc.1",
        CMCLIENT_BUILD_CHANNEL: "beta",
        CMCLIENT_BUILD_COMMIT: "a".repeat(40),
      }).build,
    ).toMatchObject({
      version: "2.0.0-rc.1",
      channel: "beta",
      commit: "a".repeat(40),
    });
  });
});
