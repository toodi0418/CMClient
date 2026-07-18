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
    expect(
      defaultGatewaySystemState().capabilities.capabilities.docker,
    ).toEqual({
      available: false,
      reasonCode: "CAPABILITY_NOT_CONFIGURED",
    });
    expect(isDockerDeployment({ CMCLIENT_DEPLOYMENT_MODE: "container" })).toBe(
      false,
    );
  });
});
