import { afterEach, describe, expect, it, vi } from "vitest";

import { dispatchGatewayEntrypoint } from "./entrypoint";
import { runGateway } from "./runtime-main";

describe("Gateway production runtime", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("rejects direct entrypoint startup before acquiring runtime resources", async () => {
    vi.stubEnv("CMCLIENT_SUPERVISED", "0");
    vi.stubEnv("CMCLIENT_RUNTIME_PROFILE", "docker");
    vi.stubEnv("CMCLIENT_GATEWAY_HOST", "0.0.0.0");
    vi.stubEnv("CMCLIENT_GATEWAY_PORT", "8081");

    await expect(
      dispatchGatewayEntrypoint([], {
        runOfflineMaintenance: async () => undefined,
        runRuntime: runGateway,
      }),
    ).rejects.toMatchObject({ code: "GATEWAY_SUPERVISION_REQUIRED" });
  });
});
