import { afterEach, describe, expect, it, vi } from "vitest";

import { dispatchGatewayEntrypoint } from "./entrypoint";
import {
  runGateway,
  startGatewayExternalRuntimes,
  validateProxyUpstreamConfiguration,
} from "./runtime-main";

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

  it("authenticates setup without starting operational transports", async () => {
    const calls: string[] = [];
    await startGatewayExternalRuntimes(true, {
      validateMeshtastic: async () => {
        calls.push("meshtastic");
      },
      validateCallMesh: async () => {
        calls.push("validate");
      },
      synchronizeCallMesh: async () => {
        calls.push("synchronize");
      },
      startProxy: async () => {
        calls.push("proxy");
      },
      startMaintenance: () => calls.push("maintenance"),
      startAprs: () => calls.push("aprs"),
      startMesh: () => calls.push("mesh"),
      throwIfShutdownRequested: () => calls.push("fence"),
    });

    expect(calls).toEqual(["meshtastic", "fence", "validate"]);
  });

  it("does not transmit a CallMesh credential when Meshtastic validation fails", async () => {
    let callmeshValidated = false;
    await expect(
      startGatewayExternalRuntimes(true, {
        validateMeshtastic: async () => {
          throw new Error("fixture endpoint is not Meshtastic");
        },
        validateCallMesh: async () => {
          callmeshValidated = true;
        },
        synchronizeCallMesh: async () => undefined,
        startProxy: async () => undefined,
        startMaintenance: () => undefined,
        startAprs: () => undefined,
        startMesh: () => undefined,
        throwIfShutdownRequested: () => undefined,
      }),
    ).rejects.toThrow("fixture endpoint is not Meshtastic");
    expect(callmeshValidated).toBe(false);
  });

  it("starts normal transports after synchronization without setup revalidation", async () => {
    const calls: string[] = [];
    await startGatewayExternalRuntimes(false, {
      validateMeshtastic: async () => {
        throw new Error("normal runtime must not use setup validation");
      },
      validateCallMesh: async () => {
        throw new Error("normal runtime must not use setup validation");
      },
      synchronizeCallMesh: async () => {
        calls.push("synchronize");
      },
      startProxy: async () => {
        calls.push("proxy");
      },
      startMaintenance: () => calls.push("maintenance"),
      startAprs: () => calls.push("aprs"),
      startMesh: () => calls.push("mesh"),
      throwIfShutdownRequested: () => calls.push("fence"),
    });

    expect(calls).toEqual([
      "synchronize",
      "fence",
      "proxy",
      "fence",
      "maintenance",
      "fence",
      "aprs",
      "fence",
      "mesh",
      "fence",
    ]);
  });

  it.each(["tcp", "serial"])(
    "rejects a dedicated Proxy upstream while %s ingest is configured",
    (transport) => {
      expect(() =>
        validateProxyUpstreamConfiguration({
          CMCLIENT_PROXY_ENABLED: "true",
          CMCLIENT_MESHTASTIC_TRANSPORT: transport,
        }),
      ).toThrowError(
        expect.objectContaining({
          code: "PROXY_SECOND_UPSTREAM_FORBIDDEN",
        }),
      );
    },
  );

  it.each([undefined, "", "disabled", " DISABLED "])(
    "allows one Proxy-owned upstream when Mesh ingest is %s",
    (transport) => {
      expect(
        validateProxyUpstreamConfiguration({
          CMCLIENT_PROXY_ENABLED: "true",
          CMCLIENT_MESHTASTIC_TRANSPORT: transport,
        }),
      ).toBe(true);
    },
  );

  it.each([undefined, "", "false", " FALSE "])(
    "does not configure a Proxy transport when enablement is %s",
    (enabled) => {
      expect(
        validateProxyUpstreamConfiguration({
          CMCLIENT_PROXY_ENABLED: enabled,
          CMCLIENT_MESHTASTIC_TRANSPORT: "tcp",
        }),
      ).toBe(false);
    },
  );
});
