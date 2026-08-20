import { afterEach, describe, expect, it, vi } from "vitest";

import { dispatchGatewayEntrypoint } from "./entrypoint";
import {
  createConfiguredGatewayCallMeshClient,
  isCmCloudAuthorityRequired,
  runGateway,
  startGatewayExternalRuntimes,
  validateProxyUpstreamConfiguration,
} from "./runtime-main";
import { CMCLOUD_AUTHORITY_REQUIRED } from "./error-codes";

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
      useCmCloud: true,
      validateMeshtastic: async () => {
        calls.push("meshtastic");
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

    expect(calls).toEqual(["meshtastic", "fence"]);
  });

  it("does not transmit a CallMesh credential when Meshtastic validation fails", async () => {
    let callmeshValidated = false;
    await expect(
      startGatewayExternalRuntimes(true, {
        useCmCloud: true,
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

  it("does not validate CallMesh during CMCloud setup validation", async () => {
    const calls: string[] = [];
    await startGatewayExternalRuntimes(true, {
      useCmCloud: true,
      validateMeshtastic: async () => {
        calls.push("meshtastic");
      },
      startProxy: async () => undefined,
      startMaintenance: () => undefined,
      startAprs: () => undefined,
      startMesh: () => undefined,
      throwIfShutdownRequested: () => calls.push("fence"),
    });

    expect(calls).toEqual(["meshtastic", "fence"]);
  });

  it("does not construct a CallMesh client in required CMCloud mode", () => {
    const environment = {
      CMCLIENT_CMCLOUD_MODE: "required",
      CMCLIENT_CMCLOUD_URL: "wss://cmcloud.tmmarc.org/agent/v1",
      CMCLIENT_CMCLOUD_INSTALLATION_ID: "00000000-0000-4000-8000-000000000001",
      CMCLIENT_CMCLOUD_INSTALLATION_GENERATION: "0",
      CMCLIENT_CMCLOUD_CREDENTIAL_VERSION: "1",
      CMCLIENT_CALLMESH_URL: "not-a-valid-callmesh-url",
    };
    const factory = vi.fn(() => {
      throw new Error("CallMesh client must not be constructed");
    });

    const useCmCloud = isCmCloudAuthorityRequired(environment);
    const client = createConfiguredGatewayCallMeshClient(
      useCmCloud,
      environment,
      "2.0.0",
      undefined,
      undefined,
      factory,
    );

    expect(useCmCloud).toBe(true);
    expect(client).toBeUndefined();
    expect(factory).not.toHaveBeenCalled();

    const legacyClient = createConfiguredGatewayCallMeshClient(
      false,
      {
        CMCLIENT_CALLMESH_URL: "https://callmesh.tmmarc.org",
      },
      "2.0.0",
      "legacy-key",
      undefined,
      factory,
    );
    expect(legacyClient).toBeUndefined();
    expect(factory).not.toHaveBeenCalled();
  });

  it("starts normal transports after synchronization without setup revalidation", async () => {
    const calls: string[] = [];
    await startGatewayExternalRuntimes(false, {
      validateMeshtastic: async () => {
        throw new Error("normal runtime must not use setup validation");
      },
      useCmCloud: true,
      startCmCloud: () => calls.push("cmcloud"),
      startProxy: async () => {
        calls.push("proxy");
      },
      startMaintenance: () => calls.push("maintenance"),
      startAprs: () => calls.push("aprs"),
      startMesh: () => calls.push("mesh"),
      throwIfShutdownRequested: () => calls.push("fence"),
    });

    expect(calls).toEqual([
      "cmcloud",
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

  it("starts the CMCloud upstream instead of synchronizing CallMesh", async () => {
    const calls: string[] = [];
    await startGatewayExternalRuntimes(false, {
      validateMeshtastic: async () => undefined,
      startCmCloud: () => calls.push("cmcloud"),
      useCmCloud: true,
      startProxy: async () => {
        calls.push("proxy");
      },
      startMaintenance: () => calls.push("maintenance"),
      startAprs: () => calls.push("aprs"),
      startMesh: () => calls.push("mesh"),
      throwIfShutdownRequested: () => calls.push("fence"),
    });

    expect(calls).toEqual([
      "cmcloud",
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

  it("fails closed when CMCloud mode has no transport starter", async () => {
    await expect(
      startGatewayExternalRuntimes(false, {
        validateMeshtastic: async () => undefined,
        useCmCloud: true,
        startProxy: async () => undefined,
        startMaintenance: () => undefined,
        startAprs: () => undefined,
        startMesh: () => undefined,
        throwIfShutdownRequested: () => undefined,
      }),
    ).rejects.toMatchObject({ code: "CMCLOUD_START_CONFIGURATION_INVALID" });
  });

  it("rejects legacy non-CMCloud startup before any upstream is started", async () => {
    await expect(
      startGatewayExternalRuntimes(false, {
        validateMeshtastic: async () => undefined,
        synchronizeCallMesh: async () => {
          throw new Error("legacy CallMesh must not run");
        },
        startProxy: async () => undefined,
        startMaintenance: () => undefined,
        startAprs: () => undefined,
        startMesh: () => undefined,
        throwIfShutdownRequested: () => undefined,
      }),
    ).rejects.toMatchObject({ code: CMCLOUD_AUTHORITY_REQUIRED });
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
