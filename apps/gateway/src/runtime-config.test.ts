import { describe, expect, it } from "vitest";

import { DomainEventBus } from "./events";
import { GatewayDatabase } from "./persistence/database";
import {
  GatewayRuntimeConfigurationError,
  createConfiguredAprsGatewayRuntime,
  createConfiguredMeshGatewayRuntime,
  parseAprsEncodingOptions,
} from "./runtime-config";

describe("Gateway production runtime configuration", () => {
  it("keeps Meshtastic explicitly disabled unless a transport is selected", async () => {
    const database = new GatewayDatabase(":memory:");

    await expect(
      createConfiguredMeshGatewayRuntime({}, database, new DomainEventBus()),
    ).resolves.toBeUndefined();
    await expect(
      createConfiguredMeshGatewayRuntime(
        { CMCLIENT_MESHTASTIC_TRANSPORT: "socket" },
        database,
        new DomainEventBus(),
      ),
    ).rejects.toMatchObject({
      code: "MESHTASTIC_TRANSPORT_CONFIGURATION_INVALID",
    });
    database.close();
  });

  it("constructs TCP and rejects a serial runtime without a device path", async () => {
    const database = new GatewayDatabase(":memory:");
    const tcp = await createConfiguredMeshGatewayRuntime(
      {
        CMCLIENT_MESHTASTIC_TRANSPORT: "tcp",
        CMCLIENT_MESHTASTIC_TCP_HOST: "127.0.0.1",
        CMCLIENT_MESHTASTIC_TCP_PORT: "4403",
        CMCLIENT_MESH_NETWORK_ID: "fixture-network",
        CMCLIENT_GATEWAY_ID: "fixture-gateway",
      },
      database,
      new DomainEventBus(),
    );

    expect(tcp).toBeDefined();
    await expect(
      createConfiguredMeshGatewayRuntime(
        { CMCLIENT_MESHTASTIC_TRANSPORT: "serial" },
        database,
        new DomainEventBus(),
      ),
    ).rejects.toMatchObject({
      code: "MESHTASTIC_SERIAL_CONFIGURATION_INVALID",
    });
    database.close();
  });

  it("requires complete APRS credentials and accepts an explicit disable", () => {
    const database = new GatewayDatabase(":memory:");
    const events = new DomainEventBus();

    expect(
      createConfiguredAprsGatewayRuntime({}, database, events),
    ).toBeUndefined();
    expect(
      createConfiguredAprsGatewayRuntime(
        {
          CMCLIENT_APRS_ENABLED: "false",
          CMCLIENT_APRS_LOGIN_CALLSIGN: "N0CALL-7",
        },
        database,
        events,
      ),
    ).toBeUndefined();
    expect(() =>
      createConfiguredAprsGatewayRuntime(
        { CMCLIENT_APRS_ENABLED: "true" },
        database,
        events,
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "APRS_CREDENTIAL_CONFIGURATION_INVALID",
      }),
    );
    expect(
      createConfiguredAprsGatewayRuntime(
        {
          CMCLIENT_APRS_ENABLED: "true",
          CMCLIENT_APRS_LOGIN_CALLSIGN: "N0CALL-7",
          CMCLIENT_APRS_PASSCODE: "12345",
        },
        database,
        events,
      ),
    ).toBeDefined();
    database.close();
  });

  it("validates deterministic APRS encoder settings before ingest starts", () => {
    expect(parseAprsEncodingOptions({})).toEqual({
      destination: "APCM20",
      symbolCode: ">",
      symbolTable: "/",
    });
    expect(() =>
      parseAprsEncodingOptions({ CMCLIENT_APRS_COMMENT: "bad\ncomment" }),
    ).toThrowError(GatewayRuntimeConfigurationError);
  });
});
