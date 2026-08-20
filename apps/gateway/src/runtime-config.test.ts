import { describe, expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { DomainEventBus } from "./events";
import { GatewayDatabase } from "./persistence/database";
import {
  GatewayRuntimeConfigurationError,
  createConfiguredAprsGatewayRuntime,
  createConfiguredCmCloudAgentClient,
  createConfiguredCmCloudDirectAprsEgress,
  createConfiguredGatewayMaintenanceRuntime,
  createConfiguredMeshGatewayRuntime,
  parseAprsEncodingOptions,
  parseAprsEndpointOptions,
} from "./runtime-config";
import {
  CMCLOUD_AUTHORITY_REQUIRED,
  CMCLOUD_LEGACY_APRS_DISABLED,
} from "./error-codes";

const aprsState = {
  mappings: [],
  mappingsFingerprint: "a".repeat(64),
  provision: {
    callsignBase: "TEST01",
    ssid: -7,
    symbolTable: "/",
    symbolCode: ">",
  },
  provisionFingerprint: "b".repeat(64),
};

describe("Gateway production runtime configuration", () => {
  it("requires an Agent-private CMCloud credential and fences local APRS and Proxy", () => {
    const database = new GatewayDatabase(":memory:");
    const events = new DomainEventBus();
    const cloudEnvironment = {
      CMCLIENT_CMCLOUD_MODE: "required",
      CMCLIENT_CMCLOUD_URL: "wss://cmcloud.tmmarc.org/agent/v1",
      CMCLIENT_CMCLOUD_INSTALLATION_ID: "00000000-0000-4000-8000-000000000001",
      CMCLIENT_CMCLOUD_INSTALLATION_GENERATION: "0",
      CMCLIENT_CMCLOUD_CREDENTIAL_VERSION: "1",
    };

    expect(() =>
      createConfiguredCmCloudAgentClient(
        cloudEnvironment,
        database,
        events,
        "2.0.0",
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "CMCLOUD_CREDENTIAL_BOOTSTRAP_REQUIRED",
      }),
    );
    expect(() =>
      createConfiguredCmCloudAgentClient(
        { ...cloudEnvironment, CMCLIENT_APRS_ENABLED: "true" },
        database,
        events,
        "2.0.0",
        "credential_value_that_is_long_enough",
      ),
    ).toThrowError(
      expect.objectContaining({ code: "CMCLOUD_DIRECT_APRS_FORBIDDEN" }),
    );
    expect(() =>
      createConfiguredCmCloudAgentClient(
        { ...cloudEnvironment, CMCLIENT_PROXY_ENABLED: "true" },
        database,
        events,
        "2.0.0",
        "credential_value_that_is_long_enough",
      ),
    ).toThrowError(
      expect.objectContaining({ code: "CMCLOUD_PROXY_FORBIDDEN" }),
    );
    expect(() =>
      createConfiguredCmCloudAgentClient(
        { ...cloudEnvironment, CMCLIENT_APRS_LOGIN_CALLSIGN: "BM5GSV-5" },
        database,
        events,
        "2.0.0",
        "credential_value_that_is_long_enough",
      ),
    ).toThrowError(
      expect.objectContaining({ code: "APRS_STATIC_IDENTITY_FORBIDDEN" }),
    );
    expect(
      createConfiguredCmCloudAgentClient(
        cloudEnvironment,
        database,
        events,
        "2.0.0",
        "credential_value_that_is_long_enough",
      )?.status(),
    ).toMatchObject({ configured: true, state: "stopped", pendingOutbox: 0 });
    database.close();
  });

  it("constructs CMCloud direct APRS egress without a local callsign and validates its opt-out", () => {
    expect(createConfiguredCmCloudDirectAprsEgress({})).toBeDefined();
    expect(
      createConfiguredCmCloudDirectAprsEgress({
        CMCLIENT_CMCLOUD_DIRECT_APRS_ENABLED: "false",
      }),
    ).toBeUndefined();
    expect(() =>
      createConfiguredCmCloudDirectAprsEgress({
        CMCLIENT_CMCLOUD_DIRECT_APRS_ENABLED: "maybe",
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "CMCLOUD_DIRECT_APRS_ENABLED_CONFIGURATION_INVALID",
      }),
    );
  });

  it("rejects invalid bounded maintenance retention settings", () => {
    const database = new GatewayDatabase(":memory:");
    const events = new DomainEventBus();

    expect(() =>
      createConfiguredGatewayMaintenanceRuntime(
        { CMCLIENT_APRS_OUTBOX_RETENTION_BATCH_SIZE: "0" },
        database,
        events,
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "APRS_OUTBOX_RETENTION_CONFIGURATION_INVALID",
      }),
    );
    expect(() =>
      createConfiguredGatewayMaintenanceRuntime(
        { CMCLIENT_JOB_RETENTION_DAYS: "not-a-number" },
        database,
        events,
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "JOB_RETENTION_CONFIGURATION_INVALID",
      }),
    );
    expect(() =>
      createConfiguredGatewayMaintenanceRuntime(
        { CMCLIENT_MESSAGE_RETENTION_DAYS: "0" },
        database,
        events,
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "MESSAGE_RETENTION_CONFIGURATION_INVALID",
      }),
    );
    expect(() =>
      createConfiguredGatewayMaintenanceRuntime(
        { CMCLIENT_POSITION_RETENTION_BATCH_SIZE: "not-a-number" },
        database,
        events,
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "POSITION_RETENTION_CONFIGURATION_INVALID",
      }),
    );
    expect(() =>
      createConfiguredGatewayMaintenanceRuntime(
        { CMCLIENT_CMCLOUD_OUTBOX_RETENTION_DAYS: "0" },
        database,
        events,
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "CMCLOUD_OUTBOX_RETENTION_CONFIGURATION_INVALID",
      }),
    );
    expect(() =>
      createConfiguredGatewayMaintenanceRuntime(
        {
          CMCLIENT_TELEMETRY_RETENTION_BATCH_SIZE: "17",
          CMCLIENT_MESSAGE_RETENTION_BATCH_SIZE: "64",
          CMCLIENT_POSITION_RETENTION_BATCH_SIZE: "63",
          CMCLIENT_OBSERVATION_RETENTION_BATCH_SIZE: "1143",
        },
        database,
        events,
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "OBSERVATION_RETENTION_CONFIGURATION_INVALID",
      }),
    );
    database.close();
  });

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

  it("requires the physical profile to be supervised and campaign-scoped", async () => {
    const database = new GatewayDatabase(":memory:");
    const directory = await mkdtemp(
      join(tmpdir(), "cmclient-runtime-physical-"),
    );
    const base = {
      CMCLIENT_MESHTASTIC_TRANSPORT: "tcp",
      CMCLIENT_MESHTASTIC_TCP_HOST: "127.0.0.1",
      CMCLIENT_MESHTASTIC_PHYSICAL_PROFILE: "true",
      CMCLIENT_BUILD_COMMIT: "a".repeat(40),
      CMCLIENT_BUILD_TREE: "b".repeat(40),
      CMCLIENT_QUALIFICATION_STAGE: "windows-source-smoke",
      CMCLIENT_TEST_MODE: "1",
      CMCLIENT_RUNTIME_ROOT: directory,
    };
    try {
      await expect(
        createConfiguredMeshGatewayRuntime(
          base,
          database,
          new DomainEventBus(),
        ),
      ).rejects.toMatchObject({
        code: "PHYSICAL_PROFILE_CONFIGURATION_INVALID",
      });
      await expect(
        createConfiguredMeshGatewayRuntime(
          { ...base, CMCLIENT_SUPERVISED: "1" },
          database,
          new DomainEventBus(),
        ),
      ).resolves.toBeDefined();
    } finally {
      database.close();
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("rejects legacy APRS runtime without CMCloud authority", () => {
    const database = new GatewayDatabase(":memory:");
    const events = new DomainEventBus();

    expect(createConfiguredAprsGatewayRuntime({}, database, events)).toBeUndefined();
    expect(() =>
      createConfiguredAprsGatewayRuntime(
        {
          CMCLIENT_APRS_ENABLED: "true",
        },
        database,
        events,
      ),
    ).toThrowError(
      expect.objectContaining({ code: CMCLOUD_AUTHORITY_REQUIRED }),
    );
    expect(() =>
      createConfiguredAprsGatewayRuntime(
        {
          CMCLIENT_CMCLOUD_MODE: "required",
          CMCLIENT_CMCLOUD_URL: "wss://cmcloud.tmmarc.org/agent/v1",
          CMCLIENT_CMCLOUD_INSTALLATION_ID: "00000000-0000-4000-8000-000000000001",
          CMCLIENT_CMCLOUD_INSTALLATION_GENERATION: "0",
          CMCLIENT_CMCLOUD_CREDENTIAL_VERSION: "1",
          CMCLIENT_APRS_ENABLED: "true",
        },
        database,
        events,
        () => aprsState,
      ),
    ).toThrowError(
      expect.objectContaining({ code: CMCLOUD_LEGACY_APRS_DISABLED }),
    );
    expect(() =>
      createConfiguredAprsGatewayRuntime(
        {
          CMCLIENT_CMCLOUD_MODE: "required",
          CMCLIENT_CMCLOUD_URL: "wss://cmcloud.tmmarc.org/agent/v1",
          CMCLIENT_CMCLOUD_INSTALLATION_ID: "00000000-0000-4000-8000-000000000001",
          CMCLIENT_CMCLOUD_INSTALLATION_GENERATION: "0",
          CMCLIENT_CMCLOUD_CREDENTIAL_VERSION: "1",
          CMCLIENT_APRS_ENABLED: "false",
          CMCLIENT_APRS_LOGIN_CALLSIGN: "N0CALL-7",
        },
        database,
        events,
        () => aprsState,
      ),
    ).toThrowError(
      expect.objectContaining({ code: "APRS_STATIC_IDENTITY_FORBIDDEN" }),
    );
    database.close();
  });

  it("validates deterministic APRS encoder settings before ingest starts", () => {
    expect(parseAprsEncodingOptions({})).toEqual({});
    expect(() =>
      parseAprsEncodingOptions({ CMCLIENT_APRS_DESTINATION: "APTMAG" }),
    ).toThrowError(GatewayRuntimeConfigurationError);
    expect(parseAprsEndpointOptions({})).toEqual({
      host: "asia.aprs2.net",
      port: 14_580,
      timeoutMs: 10_000,
    });
  });
});
