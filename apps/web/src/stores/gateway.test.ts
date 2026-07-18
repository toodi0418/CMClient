import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";

import { GatewayApiError } from "@cmclient/api-client";
import type {
  DomainEvent,
  SystemCapabilities,
  SystemStatus,
} from "@cmclient/contracts";

import { createGatewayStore, type GatewayClients } from "./gateway";

const status: SystemStatus = {
  health: "ok",
  build: { version: "2.0.0-dev.0", commit: "fixture", channel: "dev" },
};

const capabilities: SystemCapabilities = {
  schemaVersion: 1,
  platform: "linux",
  build: status.build,
  capabilities: {
    managementWeb: { available: true },
    update: { available: false, reasonCode: "CAPABILITY_OWNED_BY_AGENT" },
    tray: { available: false, reasonCode: "CAPABILITY_OWNED_BY_DESKTOP" },
    serial: { available: false, reasonCode: "CAPABILITY_NOT_CONFIGURED" },
    service: { available: false, reasonCode: "CAPABILITY_OWNED_BY_AGENT" },
    autoStart: { available: false, reasonCode: "CAPABILITY_OWNED_BY_AGENT" },
    docker: { available: true },
  },
};

describe("gateway store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  it("hydrates Gateway projections and tracks event connection state", async () => {
    const fake = createFakeClients();
    const useGateway = createGatewayStore(fake.clients);
    const gateway = useGateway();

    await gateway.initialize();
    fake.emitState("open");
    fake.emitEvent({ type: "gateway.ready" });
    fake.emitError(
      new GatewayApiError({ code: "GATEWAY_NETWORK_UNAVAILABLE" }),
    );

    expect(gateway.availability).toBe("available");
    expect(gateway.status).toEqual(status);
    expect(gateway.capabilities?.capabilities.serial).toEqual({
      available: false,
      reasonCode: "CAPABILITY_NOT_CONFIGURED",
    });
    expect(gateway.eventConnection).toBe("open");
    expect(gateway.lastEventType).toBe("gateway.ready");
    expect(gateway.recentEvents).toMatchObject([
      { eventId: "event-fixture", type: "gateway.ready" },
    ]);
    expect(gateway.errorCode).toBeUndefined();
    expect(gateway.eventErrorCode).toBe("GATEWAY_NETWORK_UNAVAILABLE");
  });

  it("keeps a stable error code when Gateway projections are unavailable", async () => {
    const fake = createFakeClients({
      status: async () => {
        throw new GatewayApiError({ code: "GATEWAY_PROXY_UNAVAILABLE" });
      },
    });
    const useGateway = createGatewayStore(fake.clients);
    const gateway = useGateway();

    await gateway.initialize();

    expect(gateway.availability).toBe("unavailable");
    expect(gateway.errorCode).toBe("GATEWAY_PROXY_UNAVAILABLE");
  });
});

function createFakeClients(
  overrides: { status?: () => Promise<SystemStatus> } = {},
) {
  let eventListener: ((event: DomainEvent) => void) | undefined;
  let stateListener:
    | ((
        state: "idle" | "connecting" | "open" | "reconnecting" | "stopped",
      ) => void)
    | undefined;
  let errorListener: ((error: unknown) => void) | undefined;

  const clients: GatewayClients = {
    api: {
      system: {
        health: async () => ({ status: "ok" }),
        version: async () => status.build,
        status: overrides.status ?? (async () => status),
        capabilities: async () => capabilities,
      },
    },
    events: {
      start() {},
      stop() {},
      onEvent(listener) {
        eventListener = listener;
        return () => undefined;
      },
      onError(listener) {
        errorListener = listener;
        return () => undefined;
      },
      onStateChange(listener) {
        stateListener = listener;
        return () => undefined;
      },
    },
  };

  return {
    clients,
    emitEvent(input: Pick<DomainEvent, "type">) {
      eventListener?.({
        eventId: "event-fixture",
        schemaVersion: 1,
        type: input.type,
        occurredAt: "2026-07-18T00:00:00.000Z",
        source: "gateway",
        payload: {},
      });
    },
    emitState(
      state: "idle" | "connecting" | "open" | "reconnecting" | "stopped",
    ) {
      stateListener?.(state);
    },
    emitError(error: unknown) {
      errorListener?.(error);
    },
  };
}
