import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";

import { GatewayApiError } from "@cmclient/api-client";

import { createMeshtasticStore, type MeshtasticClient } from "./meshtastic";

describe("Meshtastic store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  it("reads the transport connection and bounded metrics projection", async () => {
    const useMeshtastic = createMeshtasticStore({
      meshtastic: {
        status: async () => ({
          configured: true,
          meshNetworkId: "mesh-a",
          gatewayId: "gateway-a",
          connection: {
            transport: "tcp",
            status: "ready",
            changedAt: "2026-07-18T00:00:00.000Z",
          },
          metrics: {
            bytesReceived: 64,
            bytesSent: 32,
            framesReceived: 4,
            framesSent: 2,
            malformedFrames: 0,
            reconnects: 1,
          },
        }),
      },
    });

    const meshtastic = useMeshtastic();
    await meshtastic.refresh();

    expect(meshtastic.status).toMatchObject({
      configured: true,
      connection: { transport: "tcp", status: "ready" },
      metrics: { framesReceived: 4, reconnects: 1 },
    });
    expect(meshtastic.errorCode).toBeUndefined();
  });

  it("keeps a stable error code when the runtime projection fails", async () => {
    const useMeshtastic = createMeshtasticStore({
      meshtastic: {
        status: async () => {
          throw new GatewayApiError({ code: "GATEWAY_NETWORK_UNAVAILABLE" });
        },
      },
    } satisfies MeshtasticClient);

    const meshtastic = useMeshtastic();
    await meshtastic.refresh();

    expect(meshtastic.errorCode).toBe("GATEWAY_NETWORK_UNAVAILABLE");
  });
});
