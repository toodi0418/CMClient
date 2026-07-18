import { setActivePinia, createPinia } from "pinia";
import { describe, expect, it } from "vitest";

import { GatewayApiError } from "@cmclient/api-client";

import { createProxyStore } from "./proxy";

describe("proxy store", () => {
  it("loads the API projection and preserves stable errors", async () => {
    setActivePinia(createPinia());
    const store = createProxyStore({
      proxy: {
        status: async () => proxyStatusFixture(),
      },
    })();

    await store.refresh();
    expect(store.status).toMatchObject({
      state: "running",
      policy: { activeClients: 2 },
    });

    setActivePinia(createPinia());
    const unavailable = createProxyStore({
      proxy: {
        status: async () => {
          throw new GatewayApiError({ code: "PROXY_RUNTIME_UNAVAILABLE" });
        },
      },
    })();
    await unavailable.refresh();
    expect(unavailable.errorCode).toBe("PROXY_RUNTIME_UNAVAILABLE");
  });
});

function proxyStatusFixture() {
  return {
    state: "running" as const,
    listener: { host: "127.0.0.1", port: 4403 },
    policy: {
      activeClients: 2,
      allowLan: false,
      allowedAddressCount: 0,
      maxClients: 16,
      maxWritesPerMinute: 120,
      mode: "message" as const,
    },
    queue: {
      broadcastAccepted: 4,
      broadcastDropped: 1,
      broadcastFrames: 2,
      directAccepted: 1,
      directDropped: 0,
      pendingCorrelations: 0,
      queuedWrites: 0,
      writing: false,
    },
    recentAudit: [],
    upstream: {
      configFrameCount: 1,
      metrics: {
        bytesReceived: 10,
        bytesSent: 8,
        framesReceived: 2,
        framesSent: 1,
        malformedFrames: 0,
        reconnects: 0,
      },
      state: {
        changedAt: "2026-07-18T00:00:00.000Z",
        status: "ready" as const,
        transport: "tcp" as const,
      },
    },
  };
}
