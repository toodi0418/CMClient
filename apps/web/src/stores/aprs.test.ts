import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";

import { GatewayApiError } from "@cmclient/api-client";

import { createAprsStore, type AprsClient } from "./aprs";

describe("APRS store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  it("reads public outbox state without accepting APRS Data", async () => {
    const useAprs = createAprsStore({
      aprs: {
        status: async () => ({
          configured: true,
          running: true,
          monitorStatus: "connected",
          mappedCallsigns: 1,
          pendingOutbox: 1,
          failedOutbox: 0,
        }),
        outbox: async () => ({
          items: [
            {
              id: "outbox-1",
              callsign: "N0CALL-7",
              canonicalEventId: "event-1",
              status: "queued",
              attempts: 0,
              nextAttemptAt: "2026-07-18T00:00:00.000Z",
              createdAt: "2026-07-18T00:00:00.000Z",
              updatedAt: "2026-07-18T00:00:00.000Z",
            },
          ],
        }),
      },
    });

    const aprs = useAprs();
    await aprs.refresh();

    expect(aprs.entries).toHaveLength(1);
    expect(aprs.entries[0]).not.toHaveProperty("data");
    expect(aprs.status?.monitorStatus).toBe("connected");
  });

  it("keeps a stable error code when the outbox projection is unavailable", async () => {
    const useAprs = createAprsStore({
      aprs: {
        status: async () => ({
          configured: true,
          running: true,
          monitorStatus: "error",
          mappedCallsigns: 1,
          pendingOutbox: 0,
          failedOutbox: 1,
          lastErrorCode: "APRS_MONITOR_CONNECT_FAILED",
        }),
        outbox: async () => {
          throw new GatewayApiError({
            code: "GATEWAY_DOMAIN_DATA_UNAVAILABLE",
          });
        },
      },
    } satisfies AprsClient);

    const aprs = useAprs();
    await aprs.refresh();

    expect(aprs.outboxErrorCode).toBe("GATEWAY_DOMAIN_DATA_UNAVAILABLE");
    expect(aprs.status?.lastErrorCode).toBe("APRS_MONITOR_CONNECT_FAILED");
  });
});
