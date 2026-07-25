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
          pendingStationSubmissions: 1,
          failedStationSubmissions: 0,
        }),
        outbox: async () => ({
          items: [
            {
              id: "outbox-1",
              callsign: "N0CALL-7",
              canonicalEventId: "event-1",
              status: "queued",
              deliveryStatus: "queued",
              attempts: 0,
              nextAttemptAt: "2026-07-18T00:00:00.000Z",
              createdAt: "2026-07-18T00:00:00.000Z",
              updatedAt: "2026-07-18T00:00:00.000Z",
            },
          ],
        }),
        stationSubmissions: async () => ({
          items: [
            {
              id: "aprs-igate-00000000-0000-4000-8000-000000000001",
              packetKind: "beacon",
              deliveryStatus: "submitted",
              attemptedAt: "2026-07-18T00:00:00.000Z",
              submittedAt: "2026-07-18T00:00:01.000Z",
              updatedAt: "2026-07-18T00:00:01.000Z",
              observationExpiresAt: "2026-07-18T03:00:00.000Z",
            },
          ],
        }),
      },
    });

    const aprs = useAprs();
    await aprs.refresh();

    expect(aprs.entries).toHaveLength(1);
    expect(aprs.entries[0]?.deliveryStatus).toBe("queued");
    expect(aprs.entries[0]).not.toHaveProperty("data");
    expect(aprs.stationSubmissions[0]).toMatchObject({
      packetKind: "beacon",
      deliveryStatus: "submitted",
    });
    expect(aprs.stationSubmissions[0]).not.toHaveProperty("callsign");
    expect(aprs.stationSubmissions[0]).not.toHaveProperty("info");
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
          pendingStationSubmissions: 0,
          failedStationSubmissions: 1,
          lastErrorCode: "APRS_MONITOR_CONNECT_FAILED",
        }),
        outbox: async () => {
          throw new GatewayApiError({
            code: "GATEWAY_DOMAIN_DATA_UNAVAILABLE",
          });
        },
        stationSubmissions: async () => ({ items: [] }),
      },
    } satisfies AprsClient);

    const aprs = useAprs();
    await aprs.refresh();

    expect(aprs.outboxErrorCode).toBe("GATEWAY_DOMAIN_DATA_UNAVAILABLE");
    expect(aprs.status?.lastErrorCode).toBe("APRS_MONITOR_CONNECT_FAILED");
  });

  it("retains Tracker data when only station delivery is unavailable", async () => {
    const useAprs = createAprsStore({
      aprs: {
        status: async () => ({
          configured: true,
          running: true,
          monitorStatus: "connected",
          mappedCallsigns: 1,
          pendingOutbox: 0,
          failedOutbox: 0,
          pendingStationSubmissions: 0,
          failedStationSubmissions: 0,
        }),
        outbox: async () => ({ items: [] }),
        stationSubmissions: async () => {
          throw new GatewayApiError({
            code: "GATEWAY_DOMAIN_DATA_UNAVAILABLE",
          });
        },
      },
    } satisfies AprsClient);

    const aprs = useAprs();
    await aprs.refresh();

    expect(aprs.outboxErrorCode).toBeUndefined();
    expect(aprs.stationErrorCode).toBe("GATEWAY_DOMAIN_DATA_UNAVAILABLE");
    expect(aprs.entries).toEqual([]);
  });
});
