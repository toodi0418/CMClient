import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";

import { GatewayApiError } from "@cmclient/api-client";

import { createDomainStore, type DomainClient } from "./domain";

describe("domain store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  it("hydrates every bounded domain projection together", async () => {
    const useDomain = createDomainStore(fakeClient());
    const domain = useDomain();

    await domain.refresh();

    expect(domain.nodes).toHaveLength(1);
    expect(domain.messages).toHaveLength(1);
    expect(domain.telemetry).toHaveLength(1);
    expect(domain.positions).toHaveLength(1);
    expect(domain.errorCode).toBeUndefined();
  });

  it("retains a stable error code when a projection request fails", async () => {
    const useDomain = createDomainStore({
      domain: {
        nodes: async () => {
          throw new GatewayApiError({
            code: "GATEWAY_DOMAIN_DATA_UNAVAILABLE",
          });
        },
        messages: async () => ({ items: [] }),
        telemetry: async () => ({ items: [] }),
        positions: async () => ({ items: [] }),
      },
    });
    const domain = useDomain();

    await domain.refresh();

    expect(domain.errorCode).toBe("GATEWAY_DOMAIN_DATA_UNAVAILABLE");
  });
});

function fakeClient(): DomainClient {
  return {
    domain: {
      nodes: async () => ({
        items: [
          {
            schemaVersion: 1,
            meshNetworkId: "fixture",
            nodeNum: 1,
            firstSeenAt: "2026-07-18T00:00:00.000Z",
            lastSeenAt: "2026-07-18T00:00:01.000Z",
            lastObservationId: "observation-1",
          },
        ],
      }),
      messages: async () => ({
        items: [
          {
            schemaVersion: 1,
            id: "message-1",
            observationId: "observation-1",
            meshNetworkId: "fixture",
            sender: 1,
            text: "fixture",
            observedAt: "2026-07-18T00:00:01.000Z",
          },
        ],
      }),
      telemetry: async () => ({
        items: [
          {
            schemaVersion: 1,
            id: "telemetry-1",
            observationId: "observation-1",
            meshNetworkId: "fixture",
            nodeNum: 1,
            metricKind: "deviceMetrics",
            metrics: { batteryLevel: 73 },
            observedAt: "2026-07-18T00:00:01.000Z",
          },
        ],
      }),
      positions: async () => ({
        items: [
          {
            schemaVersion: 1,
            id: "event-1",
            canonicalKey: "key-1",
            meshNetworkId: "fixture",
            nodeNum: 1,
            sourceObservationId: "observation-1",
            payloadHash: "a".repeat(64),
            position: { latitudeI: 250000000, longitudeI: 1215000000 },
            createdAt: "2026-07-18T00:00:01.000Z",
          },
        ],
      }),
    },
  };
}
