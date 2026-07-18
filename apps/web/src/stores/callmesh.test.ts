import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";

import { GatewayApiError } from "@cmclient/api-client";

import { createCallMeshStore } from "./callmesh";

describe("CallMesh store", () => {
  beforeEach(() => setActivePinia(createPinia()));

  it("keeps synchronized mappings and stable failures separate", async () => {
    const useCallMesh = createCallMeshStore({
      callmesh: {
        overview: async () => ({
          status: {
            state: "ready",
            updatedAt: "2026-07-18T00:00:00.000Z",
            activeMappingVersion: "mapping-1",
            activeMappingCount: 1,
          },
          mappings: [
            {
              version: "mapping-1",
              effectiveAt: "2026-07-18T00:00:00.000Z",
              meshNetworkId: "fixture",
              nodeNum: 42,
              callsign: "N0CALL-7",
            },
          ],
        }),
      },
    });
    const callmesh = useCallMesh();

    await callmesh.refresh();

    expect(callmesh.status?.state).toBe("ready");
    expect(callmesh.mappings).toHaveLength(1);

    setActivePinia(createPinia());
    const unavailable = createCallMeshStore({
      callmesh: {
        overview: async () => {
          throw new GatewayApiError({ code: "CALLMESH_CLIENT_UNAVAILABLE" });
        },
      },
    });
    const unavailableStore = unavailable();
    await unavailableStore.refresh();
    expect(unavailableStore.errorCode).toBe("CALLMESH_CLIENT_UNAVAILABLE");
  });
});
