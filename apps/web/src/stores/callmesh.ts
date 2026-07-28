import { isGatewayApiError, type GatewayApiClient } from "@cmclient/api-client";
import type { CallMeshMapping, CallMeshStatus } from "@cmclient/contracts";
import { defineStore } from "pinia";

import { managementApi } from "../management-api";

export interface CallMeshApiClient {
  callmesh: Pick<GatewayApiClient["callmesh"], "overview">;
}

export function createCallMeshStore(client: CallMeshApiClient = managementApi) {
  return defineStore("callmesh", {
    state: () => ({
      loading: false,
      errorCode: undefined as string | undefined,
      status: undefined as CallMeshStatus | undefined,
      mappings: [] as CallMeshMapping[],
    }),
    actions: {
      async refresh() {
        if (this.loading) {
          return;
        }
        this.loading = true;
        try {
          const overview = await client.callmesh.overview();
          this.status = overview.status;
          this.mappings = overview.mappings;
          this.errorCode = undefined;
        } catch (error) {
          this.errorCode = isGatewayApiError(error)
            ? error.code
            : "GATEWAY_NETWORK_UNAVAILABLE";
        } finally {
          this.loading = false;
        }
      },
    },
  });
}

export const useCallMeshStore = createCallMeshStore();
